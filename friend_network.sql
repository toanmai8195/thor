-- =============================================================================
-- FRIEND NETWORK TRACKING TRÊN STARROCKS
-- =============================================================================
-- Xem README.md để biết thiết kế chi tiết.
--
-- Thứ tự chạy:
--   1. Database
--   2. Bảng: ODS (180 ngày) → DWD (vĩnh viễn) → DWS summary (vĩnh viễn) → DWS daily (180 ngày)
--   3. Routine Load (sửa kafka_broker_list trước khi chạy)
--   4. Task: cập nhật summary (10 phút), chụp daily (00:05 mỗi ngày) — StarRocks 3.3+
--   5. Query: chạy từng câu, đổi giá trị biến @... ở đầu phần 5
--
-- Bản local (replication 1, task chạy nhanh): com/tm/infra/starrocks/
-- =============================================================================


-- =============================================================================
-- 1. DATABASE
-- =============================================================================
CREATE DATABASE IF NOT EXISTS social;
USE social;


-- =============================================================================
-- 2. BẢNG
-- =============================================================================

-- -----------------------------------------------------------------------------
-- 2.1 ODS: lịch sử event, append, giữ 180 ngày (audit + tìm user thay đổi cho 2.3)
-- -----------------------------------------------------------------------------
-- ~17 triệu event/ngày (100 hành động/s) ≈ 0,5–0,8 GB nén/ngày → 4 bucket mỗi partition ngày.
CREATE TABLE IF NOT EXISTS ods_friend_event (
    user_id      BIGINT       NOT NULL,
    friend_id    BIGINT       NOT NULL,
    event_time   DATETIME     NOT NULL COMMENT 'thời điểm hành động, do service sinh (ms)',
    event_type   VARCHAR(16)  NOT NULL COMMENT 'REQUESTED/REVIEWED/FRIEND/CANCEL/UNFRIEND/BLOCKING/BLOCKED',
    event_id     VARCHAR(64)  NOT NULL COMMENT 'tăng dần, tie-break khi trùng event_time',
    source       VARCHAR(32)  NULL,
    ingest_time  DATETIME     NULL     COMMENT 'thời điểm vào StarRocks'
)
DUPLICATE KEY(user_id, friend_id, event_time)
PARTITION BY date_trunc('day', event_time)
DISTRIBUTED BY HASH(user_id) BUCKETS 4
PROPERTIES (
    "replication_num"       = "3",
    "partition_live_number" = "180"   -- TTL: chỉ giữ 180 partition ngày gần nhất
);

-- -----------------------------------------------------------------------------
-- 2.2 DWD: trạng thái mới nhất của mỗi cặp có hướng, upsert theo event_time, giữ vĩnh viễn
-- -----------------------------------------------------------------------------
-- 50M user × 500–1000 quan hệ = 25–50 tỉ dòng, ~0,5–1,5 TB nén → ~256 bucket (1–10 GB/tablet).
-- Chỉnh theo số cặp thực tế trước khi tạo (đổi sau phải ghi lại dữ liệu).
CREATE TABLE IF NOT EXISTS dwd_friend_status (
    user_id          BIGINT       NOT NULL,
    friend_id        BIGINT       NOT NULL,
    status           VARCHAR(16)  NOT NULL COMMENT 'event_type mới nhất của cặp',
    last_event_time  DATETIME     NOT NULL COMMENT 'cột so sánh của merge_condition',
    last_event_id    VARCHAR(64)  NOT NULL,
    updated_at       DATETIME     NULL
)
PRIMARY KEY(user_id, friend_id)
DISTRIBUTED BY HASH(user_id) BUCKETS 256
ORDER BY (user_id, status)
PROPERTIES (
    "replication_num"         = "3",
    "enable_persistent_index" = "true"
);

-- -----------------------------------------------------------------------------
-- 2.3 DWS: số lượng mới nhất theo user, giữ vĩnh viễn
-- -----------------------------------------------------------------------------
-- Không dùng MV: MV phải tính lại toàn bộ DWD (hàng chục tỉ dòng) mỗi lần refresh.
-- Task 4.1 chỉ tính lại user có event mới (≤ ~120k user / 10 phút).
CREATE TABLE IF NOT EXISTS dws_friend_summary (
    user_id               BIGINT       NOT NULL,
    friend_cnt            BIGINT,
    pending_sent_cnt      BIGINT,
    pending_received_cnt  BIGINT,
    blocking_cnt          BIGINT,
    blocked_cnt           BIGINT,
    last_activity         DATETIME,
    computed_at           DATETIME     COMMENT 'lần tính gần nhất; mốc để lần sau tìm user thay đổi'
)
PRIMARY KEY(user_id)
DISTRIBUTED BY HASH(user_id) BUCKETS 16
PROPERTIES ("replication_num" = "3");

-- -----------------------------------------------------------------------------
-- 2.4 DWS theo ngày: xu hướng số lượng, giữ 180 ngày
-- -----------------------------------------------------------------------------
-- 50M dòng/ngày ≈ 0,5–1 GB nén/ngày. Duplicate Key + ghi đè partition (task 4.2):
-- chạy lại không nhân đôi, không tốn index khoá chính cho hàng tỉ dòng.
CREATE TABLE IF NOT EXISTS dws_friend_daily (
    dt                    DATE         NOT NULL,
    user_id               BIGINT       NOT NULL,
    friend_cnt            BIGINT,
    pending_sent_cnt      BIGINT,
    pending_received_cnt  BIGINT,
    blocking_cnt          BIGINT,
    blocked_cnt           BIGINT,
    last_activity         DATETIME,
    snapshot_at           DATETIME     COMMENT 'lần chụp gần nhất'
)
DUPLICATE KEY(dt, user_id)
PARTITION BY dt
DISTRIBUTED BY HASH(user_id) BUCKETS 4
PROPERTIES (
    "replication_num"       = "3",
    "partition_live_number" = "180"
);


-- =============================================================================
-- 3. ROUTINE LOAD (Kafka → StarRocks)
-- =============================================================================
-- ODS và DWD đọc song song cùng topic, khác consumer group.

-- -----------------------------------------------------------------------------
-- 3.1 Kafka → ODS
-- -----------------------------------------------------------------------------
CREATE ROUTINE LOAD social.rl_friend_ods ON ods_friend_event
COLUMNS (user_id, friend_id, event_time, event_type, event_id, source, ingest_time = now())
PROPERTIES (
    "format"                    = "json",
    "jsonpaths"                 = "[\"$.user_id\",\"$.friend_id\",\"$.event_time\",\"$.event_type\",\"$.event_id\",\"$.source\"]",
    "desired_concurrent_number" = "3",
    "max_error_number"          = "1000"
)
FROM KAFKA (
    "kafka_broker_list"              = "broker1:9092,broker2:9092",
    "kafka_topic"                    = "friend_events",
    "property.group.id"              = "sr_friend_ods",
    "property.kafka_default_offsets" = "OFFSET_BEGINNING"
);

-- -----------------------------------------------------------------------------
-- 3.2 Kafka → DWD
-- merge_condition: chỉ ghi đè khi event_time mới >= giá trị đang có
-- → event đến trễ không làm hỏng trạng thái.
-- -----------------------------------------------------------------------------
CREATE ROUTINE LOAD social.rl_friend_dwd ON dwd_friend_status
COLUMNS (user_id, friend_id, status, last_event_time, last_event_id, updated_at = now())
PROPERTIES (
    "format"                    = "json",
    "jsonpaths"                 = "[\"$.user_id\",\"$.friend_id\",\"$.event_type\",\"$.event_time\",\"$.event_id\"]",
    "merge_condition"           = "last_event_time",
    "desired_concurrent_number" = "3",
    "max_error_number"          = "1000"
)
FROM KAFKA (
    "kafka_broker_list"              = "broker1:9092,broker2:9092",
    "kafka_topic"                    = "friend_events",
    "property.group.id"              = "sr_friend_dwd",
    "property.kafka_default_offsets" = "OFFSET_BEGINNING"
);


-- =============================================================================
-- 4. TASK (StarRocks 3.3+). Giờ theo múi giờ StarRocks (@@time_zone).
-- =============================================================================

-- -----------------------------------------------------------------------------
-- 4.1 Cập nhật dws_friend_summary mỗi 10 phút, chỉ cho user có event mới
-- -----------------------------------------------------------------------------
-- Mốc = computed_at lớn nhất lần trước (task dừng lâu thì lần sau tự lấy bù);
-- lùi 5 phút để chờ DWD nạp chậm hơn ODS. event_time lọc để chỉ đọc partition gần đây.
SUBMIT TASK t_friend_summary_refresh
SCHEDULE EVERY (INTERVAL 10 MINUTE)
AS
INSERT INTO dws_friend_summary
SELECT user_id,
       SUM(IF(status = 'FRIEND',    1, 0)),
       SUM(IF(status = 'REQUESTED', 1, 0)),
       SUM(IF(status = 'REVIEWED',  1, 0)),
       SUM(IF(status = 'BLOCKING',  1, 0)),
       SUM(IF(status = 'BLOCKED',   1, 0)),
       MAX(last_event_time),
       NOW()
FROM dwd_friend_status
WHERE user_id IN (
    SELECT DISTINCT user_id FROM ods_friend_event
    WHERE ingest_time >= (SELECT COALESCE(MAX(computed_at), '1970-01-01') FROM dws_friend_summary) - INTERVAL 5 MINUTE
      AND event_time  >= (SELECT COALESCE(MAX(computed_at), '1970-01-01') FROM dws_friend_summary) - INTERVAL 1 DAY
)
GROUP BY user_id;

-- Chạy 1 lần sau khi nạp dữ liệu ban đầu vào DWD (task 4.1 chỉ xử lý user có event mới):
-- INSERT INTO dws_friend_summary
-- SELECT user_id, SUM(IF(status = 'FRIEND', 1, 0)), SUM(IF(status = 'REQUESTED', 1, 0)),
--        SUM(IF(status = 'REVIEWED', 1, 0)), SUM(IF(status = 'BLOCKING', 1, 0)),
--        SUM(IF(status = 'BLOCKED', 1, 0)), MAX(last_event_time), NOW()
-- FROM dwd_friend_status GROUP BY user_id;

-- -----------------------------------------------------------------------------
-- 4.2 Chụp dws_friend_summary vào dws_friend_daily lúc 00:05 mỗi ngày
-- -----------------------------------------------------------------------------
-- dt = ngày của (NOW() - 10 phút) → chạy 00:05 ghi cho hôm qua = số cuối ngày.
-- Ghi đè partition của dt (dynamic_overwrite) → chạy lại không nhân đôi.
SUBMIT TASK t_friend_daily_snapshot
SCHEDULE START ('2026-09-25 00:05:00') EVERY (INTERVAL 1 DAY)
AS
INSERT /*+SET_VAR(dynamic_overwrite = true)*/ OVERWRITE dws_friend_daily
SELECT DATE(NOW() - INTERVAL 10 MINUTE), user_id,
       friend_cnt, pending_sent_cnt, pending_received_cnt,
       blocking_cnt, blocked_cnt, last_activity, NOW()
FROM dws_friend_summary;


-- =============================================================================
-- 5. QUERY
-- =============================================================================
-- Đổi giá trị biến rồi chạy từng câu.
SET @uid      = 1001;                       -- user cần xem
SET @other    = 2002;                       -- user thứ 2 (Q5, Q6, Q9)
SET @from_dt  = '2026-06-01';               -- khoảng ngày cho Q10 (trong 180 ngày)
SET @to_dt    = '2026-09-23';

-- -----------------------------------------------------------------------------
-- 5.1 TRẠNG THÁI MỚI NHẤT (DWD / DWS, giữ vĩnh viễn)
-- -----------------------------------------------------------------------------

-- Q1. Số partners ở mỗi status của 1 user (DWD, chính xác tức thì)
SELECT status, COUNT(*) AS partners
FROM dwd_friend_status
WHERE user_id = @uid
GROUP BY status;

-- Q2. Partners ở mỗi status của 1 user là ai (thêm AND status = '...' để lọc 1 status)
SELECT status, friend_id, last_event_time AS since
FROM dwd_friend_status
WHERE user_id = @uid
ORDER BY status, friend_id;

-- Q3. Đang block ai / đang bị ai block
SELECT friend_id, status
FROM dwd_friend_status
WHERE user_id = @uid AND status IN ('BLOCKING', 'BLOCKED');

-- Q4. Lời mời đã gửi / đã nhận đang chờ
SELECT friend_id, status, last_event_time
FROM dwd_friend_status
WHERE user_id = @uid AND status IN ('REQUESTED', 'REVIEWED');

-- Q5. Quan hệ giữa 2 user cụ thể
SELECT status FROM dwd_friend_status WHERE user_id = @uid AND friend_id = @other;

-- Q6. Bạn chung của 2 user
SELECT a.friend_id
FROM dwd_friend_status a
JOIN dwd_friend_status b ON a.friend_id = b.friend_id
WHERE a.user_id = @uid AND b.user_id = @other
  AND a.status = 'FRIEND' AND b.status = 'FRIEND';

-- Q7. Số lượng theo status cho nhiều user cùng lúc (DWS, trễ tối đa 1 chu kỳ task 4.1)
--     vd top 100 user nhiều bạn nhất
SELECT user_id, friend_cnt, pending_sent_cnt, pending_received_cnt, blocking_cnt, blocked_cnt
FROM dws_friend_summary
ORDER BY friend_cnt DESC
LIMIT 100;

-- -----------------------------------------------------------------------------
-- 5.2 LỊCH SỬ (ODS / DWS daily, giữ 180 ngày)
-- -----------------------------------------------------------------------------

-- Q9. Lịch sử event giữa 2 user trong 180 ngày gần nhất
SELECT event_time, event_type, event_id
FROM ods_friend_event
WHERE user_id = @uid AND friend_id = @other
ORDER BY event_time, event_id;

-- Q10. Xu hướng số lượng theo ngày của 1 user
SELECT dt, friend_cnt, pending_sent_cnt, pending_received_cnt, blocking_cnt, blocked_cnt
FROM dws_friend_daily
WHERE user_id = @uid AND dt BETWEEN @from_dt AND @to_dt
ORDER BY dt;


-- =============================================================================
-- 6. VẬN HÀNH / MONITOR
-- =============================================================================

-- Trạng thái Routine Load: xem State, ReasonOfStateChanged, ErrorLogUrls
SHOW ROUTINE LOAD FROM social;

-- Resume khi bị PAUSED (sau khi đã xử lý nguyên nhân)
-- RESUME ROUTINE LOAD FOR social.rl_friend_ods;
-- RESUME ROUTINE LOAD FOR social.rl_friend_dwd;

-- Lịch sử chạy task (summary, daily)
SELECT task_name, state, error_message, create_time, finish_time
FROM information_schema.task_runs
WHERE task_name IN ('t_friend_summary_refresh', 't_friend_daily_snapshot')
ORDER BY create_time DESC
LIMIT 20;

-- Độ trễ của summary: computed_at lớn nhất phải trong khoảng 1–2 chu kỳ task
SELECT MAX(computed_at), NOW() FROM dws_friend_summary;
