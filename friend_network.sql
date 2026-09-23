-- =============================================================================
-- FRIEND NETWORK TRACKING TRÊN STARROCKS
-- =============================================================================
-- Xem README.md để biết thiết kế chi tiết.
--
-- Thứ tự chạy:
--   1. Database
--   2. Bảng: ODS → DWD → DWS (MV) → DWS daily (tuỳ chọn)
--   3. Routine Load (sửa kafka_broker_list trước khi chạy)
--   4. Task snapshot hằng ngày (tuỳ chọn, StarRocks 3.3+)
--   5. Query: chạy từng câu, đổi giá trị biến @... ở đầu phần 5
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
-- 2.1 ODS: lịch sử event, append, giữ 30 ngày
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ods_friend_event (
    user_id      BIGINT       NOT NULL,
    friend_id    BIGINT       NOT NULL,
    event_time   DATETIME(3)  NOT NULL COMMENT 'thời điểm hành động, do service sinh',
    event_type   VARCHAR(16)  NOT NULL COMMENT 'REQUESTED/REVIEWED/FRIEND/CANCEL/UNFRIEND/BLOCKING/BLOCKED',
    event_id     VARCHAR(64)  NOT NULL COMMENT 'tăng dần, tie-break khi trùng event_time',
    source       VARCHAR(32)  NULL,
    ingest_time  DATETIME     NULL     COMMENT 'thời điểm vào StarRocks'
)
DUPLICATE KEY(user_id, friend_id, event_time)
PARTITION BY date_trunc('day', event_time)
DISTRIBUTED BY HASH(user_id) BUCKETS 32
PROPERTIES (
    "replication_num"       = "3",
    "colocate_with"         = "grp_user",
    "partition_live_number" = "30"   -- TTL: chỉ giữ 30 partition ngày gần nhất
);

-- -----------------------------------------------------------------------------
-- 2.2 DWD: trạng thái hiện tại của mỗi cặp có hướng, upsert theo event_time
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dwd_friend_status (
    user_id          BIGINT       NOT NULL,
    friend_id        BIGINT       NOT NULL,
    status           VARCHAR(16)  NOT NULL COMMENT 'event_type mới nhất của cặp',
    last_event_time  DATETIME(3)  NOT NULL COMMENT 'cột so sánh của merge_condition',
    last_event_id    VARCHAR(64)  NOT NULL,
    updated_at       DATETIME     NULL
)
PRIMARY KEY(user_id, friend_id)
DISTRIBUTED BY HASH(user_id) BUCKETS 32
ORDER BY (user_id, status)
PROPERTIES (
    "replication_num"         = "3",
    "enable_persistent_index" = "true",
    "colocate_with"           = "grp_user"
);

-- -----------------------------------------------------------------------------
-- 2.3 DWS: số liệu hiện tại theo user, refresh 10 phút
-- -----------------------------------------------------------------------------
CREATE MATERIALIZED VIEW IF NOT EXISTS dws_friend_summary
DISTRIBUTED BY HASH(user_id) BUCKETS 32
REFRESH ASYNC EVERY (INTERVAL 10 MINUTE)
AS
SELECT user_id,
       SUM(IF(status = 'FRIEND',    1, 0)) AS friend_cnt,
       SUM(IF(status = 'REQUESTED', 1, 0)) AS pending_sent_cnt,
       SUM(IF(status = 'REVIEWED',  1, 0)) AS pending_received_cnt,
       SUM(IF(status = 'BLOCKING',  1, 0)) AS blocking_cnt,
       SUM(IF(status = 'BLOCKED',   1, 0)) AS blocked_cnt,
       MAX(last_event_time)                AS last_activity
FROM dwd_friend_status
GROUP BY user_id;

-- -----------------------------------------------------------------------------
-- 2.4 (Tuỳ chọn) DWS theo ngày: xu hướng dài hơn 30 ngày
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dws_friend_daily (
    dt                    DATE         NOT NULL,
    user_id               BIGINT       NOT NULL,
    friend_cnt            BIGINT,
    pending_sent_cnt      BIGINT,
    pending_received_cnt  BIGINT,
    blocking_cnt          BIGINT,
    blocked_cnt           BIGINT,
    last_activity         DATETIME(3)
)
DUPLICATE KEY(dt, user_id)
PARTITION BY dt
DISTRIBUTED BY HASH(user_id) BUCKETS 32
PROPERTIES ("replication_num" = "3");


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
-- 4. (Tuỳ chọn) TASK SNAPSHOT HẰNG NGÀY — StarRocks 3.3+
-- =============================================================================
-- Bản cũ hơn: chạy câu INSERT bên trong từ scheduler bên ngoài (Java/cron).
SUBMIT TASK t_friend_daily_snapshot
SCHEDULE START ('2026-09-24 00:05:00') EVERY (INTERVAL 1 DAY)
AS
INSERT INTO dws_friend_daily
SELECT CURRENT_DATE() - INTERVAL 1 DAY, user_id,
       friend_cnt, pending_sent_cnt, pending_received_cnt,
       blocking_cnt, blocked_cnt, last_activity
FROM dws_friend_summary;


-- =============================================================================
-- 5. QUERY
-- =============================================================================
-- Đổi giá trị biến rồi chạy từng câu.
SET @uid      = 1001;                       -- user cần xem
SET @other    = 2002;                       -- user thứ 2 (Q5, Q6, Q9)
SET @ts       = '2026-09-23 12:00:00.000';  -- thời điểm as-of (Q8), trong 30 ngày gần nhất
SET @from_dt  = '2026-01-01';               -- khoảng ngày cho Q10
SET @to_dt    = '2026-09-23';

-- -----------------------------------------------------------------------------
-- 5.1 HIỆN TẠI (đọc DWD / DWS)
-- -----------------------------------------------------------------------------

-- Q1. Số liệu tổng hợp của user (trễ tối đa = chu kỳ refresh MV)
SELECT * FROM dws_friend_summary WHERE user_id = @uid;

-- Q2. Danh sách bạn
SELECT friend_id, last_event_time AS friend_since
FROM dwd_friend_status
WHERE user_id = @uid AND status = 'FRIEND';

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

-- Q7. Số bạn chính xác tuyệt đối (không chờ MV refresh)
SELECT COUNT(*) AS friend_cnt
FROM dwd_friend_status
WHERE user_id = @uid AND status = 'FRIEND';

-- -----------------------------------------------------------------------------
-- 5.2 QUÁ KHỨ (đọc ODS, chỉ trong 30 ngày gần nhất)
-- -----------------------------------------------------------------------------

-- Q8. Danh sách bạn của user tại thời điểm @ts
--     Đổi SELECT friend_id → SELECT COUNT(*) để lấy số lượng,
--     đổi 'FRIEND' → 'BLOCKING' / 'BLOCKED' cho câu hỏi block.
SELECT friend_id
FROM (
    SELECT friend_id, event_type,
           ROW_NUMBER() OVER (PARTITION BY friend_id
                              ORDER BY event_time DESC, event_id DESC) AS rn
    FROM ods_friend_event
    WHERE user_id = @uid AND event_time <= @ts
) t
WHERE rn = 1 AND event_type = 'FRIEND';

-- Q9. Lịch sử quan hệ giữa 2 user trong 30 ngày gần nhất
SELECT event_time, event_type, event_id
FROM ods_friend_event
WHERE user_id = @uid AND friend_id = @other
ORDER BY event_time, event_id;

-- Q10. Xu hướng số bạn theo ngày, không giới hạn 30 ngày (cần dws_friend_daily)
SELECT dt, friend_cnt
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

-- Lịch sử refresh MV
SELECT task_name, state, error_message, create_time, finish_time
FROM information_schema.task_runs
WHERE definition LIKE '%dws_friend_summary%'
ORDER BY create_time DESC
LIMIT 20;

-- Refresh MV thủ công (khi cần số liệu ngay)
-- REFRESH MATERIALIZED VIEW dws_friend_summary;
