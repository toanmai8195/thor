-- =============================================================================
-- PHONEBOOK (DANH BẠ) TRÊN STARROCKS
-- =============================================================================
-- Xem PHONEBOOK.md để biết thiết kế chi tiết.
--
-- Số điện thoại chỉ có ở dạng phone_enc (BIGINT, FF1, PHONEBOOK.md mục 4);
-- decode bằng CLI phonecodec, StarRocks không có khoá.
--
-- Thứ tự chạy:
--   1. Database
--   2. Bảng: ODS (30 ngày) → DWD → DWS summary → DWS daily (180 ngày) [→ DWD theo phone, tuỳ chọn]
--   3. Routine Load từ topic contact_events (sửa kafka_broker_list trước khi chạy)
--   4. Task: summary (10 phút), daily (00:05), dọn dòng đã xoá (01:00) — StarRocks 3.3+
--   5. Query: đổi giá trị biến @... rồi chạy từng câu
-- =============================================================================


-- =============================================================================
-- 1. DATABASE (dùng chung với friend network)
-- =============================================================================
CREATE DATABASE IF NOT EXISTS social;
USE social;


-- =============================================================================
-- 2. BẢNG
-- =============================================================================

-- -----------------------------------------------------------------------------
-- 2.1 ODS: lịch sử event, giữ 30 ngày (tìm user thay đổi cho 4.1, xem các lần sync gần đây)
-- -----------------------------------------------------------------------------
-- ~450M event/ngày ≈ 5 GB nén/ngày (xấu nhất ~4,3 tỉ ≈ 50 GB) → 16 bucket mỗi partition ngày.
CREATE TABLE IF NOT EXISTS ods_contact_event (
    user_id       BIGINT       NOT NULL,
    phone_enc     BIGINT       NOT NULL COMMENT 'số đã mã hoá FF1',
    event_time    DATETIME     NOT NULL COMMENT 'thời điểm sync, do service sinh (ms)',
    event_type    VARCHAR(8)   NOT NULL COMMENT 'ADD/DELETE',
    sync_id       VARCHAR(20)  NOT NULL COMMENT 'giống nhau cho mọi event của 1 lần sync',
    ingest_time   DATETIME     NULL     COMMENT 'thời điểm vào StarRocks'
)
DUPLICATE KEY(user_id, phone_enc, event_time)
PARTITION BY date_trunc('day', event_time)
DISTRIBUTED BY HASH(user_id) BUCKETS 16
PROPERTIES (
    "replication_num"       = "3",
    "partition_live_number" = "30"
);

-- -----------------------------------------------------------------------------
-- 2.2 DWD: trạng thái mới nhất của mỗi (user, phone), upsert theo event_time
-- -----------------------------------------------------------------------------
-- 50M user × 1.000 = 50 tỉ dòng. Khoá 2 BIGINT (16 byte) để persistent index nhỏ nhất
-- (~1–1,5 TB/bản sao); dữ liệu ~0,4–0,6 TB/bản sao → 256 bucket (~2 GB/tablet).
-- Xoá mềm (active = 0) để merge_condition chặn event DELETE đến trễ; dọn ở task 4.3.
CREATE TABLE IF NOT EXISTS dwd_contact (
    user_id          BIGINT    NOT NULL,
    phone_enc        BIGINT    NOT NULL,
    active           TINYINT   NOT NULL COMMENT '1 = đang trong danh bạ, 0 = đã xoá',
    last_event_time  DATETIME  NOT NULL COMMENT 'cột so sánh của merge_condition'
)
PRIMARY KEY(user_id, phone_enc)
DISTRIBUTED BY HASH(user_id) BUCKETS 256
ORDER BY (user_id, active)
PROPERTIES (
    "replication_num"         = "3",
    "enable_persistent_index" = "true"
);

-- -----------------------------------------------------------------------------
-- 2.3 DWS: số contact mới nhất theo user, giữ vĩnh viễn
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dws_contact_summary (
    user_id           BIGINT    NOT NULL,
    contact_cnt       BIGINT    COMMENT 'số contact active',
    last_change_time  DATETIME,
    computed_at       DATETIME  COMMENT 'lần tính gần nhất; mốc để lần sau tìm user thay đổi'
)
PRIMARY KEY(user_id)
DISTRIBUTED BY HASH(user_id) BUCKETS 16
PROPERTIES ("replication_num" = "3");

-- -----------------------------------------------------------------------------
-- 2.4 DWS theo ngày: xu hướng số contact, giữ 180 ngày
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dws_contact_daily (
    dt                DATE      NOT NULL,
    user_id           BIGINT    NOT NULL,
    contact_cnt       BIGINT,
    last_change_time  DATETIME,
    snapshot_at       DATETIME
)
DUPLICATE KEY(dt, user_id)
PARTITION BY dt
DISTRIBUTED BY HASH(user_id) BUCKETS 4
PROPERTIES (
    "replication_num"       = "3",
    "partition_live_number" = "180"
);

-- -----------------------------------------------------------------------------
-- 2.5 (TUỲ CHỌN) DWD theo phone: "ai đang lưu số X" mà không quét 256 bucket.
-- Gấp đôi dung lượng + index của DWD → chỉ tạo khi thật cần (kèm Routine Load 3.3).
-- -----------------------------------------------------------------------------
-- CREATE TABLE IF NOT EXISTS dwd_contact_by_phone (
--     phone_enc        BIGINT    NOT NULL,
--     user_id          BIGINT    NOT NULL,
--     active           TINYINT   NOT NULL,
--     last_event_time  DATETIME  NOT NULL
-- )
-- PRIMARY KEY(phone_enc, user_id)
-- DISTRIBUTED BY HASH(phone_enc) BUCKETS 256
-- ORDER BY (phone_enc, active)
-- PROPERTIES ("replication_num" = "3", "enable_persistent_index" = "true");


-- =============================================================================
-- 3. ROUTINE LOAD (Kafka contact_events → StarRocks)
-- =============================================================================
-- 1 message = mảng JSON ≤ 1.000 event cùng user → strip_outer_array tách thành từng dòng.
-- ODS và DWD đọc song song cùng topic, khác consumer group.

-- 3.1 Kafka → ODS
CREATE ROUTINE LOAD social.rl_contact_ods ON ods_contact_event
COLUMNS (user_id, phone_enc, event_type, event_time, sync_id, ingest_time = now())
PROPERTIES (
    "format"                    = "json",
    "strip_outer_array"         = "true",
    "jsonpaths"                 = "[\"$.user_id\",\"$.phone_enc\",\"$.event_type\",\"$.event_time\",\"$.sync_id\"]",
    "desired_concurrent_number" = "6",
    "max_error_number"          = "1000"
)
FROM KAFKA (
    "kafka_broker_list"              = "broker1:9092,broker2:9092",
    "kafka_topic"                    = "contact_events",
    "property.group.id"              = "sr_contact_ods",
    "property.kafka_default_offsets" = "OFFSET_BEGINNING"
);

-- 3.2 Kafka → DWD
-- event_type không có trong bảng: cột tạm để tính active.
-- merge_condition: chỉ ghi đè khi event_time mới >= giá trị đang có.
CREATE ROUTINE LOAD social.rl_contact_dwd ON dwd_contact
COLUMNS (user_id, phone_enc, event_type, last_event_time,
         active = IF(event_type = 'DELETE', 0, 1))
PROPERTIES (
    "format"                    = "json",
    "strip_outer_array"         = "true",
    "jsonpaths"                 = "[\"$.user_id\",\"$.phone_enc\",\"$.event_type\",\"$.event_time\"]",
    "merge_condition"           = "last_event_time",
    "desired_concurrent_number" = "6",
    "max_error_number"          = "1000"
)
FROM KAFKA (
    "kafka_broker_list"              = "broker1:9092,broker2:9092",
    "kafka_topic"                    = "contact_events",
    "property.group.id"              = "sr_contact_dwd",
    "property.kafka_default_offsets" = "OFFSET_BEGINNING"
);

-- 3.3 (TUỲ CHỌN, đi cùng bảng 2.5): như 3.2, ON dwd_contact_by_phone, group sr_contact_by_phone.


-- =============================================================================
-- 4. TASK (StarRocks 3.3+). Giờ theo múi giờ StarRocks (@@time_zone).
-- =============================================================================

-- -----------------------------------------------------------------------------
-- 4.1 Cập nhật dws_contact_summary mỗi 10 phút, chỉ cho user có event mới
-- -----------------------------------------------------------------------------
-- Mốc = MAX(computed_at) lần trước, lùi 5 phút chờ DWD nạp chậm hơn ODS.
-- Chỉ user có danh bạ đổi mới có event → ≤ 30k user, ≤ ~30M dòng DWD mỗi lần.
SUBMIT TASK t_contact_summary_refresh
SCHEDULE EVERY (INTERVAL 10 MINUTE)
AS
INSERT INTO dws_contact_summary
SELECT user_id,
       SUM(active),
       MAX(last_event_time),
       NOW()
FROM dwd_contact
WHERE user_id IN (
    SELECT DISTINCT user_id FROM ods_contact_event
    WHERE ingest_time >= (SELECT COALESCE(MAX(computed_at), '1970-01-01') FROM dws_contact_summary) - INTERVAL 5 MINUTE
      AND event_time  >= (SELECT COALESCE(MAX(computed_at), '1970-01-01') FROM dws_contact_summary) - INTERVAL 1 DAY
)
GROUP BY user_id;

-- Chạy 1 lần nếu DWD được nạp bằng cách khác Routine Load (vd Broker Load từ bản export):
-- INSERT INTO dws_contact_summary
-- SELECT user_id, SUM(active), MAX(last_event_time), NOW() FROM dwd_contact GROUP BY user_id;

-- -----------------------------------------------------------------------------
-- 4.2 Chụp dws_contact_summary vào dws_contact_daily lúc 00:05 mỗi ngày
-- -----------------------------------------------------------------------------
SUBMIT TASK t_contact_daily_snapshot
SCHEDULE START ('2026-09-25 00:05:00') EVERY (INTERVAL 1 DAY)
AS
INSERT /*+SET_VAR(dynamic_overwrite = true)*/ OVERWRITE dws_contact_daily
SELECT DATE(NOW() - INTERVAL 10 MINUTE), user_id, contact_cnt, last_change_time, NOW()
FROM dws_contact_summary;

-- -----------------------------------------------------------------------------
-- 4.3 Dọn dòng đã xoá quá 30 ngày, 01:00 mỗi ngày
-- -----------------------------------------------------------------------------
-- Không có event nào trễ tới 30 ngày (gửi bù sau lỗi Kafka dùng event_time mới)
-- → xoá thật không bị event cũ ghi đè sai. Không đổi contact_cnt (chỉ đếm active = 1).
SUBMIT TASK t_contact_purge_deleted
SCHEDULE START ('2026-09-25 01:00:00') EVERY (INTERVAL 1 DAY)
AS
DELETE FROM dwd_contact
WHERE active = 0 AND last_event_time < NOW() - INTERVAL 30 DAY;


-- =============================================================================
-- 5. QUERY
-- =============================================================================
SET @uid      = 1001;                 -- user cần xem
SET @other    = 2002;                 -- user thứ 2 (P5)
SET @phone    = 1735820019;           -- phone_enc cần tra (P6): phonecodec encode 0901234567
SET @from_dt  = '2026-06-01';         -- khoảng ngày cho P8
SET @to_dt    = '2026-09-24';

-- -----------------------------------------------------------------------------
-- 5.1 TRẠNG THÁI MỚI NHẤT
-- -----------------------------------------------------------------------------

-- P1. User có bao nhiêu contact (DWD, chính xác tức thì, đọc 1 bucket)
SELECT COUNT(*) AS contact_cnt
FROM dwd_contact
WHERE user_id = @uid AND active = 1;

-- P2. User có những số nào (decode phone_enc bằng phonecodec)
SELECT phone_enc, last_event_time AS since
FROM dwd_contact
WHERE user_id = @uid AND active = 1
ORDER BY phone_enc;

-- P3. Số user có trên 500 contact (DWS, trễ ≤ 1 chu kỳ task 4.1)
SELECT COUNT(*) AS users
FROM dws_contact_summary
WHERE contact_cnt > 500;

-- P4. Phân bố số contact theo user
SELECT CASE WHEN contact_cnt <= 100  THEN '0-100'
            WHEN contact_cnt <= 500  THEN '101-500'
            WHEN contact_cnt <= 1000 THEN '501-1000'
            WHEN contact_cnt <= 5000 THEN '1001-5000'
            ELSE '>5000' END AS bucket,
       COUNT(*) AS users
FROM dws_contact_summary
GROUP BY bucket
ORDER BY MIN(contact_cnt);

-- P5. Contact chung của 2 user
SELECT a.phone_enc
FROM dwd_contact a
JOIN dwd_contact b ON a.phone_enc = b.phone_enc
WHERE a.user_id = @uid AND b.user_id = @other
  AND a.active = 1 AND b.active = 1;

-- P6. Những ai đang lưu số X
--     Có bảng 2.5: đọc 1 bucket.
-- SELECT user_id FROM dwd_contact_by_phone WHERE phone_enc = @phone AND active = 1;
--     Không có bảng 2.5: quét toàn bộ DWD (50 tỉ dòng), chỉ chạy thỉnh thoảng.
SELECT user_id FROM dwd_contact WHERE phone_enc = @phone AND active = 1;

-- -----------------------------------------------------------------------------
-- 5.2 LỊCH SỬ
-- -----------------------------------------------------------------------------

-- P7. Các lần sync có thay đổi của user (30 ngày)
SELECT sync_id,
       MIN(event_time)                      AS sync_time,
       SUM(IF(event_type = 'ADD',    1, 0)) AS added,
       SUM(IF(event_type = 'DELETE', 1, 0)) AS deleted
FROM ods_contact_event
WHERE user_id = @uid
GROUP BY sync_id
ORDER BY sync_time DESC;

-- P8. Số contact của user theo ngày (180 ngày)
SELECT dt, contact_cnt
FROM dws_contact_daily
WHERE user_id = @uid AND dt BETWEEN @from_dt AND @to_dt
ORDER BY dt;
