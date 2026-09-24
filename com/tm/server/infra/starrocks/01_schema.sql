-- =============================================================================
-- StarRocks local: database + bảng ODS / DWD / DWS cho Friend Network.
-- Bản local của friend_network.sql (gốc repo): replication_num = 1, ít bucket,
-- task chạy nhanh để test (summary 1 phút, daily 5 phút). Chạy lại nhiều lần được.
-- =============================================================================
CREATE DATABASE IF NOT EXISTS social;
USE social;

-- ODS: lịch sử event, append, giữ 180 ngày (audit + tìm user thay đổi cho summary)
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
    "replication_num"       = "1",
    "partition_live_number" = "180"
);

-- DWD: trạng thái mới nhất của mỗi cặp có hướng, upsert theo event_time, giữ vĩnh viễn
CREATE TABLE IF NOT EXISTS dwd_friend_status (
    user_id          BIGINT       NOT NULL,
    friend_id        BIGINT       NOT NULL,
    status           VARCHAR(16)  NOT NULL COMMENT 'event_type mới nhất của cặp',
    last_event_time  DATETIME     NOT NULL COMMENT 'cột so sánh của merge_condition',
    last_event_id    VARCHAR(64)  NOT NULL,
    updated_at       DATETIME     NULL
)
PRIMARY KEY(user_id, friend_id)
DISTRIBUTED BY HASH(user_id) BUCKETS 4
ORDER BY (user_id, status)
PROPERTIES (
    "replication_num"         = "1",
    "enable_persistent_index" = "true"
);

-- DWS: số lượng mới nhất theo user, giữ vĩnh viễn. Task t_friend_summary_refresh (init.sh)
-- chỉ tính lại user có event mới → không phải quét toàn bộ DWD.
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
DISTRIBUTED BY HASH(user_id) BUCKETS 4
PROPERTIES ("replication_num" = "1");

-- DWS theo ngày: task t_friend_daily_snapshot (init.sh) chụp dws_friend_summary, ghi đè partition
-- của ngày đó (chạy lại không nhân đôi). Giữ 180 ngày. dt theo múi giờ StarRocks (local: UTC).
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
DISTRIBUTED BY HASH(user_id) BUCKETS 2
PROPERTIES (
    "replication_num"       = "1",
    "partition_live_number" = "180"
);
