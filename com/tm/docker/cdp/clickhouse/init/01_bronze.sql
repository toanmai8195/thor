-- =============================================================================
-- BRONZE LAYER (CDP Realtime): login_bronze + Kafka Engine
-- =============================================================================
-- Flow:
--   login-event-producer → "login-events"
--        → [cdp-ingestor] validate JSON
--        → "login-events-ingest"
--        → login_kafka (CH Kafka Engine)     ← cùng topic với Flink DW,
--        → login_bronze_mv (Materialized View)  khác consumer group
--        → login_bronze (MergeTree, storage)
-- =============================================================================

CREATE DATABASE IF NOT EXISTS cdp;

-- =============================================================================
-- STORAGE TABLE: cdp.login_bronze (MergeTree)
-- =============================================================================
CREATE TABLE IF NOT EXISTS cdp.login_bronze
(
    event_id    String,
    user_id     UInt64,
    session_id  String,
    device_id   String,
    platform    String,         -- raw: ios/android/web/unknown/empty
    country     String,         -- raw: VN/US/... hoặc empty
    event_date  Date,
    login_at    DateTime64(3),
    ingested_at DateTime64(3) DEFAULT now64(3)
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(event_date)
ORDER BY (event_date, user_id, event_id)
TTL event_date + INTERVAL 90 DAY
SETTINGS index_granularity = 8192;

-- =============================================================================
-- KAFKA ENGINE TABLE: cdp.login_kafka
-- Consume từ "login-events-ingest" — cùng topic với Flink, consumer group riêng
-- =============================================================================
CREATE TABLE IF NOT EXISTS cdp.login_kafka
(
    event_id   String,
    user_id    UInt64,
    session_id String,
    device_id  String,
    platform   String,
    country    String,
    event_date String,   -- "2025-01-15"
    login_at   String    -- "2025-01-15T10:30:00.000Z"
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list           = 'kafka:9092',
    kafka_topic_list            = 'login-events-ingest',
    kafka_group_name            = 'ch-cdp-login',
    kafka_format                = 'JSONEachRow',
    kafka_num_consumers         = 4,
    kafka_max_block_size        = 65536,
    kafka_skip_broken_messages  = 1000,
    kafka_handle_error_mode     = 'stream';

-- =============================================================================
-- MATERIALIZED VIEW: cdp.login_bronze_mv
-- =============================================================================
CREATE MATERIALIZED VIEW IF NOT EXISTS cdp.login_bronze_mv
TO cdp.login_bronze
AS
SELECT
    event_id,
    user_id,
    session_id,
    device_id,
    platform,
    country,
    toDate(event_date)                     AS event_date,
    parseDateTime64BestEffort(login_at, 3) AS login_at
FROM cdp.login_kafka
WHERE _error = '';
