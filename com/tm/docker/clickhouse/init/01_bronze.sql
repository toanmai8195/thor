-- =============================================================================
-- BRONZE LAYER: payments_bronze + Kafka Engine
-- =============================================================================
-- ClickHouse Kafka Engine consume "payment-events-ingest" (đã được validate
-- bởi Go tracking-ingestor), Materialized View pipe vào MergeTree storage.
--
-- Flow:
--   event-producer → "payment-events"
--        → Go tracking-ingestor (validate JSON)
--        → "payment-events-ingest"
--        → payments_kafka (CH Kafka Engine)
--        → payments_bronze_mv (Materialized View)
--        → payments_bronze (MergeTree, storage thật)
-- =============================================================================

CREATE DATABASE IF NOT EXISTS tracking;

-- =============================================================================
-- STORAGE TABLE: payments_bronze (MergeTree)
-- =============================================================================
-- Đây là bảng lưu trữ thực sự, không thay đổi schema.
-- Kafka Engine + MV sẽ insert vào đây tự động.
-- =============================================================================

CREATE TABLE IF NOT EXISTS tracking.payments_bronze
(
    event_id      String,           -- UUID của payment event
    user_id       UInt64,           -- ID user
    amount        Float64,          -- Số tiền (raw, chưa validate)
    status        String,           -- Trạng thái thô: "paid" / "PAID" / ...
    payment_date  Date,             -- Ngày thanh toán
    updated_at    DateTime64(3),    -- Thời điểm record được cập nhật (ms)

    -- Ingestion metadata (tự động set bởi ClickHouse)
    ingested_at   DateTime64(3) DEFAULT now64(3)
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(payment_date)
ORDER BY (payment_date, event_id)
TTL payment_date + INTERVAL 90 DAY
SETTINGS index_granularity = 8192;

-- =============================================================================
-- KAFKA ENGINE TABLE: payments_kafka
-- =============================================================================
-- Đây là "virtual table" - không lưu data, chỉ là cầu nối tới Kafka.
-- Mỗi SELECT từ bảng này sẽ consume messages từ Kafka (dùng bởi MV).
-- kafka_num_consumers: số consumer threads song song trong CH node.
-- kafka_skip_broken_messages: bỏ qua tối đa N message lỗi trước khi stop.
-- =============================================================================

CREATE TABLE IF NOT EXISTS tracking.payments_kafka
(
    event_id      String,   -- raw, sẽ parse trong MV
    user_id       UInt64,
    amount        Float64,
    status        String,
    payment_date  String,   -- raw "2024-01-15", MV parse → Date
    updated_at    String    -- raw ISO8601, MV parse → DateTime64
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list           = 'kafka:9092',
    kafka_topic_list            = 'payment-events-ingest',  -- validated bởi Go ingestor
    kafka_group_name            = 'ch-payment-ingestor',
    kafka_format                = 'JSONEachRow',
    kafka_num_consumers         = 4,
    kafka_max_block_size        = 65536,
    kafka_skip_broken_messages  = 1000,
    kafka_handle_error_mode     = 'stream';

-- =============================================================================
-- MATERIALIZED VIEW: payments_bronze_mv
-- =============================================================================
-- Trigger tự động: khi CH consume batch từ Kafka Engine,
-- MV parse + transform rồi INSERT vào payments_bronze.
-- TO clause → append-only, dùng schema của payments_bronze.
-- =============================================================================

CREATE MATERIALIZED VIEW IF NOT EXISTS tracking.payments_bronze_mv
TO tracking.payments_bronze
AS
SELECT
    event_id,
    user_id,
    amount,
    status,
    toDate(payment_date)                     AS payment_date,
    parseDateTime64BestEffort(updated_at, 3) AS updated_at
FROM tracking.payments_kafka
WHERE _error = '';  -- kafka_handle_error_mode='stream' → bỏ qua rows lỗi parse



--Test
SELECT *
FROM system.kafka_consumers;