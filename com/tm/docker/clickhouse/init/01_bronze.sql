-- =============================================================================
-- BRONZE LAYER: payments_bronze
-- =============================================================================
-- Raw payment events từ Kafka, KHÔNG transform, KHÔNG validate.
-- Nguyên tắc bronze: lưu nguyên trạng dữ liệu từ nguồn.
--
-- Flow: Kafka → Go ingestor → payments_bronze
-- =============================================================================

CREATE DATABASE IF NOT EXISTS tracking;

CREATE TABLE IF NOT EXISTS tracking.payments_bronze
(
    event_id      String,           -- UUID của payment event
    user_id       UInt64,           -- ID user
    amount        Float64,          -- Số tiền (raw, chưa validate: có thể null/âm)
    status        String,           -- Trạng thái thô: "paid" / "PAID" / "Paid" / "" / invalid
    payment_date  Date,             -- Ngày thanh toán
    updated_at    DateTime64(3),    -- Thời điểm record được cập nhật (ms precision)

    -- Ingestion metadata (tự động set bởi ClickHouse)
    ingested_at   DateTime64(3) DEFAULT now64(3)
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(payment_date)
ORDER BY (payment_date, event_id)
TTL payment_date + INTERVAL 90 DAY
SETTINGS index_granularity = 8192;
