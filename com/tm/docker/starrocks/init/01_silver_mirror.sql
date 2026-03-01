-- =============================================================================
-- STARROCKS SILVER MIRROR: silver_payments
-- =============================================================================
-- Bản sao của ClickHouse silver_payments sau khi đã normalize.
-- Sync từ ClickHouse sang StarRocks qua Airflow (ch_to_starrocks.py).
-- dbt_starrocks đọc table này để tạo Gold layer.
-- =============================================================================

CREATE DATABASE IF NOT EXISTS analytics;

USE analytics;

-- PRIMARY KEY model → hỗ trợ UPSERT (sync nhiều lần không bị duplicate)
CREATE TABLE IF NOT EXISTS silver_payments (
    event_id      VARCHAR(64)     NOT NULL COMMENT 'UUID của event',
    user_id       BIGINT          NOT NULL COMMENT 'ID user',
    amount        DECIMAL(12, 2)  NOT NULL COMMENT 'Số tiền đã validate (>= 0)',
    status        VARCHAR(20)     NOT NULL COMMENT 'paid | pending | failed | refunded | cancelled | unknown',
    payment_date  DATE            NOT NULL COMMENT 'Ngày thanh toán',
    updated_at    DATETIME        NOT NULL COMMENT 'Thời gian cập nhật'
)
PRIMARY KEY (event_id, payment_date)
PARTITION BY RANGE(payment_date) (
    START ("2024-01-01") END ("2026-01-01") EVERY (INTERVAL 1 MONTH)
)
DISTRIBUTED BY HASH(user_id) BUCKETS 8
PROPERTIES (
    "replication_num" = "1",
    "dynamic_partition.enable" = "true",
    "dynamic_partition.time_unit" = "MONTH",
    "dynamic_partition.start" = "-12",
    "dynamic_partition.end" = "3",
    "dynamic_partition.prefix" = "p"
);
