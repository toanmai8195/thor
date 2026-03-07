-- =============================================================================
-- SILVER LAYER: silver_events (pre-create for dbt)
-- =============================================================================
-- dbt-clickhouse 1.10.0 + ClickHouse 26.x new analyzer không tương thích:
-- dbt gọi SELECT * FROM (compiled_sql) LIMIT 0 để detect schema,
-- nhưng ClickHouse 26.x new analyzer không resolve database.table
-- trong subquery context → UNKNOWN_TABLE.
--
-- Fix: pre-create table thủ công → dbt bỏ qua bước CREATE, chỉ INSERT.
-- on_schema_change: ignore trong dbt_project.yml để bỏ qua schema diff query.
-- =============================================================================

CREATE TABLE IF NOT EXISTS tracking.silver_events
(
    event_id     String,
    user_id      UInt64,
    amount       Float64,
    status       String,        -- normalized: paid/pending/failed/refunded/cancelled/unknown
    payment_date Date,
    updated_at   DateTime64(3)
)
ENGINE = ReplacingMergeTree()
ORDER BY (payment_date, user_id, event_id)
PARTITION BY toYYYYMM(payment_date)
SETTINGS index_granularity = 8192;
