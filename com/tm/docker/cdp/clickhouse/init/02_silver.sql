-- =============================================================================
-- SILVER LAYER (CDP Realtime): cdp.silver_login (pre-create for dbt)
-- =============================================================================
-- Pre-create để tránh lỗi ClickHouse 26.x new analyzer với dbt subquery.
-- dbt sẽ INSERT INTO bảng này (không tự CREATE).
-- =============================================================================

CREATE TABLE IF NOT EXISTS cdp.silver_login
(
    event_id   String,
    event_date Date,
    user_id    UInt64,
    session_id String,
    device_id  String,
    platform   String,    -- normalized: ios/android/web/unknown
    country    String,    -- normalized: VN/US/SG/... hoặc 'unknown'
    login_at   DateTime64(3)
)
ENGINE = ReplacingMergeTree()
ORDER BY (event_date, user_id, event_id)
PARTITION BY toYYYYMM(event_date)
SETTINGS index_granularity = 8192;
