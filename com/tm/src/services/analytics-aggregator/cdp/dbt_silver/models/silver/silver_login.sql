-- =============================================================================
-- SILVER LAYER (CDP): cdp.silver_login
-- =============================================================================
-- Chuẩn hoá dữ liệu từ iceberg_catalog.bronze.login_events:
--
-- 1. platform: lowercase + trim + map invalid → 'unknown'
--    Valid: ios | android | web | unknown
--
-- 2. country: UPPER + trim + '' → 'unknown'
--    Valid: VN/US/SG/TH/ID/... hoặc 'unknown'
--
-- 3. session_id / device_id: '' hoặc null → 'unknown' (không drop, masih bisa track)
--
-- 4. event_id / user_id: drop nếu null (không thể UPSERT an toàn)
--
-- Target: cdp.silver_login (StarRocks PRIMARY KEY table)
--   → INSERT INTO PRIMARY KEY = UPSERT by (event_id, event_date)
-- Source: iceberg_catalog.bronze.login_events (Iceberg external catalog)
-- =============================================================================

{{
  config(
    materialized = 'incremental',
    on_schema_change = 'ignore'
  )
}}

SELECT
    CAST(event_id AS VARCHAR(64))  AS event_id,
    event_date,
    user_id,

    -- Session/Device: fallback 'unknown' thay vì drop
    -- (vẫn cần để track behavioral flow kể cả khi field rỗng)
    CAST(COALESCE(NULLIF(TRIM(session_id), ''), 'unknown') AS VARCHAR(64)) AS session_id,
    CAST(COALESCE(NULLIF(TRIM(device_id),  ''), 'unknown') AS VARCHAR(64)) AS device_id,

    -- Normalize platform
    CAST(
        CASE
            WHEN LOWER(TRIM(platform)) IN ('ios', 'android', 'web')
                THEN LOWER(TRIM(platform))
            ELSE 'unknown'
        END
    AS VARCHAR(20)) AS platform,

    -- Normalize country: uppercase, empty → 'unknown'
    CAST(
        CASE
            WHEN TRIM(country) = '' OR country IS NULL THEN 'unknown'
            ELSE UPPER(TRIM(country))
        END
    AS VARCHAR(10)) AS country,

    -- login_at: fallback sang ingested_at nếu null
    COALESCE(login_at, ingested_at) AS login_at

FROM {{ source('bronze', 'login_events') }}

WHERE
    event_id   IS NOT NULL AND event_id <> ''
    AND user_id IS NOT NULL AND user_id > 0
    AND event_date IS NOT NULL

{% if is_incremental() %}
    AND event_date >= (
        SELECT COALESCE(
            DATE_SUB(MAX(event_date), INTERVAL 1 DAY),
            '1970-01-01'
        )
        FROM {{ this }}
    )
{% endif %}
