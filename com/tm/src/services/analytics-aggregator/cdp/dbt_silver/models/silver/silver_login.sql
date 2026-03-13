-- =============================================================================
-- SILVER LAYER (CDP): cdp.silver_login
-- =============================================================================
-- Chuẩn hoá dữ liệu từ iceberg_catalog.bronze.login_events.
-- Dùng macros từ macros/cdp_normalize.sql — cùng logic, ít boilerplate.
--
-- Target: cdp.silver_login (StarRocks PRIMARY KEY table)
-- Source: iceberg_catalog.bronze.login_events (Iceberg external catalog)
-- =============================================================================

{{
  config(
    materialized = 'incremental',
    on_schema_change = 'ignore'
  )
}}

SELECT
    CAST(event_id AS VARCHAR(64))          AS event_id,
    event_date,
    user_id,
    {{ normalize_id('session_id') }}       AS session_id,
    {{ normalize_id('device_id') }}        AS device_id,
    {{ normalize_platform('platform') }}   AS platform,
    {{ normalize_country('country') }}     AS country,
    COALESCE(login_at, ingested_at)        AS login_at

FROM {{ source('bronze', 'login_events') }}

WHERE
    event_id  IS NOT NULL AND event_id <> ''
    AND user_id IS NOT NULL AND user_id > 0
    AND event_date IS NOT NULL

{% if is_incremental() %}
    AND {{ silver_incremental_filter('event_date') }}
{% endif %}
