-- =============================================================================
-- SILVER LAYER (CDP Realtime): cdp.silver_login
-- =============================================================================
-- Chuẩn hoá dữ liệu từ cdp.login_bronze (ClickHouse).
-- Dùng macros từ macros/cdp_normalize.sql — cùng logic với DW silver.
--
-- Target: cdp.silver_login (ReplacingMergeTree — pre-created ở 02_silver.sql)
-- Source: cdp.login_bronze
-- Schedule: mỗi 1 phút qua Airflow DAG cdp_realtime_pipeline
-- =============================================================================

{{
  config(
    materialized = 'incremental',
    unique_key = 'event_id',
    incremental_strategy = 'append'
  )
}}

SELECT
    event_id,
    event_date,
    user_id,
    {{ normalize_id('session_id') }}     AS session_id,
    {{ normalize_id('device_id') }}      AS device_id,
    {{ normalize_platform('platform') }} AS platform,
    {{ normalize_country('country') }}   AS country,
    login_at

FROM {{ source('bronze', 'login_bronze') }}

WHERE
    event_id   != ''
    AND user_id > 0
    AND event_date IS NOT NULL

{% if is_incremental() %}
    AND {{ silver_incremental_filter('event_date') }}
{% endif %}
