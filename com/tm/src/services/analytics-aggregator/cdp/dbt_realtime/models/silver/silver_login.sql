-- =============================================================================
-- SILVER LAYER (CDP Realtime): cdp.silver_login
-- =============================================================================
-- Chuẩn hoá dữ liệu từ cdp.login_bronze (ClickHouse).
-- Cùng logic normalize với DW silver_login (StarRocks).
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

    -- session_id / device_id: empty → 'unknown'
    if(trim(session_id) = '', 'unknown', trim(session_id)) AS session_id,
    if(trim(device_id)  = '', 'unknown', trim(device_id))  AS device_id,

    -- Normalize platform
    CASE
        WHEN lower(trim(platform)) IN ('ios', 'android', 'web')
            THEN lower(trim(platform))
        ELSE 'unknown'
    END AS platform,

    -- Normalize country: uppercase, empty → 'unknown'
    CASE
        WHEN trim(country) = '' THEN 'unknown'
        ELSE upper(trim(country))
    END AS country,

    login_at

FROM {{ source('bronze', 'login_bronze') }}

WHERE
    event_id   != ''
    AND user_id > 0
    AND event_date IS NOT NULL

{% if is_incremental() %}
    -- Incremental: 1 ngày overlap để catch late data.
    -- ClickHouse Date là UInt16: dùng if(count()>0,...) để tránh overflow khi table rỗng.
    AND event_date >= (
        SELECT if(
            count() > 0,
            max(event_date) - INTERVAL 1 DAY,
            toDate('1970-01-01')
        )
        FROM {{ this }}
    )
{% endif %}
