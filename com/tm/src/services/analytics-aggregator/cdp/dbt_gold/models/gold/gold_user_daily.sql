-- =============================================================================
-- GOLD LAYER: cdp.gold_user_daily
-- =============================================================================
-- User engagement metrics theo ngày từ silver_login.
-- Grain: 1 row = 1 ngày (event_date)
--
-- Key metrics:
--   dau             = COUNT(DISTINCT user_id)  → Daily Active Users
--   total_logins    = COUNT(*)                 → Tổng số login events
--   unique_sessions = COUNT(DISTINCT session_id)
--   unique_devices  = COUNT(DISTINCT device_id)
--   platform breakdown: ios/android/web/unknown
--
-- Superset query example:
--   SELECT event_date, dau, total_logins, ios_logins, android_logins
--   FROM cdp.gold_user_daily
--   ORDER BY event_date DESC;
-- =============================================================================

{{
  config(
    materialized = 'table'
  )
}}

SELECT
    event_date,

    -- =========================================================================
    -- USER ENGAGEMENT
    -- =========================================================================
    COUNT(DISTINCT user_id)    AS dau,             -- Daily Active Users
    COUNT(*)                   AS total_logins,    -- Tổng login events
    COUNT(DISTINCT session_id) AS unique_sessions, -- Unique sessions (mỗi app open = 1 session)
    COUNT(DISTINCT device_id)  AS unique_devices,  -- Unique devices

    -- =========================================================================
    -- PLATFORM BREAKDOWN
    -- =========================================================================
    COUNT(CASE WHEN platform = 'ios'     THEN 1 END) AS ios_logins,
    COUNT(CASE WHEN platform = 'android' THEN 1 END) AS android_logins,
    COUNT(CASE WHEN platform = 'web'     THEN 1 END) AS web_logins,
    COUNT(CASE WHEN platform = 'unknown' THEN 1 END) AS unknown_logins,

    -- =========================================================================
    -- TOP COUNTRIES (top 3 by login count — approximate)
    -- =========================================================================
    COUNT(CASE WHEN country = 'VN' THEN 1 END) AS vn_logins,
    COUNT(CASE WHEN country = 'US' THEN 1 END) AS us_logins,
    COUNT(CASE WHEN country = 'SG' THEN 1 END) AS sg_logins,
    COUNT(CASE WHEN country NOT IN ('VN', 'US', 'SG') THEN 1 END) AS other_logins,

    NOW() AS dbt_updated_at

FROM {{ source('silver', 'silver_login') }}

GROUP BY event_date
ORDER BY event_date DESC
