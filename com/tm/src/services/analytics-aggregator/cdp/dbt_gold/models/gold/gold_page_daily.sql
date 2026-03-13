-- =============================================================================
-- GOLD: cdp.gold_page_daily
-- =============================================================================
-- Page view metrics by date.
-- Grain: 1 row = 1 ngày (event_date)
-- =============================================================================

{{ config(materialized='table') }}

SELECT
    event_date,

    COUNT(*)                   AS total_views,
    COUNT(DISTINCT user_id)    AS unique_viewers,
    COUNT(DISTINCT session_id) AS unique_sessions,
    COUNT(DISTINCT page_url)   AS unique_pages,

    -- Direct vs referred traffic
    COUNT(CASE WHEN referrer IS NULL THEN 1 END) AS direct_views,
    COUNT(CASE WHEN referrer IS NOT NULL THEN 1 END) AS referred_views,

    -- Platform breakdown
    COUNT(CASE WHEN platform = 'ios'     THEN 1 END) AS ios_views,
    COUNT(CASE WHEN platform = 'android' THEN 1 END) AS android_views,
    COUNT(CASE WHEN platform = 'web'     THEN 1 END) AS web_views,

    NOW() AS dbt_updated_at

FROM {{ source('silver', 'silver_view') }}
GROUP BY event_date
ORDER BY event_date DESC
