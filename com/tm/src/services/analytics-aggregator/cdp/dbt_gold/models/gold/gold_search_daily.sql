-- =============================================================================
-- GOLD: cdp.gold_search_daily
-- =============================================================================
-- Search behavior metrics by date.
-- Grain: 1 row = 1 ngày (event_date)
-- Key metric: zero_result_rate — indicator of content gaps
-- =============================================================================

{{ config(materialized='table') }}

SELECT
    event_date,

    COUNT(*)                             AS total_searches,
    COUNT(DISTINCT user_id)              AS unique_searchers,
    COUNT(DISTINCT session_id)           AS unique_sessions,

    -- Zero-result tracking — key signal for content/catalog gaps
    COUNT(CASE WHEN NOT has_results THEN 1 END)  AS zero_result_searches,
    COUNT(CASE WHEN has_results     THEN 1 END)  AS has_result_searches,
    ROUND(
        COUNT(CASE WHEN NOT has_results THEN 1 END) * 100.0 / NULLIF(COUNT(*), 0),
        2
    )                                    AS zero_result_rate_pct,

    -- Click-through: did user click a result?
    COUNT(CASE WHEN clicked_result_id IS NOT NULL THEN 1 END) AS clicked_searches,
    ROUND(
        COUNT(CASE WHEN clicked_result_id IS NOT NULL THEN 1 END) * 100.0 / NULLIF(COUNT(*), 0),
        2
    )                                    AS click_through_rate_pct,

    -- Average results returned
    ROUND(AVG(result_count), 1)          AS avg_result_count,

    NOW() AS dbt_updated_at

FROM {{ source('silver', 'silver_search') }}
GROUP BY event_date
ORDER BY event_date DESC
