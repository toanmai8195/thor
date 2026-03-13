-- =============================================================================
-- SILVER: cdp.silver_search
-- =============================================================================
-- Source-specific fields: query, result_count, clicked_result_id
-- Common normalization: platform, country, session_id via macros
-- NOTE: device_id không có trong search events (search là intent-level signal)
-- =============================================================================

{{ config(materialized='incremental', on_schema_change='ignore') }}

SELECT
    CAST(event_id AS VARCHAR(64))        AS event_id,
    event_date,
    user_id,
    {{ normalize_id('session_id') }}     AS session_id,
    {{ normalize_platform('platform') }} AS platform,
    {{ normalize_country('country') }}   AS country,

    -- Source-specific: search fields
    CAST(TRIM(query) AS VARCHAR(500))    AS query,

    -- result_count: negative is data quality issue → set to 0
    CASE WHEN result_count < 0 THEN 0 ELSE result_count END AS result_count,

    -- has_results: derived field — key signal for zero-result tracking
    result_count > 0                     AS has_results,

    -- clicked_result_id: nullable — null means zero-click search
    CAST(
        CASE WHEN TRIM(clicked_result_id) = '' OR clicked_result_id IS NULL
             THEN NULL
             ELSE TRIM(clicked_result_id)
        END
    AS VARCHAR(64))                      AS clicked_result_id,

    COALESCE(searched_at, ingested_at)   AS searched_at

FROM {{ source('bronze', 'search_events') }}

WHERE
    event_id   IS NOT NULL AND event_id <> ''
    AND user_id IS NOT NULL AND user_id > 0
    AND event_date IS NOT NULL
    AND query IS NOT NULL AND TRIM(query) <> ''   -- source-specific: query required

{% if is_incremental() %}
    AND {{ silver_incremental_filter('event_date') }}
{% endif %}
