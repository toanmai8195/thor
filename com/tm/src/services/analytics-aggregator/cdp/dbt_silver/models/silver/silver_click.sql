-- =============================================================================
-- SILVER: cdp.silver_click
-- =============================================================================
-- Source-specific fields: page_url, element_id (required), element_type (normalized)
-- Common normalization: platform, country, session_id, device_id via macros
-- =============================================================================

{{ config(materialized='incremental', on_schema_change='ignore') }}

SELECT
    CAST(event_id AS VARCHAR(64))        AS event_id,
    event_date,
    user_id,
    {{ normalize_id('session_id') }}     AS session_id,
    {{ normalize_id('device_id') }}      AS device_id,
    {{ normalize_platform('platform') }} AS platform,
    {{ normalize_country('country') }}   AS country,

    -- Source-specific: click context
    CAST(TRIM(page_url) AS VARCHAR(500)) AS page_url,
    {{ normalize_id('element_id') }}     AS element_id,   -- DOM/component ID, required

    -- Source-specific: element_type normalization
    CAST(
        CASE
            WHEN LOWER(TRIM(element_type)) IN ('button', 'link', 'image')
                THEN LOWER(TRIM(element_type))
            ELSE 'other'
        END
    AS VARCHAR(20))                      AS element_type,

    COALESCE(clicked_at, ingested_at)    AS clicked_at

FROM {{ source('bronze', 'click_events') }}

WHERE
    event_id   IS NOT NULL AND event_id <> ''
    AND user_id IS NOT NULL AND user_id > 0
    AND event_date IS NOT NULL
    AND element_id IS NOT NULL AND TRIM(element_id) <> ''   -- source-specific

{% if is_incremental() %}
    AND {{ silver_incremental_filter('event_date') }}
{% endif %}
