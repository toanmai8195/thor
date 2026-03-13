-- =============================================================================
-- SILVER: cdp.silver_view
-- =============================================================================
-- Source-specific fields: page_url (required), referrer (nullable)
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

    -- Source-specific: URL fields
    CAST(TRIM(page_url) AS VARCHAR(500)) AS page_url,
    CAST(
        CASE WHEN TRIM(referrer) = '' OR referrer IS NULL THEN NULL
             ELSE TRIM(referrer)
        END
    AS VARCHAR(500))                     AS referrer,

    COALESCE(viewed_at, ingested_at)     AS viewed_at

FROM {{ source('bronze', 'view_events') }}

WHERE
    event_id  IS NOT NULL AND event_id <> ''
    AND user_id IS NOT NULL AND user_id > 0
    AND event_date IS NOT NULL
    AND page_url IS NOT NULL AND TRIM(page_url) <> ''   -- source-specific: URL required

{% if is_incremental() %}
    AND {{ silver_incremental_filter('event_date') }}
{% endif %}
