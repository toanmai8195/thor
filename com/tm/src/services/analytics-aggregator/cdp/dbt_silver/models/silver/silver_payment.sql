-- =============================================================================
-- SILVER: cdp.silver_payment
-- =============================================================================
-- Source-specific fields: order_id, amount, currency, payment_method, status
-- Common normalization: platform, country, session_id via macros
-- NOTE: device_id không có trong payment events (transaction-level, not device-level)
-- =============================================================================

{{ config(materialized='incremental', on_schema_change='ignore') }}

SELECT
    CAST(event_id AS VARCHAR(64))        AS event_id,
    event_date,
    user_id,
    {{ normalize_id('session_id') }}     AS session_id,
    CAST(order_id AS VARCHAR(64))        AS order_id,
    {{ normalize_platform('platform') }} AS platform,
    {{ normalize_country('country') }}   AS country,

    -- Source-specific: financial fields
    amount,

    -- currency: uppercase, default VND
    CAST(
        CASE
            WHEN TRIM(currency) = '' OR currency IS NULL THEN 'VND'
            ELSE UPPER(TRIM(currency))
        END
    AS VARCHAR(10))                      AS currency,

    -- payment_method: normalize to known values
    CAST(
        CASE
            WHEN LOWER(TRIM(payment_method)) IN ('momo', 'vnpay', 'visa', 'mastercard')
                THEN LOWER(TRIM(payment_method))
            ELSE 'other'
        END
    AS VARCHAR(20))                      AS payment_method,

    -- status: normalize to lifecycle values
    CAST(
        CASE
            WHEN LOWER(TRIM(status)) IN ('paid', 'pending', 'failed', 'refunded')
                THEN LOWER(TRIM(status))
            ELSE 'unknown'
        END
    AS VARCHAR(20))                      AS status,

    COALESCE(paid_at, ingested_at)       AS paid_at

FROM {{ source('bronze', 'payment_events') }}

WHERE
    event_id   IS NOT NULL AND event_id <> ''
    AND user_id IS NOT NULL AND user_id > 0
    AND event_date IS NOT NULL
    AND order_id IS NOT NULL AND TRIM(order_id) <> ''   -- source-specific: transaction required
    AND amount > 0                                       -- source-specific: amount must be positive

{% if is_incremental() %}
    AND {{ silver_incremental_filter('event_date') }}
{% endif %}
