-- =============================================================================
-- STAGING MODEL: stg_events
-- =============================================================================
-- Model này clean và standardize raw events từ ClickHouse.
--
-- STAGING LAYER PURPOSE:
-- 1. Rename columns cho clarity
-- 2. Cast data types
-- 3. Handle nulls và defaults
-- 4. Basic transformations (không có business logic)
-- 5. Deduplication nếu cần
--
-- KHÔNG LÀM Ở STAGING:
-- - Business logic phức tạp
-- - Aggregations
-- - Joins với other sources
--
-- OUTPUT:
-- - 1:1 mapping với source table
-- - Clean, consistent data types
-- - Ready cho intermediate/marts layers
--
-- USAGE:
-- Models khác reference model này:
-- SELECT * FROM {{ ref('stg_events') }}
-- =============================================================================

-- Config cho model này
-- materialized: view = không tạo physical table
-- Mỗi lần query sẽ đọc trực tiếp từ source
{{
    config(
        materialized = 'view',
        tags = ['staging', 'hourly']
    )
}}

-- =============================================================================
-- MAIN QUERY
-- =============================================================================

WITH source AS (
    -- Đọc từ source table (ClickHouse events)
    SELECT * FROM {{ source('clickhouse', 'events') }}
),

-- =============================================================================
-- CLEANING & TRANSFORMATION
-- =============================================================================
cleaned AS (
    SELECT
        -- =================================================================
        -- EVENT IDENTIFICATION
        -- =================================================================
        -- event_id: Giữ nguyên, đã là UUID
        event_id,

        -- event_type: Lowercase để consistent
        LOWER(event_type) AS event_type,

        -- event_time: Timestamp của event
        -- Đã là DateTime64 trong ClickHouse, giữ nguyên
        event_time,

        -- processed_time: Timestamp khi được ingest
        processed_time,

        -- Tính ingestion lag (thời gian từ event đến khi được process)
        -- Hữu ích để monitor pipeline health
        dateDiff('second', event_time, processed_time) AS ingestion_lag_seconds,

        -- =================================================================
        -- USER IDENTIFICATION
        -- =================================================================
        -- user_id: 0 = anonymous user
        user_id,

        -- is_registered: Boolean flag cho dễ query
        CASE
            WHEN user_id > 0 THEN TRUE
            ELSE FALSE
        END AS is_registered_user,

        -- session_id: Giữ nguyên
        session_id,

        -- device_id: Handle empty strings
        NULLIF(device_id, '') AS device_id,

        -- =================================================================
        -- DEVICE & PLATFORM
        -- =================================================================
        -- Platform: Lowercase, default 'web'
        COALESCE(NULLIF(LOWER(platform), ''), 'web') AS platform,

        -- Device type: Lowercase, default 'unknown'
        COALESCE(NULLIF(LOWER(device_type), ''), 'unknown') AS device_type,

        -- Browser info
        NULLIF(browser, '') AS browser,
        NULLIF(browser_version, '') AS browser_version,

        -- OS info
        NULLIF(os, '') AS os,
        NULLIF(os_version, '') AS os_version,

        -- Screen resolution: Parse width và height
        screen_resolution,
        CAST(
            splitByChar('x', screen_resolution)[1] AS Nullable(UInt16)
        ) AS screen_width,
        CAST(
            splitByChar('x', screen_resolution)[2] AS Nullable(UInt16)
        ) AS screen_height,

        -- =================================================================
        -- LOCATION
        -- =================================================================
        NULLIF(ip_address, '') AS ip_address,

        -- Country code: Uppercase, 2 chars
        UPPER(NULLIF(country_code, '')) AS country_code,

        -- City: Title case would be ideal, but keep as-is for now
        NULLIF(city, '') AS city,

        -- =================================================================
        -- TRAFFIC SOURCE
        -- =================================================================
        COALESCE(NULLIF(LOWER(traffic_source), ''), 'direct') AS traffic_source,

        -- UTM parameters: Lowercase
        NULLIF(LOWER(utm_source), '') AS utm_source,
        NULLIF(LOWER(utm_medium), '') AS utm_medium,
        NULLIF(LOWER(utm_campaign), '') AS utm_campaign,
        NULLIF(LOWER(utm_content), '') AS utm_content,
        NULLIF(LOWER(utm_term), '') AS utm_term,

        -- Referrer
        NULLIF(referrer_url, '') AS referrer_url,

        -- Extract referrer domain
        CASE
            WHEN referrer_url != ''
            THEN domain(referrer_url)
            ELSE NULL
        END AS referrer_domain,

        -- =================================================================
        -- PAGE INFO
        -- =================================================================
        NULLIF(page_url, '') AS page_url,
        NULLIF(page_path, '') AS page_path,
        NULLIF(page_title, '') AS page_title,

        -- Extract page category from path
        -- /product/123 -> 'product'
        -- /search -> 'search'
        -- / -> 'home'
        CASE
            WHEN page_path = '/' THEN 'home'
            WHEN page_path LIKE '/product%' THEN 'product'
            WHEN page_path LIKE '/category%' THEN 'category'
            WHEN page_path LIKE '/search%' THEN 'search'
            WHEN page_path LIKE '/cart%' THEN 'cart'
            WHEN page_path LIKE '/checkout%' THEN 'checkout'
            WHEN page_path LIKE '/account%' THEN 'account'
            ELSE 'other'
        END AS page_category,

        -- =================================================================
        -- PRODUCT INFO
        -- =================================================================
        -- product_id: 0 = no product
        product_id,

        -- has_product: Boolean flag
        product_id > 0 AS has_product,

        NULLIF(product_sku, '') AS product_sku,
        NULLIF(product_name, '') AS product_name,

        category_id,
        NULLIF(category_name, '') AS category_name,
        NULLIF(brand, '') AS brand,

        -- Price in cents
        price_cents,

        -- Price in dollars (cho dễ đọc)
        -- Dùng Decimal để tránh floating point errors
        CAST(price_cents AS Decimal(18, 2)) / 100 AS price_dollars,

        COALESCE(NULLIF(currency, ''), 'USD') AS currency,
        COALESCE(quantity, 1) AS quantity,

        -- =================================================================
        -- SEARCH INFO
        -- =================================================================
        NULLIF(search_query, '') AS search_query,
        search_results_count,
        NULLIF(search_filters, '{}') AS search_filters,

        -- Has search results flag
        search_results_count > 0 AS has_search_results,

        -- =================================================================
        -- ORDER INFO
        -- =================================================================
        NULLIF(order_id, '') AS order_id,
        NULLIF(cart_id, '') AS cart_id,

        -- Monetary values in cents
        subtotal_cents,
        discount_cents,
        shipping_cents,
        tax_cents,
        total_cents,

        -- Monetary values in dollars
        CAST(subtotal_cents AS Decimal(18, 2)) / 100 AS subtotal_dollars,
        CAST(discount_cents AS Decimal(18, 2)) / 100 AS discount_dollars,
        CAST(shipping_cents AS Decimal(18, 2)) / 100 AS shipping_dollars,
        CAST(tax_cents AS Decimal(18, 2)) / 100 AS tax_dollars,
        CAST(total_cents AS Decimal(18, 2)) / 100 AS total_dollars,

        -- Discount percentage
        CASE
            WHEN subtotal_cents > 0
            THEN CAST(discount_cents AS Decimal(18, 4)) / subtotal_cents
            ELSE 0
        END AS discount_rate,

        NULLIF(payment_method, '') AS payment_method,

        -- =================================================================
        -- SUPPLIER INFO (Dropshipping Specific)
        -- =================================================================
        supplier_id,
        supplier_id > 0 AS has_supplier,

        NULLIF(supplier_name, '') AS supplier_name,
        NULLIF(supplier_country, '') AS supplier_country,

        -- Cost and margin calculations
        cost_price_cents,
        CAST(cost_price_cents AS Decimal(18, 2)) / 100 AS cost_price_dollars,

        -- Gross margin per unit
        -- margin = selling price - cost price
        price_cents - cost_price_cents AS margin_cents,
        CAST(price_cents - cost_price_cents AS Decimal(18, 2)) / 100 AS margin_dollars,

        -- Margin percentage
        -- margin_rate = (price - cost) / price
        CASE
            WHEN price_cents > 0
            THEN CAST(price_cents - cost_price_cents AS Decimal(18, 4)) / price_cents
            ELSE 0
        END AS margin_rate,

        estimated_shipping_days,

        -- Shipping tier based on days
        CASE
            WHEN estimated_shipping_days <= 7 THEN 'express'
            WHEN estimated_shipping_days <= 14 THEN 'standard'
            WHEN estimated_shipping_days <= 21 THEN 'economy'
            ELSE 'slow'
        END AS shipping_tier,

        -- =================================================================
        -- EXTRA PROPERTIES
        -- =================================================================
        NULLIF(extra_properties, '{}') AS extra_properties,

        -- =================================================================
        -- DERIVED DATE/TIME DIMENSIONS
        -- =================================================================
        -- Useful cho time-based analytics

        -- Date parts
        toDate(event_time) AS event_date,
        toYear(event_time) AS event_year,
        toMonth(event_time) AS event_month,
        toDayOfMonth(event_time) AS event_day,
        toDayOfWeek(event_time) AS event_day_of_week,  -- 1=Monday, 7=Sunday
        toHour(event_time) AS event_hour,

        -- Week number
        toISOWeek(event_time) AS event_week,

        -- Quarter
        toQuarter(event_time) AS event_quarter,

        -- Is weekend?
        toDayOfWeek(event_time) >= 6 AS is_weekend,

        -- Time of day bucket
        CASE
            WHEN toHour(event_time) BETWEEN 6 AND 11 THEN 'morning'
            WHEN toHour(event_time) BETWEEN 12 AND 17 THEN 'afternoon'
            WHEN toHour(event_time) BETWEEN 18 AND 21 THEN 'evening'
            ELSE 'night'
        END AS time_of_day

    FROM source
)

SELECT * FROM cleaned
