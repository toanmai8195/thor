-- =============================================================================
-- FACT TABLE: fct_product_performance
-- =============================================================================
-- Model này tính toán product performance metrics.
--
-- USE CASES:
-- - Identify best-selling products
-- - Find products với low conversion
-- - Analyze margin by product
-- - Supplier performance analysis
-- - Inventory decisions (dropshipping: chọn product nào để promote)
--
-- GRAIN: 1 row = 1 product + 1 date
--
-- KEY METRICS:
-- - Views, clicks, add-to-cart, purchases (funnel)
-- - Revenue, margin, profitability
-- - Conversion rates
-- - Supplier reliability
-- =============================================================================

{{
    config(
        materialized = 'table',
        tags = ['marts', 'product', 'daily']
    )
}}

WITH events AS (
    SELECT * FROM {{ ref('stg_events') }}
    WHERE has_product = TRUE  -- Chỉ lấy events có product
),

-- =============================================================================
-- AGGREGATE BY PRODUCT + DATE
-- =============================================================================
product_daily AS (
    SELECT
        -- Dimensions
        event_date AS date_day,
        product_id,

        -- Product info (lấy giá trị đầu tiên vì không đổi)
        any(product_sku) AS product_sku,
        any(product_name) AS product_name,
        any(category_id) AS category_id,
        any(category_name) AS category_name,
        any(brand) AS brand,

        -- Supplier info
        any(supplier_id) AS supplier_id,
        any(supplier_name) AS supplier_name,
        any(supplier_country) AS supplier_country,

        -- Current price (lấy giá gần nhất)
        anyLast(price_cents) AS current_price_cents,
        anyLast(cost_price_cents) AS current_cost_cents,
        anyLast(margin_cents) AS current_margin_cents,
        anyLast(margin_rate) AS current_margin_rate,
        anyLast(estimated_shipping_days) AS shipping_days,
        anyLast(shipping_tier) AS shipping_tier,

        -- =================================================================
        -- FUNNEL METRICS
        -- =================================================================
        -- Product Views
        -- Số lần product được xem (product detail page)
        SUM(CASE WHEN event_type = 'product_view' THEN 1 ELSE 0 END) AS views,

        -- Unique sessions viewed
        COUNT(DISTINCT CASE WHEN event_type = 'product_view' THEN session_id END) AS unique_viewers,

        -- Product Clicks
        -- Click từ listing/search results
        SUM(CASE WHEN event_type = 'product_click' THEN 1 ELSE 0 END) AS clicks,

        -- Add to Cart
        SUM(CASE WHEN event_type = 'add_to_cart' THEN 1 ELSE 0 END) AS add_to_cart_count,
        SUM(CASE WHEN event_type = 'add_to_cart' THEN quantity ELSE 0 END) AS add_to_cart_quantity,
        COUNT(DISTINCT CASE WHEN event_type = 'add_to_cart' THEN session_id END) AS unique_cart_adds,

        -- Remove from Cart (negative signal)
        SUM(CASE WHEN event_type = 'remove_from_cart' THEN 1 ELSE 0 END) AS remove_from_cart_count,

        -- Purchases
        SUM(CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END) AS purchase_count,
        SUM(CASE WHEN event_type = 'purchase' THEN quantity ELSE 0 END) AS units_sold,
        COUNT(DISTINCT CASE WHEN event_type = 'purchase' THEN session_id END) AS unique_buyers,
        COUNT(DISTINCT CASE WHEN event_type = 'purchase' THEN order_id END) AS order_count,

        -- =================================================================
        -- REVENUE METRICS
        -- =================================================================
        -- Gross revenue from this product
        SUM(CASE WHEN event_type = 'purchase' THEN price_cents * quantity ELSE 0 END) AS revenue_cents,

        -- Cost (COGS)
        SUM(CASE WHEN event_type = 'purchase' THEN cost_price_cents * quantity ELSE 0 END) AS cogs_cents,

        -- Gross profit
        SUM(CASE WHEN event_type = 'purchase' THEN margin_cents * quantity ELSE 0 END) AS gross_profit_cents,

        -- Average selling price (for this period)
        AVG(CASE WHEN event_type = 'purchase' THEN price_cents END) AS avg_selling_price_cents,

        -- =================================================================
        -- WISHLIST METRICS
        -- =================================================================
        SUM(CASE WHEN event_type = 'add_to_wishlist' THEN 1 ELSE 0 END) AS wishlist_adds,
        SUM(CASE WHEN event_type = 'remove_from_wishlist' THEN 1 ELSE 0 END) AS wishlist_removes,

        -- =================================================================
        -- IMAGE/REVIEW ENGAGEMENT
        -- =================================================================
        SUM(CASE WHEN event_type = 'product_image_zoom' THEN 1 ELSE 0 END) AS image_zooms,
        SUM(CASE WHEN event_type = 'product_review' THEN 1 ELSE 0 END) AS review_reads

    FROM events
    GROUP BY
        event_date,
        product_id
),

-- =============================================================================
-- ADD CONVERSION RATES AND DERIVED METRICS
-- =============================================================================
with_rates AS (
    SELECT
        *,

        -- =================================================================
        -- CONVERSION FUNNEL RATES
        -- =================================================================
        -- Click-to-View Rate (CTR from listings)
        CASE
            WHEN clicks > 0 THEN CAST(views AS Float64) / clicks
            ELSE 0
        END AS click_to_view_rate,

        -- View-to-Cart Rate
        -- % views mà dẫn đến add to cart
        CASE
            WHEN views > 0 THEN CAST(add_to_cart_count AS Float64) / views
            ELSE 0
        END AS view_to_cart_rate,

        -- Cart-to-Purchase Rate
        -- % cart adds mà dẫn đến purchase
        CASE
            WHEN add_to_cart_count > 0 THEN CAST(purchase_count AS Float64) / add_to_cart_count
            ELSE 0
        END AS cart_to_purchase_rate,

        -- View-to-Purchase Rate (Overall conversion)
        CASE
            WHEN views > 0 THEN CAST(purchase_count AS Float64) / views
            ELSE 0
        END AS conversion_rate,

        -- Cart Abandonment Rate
        -- % products added to cart nhưng không mua
        CASE
            WHEN add_to_cart_count > 0
            THEN CAST(add_to_cart_count - purchase_count AS Float64) / add_to_cart_count
            ELSE 0
        END AS cart_abandonment_rate,

        -- =================================================================
        -- PROFITABILITY METRICS
        -- =================================================================
        -- Gross Margin Percentage
        CASE
            WHEN revenue_cents > 0 THEN CAST(gross_profit_cents AS Float64) / revenue_cents
            ELSE 0
        END AS gross_margin_pct,

        -- Revenue per view (efficiency metric)
        CASE
            WHEN views > 0 THEN CAST(revenue_cents AS Float64) / views
            ELSE 0
        END AS revenue_per_view_cents,

        -- Profit per view
        CASE
            WHEN views > 0 THEN CAST(gross_profit_cents AS Float64) / views
            ELSE 0
        END AS profit_per_view_cents,

        -- =================================================================
        -- ENGAGEMENT METRICS
        -- =================================================================
        -- Wishlist to Purchase Rate
        -- % wishlist adds mà convert
        CASE
            WHEN wishlist_adds > 0 THEN CAST(purchase_count AS Float64) / wishlist_adds
            ELSE 0
        END AS wishlist_conversion_rate,

        -- Image engagement rate
        CASE
            WHEN views > 0 THEN CAST(image_zooms AS Float64) / views
            ELSE 0
        END AS image_engagement_rate,

        -- Review engagement rate
        CASE
            WHEN views > 0 THEN CAST(review_reads AS Float64) / views
            ELSE 0
        END AS review_engagement_rate,

        -- Net cart adds (adds - removes)
        add_to_cart_count - remove_from_cart_count AS net_cart_adds

    FROM product_daily
),

-- =============================================================================
-- PRODUCT SCORING
-- =============================================================================
-- Tính score để rank products
with_scores AS (
    SELECT
        *,

        -- =================================================================
        -- PRODUCT HEALTH SCORE (0-100)
        -- =================================================================
        -- Combines multiple factors:
        -- - Conversion rate (40%)
        -- - Margin (30%)
        -- - Engagement (20%)
        -- - Demand (10%)
        (
            -- Conversion score (normalized, assume 5% conversion is excellent)
            LEAST(conversion_rate / 0.05, 1.0) * 40 +

            -- Margin score (assume 40% margin is excellent)
            LEAST(current_margin_rate / 0.40, 1.0) * 30 +

            -- Engagement score (image + review engagement)
            LEAST((image_engagement_rate + review_engagement_rate) / 0.50, 1.0) * 20 +

            -- Demand score (based on views, assume 100 views/day is good)
            LEAST(views / 100.0, 1.0) * 10
        ) AS product_health_score,

        -- =================================================================
        -- PROFITABILITY TIER
        -- =================================================================
        CASE
            WHEN gross_margin_pct >= 0.40 THEN 'high_margin'
            WHEN gross_margin_pct >= 0.25 THEN 'medium_margin'
            WHEN gross_margin_pct >= 0.15 THEN 'low_margin'
            ELSE 'very_low_margin'
        END AS margin_tier,

        -- =================================================================
        -- CONVERSION TIER
        -- =================================================================
        CASE
            WHEN conversion_rate >= 0.05 THEN 'high_converting'
            WHEN conversion_rate >= 0.02 THEN 'medium_converting'
            WHEN conversion_rate >= 0.01 THEN 'low_converting'
            ELSE 'very_low_converting'
        END AS conversion_tier,

        -- =================================================================
        -- DEMAND TIER (based on views)
        -- =================================================================
        CASE
            WHEN views >= 500 THEN 'high_demand'
            WHEN views >= 100 THEN 'medium_demand'
            WHEN views >= 20 THEN 'low_demand'
            ELSE 'very_low_demand'
        END AS demand_tier

    FROM with_rates
)

SELECT
    -- Unique key
    {{ dbt_utils.generate_surrogate_key(['date_day', 'product_id']) }} AS performance_id,
    *,
    NOW() AS dbt_updated_at
FROM with_scores
