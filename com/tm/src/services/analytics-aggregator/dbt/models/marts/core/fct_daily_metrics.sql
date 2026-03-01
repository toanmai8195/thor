-- =============================================================================
-- FACT TABLE: fct_daily_metrics
-- =============================================================================
-- Model này aggregates tracking events thành daily metrics.
--
-- FACT TABLE LÀ GÌ?
-- - Chứa measurements/metrics (số lượng, doanh thu, etc.)
-- - Thường có foreign keys đến dimension tables
-- - Grain (độ chi tiết): 1 row = 1 ngày + 1 platform + 1 country
--
-- USE CASES:
-- - Daily KPI dashboards
-- - Trend analysis
-- - Performance monitoring
-- - Alerting (anomaly detection)
--
-- METRICS INCLUDED:
-- - Traffic: Sessions, users, page views
-- - Engagement: Product views, searches, cart adds
-- - Conversion: Purchases, revenue, orders
-- - Dropshipping: Margin, supplier performance
--
-- MATERIALIZATION:
-- - table: Full refresh mỗi ngày
-- - Trong production, có thể dùng incremental
-- =============================================================================

{{
    config(
        materialized = 'table',
        tags = ['marts', 'core', 'daily'],
        -- Partition by date cho query performance
        partition_by = {
            'field': 'date_day',
            'data_type': 'date'
        }
    )
}}

-- =============================================================================
-- DAILY AGGREGATION
-- =============================================================================

WITH events AS (
    SELECT * FROM {{ ref('stg_events') }}
),

-- =============================================================================
-- AGGREGATE BY DAY, PLATFORM, COUNTRY
-- =============================================================================
daily_metrics AS (
    SELECT
        -- =================================================================
        -- DIMENSIONS (Grain)
        -- =================================================================
        event_date AS date_day,
        platform,
        country_code,

        -- =================================================================
        -- TRAFFIC METRICS
        -- =================================================================
        -- Unique sessions trong ngày
        -- Session = một lượt truy cập của user
        COUNT(DISTINCT session_id) AS total_sessions,

        -- Unique registered users
        -- Chỉ đếm user_id > 0 (đã đăng nhập)
        COUNT(DISTINCT CASE WHEN user_id > 0 THEN user_id END) AS unique_users,

        -- Anonymous sessions (chưa đăng nhập)
        COUNT(DISTINCT CASE WHEN user_id = 0 THEN session_id END) AS anonymous_sessions,

        -- Total events
        COUNT(*) AS total_events,

        -- Page views
        -- Đếm các events liên quan đến xem trang
        SUM(CASE WHEN event_type IN ('page_view', 'home_page') THEN 1 ELSE 0 END) AS page_views,

        -- =================================================================
        -- ENGAGEMENT METRICS
        -- =================================================================
        -- Product views
        SUM(CASE WHEN event_type = 'product_view' THEN 1 ELSE 0 END) AS product_views,

        -- Unique products viewed
        COUNT(DISTINCT CASE WHEN event_type = 'product_view' THEN product_id END) AS unique_products_viewed,

        -- Product clicks
        SUM(CASE WHEN event_type = 'product_click' THEN 1 ELSE 0 END) AS product_clicks,

        -- Search events
        SUM(CASE WHEN event_type = 'search' THEN 1 ELSE 0 END) AS searches,

        -- Unique search queries
        COUNT(DISTINCT CASE WHEN event_type = 'search' THEN search_query END) AS unique_search_queries,

        -- Zero-result searches (potential content gap)
        SUM(CASE WHEN event_type = 'search' AND has_search_results = FALSE THEN 1 ELSE 0 END) AS zero_result_searches,

        -- =================================================================
        -- CART METRICS
        -- =================================================================
        -- Add to cart events
        SUM(CASE WHEN event_type = 'add_to_cart' THEN 1 ELSE 0 END) AS add_to_cart_events,

        -- Unique sessions that added to cart
        COUNT(DISTINCT CASE WHEN event_type = 'add_to_cart' THEN session_id END) AS sessions_with_add_to_cart,

        -- Remove from cart (cart abandonment signals)
        SUM(CASE WHEN event_type = 'remove_from_cart' THEN 1 ELSE 0 END) AS remove_from_cart_events,

        -- Cart views
        SUM(CASE WHEN event_type = 'view_cart' THEN 1 ELSE 0 END) AS cart_views,

        -- =================================================================
        -- CHECKOUT METRICS
        -- =================================================================
        -- Checkout starts
        SUM(CASE WHEN event_type = 'checkout_start' THEN 1 ELSE 0 END) AS checkout_starts,

        -- Checkout completions (reached payment step)
        SUM(CASE WHEN event_type = 'checkout_payment' THEN 1 ELSE 0 END) AS checkout_payments,

        -- Unique sessions that started checkout
        COUNT(DISTINCT CASE WHEN event_type = 'checkout_start' THEN session_id END) AS sessions_with_checkout,

        -- =================================================================
        -- CONVERSION METRICS
        -- =================================================================
        -- Total purchases
        SUM(CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END) AS total_orders,

        -- Unique buyers (sessions that purchased)
        COUNT(DISTINCT CASE WHEN event_type = 'purchase' THEN session_id END) AS buyers,

        -- Unique order IDs
        COUNT(DISTINCT CASE WHEN event_type = 'purchase' THEN order_id END) AS unique_orders,

        -- =================================================================
        -- REVENUE METRICS
        -- =================================================================
        -- Gross revenue (total_cents from purchase events)
        SUM(CASE WHEN event_type = 'purchase' THEN total_cents ELSE 0 END) AS gross_revenue_cents,

        -- Net revenue (after discounts)
        SUM(CASE WHEN event_type = 'purchase' THEN subtotal_cents - discount_cents ELSE 0 END) AS net_revenue_cents,

        -- Total discounts given
        SUM(CASE WHEN event_type = 'purchase' THEN discount_cents ELSE 0 END) AS total_discount_cents,

        -- Shipping revenue
        SUM(CASE WHEN event_type = 'purchase' THEN shipping_cents ELSE 0 END) AS shipping_revenue_cents,

        -- Tax collected
        SUM(CASE WHEN event_type = 'purchase' THEN tax_cents ELSE 0 END) AS tax_collected_cents,

        -- Items sold (quantity)
        SUM(CASE WHEN event_type = 'purchase' THEN quantity ELSE 0 END) AS items_sold,

        -- =================================================================
        -- DROPSHIPPING METRICS (Crucial for dropshipping business)
        -- =================================================================
        -- Cost of goods sold (COGS)
        -- Chi phí mua hàng từ suppliers
        SUM(CASE WHEN event_type = 'purchase' THEN cost_price_cents * quantity ELSE 0 END) AS cogs_cents,

        -- Gross profit = Revenue - COGS
        SUM(CASE WHEN event_type = 'purchase' THEN margin_cents * quantity ELSE 0 END) AS gross_profit_cents,

        -- Average margin rate
        AVG(CASE WHEN event_type = 'purchase' AND margin_rate > 0 THEN margin_rate END) AS avg_margin_rate,

        -- Unique suppliers sold from
        COUNT(DISTINCT CASE WHEN event_type = 'purchase' THEN supplier_id END) AS unique_suppliers,

        -- =================================================================
        -- USER ACQUISITION METRICS
        -- =================================================================
        -- Sign ups
        SUM(CASE WHEN event_type = 'sign_up' THEN 1 ELSE 0 END) AS sign_ups,

        -- Logins
        SUM(CASE WHEN event_type = 'login' THEN 1 ELSE 0 END) AS logins,

        -- =================================================================
        -- TRAFFIC SOURCE BREAKDOWN
        -- =================================================================
        -- Sessions by source
        COUNT(DISTINCT CASE WHEN traffic_source = 'organic' THEN session_id END) AS organic_sessions,
        COUNT(DISTINCT CASE WHEN traffic_source = 'paid' THEN session_id END) AS paid_sessions,
        COUNT(DISTINCT CASE WHEN traffic_source = 'social' THEN session_id END) AS social_sessions,
        COUNT(DISTINCT CASE WHEN traffic_source = 'email' THEN session_id END) AS email_sessions,
        COUNT(DISTINCT CASE WHEN traffic_source = 'direct' THEN session_id END) AS direct_sessions,
        COUNT(DISTINCT CASE WHEN traffic_source = 'referral' THEN session_id END) AS referral_sessions,
        COUNT(DISTINCT CASE WHEN traffic_source = 'affiliate' THEN session_id END) AS affiliate_sessions,

        -- =================================================================
        -- DEVICE BREAKDOWN
        -- =================================================================
        COUNT(DISTINCT CASE WHEN device_type = 'desktop' THEN session_id END) AS desktop_sessions,
        COUNT(DISTINCT CASE WHEN device_type = 'mobile' THEN session_id END) AS mobile_sessions,
        COUNT(DISTINCT CASE WHEN device_type = 'tablet' THEN session_id END) AS tablet_sessions

    FROM events
    GROUP BY
        event_date,
        platform,
        country_code
),

-- =============================================================================
-- ADD CALCULATED METRICS
-- =============================================================================
with_rates AS (
    SELECT
        *,

        -- =================================================================
        -- CONVERSION RATES
        -- =================================================================
        -- View-to-cart rate
        -- % sessions xem product mà add to cart
        CASE
            WHEN product_views > 0
            THEN CAST(add_to_cart_events AS Float64) / product_views
            ELSE 0
        END AS view_to_cart_rate,

        -- Cart-to-checkout rate
        -- % sessions có cart mà bắt đầu checkout
        CASE
            WHEN sessions_with_add_to_cart > 0
            THEN CAST(sessions_with_checkout AS Float64) / sessions_with_add_to_cart
            ELSE 0
        END AS cart_to_checkout_rate,

        -- Checkout-to-purchase rate
        -- % checkouts thành công
        CASE
            WHEN checkout_starts > 0
            THEN CAST(total_orders AS Float64) / checkout_starts
            ELSE 0
        END AS checkout_conversion_rate,

        -- Overall conversion rate
        -- % sessions mà purchase
        CASE
            WHEN total_sessions > 0
            THEN CAST(buyers AS Float64) / total_sessions
            ELSE 0
        END AS conversion_rate,

        -- =================================================================
        -- AVERAGE ORDER VALUES
        -- =================================================================
        -- AOV (Average Order Value)
        CASE
            WHEN unique_orders > 0
            THEN CAST(gross_revenue_cents AS Float64) / unique_orders
            ELSE 0
        END AS aov_cents,

        -- Average items per order
        CASE
            WHEN unique_orders > 0
            THEN CAST(items_sold AS Float64) / unique_orders
            ELSE 0
        END AS avg_items_per_order,

        -- Average discount per order
        CASE
            WHEN unique_orders > 0
            THEN CAST(total_discount_cents AS Float64) / unique_orders
            ELSE 0
        END AS avg_discount_cents,

        -- =================================================================
        -- ENGAGEMENT RATES
        -- =================================================================
        -- Pages per session
        CASE
            WHEN total_sessions > 0
            THEN CAST(page_views AS Float64) / total_sessions
            ELSE 0
        END AS pages_per_session,

        -- Products viewed per session
        CASE
            WHEN total_sessions > 0
            THEN CAST(product_views AS Float64) / total_sessions
            ELSE 0
        END AS products_per_session,

        -- Search rate (% sessions that searched)
        CASE
            WHEN total_sessions > 0
            THEN CAST(searches AS Float64) / total_sessions
            ELSE 0
        END AS search_rate,

        -- Zero-result rate (quality of search)
        CASE
            WHEN searches > 0
            THEN CAST(zero_result_searches AS Float64) / searches
            ELSE 0
        END AS zero_result_rate,

        -- =================================================================
        -- DROPSHIPPING PROFITABILITY
        -- =================================================================
        -- Gross margin percentage
        -- gross_profit / revenue
        CASE
            WHEN gross_revenue_cents > 0
            THEN CAST(gross_profit_cents AS Float64) / gross_revenue_cents
            ELSE 0
        END AS gross_margin_pct,

        -- =================================================================
        -- TRAFFIC SOURCE PERCENTAGES
        -- =================================================================
        CASE WHEN total_sessions > 0 THEN CAST(organic_sessions AS Float64) / total_sessions ELSE 0 END AS organic_pct,
        CASE WHEN total_sessions > 0 THEN CAST(paid_sessions AS Float64) / total_sessions ELSE 0 END AS paid_pct,
        CASE WHEN total_sessions > 0 THEN CAST(social_sessions AS Float64) / total_sessions ELSE 0 END AS social_pct,
        CASE WHEN total_sessions > 0 THEN CAST(email_sessions AS Float64) / total_sessions ELSE 0 END AS email_pct,
        CASE WHEN total_sessions > 0 THEN CAST(direct_sessions AS Float64) / total_sessions ELSE 0 END AS direct_pct,

        -- =================================================================
        -- DEVICE PERCENTAGES
        -- =================================================================
        CASE WHEN total_sessions > 0 THEN CAST(desktop_sessions AS Float64) / total_sessions ELSE 0 END AS desktop_pct,
        CASE WHEN total_sessions > 0 THEN CAST(mobile_sessions AS Float64) / total_sessions ELSE 0 END AS mobile_pct

    FROM daily_metrics
)

SELECT
    -- Add unique key
    {{ dbt_utils.generate_surrogate_key(['date_day', 'platform', 'country_code']) }} AS metric_id,
    *,
    -- Metadata
    NOW() AS dbt_updated_at
FROM with_rates
