-- =============================================================================
-- FACT TABLE: fct_marketing_attribution
-- =============================================================================
-- Model này phân tích hiệu quả marketing channels.
--
-- MARKETING ATTRIBUTION LÀ GÌ?
-- Xác định marketing channel nào đóng góp vào conversions.
--
-- ATTRIBUTION MODELS:
-- 1. Last-touch: 100% credit cho channel cuối cùng
-- 2. First-touch: 100% credit cho channel đầu tiên
-- 3. Linear: Chia đều credit
-- 4. Time-decay: Channel gần conversion nhận nhiều credit hơn
--
-- Model này implement LAST-TOUCH attribution (đơn giản nhất).
--
-- USE CASES:
-- - Đánh giá ROI của marketing campaigns
-- - Budget allocation decisions
-- - Channel optimization
-- - UTM tracking effectiveness
--
-- GRAIN: 1 row = 1 source + 1 medium + 1 campaign + 1 date
-- =============================================================================

{{
    config(
        materialized = 'table',
        tags = ['marts', 'marketing', 'daily']
    )
}}

WITH events AS (
    SELECT * FROM {{ ref('stg_events') }}
),

-- =============================================================================
-- SESSION-LEVEL ATTRIBUTION
-- =============================================================================
-- Lấy thông tin attribution cho mỗi session
-- Last-touch: Dùng thông tin từ event đầu tiên của session
-- (vì UTM params thường chỉ có ở landing page)
-- =============================================================================
session_attribution AS (
    SELECT
        session_id,
        event_date,

        -- Traffic source (first value in session)
        argMin(traffic_source, event_time) AS traffic_source,

        -- UTM parameters
        argMin(utm_source, event_time) AS utm_source,
        argMin(utm_medium, event_time) AS utm_medium,
        argMin(utm_campaign, event_time) AS utm_campaign,
        argMin(utm_content, event_time) AS utm_content,
        argMin(utm_term, event_time) AS utm_term,

        -- Referrer
        argMin(referrer_domain, event_time) AS referrer_domain,

        -- User info
        argMin(user_id, event_time) AS user_id,
        argMin(is_registered_user, event_time) AS is_registered_user,
        argMin(platform, event_time) AS platform,
        argMin(device_type, event_time) AS device_type,
        argMin(country_code, event_time) AS country_code,

        -- Session metrics
        COUNT(*) AS events_in_session,
        MIN(event_time) AS session_start,
        MAX(event_time) AS session_end,

        -- Conversion flags
        MAX(CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END) AS has_purchase,
        MAX(CASE WHEN event_type = 'add_to_cart' THEN 1 ELSE 0 END) AS has_add_to_cart,
        MAX(CASE WHEN event_type = 'sign_up' THEN 1 ELSE 0 END) AS has_signup,
        MAX(CASE WHEN event_type = 'checkout_start' THEN 1 ELSE 0 END) AS has_checkout,

        -- Revenue (if purchased)
        SUM(CASE WHEN event_type = 'purchase' THEN total_cents ELSE 0 END) AS revenue_cents,
        SUM(CASE WHEN event_type = 'purchase' THEN gross_profit_cents ELSE 0 END) AS profit_cents,
        SUM(CASE WHEN event_type = 'purchase' THEN quantity ELSE 0 END) AS items_purchased

    FROM events
    GROUP BY session_id, event_date
),

-- =============================================================================
-- AGGREGATE BY MARKETING CHANNEL
-- =============================================================================
channel_metrics AS (
    SELECT
        -- =================================================================
        -- DIMENSIONS
        -- =================================================================
        event_date AS date_day,

        -- Channel grouping
        traffic_source,
        COALESCE(utm_source, traffic_source) AS source,
        COALESCE(utm_medium, 'none') AS medium,
        COALESCE(utm_campaign, 'none') AS campaign,

        -- For detailed analysis
        COALESCE(utm_content, 'none') AS content,
        COALESCE(utm_term, 'none') AS term,

        -- =================================================================
        -- TRAFFIC METRICS
        -- =================================================================
        COUNT(DISTINCT session_id) AS sessions,
        COUNT(DISTINCT CASE WHEN is_registered_user THEN user_id END) AS unique_users,
        SUM(events_in_session) AS total_events,

        -- Session duration (seconds)
        AVG(dateDiff('second', session_start, session_end)) AS avg_session_duration_seconds,

        -- =================================================================
        -- ENGAGEMENT METRICS
        -- =================================================================
        -- Bounce rate approximation (single event sessions)
        SUM(CASE WHEN events_in_session = 1 THEN 1 ELSE 0 END) AS bounced_sessions,

        -- Average events per session
        AVG(events_in_session) AS avg_events_per_session,

        -- =================================================================
        -- CONVERSION METRICS
        -- =================================================================
        -- Add to cart
        SUM(has_add_to_cart) AS sessions_with_cart,

        -- Checkout starts
        SUM(has_checkout) AS sessions_with_checkout,

        -- Sign ups
        SUM(has_signup) AS signups,

        -- Purchases (conversions)
        SUM(has_purchase) AS conversions,

        -- Orders
        COUNT(DISTINCT CASE WHEN has_purchase = 1 THEN session_id END) AS converting_sessions,

        -- =================================================================
        -- REVENUE METRICS
        -- =================================================================
        SUM(revenue_cents) AS total_revenue_cents,
        SUM(profit_cents) AS total_profit_cents,
        SUM(items_purchased) AS total_items,

        -- =================================================================
        -- DEVICE & GEO BREAKDOWN
        -- =================================================================
        COUNT(DISTINCT CASE WHEN device_type = 'desktop' THEN session_id END) AS desktop_sessions,
        COUNT(DISTINCT CASE WHEN device_type = 'mobile' THEN session_id END) AS mobile_sessions,
        COUNT(DISTINCT CASE WHEN platform = 'ios' THEN session_id END) AS ios_sessions,
        COUNT(DISTINCT CASE WHEN platform = 'android' THEN session_id END) AS android_sessions

    FROM session_attribution
    GROUP BY
        event_date,
        traffic_source,
        COALESCE(utm_source, traffic_source),
        COALESCE(utm_medium, 'none'),
        COALESCE(utm_campaign, 'none'),
        COALESCE(utm_content, 'none'),
        COALESCE(utm_term, 'none')
),

-- =============================================================================
-- ADD RATES AND DERIVED METRICS
-- =============================================================================
with_rates AS (
    SELECT
        *,

        -- =================================================================
        -- CONVERSION RATES
        -- =================================================================
        -- Cart rate
        CASE
            WHEN sessions > 0 THEN CAST(sessions_with_cart AS Float64) / sessions
            ELSE 0
        END AS cart_rate,

        -- Checkout rate
        CASE
            WHEN sessions > 0 THEN CAST(sessions_with_checkout AS Float64) / sessions
            ELSE 0
        END AS checkout_rate,

        -- Conversion rate
        CASE
            WHEN sessions > 0 THEN CAST(conversions AS Float64) / sessions
            ELSE 0
        END AS conversion_rate,

        -- Signup rate
        CASE
            WHEN sessions > 0 THEN CAST(signups AS Float64) / sessions
            ELSE 0
        END AS signup_rate,

        -- =================================================================
        -- BOUNCE RATE
        -- =================================================================
        CASE
            WHEN sessions > 0 THEN CAST(bounced_sessions AS Float64) / sessions
            ELSE 0
        END AS bounce_rate,

        -- =================================================================
        -- REVENUE METRICS
        -- =================================================================
        -- AOV (Average Order Value)
        CASE
            WHEN conversions > 0 THEN CAST(total_revenue_cents AS Float64) / conversions
            ELSE 0
        END AS aov_cents,

        -- Revenue per session
        CASE
            WHEN sessions > 0 THEN CAST(total_revenue_cents AS Float64) / sessions
            ELSE 0
        END AS revenue_per_session_cents,

        -- Profit per session
        CASE
            WHEN sessions > 0 THEN CAST(total_profit_cents AS Float64) / sessions
            ELSE 0
        END AS profit_per_session_cents,

        -- Average profit per order
        CASE
            WHEN conversions > 0 THEN CAST(total_profit_cents AS Float64) / conversions
            ELSE 0
        END AS avg_profit_per_order_cents,

        -- =================================================================
        -- DEVICE DISTRIBUTION
        -- =================================================================
        CASE
            WHEN sessions > 0 THEN CAST(desktop_sessions AS Float64) / sessions
            ELSE 0
        END AS desktop_pct,

        CASE
            WHEN sessions > 0 THEN CAST(mobile_sessions AS Float64) / sessions
            ELSE 0
        END AS mobile_pct

    FROM channel_metrics
),

-- =============================================================================
-- CHANNEL SCORING
-- =============================================================================
with_scores AS (
    SELECT
        *,

        -- =================================================================
        -- CHANNEL EFFICIENCY SCORE (0-100)
        -- =================================================================
        -- Combines:
        -- - Conversion rate (50%)
        -- - Revenue efficiency (30%)
        -- - Engagement (20%)
        (
            -- Conversion score (assume 5% conversion is excellent)
            LEAST(conversion_rate / 0.05, 1.0) * 50 +

            -- Revenue score (assume $5 revenue per session is excellent)
            LEAST(revenue_per_session_cents / 500.0, 1.0) * 30 +

            -- Engagement score (inverse bounce rate)
            LEAST((1 - bounce_rate), 1.0) * 20
        ) AS channel_efficiency_score,

        -- =================================================================
        -- CHANNEL TIER
        -- =================================================================
        CASE
            WHEN conversion_rate >= 0.05 AND revenue_per_session_cents >= 500 THEN 'tier_1'
            WHEN conversion_rate >= 0.02 AND revenue_per_session_cents >= 200 THEN 'tier_2'
            WHEN conversion_rate >= 0.01 THEN 'tier_3'
            ELSE 'tier_4'
        END AS channel_tier,

        -- =================================================================
        -- CHANNEL TYPE CLASSIFICATION
        -- =================================================================
        CASE
            WHEN traffic_source = 'paid' THEN 'paid'
            WHEN traffic_source = 'organic' THEN 'organic'
            WHEN traffic_source = 'social' THEN 'social'
            WHEN traffic_source = 'email' THEN 'email'
            WHEN traffic_source = 'affiliate' THEN 'affiliate'
            WHEN traffic_source = 'referral' THEN 'referral'
            ELSE 'direct'
        END AS channel_type,

        -- =================================================================
        -- IS PAID TRAFFIC
        -- =================================================================
        traffic_source = 'paid' OR medium IN ('cpc', 'ppc', 'paid', 'display', 'retargeting') AS is_paid

    FROM with_rates
)

SELECT
    -- Unique key
    {{ dbt_utils.generate_surrogate_key(['date_day', 'source', 'medium', 'campaign', 'content', 'term']) }} AS attribution_id,
    *,
    NOW() AS dbt_updated_at
FROM with_scores
