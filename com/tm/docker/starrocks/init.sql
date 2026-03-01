-- =============================================================================
-- STARROCKS INITIALIZATION SCRIPT
-- =============================================================================
-- Script này được chạy khi StarRocks khởi động để tạo database và tables.
--
-- STARROCKS LÀ GÌ?
-- - Real-time analytics database
-- - Sub-second query latency
-- - MPP (Massively Parallel Processing) architecture
-- - Compatible với MySQL protocol
--
-- TẠI SAO DÙNG STARROCKS CHO ANALYTICS?
-- - ClickHouse tốt cho raw data storage (write-heavy)
-- - StarRocks tốt cho analytics queries (read-heavy)
-- - StarRocks có better query optimizer
-- - Dễ integrate với BI tools (MySQL protocol)
--
-- ARCHITECTURE:
-- - FE (Frontend): Query engine, metadata
-- - BE (Backend): Data storage, computation
--
-- TABLE TYPES:
-- - Duplicate Key: Lưu tất cả rows
-- - Aggregate Key: Pre-aggregate khi insert
-- - Unique Key: Latest value cho mỗi key
-- - Primary Key: UPSERT support
--
-- CÁCH CHẠY:
-- mysql -h localhost -P 9030 -u root < init.sql
-- =============================================================================

-- =============================================================================
-- 1. TẠO DATABASE
-- =============================================================================
CREATE DATABASE IF NOT EXISTS analytics
COMMENT 'Analytics database for dropshipping platform';

USE analytics;

-- =============================================================================
-- 2. FACT TABLE: fct_daily_metrics
-- =============================================================================
-- Aggregated daily metrics by platform and country.
--
-- Model: DUPLICATE KEY
-- - Mỗi row là unique (date + platform + country)
-- - Không cần aggregation on insert
--
-- Distribution: HASH(date_day)
-- - Data được phân phối đều theo ngày
-- - Queries filter by date sẽ efficient
--
-- Bucket: 8 buckets
-- - Mỗi partition chia thành 8 tablets
-- - Parallelism cho queries
-- =============================================================================
CREATE TABLE IF NOT EXISTS fct_daily_metrics (
    -- Surrogate key
    metric_id VARCHAR(64) NOT NULL,

    -- Dimensions
    date_day DATE NOT NULL COMMENT 'Date of metrics',
    platform VARCHAR(20) COMMENT 'Platform: web, ios, android',
    country_code VARCHAR(5) COMMENT 'ISO country code',

    -- Traffic Metrics
    total_sessions BIGINT COMMENT 'Total sessions',
    unique_users BIGINT COMMENT 'Unique registered users',
    anonymous_sessions BIGINT COMMENT 'Anonymous sessions',
    total_events BIGINT COMMENT 'Total events',
    page_views BIGINT COMMENT 'Page view events',

    -- Engagement Metrics
    product_views BIGINT COMMENT 'Product detail page views',
    unique_products_viewed BIGINT COMMENT 'Unique products viewed',
    product_clicks BIGINT COMMENT 'Product click events',
    searches BIGINT COMMENT 'Search events',
    unique_search_queries BIGINT COMMENT 'Unique search queries',
    zero_result_searches BIGINT COMMENT 'Searches with no results',

    -- Cart Metrics
    add_to_cart_events BIGINT COMMENT 'Add to cart events',
    sessions_with_add_to_cart BIGINT COMMENT 'Sessions that added to cart',
    remove_from_cart_events BIGINT COMMENT 'Remove from cart events',
    cart_views BIGINT COMMENT 'Cart view events',

    -- Checkout Metrics
    checkout_starts BIGINT COMMENT 'Checkout start events',
    checkout_payments BIGINT COMMENT 'Payment step reached',
    sessions_with_checkout BIGINT COMMENT 'Sessions that started checkout',

    -- Conversion Metrics
    total_orders BIGINT COMMENT 'Total purchase events',
    buyers BIGINT COMMENT 'Unique buying sessions',
    unique_orders BIGINT COMMENT 'Unique order IDs',

    -- Revenue Metrics (in cents)
    gross_revenue_cents BIGINT COMMENT 'Gross revenue in cents',
    net_revenue_cents BIGINT COMMENT 'Net revenue after discounts',
    total_discount_cents BIGINT COMMENT 'Total discounts given',
    shipping_revenue_cents BIGINT COMMENT 'Shipping revenue',
    tax_collected_cents BIGINT COMMENT 'Tax collected',
    items_sold BIGINT COMMENT 'Total items sold',

    -- Dropshipping Metrics
    cogs_cents BIGINT COMMENT 'Cost of goods sold (cents)',
    gross_profit_cents BIGINT COMMENT 'Gross profit (cents)',
    avg_margin_rate DOUBLE COMMENT 'Average margin rate',
    unique_suppliers BIGINT COMMENT 'Unique suppliers sold from',

    -- User Acquisition
    sign_ups BIGINT COMMENT 'New sign ups',
    logins BIGINT COMMENT 'Login events',

    -- Traffic Source Breakdown
    organic_sessions BIGINT,
    paid_sessions BIGINT,
    social_sessions BIGINT,
    email_sessions BIGINT,
    direct_sessions BIGINT,
    referral_sessions BIGINT,
    affiliate_sessions BIGINT,

    -- Device Breakdown
    desktop_sessions BIGINT,
    mobile_sessions BIGINT,
    tablet_sessions BIGINT,

    -- Calculated Rates
    view_to_cart_rate DOUBLE COMMENT 'View to cart conversion rate',
    cart_to_checkout_rate DOUBLE COMMENT 'Cart to checkout rate',
    checkout_conversion_rate DOUBLE COMMENT 'Checkout to purchase rate',
    conversion_rate DOUBLE COMMENT 'Overall conversion rate',
    aov_cents DOUBLE COMMENT 'Average order value (cents)',
    avg_items_per_order DOUBLE COMMENT 'Average items per order',
    pages_per_session DOUBLE COMMENT 'Average pages per session',
    gross_margin_pct DOUBLE COMMENT 'Gross margin percentage',

    -- Metadata
    dbt_updated_at DATETIME COMMENT 'Last updated by dbt'
)
-- Duplicate key model: Store all rows
DUPLICATE KEY(metric_id, date_day)
-- Partition by month
PARTITION BY RANGE(date_day) (
    START ("2024-01-01") END ("2025-12-31") EVERY (INTERVAL 1 MONTH)
)
-- Distribute by date for query efficiency
DISTRIBUTED BY HASH(date_day) BUCKETS 8
PROPERTIES (
    "replication_num" = "1",  -- 1 replica for dev, 3 for prod
    "storage_medium" = "HDD"
);

-- =============================================================================
-- 3. FACT TABLE: fct_product_performance
-- =============================================================================
-- Daily product performance metrics.
-- =============================================================================
CREATE TABLE IF NOT EXISTS fct_product_performance (
    -- Keys
    performance_id VARCHAR(64) NOT NULL,
    date_day DATE NOT NULL,
    product_id BIGINT NOT NULL,

    -- Product Info
    product_sku VARCHAR(50),
    product_name VARCHAR(255),
    category_id INT,
    category_name VARCHAR(100),
    brand VARCHAR(100),

    -- Supplier Info
    supplier_id INT,
    supplier_name VARCHAR(100),
    supplier_country VARCHAR(5),

    -- Pricing
    current_price_cents BIGINT,
    current_cost_cents BIGINT,
    current_margin_cents BIGINT,
    current_margin_rate DOUBLE,
    shipping_days INT,
    shipping_tier VARCHAR(20),

    -- Funnel Metrics
    views BIGINT COMMENT 'Product page views',
    unique_viewers BIGINT COMMENT 'Unique sessions that viewed',
    clicks BIGINT COMMENT 'Product clicks from listings',
    add_to_cart_count BIGINT COMMENT 'Add to cart events',
    add_to_cart_quantity BIGINT COMMENT 'Total quantity added',
    unique_cart_adds BIGINT COMMENT 'Unique sessions added to cart',
    remove_from_cart_count BIGINT,
    purchase_count BIGINT,
    units_sold BIGINT,
    unique_buyers BIGINT,
    order_count BIGINT,

    -- Revenue
    revenue_cents BIGINT COMMENT 'Total revenue from product',
    cogs_cents BIGINT COMMENT 'Cost of goods sold',
    gross_profit_cents BIGINT COMMENT 'Gross profit',
    avg_selling_price_cents DOUBLE,

    -- Engagement
    wishlist_adds BIGINT,
    wishlist_removes BIGINT,
    image_zooms BIGINT,
    review_reads BIGINT,

    -- Rates
    click_to_view_rate DOUBLE,
    view_to_cart_rate DOUBLE,
    cart_to_purchase_rate DOUBLE,
    conversion_rate DOUBLE,
    cart_abandonment_rate DOUBLE,
    gross_margin_pct DOUBLE,
    revenue_per_view_cents DOUBLE,
    profit_per_view_cents DOUBLE,
    wishlist_conversion_rate DOUBLE,
    image_engagement_rate DOUBLE,
    review_engagement_rate DOUBLE,
    net_cart_adds BIGINT,

    -- Scoring
    product_health_score DOUBLE COMMENT 'Product health score (0-100)',
    margin_tier VARCHAR(20) COMMENT 'high/medium/low/very_low margin',
    conversion_tier VARCHAR(20) COMMENT 'high/medium/low converting',
    demand_tier VARCHAR(20) COMMENT 'high/medium/low demand',

    -- Metadata
    dbt_updated_at DATETIME
)
DUPLICATE KEY(performance_id, date_day, product_id)
PARTITION BY RANGE(date_day) (
    START ("2024-01-01") END ("2025-12-31") EVERY (INTERVAL 1 MONTH)
)
DISTRIBUTED BY HASH(product_id) BUCKETS 8
PROPERTIES (
    "replication_num" = "1"
);

-- =============================================================================
-- 4. FACT TABLE: fct_marketing_attribution
-- =============================================================================
-- Marketing channel performance with last-touch attribution.
-- =============================================================================
CREATE TABLE IF NOT EXISTS fct_marketing_attribution (
    -- Keys
    attribution_id VARCHAR(64) NOT NULL,
    date_day DATE NOT NULL,

    -- Channel Dimensions
    traffic_source VARCHAR(50) COMMENT 'Traffic source category',
    source VARCHAR(100) COMMENT 'UTM source or traffic_source',
    medium VARCHAR(50) COMMENT 'UTM medium',
    campaign VARCHAR(255) COMMENT 'UTM campaign',
    content VARCHAR(255) COMMENT 'UTM content',
    term VARCHAR(255) COMMENT 'UTM term (keyword)',

    -- Traffic Metrics
    sessions BIGINT,
    unique_users BIGINT,
    total_events BIGINT,
    avg_session_duration_seconds DOUBLE,
    bounced_sessions BIGINT,
    avg_events_per_session DOUBLE,

    -- Conversion Metrics
    sessions_with_cart BIGINT,
    sessions_with_checkout BIGINT,
    signups BIGINT,
    conversions BIGINT,
    converting_sessions BIGINT,

    -- Revenue
    total_revenue_cents BIGINT,
    total_profit_cents BIGINT,
    total_items BIGINT,

    -- Device Breakdown
    desktop_sessions BIGINT,
    mobile_sessions BIGINT,
    ios_sessions BIGINT,
    android_sessions BIGINT,

    -- Rates
    cart_rate DOUBLE,
    checkout_rate DOUBLE,
    conversion_rate DOUBLE,
    signup_rate DOUBLE,
    bounce_rate DOUBLE,
    aov_cents DOUBLE,
    revenue_per_session_cents DOUBLE,
    profit_per_session_cents DOUBLE,
    avg_profit_per_order_cents DOUBLE,
    desktop_pct DOUBLE,
    mobile_pct DOUBLE,

    -- Channel Scoring
    channel_efficiency_score DOUBLE COMMENT 'Channel efficiency (0-100)',
    channel_tier VARCHAR(10) COMMENT 'tier_1 to tier_4',
    channel_type VARCHAR(20) COMMENT 'paid/organic/social/etc',
    is_paid BOOLEAN COMMENT 'Is paid traffic',

    -- Metadata
    dbt_updated_at DATETIME
)
DUPLICATE KEY(attribution_id, date_day)
PARTITION BY RANGE(date_day) (
    START ("2024-01-01") END ("2025-12-31") EVERY (INTERVAL 1 MONTH)
)
DISTRIBUTED BY HASH(source) BUCKETS 8
PROPERTIES (
    "replication_num" = "1"
);

-- =============================================================================
-- 5. TẠO VIEWS CHO COMMON QUERIES
-- =============================================================================

-- View: Top products by revenue (last 30 days)
CREATE VIEW IF NOT EXISTS v_top_products_30d AS
SELECT
    product_name,
    category_name,
    brand,
    SUM(views) as total_views,
    SUM(units_sold) as total_sold,
    SUM(revenue_cents) / 100 as revenue_dollars,
    SUM(gross_profit_cents) / 100 as profit_dollars,
    AVG(conversion_rate) as avg_conversion_rate,
    AVG(gross_margin_pct) as avg_margin_pct
FROM fct_product_performance
WHERE date_day >= CURRENT_DATE() - INTERVAL 30 DAY
GROUP BY product_name, category_name, brand
ORDER BY revenue_dollars DESC
LIMIT 100;

-- View: Channel performance (last 7 days)
CREATE VIEW IF NOT EXISTS v_channel_performance_7d AS
SELECT
    traffic_source,
    source,
    medium,
    SUM(sessions) as sessions,
    SUM(conversions) as conversions,
    SUM(total_revenue_cents) / 100 as revenue_dollars,
    AVG(conversion_rate) as avg_conversion_rate,
    AVG(channel_efficiency_score) as avg_efficiency
FROM fct_marketing_attribution
WHERE date_day >= CURRENT_DATE() - INTERVAL 7 DAY
GROUP BY traffic_source, source, medium
ORDER BY revenue_dollars DESC;

-- View: Daily KPIs
CREATE VIEW IF NOT EXISTS v_daily_kpis AS
SELECT
    date_day,
    SUM(total_sessions) as sessions,
    SUM(total_orders) as orders,
    SUM(gross_revenue_cents) / 100 as revenue,
    SUM(gross_profit_cents) / 100 as profit,
    AVG(conversion_rate) as conversion_rate,
    AVG(gross_margin_pct) as margin_pct
FROM fct_daily_metrics
GROUP BY date_day
ORDER BY date_day DESC;

-- =============================================================================
-- NOTES
-- =============================================================================
--
-- Performance Tips:
-- 1. Use partition pruning: WHERE date_day >= '2024-01-01'
-- 2. Use bucket key in WHERE: WHERE product_id = 123
-- 3. Avoid SELECT * - specify columns
-- 4. Use LIMIT for exploration
--
-- Maintenance:
-- 1. Monitor disk usage: SHOW DATA
-- 2. Check table stats: SHOW STATS <table>
-- 3. Refresh stats: ANALYZE TABLE <table>
--
-- Connection:
-- mysql -h localhost -P 9030 -u root analytics
-- =============================================================================
