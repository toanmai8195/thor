-- =============================================================================
-- GOLD LAYER: gold_revenue
-- =============================================================================
-- Revenue metrics theo ngày từ silver_payments.
-- Grain: 1 row = 1 ngày (payment_date)
--
-- Metrics chính:
--   total_revenue     = SUM(amount) WHERE status = 'paid'
--   total_paid_orders = COUNT(*) WHERE status = 'paid'
--
-- Superset query example:
--   SELECT payment_date, total_revenue, total_paid_orders
--   FROM analytics.gold_revenue
--   ORDER BY payment_date DESC;
-- =============================================================================

{{
  config(
    materialized = 'table'
  )
}}

SELECT
    payment_date,

    -- =========================================================================
    -- REVENUE METRICS (chỉ tính đơn 'paid')
    -- =========================================================================
    SUM(CASE WHEN status = 'paid' THEN amount ELSE 0 END)   AS total_revenue,
    COUNT(CASE WHEN status = 'paid' THEN 1 END)             AS total_paid_orders,

    -- =========================================================================
    -- BREAKDOWN THEO STATUS (để debug)
    -- =========================================================================
    COUNT(CASE WHEN status = 'pending'   THEN 1 END)        AS pending_orders,
    COUNT(CASE WHEN status = 'failed'    THEN 1 END)        AS failed_orders,
    COUNT(CASE WHEN status = 'refunded'  THEN 1 END)        AS refunded_orders,
    COUNT(CASE WHEN status = 'cancelled' THEN 1 END)        AS cancelled_orders,
    COUNT(CASE WHEN status = 'unknown'   THEN 1 END)        AS unknown_orders,

    -- =========================================================================
    -- UNIQUE USERS
    -- =========================================================================
    COUNT(DISTINCT user_id)                                  AS unique_users,

    -- =========================================================================
    -- AVERAGE ORDER VALUE (AOV)
    -- Chỉ tính trên đơn paid, tránh division by zero
    -- =========================================================================
    CASE
        WHEN COUNT(CASE WHEN status = 'paid' THEN 1 END) > 0
        THEN SUM(CASE WHEN status = 'paid' THEN amount ELSE 0 END)
             / COUNT(CASE WHEN status = 'paid' THEN 1 END)
        ELSE 0
    END                                                      AS avg_order_value,

    NOW()                                                    AS dbt_updated_at

FROM {{ source('silver', 'silver_payments') }}

GROUP BY payment_date
ORDER BY payment_date DESC
