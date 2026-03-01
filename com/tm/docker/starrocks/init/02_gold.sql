-- =============================================================================
-- STARROCKS GOLD LAYER: gold_revenue
-- =============================================================================
-- Revenue metrics theo ngày, tạo bởi dbt_starrocks từ silver_payments.
-- Superset kết nối trực tiếp vào table này.
--
-- Metrics:
--   total_revenue      = SUM(amount) WHERE status = 'paid'
--   total_paid_orders  = COUNT(*) WHERE status = 'paid'
-- =============================================================================

USE analytics;

CREATE TABLE IF NOT EXISTS gold_revenue (
    payment_date        DATE            NOT NULL COMMENT 'Ngày (grain)',

    -- Revenue metrics
    total_revenue       DECIMAL(15, 2)  NOT NULL DEFAULT 0 COMMENT 'Tổng doanh thu (chỉ tính paid)',
    total_paid_orders   BIGINT          NOT NULL DEFAULT 0 COMMENT 'Số đơn paid',

    -- Breakdown theo status (để debug và so sánh)
    pending_orders      BIGINT          NOT NULL DEFAULT 0,
    failed_orders       BIGINT          NOT NULL DEFAULT 0,
    refunded_orders     BIGINT          NOT NULL DEFAULT 0,
    cancelled_orders    BIGINT          NOT NULL DEFAULT 0,
    unknown_orders      BIGINT          NOT NULL DEFAULT 0,

    -- Unique users ngày đó
    unique_users        BIGINT          NOT NULL DEFAULT 0,

    -- Average order value (AOV) cho đơn paid
    avg_order_value     DECIMAL(10, 2)  COMMENT 'total_revenue / total_paid_orders',

    dbt_updated_at      DATETIME        COMMENT 'Thời gian dbt chạy'
)
PRIMARY KEY (payment_date)
DISTRIBUTED BY HASH(payment_date) BUCKETS 4
PROPERTIES ("replication_num" = "1");
