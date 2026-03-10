-- =============================================================================
-- SILVER LAYER (DW): tracking.silver_payments
-- =============================================================================
-- Chuẩn hoá dữ liệu từ iceberg_catalog.bronze.payments:
--
-- 1. status: lowercase + trim + map invalid → 'unknown'
--    Input:  "paid" / "PAID" / "Paid " / null / "invalid_xyz" / ""
--    Output: "paid" / "paid" / "paid"  / "unknown" / "unknown"  / "unknown"
--    Valid:  paid | pending | failed | refunded | cancelled | unknown
--
-- 2. amount: loại bỏ null, âm, zero
--    Input:  null / -100 / 0 / 99.99
--    Output: loại / loại / loại / 99.99
--
-- 3. event_id / user_id: loại bỏ null (không thể UPSERT an toàn)
--
-- Target: tracking.silver_payments (StarRocks PRIMARY KEY table)
--   → dbt-starrocks INSERT INTO PRIMARY KEY table = UPSERT by (event_id, payment_date)
-- Source: iceberg_catalog.bronze.payments (Iceberg external catalog, Parquet on MinIO)
-- =============================================================================

{{
  config(
    materialized = 'incremental',
    unique_key = ['event_id', 'payment_date']
  )
}}

SELECT
    event_id,
    payment_date,
    user_id,

    -- ===========================================================================
    -- NORMALIZE amount
    -- ===========================================================================
    -- Bronze có thể chứa: null, âm, 0 (bad data từ event-producer)
    -- Silver chỉ giữ amount > 0
    amount,

    -- ===========================================================================
    -- NORMALIZE status
    -- ===========================================================================
    -- Bước 1: lowercase + trim
    -- Bước 2: map giá trị không hợp lệ → 'unknown'
    CASE
        WHEN LOWER(TRIM(status)) IN ('paid', 'pending', 'failed', 'refunded', 'cancelled')
            THEN LOWER(TRIM(status))
        ELSE 'unknown'
    END AS status,

    updated_at

FROM {{ source('bronze', 'payments') }}

WHERE
    -- Lọc bad data
    event_id IS NOT NULL AND event_id != ''     -- cần event_id để UPSERT
    AND user_id IS NOT NULL AND user_id > 0     -- user_id hợp lệ
    AND amount IS NOT NULL AND amount > 0       -- chỉ giữ amount dương
    AND payment_date IS NOT NULL

{% if is_incremental() %}
    -- Incremental: chỉ xử lý data có payment_date gần nhất (1 ngày overlap để catch late data).
    -- StarRocks: COALESCE an toàn vì MAX() trả về NULL khi table rỗng (không có UInt16 overflow như CH).
    AND payment_date >= (
        SELECT COALESCE(
            DATE_SUB(MAX(payment_date), INTERVAL 1 DAY),
            '1970-01-01'
        )
        FROM {{ this }}
    )
{% endif %}
