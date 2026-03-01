-- =============================================================================
-- SILVER LAYER: silver_payments
-- =============================================================================
-- Chuẩn hoá dữ liệu từ payments_bronze:
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
-- 3. event_id: loại bỏ blank (không thể deduplicate)
--
-- Rename: silver_events.sql (file name) → model tên silver_payments
-- =============================================================================

{{
  config(
    materialized = 'incremental',
    unique_key = 'event_id',
    incremental_strategy = 'append'
  )
}}

SELECT
    event_id,
    user_id,

    -- ===========================================================================
    -- NORMALIZE amount
    -- ===========================================================================
    -- Bronze có thể chứa: null, âm, 0 (bad data từ producer)
    -- Silver chỉ giữ amount > 0
    amount,

    -- ===========================================================================
    -- NORMALIZE status
    -- ===========================================================================
    -- Bước 1: lowercase + trim (loại bỏ khoảng trắng đầu/cuối)
    -- Bước 2: map giá trị không hợp lệ → 'unknown'
    CASE
        WHEN lower(trim(status)) IN ('paid', 'pending', 'failed', 'refunded', 'cancelled')
            THEN lower(trim(status))
        ELSE 'unknown'
    END AS status,

    payment_date,
    updated_at

FROM {{ source('bronze', 'payments_bronze') }}

WHERE
    -- Lọc bad data
    event_id IS NOT NULL AND event_id != ''     -- phải có event_id để deduplicate
    AND user_id IS NOT NULL AND user_id > 0     -- phải có user_id hợp lệ
    AND amount IS NOT NULL AND amount > 0       -- chỉ giữ amount dương
    AND payment_date IS NOT NULL

{% if is_incremental() %}
    -- Incremental: chỉ xử lý data mới (1 ngày overlap để catch late-arriving)
    AND payment_date >= (
        SELECT max(payment_date) - INTERVAL 1 DAY
        FROM {{ this }}
    )
{% endif %}
