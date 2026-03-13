-- =============================================================================
-- CDP Normalize Macros — StarRocks (dbt_cdp_silver)
-- =============================================================================
-- Dùng chung cho tất cả silver models trong dbt_cdp_silver.
-- Khi thêm event source mới, silver model chỉ cần gọi macro thay vì copy SQL.
-- =============================================================================

-- Normalize platform: lowercase valid values, invalid → 'unknown'
-- Valid: ios | android | web | unknown
{% macro normalize_platform(col) %}
CAST(
    CASE
        WHEN LOWER(TRIM({{ col }})) IN ('ios', 'android', 'web')
            THEN LOWER(TRIM({{ col }}))
        ELSE 'unknown'
    END
AS VARCHAR(20))
{%- endmacro %}


-- Normalize country: UPPERCASE, empty/null → 'unknown'
{% macro normalize_country(col) %}
CAST(
    CASE
        WHEN TRIM({{ col }}) = '' OR {{ col }} IS NULL THEN 'unknown'
        ELSE UPPER(TRIM({{ col }}))
    END
AS VARCHAR(10))
{%- endmacro %}


-- Normalize identifier (session_id, device_id): empty/null → 'unknown'
{% macro normalize_id(col, max_len=64) %}
CAST(COALESCE(NULLIF(TRIM({{ col }}), ''), 'unknown') AS VARCHAR({{ max_len }}))
{%- endmacro %}


-- Incremental filter: 1-day overlap để catch late-arriving data
-- Tránh reprocess toàn bộ table khi is_incremental()
{% macro silver_incremental_filter(date_col='event_date') %}
{{ date_col }} >= (
    SELECT COALESCE(
        DATE_SUB(MAX({{ date_col }}), INTERVAL 1 DAY),
        '1970-01-01'
    )
    FROM {{ this }}
)
{%- endmacro %}
