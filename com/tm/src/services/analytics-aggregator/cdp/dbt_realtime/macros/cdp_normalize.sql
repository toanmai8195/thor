-- =============================================================================
-- CDP Normalize Macros — ClickHouse (dbt_cdp_realtime)
-- =============================================================================
-- Cùng interface với dbt_cdp_silver macros nhưng ClickHouse syntax.
-- ClickHouse String không có NULL (default ''), nên chỉ cần check empty string.
-- =============================================================================

-- Normalize platform: lowercase valid values, invalid → 'unknown'
{% macro normalize_platform(col) %}
CASE
    WHEN lower(trim({{ col }})) IN ('ios', 'android', 'web')
        THEN lower(trim({{ col }}))
    ELSE 'unknown'
END
{%- endmacro %}


-- Normalize country: UPPERCASE, empty → 'unknown'
-- Note: ClickHouse String không null → chỉ check empty string
{% macro normalize_country(col) %}
CASE
    WHEN trim({{ col }}) = '' THEN 'unknown'
    ELSE upper(trim({{ col }}))
END
{%- endmacro %}


-- Normalize identifier: empty → 'unknown'
{% macro normalize_id(col) %}
if(trim({{ col }}) = '', 'unknown', trim({{ col }}))
{%- endmacro %}


-- Incremental filter: 1-day overlap, tránh UInt16 Date overflow khi table rỗng
{% macro silver_incremental_filter(date_col='event_date') %}
{{ date_col }} >= (
    SELECT if(
        count() > 0,
        max({{ date_col }}) - INTERVAL 1 DAY,
        toDate('1970-01-01')
    )
    FROM {{ this }}
)
{%- endmacro %}
