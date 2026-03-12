# Luồng 1: Realtime Pipeline (ClickHouse)

Pipeline xử lý login events **theo thời gian thực** với latency < 2 phút.

---

## Kiến trúc

```
Kafka: login-events-ingest
        │ consumer group: ch-cdp-login
        ▼
cdp.login_kafka (ClickHouse Kafka Engine)
        │ Materialized View (filter _error = '')
        ▼
cdp.login_bronze (ClickHouse MergeTree, TTL 90d)
        │ Airflow: cdp_realtime_pipeline (*/1 * * * *)
        │ dbt-clickhouse: silver_login (incremental append)
        ▼
cdp.silver_login (ClickHouse ReplacingMergeTree)
        │
    Superset (clickhousedb://default@clickhouse:8123/cdp)
```

---

## Schema

### `cdp.login_kafka` — Kafka Engine (staging)

| Column | Type | Ghi chú |
|--------|------|---------|
| `event_id` | String | |
| `user_id` | Int64 | |
| `session_id` | String | |
| `device_id` | String | |
| `platform` | String | |
| `country` | String | |
| `event_date` | Date | |
| `login_at` | String | Raw ISO8601 string |
| `_error` | String | Parse error — lọc bởi MV |

Không persist. Chỉ là cầu nối từ Kafka → MergeTree.

### `cdp.login_bronze` — Bronze (MergeTree)

| Column | Type | Ghi chú |
|--------|------|---------|
| `event_date` | Date | Partition key (PARTITION BY toYYYYMM) |
| `event_id` | String | |
| `user_id` | Int64 | |
| `session_id` | String | |
| `device_id` | String | |
| `platform` | String | Raw — chưa normalize |
| `country` | String | Raw — chưa normalize |
| `login_at` | String | |

- **Engine:** MergeTree
- **ORDER BY:** `(event_date, user_id, event_id)`
- **TTL:** 90 ngày (tự động xóa data cũ)
- **Partition:** `toYYYYMM(event_date)`

### `cdp.silver_login` — Silver (ReplacingMergeTree)

| Column | Type | Ghi chú |
|--------|------|---------|
| `event_date` | Date | Partition key |
| `event_id` | String | Dedup key |
| `user_id` | Int64 | |
| `session_id` | String | empty → `'unknown'` |
| `device_id` | String | empty → `'unknown'` |
| `platform` | LowCardinality(String) | ios/android/web/unknown |
| `country` | LowCardinality(String) | uppercase, empty → `'unknown'` |
| `login_at` | String | |

- **Engine:** ReplacingMergeTree
- **ORDER BY:** `(event_date, user_id, event_id)`
- **Dedup:** ReplacingMergeTree tự merge duplicate event_id theo background

---

## dbt Project: `dbt_cdp_realtime`

**Path:** `com/tm/src/services/analytics-aggregator/cdp/dbt_realtime/`

**Profile:** `cdp_realtime` — kết nối ClickHouse `cdp` database

```yaml
# profiles.yml
cdp_realtime:
  target: dev
  outputs:
    dev:
      type: clickhouse
      host: clickhouse
      port: 8123
      schema: cdp
      user: default
      password: ""
```

**Model `silver_login`:**

```sql
{{ config(
    materialized = 'incremental',
    unique_key = 'event_id',
    incremental_strategy = 'append',
    on_schema_change = 'ignore'   -- tránh ClickHouse 26.x new analyzer bug
) }}

SELECT
    event_id, event_date, user_id,
    if(trim(session_id) = '', 'unknown', trim(session_id)) AS session_id,
    if(trim(device_id)  = '', 'unknown', trim(device_id))  AS device_id,
    CASE WHEN lower(trim(platform)) IN ('ios','android','web')
         THEN lower(trim(platform)) ELSE 'unknown' END AS platform,
    CASE WHEN trim(country) = '' THEN 'unknown'
         ELSE upper(trim(country)) END AS country,
    login_at
FROM {{ source('bronze', 'login_bronze') }}
WHERE event_id != '' AND user_id > 0 AND event_date IS NOT NULL
{% if is_incremental() %}
    AND event_date >= (
        SELECT if(count() > 0, max(event_date) - INTERVAL 1 DAY, toDate('1970-01-01'))
        FROM {{ this }}
    )
{% endif %}
```

**Lưu ý ClickHouse 26.x:**
- Bảng `cdp.silver_login` phải được pre-create trong init SQL (`02_silver.sql`)
- Dùng `on_schema_change: ignore` để dbt không thực hiện ALTER TABLE (gây lỗi với new analyzer)
- Incremental guard dùng `if(count()>0,...)` thay vì `COALESCE` — tránh Date UInt16 overflow khi bảng rỗng

---

## Airflow DAG: `cdp_realtime_pipeline`

**File:** `com/tm/src/services/analytics-aggregator/cdp/dags/cdp_realtime_dag.py`

**Schedule:** `*/1 * * * *` (mỗi 1 phút)

| Task | Mô tả |
|------|-------|
| `check_bronze_freshness` | Query `COUNT(*) FROM cdp.login_bronze` qua HTTP — fail nếu = 0 |
| `dbt_silver_run` | `dbt run --select silver_login` trong `/opt/analytics/cdp/dbt_realtime` |

```
check_bronze_freshness → dbt_silver_run
```

**Environment trong container Airflow:**

| Biến | Default | Ghi chú |
|------|---------|---------|
| `CLICKHOUSE_HOST` | `clickhouse` | hostname trong cdp-network |
| `CLICKHOUSE_HTTP_PORT` | `8123` | Internal port |
| `CLICKHOUSE_USER` | `default` | |
| `CLICKHOUSE_PASSWORD` | `""` | |

---

## Verify

```bash
# Bronze rows
curl -s "http://localhost:8124/?query=SELECT+COUNT()+FROM+cdp.login_bronze"

# Silver sample
curl -s "http://localhost:8124/?query=SELECT+event_date,platform,country,COUNT()+as+cnt+FROM+cdp.silver_login+GROUP+BY+1,2,3+ORDER+BY+1+DESC+LIMIT+10&default_format=TSV"

# Consumer lag
curl -s "http://localhost:8124/?query=SELECT+*+FROM+system.kafka_consumers+FORMAT+Vertical"

# DAG trạng thái
open http://localhost:8086   # Airflow — DAG: cdp_realtime_pipeline
```
