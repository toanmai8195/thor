# Luồng 2: DW Pipeline (Iceberg + StarRocks)

Pipeline xử lý **5 event sources** theo **batch** với exactly-once semantics, phục vụ analytics và Gold aggregations.

---

## Kiến trúc

```
login-event-producer (~500/s)   event-producer (10/s × 4)
        │ login-events                 │ view/click/payment/search-events
        ▼                             ▼
cdp-ingestor-login          cdp-ingestor-{view,click,payment,search}
        │                             │
        └──────────────┬──────────────┘
                       ▼ *-events-ingest topics
        Apache Flink (exactly-once, checkpoint 60s)
                       │ Parquet files
                       ▼
        Iceberg: bronze/{login,view,click,payment,search}_events
                (MinIO, format-version=2)
                       │ StarRocks external catalog (iceberg_catalog)
                       ▼
        iceberg_catalog.bronze.*_events   ← Bronze (external)
                       │ Airflow: cdp_medallion_pipeline (*/3 * * * *)
                       │ dbt-starrocks silver_* (UPSERT per source)
                       ▼
        cdp.silver_{login,view,click,payment,search}  (StarRocks PRIMARY KEY)
                       │ dbt-starrocks gold_* (full table rebuild)
                       ▼
        cdp.gold_{user_daily,page_daily,search_daily}  (StarRocks aggregate)
                       │
                   Superset (starrocks+pymysql://root:@starrocks:9030/cdp)
```

---

## Flink Job

**Template:** `com/tm/docker/cdp/flink/templates/kafka_to_iceberg.sql.tmpl`

**Submit script:** `com/tm/docker/cdp/flink/submit_from_registry.sh` — đọc `event_registry.yml`, generate SQL per source, submit qua sql-client.

**Consumer groups:** `flink-iceberg-cdp-{login,view,click,payment,search}`

**Checkpoint:** 60 giây (exactly-once, barrier-based)

**Catalog:** HadoopCatalog trên MinIO (`s3a://cdp-lake/iceberg`)

**Sink config (chung):**
```sql
'write.format.default' = 'parquet',
'format-version' = '2',
'write.parquet.compression-codec' = 'snappy',
'write.target-file-size-bytes' = '134217728'  -- 128 MB per file
```

**Timestamp parse (chung — áp dụng cho tất cả *_at fields):**
```sql
TO_TIMESTAMP(
    LEFT(REPLACE(<ts_field>, 'Z', ''), 23),
    'yyyy-MM-dd''T''HH:mm:ss.SSS'
) AS <ts_field>
```
Lý do: Flink SQL không parse ISO8601 với 'Z' suffix trực tiếp.

---

## Schema

### Bronze — Iceberg (external via StarRocks catalog)

#### `bronze.login_events`

| Column | Type | Ghi chú |
|--------|------|---------|
| `event_id` | STRING | |
| `user_id` | BIGINT | |
| `session_id` | STRING | |
| `device_id` | STRING | |
| `platform` | STRING | Raw |
| `country` | STRING | Raw |
| `event_date` | DATE | Partition column |
| `login_at` | TIMESTAMP(3) | |

#### `bronze.view_events`

| Column | Type | Ghi chú |
|--------|------|---------|
| `event_id` | STRING | |
| `user_id` | BIGINT | |
| `session_id` | STRING | |
| `device_id` | STRING | |
| `platform` | STRING | Raw |
| `country` | STRING | Raw |
| `page_url` | STRING | URL path |
| `referrer` | STRING | Empty = direct traffic |
| `event_date` | DATE | Partition column |
| `viewed_at` | TIMESTAMP(3) | |

#### `bronze.click_events`

| Column | Type | Ghi chú |
|--------|------|---------|
| `event_id` | STRING | |
| `user_id` | BIGINT | |
| `session_id` | STRING | |
| `device_id` | STRING | |
| `platform` | STRING | Raw |
| `country` | STRING | Raw |
| `page_url` | STRING | |
| `element_id` | STRING | DOM/component ID |
| `element_type` | STRING | button/link/image/other |
| `event_date` | DATE | Partition column |
| `clicked_at` | TIMESTAMP(3) | |

#### `bronze.payment_events`

| Column | Type | Ghi chú |
|--------|------|---------|
| `event_id` | STRING | |
| `user_id` | BIGINT | |
| `session_id` | STRING | |
| `order_id` | STRING | UUID — required |
| `amount` | BIGINT | Smallest currency unit |
| `currency` | STRING | VND/USD/SGD |
| `payment_method` | STRING | momo/vnpay/visa/mastercard/other |
| `status` | STRING | paid/pending/failed/refunded |
| `platform` | STRING | Raw |
| `country` | STRING | Raw |
| `event_date` | DATE | Partition column |
| `paid_at` | TIMESTAMP(3) | |

#### `bronze.search_events`

| Column | Type | Ghi chú |
|--------|------|---------|
| `event_id` | STRING | |
| `user_id` | BIGINT | |
| `session_id` | STRING | |
| `platform` | STRING | Raw |
| `country` | STRING | Raw |
| `query` | STRING | Search text |
| `result_count` | BIGINT | 0 = zero-result |
| `clicked_result_id` | STRING | Empty = no click |
| `event_date` | DATE | Partition column |
| `searched_at` | TIMESTAMP(3) | |

---

### Silver — StarRocks PRIMARY KEY

Tất cả silver tables dùng **UPSERT** (dbt incremental `merge`), partition RANGE by month, HASH bucket by `event_id`.

#### `cdp.silver_login`

| Column | Type | Ghi chú |
|--------|------|---------|
| `event_id` | VARCHAR(64) | PRIMARY KEY (1) |
| `event_date` | DATE | PRIMARY KEY (2) |
| `user_id` | BIGINT | |
| `session_id` | VARCHAR(128) | empty → `'unknown'` |
| `device_id` | VARCHAR(128) | empty → `'unknown'` |
| `platform` | VARCHAR(20) | ios/android/web/unknown |
| `country` | CHAR(2) | uppercase, empty → `'unknown'` |
| `login_at` | DATETIME | |
| `dbt_updated_at` | DATETIME | |

#### `cdp.silver_view`

| Column | Type | Ghi chú |
|--------|------|---------|
| `event_id` | VARCHAR(64) | PRIMARY KEY (1) |
| `event_date` | DATE | PRIMARY KEY (2) |
| `user_id` | BIGINT | |
| `session_id` | VARCHAR(128) | empty → `'unknown'` |
| `device_id` | VARCHAR(128) | empty → `'unknown'` |
| `platform` | VARCHAR(20) | |
| `country` | CHAR(2) | |
| `page_url` | VARCHAR(2048) | |
| `referrer` | VARCHAR(2048) | NULL nếu empty (direct traffic) |
| `viewed_at` | DATETIME | |
| `dbt_updated_at` | DATETIME | |

#### `cdp.silver_click`

| Column | Type | Ghi chú |
|--------|------|---------|
| `event_id` | VARCHAR(64) | PRIMARY KEY (1) |
| `event_date` | DATE | PRIMARY KEY (2) |
| `user_id` | BIGINT | |
| `session_id` | VARCHAR(128) | |
| `device_id` | VARCHAR(128) | |
| `platform` | VARCHAR(20) | |
| `country` | CHAR(2) | |
| `page_url` | VARCHAR(2048) | |
| `element_id` | VARCHAR(256) | |
| `element_type` | VARCHAR(20) | button/link/image/other |
| `clicked_at` | DATETIME | |
| `dbt_updated_at` | DATETIME | |

#### `cdp.silver_payment`

| Column | Type | Ghi chú |
|--------|------|---------|
| `event_id` | VARCHAR(64) | PRIMARY KEY (1) |
| `event_date` | DATE | PRIMARY KEY (2) |
| `user_id` | BIGINT | |
| `session_id` | VARCHAR(128) | |
| `order_id` | VARCHAR(64) | |
| `amount` | BIGINT | filter: amount > 0 |
| `currency` | VARCHAR(3) | default VND |
| `payment_method` | VARCHAR(20) | |
| `status` | VARCHAR(20) | paid/pending/failed/refunded |
| `platform` | VARCHAR(20) | |
| `country` | CHAR(2) | |
| `paid_at` | DATETIME | |
| `dbt_updated_at` | DATETIME | |

#### `cdp.silver_search`

| Column | Type | Ghi chú |
|--------|------|---------|
| `event_id` | VARCHAR(64) | PRIMARY KEY (1) |
| `event_date` | DATE | PRIMARY KEY (2) |
| `user_id` | BIGINT | |
| `session_id` | VARCHAR(128) | |
| `platform` | VARCHAR(20) | |
| `country` | CHAR(2) | |
| `query` | VARCHAR(1024) | |
| `result_count` | BIGINT | 0 if negative |
| `has_results` | BOOLEAN | result_count > 0 |
| `clicked_result_id` | VARCHAR(128) | NULL nếu empty |
| `searched_at` | DATETIME | |
| `dbt_updated_at` | DATETIME | |

---

### Gold — StarRocks (full table rebuild mỗi 3 phút)

#### `cdp.gold_user_daily` — from `silver_login`

Grain: 1 row per ngày.

| Column | Type | Mô tả |
|--------|------|-------|
| `event_date` | DATE | PRIMARY KEY |
| `dau` | BIGINT | Daily Active Users |
| `total_logins` | BIGINT | Tổng login events |
| `ios_logins` | BIGINT | platform = ios |
| `android_logins` | BIGINT | platform = android |
| `web_logins` | BIGINT | platform = web |
| `unknown_logins` | BIGINT | platform = unknown |
| `unique_sessions` | BIGINT | |
| `unique_devices` | BIGINT | |
| `vn_users` | BIGINT | country = VN |
| `us_users` | BIGINT | country = US |
| `sg_users` | BIGINT | country = SG |
| `other_users` | BIGINT | |
| `dbt_updated_at` | DATETIME | |

#### `cdp.gold_page_daily` — from `silver_view`

Grain: 1 row per ngày.

| Column | Type | Mô tả |
|--------|------|-------|
| `event_date` | DATE | PRIMARY KEY |
| `total_views` | BIGINT | |
| `unique_viewers` | BIGINT | COUNT DISTINCT user_id |
| `unique_sessions` | BIGINT | |
| `unique_pages` | BIGINT | COUNT DISTINCT page_url |
| `direct_views` | BIGINT | referrer IS NULL |
| `referred_views` | BIGINT | referrer IS NOT NULL |
| `ios_views` | BIGINT | |
| `android_views` | BIGINT | |
| `web_views` | BIGINT | |
| `dbt_updated_at` | DATETIME | |

#### `cdp.gold_search_daily` — from `silver_search`

Grain: 1 row per ngày.

| Column | Type | Mô tả |
|--------|------|-------|
| `event_date` | DATE | PRIMARY KEY |
| `total_searches` | BIGINT | |
| `unique_searchers` | BIGINT | COUNT DISTINCT user_id |
| `zero_result_searches` | BIGINT | has_results = false |
| `zero_result_rate` | DOUBLE | zero_result / total |
| `clicked_searches` | BIGINT | clicked_result_id IS NOT NULL |
| `click_through_rate` | DOUBLE | clicked / total |
| `avg_result_count` | DOUBLE | |
| `dbt_updated_at` | DATETIME | |

---

## dbt Projects

### `dbt_cdp_silver` — Silver transform

**Path:** `com/tm/src/services/analytics-aggregator/cdp/dbt_silver/`

**Profile:** `cdp_silver` — kết nối StarRocks `cdp` database (port 9030)

**Models:** `silver_login`, `silver_view`, `silver_click`, `silver_payment`, `silver_search`

Tất cả dùng:
```sql
{{ config(
    materialized = 'incremental',
    unique_key = ['event_id', 'event_date'],
    incremental_strategy = 'merge',
    on_schema_change = 'ignore'
) }}
```

Incremental filter macro: `silver_incremental_filter('event_date')` → `event_date >= MAX - 1 DAY`

**Shared normalization macros** (`macros/cdp_normalize.sql`):
- `normalize_platform()` → ios|android|web|unknown
- `normalize_country()` → uppercase 2-letter or 'unknown'
- `normalize_id()` → empty/null → 'unknown'

### `dbt_cdp_gold` — Gold transform

**Path:** `com/tm/src/services/analytics-aggregator/cdp/dbt_gold/`

**Models:** `gold_user_daily`, `gold_page_daily`, `gold_search_daily`

Tất cả dùng `materialized = 'table'` (full rebuild mỗi 3 phút).

---

## Airflow DAG: `cdp_medallion_pipeline`

**File:** `com/tm/src/services/analytics-aggregator/cdp/dags/cdp_pipeline_dag.py`

**Schedule:** `*/3 * * * *`

DAG tự động đọc `event_registry.yml` tại parse time — không cần sửa DAG khi thêm source mới.

**Task flow:**

```
check_bronze_freshness
        │
  ┌─────┬──────┬─────────┬────────┐
  ▼     ▼      ▼         ▼        ▼
login  view  click   payment  search
layer  layer  layer    layer    layer
```

Mỗi layer:
```
silver.run → silver.test → [gold.run → gold.test]  (gold nếu có model)
```

| Source | Gold model |
|--------|-----------|
| login | `gold_user_daily` ✓ |
| view | `gold_page_daily` ✓ |
| click | — |
| payment | — |
| search | `gold_search_daily` ✓ |

---

## Verify

```bash
# MinIO — kiểm tra Iceberg files tất cả sources
open http://localhost:9011
# Navigate: cdp-lake/iceberg/bronze/{login,view,click,payment,search}_events/

# Flink — kiểm tra 5 running jobs
open http://localhost:8085

# Bronze row counts
docker exec cdp-starrocks mysql -h 127.0.0.1 -P 9030 -u root -e "
  SELECT 'login'   AS src, COUNT(*) AS cnt FROM iceberg_catalog.bronze.login_events
  UNION ALL
  SELECT 'view'    AS src, COUNT(*) FROM iceberg_catalog.bronze.view_events
  UNION ALL
  SELECT 'click'   AS src, COUNT(*) FROM iceberg_catalog.bronze.click_events
  UNION ALL
  SELECT 'payment' AS src, COUNT(*) FROM iceberg_catalog.bronze.payment_events
  UNION ALL
  SELECT 'search'  AS src, COUNT(*) FROM iceberg_catalog.bronze.search_events;"

# Silver samples
docker exec cdp-starrocks mysql -h 127.0.0.1 -P 9030 -u root -e \
  "SELECT event_date, platform, country, COUNT(*) cnt FROM cdp.silver_login
   GROUP BY 1,2,3 ORDER BY 1 DESC LIMIT 10;"

docker exec cdp-starrocks mysql -h 127.0.0.1 -P 9030 -u root -e \
  "SELECT event_date, COUNT(*) total_views,
          SUM(referrer IS NULL) direct FROM cdp.silver_view
   GROUP BY 1 ORDER BY 1 DESC LIMIT 7;"

docker exec cdp-starrocks mysql -h 127.0.0.1 -P 9030 -u root -e \
  "SELECT event_date, payment_method, status, COUNT(*) cnt
   FROM cdp.silver_payment GROUP BY 1,2,3 ORDER BY 1 DESC LIMIT 10;"

docker exec cdp-starrocks mysql -h 127.0.0.1 -P 9030 -u root -e \
  "SELECT event_date, SUM(has_results=0) zero_results, COUNT(*) total
   FROM cdp.silver_search GROUP BY 1 ORDER BY 1 DESC LIMIT 7;"

# Gold metrics
docker exec cdp-starrocks mysql -h 127.0.0.1 -P 9030 -u root -e \
  "SELECT * FROM cdp.gold_user_daily ORDER BY event_date DESC LIMIT 7;"

docker exec cdp-starrocks mysql -h 127.0.0.1 -P 9030 -u root -e \
  "SELECT * FROM cdp.gold_page_daily ORDER BY event_date DESC LIMIT 7;"

docker exec cdp-starrocks mysql -h 127.0.0.1 -P 9030 -u root -e \
  "SELECT * FROM cdp.gold_search_daily ORDER BY event_date DESC LIMIT 7;"

# Airflow DAG
open http://localhost:8086   # DAG: cdp_medallion_pipeline
```
