# Luồng 2: DW Pipeline (Iceberg + StarRocks)

Pipeline xử lý login events theo **batch** với exactly-once semantics, phục vụ analytics và Gold aggregations.

---

## Kiến trúc

```
Kafka: login-events-ingest
        │ consumer group: flink-iceberg-cdp-login
        ▼
Apache Flink (exactly-once, checkpoint 60s)
        │ Parquet files
        ▼
Iceberg: bronze/login_events  (MinIO, format-version=2)
        │ StarRocks external catalog (iceberg_catalog)
        ▼
iceberg_catalog.bronze.login_events   ← Bronze (external)
        │ Airflow: cdp_medallion_pipeline (*/3 * * * *)
        │ dbt-starrocks silver_login (UPSERT)
        ▼
cdp.silver_login  (StarRocks PRIMARY KEY)
        │ dbt-starrocks gold_user_daily (full table rebuild)
        ▼
cdp.gold_user_daily  (StarRocks aggregate)
        │
    Superset (starrocks+pymysql://root:@starrocks:9030/cdp)
```

---

## Flink Job

**File:** `com/tm/docker/cdp/flink/kafka_login_to_iceberg.sql`

**Consumer group:** `flink-iceberg-cdp-login`

**Checkpoint:** 60 giây (exactly-once, barrier-based)

**Catalog:** HadoopCatalog trên MinIO (`s3a://cdp-lake/iceberg`)

**Sink config:**
```sql
'write.format.default' = 'parquet',
'format-version' = '2',
'write.parquet.compression-codec' = 'snappy',
'write.target-file-size-bytes' = '134217728'  -- 128 MB per file
```

**login_at parse:**
```sql
TO_TIMESTAMP(
    LEFT(REPLACE(login_at, 'Z', ''), 23),
    'yyyy-MM-dd''T''HH:mm:ss.SSS'
) AS login_at
```
Lý do: Flink SQL không parse ISO8601 với 'Z' suffix trực tiếp — phải strip 'Z' và truncate ms.

---

## Schema

### `iceberg_catalog.bronze.login_events` — Bronze (Iceberg external)

| Column | Type | Ghi chú |
|--------|------|---------|
| `event_id` | STRING | |
| `user_id` | BIGINT | |
| `session_id` | STRING | |
| `device_id` | STRING | |
| `platform` | STRING | Raw |
| `country` | STRING | Raw |
| `event_date` | DATE | Partition column |
| `login_at` | TIMESTAMP(3) | Parse từ ISO8601 |

- **Format:** Parquet, Iceberg format-version 2
- **Partition:** `event_date`
- **Storage:** MinIO bucket `cdp-lake`, path `s3a://cdp-lake/iceberg/bronze/login_events/`
- **Flink commit interval:** 60s checkpoint → file commit sau mỗi checkpoint

### `cdp.silver_login` — Silver (StarRocks PRIMARY KEY)

| Column | Type | Ghi chú |
|--------|------|---------|
| `event_id` | VARCHAR(64) | PRIMARY KEY (1) |
| `event_date` | DATE | PRIMARY KEY (2), partition key |
| `user_id` | BIGINT | |
| `session_id` | VARCHAR(128) | empty → `'unknown'` |
| `device_id` | VARCHAR(128) | empty → `'unknown'` |
| `platform` | VARCHAR(20) | ios/android/web/unknown |
| `country` | CHAR(2) | uppercase, empty → `'unknown'` |
| `login_at` | DATETIME | |
| `dbt_updated_at` | DATETIME | `NOW()` lúc dbt chạy |

- **Engine:** PRIMARY KEY (`event_id`, `event_date`)
- **Strategy:** UPSERT (dbt incremental `unique_key = ['event_id', 'event_date']`)
- **Partition:** RANGE by month (`event_date`), dynamic partition auto-create
- **Bucket:** HASH(`event_id`) 8 buckets

### `cdp.gold_user_daily` — Gold (StarRocks aggregate)

Grain = 1 row per ngày. Full table rebuild mỗi 3 phút.

| Column | Type | Mô tả |
|--------|------|-------|
| `event_date` | DATE | Ngày (PRIMARY KEY) |
| `dau` | BIGINT | Daily Active Users (`COUNT DISTINCT user_id`) |
| `total_logins` | BIGINT | Tổng login events |
| `ios_logins` | BIGINT | Platform = ios |
| `android_logins` | BIGINT | Platform = android |
| `web_logins` | BIGINT | Platform = web |
| `unknown_logins` | BIGINT | Platform = unknown |
| `unique_sessions` | BIGINT | `COUNT DISTINCT session_id` |
| `unique_devices` | BIGINT | `COUNT DISTINCT device_id` |
| `vn_users` | BIGINT | Country = VN |
| `us_users` | BIGINT | Country = US |
| `sg_users` | BIGINT | Country = SG |
| `other_users` | BIGINT | Country khác |
| `dbt_updated_at` | DATETIME | Thời gian dbt rebuild |

---

## dbt Projects

### `dbt_cdp_silver` — Silver transform

**Path:** `com/tm/src/services/analytics-aggregator/cdp/dbt_silver/`

**Profile:** `cdp_silver` — kết nối StarRocks `cdp` database (port 9030)

**Model `silver_login`:**

```sql
{{ config(
    materialized = 'incremental',
    unique_key = ['event_id', 'event_date'],
    incremental_strategy = 'merge'
) }}

SELECT
    event_id, event_date, user_id,
    NULLIF(TRIM(session_id), '') AS session_id,    -- null nếu empty
    NULLIF(TRIM(device_id),  '') AS device_id,
    CASE WHEN LOWER(TRIM(platform)) IN ('ios','android','web')
         THEN LOWER(TRIM(platform)) ELSE 'unknown' END AS platform,
    CASE WHEN TRIM(country) = '' THEN 'unknown'
         ELSE UPPER(TRIM(country)) END AS country,
    login_at, NOW() AS dbt_updated_at
FROM {{ source('bronze', 'login_events') }}
WHERE event_id IS NOT NULL AND user_id IS NOT NULL
{% if is_incremental() %}
    AND event_date >= DATE_SUB(
        (SELECT MAX(event_date) FROM {{ this }}),
        INTERVAL 1 DAY
    )
{% endif %}
```

**Tests:** `not_null` + `unique` trên `event_id`, `not_null` trên `user_id`/`event_date`/`platform`

### `dbt_cdp_gold` — Gold transform

**Path:** `com/tm/src/services/analytics-aggregator/cdp/dbt_gold/`

**Profile:** `cdp_gold` — kết nối StarRocks `cdp` database

**Model `gold_user_daily`:**

```sql
{{ config(materialized = 'table') }}  -- full rebuild

SELECT
    event_date,
    COUNT(DISTINCT user_id) AS dau,
    COUNT(*) AS total_logins,
    SUM(platform = 'ios')     AS ios_logins,
    SUM(platform = 'android') AS android_logins,
    SUM(platform = 'web')     AS web_logins,
    SUM(platform = 'unknown') AS unknown_logins,
    COUNT(DISTINCT session_id) AS unique_sessions,
    COUNT(DISTINCT device_id)  AS unique_devices,
    SUM(country = 'VN') AS vn_users,
    SUM(country = 'US') AS us_users,
    SUM(country = 'SG') AS sg_users,
    SUM(country NOT IN ('VN','US','SG')) AS other_users,
    NOW() AS dbt_updated_at
FROM {{ ref('silver_login') }}
GROUP BY event_date
```

---

## Airflow DAG: `cdp_medallion_pipeline`

**File:** `com/tm/src/services/analytics-aggregator/cdp/dags/cdp_pipeline_dag.py`

**Schedule:** `*/3 * * * *` (mỗi 3 phút — Flink commit mỗi 60s, đủ thời gian có file mới)

| Task | Mô tả |
|------|-------|
| `check_bronze_freshness` | `SELECT COUNT(*) FROM iceberg_catalog.bronze.login_events` qua mysql — fail nếu = 0 |
| `silver_layer.dbt_silver_run` | `dbt run --select silver_login` |
| `silver_layer.dbt_silver_test` | `dbt test --select silver_login` |
| `gold_layer.dbt_gold_run` | `dbt run` (gold_user_daily full rebuild) |
| `gold_layer.dbt_gold_test` | `dbt test` |

```
check_bronze_freshness
        │
  [silver_layer]
  dbt_silver_run → dbt_silver_test
        │
   [gold_layer]
  dbt_gold_run → dbt_gold_test
```

**Environment trong container Airflow:**

| Biến | Default |
|------|---------|
| `STARROCKS_HOST` | `starrocks` |
| `STARROCKS_MYSQL_PORT` | `9030` |

---

## Verify

```bash
# MinIO — kiểm tra Iceberg files
open http://localhost:9011
# Navigate: cdp-lake/iceberg/bronze/login_events/

# Flink — kiểm tra running job
open http://localhost:8085

# Bronze rows (qua StarRocks catalog)
mysql -h 127.0.0.1 -P 9031 -u root -e \
  "SELECT COUNT(*) FROM iceberg_catalog.bronze.login_events;"

# Silver sample
mysql -h 127.0.0.1 -P 9031 -u root -e \
  "SELECT event_date, platform, country, COUNT(*) as cnt
   FROM cdp.silver_login
   GROUP BY event_date, platform, country
   ORDER BY event_date DESC LIMIT 20;"

# Gold — daily metrics
mysql -h 127.0.0.1 -P 9031 -u root -e \
  "SELECT * FROM cdp.gold_user_daily ORDER BY event_date DESC LIMIT 7;"

# Airflow DAG
open http://localhost:8086   # DAG: cdp_medallion_pipeline
```
