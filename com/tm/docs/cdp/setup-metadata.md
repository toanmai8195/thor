# CDP Metadata Layer — Setup & Hướng dẫn thêm source mới

Bước 2 của CDP roadmap. Sau khi setup, thêm event source mới chỉ cần **3 bước** thay vì copy toàn bộ pipeline.

---

## Các file của Metadata Layer

```
com/tm/docker/cdp/
└── registry/
    └── event_registry.yml              ← Source of truth — khai báo tất cả sources
└── flink/
    ├── templates/
    │   └── kafka_to_iceberg.sql.tmpl   ← Generic Flink SQL template
    └── submit_from_registry.sh         ← Generate + submit Flink job

com/tm/src/services/analytics-aggregator/cdp/
├── dbt_silver/macros/cdp_normalize.sql   ← Macros dùng chung (StarRocks)
├── dbt_realtime/macros/cdp_normalize.sql ← Macros dùng chung (ClickHouse)
└── dags/cdp_pipeline_dag.py              ← Registry-driven DAG
```

---

## Xem sources đang active

```bash
# List tất cả sources trong registry
cd com/tm/docker/cdp
./flink/submit_from_registry.sh

# Query trực tiếp StarRocks
docker exec cdp-starrocks mysql -h 127.0.0.1 -P 9030 -u root \
  -e "SELECT source_id, enabled, sr_silver_model, sr_gold_model FROM cdp.event_registry;"
```

---

## Thêm event source mới

### Bước 1 — Khai báo trong registry

Thêm entry vào `com/tm/docker/cdp/registry/event_registry.yml`:

```yaml
- source_id: view
  description: "Page view events"
  kafka_topic: view-events-ingest
  flink_consumer_group: flink-iceberg-cdp-view
  ch_consumer_group: ch-cdp-view
  iceberg_database: bronze
  iceberg_table: view_events
  sr_silver_model: silver_view
  sr_gold_model: null              # bỏ qua nếu chưa có gold
  partition_field: event_date
  enabled: true
  fields:
    - {name: event_id,   type: STRING}
    - {name: user_id,    type: BIGINT}
    - {name: session_id, type: STRING}
    - {name: page_url,   type: STRING}
    - {name: referrer,   type: STRING}
    - {name: event_date, type: STRING}
    - {name: viewed_at,  type: STRING}
```

### Bước 2 — Submit Flink job

Script tự generate SQL từ template + submit vào Flink:

```bash
cd com/tm/docker/cdp

# Cần: python3 + pyyaml, Docker container cdp-flink-jobmanager đang chạy
pip install pyyaml   # nếu chưa có

./flink/submit_from_registry.sh view
# Output:
# ==> Generating Flink SQL for source: view
# Generated: /tmp/cdp_flink_view.sql
# ==> Copying SQL to Flink JobManager container
# ==> Submitting job to Flink SQL Client
# ==> Done. Check Flink UI: http://localhost:8085
```

Verify job đang chạy:
```bash
# Flink Web UI
open http://localhost:8085

# Hoặc check qua REST API
curl -s http://localhost:8085/jobs | python3 -m json.tool
```

### Bước 3 — Tạo dbt silver model

Tạo `com/tm/src/services/analytics-aggregator/cdp/dbt_silver/models/silver/silver_view.sql`:

```sql
{{ config(materialized='incremental', on_schema_change='ignore') }}

SELECT
    CAST(event_id AS VARCHAR(64))        AS event_id,
    event_date,
    user_id,
    {{ normalize_id('session_id') }}     AS session_id,
    {{ normalize_country('country') }}   AS country,   -- nếu có
    CAST(page_url AS VARCHAR(500))       AS page_url,
    CAST(referrer AS VARCHAR(500))       AS referrer,
    COALESCE(viewed_at, ingested_at)     AS viewed_at

FROM {{ source('bronze', 'view_events') }}   -- phải khớp iceberg_table trong registry

WHERE
    event_id IS NOT NULL AND event_id <> ''
    AND user_id IS NOT NULL AND user_id > 0

{% if is_incremental() %}
    AND {{ silver_incremental_filter('event_date') }}
{% endif %}
```

Đồng thời thêm vào `sources.yml`:
```yaml
- name: view_events
  description: Page view events from Flink
```

### Bước 4 — Airflow tự pick up

Không cần sửa DAG. Restart để parse lại registry:

```bash
cd com/tm/docker/cdp
docker compose restart airflow-webserver airflow-scheduler
```

Vào Airflow UI (http://localhost:8086) → `cdp_medallion_pipeline` → thấy TaskGroup `view_layer` mới.

---

## Macros có sẵn

Dùng trong bất kỳ silver model nào, không cần import.

### dbt_cdp_silver (StarRocks)

| Macro | Input | Output |
|-------|-------|--------|
| `{{ normalize_platform('col') }}` | `'IOS'`, `'desktop'`, `''` | `'ios'`, `'unknown'`, `'unknown'` |
| `{{ normalize_country('col') }}` | `'vn'`, `''`, `null` | `'VN'`, `'unknown'`, `'unknown'` |
| `{{ normalize_id('col') }}` | `''`, `null`, `'abc'` | `'unknown'`, `'unknown'`, `'abc'` |
| `{{ silver_incremental_filter('event_date') }}` | — | `WHERE event_date >= DATE_SUB(MAX(...), 1 DAY)` |

### dbt_cdp_realtime (ClickHouse)

Cùng tên macro, ClickHouse syntax. ClickHouse String không có null nên `normalize_id` chỉ check empty string.

---

## Disable source tạm thời

Không cần xóa — set `enabled: false` trong registry:

```yaml
- source_id: view
  enabled: false   # DAG bỏ qua, Flink job vẫn chạy (dừng Flink job riêng nếu cần)
```

Restart Airflow scheduler để apply.

---

## Cấu trúc Flink SQL được generate

`submit_from_registry.sh` generate SQL từ `templates/kafka_to_iceberg.sql.tmpl`:

```
registry fields    →  __FIELD_DEFINITIONS__  →  CREATE TABLE schema
registry topic     →  __KAFKA_TOPIC__        →  Kafka connector config
registry iceberg   →  __ICEBERG_TABLE__      →  Iceberg sink table
```

File generate lưu tại `/tmp/cdp_flink_<source_id>.sql` trước khi submit — có thể inspect để debug:

```bash
./flink/submit_from_registry.sh view
cat /tmp/cdp_flink_view.sql
```
