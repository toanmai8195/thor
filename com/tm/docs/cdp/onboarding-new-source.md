# Onboarding — Tích hợp event source mới vào CDP DW

Tài liệu này dành cho **team tích hợp** khi muốn đưa một event type mới vào CDP Data Warehouse.

Thời gian ước tính: **2–4 giờ** cho một source mới (phần lớn là viết silver SQL + unit tests).

---

## Tổng quan: 5 việc cần làm

```
1. Khai báo registry          event_registry.yml          ~15 phút
2. Submit Flink job            submit_from_registry.sh     ~5 phút (script)
3. Khai báo bronze table       sources.yml                 ~10 phút
4. Viết silver transform       silver_<source>.sql         ~1–2 giờ
5. (Optional) Viết gold model  gold_<source>_daily.sql     ~1 giờ
```

Airflow DAG **tự động pick up** source mới sau khi restart — không cần sửa DAG.

---

## Bước 1 — Khai báo registry

File: `com/tm/docker/cdp/registry/event_registry.yml`

Thêm entry theo template sau:

```yaml
- source_id: <tên ngắn, snake_case>    # ví dụ: cart, wishlist, notification
  description: "<mô tả ngắn>"
  kafka_topic: <source>-events-ingest  # topic SAU cdp-ingestor (đã validated)
  flink_consumer_group: flink-iceberg-cdp-<source>
  ch_consumer_group: ch-cdp-<source>   # dùng cho realtime CH path (nếu có)
  iceberg_database: bronze
  iceberg_table: <source>_events       # ví dụ: cart_events
  sr_silver_model: silver_<source>     # tên dbt model sẽ tạo ở Bước 4
  sr_gold_model: gold_<source>_daily   # null nếu chưa có gold
  partition_field: event_date          # hầu hết dùng event_date
  enabled: true
  fields:                              # schema của event JSON từ Kafka
    - {name: event_id,   type: STRING, comment: "UUID v4 — bắt buộc"}
    - {name: user_id,    type: BIGINT, comment: "User ID — bắt buộc"}
    - {name: session_id, type: STRING, comment: "Session UUID"}
    - {name: platform,   type: STRING, comment: "ios|android|web|unknown"}
    - {name: country,    type: STRING, comment: "Country code"}
    - {name: event_date, type: STRING, comment: "YYYY-MM-DD — bắt buộc"}
    - {name: <ts_field>, type: STRING, comment: "ISO8601 timestamp"}
    # ... thêm fields riêng của source ...
```

**Quy tắc đặt tên:**

| Thành phần | Convention | Ví dụ |
|-----------|------------|-------|
| `source_id` | snake_case, động từ hoặc danh từ | `cart`, `wishlist`, `notification` |
| `kafka_topic` | `<source>-events-ingest` | `cart-events-ingest` |
| `iceberg_table` | `<source>_events` | `cart_events` |
| `sr_silver_model` | `silver_<source>` | `silver_cart` |
| `sr_gold_model` | `gold_<source>_daily` | `gold_cart_daily` |

**Field types hỗ trợ:** `STRING`, `BIGINT`, `DOUBLE`, `BOOLEAN`
(Flink template truyền tất cả qua raw — type casting thực tế xảy ra ở silver SQL)

---

## Bước 2 — Submit Flink job

```bash
# Chạy trên host machine (máy local), KHÔNG phải trong container
cd com/tm/docker/cdp

# Yêu cầu:
#   - python3 + pyyaml trên host: pip install pyyaml
#   - Docker daemon đang chạy
#   - Container cdp-flink-jobmanager đang up
./flink/submit_from_registry.sh <source_id>
```

Script này tự động:
1. Đọc `event_registry.yml`, lấy fields + config của source
2. Generate Flink SQL từ `flink/templates/kafka_to_iceberg.sql.tmpl`
3. `docker cp` vào Flink container
4. Submit qua `sql-client.sh`

Kết quả: Flink job chạy liên tục, đọc từ `<source>-events-ingest`, ghi vào `iceberg_catalog.bronze.<source>_events`.

Verify:
```bash
# Flink Web UI
open http://localhost:8085
# Tìm job tên "cdp_<source>_ingest" → trạng thái RUNNING
```

**Lưu ý:** Nếu topic `<source>-events-ingest` chưa có message, Flink job vẫn RUNNING nhưng metrics = 0. Bình thường.

---

## Bước 3 — Khai báo bronze table cho dbt

File: `com/tm/src/services/analytics-aggregator/cdp/dbt_silver/models/sources.yml`

Thêm vào section `tables:` của source `bronze`:

```yaml
      - name: <source>_events          # khớp với iceberg_table trong registry
        description: "Raw <source> events from Flink"
        columns:
          - name: event_id
            tests: [not_null]
          - name: user_id
            tests: [not_null]
          - name: event_date
            tests: [not_null]
          # thêm các field bắt buộc của source này:
          - name: <required_field>
            tests: [not_null]
```

Chỉ cần khai báo các **field bắt buộc** (sẽ fail pipeline nếu null). Field optional không cần liệt kê.

---

## Bước 4 — Viết silver transform (phần tốn thời gian nhất)

File: `com/tm/src/services/analytics-aggregator/cdp/dbt_silver/models/silver/silver_<source>.sql`

### Template chuẩn

```sql
-- =============================================================================
-- SILVER: cdp.silver_<source>
-- =============================================================================
-- Source-specific fields: <liệt kê fields riêng>
-- Common normalization: platform, country, session_id via macros
-- =============================================================================

{{ config(materialized='incremental', on_schema_change='ignore') }}

SELECT
    CAST(event_id AS VARCHAR(64))        AS event_id,
    event_date,
    user_id,
    {{ normalize_id('session_id') }}     AS session_id,

    -- [COMMON] Platform + country — dùng macro, không cần viết lại
    {{ normalize_platform('platform') }} AS platform,
    {{ normalize_country('country') }}   AS country,

    -- [SOURCE-SPECIFIC] Thêm fields riêng của source ở đây
    -- ...

    COALESCE(<ts_field>, ingested_at)    AS <ts_field>

FROM {{ source('bronze', '<source>_events') }}

WHERE
    -- [COMMON] Bắt buộc cho tất cả sources
    event_id   IS NOT NULL AND event_id <> ''
    AND user_id IS NOT NULL AND user_id > 0
    AND event_date IS NOT NULL

    -- [SOURCE-SPECIFIC] Thêm điều kiện bắt buộc của source này
    -- AND <required_field> IS NOT NULL AND TRIM(<required_field>) <> ''

{% if is_incremental() %}
    AND {{ silver_incremental_filter('event_date') }}
{% endif %}
```

### Macros có sẵn (dùng trực tiếp, không cần import)

| Macro | Dùng khi | Output |
|-------|----------|--------|
| `{{ normalize_platform('col') }}` | Field chứa platform | `ios`/`android`/`web`/`unknown` |
| `{{ normalize_country('col') }}` | Field chứa country code | `VN`/`US`/`SG`.../`unknown` |
| `{{ normalize_id('col') }}` | ID field có thể empty/null | giá trị hoặc `'unknown'` |
| `{{ silver_incremental_filter('event_date') }}` | Luôn dùng trong `{% if is_incremental() %}` | `WHERE event_date >= MAX-1d` |

### Quyết định quan trọng khi viết silver

**1. Field bắt buộc vs optional:**
- Field bắt buộc (không có → drop row): thêm vào `WHERE` clause
- Field optional (thiếu thì fallback): dùng `COALESCE` hoặc `CASE WHEN`

**2. Empty string → NULL hay 'unknown'?**

| Trường hợp | Xử lý | Ví dụ |
|-----------|-------|-------|
| ID field (session_id, device_id) | empty → `'unknown'` | `normalize_id()` |
| URL/referrer (nullable theo business) | empty → `NULL` | `CASE WHEN TRIM(x)='' THEN NULL` |
| Enum field (status, type) | invalid → default value | `CASE WHEN ... ELSE 'other'` |

**3. Derived fields:**
Tính toán từ fields khác ngay trong silver:
```sql
-- Ví dụ: has_results derived từ result_count
result_count > 0 AS has_results
```
Gold model sẽ aggregate được ngay mà không cần tính lại.

**4. Field không có trong source này:**
Một số sources không có `device_id` (ví dụ: payment là transaction-level, search là intent-level).
→ Bỏ luôn khỏi silver model, không cần gán NULL.

### Ví dụ tham khảo

| Silver model | Điểm đặc biệt |
|-------------|--------------|
| [silver_login.sql](../../../src/services/analytics-aggregator/cdp/dbt_silver/models/silver/silver_login.sql) | `login_at` fallback sang `ingested_at` |
| [silver_view.sql](../../../src/services/analytics-aggregator/cdp/dbt_silver/models/silver/silver_view.sql) | `referrer` empty → NULL (direct traffic) |
| [silver_click.sql](../../../src/services/analytics-aggregator/cdp/dbt_silver/models/silver/silver_click.sql) | `element_type` whitelist: button/link/image/other |
| [silver_payment.sql](../../../src/services/analytics-aggregator/cdp/dbt_silver/models/silver/silver_payment.sql) | `currency` default VND, `amount > 0` filter, không có `device_id` |
| [silver_search.sql](../../../src/services/analytics-aggregator/cdp/dbt_silver/models/silver/silver_search.sql) | `has_results` derived, `clicked_result_id` nullable, không có `device_id` |

---

## Bước 5 (Optional) — Viết gold model

Chỉ cần nếu source có **aggregate metrics** hữu ích cho BI/dashboard.

File: `com/tm/src/services/analytics-aggregator/cdp/dbt_gold/models/gold/gold_<source>_daily.sql`

```sql
{{ config(materialized='table') }}

SELECT
    event_date,
    COUNT(*)                AS total_<events>,
    COUNT(DISTINCT user_id) AS unique_users,
    -- ... metrics riêng của source ...
    NOW() AS dbt_updated_at

FROM {{ source('silver', 'silver_<source>') }}
GROUP BY event_date
ORDER BY event_date DESC
```

Thêm vào `dbt_gold/models/sources.yml`:
```yaml
      - name: silver_<source>
        description: "..."
        columns:
          - name: event_id
            tests: [not_null]
```

Đăng ký gold model vào registry (`sr_gold_model: gold_<source>_daily`).

---

## Unit tests (Khuyến khích)

Thêm unit tests vào `dbt_silver/models/silver/schema.yml` để verify logic source-specific.
Không cần test lại normalize_platform/country (đã test ở silver_login).

**Chỉ test logic riêng của source:**

```yaml
- name: test_<source>_<tên_logic>
  model: silver_<source>
  given:
    - input: source('bronze', '<source>_events')
      rows:
        - {event_id: "e1", user_id: 1, ..., <field>: "<input_value>", event_date: "2024-01-01", <ts>: "...", ingested_at: "..."}
  expect:
    rows:
      - {event_id: "e1", <field>: "<expected_output>"}
```

---

## Checklist trước khi deploy

```
[ ] event_registry.yml — entry đã thêm, enabled: true
[ ] Flink job đang RUNNING trên http://localhost:8085
[ ] sources.yml — bronze table đã khai báo
[ ] silver_<source>.sql — đã tạo, dbt compile không lỗi
[ ] Airflow đã restart và thấy TaskGroup mới trong cdp_medallion_pipeline
[ ] Có ít nhất 1 DAG run success sau khi có data vào bronze
[ ] (Optional) gold_<source>_daily.sql — nếu cần
[ ] (Optional) Unit tests trong schema.yml
```

---

## Xử lý sự cố thường gặp

**Flink job fail ngay sau submit:**
```bash
docker logs cdp-flink-login-job 2>&1 | tail -30
# Thường là: bucket chưa tạo, Kafka topic chưa có, MinIO chưa sẵn sàng
```

**dbt run lỗi "table not found":**
- Bronze table chưa có data → Flink chưa commit (cần chờ 60s sau khi có events)
- `sources.yml` sai tên table → kiểm tra `iceberg_table` trong registry

**DAG không thấy source mới:**
```bash
docker compose -f com/tm/docker/cdp/docker-compose.yml restart airflow-webserver airflow-scheduler
# Airflow cần restart để parse lại DAG file và đọc registry.yml
```

**StarRocks lỗi "Unknown catalog 'iceberg_catalog'":**
```bash
# Chạy init SQL nếu healthcheck chưa trigger
docker exec cdp-starrocks mysql -h 127.0.0.1 -P 9030 -u root \
  < com/tm/docker/cdp/starrocks/init/00_cdp_schema.sql
```
