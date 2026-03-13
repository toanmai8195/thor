# CDP DW — Roadmap

Xây dựng Data Warehouse cho **Customer Data Platform (CDP)** theo Medallion Architecture.

Dual-pipeline: **Realtime** (ClickHouse) + **DW** (Iceberg/StarRocks) song song từ cùng Kafka topic.

`Kafka → [CH Kafka Engine → ClickHouse silver (1min)] || [Flink → Iceberg → StarRocks silver+gold (3min)]`

---

## Tổng quan 7 bước

| Bước | Tên | Status | Ghi chú |
|------|-----|--------|---------|
| 1 | [Login Event (first source)](#bước-1-login-event) | ✅ Done | Full flow, 1 event type |
| 2 | [Metadata Layer](#bước-2-metadata-layer) | ✅ Done | Registry tự động hoá multi-source |
| 3 | [Multi Data Sources](#bước-3-multi-data-sources) | ⬜ Planned | View, click, payment, search |
| 4 | [Identity Resolution](#bước-4-identity-resolution) | ⬜ Planned | Silver layer — anonymous_id → user_id |
| 5 | [Unified User Profile (360°)](#bước-5-unified-user-profile) | ⬜ Planned | Core CDP output — 1 row/user |
| 6 | [User Segmentation](#bước-6-user-segmentation) | ⬜ Planned | Dynamic segments |
| 7 | [Data Activation / Export](#bước-7-data-activation) | ⬜ Planned | Kafka output + downstream |

---

## Bước 1: Login Event

**Mục tiêu:** Validate toàn bộ dual-pipeline với 1 event source — bao gồm cả Realtime (ClickHouse) và DW (Iceberg/StarRocks).

### Dual-Pipeline Architecture

```
Kafka: login-events-ingest
        │
  ┌─────┴────────────────────────────┐
  │ group: ch-cdp-login              │ group: flink-iceberg-cdp-login
  ▼                                  ▼
LUỒNG 1: REALTIME               LUỒNG 2: DW
ClickHouse Kafka Engine         Apache Flink (checkpoint 60s)
cdp.login_bronze                Iceberg: bronze/login_events
dbt (1min, append)              dbt-starrocks (3min, UPSERT)
cdp.silver_login (CH)           cdp.silver_login (SR)
                                cdp.gold_user_daily
```

### Login Event Schema

```json
{
  "event_id":   "uuid-v4",
  "user_id":    12345,
  "session_id": "sess-abc123",
  "device_id":  "dev-xyz789",
  "platform":   "ios",
  "country":    "VN",
  "event_date": "2025-01-01",
  "login_at":   "2025-01-01T10:00:00.000Z"
}
```

Producer: ~500 events/s, 100k unique users, platform distribution ios(30%)/android(40%)/web(20%)/unknown(10%).

### Services & Files

| Thành phần | Path |
|-----------|------|
| Go producer | `com/tm/src/services/login-event-producer/` |
| Go ingestor | `com/tm/src/services/cdp-ingestor/` |
| Docker stack | `com/tm/docker/cdp/` |
| dbt Realtime | `com/tm/src/services/analytics-aggregator/cdp/dbt_realtime/` |
| dbt Silver | `com/tm/src/services/analytics-aggregator/cdp/dbt_silver/` |
| dbt Gold | `com/tm/src/services/analytics-aggregator/cdp/dbt_gold/` |
| Airflow DAGs | `com/tm/src/services/analytics-aggregator/cdp/dags/` |

### Tài liệu chi tiết

| Doc | Nội dung |
|-----|---------|
| [setup.md](setup.md) | Overview + shared infra (Kafka, Go images) |
| [setup-realtime.md](setup-realtime.md) | Setup step-by-step Luồng 1 + troubleshooting |
| [setup-dw.md](setup-dw.md) | Setup step-by-step Luồng 2 + troubleshooting |
| [pipeline-realtime.md](pipeline-realtime.md) | Kiến trúc, schema, dbt config Luồng 1 |
| [pipeline-dw.md](pipeline-dw.md) | Kiến trúc, schema, Flink, dbt config Luồng 2 |

---

## Bước 2: Metadata Layer

**Mục tiêu:** Không cần sửa Flink SQL / Airflow DAG khi thêm event source mới.

**Vấn đề của Bước 1:** Mỗi event source = copy toàn bộ Flink SQL + dbt boilerplate + DAG task.

### Giải pháp — 4 thành phần

| Thành phần | File | Chức năng |
|-----------|------|-----------|
| Event Registry | `docker/cdp/registry/event_registry.yml` | Source of truth — 1 nơi khai báo tất cả sources |
| Flink template | `docker/cdp/flink/templates/kafka_to_iceberg.sql.tmpl` | Generic Flink SQL, tham số hoá từ registry |
| dbt macros | `dbt_silver/macros/cdp_normalize.sql` | Hàm normalize tái sử dụng — bỏ copy-paste |
| Registry-driven DAG | `dags/cdp_pipeline_dag.py` | Tự tạo TaskGroup per source từ registry |

### Workflow thêm source mới (sau Bước 2)

```bash
# 1. Thêm entry vào event_registry.yml (khai báo topic, tables, model names)
vim docker/cdp/registry/event_registry.yml

# 2. Submit Flink job — generate SQL từ template + submit
./docker/cdp/flink/submit_from_registry.sh view

# 3. Tạo dbt silver model cho logic normalize riêng của source
# (dùng macros có sẵn — không cần viết lại normalize_platform/country)
vim src/services/analytics-aggregator/cdp/dbt_silver/models/silver/silver_view.sql

# 4. Restart Airflow → DAG tự pick up source mới, không sửa DAG
docker compose restart airflow-webserver airflow-scheduler
```

### dbt macros (StarRocks + ClickHouse)

Cùng interface, khác DB syntax:

```sql
-- Dùng trong silver model thay vì copy-paste CASE expression
{{ normalize_platform('platform') }}   -- ios/android/web/unknown
{{ normalize_country('country') }}     -- VN/US/SG.../unknown
{{ normalize_id('session_id') }}       -- empty/null → 'unknown'
{{ silver_incremental_filter() }}      -- WHERE event_date >= MAX-1d
```

### cdp.event_registry (StarRocks table)

```sql
source_id            VARCHAR(50)  PK   -- login | view | click | payment | search
kafka_topic          VARCHAR(100)      -- login-events-ingest
iceberg_database     VARCHAR(50)       -- bronze
iceberg_table        VARCHAR(100)      -- login_events
sr_silver_model      VARCHAR(100)      -- silver_login (tên dbt model)
sr_gold_model        VARCHAR(100)      -- gold_user_daily (nullable)
flink_consumer_group VARCHAR(100)      -- flink-iceberg-cdp-login
enabled              BOOLEAN           -- FALSE = skip trong pipeline
```

### Tài liệu chi tiết

→ [setup-metadata.md](setup-metadata.md)

---

## Bước 3: Multi Data Sources

**Mục tiêu:** Thêm các event sources còn lại bằng cách khai báo vào registry (Bước 2).

**Event sources cần thêm:**

| Source | Topic | Key Metrics |
|--------|-------|-------------|
| `view` | `view-events` | Page views, bounce rate, session depth |
| `click` | `click-events` | CTR, element interaction heatmap |
| `payment` | `payment-events` (reuse từ Revenue DW) | Conversion, revenue per user |
| `search` | `search-events` | Search queries, zero-result rate |

**Mỗi source cần:**
1. Event schema definition (thêm vào event_registry)
2. Event producer update (hoặc real app events)
3. Bronze Iceberg table (auto-created bởi Flink template)
4. Silver SR table (auto-created bởi dbt macro)
5. Gold SR table (specific per source)

---

## Bước 4: Identity Resolution

**Thuộc DW hay CDP?** → **Thuộc DW (Silver layer)**

**Vấn đề:** Cùng 1 user có thể:
- Login từ nhiều devices (device_id khác nhau)
- Có session trước khi login (anonymous_id → user_id mapping)
- Login bằng nhiều platforms (ios + web + android)

**Giải pháp — Identity Graph trong Silver:**

```
Bronze events: anonymous_id, session_id, device_id, user_id (có thể null trước login)
                    ↓ dbt Silver transform
cdp.identity_graph  ← bảng mapping: identity_key → canonical_user_id
                    ↓
cdp.silver_* tables ← tất cả enriched với canonical_user_id
```

**Schema `cdp.identity_graph`:**

| Column | Mô tả |
|--------|-------|
| `identity_key` | anonymous_id / session_id / device_id |
| `identity_type` | anonymous_id / session / device |
| `canonical_user_id` | user_id đã xác định (từ login event) |
| `first_seen_at` | Lần đầu tiên thấy |
| `last_seen_at` | Lần cuối thấy |
| `confidence` | DECIMAL — mức độ tin cậy của mapping |

**Logic:**
1. Khi có login event: `session_id → user_id` mapping được tạo
2. Pre-login events cùng session_id → retroactively assign user_id
3. Cùng device_id đăng nhập nhiều accounts → track lịch sử

---

## Bước 5: Unified User Profile

**Tại sao cần bước này:**
CDP = "360° single view of each customer". Sau khi có multi-source events + identity resolution,
cần **aggregate tất cả behavioral signals thành 1 row per user** — đây là **core output của CDP**.

**Schema `cdp.user_profiles` (grain = 1 row/user):**

| Column | Type | Nguồn |
|--------|------|-------|
| `user_id` | BIGINT PK | — |
| `first_seen_at` | DATETIME | MIN(login_at) across all events |
| `last_seen_at` | DATETIME | MAX(event_at) across all events |
| `total_sessions` | BIGINT | COUNT DISTINCT session_id |
| `total_logins` | BIGINT | silver_login |
| `total_views` | BIGINT | silver_view |
| `total_clicks` | BIGINT | silver_click |
| `total_purchases` | BIGINT | silver_payment |
| `total_searches` | BIGINT | silver_search |
| `ltv` | DECIMAL | SUM(amount) WHERE payment status='paid' |
| `preferred_platform` | VARCHAR | platform có nhiều sessions nhất |
| `country` | VARCHAR | country phổ biến nhất |
| `is_active_30d` | BOOLEAN | last_seen trong 30 ngày |
| `churn_risk` | VARCHAR | low/medium/high (rule-based) |
| `dbt_updated_at` | DATETIME | — |

**Schedule:** Full rebuild daily (data lớn, query phức tạp).

---

## Bước 6: User Segmentation

**Tại sao cần bước này:**
CDP không chỉ lưu data — cần **phân khúc users để phục vụ personalization, marketing automation, ads targeting**.

**Tables:**

```sql
-- Định nghĩa segment (rule-based)
cdp.segments (
    segment_id   VARCHAR(50) PK,
    name         VARCHAR(100),
    description  TEXT,
    rule_sql     TEXT,          -- SQL WHERE clause để filter từ user_profiles
    is_active    BOOLEAN,
    updated_at   DATETIME
)

-- User × Segment mapping
cdp.user_segments (
    user_id     BIGINT,
    segment_id  VARCHAR(50),
    entered_at  DATETIME,
    exited_at   DATETIME,       -- NULL = vẫn trong segment
    PRIMARY KEY (user_id, segment_id)
)
```

**Ví dụ segments:**

| Segment | Rule |
|---------|------|
| `high_value` | `ltv > 5000000 AND total_purchases >= 3` |
| `churned` | `last_seen_at < NOW() - INTERVAL 30 DAY` |
| `new_users` | `first_seen_at >= NOW() - INTERVAL 7 DAY` |
| `mobile_first` | `preferred_platform IN ('ios', 'android')` |
| `power_users` | `total_sessions >= 50 AND is_active_30d = TRUE` |

**Airflow DAG:** `cdp_segmentation` — daily sau khi `user_profiles` rebuild xong.

---

## Bước 7: Data Activation

**Tại sao cần bước này:**
Segments cần được **export ra downstream systems** để thực sự có giá trị:
push notification, ads targeting, email marketing, A/B testing, feature flags.

**3 activation channels:**

### 1. Direct Query (đơn giản nhất)
Downstream service query thẳng StarRocks qua MySQL protocol:
```sql
SELECT user_id FROM cdp.user_segments
WHERE segment_id = 'high_value' AND exited_at IS NULL;
```

### 2. Kafka Output Topic (event-driven)
Khi segment membership thay đổi (Airflow trigger) → publish ra Kafka:
- Topic: `cdp-segment-updates`
- Schema: `{ user_id, segment_id, action: "entered"|"exited", timestamp }`
- Downstream: push service, ads platform, CRM consume topic này

### 3. Export API (future)
REST endpoint wrapping StarRocks queries — cho 3rd party integrations không thể consume Kafka.

**Note về scope:**
- Channels 1 & 2: thuộc DW layer (dbt + Airflow + Flink output job)
- Channel 3: thuộc CDP application layer (separate service)

---

## Kiến trúc tổng thể (sau Bước 7)

```
[Login] [View] [Click] [Payment] [Search]   ← event producers
        │ Kafka: <source>-events
        ▼
   cdp-ingestor (generic JSON validate)
        │ Kafka: <source>-events-ingest
        ▼
   Apache Flink (per-source job, checkpoint 60s)
        │ Parquet files
        ▼
Iceberg: bronze.login_events / bronze.view_events / ...  (MinIO)
        │ StarRocks external catalog
        ▼
  [Identity Resolution] → cdp.identity_graph
        │ dbt Silver (per source, 15min)
        ▼
  cdp.silver_login / silver_view / silver_click / silver_payment / silver_search
        │ dbt Gold (daily)
        ▼
  cdp.gold_user_daily / gold_page_views / ...
        │ dbt Unified (daily)
        ▼
  cdp.user_profiles  ← 360° view per user
        │ dbt Segmentation (daily)
        ▼
  cdp.user_segments  ← segment membership
        │
  ┌─────┴──────┐
  │            │
  ▼            ▼
Superset  Kafka: cdp-segment-updates
(BI)      (downstream activation)
```
