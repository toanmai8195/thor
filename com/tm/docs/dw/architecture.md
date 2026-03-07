# Kiến trúc — Luồng 2: Warehouse DW (Iceberg + StarRocks)

## Flow dữ liệu

```
tracking-ingestor (validate JSON)       ← shared với Luồng 1
        │ Kafka: payment-events-ingest
        ▼
Apache Flink (streaming, checkpoint 60s)
        │ exactly-once, Parquet files
        ▼
Apache Iceberg: bronze/payments
(HadoopCatalog, s3a://lakehouse/warehouse/, MinIO)
        │ StarRocks external catalog (query-on-read, no copy)
        ▼
iceberg_catalog.bronze.payments         ← "SR Bronze"
        │ [future] dbt-starrocks (Airflow)
        ▼
tracking.silver_payments                ← SR native table
        │ [future] dbt-starrocks (Airflow)
        ▼
analytics.gold_revenue                  ← SR native table
        │
Superset / Segment Engine
```

**Mục tiêu:** Historical analysis, BI dashboards, user segmentation — dữ liệu đầy đủ, chính xác hơn Realtime.

---

## Tech Stack

| Component | Technology | Version | Vai trò |
|-----------|------------|---------|---------|
| Object Store | MinIO | latest | S3-compatible, lưu Iceberg Parquet + metadata |
| Lake Format | Apache Iceberg | 1.6.1 | Open table format (Parquet, ACID, time-travel) |
| Streaming Ingest | Apache Flink | 1.18 | Kafka → Iceberg Bronze (exactly-once) |
| DW Query Engine | StarRocks (allin1) | 3.2.7 | Iceberg ext catalog + Silver + Gold (MPP) |
| Transform (DW) | dbt-starrocks | — | SR Bronze → Silver → Gold [future] |
| Orchestration | Apache Airflow | 2.8.0 | Schedule dbt DW [future] |
| Visualization | Apache Superset | latest | DW BI dashboards [future] |

---

## Chi tiết từng thành phần

### MinIO (Object Store)

S3-compatible storage cho Iceberg data + metadata files.

| Item | Giá trị |
|------|---------|
| Bucket | `lakehouse` |
| Warehouse path | `s3a://lakehouse/warehouse/` |
| Console | http://localhost:9001 |
| Credentials | `minioadmin` / `minioadmin` |

Flink ghi Parquet files vào đây. StarRocks đọc qua S3A connector — **không di chuyển data**.

### Apache Flink (Kafka → Iceberg)

Job: `docker/dw/flink/kafka_to_iceberg.sql`
Image: `docker/dw/Dockerfile.flink` (Flink 1.18 + Kafka connector + Iceberg runtime + S3 plugin)

| Param | Giá trị |
|-------|---------|
| Source topic | `payment-events-ingest` (group: `flink-iceberg-bronze`) |
| Checkpoint interval | 60 giây = Iceberg commit interval |
| Exactly-once | ✓ (checkpoint-based 2-phase commit) |
| Partition | `DAY(payment_date)` — 1 folder/ngày |
| File format | Parquet, Iceberg format-version=2 |
| Target file size | 64 MB |
| Catalog | HadoopCatalog (`s3a://lakehouse/warehouse`) |

Transformations trong Flink SQL:
- `amount` → `ROUND(amount, 2)` → `DECIMAL(12,2)`
- `payment_date` (String) → `DATE`
- `updated_at` (RFC3339) → `TIMESTAMP(3)` (lấy 19 ký tự đầu, bỏ timezone)
- Thêm `ingested_at = CURRENT_TIMESTAMP`
- Filter: `event_id IS NOT NULL AND user_id IS NOT NULL AND payment_date IS NOT NULL`

### StarRocks Bronze = Iceberg External Catalog

```sql
CREATE EXTERNAL CATALOG iceberg_catalog
PROPERTIES (
    "type"                           = "iceberg",
    "iceberg.catalog.type"           = "hadoop",
    "iceberg.catalog.warehouse"      = "s3a://lakehouse/warehouse",
    "aws.s3.endpoint"                = "http://minio:9000",
    "aws.s3.enable-path-style-access"= "true",
    ...
);
```

`iceberg_catalog.bronze.payments` = SR Bronze — query trực tiếp Parquet files trên MinIO, **không copy data**.
Chỉ có data sau khi Flink commit ít nhất 1 checkpoint (~60s đầu tiên).

### StarRocks Silver + Gold (Future)

dbt_starrocks sẽ transform:
- `iceberg_catalog.bronze.payments` → `tracking.silver_payments` (native SR Primary Key table)
- `tracking.silver_payments` → `analytics.gold_revenue` (native SR Aggregate table)

---

## Cấu trúc thư mục (Luồng 2)

```
docker/dw/
├── Dockerfile.flink                # Flink 1.18 + Kafka + Iceberg + S3 plugin
├── flink/
│   └── kafka_to_iceberg.sql        # Flink SQL: Kafka → Iceberg Bronze
└── starrocks/
    └── init/
        ├── 00_iceberg_catalog.sql  # SR Iceberg ext catalog + silver_payments pre-create
        ├── 01_silver_mirror.sql    # [future] dbt Silver transform
        └── 02_gold.sql             # [future] dbt Gold aggregation

src/services/analytics-aggregator/dw/
└── dbt/                            # [future] dbt-starrocks project
    ├── dbt_project.yml
    ├── profiles.yml
    └── models/gold/gold_revenue.sql
```
