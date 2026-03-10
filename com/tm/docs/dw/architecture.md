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
        │ dbt_starrocks_silver (Airflow, mỗi 15 phút)
        ▼
tracking.silver_payments                ← SR native PRIMARY KEY table
        │ dbt_starrocks_gold (Airflow, daily 02:00 UTC)
        ▼
analytics.gold_revenue                  ← SR native table
        │
Superset (http://localhost:8088)
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
| Transform Silver | dbt-starrocks (dbt_starrocks_silver) | — | Iceberg Bronze → `tracking.silver_payments` |
| Transform Gold | dbt-starrocks (dbt_starrocks_gold) | — | Silver → `analytics.gold_revenue` |
| Orchestration | Apache Airflow | 2.8.0 | `dw_silver_ingest` (*/15) + `dw_medallion_pipeline` (daily) |
| Visualization | Apache Superset | latest | BI dashboard — StarRocks `analytics` DB |

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

### StarRocks Silver — `tracking.silver_payments`

dbt_starrocks_silver transform từ Iceberg Bronze:
- Source: `iceberg_catalog.bronze.payments` (external catalog, Parquet on MinIO)
- Target: `tracking.silver_payments` (PRIMARY KEY table, UPSERT by `event_id + payment_date`)
- Incremental: chỉ process `payment_date >= MAX - 1 day` mỗi lần chạy
- Schedule: mỗi 15 phút qua Airflow DAG `dw_silver_ingest`

### StarRocks Gold — `analytics.gold_revenue`

dbt_starrocks_gold aggregate từ Silver:
- Source: `tracking.silver_payments`
- Target: `analytics.gold_revenue` (full rebuild mỗi lần chạy)
- Metrics: `total_revenue`, `total_paid_orders`, `avg_order_value`, `unique_users` theo `payment_date`
- Schedule: daily 02:00 UTC qua Airflow DAG `dw_medallion_pipeline`

---

## Cấu trúc thư mục (Luồng 2)

```
docker/dw/
├── Dockerfile.flink                # Flink 1.18 + Kafka + Iceberg + S3 plugin
├── flink/
│   └── kafka_to_iceberg.sql        # Flink SQL: Kafka → Iceberg Bronze
└── starrocks/
    └── init/
        └── 00_iceberg_catalog.sql  # SR Iceberg ext catalog + silver_payments + analytics DB

src/services/analytics-aggregator/dw/
├── Dockerfile.dbt                  # dbt-starrocks image
├── dbt_silver/                     # dbt project: Iceberg Bronze → tracking.silver_payments
│   ├── dbt_project.yml
│   ├── profiles.yml
│   └── models/
│       ├── sources.yml             # Source: iceberg_catalog.bronze.payments
│       └── silver/
│           └── silver_payments.sql
├── dbt/                            # dbt project: tracking.silver_payments → analytics.gold_revenue
│   ├── dbt_project.yml
│   ├── profiles.yml
│   └── models/
│       ├── sources.yml             # Source: tracking.silver_payments
│       └── gold/
│           └── gold_revenue.sql
└── dags/                           # Airflow DAGs
    ├── silver_ingest_dag.py        # DAG dw_silver_ingest (*/15 min)
    └── medallion_pipeline_dag.py   # DAG dw_medallion_pipeline (daily 02:00)
```
