# Kiến trúc — Luồng 1: Realtime (ClickHouse)

## Flow dữ liệu

```
event-producer (~500 events/s)
        │ Kafka: payment-events
        ▼
tracking-ingestor (validate JSON)       ← single validation gate
        │ Kafka: payment-events-ingest
        ▼
ClickHouse Kafka Engine (payments_kafka)
        │ Materialized View (payments_bronze_mv)
        ▼
tracking.payments_bronze
(raw, MergeTree, TTL 90 ngày)
        │ dbt-clickhouse (Airflow, mỗi 1 phút)
        ▼
tracking.silver_events
(normalized, incremental)
        │
Superset (realtime charts)
```

**Mục tiêu:** Low-latency analytics (~1 phút latency end-to-end), anomaly detection, realtime monitoring.

---

## Tech Stack

| Component | Technology | Version | Vai trò |
|-----------|------------|---------|---------|
| Message Broker | Apache Kafka (KRaft) | 4.2.0 | Event streaming |
| Ingestor | Go + Bazel | Go 1.22 | Validate + forward events |
| Realtime DB | ClickHouse | 26.2 | Bronze + Silver (Kafka Engine, OLAP) |
| Transform | dbt-clickhouse | 1.10.0 | CH Bronze → Silver |
| Orchestration | Apache Airflow | 2.8.0 | Schedule dbt silver mỗi 1 phút |
| Visualization | Apache Superset | latest | Realtime BI dashboards |

---

## Chi tiết từng thành phần

### Tracking Ingestor (Shared)

`src/services/tracking-ingestor` — **single validation gate** cho cả hai pipelines.

- Kafka ConsumerGroup đọc `payment-events`
- Validate JSON schema (required fields, type check), drop malformed records
- Forward validated bytes sang `payment-events-ingest` (key = `event_id`)

### ClickHouse Kafka Engine

Init SQL: `docker/realtime/clickhouse/init/01_bronze.sql`

- `payments_kafka` — Kafka Engine table, consumer group `ch-payment-ingestor`
- `payments_bronze_mv` — Materialized View: Kafka → `payments_bronze` (trigger per row)
- `payments_bronze` — MergeTree storage, partition `toYYYYMM(payment_date)`, TTL 90 ngày

### dbt-clickhouse (Silver transform)

Project: `src/services/analytics-aggregator/realtime/dbt/`

| Param | Giá trị |
|-------|---------|
| Materialization | `incremental` (append, watermark `payment_date`) |
| Schedule | Airflow DAG `silver_ingest` — mỗi 1 phút |
| Source | `tracking.payments_bronze` |
| Target | `tracking.silver_events` |

Logic normalize:
- `lower(trim(status))` → nếu không thuộc valid set → `'unknown'`
- Filter: `amount > 0`, `event_id != ''`, `user_id != 0`

Valid status: `paid` | `pending` | `failed` | `refunded` | `cancelled` | `unknown`

### Airflow DAGs

DAG files: `src/services/analytics-aggregator/realtime/dags/`

| DAG | Schedule | Nhiệm vụ |
|-----|----------|---------|
| `silver_ingest` | `*/1 * * * *` | Chạy `dbt run` → silver_events |
| `medallion_pipeline` | On-demand | Full Bronze → Silver rebuild |

---

## Cấu trúc thư mục (Luồng 1)

```
docker/realtime/clickhouse/
├── config.xml              # Kafka librdkafka config
├── users.xml
└── init/
    ├── 01_bronze.sql       # payments_bronze + Kafka Engine + MV
    └── 02_silver.sql       # silver_events (pre-create cho dbt)

src/services/analytics-aggregator/realtime/
├── Dockerfile.dbt          # dbt-clickhouse image
├── dbt/
│   ├── dbt_project.yml
│   ├── profiles.yml
│   └── models/silver/silver_events.sql
└── dags/
    ├── silver_ingest_dag.py
    └── medallion_pipeline_dag.py
```
