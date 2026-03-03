# Kiến trúc hệ thống

## Tổng quan

Payment Revenue Analytics Platform theo **Medallion Architecture**: dữ liệu đi qua 3 lớp Bronze → Silver → Gold, mỗi lớp tăng dần mức độ xử lý và độ tin cậy.

## Flow dữ liệu

```
[Event Producer]
      │  Sinh payment events giả lập (1000 events/giây)
      │  Schema: event_id, user_id, amount, status, payment_date, updated_at
      ▼
[Apache Kafka]
      │  Topic: "payment-events" (6 partitions)
      │
      ▼
[Tracking Ingestor]  ← Go service
      │  ConsumerGroup đọc "payment-events"
      │  Validate JSON schema
      │  Forward sang "payment-events-ingest"
      ▼
[Apache Kafka]
      │  Topic: "payment-events-ingest" (validated events)
      │
      ▼
[ClickHouse: payments_kafka]   ← Kafka Engine (tự consume)
      │  Materialized View payments_bronze_mv
      ▼
[ClickHouse: payments_bronze]  ← BRONZE (raw, không transform)
      │  event_id | user_id | amount | status | payment_date | updated_at
      │
      ▼  dbt_clickhouse (Airflow, hàng ngày 02:00 UTC)
[ClickHouse: silver_payments]  ← SILVER (normalize + filter)
      │  status lowercase, amount > 0, invalid → 'unknown'
      │
      ▼  Sync job ch_to_starrocks.py
[StarRocks: silver_payments]   ← SILVER MIRROR
      │
      ▼  dbt_starrocks (Airflow, hàng ngày)
[StarRocks: gold_revenue]      ← GOLD (aggregated by date)
      │
      ▼
[Apache Superset]              ← Revenue dashboards
```

**Airflow DAG** `medallion_pipeline` (02:00 UTC hàng ngày):
```
check_bronze → silver_layer → sync_silver_payments → gold_layer
```

---

## Tech Stack

| Component | Technology | Vai trò |
|-----------|------------|---------|
| Event Generator | Go + Bazel | Sinh payment events giả lập |
| Message Broker | Apache Kafka 7.5 | Event streaming |
| Ingestor | Go + Bazel | Validate + forward Kafka events |
| Bronze/Silver DB | ClickHouse 24.1 | Kafka Engine + OLAP storage |
| Gold DB | StarRocks 3.2 | MPP analytics, Bitmap segments |
| Transform | dbt (2 projects) | Bronze→Silver (CH), Silver→Gold (SR) |
| Orchestration | Apache Airflow 2.8 | Schedule pipeline hàng ngày |
| Visualization | Apache Superset 3.1 | Revenue dashboards |
| Build | Bazel 9 + bzlmod | Build Go services |

---

## Chi tiết từng lớp

### Bronze — Kafka Engine + Tracking Ingestor

**Event Producer** (`src/services/event-producer`) sinh payment events với distribution:

| Status | Tỉ lệ | Ghi chú |
|--------|-------|---------|
| `paid` | 60% | Đơn thành công |
| `pending` | 20% | Đang xử lý |
| `failed` | 10% | Thất bại |
| `refunded` | 5% | Hoàn tiền |
| `cancelled` | 3% | Huỷ đơn |
| `PAID` / invalid | 2% | Bad data để test silver normalization |

Amount: $1 → $2000, đôi khi âm/zero để test.

**Tracking Ingestor** (`src/services/tracking-ingestor`):
- Kafka ConsumerGroup (sticky rebalancing) đọc `"payment-events"`
- Validate JSON: drop message lỗi parse, log + mark offset
- Forward raw bytes sang `"payment-events-ingest"` với key = `event_id`
- Không mark offset nếu produce lỗi → reprocess khi restart

**ClickHouse Kafka Engine** (`payments_kafka`):
- Tự consume `"payment-events-ingest"` (consumer group `ch-payment-ingestor`)
- 4 consumer threads song song, batch 65K messages
- Materialized View `payments_bronze_mv` parse date → insert `payments_bronze`
- `kafka_handle_error_mode = 'stream'` + `kafka_skip_broken_messages = 1000`

### Silver — dbt_clickhouse

Project: `analytics-aggregator/dbt_clickhouse/`

Model `silver_events.sql` → table `silver_payments`:

```sql
-- Normalize status
CASE
    WHEN lower(trim(status)) IN ('paid','pending','failed','refunded','cancelled')
        THEN lower(trim(status))
    ELSE 'unknown'
END AS status

-- Filter bad data
WHERE amount IS NOT NULL AND amount > 0
  AND event_id != ''
  AND user_id != 0
```

Materialization: `incremental` (chỉ xử lý data mới, 1 ngày overlap).

### Sync — ch_to_starrocks.py

`analytics-aggregator/sync/ch_to_starrocks.py`:

1. Export `silver_payments` từ ClickHouse HTTP (`FORMAT JSONEachRow`)
2. PUT lên StarRocks Stream Load API (`http://starrocks-fe:8030/api/analytics/silver_payments/_stream_load`)
3. PRIMARY KEY table → UPSERT, idempotent

### Gold — dbt_starrocks

Project: `analytics-aggregator/dbt_starrocks/`

Model `gold_revenue.sql` → table `gold_revenue`:

```sql
SELECT
    payment_date,
    SUM(CASE WHEN status = 'paid' THEN amount ELSE 0 END) AS total_revenue,
    COUNT(CASE WHEN status = 'paid' THEN 1 END)           AS total_paid_orders,
    COUNT(DISTINCT user_id)                                AS unique_users,
    total_revenue / NULLIF(total_paid_orders, 0)           AS avg_order_value
FROM silver_payments
GROUP BY payment_date
```

---

## Cấu trúc thư mục

```
com/tm/
├── docker/
│   ├── docker-compose.yml              # Infrastructure (Kafka, CH, StarRocks, ...)
│   ├── clickhouse/init/
│   │   └── 01_bronze.sql               # payments_bronze + Kafka Engine + MV
│   └── starrocks/init/
│       ├── 01_silver_mirror.sql        # silver_payments (StarRocks mirror)
│       └── 02_gold.sql                 # gold_revenue
│
├── src/services/
│   ├── event-producer/
│   │   └── main.go                     # Sinh events → "payment-events"
│   ├── tracking-ingestor/
│   │   └── main.go                     # Validate + forward → "payment-events-ingest"
│   └── analytics-aggregator/
│       ├── dbt_clickhouse/             # Silver layer
│       │   └── models/silver/silver_events.sql
│       ├── dbt_starrocks/              # Gold layer
│       │   └── models/gold/gold_revenue.sql
│       ├── sync/
│       │   └── ch_to_starrocks.py      # Sync CH silver → StarRocks
│       └── dags/
│           └── medallion_pipeline_dag.py
│
└── docs/
    ├── README.md                       # Index
    ├── architecture.md                 # Tài liệu này
    ├── data.md                         # Data schemas + queries
    └── setup.md                        # Cài đặt + troubleshooting
```

---

## Glossary

| Thuật ngữ | Giải thích |
|-----------|-----------|
| **Medallion Architecture** | Kiến trúc 3 lớp: Bronze (raw) → Silver (clean) → Gold (aggregated) |
| **Bronze** | Dữ liệu thô, không transform, lưu nguyên trạng từ nguồn |
| **Silver** | Dữ liệu đã validate, normalize, clean |
| **Gold** | Dữ liệu đã aggregate, business-ready, dùng cho dashboard |
| **Kafka Engine** | Table engine của ClickHouse tự consume Kafka topic |
| **Materialized View** | View tự động insert vào target table khi có data mới |
| **MergeTree** | Storage engine của ClickHouse, tối ưu cho write + analytics |
| **Stream Load** | API của StarRocks để load data nhanh qua HTTP PUT |
| **ConsumerGroup** | Nhóm Kafka consumers chia nhau xử lý partitions |
| **Incremental model** | dbt model chỉ xử lý data mới, không rebuild toàn bộ |
| **AOV** | Average Order Value = total_revenue / total_paid_orders |
| **MPP** | Massively Parallel Processing — kiến trúc của StarRocks |
