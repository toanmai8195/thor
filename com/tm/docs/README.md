# Payment Revenue Analytics Platform

Hệ thống tracking thanh toán theo Medallion Architecture: Bronze → Silver → Gold.

## Mục lục

1. [Kiến trúc](#kiến-trúc)
2. [Tech Stack](#tech-stack)
3. [Data Schema](#data-schema)
4. [Cài đặt & Chạy](#cài-đặt--chạy)
5. [Chi tiết từng lớp](#chi-tiết-từng-lớp)
6. [Analytics Queries](#analytics-queries)
7. [Troubleshooting](#troubleshooting)

---

## Kiến trúc

```
[Event Producer]
      │  Sinh payment events giả lập (1000 events/giây)
      │  Schema: event_id, user_id, amount, status, payment_date, updated_at
      ▼
[Apache Kafka]
      │  Topic: payment-events (6 partitions)
      │
      ▼
[Tracking Ingestor]
      │  Kafka ConsumerGroup → batch insert 10K rows/lần
      ▼
[ClickHouse: payments_bronze]  ← BRONZE (raw, không validate)
      │  event_id | user_id | amount | status | payment_date | updated_at
      │
      ▼ dbt_clickhouse chạy hàng ngày (Airflow)
[ClickHouse: silver_payments]  ← SILVER (đã normalize)
      │  - status: lowercase + trim + map invalid → 'unknown'
      │  - amount: loại bỏ null, âm, zero
      │
      ▼ Sync job (ch_to_starrocks.py)
[StarRocks: silver_payments]   ← SILVER MIRROR (sync từ ClickHouse)
      │
      ▼ dbt_starrocks chạy hàng ngày (Airflow)
[StarRocks: gold_revenue]      ← GOLD (aggregated revenue)
      │  payment_date | total_revenue | total_paid_orders | avg_order_value
      │
      ▼
[Apache Superset]              ← Dashboards
```

**Airflow DAG** (`medallion_pipeline`, chạy 02:00 UTC hàng ngày):
```
check_bronze → silver_layer → sync_silver_payments → gold_layer
```

---

## Tech Stack

| Component | Technology | Vai trò |
|-----------|------------|---------|
| Event Generator | Go + Bazel | Sinh payment events giả lập |
| Message Broker | Apache Kafka 7.5 | Truyền events bất đồng bộ |
| Bronze/Silver DB | ClickHouse 24.1 | OLAP, write-optimized |
| Gold DB | StarRocks 3.2 | Analytical queries, MPP |
| Transform | dbt (2 projects) | Bronze→Silver (CH), Silver→Gold (SR) |
| Orchestration | Apache Airflow 2.8 | Schedule pipeline hàng ngày |
| Visualization | Apache Superset 3.1 | Revenue dashboards |
| Build | Bazel 9 + bzlmod | Build Go services + Python |

---

## Data Schema

### Bronze: `tracking.payments_bronze` (ClickHouse)

Raw data, lưu nguyên trạng từ Kafka. **Không validate, không transform.**

| Column | Type | Mô tả |
|--------|------|-------|
| `event_id` | String | UUID của payment event |
| `user_id` | UInt64 | ID user |
| `amount` | Float64 | Số tiền (có thể null, âm — bronze không check) |
| `status` | String | Trạng thái thô: `"paid"` / `"PAID"` / `""` / invalid |
| `payment_date` | Date | Ngày thanh toán |
| `updated_at` | DateTime64(3) | Thời gian cập nhật |
| `ingested_at` | DateTime64(3) | Thời gian ingestor ghi (auto) |

TTL: 90 ngày. Partition by month.

---

### Silver: `tracking.silver_payments` (ClickHouse) → mirror tại StarRocks

Sau khi dbt_clickhouse chạy. Chỉ giữ lại records hợp lệ.

| Column | Type | Normalize rule |
|--------|------|---------------|
| `event_id` | String | Loại bỏ blank |
| `user_id` | UInt64 | Loại bỏ 0 hoặc null |
| `amount` | Float64 | Chỉ giữ `> 0` (loại null, âm, zero) |
| `status` | String | `lower(trim(status))` → nếu không phải valid value thì → `'unknown'` |
| `payment_date` | Date | Giữ nguyên |
| `updated_at` | DateTime64(3) | Giữ nguyên |

**Status normalization:**
```
"paid"          → "paid"      ✓
"PAID"          → "paid"      ✓ (lowercase)
"Paid "         → "paid"      ✓ (trim + lowercase)
null            → "unknown"   ✓
""              → "unknown"   ✓
"invalid_xyz"   → "unknown"   ✓
```

**Valid status values:** `paid` | `pending` | `failed` | `refunded` | `cancelled` | `unknown`

---

### Gold: `analytics.gold_revenue` (StarRocks)

Aggregated revenue metrics theo ngày.

| Column | Type | Mô tả |
|--------|------|-------|
| `payment_date` | DATE | Ngày (grain/key) |
| `total_revenue` | DECIMAL | `SUM(amount)` WHERE status = 'paid' |
| `total_paid_orders` | BIGINT | `COUNT(*)` WHERE status = 'paid' |
| `avg_order_value` | DECIMAL | `total_revenue / total_paid_orders` |
| `unique_users` | BIGINT | `COUNT(DISTINCT user_id)` |
| `pending_orders` | BIGINT | Số đơn pending |
| `failed_orders` | BIGINT | Số đơn failed |
| `refunded_orders` | BIGINT | Số đơn refunded |
| `cancelled_orders` | BIGINT | Số đơn cancelled |
| `unknown_orders` | BIGINT | Số đơn không xác định status |

---

## Cài đặt & Chạy

### Prerequisites

- Docker Desktop (4GB+ RAM)
- Go 1.22+
- Bazel 9.0+

### Khởi động infrastructure

```bash
cd com/tm/docker
docker compose up -d

# Đợi ~2 phút cho services sẵn sàng
docker compose ps
```

### Build Go services

```bash
# Build cả 2 services
bazel build //com/tm/src/services/...
```

### Chạy pipeline

```bash
# Terminal 1: Ingestor (lắng nghe Kafka → ghi ClickHouse)
bazel run //com/tm/src/services/tracking-ingestor:tracking-ingestor -- \
  --kafka=localhost:9092 \
  --ch-host=localhost

# Terminal 2: Producer (sinh payment events)
bazel run //com/tm/src/services/event-producer:event-producer -- \
  --kafka=localhost:9092 \
  --rate=1000
```

### Trigger Airflow DAG thủ công

```bash
# Trigger pipeline
docker exec airflow-webserver airflow dags trigger medallion_pipeline

# Xem kết quả
docker exec airflow-webserver airflow dags list-runs --dag-id medallion_pipeline
```

### Chạy sync/dbt thủ công

```bash
# Sync silver CH → StarRocks
bazel run //com/tm/src/services/analytics-aggregator:run_sync -- 2024-01-15

# Validate DAG
bazel test //com/tm/src/services/analytics-aggregator:dag_validation_test
```

### Service URLs

| Service | URL | Login |
|---------|-----|-------|
| Kafka UI | http://localhost:8082 | — |
| ClickHouse Play | http://localhost:8123/play | default / (trống) |
| StarRocks | `mysql -h 127.0.0.1 -P 9030 -u root` | root / (trống) |
| Airflow | http://localhost:8081 | admin / admin123 |
| Superset | http://localhost:8088 | admin / admin123 |
| HBase Web UI | http://localhost:16010 | — |

---

## Chi tiết từng lớp

### Bronze — Event Producer + Ingestor

**Event Producer** (`src/services/event-producer`) sinh payment events giả lập với distribution:

| Status | Tỉ lệ | Ghi chú |
|--------|-------|---------|
| `paid` | 60% | Đơn thành công |
| `pending` | 20% | Đang xử lý |
| `failed` | 10% | Thất bại |
| `refunded` | 5% | Hoàn tiền |
| `cancelled` | 3% | Huỷ đơn |
| `PAID` / invalid | 2% | **Bad data** để test silver normalization |

Amount range: $1 → $2000. Đôi khi sinh giá trị âm/zero để test.

**Tracking Ingestor** (`src/services/tracking-ingestor`):
- Kafka ConsumerGroup (sticky rebalancing)
- 4 worker goroutines, mỗi worker batch 10K rows
- Flush khi đủ batch size **hoặc** timeout 5 giây
- Retry 3 lần với exponential backoff
- Ghi vào `tracking.payments_bronze`

### Silver — dbt_clickhouse

Project: `analytics-aggregator/dbt_clickhouse/`

Model duy nhất: `silver_events.sql` → tạo table `silver_payments`

```sql
-- Normalize status
CASE
    WHEN lower(trim(status)) IN ('paid','pending','failed','refunded','cancelled')
        THEN lower(trim(status))
    ELSE 'unknown'
END AS status

-- Filter bad amount
WHERE amount IS NOT NULL AND amount > 0
```

Materialization: `incremental` (chỉ xử lý data ngày mới, 1 ngày overlap).

### Sync — ch_to_starrocks.py

`analytics-aggregator/sync/ch_to_starrocks.py`:
1. Export `silver_payments` từ ClickHouse HTTP (`FORMAT JSONEachRow`)
2. PUT lên StarRocks Stream Load API (`http://starrocks-fe:8030/api/analytics/silver_payments/_stream_load`)
3. PRIMARY KEY table → UPSERT, chạy nhiều lần không bị duplicate

### Gold — dbt_starrocks

Project: `analytics-aggregator/dbt_starrocks/`

Model duy nhất: `gold_revenue.sql` → tạo table `gold_revenue`

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

## Analytics Queries

### Revenue theo ngày (30 ngày gần nhất)

```sql
-- StarRocks / Superset
SELECT
    payment_date,
    total_revenue,
    total_paid_orders,
    avg_order_value,
    unique_users
FROM analytics.gold_revenue
WHERE payment_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
ORDER BY payment_date DESC;
```

### So sánh revenue theo tuần

```sql
SELECT
    DATE_TRUNC('week', payment_date) AS week,
    SUM(total_revenue)               AS weekly_revenue,
    SUM(total_paid_orders)           AS weekly_orders
FROM analytics.gold_revenue
GROUP BY week
ORDER BY week DESC;
```

### Tỉ lệ đơn paid vs failed (từ silver — debug)

```sql
-- ClickHouse
SELECT
    payment_date,
    countIf(status = 'paid')    AS paid,
    countIf(status = 'pending') AS pending,
    countIf(status = 'failed')  AS failed,
    countIf(status = 'unknown') AS unknown,
    count()                     AS total
FROM tracking.silver_payments
WHERE payment_date >= today() - 7
GROUP BY payment_date
ORDER BY payment_date DESC;
```

### Kiểm tra data quality Bronze (trước normalize)

```sql
-- ClickHouse: xem bad data trong bronze
SELECT
    status,
    count()     AS count,
    min(amount) AS min_amount,
    max(amount) AS max_amount
FROM tracking.payments_bronze
WHERE payment_date = today()
GROUP BY status
ORDER BY count DESC;
```

---

## Troubleshooting

### Kafka không nhận được events

```bash
docker compose logs kafka
docker exec kafka kafka-topics --bootstrap-server localhost:29092 --list

# Check consumer lag
docker exec kafka kafka-consumer-groups \
  --bootstrap-server localhost:29092 \
  --describe --group payment-ingestor
```

### ClickHouse không có data

```bash
# Check service
docker compose logs clickhouse

# Check bronze table
curl 'http://localhost:8123/?query=SELECT+count()+FROM+tracking.payments_bronze'

# Check silver table
curl 'http://localhost:8123/?query=SELECT+count()+FROM+tracking.silver_payments'
```

### dbt chạy bị lỗi

```bash
# Chạy dbt thủ công trong container Airflow
docker exec airflow-worker bash -c "
  cd /opt/analytics/dbt_clickhouse &&
  dbt debug --profiles-dir . &&
  dbt run --profiles-dir . --target dev
"
```

### StarRocks không nhận sync

```bash
# Test connection
mysql -h 127.0.0.1 -P 9030 -u root -e "SHOW DATABASES;"

# Kiểm tra silver_payments
mysql -h 127.0.0.1 -P 9030 -u root analytics -e "SELECT COUNT(*) FROM silver_payments;"

# Chạy sync thủ công
bazel run //com/tm/src/services/analytics-aggregator:run_sync -- $(date +%Y-%m-%d)
```

### Airflow DAG stuck

```bash
docker compose logs airflow-scheduler

# Trigger thủ công
docker exec airflow-webserver airflow dags trigger medallion_pipeline

# Xem task logs
docker exec airflow-webserver airflow tasks logs medallion_pipeline silver_layer.dbt_ch_run latest
```

### Performance — ClickHouse table size

```sql
SELECT
    table,
    formatReadableSize(sum(bytes_on_disk)) AS disk_size,
    sum(rows)                               AS total_rows,
    max(modification_time)                  AS last_modified
FROM system.parts
WHERE database = 'tracking'
GROUP BY table;
```

---

## Cấu trúc thư mục

```
com/tm/
├── docker/
│   ├── docker-compose.yml          # Toàn bộ infrastructure
│   ├── clickhouse/init/
│   │   └── 01_bronze.sql           # Schema payments_bronze
│   └── starrocks/init/
│       ├── 01_silver_mirror.sql    # Schema silver_payments (mirror)
│       └── 02_gold.sql             # Schema gold_revenue
│
├── src/services/
│   ├── event-producer/
│   │   └── main.go                 # Sinh payment events → Kafka
│   ├── tracking-ingestor/
│   │   └── main.go                 # Kafka → payments_bronze (batch insert)
│   └── analytics-aggregator/
│       ├── dbt_clickhouse/         # Silver layer (normalize)
│       │   └── models/silver/
│       │       └── silver_events.sql
│       ├── dbt_starrocks/          # Gold layer (aggregate)
│       │   └── models/gold/
│       │       └── gold_revenue.sql
│       ├── sync/
│       │   └── ch_to_starrocks.py  # Sync CH silver → SR
│       └── dags/
│           └── medallion_pipeline_dag.py  # Airflow DAG
│
└── docs/
    └── README.md
```

---

## Glossary

| Thuật ngữ | Giải thích |
|-----------|-----------|
| **Medallion Architecture** | Kiến trúc 3 lớp: Bronze (raw) → Silver (clean) → Gold (aggregated) |
| **Bronze** | Dữ liệu thô, không transform, lưu nguyên trạng từ nguồn |
| **Silver** | Dữ liệu đã validate, normalize, clean |
| **Gold** | Dữ liệu đã aggregate, business-ready, dùng cho dashboard |
| **MergeTree** | Storage engine của ClickHouse, tối ưu cho write + analytics |
| **Stream Load** | API của StarRocks để load data nhanh qua HTTP PUT |
| **ConsumerGroup** | Nhóm Kafka consumers chia nhau xử lý partitions |
| **Incremental model** | dbt model chỉ xử lý data mới, không rebuild toàn bộ |
| **AOV** | Average Order Value = total_revenue / total_paid_orders |
| **MPP** | Massively Parallel Processing — kiến trúc của StarRocks |
