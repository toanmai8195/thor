# Data — Luồng 2: Warehouse DW (Iceberg + StarRocks)

## Schemas

### Bronze: `iceberg_catalog.bronze.payments` (Iceberg on MinIO)

Raw data do **Flink** ghi vào Apache Iceberg (Parquet files trên MinIO).
StarRocks đọc qua **external catalog** — không copy data, không có native SR table ở tầng Bronze.

| Column | Type | Mô tả |
|--------|------|-------|
| `event_id` | STRING | UUID của payment event |
| `user_id` | BIGINT | ID user (1–100,000) |
| `amount` | DECIMAL(12,2) | Số tiền raw (có thể âm, zero — chưa filter ở Bronze) |
| `status` | STRING | Trạng thái thô: `"paid"` / `"PAID"` / `"invalid_status"` / ... |
| `payment_date` | DATE | Ngày thanh toán (partition key) |
| `updated_at` | TIMESTAMP(3) | Timestamp gốc từ event-producer (RFC3339) |
| `ingested_at` | TIMESTAMP(3) | Thời điểm Flink write vào Iceberg |

- Format: Parquet, Iceberg format-version=2 (hỗ trợ row-level delete)
- Catalog: HadoopCatalog → `s3a://lakehouse/warehouse/bronze/payments/`
- Partition: `DAY(payment_date)` — 1 folder/ngày
- Commit interval: mỗi 60 giây (= Flink checkpoint, exactly-once)

---

### Silver: `tracking.silver_payments` (StarRocks native) [future]

dbt transform từ `iceberg_catalog.bronze.payments`. Cùng logic normalize với CH Silver.

| Column | Type | Mô tả |
|--------|------|-------|
| `event_id` | VARCHAR(64) NOT NULL | UUID |
| `user_id` | BIGINT NOT NULL | ID user |
| `amount` | DECIMAL(12,2) NOT NULL | Số tiền hợp lệ (> 0) |
| `status` | VARCHAR(20) NOT NULL | Normalized: paid/pending/failed/refunded/cancelled/unknown |
| `payment_date` | DATE NOT NULL | Ngày thanh toán |
| `updated_at` | DATETIME NOT NULL | Timestamp gốc |

- Model: PRIMARY KEY (`event_id`, `payment_date`) → UPSERT (idempotent)
- Partition: RANGE(`payment_date`) by MONTH, dynamic partition (giữ 6 tháng, tạo sẵn 3 tháng)
- Distribute: HASH(`user_id`) 8 buckets

---

### Gold: `analytics.gold_revenue` (StarRocks native) [future]

Aggregated revenue metrics theo ngày. Rebuild bởi dbt_starrocks.

| Column | Type | Mô tả |
|--------|------|-------|
| `payment_date` | DATE | Ngày (grain/key) |
| `total_revenue` | DECIMAL | `SUM(amount)` WHERE status = `'paid'` |
| `total_paid_orders` | BIGINT | `COUNT(*)` WHERE status = `'paid'` |
| `avg_order_value` | DECIMAL | `total_revenue / total_paid_orders` |
| `unique_users` | BIGINT | `COUNT(DISTINCT user_id)` |
| `pending_orders` | BIGINT | Số đơn pending |
| `failed_orders` | BIGINT | Số đơn failed |
| `refunded_orders` | BIGINT | Số đơn refunded |
| `cancelled_orders` | BIGINT | Số đơn cancelled |
| `unknown_orders` | BIGINT | Số đơn không xác định status |
| `dbt_updated_at` | DATETIME | Thời gian dbt chạy |

---

## Analytics Queries

### Kiểm tra Iceberg Bronze (StarRocks external catalog)

```sql
-- Row count Bronze hôm nay
SELECT COUNT(*), MIN(payment_date), MAX(payment_date)
FROM iceberg_catalog.bronze.payments;

-- Phân bổ status trong Bronze
SELECT status, COUNT(*) AS cnt
FROM iceberg_catalog.bronze.payments
WHERE payment_date = CURRENT_DATE()
GROUP BY status
ORDER BY cnt DESC;

-- Flink ingest rate theo phút
SELECT
    DATE_TRUNC('minute', ingested_at) AS minute,
    COUNT(*)                          AS rows_ingested
FROM iceberg_catalog.bronze.payments
WHERE ingested_at >= NOW() - INTERVAL 10 MINUTE
GROUP BY minute
ORDER BY minute DESC;
```

### Gold queries [future — sau khi dbt Silver + Gold chạy]

```sql
-- Revenue theo ngày (30 ngày gần nhất)
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

```sql
-- Revenue theo tuần
SELECT
    DATE_TRUNC('week', payment_date) AS week,
    SUM(total_revenue)               AS weekly_revenue,
    SUM(total_paid_orders)           AS weekly_orders,
    SUM(unique_users)                AS total_users
FROM analytics.gold_revenue
GROUP BY week
ORDER BY week DESC;
```

---

## Data Quality

### So sánh Flink lag — CH Bronze vs Iceberg Bronze

```sql
-- ClickHouse: row count CH Bronze (hôm nay)
SELECT count() FROM tracking.payments_bronze WHERE payment_date = today();

-- StarRocks (via Iceberg external catalog): row count Iceberg Bronze (hôm nay)
SELECT COUNT(*) FROM iceberg_catalog.bronze.payments WHERE payment_date = CURRENT_DATE();

-- Chênh lệch = số rows Flink chưa flush vào Iceberg (buffered trong checkpoint)
```

### Kiểm tra Silver drop rate [future]

```sql
-- So sánh Bronze vs Silver (sau khi dbt chạy)
SELECT
    b.bronze_count,
    s.silver_count,
    b.bronze_count - s.silver_count AS dropped
FROM
    (SELECT COUNT(*) AS bronze_count
     FROM iceberg_catalog.bronze.payments
     WHERE payment_date = CURRENT_DATE()) b,
    (SELECT COUNT(*) AS silver_count
     FROM tracking.silver_payments
     WHERE payment_date = CURRENT_DATE()) s;
```
