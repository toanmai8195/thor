# Data

## Schemas

### Bronze: `tracking.payments_bronze` (ClickHouse)

Raw data từ Kafka Engine. **Không validate, không transform.**
Được ghi bởi Materialized View `payments_bronze_mv` từ Kafka Engine `payments_kafka`.

| Column | Type | Mô tả |
|--------|------|-------|
| `event_id` | String | UUID của payment event |
| `user_id` | UInt64 | ID user |
| `amount` | Float64 | Số tiền (có thể âm, zero — bronze không check) |
| `status` | String | Trạng thái thô: `"paid"` / `"PAID"` / `""` / invalid |
| `payment_date` | Date | Ngày thanh toán |
| `updated_at` | DateTime64(3) | Thời gian cập nhật (ms precision) |
| `ingested_at` | DateTime64(3) | Thời gian CH Kafka Engine ghi (auto `now64(3)`) |

- Engine: `MergeTree`, partition by `toYYYYMM(payment_date)`, order by `(payment_date, event_id)`
- TTL: 90 ngày kể từ `payment_date`

**Kafka Engine** `payments_kafka` (virtual table, không lưu data):

| Setting | Giá trị |
|---------|---------|
| Broker | `kafka:29092` |
| Topic | `payment-events-ingest` |
| Consumer group | `ch-payment-ingestor` |
| Format | `JSONEachRow` |
| Consumers | 4 threads |
| Batch size | 65.536 messages |

---

### Silver: `tracking.silver_payments` (ClickHouse) → mirror tại StarRocks

Sau khi dbt_clickhouse chạy. Chỉ giữ lại records hợp lệ.

| Column | Type | Normalize rule |
|--------|------|---------------|
| `event_id` | String | Loại bỏ blank (`event_id != ''`) |
| `user_id` | UInt64 | Loại bỏ 0 (`user_id != 0`) |
| `amount` | Float64 | Chỉ giữ `> 0` (loại null, âm, zero) |
| `status` | String | `lower(trim(status))` → nếu không valid → `'unknown'` |
| `payment_date` | Date | Giữ nguyên |
| `updated_at` | DateTime64(3) | Giữ nguyên |

**Status normalization:**

| Input | Output | Lý do |
|-------|--------|-------|
| `"paid"` | `"paid"` | Valid |
| `"PAID"` | `"paid"` | Lowercase |
| `"Paid "` | `"paid"` | Trim + lowercase |
| `null` / `""` | `"unknown"` | Missing |
| `"invalid_xyz"` | `"unknown"` | Không thuộc valid set |

Valid status values: `paid` \| `pending` \| `failed` \| `refunded` \| `cancelled` \| `unknown`

---

### Gold: `analytics.gold_revenue` (StarRocks)

Aggregated revenue metrics theo ngày. Rebuild hàng ngày từ silver.

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
-- StarRocks
SELECT
    DATE_TRUNC('week', payment_date) AS week,
    SUM(total_revenue)               AS weekly_revenue,
    SUM(total_paid_orders)           AS weekly_orders
FROM analytics.gold_revenue
GROUP BY week
ORDER BY week DESC;
```

### Status breakdown từ silver (7 ngày gần nhất)

```sql
-- ClickHouse
SELECT
    payment_date,
    countIf(status = 'paid')      AS paid,
    countIf(status = 'pending')   AS pending,
    countIf(status = 'failed')    AS failed,
    countIf(status = 'unknown')   AS unknown,
    count()                        AS total
FROM tracking.silver_payments
WHERE payment_date >= today() - 7
GROUP BY payment_date
ORDER BY payment_date DESC;
```

---

## Data Quality

### Kiểm tra bad data trong Bronze

```sql
-- ClickHouse: phân bổ status + amount range hôm nay
SELECT
    status,
    count()                     AS count,
    countIf(amount <= 0)        AS bad_amount,
    min(amount)                 AS min_amount,
    max(amount)                 AS max_amount
FROM tracking.payments_bronze
WHERE payment_date = today()
GROUP BY status
ORDER BY count DESC;
```

### Tỉ lệ drop Bronze → Silver

```sql
-- ClickHouse: so sánh số rows bị lọc bởi silver
SELECT
    b.total_bronze,
    s.total_silver,
    b.total_bronze - s.total_silver AS dropped,
    round((b.total_bronze - s.total_silver) / b.total_bronze * 100, 2) AS drop_pct
FROM
    (SELECT count() AS total_bronze FROM tracking.payments_bronze
     WHERE payment_date = today()) b,
    (SELECT count() AS total_silver FROM tracking.silver_payments
     WHERE payment_date = today()) s;
```

### Kiểm tra Kafka Engine ingestion rate

```sql
-- ClickHouse: rows được CH Kafka Engine ingest trong 1 phút vừa rồi
SELECT
    toStartOfMinute(ingested_at) AS minute,
    count()                       AS rows_ingested
FROM tracking.payments_bronze
WHERE ingested_at >= now64(3) - INTERVAL 5 MINUTE
GROUP BY minute
ORDER BY minute DESC;
```

### Table size Bronze

```sql
-- ClickHouse: disk usage theo table
SELECT
    table,
    formatReadableSize(sum(bytes_on_disk)) AS disk_size,
    sum(rows)                               AS total_rows,
    max(modification_time)                  AS last_modified
FROM system.parts
WHERE database = 'tracking'
GROUP BY table;
```
