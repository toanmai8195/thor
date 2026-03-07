# Data — Luồng 1: Realtime (ClickHouse)

## Schemas

### Bronze: `tracking.payments_bronze`

Raw data từ Kafka Engine. **Không validate, không transform.**

| Column | Type | Mô tả |
|--------|------|-------|
| `event_id` | String | UUID của payment event |
| `user_id` | UInt64 | ID user (1–100,000) |
| `amount` | Float64 | Số tiền raw (có thể âm, zero) |
| `status` | String | Trạng thái thô: `"paid"` / `"PAID"` / `"invalid_status"` / ... |
| `payment_date` | Date | Ngày thanh toán |
| `updated_at` | DateTime64(3) | Timestamp từ event-producer |
| `ingested_at` | DateTime64(3) | Thời gian CH Kafka Engine ghi (`now64(3)`) |

- Engine: `MergeTree`, partition `toYYYYMM(payment_date)`, order `(payment_date, event_id)`
- TTL: 90 ngày

### Silver: `tracking.silver_events`

Sau khi dbt_clickhouse chạy. Chỉ giữ records hợp lệ.

| Column | Type | Normalize rule |
|--------|------|---------------|
| `event_id` | String | Drop nếu blank |
| `user_id` | UInt64 | Drop nếu = 0 |
| `amount` | Float64 | Drop nếu ≤ 0 (null/âm/zero) |
| `status` | String | `lower(trim(status))` → nếu không hợp lệ → `'unknown'` |
| `payment_date` | Date | Giữ nguyên |
| `updated_at` | DateTime64(3) | Giữ nguyên |

Valid status values: `paid` | `pending` | `failed` | `refunded` | `cancelled` | `unknown`

**Status normalization:**

| Input | Output | Lý do |
|-------|--------|-------|
| `"paid"` | `"paid"` | Valid |
| `"PAID"` | `"paid"` | Lowercase |
| `"Paid "` | `"paid"` | Trim + lowercase |
| `null` / `""` | `"unknown"` | Missing |
| `"invalid_status"` | `"unknown"` | Không thuộc valid set |

---

## Analytics Queries

```sql
-- Status breakdown 7 ngày gần nhất
SELECT
    payment_date,
    countIf(status = 'paid')      AS paid,
    countIf(status = 'pending')   AS pending,
    countIf(status = 'failed')    AS failed,
    countIf(status = 'unknown')   AS unknown,
    count()                       AS total
FROM tracking.silver_events
WHERE payment_date >= today() - 7
GROUP BY payment_date
ORDER BY payment_date DESC;
```

```sql
-- Tổng revenue hôm nay
SELECT
    count()         AS total_transactions,
    countIf(status = 'paid')    AS paid_count,
    sum(amount)                 AS total_amount,
    avg(amount)                 AS avg_amount,
    uniq(user_id)               AS unique_users
FROM tracking.silver_events
WHERE payment_date = today();
```

```sql
-- Top 10 user giao dịch nhiều nhất (hôm nay)
SELECT
    user_id,
    count()         AS num_transactions,
    sum(amount)     AS total_amount
FROM tracking.silver_events
WHERE payment_date = today()
  AND status = 'paid'
GROUP BY user_id
ORDER BY num_transactions DESC
LIMIT 10;
```

```sql
-- Ingestion rate (Kafka → CH Bronze) theo phút
SELECT
    toStartOfMinute(ingested_at) AS minute,
    count()                       AS rows_ingested
FROM tracking.payments_bronze
WHERE ingested_at >= now64(3) - INTERVAL 10 MINUTE
GROUP BY minute
ORDER BY minute DESC;
```

---

## Data Quality

### Bronze vs Silver drop rate (hôm nay)

```sql
SELECT
    b.total_bronze,
    s.total_silver,
    b.total_bronze - s.total_silver AS dropped,
    round((b.total_bronze - s.total_silver) / b.total_bronze * 100, 2) AS drop_pct
FROM
    (SELECT count() AS total_bronze FROM tracking.payments_bronze
     WHERE payment_date = today()) b,
    (SELECT count() AS total_silver FROM tracking.silver_events
     WHERE payment_date = today()) s;
```

### Phân bổ bad data trong Bronze

```sql
SELECT
    status,
    count()              AS count,
    countIf(amount <= 0) AS bad_amount,
    min(amount)          AS min_amount,
    max(amount)          AS max_amount
FROM tracking.payments_bronze
WHERE payment_date = today()
GROUP BY status
ORDER BY count DESC;
```

### Disk usage

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
