# Setup — Luồng 1: Realtime (ClickHouse)

Pipeline: `Kafka → ClickHouse Kafka Engine → cdp.login_bronze → dbt (1min) → cdp.silver_login`

Tham khảo kiến trúc đầy đủ: [pipeline-realtime.md](pipeline-realtime.md)

---

## Services liên quan

| Service | Container | Port |
|---------|-----------|------|
| Kafka | `cdp-kafka` | internal only |
| ClickHouse | `cdp-clickhouse` | `8124` (HTTP) |
| Airflow webserver | `cdp-airflow-webserver` | `8086` |
| Airflow scheduler | `cdp-airflow-scheduler` | — |
| Superset | `cdp-superset` | `8089` |

---

## Khởi động

```bash
cd com/tm/docker/cdp

# Khởi động toàn bộ (ClickHouse + Kafka + Airflow + Superset)
docker compose up -d --build kafka clickhouse login-event-producer cdp-ingestor \
  airflow-postgres airflow-webserver airflow-scheduler superset

# Hoặc start tất cả (bao gồm cả Luồng 2)
docker compose up -d --build
```

**Thứ tự healthcheck:**
```
kafka (healthy) → clickhouse (healthy) → login-event-producer + cdp-ingestor
airflow-postgres (healthy) → airflow-webserver + airflow-scheduler
starrocks (healthy) → superset
```

Chờ ~**90 giây** để Kafka + ClickHouse healthy.

---

## Verify từng bước

### 1. Kafka topics có data

```bash
# Kafka UI
open http://localhost:9082
# Kiểm tra topic login-events-ingest có messages tăng liên tục
```

### 2. ClickHouse init SQL đã chạy

```bash
# Liệt kê tables trong database cdp
curl -s "http://localhost:8124/?query=SHOW+TABLES+FROM+cdp"
# Phải thấy: login_bronze, login_bronze_mv, login_kafka, silver_login
```

### 3. Kafka Engine đang consume

```bash
# Xem consumer group ch-cdp-login
curl -s "http://localhost:8124/?query=SELECT+*+FROM+system.kafka_consumers&default_format=Vertical"
# Cột "rdkafka_stat" — lag giảm dần là đang consume
```

### 4. Bronze đang nhận data

```bash
# Đếm rows
curl -s "http://localhost:8124/?query=SELECT+COUNT()+FROM+cdp.login_bronze"

# Sample 5 rows
curl -s "http://localhost:8124/?query=SELECT+*+FROM+cdp.login_bronze+LIMIT+5&default_format=JSONEachRow"

# Phân bố platform/country
curl -s "http://localhost:8124/?query=SELECT+platform,country,COUNT()+c+FROM+cdp.login_bronze+GROUP+BY+1,2+ORDER+BY+c+DESC+LIMIT+10"
```

### 5. Airflow DAG `cdp_realtime_pipeline`

```bash
open http://localhost:8086
# Login: admin / admin123
# DAG: cdp_realtime_pipeline — phải Active, schedule */1 * * * *
```

Sau lần chạy đầu tiên (~1 phút):
```bash
# Silver đã có data
curl -s "http://localhost:8124/?query=SELECT+COUNT()+FROM+cdp.silver_login"

# So sánh bronze vs silver
curl -s "http://localhost:8124/?query=SELECT+(SELECT+COUNT()+FROM+cdp.login_bronze)+as+bronze,(SELECT+COUNT()+FROM+cdp.silver_login)+as+silver"
```

### 6. Superset kết nối ClickHouse

```bash
open http://localhost:8089
# Login: admin / admin
```

Thêm database connection:
- **Database:** ClickHouse CDP Realtime
- **SQLAlchemy URI:** `clickhousedb://default@clickhouse:8123/cdp`

Query thử:
```sql
SELECT event_date, platform, COUNT(*) as logins
FROM cdp.silver_login
GROUP BY event_date, platform
ORDER BY event_date DESC
LIMIT 30
```

---

## Troubleshooting

**ClickHouse không có Bronze data sau 30 giây:**
```bash
# Xem logs ClickHouse (kafka engine errors)
docker logs cdp-clickhouse 2>&1 | grep -i kafka | tail -20

# Kiểm tra Kafka Engine bằng system table
curl -s "http://localhost:8124/?query=SELECT+*+FROM+system.errors+WHERE+name+LIKE+'%kafka%'&default_format=Vertical"

# Fix: restart ClickHouse để Kafka Engine reconnect
docker compose restart clickhouse
```

**Materialized View không insert vào Bronze:**
```bash
# Xem errors của MV
curl -s "http://localhost:8124/?query=SELECT+*+FROM+system.errors&default_format=Vertical"

# Kiểm tra có rows lỗi parse không (lọc bởi MV, không vào bronze)
curl -s "http://localhost:8124/?query=SELECT+COUNT()+FROM+cdp.login_kafka+WHERE+_error+!=+''"
```

**dbt Silver run thất bại (Airflow task đỏ):**
```bash
# Xem logs task trong Airflow UI → cdp_realtime_pipeline → dbt_silver_run → Log

# Hoặc chạy tay trong container
docker exec cdp-airflow-webserver \
  bash -c "cd /opt/analytics/cdp/dbt_realtime && dbt run --profiles-dir . --select silver_login --target dev"
```

**Silver rows ít hơn Bronze nhiều:**

Đây là bình thường — dbt chỉ chạy mỗi 1 phút, Bronze tích lũy liên tục.
Silver sẽ catch up sau vài lần DAG chạy.
Ngoài ra ReplacingMergeTree deduplicate nên silver ≤ bronze về mặt lý thuyết.
