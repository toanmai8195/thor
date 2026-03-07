# Setup — Luồng 1: Realtime (ClickHouse)

## Prerequisites

- Docker Desktop (**6GB+ RAM** cho luồng này)
- Go 1.22+, Bazel 9.0+

---

## Bước 1 — Build custom Docker images

```bash
cd com/tm/docker/realtime

# Build Airflow image (thêm dbt-clickhouse)
docker compose build airflow-webserver airflow-scheduler

# Build Superset image (thêm clickhouse-connect)
docker compose build superset

# Build dbt-silver image
docker compose build dbt-silver
```

---

## Bước 2 — Start infrastructure services

```bash
cd com/tm/docker/realtime

docker compose up -d \
  kafka \
  kafka-ui \
  clickhouse \
  airflow-postgres \
  airflow-webserver \
  airflow-scheduler \
  superset
```

Đợi services healthy (~60s):

```bash
docker compose ps
# clickhouse: healthy
# airflow-postgres: healthy
# airflow-webserver: running (port 8081)
# superset: running (port 8088)
```

---

## Bước 3 — Build & start Go services

```bash
# Build Docker images từ Bazel (chạy từ root repo)
bazel run //com/tm/src/services/event-producer:event-producer_docker
bazel run //com/tm/src/services/tracking-ingestor:tracking-ingestor_docker

# Start (từ com/tm/docker/realtime/)
docker compose up -d event-producer tracking-ingestor
```

---

## Bước 4 — Tạo Kafka topics

Kafka `auto.create.topics.enable=true` nên topics tự tạo khi producer/consumer connect.
Nếu muốn tạo thủ công trước:

```bash
# Topic: raw events từ event-producer
docker exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create --if-not-exists \
  --topic payment-events \
  --partitions 6 --replication-factor 1

# Topic: validated events từ tracking-ingestor → ClickHouse + Flink consume từ đây
docker exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create --if-not-exists \
  --topic payment-events-ingest \
  --partitions 6 --replication-factor 1

# Xác nhận
docker exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --list
```

---

## Bước 5 — ClickHouse tự init tables

ClickHouse chạy scripts trong `/docker-entrypoint-initdb.d/` khi khởi động lần đầu:

| Script | Tạo gì |
|--------|--------|
| `realtime/clickhouse/init/01_bronze.sql` | Database `tracking`, Kafka Engine `payments_kafka`, MV `payments_bronze_mv`, table `payments_bronze` |
| `realtime/clickhouse/init/02_silver.sql` | Table `silver_events` (pre-create để dbt insert) |

Xác nhận:

```bash
curl 'http://localhost:8123/?query=SHOW+TABLES+FROM+tracking'
# payments_bronze
# payments_bronze_mv
# payments_kafka
# silver_events
```

ClickHouse bắt đầu consume `payment-events-ingest` ngay khi Kafka Engine tạo xong.

---

## Bước 6 — Airflow: init DB + tạo admin

```bash
docker exec airflow-webserver airflow db upgrade

docker exec airflow-webserver airflow users create \
  --username admin \
  --password admin123 \
  --firstname Admin \
  --lastname Admin \
  --role Admin \
  --email admin@example.com
```

Airflow UI: http://localhost:8081 → `admin` / `admin123`

DAGs sẽ tự load từ volume mount (`analytics-aggregator/realtime/dags/`):

| DAG | Schedule | Nhiệm vụ |
|-----|----------|---------|
| `silver_ingest` | `*/1 * * * *` | `dbt run` → `silver_events` |
| `medallion_pipeline` | On-demand | Full Bronze → Silver rebuild |

---

## Bước 7 — Superset: init + kết nối ClickHouse

```bash
docker exec superset superset db upgrade

docker exec superset superset fab create-admin \
  --username admin \
  --firstname Admin \
  --lastname Admin \
  --email admin@superset.com \
  --password admin

docker exec superset superset init
```

Superset UI: http://localhost:8088 → `admin` / `admin`

Thêm ClickHouse database connection:
Settings → Database Connections → + → Other → SQLAlchemy URI:
```
clickhousedb://default:@clickhouse:8123/tracking
```

---

## Bước 8 — Verify end-to-end

```bash
# 1. Kiểm tra Bronze đang nhận data (chờ ~10s sau khi start)
curl 'http://localhost:8123/?query=SELECT+count()+FROM+tracking.payments_bronze'

# 2. Trigger dbt silver thủ công (tự chạy mỗi 1 phút)
docker exec airflow-webserver airflow dags trigger silver_ingest

# 3. Kiểm tra Silver (chờ ~30s)
curl 'http://localhost:8123/?query=SELECT+count()+FROM+tracking.silver_events'

# 4. Ingestion rate (last 5 phút)
curl 'http://localhost:8123/?query=SELECT+toStartOfMinute(ingested_at)+AS+m,count()+FROM+tracking.payments_bronze+WHERE+ingested_at>=now()-INTERVAL+5+MINUTE+GROUP+BY+m+ORDER+BY+m'
```

---

## Troubleshooting

### Bronze trống sau 30s

```bash
# Kiểm tra tracking-ingestor đang chạy
docker compose logs tracking-ingestor

# Kiểm tra Kafka consumer lag
docker exec kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 --describe --group ch-payment-ingestor

# Kiểm tra ClickHouse Kafka Engine
curl 'http://localhost:8123/?query=SELECT+*+FROM+system.kafka_consumers'
```

### Airflow DAG không load

```bash
docker exec airflow-webserver airflow dags list
docker exec airflow-webserver ls /opt/airflow/dags/
# Phải thấy: silver_ingest_dag.py
```

### Superset lỗi kết nối ClickHouse

```bash
# Kiểm tra clickhouse-connect đã install trong venv
docker exec superset /app/.venv/bin/python -c "import clickhouse_connect; print('OK')"
```

---

## Service URLs

| Service | URL | Credentials |
|---------|-----|-------------|
| Kafka UI | http://localhost:8082 | — |
| ClickHouse HTTP | http://localhost:8123/play | `default` / (trống) |
| ClickHouse Native TCP | `localhost:19000` | `default` / (trống) |
| Airflow | http://localhost:8081 | `admin` / `admin123` |
| Superset | http://localhost:8088 | `admin` / `admin` |
