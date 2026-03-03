# Setup

## Prerequisites

- Docker Desktop (4GB+ RAM allocated)
- Go 1.22+
- Bazel 9.0+

---

## Khởi động infrastructure

```bash
cd com/tm/docker
docker compose up -d

# Đợi ~2 phút cho tất cả services sẵn sàng
docker compose ps
```

Services khởi động theo thứ tự: Zookeeper → Kafka → ClickHouse → StarRocks → HBase → Airflow → Superset.

---

## Build Go services

```bash
bazel build //com/tm/src/services/...
```

---

## Chạy pipeline

```bash
# Terminal 1: Tracking Ingestor
# Consume "payment-events" → validate → forward → "payment-events-ingest"
# CH Kafka Engine sẽ tự đọc từ "payment-events-ingest"
bazel run //com/tm/src/services/tracking-ingestor:tracking-ingestor -- \
  --kafka=localhost:9092 \
  --input-topic=payment-events \
  --output-topic=payment-events-ingest \
  --group=payment-ingestor

# Terminal 2: Event Producer (sinh payment events)
bazel run //com/tm/src/services/event-producer:event-producer -- \
  --kafka=localhost:9092 \
  --rate=1000
```

Sau khi cả hai service chạy, ClickHouse Kafka Engine tự động consume `payment-events-ingest` và insert vào `payments_bronze` qua Materialized View.

---

## Trigger Airflow DAG

```bash
# Trigger pipeline thủ công
docker exec airflow-webserver airflow dags trigger medallion_pipeline

# Xem trạng thái các runs
docker exec airflow-webserver airflow dags list-runs --dag-id medallion_pipeline

# Xem log task cụ thể
docker exec airflow-webserver airflow tasks logs medallion_pipeline silver_layer.dbt_ch_run latest
```

---

## Chạy từng bước thủ công

```bash
# Sync silver CH → StarRocks (tham số: ngày cần sync)
bazel run //com/tm/src/services/analytics-aggregator:run_sync -- 2024-01-15

# Validate Airflow DAG
bazel test //com/tm/src/services/analytics-aggregator:dag_validation_test

# dbt debug trong container Airflow
docker exec airflow-worker bash -c "
  cd /opt/analytics/dbt_clickhouse &&
  dbt debug --profiles-dir . &&
  dbt run --profiles-dir . --target dev
"
```

---

## Service URLs

| Service | URL | Credentials |
|---------|-----|-------------|
| Kafka UI | http://localhost:8082 | — |
| ClickHouse Play | http://localhost:8123/play | `default` / (trống) |
| StarRocks | `mysql -h 127.0.0.1 -P 9030 -u root` | `root` / (trống) |
| Airflow | http://localhost:8081 | `admin` / `admin123` |
| Superset | http://localhost:8088 | `admin` / `admin123` |
| HBase Web UI | http://localhost:16010 | — |

---

## Troubleshooting

### Kafka không nhận được events

```bash
docker compose logs kafka

# Kiểm tra topic tồn tại
docker exec kafka kafka-topics \
  --bootstrap-server localhost:29092 --list

# Kiểm tra consumer lag của ingestor (Go service)
docker exec kafka kafka-consumer-groups \
  --bootstrap-server localhost:29092 \
  --describe --group payment-ingestor

# Kiểm tra consumer lag của CH Kafka Engine
docker exec kafka kafka-consumer-groups \
  --bootstrap-server localhost:29092 \
  --describe --group ch-payment-ingestor
```

### ClickHouse không có data trong payments_bronze

```bash
docker compose logs clickhouse

# Kiểm tra row count
curl 'http://localhost:8123/?query=SELECT+count()+FROM+tracking.payments_bronze'

# Kiểm tra Kafka Engine + MV có tồn tại không
curl 'http://localhost:8123/?query=SHOW+TABLES+FROM+tracking'

# Kiểm tra Kafka Engine consumers đang active
curl 'http://localhost:8123/?query=SELECT+*+FROM+system.kafka_consumers+WHERE+database=%27tracking%27'

# Xem ingestion rate gần đây
curl 'http://localhost:8123/?query=SELECT+toStartOfMinute(ingested_at)+AS+m,count()+FROM+tracking.payments_bronze+WHERE+ingested_at>=now64(3)-INTERVAL+5+MINUTE+GROUP+BY+m+ORDER+BY+m+DESC'
```

### dbt chạy bị lỗi

```bash
docker exec airflow-worker bash -c "
  cd /opt/analytics/dbt_clickhouse &&
  dbt debug --profiles-dir .
"
```

### StarRocks không nhận sync

```bash
# Test connection
mysql -h 127.0.0.1 -P 9030 -u root -e "SHOW DATABASES;"

# Kiểm tra silver mirror
mysql -h 127.0.0.1 -P 9030 -u root analytics \
  -e "SELECT COUNT(*) FROM silver_payments;"

# Chạy sync thủ công
bazel run //com/tm/src/services/analytics-aggregator:run_sync -- $(date +%Y-%m-%d)
```

### Airflow DAG stuck / failed

```bash
docker compose logs airflow-scheduler

# Trigger lại thủ công
docker exec airflow-webserver airflow dags trigger medallion_pipeline

# Clear task để chạy lại
docker exec airflow-webserver airflow tasks clear medallion_pipeline \
  --task-regex silver_layer --yes
```

### topic "payment-events-ingest" chưa tồn tại

Topic sẽ được Kafka tự tạo khi tracking-ingestor produce message lần đầu (do `KAFKA_AUTO_CREATE_TOPICS_ENABLE: "true"` trong docker-compose). Nếu cần tạo thủ công:

```bash
docker exec kafka kafka-topics \
  --bootstrap-server localhost:29092 \
  --create --topic payment-events-ingest \
  --partitions 6 --replication-factor 1
```
