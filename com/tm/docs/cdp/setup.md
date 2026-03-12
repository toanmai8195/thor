# CDP Stack — Setup Guide

Hướng dẫn khởi động toàn bộ CDP stack từ đầu.
Stack bao gồm cả 2 luồng: Realtime (ClickHouse) + DW (Iceberg/StarRocks).

---

## Prerequisites

- Docker Desktop >= 24, RAM >= 8 GB allocated
- Bazel >= 7 (để build Go images)
- `mysql` client (để query StarRocks)

---

## Bước 1 — Build Go Docker Images

```bash
# Từ repo root
bazel run //com/tm/src/services/login-event-producer:login-event-producer_docker
bazel run //com/tm/src/services/cdp-ingestor:cdp-ingestor_docker
```

Images được tag là `com.tm.go.login-event-producer:v1.0.0` và `com.tm.go.cdp-ingestor:v1.0.0`.

---

## Bước 2 — Khởi động Stack

```bash
cd com/tm/docker/cdp

# Build custom images (Flink, Airflow, Superset) + khởi động tất cả services
docker compose up -d --build

# Kiểm tra trạng thái
docker compose ps
```

**Thứ tự khởi động tự động:**
```
kafka → clickhouse → (login-event-producer + cdp-ingestor)
      → minio → starrocks → flink-jobmanager → flink-taskmanager → flink-login-job
      → airflow-postgres → (airflow-webserver + airflow-scheduler)
      → superset
```

Chờ khoảng **2-3 phút** để tất cả healthchecks pass.

---

## Bước 3 — Verify Kafka Topics

```bash
# Mở Kafka UI
open http://localhost:9082

# Hoặc kiểm tra qua docker
docker exec cdp-kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --list
```

Phải có 2 topics: `login-events` và `login-events-ingest`.

---

## Bước 4 — Verify Luồng 1: ClickHouse Realtime

```bash
# Kiểm tra Bronze (Kafka Engine ghi liên tục)
curl -s "http://localhost:8124/?query=SELECT+COUNT()+FROM+cdp.login_bronze"

# Kiểm tra Silver (sau khi Airflow DAG cdp_realtime_pipeline chạy lần đầu)
curl -s "http://localhost:8124/?query=SELECT+COUNT()+FROM+cdp.silver_login"

# Xem sample data
curl -s "http://localhost:8124/?query=SELECT+*+FROM+cdp.silver_login+LIMIT+5&default_format=JSONEachRow"
```

---

## Bước 5 — Verify Luồng 2: DW (Iceberg + StarRocks)

```bash
# Kiểm tra Flink job đã submit thành công
open http://localhost:8085  # Flink Web UI — phải thấy 1 running job

# Sau ~2 phút, Iceberg Bronze bắt đầu có data
mysql -h 127.0.0.1 -P 9031 -u root -e \
  "SELECT COUNT(*) FROM iceberg_catalog.bronze.login_events;"

# Sau khi Airflow DAG cdp_medallion_pipeline chạy (3 phút) — Silver + Gold
mysql -h 127.0.0.1 -P 9031 -u root -e \
  "SELECT COUNT(*) FROM cdp.silver_login;
   SELECT * FROM cdp.gold_user_daily ORDER BY event_date DESC LIMIT 5;"
```

---

## Bước 6 — Verify Airflow DAGs

```bash
open http://localhost:8086
# Login: admin / admin123
```

Phải có 2 DAGs đang active:
- `cdp_realtime_pipeline` — schedule `*/1 * * * *` (mỗi 1 phút)
- `cdp_medallion_pipeline` — schedule `*/3 * * * *` (mỗi 3 phút)

---

## Bước 7 — Superset

```bash
open http://localhost:8089
# Login: admin / admin
```

Kết nối 2 databases:
- **ClickHouse** (Luồng 1): `clickhousedb://default@clickhouse:8123/cdp`
- **StarRocks** (Luồng 2): `starrocks+pymysql://root:@starrocks:9030/cdp`

---

## Dừng Stack

```bash
cd com/tm/docker/cdp

# Dừng nhưng giữ volumes (data)
docker compose down

# Dừng và xóa toàn bộ data (reset hoàn toàn)
docker compose down -v
```

---

## Troubleshooting

**Flink job không submit:**
```bash
docker logs cdp-flink-login-job
# Thường do MinIO chưa ready — restart job container
docker compose restart flink-login-job
```

**ClickHouse không có Bronze data:**
```bash
# Kiểm tra Kafka Engine có consume không
curl -s "http://localhost:8124/?query=SELECT+*+FROM+system.kafka_consumers+FORMAT+Vertical"
# Nếu lag tăng mà không giảm → restart ClickHouse
docker compose restart clickhouse
```

**StarRocks không query được Iceberg:**
```bash
mysql -h 127.0.0.1 -P 9031 -u root -e "SHOW CATALOGS;"
# Phải thấy iceberg_catalog
# Nếu không có → StarRocks chưa apply init SQL, check logs:
docker logs cdp-starrocks | grep -i iceberg
```

**Airflow DAG không trigger:**
```bash
docker logs cdp-airflow-scheduler | tail -50
# Kiểm tra dbt path trong container
docker exec cdp-airflow-scheduler ls /opt/analytics/cdp/
```
