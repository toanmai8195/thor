# Setup — Luồng 2: Warehouse DW (Iceberg + StarRocks)

## Prerequisites

- Docker Desktop (**8GB+ RAM** cho luồng này — StarRocks + Flink nặng)
- Go 1.22+, Bazel 9.0+

---

## Bước 1 — Build custom Docker images

```bash
cd com/tm/docker/dw

# Build Flink image (thêm Kafka connector + Iceberg runtime + S3 plugin)
docker compose build flink-jobmanager flink-taskmanager flink-iceberg-job
# Lần đầu ~5-10 phút (download JARs từ Maven)
```

---

## Bước 2 — Start infrastructure services

```bash
cd com/tm/docker/dw

docker compose up -d \
  kafka \
  kafka-ui \
  minio \
  starrocks
```

Đợi services healthy (~90s — StarRocks cần thời gian khởi động):

```bash
docker compose ps
# kafka: healthy
# minio: healthy
# starrocks: healthy (chờ ~60s)

# Kiểm tra StarRocks FE healthy
curl -sf http://localhost:8030/api/health
# {"status":"OK"}
```

---

## Bước 3 — Tạo MinIO bucket `lakehouse`

**Cách 1 — Web Console (đơn giản):**
Vào http://localhost:9001 → `minioadmin` / `minioadmin` → Buckets → Create Bucket → tên: `lakehouse` → Create.

**Cách 2 — CLI:**
```bash
docker run --rm --network tracking-network minio/mc:latest \
  sh -c "mc alias set local http://minio:9000 minioadmin minioadmin \
         && mc mb --ignore-existing local/lakehouse \
         && echo 'Bucket ready'"

# Xác nhận
docker run --rm --network tracking-network minio/mc:latest \
  sh -c "mc alias set local http://minio:9000 minioadmin minioadmin && mc ls local/"
# [bucket] lakehouse
```

---

## Bước 4 — Tạo Kafka topics

```bash
# Topic: raw events từ event-producer
docker exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create --if-not-exists \
  --topic payment-events \
  --partitions 6 --replication-factor 1

# Topic: validated events (tracking-ingestor → Flink consume từ đây)
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

## Bước 5 — Build & start Go services

```bash
# Build Docker images từ Bazel (chạy từ root repo)
bazel run //com/tm/src/services/event-producer:event-producer_docker
bazel run //com/tm/src/services/tracking-ingestor:tracking-ingestor_docker

# Start (từ com/tm/docker/dw/)
docker compose up -d event-producer tracking-ingestor
```

---

## Bước 6 — Tạo StarRocks Iceberg external catalog + tables

Chạy init SQL (tạo catalog + pre-create `silver_payments` table).

Script này tạo:

| Object | Loại | Mô tả |
|--------|------|-------|
| `iceberg_catalog` | EXTERNAL CATALOG | Trỏ vào MinIO `s3a://lakehouse/warehouse` |
| `tracking` | DATABASE | Native SR database cho Silver + Gold |
| `tracking.silver_payments` | PRIMARY KEY TABLE | Pre-create để dbt insert sau này |

**Cách 1 — Qua Docker (khuyến nghị — tránh lỗi MySQL 9.x):**

```bash
# chạy từ root repo (thor/)
docker exec -i starrocks mysql -h 127.0.0.1 -P 9030 -u root \
  < com/tm/docker/dw/starrocks/init/00_iceberg_catalog.sql
```

**Cách 2 — mysql client từ host (MySQL 8.x trở xuống):**

> MySQL 9.x không tương thích do đã bỏ `mysql_native_password`. Dùng Cách 1 thay thế.

```bash
# từ root repo
mysql -h 127.0.0.1 -P 9030 -u root < com/tm/docker/dw/starrocks/init/00_iceberg_catalog.sql
```

Xác nhận:

```sql
SHOW CATALOGS;
-- default_catalog
-- iceberg_catalog   ← phải thấy

SHOW TABLES FROM tracking;
-- silver_payments   ← phải thấy
```

---

## Bước 7 — Start Flink cluster + submit job

```bash
# từ com/tm/docker/dw/
docker compose up -d flink-jobmanager flink-taskmanager

# Đợi jobmanager healthy (~30s)
curl -sf http://localhost:8083/overview
# {"taskmanagers":1,...}

# Submit Flink SQL job: Kafka → Iceberg Bronze
docker compose up flink-iceberg-job
# Log cuối cùng phải có: "Job has been submitted with JobID ..."
```

`flink-iceberg-job` là one-shot container (chạy rồi exit). Job được submit vào Flink cluster, container sau đó exit 0.

Kiểm tra job đang RUNNING:

```bash
curl -sf http://localhost:8083/jobs | python3 -m json.tool
# "status": "RUNNING"

# Hoặc vào Flink Web UI: http://localhost:8083
```

---

## Bước 8 — Verify end-to-end

Flink commit Iceberg sau mỗi 60 giây (checkpoint interval). Đợi ~90s rồi kiểm tra:

```bash
# 1. Kiểm tra Parquet files trên MinIO
docker exec minio find /data/lakehouse -name "*.parquet" | head -5
# Phải thấy files trong: /data/lakehouse/warehouse/bronze/payments/...

# 2. Query Bronze qua StarRocks external catalog
mysql -h 127.0.0.1 -P 9030 -u root -e \
  "SELECT COUNT(*), MIN(payment_date), MAX(payment_date) \
   FROM iceberg_catalog.bronze.payments;"

# 3. Kiểm tra Flink consumer lag
docker exec kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 --describe --group flink-iceberg-bronze
```

---

## Troubleshooting

### Flink job fail khi submit

```bash
# Xem logs submission
docker logs flink-iceberg-job

# Re-submit
docker compose up flink-iceberg-job
```

### Iceberg không có data sau 2 phút

```bash
# Kiểm tra checkpoint trong Flink UI
# http://localhost:8083 → Jobs → kafka_to_iceberg → Checkpoints

# Kiểm tra MinIO có nhận files không
docker exec minio find /data/lakehouse -type f | head -20

# Kiểm tra S3 plugin
docker exec flink-jobmanager ls /opt/flink/plugins/s3-fs-hadoop/
# Phải có file flink-s3-fs-hadoop-*.jar

# Rebuild nếu thiếu plugin (từ com/tm/docker/dw/)
docker compose build --no-cache flink-jobmanager flink-taskmanager flink-iceberg-job
```

### StarRocks query Iceberg trả về empty / lỗi

```bash
# Chỉ có data sau khi Flink commit >= 1 checkpoint (~60s)
# Kiểm tra catalog còn tồn tại không
mysql -h 127.0.0.1 -P 9030 -u root -e "SHOW CATALOGS;"

# Nếu mất catalog (SR restart), chạy lại init
mysql -h 127.0.0.1 -P 9030 -u root < docker/dw/starrocks/init/00_iceberg_catalog.sql
```

### MinIO lỗi kết nối từ Flink / SR

```bash
# Kiểm tra MinIO healthy
curl http://localhost:9000/minio/health/live

# Kiểm tra bucket tồn tại
docker run --rm --network tracking-network minio/mc:latest \
  sh -c "mc alias set local http://minio:9000 minioadmin minioadmin && mc ls local/"
```

---

## Service URLs

| Service | URL / Host | Credentials |
|---------|-----------|-------------|
| Kafka UI | http://localhost:8082 | — |
| MinIO Console | http://localhost:9001 | `minioadmin` / `minioadmin` |
| MinIO S3 API | http://localhost:9000 | — |
| Flink Web UI | http://localhost:8083 | — |
| StarRocks MySQL | `mysql -h 127.0.0.1 -P 9030 -u root` | `root` / (trống) |
| StarRocks FE HTTP | http://localhost:8030 | `root` / (trống) |
