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

# Build Airflow image (thêm dbt-starrocks + mysql-client)
docker compose build airflow-webserver airflow-scheduler

# Build dbt-silver image (dbt-starrocks)
docker compose build dbt-silver
```

---

## Bước 2 — Start infrastructure services

```bash
cd com/tm/docker/dw

docker compose up -d \
  kafka \
  kafka-ui \
  minio \
  starrocks \
  airflow-postgres \
  airflow-webserver \
  airflow-scheduler
```

Đợi services healthy (~90s — StarRocks cần thời gian khởi động):

```bash
docker compose ps
# kafka: healthy
# minio: healthy
# starrocks: healthy (chờ ~60s)
# airflow-postgres: healthy
# airflow-webserver: running (port 8084)

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
| `tracking` | DATABASE | Native SR database — Silver layer |
| `tracking.silver_payments` | PRIMARY KEY TABLE | Pre-create để dbt_starrocks_silver UPSERT |
| `analytics` | DATABASE | Native SR database — Gold layer (dbt tự tạo table) |

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

SHOW DATABASES;
-- tracking          ← Silver DB
-- analytics         ← Gold DB

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

## Bước 8 — Airflow: init DB + tạo admin

```bash
docker exec dw-airflow-webserver airflow db migrate

docker exec dw-airflow-webserver airflow users create \
  --username admin \
  --password admin123 \
  --firstname Admin \
  --lastname Admin \
  --role Admin \
  --email admin@example.com
```

Airflow UI: http://localhost:8084 → `admin` / `admin123`

DAGs sẽ tự load từ volume mount (`analytics-aggregator/dw/dags/`):

| DAG | Schedule | Nhiệm vụ |
|-----|----------|---------|
| `dw_medallion_pipeline` | `*/3 * * * *` | Full pipeline: Bronze check → Silver (incremental UPSERT) → Gold (rebuild) |

---

## Bước 9 — Superset: init + kết nối StarRocks

```bash
docker exec dw-superset superset db upgrade

docker exec dw-superset superset fab create-admin \
  --username admin \
  --firstname Admin \
  --lastname Admin \
  --email admin@superset.com \
  --password admin

docker exec dw-superset superset init
```

Superset UI: http://localhost:8088 → `admin` / `admin`

Thêm StarRocks database connection:
Settings → Database Connections → + → Other → SQLAlchemy URI:
```
starrocks+pymysql://root:@starrocks:9030/analytics
```

### Chỉ số có thể visualize

Source: `analytics.gold_revenue` — grain 1 row = 1 ngày (`payment_date`)

**Bước 1 — Thêm dataset:**
Datasets → + Dataset → Database: `StarRocks` → Schema: `analytics` → Table: `gold_revenue` → Create dataset and create chart

**Bước 2 — Tạo từng chart** (Charts → + Chart → chọn dataset `gold_revenue`):

---

#### Doanh thu theo ngày — Line chart

| Field | Giá trị |
|-------|---------|
| Chart type | Line Chart |
| Time column | `payment_date` |
| Time grain | Day |
| Metrics | `SUM(total_revenue)` |
| Series | _(để trống)_ |

---

#### Số đơn thành công theo ngày — Bar chart

| Field | Giá trị |
|-------|---------|
| Chart type | Bar Chart |
| Time column | `payment_date` |
| Time grain | Day |
| Metrics | `SUM(total_paid_orders)` |

---

#### Giá trị đơn trung bình (AOV) — Line chart

`avg_order_value` đã được tính sẵn trong Gold (= `total_revenue / total_paid_orders` per ngày), chỉ cần AVG hoặc SUM đều cho kết quả đúng vì grain là ngày.

| Field | Giá trị |
|-------|---------|
| Chart type | Line Chart |
| Time column | `payment_date` |
| Time grain | Day |
| Metrics | `AVG(avg_order_value)` |

---

#### Số user unique theo ngày — Line chart

| Field | Giá trị |
|-------|---------|
| Chart type | Line Chart |
| Time column | `payment_date` |
| Time grain | Day |
| Metrics | `SUM(unique_users)` |

---

#### Tỷ lệ đơn theo trạng thái — Pie chart

| Field | Giá trị |
|-------|---------|
| Chart type | Pie Chart |
| Dimensions | _(không dùng time column)_ |
| Metrics | `SUM(total_paid_orders)`, `SUM(pending_orders)`, `SUM(failed_orders)`, `SUM(refunded_orders)`, `SUM(cancelled_orders)` |

> Hoặc dùng **Bar Chart** với Breakdown by: tạo 5 metrics riêng, enable "Stack".

---

#### Tổng doanh thu (Big Number)

| Field | Giá trị |
|-------|---------|
| Chart type | Big Number with Trendline |
| Time column | `payment_date` |
| Time grain | Day |
| Metric | `SUM(total_revenue)` |

---

#### Revenue theo tuần — Bar chart

| Field | Giá trị |
|-------|---------|
| Chart type | Bar Chart |
| Time column | `payment_date` |
| Time grain | **Week** |
| Metrics | `SUM(total_revenue)` |

---

## Bước 10 — Verify end-to-end

Flink commit Iceberg sau mỗi 60 giây (checkpoint interval). Đợi ~90s rồi kiểm tra:

```bash
# 1. Kiểm tra Parquet files trên MinIO
docker exec minio find /data/lakehouse -name "*.parquet" | head -5
# Phải thấy files trong: /data/lakehouse/warehouse/bronze/payments/...

# 2. Query Bronze qua StarRocks external catalog
mysql -h 127.0.0.1 -P 9030 -u root -e \
  "SELECT COUNT(*), MIN(payment_date), MAX(payment_date) \
   FROM iceberg_catalog.bronze.payments;"

# 3. Trigger pipeline thủ công (tự chạy mỗi 3 phút)
docker exec dw-airflow-webserver airflow dags trigger dw_medallion_pipeline

# 4. Kiểm tra Silver sau ~30s
mysql -h 127.0.0.1 -P 9030 -u root -e \
  "SELECT COUNT(*), MIN(payment_date), MAX(payment_date) \
   FROM tracking.silver_payments;"

# 6. Kiểm tra Gold sau ~60s
mysql -h 127.0.0.1 -P 9030 -u root -e \
  "SELECT payment_date, total_revenue, total_paid_orders, unique_users \
   FROM analytics.gold_revenue \
   ORDER BY payment_date DESC LIMIT 5;"

# 7. Kiểm tra Flink consumer lag
docker exec kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 --describe --group flink-iceberg-bronze
```

---

## Cập nhật config

> **Cơ chế reload:** DAG files và dbt models được mount vào container bằng **bind mount** (volume trỏ thẳng vào thư mục trên host). Container đọc trực tiếp từ filesystem host — khi file thay đổi trên host, container thấy ngay. Airflow scheduler chủ động poll thư mục DAGs mỗi ~30s; dbt không chạy liên tục mà spawn process mới mỗi lần trigger nên luôn đọc file mới nhất.
>
> Ngược lại, **Dockerfile** thay đổi (thêm package, JAR) thì code nằm trong image layer — container đang chạy không thể tự cập nhật, bắt buộc phải rebuild image và restart container.

| Thay đổi | Cơ chế | Cần làm gì |
|----------|--------|-----------|
| DAG files (`dags/*.py`) | Bind mount + Airflow poll 30s | Không cần restart |
| dbt models (`dbt_silver/**`, `dbt/**`) | Bind mount + dbt spawn fresh process | Không cần restart |
| `flink/kafka_to_iceberg.sql` | Bind mount — nhưng Flink job đã chạy, không tự reload | Cancel job cũ + re-submit |
| `starrocks/init/*.sql` | Chỉ chạy lần đầu khởi động container | Re-run thủ công |
| `Dockerfile.airflow` (thêm package Python, system lib) | Code trong image layer | Rebuild + restart Airflow |
| `Dockerfile.flink` (thêm connector JAR) | Code trong image layer | Rebuild + restart Flink + re-submit job |
| `Dockerfile.superset` (thêm driver DB) | Code trong image layer | Rebuild + restart Superset |
| `docker-compose.yml` (env vars, ports, volumes) | Docker Compose config | `docker compose up -d` |

### Rebuild + restart Airflow

```bash
cd com/tm/docker/dw
docker compose build airflow-webserver airflow-scheduler
docker compose up -d airflow-webserver airflow-scheduler
```

### Rebuild + restart Flink + re-submit job

```bash
cd com/tm/docker/dw

# Cancel job đang chạy
JOB_ID=$(curl -sf http://localhost:8083/jobs | python3 -c \
  "import sys,json; jobs=json.load(sys.stdin)['jobs']; \
   print(next(j['id'] for j in jobs if j['status']=='RUNNING'), end='')")
curl -sf -X PATCH "http://localhost:8083/jobs/$JOB_ID?mode=cancel"

# Rebuild image
docker compose build flink-jobmanager flink-taskmanager flink-iceberg-job

# Restart cluster
docker compose up -d flink-jobmanager flink-taskmanager

# Re-submit job (đợi cluster healthy ~15s)
docker compose up flink-iceberg-job
```

### Cập nhật Flink SQL (kafka_to_iceberg.sql) không rebuild image

```bash
# File được mount vào container, chỉ cần cancel + re-submit
JOB_ID=$(curl -sf http://localhost:8083/jobs | python3 -c \
  "import sys,json; jobs=json.load(sys.stdin)['jobs']; \
   print(next(j['id'] for j in jobs if j['status']=='RUNNING'), end='')")
curl -sf -X PATCH "http://localhost:8083/jobs/$JOB_ID?mode=cancel"
docker compose up flink-iceberg-job
```

### Rebuild + restart Superset

```bash
cd com/tm/docker/dw
docker compose build superset
docker compose up -d superset
```

### Re-run StarRocks init SQL

```bash
# IF NOT EXISTS — an toàn với data hiện có
docker exec -i starrocks mysql -h 127.0.0.1 -P 9030 -u root \
  < com/tm/docker/dw/starrocks/init/00_iceberg_catalog.sql
```

### Cập nhật docker-compose.yml (env vars, ports, volumes)

```bash
cd com/tm/docker/dw
docker compose up -d          # chỉ restart container bị thay đổi config
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

### Airflow DAG không load

```bash
docker exec dw-airflow-webserver airflow dags list
docker exec dw-airflow-webserver ls /opt/airflow/dags/
# Phải thấy: medallion_pipeline_dag.py
```

### Silver trống sau Airflow chạy

```bash
# Kiểm tra dbt run logs trong Airflow UI: http://localhost:8084
# Hoặc check task log:
docker exec dw-airflow-webserver airflow tasks logs dw_medallion_pipeline silver_layer.dbt_silver_run $(date +%Y-%m-%dT%H:%M:%S)

# Verify Iceberg Bronze đã có data
mysql -h 127.0.0.1 -P 9030 -u root -e "SELECT COUNT(*) FROM iceberg_catalog.bronze.payments;"
# Phải > 0 (nếu = 0: Flink chưa commit, đợi thêm 60s)
```

### Gold trống / analytics.gold_revenue không tồn tại

```bash
# Kiểm tra analytics DB tồn tại
mysql -h 127.0.0.1 -P 9030 -u root -e "SHOW DATABASES;"

# Nếu mất (SR restart), chạy lại init
docker exec -i starrocks mysql -h 127.0.0.1 -P 9030 -u root \
  < com/tm/docker/dw/starrocks/init/00_iceberg_catalog.sql

# Re-trigger Gold build
docker exec dw-airflow-webserver airflow dags trigger dw_medallion_pipeline
```

### Superset lỗi kết nối StarRocks

```bash
# Kiểm tra starrocks package đã install
docker exec dw-superset /app/.venv/bin/python -c "import starrocks; print('OK')"

# Test kết nối thủ công
docker exec dw-superset python3 -c "
import sqlalchemy as sa
e = sa.create_engine('starrocks+pymysql://root:@starrocks:9030/analytics')
print(e.execute('SELECT 1').fetchone())
"
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
| Airflow | http://localhost:8084 | `admin` / `admin123` |
| Superset | http://localhost:8088 | `admin` / `admin` |
