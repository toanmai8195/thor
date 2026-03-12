# Setup — Luồng 2: DW (Iceberg + StarRocks)

Pipeline: `Kafka → Flink → Iceberg Bronze (MinIO) → dbt (3min) → SR Silver → dbt (3min) → SR Gold`

Tham khảo kiến trúc đầy đủ: [pipeline-dw.md](pipeline-dw.md)

---

## Services liên quan

| Service | Container | Port |
|---------|-----------|------|
| Kafka | `cdp-kafka` | internal only |
| MinIO | `cdp-minio` | `9010` (S3), `9011` (Console) |
| Flink JobManager | `cdp-flink-jobmanager` | `8085` (Web UI) |
| Flink TaskManager | `cdp-flink-taskmanager` | — |
| Flink login job | `cdp-flink-login-job` | — (one-shot submit) |
| StarRocks | `cdp-starrocks` | `9031` (MySQL), `8031` (FE HTTP) |
| Airflow webserver | `cdp-airflow-webserver` | `8086` |
| Airflow scheduler | `cdp-airflow-scheduler` | — |
| Superset | `cdp-superset` | `8089` |

---

## Khởi động

```bash
cd com/tm/docker/cdp

# Khởi động toàn bộ (bao gồm cả Luồng 1)
docker compose up -d --build

# Kiểm tra trạng thái
docker compose ps
```

**Thứ tự healthcheck (Luồng 2):**
```
kafka (healthy) → cdp-ingestor
minio (healthy) → flink-jobmanager (healthy) → flink-taskmanager → flink-login-job
starrocks (healthy, ~60s) → superset
airflow-postgres (healthy) → airflow-webserver + airflow-scheduler
```

Chờ ~**3 phút** để StarRocks fully started (FE + BE initialization).

---

## Verify từng bước

### 1. MinIO bucket khởi tạo

```bash
open http://localhost:9011
# Login: minioadmin / minioadmin
# Bucket cdp-lake phải tồn tại (tự tạo bởi Flink lần đầu ghi)
```

Hoặc qua CLI:
```bash
docker exec cdp-minio mc ls /data/
# Sau Flink ghi lần đầu: thấy cdp-lake/
```

### 2. Flink job đang chạy

```bash
open http://localhost:8085
# Phải thấy 1 running job: "kafka_login_to_iceberg"
# Status: RUNNING, với source (Kafka) và sink (Iceberg)
```

Kiểm tra qua API:
```bash
curl -s http://localhost:8085/jobs | python3 -m json.tool
# jobs[0].status = "RUNNING"
```

Nếu job ở trạng thái FAILED:
```bash
docker logs cdp-flink-login-job
```

### 3. Iceberg Bronze có data (~2 phút sau khi Flink chạy)

```bash
# Flink checkpoint mỗi 60s → file commit sau mỗi checkpoint
# Kiểm tra files trong MinIO
open http://localhost:9011
# Navigate: cdp-lake → iceberg → bronze → login_events → data/

# Kiểm tra qua StarRocks catalog
mysql -h 127.0.0.1 -P 9031 -u root -e \
  "SELECT COUNT(*) FROM iceberg_catalog.bronze.login_events;"
```

### 4. StarRocks catalog + schema

```bash
mysql -h 127.0.0.1 -P 9031 -u root <<'EOF'
-- Kiểm tra catalogs
SHOW CATALOGS;

-- Kiểm tra database cdp
SHOW DATABASES FROM default_catalog;

-- Kiểm tra tables trong cdp
SHOW TABLES FROM cdp;

-- Phải thấy: silver_login, gold_user_daily
EOF
```

### 5. Airflow DAG `cdp_medallion_pipeline`

```bash
open http://localhost:8086
# Login: admin / admin123
# DAG: cdp_medallion_pipeline — Active, schedule */3 * * * *
```

Sau lần chạy đầu tiên (~3 phút kể từ khi Iceberg có data):
```bash
# Silver
mysql -h 127.0.0.1 -P 9031 -u root -e \
  "SELECT COUNT(*) FROM cdp.silver_login;"

# Gold
mysql -h 127.0.0.1 -P 9031 -u root -e \
  "SELECT * FROM cdp.gold_user_daily ORDER BY event_date DESC LIMIT 7;"
```

### 6. Full end-to-end check

```bash
mysql -h 127.0.0.1 -P 9031 -u root <<'EOF'
-- Rows theo từng layer
SELECT
    (SELECT COUNT(*) FROM iceberg_catalog.bronze.login_events) AS bronze_rows,
    (SELECT COUNT(*) FROM cdp.silver_login)                    AS silver_rows,
    (SELECT COUNT(*) FROM cdp.gold_user_daily)                 AS gold_rows;

-- Gold metrics hôm nay
SELECT event_date, dau, total_logins, ios_logins, android_logins, web_logins,
       unique_sessions, unique_devices
FROM cdp.gold_user_daily
ORDER BY event_date DESC
LIMIT 5;
EOF
```

### 7. Superset kết nối StarRocks

```bash
open http://localhost:8089
# Login: admin / admin
```

Thêm database connection:
- **Database:** StarRocks CDP DW
- **SQLAlchemy URI:** `starrocks+pymysql://root:@starrocks:9030/cdp`

Query thử:
```sql
SELECT event_date, dau, total_logins, ios_logins, android_logins, web_logins
FROM cdp.gold_user_daily
ORDER BY event_date DESC
LIMIT 30
```

---

## Troubleshooting

**Flink job không submit / container exit ngay:**
```bash
docker logs cdp-flink-login-job

# Nguyên nhân thường gặp:
# 1. MinIO chưa ready → restart job
docker compose restart flink-login-job

# 2. JobManager chưa accept connections
curl -s http://localhost:8085/overview
# Nếu error → chờ thêm hoặc restart jobmanager
docker compose restart flink-jobmanager
```

**Flink job RUNNING nhưng Iceberg không có data sau 5 phút:**
```bash
# Xem logs TaskManager (nơi thực sự execute)
docker logs cdp-flink-taskmanager | tail -50

# Kiểm tra checkpoint có thành công không (Flink UI)
open http://localhost:8085
# Job → Checkpoints tab → Latest Completed checkpoint
```

**StarRocks không thấy Iceberg catalog:**
```bash
mysql -h 127.0.0.1 -P 9031 -u root -e "SHOW CATALOGS;"
# Nếu chỉ thấy default_catalog → init SQL chưa chạy

# Xem StarRocks logs
docker logs cdp-starrocks | grep -E "(ERROR|iceberg_catalog)" | tail -20

# Chạy lại init SQL thủ công
mysql -h 127.0.0.1 -P 9031 -u root < ./starrocks/init/00_cdp_schema.sql
```

**dbt Silver/Gold thất bại:**
```bash
# Xem log trong Airflow UI → Task log

# Chạy tay
docker exec cdp-airflow-webserver \
  bash -c "cd /opt/analytics/cdp/dbt_silver && dbt run --profiles-dir . --select silver_login --target dev"

docker exec cdp-airflow-webserver \
  bash -c "cd /opt/analytics/cdp/dbt_gold && dbt run --profiles-dir . --target dev"
```

**StarRocks query Iceberg chậm (>30s lần đầu):**

Bình thường — lần đầu StarRocks cần load Iceberg metadata từ MinIO.
Các lần sau được cache. Nếu vẫn timeout → tăng `--connect-timeout` trong DAG.
