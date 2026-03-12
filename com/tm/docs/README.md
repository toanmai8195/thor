# CDP DW — Customer Data Platform

Hệ thống **Customer Data Platform** theo Medallion Architecture.

```
     login-event-producer (~500 events/s)
                 │ Kafka: login-events
                 ▼
        cdp-ingestor (validate JSON)
                 │ Kafka: login-events-ingest
                 ▼
    Apache Flink (checkpoint 60s, exactly-once)
                 │ Parquet files
                 ▼
  Iceberg: bronze/login_events (MinIO)
                 │ StarRocks external catalog
                 ▼
iceberg_catalog.bronze.login_events   ← Bronze
                 │ dbt (3min)
           cdp.silver_login           ← Silver
                 │ dbt (3min)
        cdp.gold_user_daily           ← Gold
                 │
             Superset
```

| Layer | Table | Mô tả |
|-------|-------|-------|
| Bronze | `iceberg_catalog.bronze.login_events` | Raw events, Flink ghi mỗi 60s (Parquet, MinIO) |
| Silver | `cdp.silver_login` | Normalized platform/country, UPSERT by (event_id, event_date) |
| Gold | `cdp.gold_user_daily` | DAU, sessions, platform breakdown theo ngày |

---

## Tài liệu

| Doc | Nội dung |
|-----|---------|
| [cdp/roadmap.md](cdp/roadmap.md) | Roadmap 7 bước: login → metadata layer → multi-source → identity resolution → user profile → segmentation → activation |

---

## Quick start

```bash
# Build Go images
bazel run //com/tm/src/services/login-event-producer:login-event-producer_docker
bazel run //com/tm/src/services/cdp-ingestor:cdp-ingestor_docker

# Start CDP stack
cd com/tm/docker/cdp && docker compose up -d
```

Xem [cdp/roadmap.md](cdp/roadmap.md) để biết chi tiết từng bước.

---

## Service URLs

| Service | URL | Credentials |
|---------|-----|-------------|
| Kafka UI | http://localhost:9082 | — |
| MinIO Console | http://localhost:9011 | `minioadmin` / `minioadmin` |
| MinIO S3 API | http://localhost:9010 | — |
| Flink Web UI | http://localhost:8085 | — |
| StarRocks MySQL | `mysql -h 127.0.0.1 -P 9031 -u root` | `root` / (trống) |
| StarRocks FE HTTP | http://localhost:8031 | — |
| Airflow | http://localhost:8086 | `admin` / `admin123` |
| Superset | http://localhost:8089 | `admin` / `admin` |
