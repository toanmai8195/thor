# CDP DW — Customer Data Platform

Hệ thống **Customer Data Platform** theo **Dual-Pipeline Medallion Architecture**:
hai luồng xử lý song song từ cùng Kafka topic, phục vụ realtime monitoring và batch analytics.

```
login-event-producer (~500 events/s)
        │ Kafka: login-events
        ▼
cdp-ingestor (validate JSON + event_id)
        │ Kafka: login-events-ingest
  ┌─────┴────────────────────────────┐
  │                                  │
  ▼                                  ▼
LUỒNG 1: REALTIME               LUỒNG 2: DW
(ClickHouse)                (Iceberg + StarRocks)
  │                                  │
CH Kafka Engine                Flink (checkpoint 60s)
  │                                  │ Parquet
cdp.login_bronze            Iceberg: bronze/login_events
  │ dbt (1min)               (MinIO)
cdp.silver_login                     │ SR external catalog
  │                          cdp.silver_login (UPSERT, 3min)
Superset                             │ dbt (3min)
(realtime)                  cdp.gold_user_daily
                                     │
                                 Superset (daily)
```

---

## Layers

| Layer | Luồng 1 (Realtime) | Luồng 2 (DW) |
|-------|-------------------|--------------|
| Bronze | `cdp.login_bronze` (ClickHouse MergeTree, TTL 90d) | `iceberg_catalog.bronze.login_events` (Parquet, MinIO) |
| Silver | `cdp.silver_login` (ClickHouse ReplacingMergeTree) | `cdp.silver_login` (StarRocks PRIMARY KEY) |
| Gold | — | `cdp.gold_user_daily` (StarRocks aggregate) |

---

## Tài liệu

| Doc | Nội dung |
|-----|---------|
| [cdp/setup.md](cdp/setup.md) | Overview khởi động stack + shared infra |
| [cdp/setup-realtime.md](cdp/setup-realtime.md) | Setup Luồng 1: ClickHouse realtime — step-by-step + troubleshooting |
| [cdp/setup-dw.md](cdp/setup-dw.md) | Setup Luồng 2: Iceberg + StarRocks DW — step-by-step + troubleshooting |
| [cdp/pipeline-realtime.md](cdp/pipeline-realtime.md) | Luồng 1: kiến trúc, schema, dbt config, DAG |
| [cdp/pipeline-dw.md](cdp/pipeline-dw.md) | Luồng 2: kiến trúc, schema, Flink, dbt config, DAG |
| [cdp/roadmap.md](cdp/roadmap.md) | Roadmap 7 bước: login → multi-source → identity → profile → segmentation → activation |

---

## Service URLs

| Service | URL | Credentials |
|---------|-----|-------------|
| Kafka UI | http://localhost:9082 | — |
| ClickHouse HTTP | http://localhost:8124 | `default` / (trống) |
| MinIO Console | http://localhost:9011 | `minioadmin` / `minioadmin` |
| MinIO S3 API | http://localhost:9010 | — |
| StarRocks MySQL | `mysql -h 127.0.0.1 -P 9031 -u root` | `root` / (trống) |
| StarRocks FE HTTP | http://localhost:8031 | — |
| Flink Web UI | http://localhost:8085 | — |
| Airflow | http://localhost:8086 | `admin` / `admin123` |
| Superset | http://localhost:8089 | `admin` / `admin` |
