# CDP DW — Customer Data Platform

Hệ thống **Customer Data Platform** theo **Dual-Pipeline Medallion Architecture**:
hai luồng xử lý song song từ cùng Kafka topics, phục vụ realtime monitoring và batch analytics.

```
login-event-producer (~500 events/s)     event-producer (10 events/s × 4 sources)
        │ Kafka: login-events                   │ Kafka: view/click/payment/search-events
        ▼                                       ▼
cdp-ingestor-login                  cdp-ingestor-{view,click,payment,search}
        │ Kafka: login-events-ingest            │ Kafka: *-events-ingest
  ┌─────┴──────────────────────────────────────┤
  │                                            │
  ▼                                            ▼
LUỒNG 1: REALTIME                         LUỒNG 2: DW
(ClickHouse — login only)           (Iceberg + StarRocks — 5 sources)
  │                                            │
CH Kafka Engine                      Flink (checkpoint 60s)
  │                                            │ Parquet
cdp.login_bronze                     Iceberg: bronze/{login,view,click,payment,search}_events
  │ dbt (1min)                               (MinIO)
cdp.silver_login                              │ SR external catalog
  │                               cdp.silver_{login,view,click,payment,search} (UPSERT, 3min)
Superset                                       │ dbt (3min)
(realtime)                         cdp.gold_{user_daily,page_daily,search_daily}
                                               │
                                           Superset (batch)
```

---

## Event Sources

| Source | Producer | Topic | RPS | Ingestor topic |
|--------|----------|-------|-----|----------------|
| login | `login-event-producer` | `login-events` | 500 | `login-events-ingest` |
| view | `event-producer` | `view-events` | 10 | `view-events-ingest` |
| click | `event-producer` | `click-events` | 10 | `click-events-ingest` |
| payment | `event-producer` | `payment-events` | 10 | `payment-events-ingest` |
| search | `event-producer` | `search-events` | 10 | `search-events-ingest` |

---

## Layers

| Layer | Luồng 1 (Realtime) | Luồng 2 (DW) |
|-------|-------------------|--------------|
| Bronze | `cdp.login_bronze` (ClickHouse MergeTree, TTL 90d) | `iceberg_catalog.bronze.*_events` (Parquet, MinIO) |
| Silver | `cdp.silver_login` (ClickHouse ReplacingMergeTree) | `cdp.silver_{login,view,click,payment,search}` (StarRocks PRIMARY KEY) |
| Gold | — | `cdp.gold_{user_daily,page_daily,search_daily}` (StarRocks aggregate) |

---

## Tài liệu

| Doc | Nội dung |
|-----|---------|
| [cdp/setup.md](cdp/setup.md) | Overview khởi động stack + shared infra |
| [cdp/setup-realtime.md](cdp/setup-realtime.md) | Setup Luồng 1: ClickHouse realtime — step-by-step + troubleshooting |
| [cdp/setup-dw.md](cdp/setup-dw.md) | Setup Luồng 2: Iceberg + StarRocks DW — step-by-step + troubleshooting |
| [cdp/setup-metadata.md](cdp/setup-metadata.md) | Metadata layer, event registry, onboarding new source |
| [cdp/pipeline-realtime.md](cdp/pipeline-realtime.md) | Luồng 1: kiến trúc, schema, dbt config, DAG (login only) |
| [cdp/pipeline-dw.md](cdp/pipeline-dw.md) | Luồng 2: kiến trúc, schema tất cả 5 sources, Flink, dbt, DAG |
| [cdp/onboarding-new-source.md](cdp/onboarding-new-source.md) | Step-by-step guide thêm event source mới |
| [cdp/roadmap.md](cdp/roadmap.md) | Roadmap 7 bước: login → multi-source → identity → profile → segmentation → activation |

---

## Service URLs

| Service | URL | Credentials |
|---------|-----|-------------|
| Kafka UI | http://localhost:9082 | — |
| ClickHouse HTTP | http://localhost:8124 | `default` / (trống) |
| MinIO Console | http://localhost:9011 | `minioadmin` / `minioadmin` |
| MinIO S3 API | http://localhost:9010 | — |
| StarRocks MySQL | `docker exec -it cdp-starrocks mysql -h 127.0.0.1 -P 9030 -u root` | `root` / (trống) — MySQL 9.x không tương thích |
| StarRocks FE HTTP | http://localhost:8031 | — |
| Flink Web UI | http://localhost:8085 | — |
| Airflow | http://localhost:8086 | `admin` / `admin` |
| Superset | http://localhost:8089 | `admin` / `admin` |
