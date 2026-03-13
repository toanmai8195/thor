# CDP Stack — Setup Overview

CDP stack gồm **2 luồng độc lập** chạy song song từ cùng Kafka topic.
Có thể khởi động riêng từng luồng hoặc cả 2 cùng lúc.

---

## Shared Infrastructure (cần cho cả 2 luồng)

| Service | Vai trò |
|---------|---------|
| Kafka | Message broker — topic `login-events` + `login-events-ingest` |
| login-event-producer | Sinh ~500 login events/s |
| cdp-ingestor | Validate JSON + forward sang `login-events-ingest` |
| Airflow | Orchestrate dbt jobs |

### Build Go Images

```bash
# Từ repo root — cần thiết trước khi docker compose up
bazel run //com/tm/src/services/login-event-producer:login-event-producer_docker
bazel run //com/tm/src/services/cdp-ingestor:cdp-ingestor_docker
```

### Start toàn bộ stack

```bash
cd com/tm/docker/cdp
docker compose up -d --build
```

---

## Setup theo từng luồng

| Luồng | Setup doc | Components |
|-------|-----------|------------|
| Luồng 1: Realtime | [setup-realtime.md](setup-realtime.md) | ClickHouse + dbt-clickhouse + `cdp_realtime_pipeline` DAG |
| Luồng 2: DW | [setup-dw.md](setup-dw.md) | MinIO + Flink + StarRocks + dbt-starrocks + `cdp_medallion_pipeline` DAG |

## Metadata Layer & Tích hợp source mới

| Doc | Dành cho | Nội dung |
|-----|---------|---------|
| [setup-metadata.md](setup-metadata.md) | DE team | Kiến trúc Metadata Layer, macros reference |
| [onboarding-new-source.md](onboarding-new-source.md) | Team tích hợp | Step-by-step thêm event source mới vào CDP DW |

---

## Stop / Reset

```bash
cd com/tm/docker/cdp

# Dừng, giữ data
docker compose down

# Dừng + xóa toàn bộ data (reset)
docker compose down -v
```
