# Payment Revenue Analytics Platform

Hệ thống tracking thanh toán theo **Dual-Pipeline Medallion Architecture**.

```
                   event-producer (~500 events/s)
                           │ Kafka: payment-events
                           ▼
                  tracking-ingestor (validate)       ← single validation gate
                           │ Kafka: payment-events-ingest
              ┌────────────┴──────────────┐
              │                           │
              ▼                           ▼
   LUỒNG 1: REALTIME          LUỒNG 2: WAREHOUSE (DW)
     (ClickHouse)                (Iceberg + StarRocks)
              │                           │
      CH Kafka Engine            Flink (checkpoint 60s)
              │                           │
    CH: payments_bronze         Iceberg: bronze/payments
              │ dbt (1min)             (MinIO, Parquet)
    CH: silver_events                    │
              │                   SR Iceberg ext catalog
         Superset                   = "SR Bronze"
       (realtime)                        │ [future] dbt
                                  SR: silver_payments
                                         │ [future] dbt
                                  SR: gold_revenue
```

| | Luồng 1 (ClickHouse) | Luồng 2 (StarRocks) |
|-|---------------------|---------------------|
| Latency | ~1 phút | ~1 phút (Flink checkpoint) |
| Dùng cho | Realtime monitoring | Historical analysis, BI, segmentation |
| Bronze storage | ClickHouse MergeTree | Apache Iceberg / Parquet (MinIO) |
| Silver transform | dbt-clickhouse (Airflow) | dbt-starrocks (future) |

---

## Tài liệu

### Luồng 1: Realtime (ClickHouse)

| Doc | Nội dung |
|-----|---------|
| [realtime/architecture.md](realtime/architecture.md) | Flow, tech stack, CH Kafka Engine, dbt, Airflow |
| [realtime/data.md](realtime/data.md) | Schemas (Bronze + Silver), analytics queries, data quality |
| [realtime/setup.md](realtime/setup.md) | Manual setup, kiểm tra, troubleshooting |

### Luồng 2: Warehouse DW (Iceberg + StarRocks)

| Doc | Nội dung |
|-----|---------|
| [dw/architecture.md](dw/architecture.md) | Flow, tech stack, Flink, Iceberg, StarRocks ext catalog |
| [dw/data.md](dw/data.md) | Schemas (Iceberg Bronze + SR Silver/Gold), analytics queries |
| [dw/setup.md](dw/setup.md) | Manual setup, kiểm tra, troubleshooting |


---

## Quick start

```bash
# Luồng 1: Realtime
cd com/tm/docker/realtime && docker compose up -d

# Luồng 2: Warehouse DW
cd com/tm/docker/dw && docker compose up -d
```

Xem [realtime/setup.md](realtime/setup.md) hoặc [dw/setup.md](dw/setup.md) để biết các bước chi tiết.
