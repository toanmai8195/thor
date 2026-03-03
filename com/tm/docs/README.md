# Payment Revenue Analytics Platform

Hệ thống tracking thanh toán theo **Medallion Architecture**: Bronze → Silver → Gold.

```
Event Producer → Kafka → Tracking Ingestor → Kafka → CH Kafka Engine
                                                             ↓ MV
                                                     payments_bronze (Bronze)
                                                             ↓ dbt
                                                     silver_payments (Silver)
                                                             ↓ sync
                                                     StarRocks → gold_revenue (Gold)
                                                             ↓
                                                         Superset
```

## Tài liệu

| Doc | Nội dung |
|-----|---------|
| [architecture.md](architecture.md) | Flow dữ liệu, tech stack, chi tiết từng lớp, cấu trúc thư mục, glossary |
| [data.md](data.md) | Schemas (Bronze/Silver/Gold), analytics queries, data quality queries |
| [setup.md](setup.md) | Cài đặt, chạy services, service URLs, troubleshooting |

## Quick start

```bash
# 1. Khởi động infrastructure
cd com/tm/docker && docker compose up -d

# 2. Build
bazel build //com/tm/src/services/...

# 3. Chạy ingestor (validate Kafka events → forward cho CH)
bazel run //com/tm/src/services/tracking-ingestor:tracking-ingestor -- \
  --kafka=localhost:9092

# 4. Chạy producer (sinh events)
bazel run //com/tm/src/services/event-producer:event-producer -- \
  --kafka=localhost:9092 --rate=1000
```

Xem [setup.md](setup.md) để biết thêm chi tiết.
