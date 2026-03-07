-- =============================================================================
-- FLINK SQL JOB: Kafka → Apache Iceberg (DW Bronze Lake)
-- =============================================================================
-- Đọc validated payment events từ "payment-events-ingest"
-- (tracking-ingestor validate JSON và forward từ "payment-events"),
-- ghi vào Apache Iceberg table trên MinIO (S3-compatible).
--
-- Flow:
--   event-producer → "payment-events"
--        → [tracking-ingestor] validate + forward
--        → "payment-events-ingest"
--        → [Flink] batch buffer (checkpoint interval = 60s)
--        → Iceberg: s3a://lakehouse/warehouse/bronze/payments/
--        → StarRocks Iceberg external catalog → "SR Bronze"
--
-- Iceberg format-version=2: hỗ trợ row-level delete, upsert (MERGE-on-READ)
-- Partition: DAY(payment_date) → file per day, partition pruning efficient
--
-- Chạy: sql-client.sh -f /opt/flink/jobs/kafka_to_iceberg.sql
-- =============================================================================

SET 'execution.runtime-mode' = 'STREAMING';
SET 'parallelism.default' = '2';

-- Checkpoint mỗi 60s → Iceberg commit mỗi 60s (= batch interval)
SET 'execution.checkpointing.interval' = '60s';
SET 'execution.checkpointing.mode' = 'EXACTLY_ONCE';

-- =============================================================================
-- ICEBERG CATALOG (HadoopCatalog on MinIO)
-- Metadata + data files: s3a://lakehouse/warehouse/
-- S3 config được inject qua FLINK_PROPERTIES env (s3.endpoint, s3.access-key, ...)
-- =============================================================================
CREATE CATALOG iceberg_catalog WITH (
    'type'                                      = 'iceberg',
    'catalog-type'                              = 'hadoop',
    'warehouse'                                 = 's3a://lakehouse/warehouse',
    -- S3A credentials + MinIO endpoint (hadoop.* prefix → Hadoop Configuration)
    'hadoop.fs.s3a.access.key'                  = 'minioadmin',
    'hadoop.fs.s3a.secret.key'                  = 'minioadmin',
    'hadoop.fs.s3a.endpoint'                    = 'http://minio:9000',
    'hadoop.fs.s3a.path.style.access'           = 'true',
    'hadoop.fs.s3a.impl'                        = 'org.apache.hadoop.fs.s3a.S3AFileSystem',
    'hadoop.fs.s3a.aws.credentials.provider'    = 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider'
);

-- =============================================================================
-- SOURCE: Kafka "payment-events-ingest" (validated by tracking-ingestor)
-- Tạo trong default_catalog để dùng Kafka connector
-- =============================================================================
CREATE TABLE IF NOT EXISTS default_catalog.default_database.kafka_payments_validated
(
    event_id     STRING,
    user_id      BIGINT,
    amount       DOUBLE,
    status       STRING,
    payment_date STRING,    -- "2026-01-15" (YYYY-MM-DD)
    updated_at   STRING     -- "2026-01-15T10:30:00.123Z" (RFC3339)
) WITH (
    'connector'                    = 'kafka',
    'topic'                        = 'payment-events-ingest',
    'properties.bootstrap.servers' = 'kafka:9092',
    'properties.group.id'          = 'flink-iceberg-bronze',
    'scan.startup.mode'            = 'latest-offset',
    'format'                       = 'json',
    'json.ignore-parse-errors'     = 'true'
);

-- =============================================================================
-- SINK: Apache Iceberg bronze table
-- Tạo database và table trong iceberg_catalog
-- =============================================================================
CREATE DATABASE IF NOT EXISTS iceberg_catalog.bronze;

CREATE TABLE IF NOT EXISTS iceberg_catalog.bronze.payments
(
    event_id     STRING      COMMENT 'UUID từ event-producer',
    user_id      BIGINT      COMMENT 'User ID (1-100,000)',
    amount       DECIMAL(12, 2) COMMENT 'Số tiền raw (có thể âm/0 — chưa filter ở Bronze)',
    status       STRING      COMMENT 'Raw status: paid/PAID/invalid_status/...',
    payment_date DATE        COMMENT 'Ngày thanh toán',
    updated_at   TIMESTAMP(3) COMMENT 'Timestamp gốc từ producer (RFC3339)',
    ingested_at  TIMESTAMP(3) COMMENT 'Thời điểm Flink write vào Iceberg'
) PARTITIONED BY (payment_date)
WITH (
    'format-version'                = '2',
    'write.target-file-size-bytes'  = '67108864',   -- 64MB per file
    'write.distribution-mode'       = 'hash'         -- distribute by partition key
);

-- =============================================================================
-- STREAMING JOB: Kafka → Iceberg (continuous, checkpoint-driven commits)
-- =============================================================================
INSERT INTO iceberg_catalog.bronze.payments
SELECT
    event_id,
    user_id,
    CAST(amount       AS DECIMAL(12, 2)),
    status,
    CAST(payment_date AS DATE),
    TO_TIMESTAMP(REPLACE(updated_at, 'Z', ''), 'yyyy-MM-dd''T''HH:mm:ss.SSS'),
    CURRENT_TIMESTAMP   -- ingested_at
FROM default_catalog.default_database.kafka_payments_validated
WHERE event_id     IS NOT NULL
  AND user_id      IS NOT NULL
  AND payment_date IS NOT NULL;
