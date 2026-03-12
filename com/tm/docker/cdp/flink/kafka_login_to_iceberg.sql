-- =============================================================================
-- FLINK SQL JOB: Kafka → Apache Iceberg (CDP Bronze Lake — Login Events)
-- =============================================================================
-- Đọc validated login events từ "login-events-ingest"
-- (cdp-ingestor validate JSON và forward từ "login-events"),
-- ghi vào Apache Iceberg table trên MinIO (S3-compatible).
--
-- Flow:
--   login-event-producer → "login-events"
--        → [cdp-ingestor] validate + forward
--        → "login-events-ingest"
--        → [Flink] batch buffer (checkpoint interval = 60s)
--        → Iceberg: s3a://lakehouse/warehouse/bronze/login_events/
--        → StarRocks Iceberg external catalog → "SR Bronze"
--
-- Chạy: sql-client.sh -f /opt/flink/jobs/kafka_login_to_iceberg.sql
-- =============================================================================

SET 'execution.runtime-mode' = 'STREAMING';
SET 'parallelism.default' = '2';

-- Checkpoint mỗi 60s → Iceberg commit mỗi 60s
SET 'execution.checkpointing.interval' = '60s';
SET 'execution.checkpointing.mode' = 'EXACTLY_ONCE';

-- =============================================================================
-- ICEBERG CATALOG (HadoopCatalog on MinIO)
-- =============================================================================
CREATE CATALOG iceberg_catalog WITH (
    'type'                                      = 'iceberg',
    'catalog-type'                              = 'hadoop',
    'warehouse'                                 = 's3a://lakehouse/warehouse',
    'hadoop.fs.s3a.access.key'                  = 'minioadmin',
    'hadoop.fs.s3a.secret.key'                  = 'minioadmin',
    'hadoop.fs.s3a.endpoint'                    = 'http://minio:9000',
    'hadoop.fs.s3a.path.style.access'           = 'true',
    'hadoop.fs.s3a.impl'                        = 'org.apache.hadoop.fs.s3a.S3AFileSystem',
    'hadoop.fs.s3a.aws.credentials.provider'    = 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider'
);

-- =============================================================================
-- SOURCE: Kafka "login-events-ingest" (validated by cdp-ingestor)
-- =============================================================================
CREATE TABLE IF NOT EXISTS default_catalog.default_database.kafka_login_validated
(
    event_id   STRING,
    user_id    BIGINT,
    session_id STRING,
    device_id  STRING,
    platform   STRING,
    country    STRING,
    event_date STRING,   -- "2025-01-15" (YYYY-MM-DD)
    login_at   STRING    -- "2025-01-15T10:30:00.000Z" (ISO8601)
) WITH (
    'connector'                    = 'kafka',
    'topic'                        = 'login-events-ingest',
    'properties.bootstrap.servers' = 'kafka:9092',
    'properties.group.id'          = 'flink-iceberg-cdp-login',
    'scan.startup.mode'            = 'latest-offset',
    'format'                       = 'json',
    'json.ignore-parse-errors'     = 'true'
);

-- =============================================================================
-- SINK: Apache Iceberg bronze table
-- =============================================================================
CREATE DATABASE IF NOT EXISTS iceberg_catalog.bronze;

CREATE TABLE IF NOT EXISTS iceberg_catalog.bronze.login_events
(
    event_id   STRING       COMMENT 'UUID từ login-event-producer',
    user_id    BIGINT       COMMENT 'User ID (1-100,000)',
    session_id STRING       COMMENT 'Session UUID (mới mỗi lần app mở)',
    device_id  STRING       COMMENT 'Device UUID (stable per device)',
    platform   STRING       COMMENT 'Raw platform: ios/android/web/unknown/empty',
    country    STRING       COMMENT 'Country code: VN/US/SG/... hoặc empty',
    event_date DATE         COMMENT 'Ngày login (partition key)',
    login_at   TIMESTAMP(3) COMMENT 'Timestamp login gốc (RFC3339)',
    ingested_at TIMESTAMP(3) COMMENT 'Thời điểm Flink write vào Iceberg'
) PARTITIONED BY (event_date)
WITH (
    'format-version'                = '2',
    'write.target-file-size-bytes'  = '67108864',
    'write.distribution-mode'       = 'hash'
);

-- =============================================================================
-- STREAMING JOB: Kafka → Iceberg (continuous, checkpoint-driven commits)
-- =============================================================================
INSERT INTO iceberg_catalog.bronze.login_events
SELECT
    event_id,
    user_id,
    session_id,
    device_id,
    platform,
    country,
    CAST(event_date AS DATE),
    -- Parse login_at: "2025-01-15T10:30:00.000Z" → TIMESTAMP(3)
    TO_TIMESTAMP(LEFT(REPLACE(login_at, 'Z', ''), 23), 'yyyy-MM-dd''T''HH:mm:ss.SSS'),
    CURRENT_TIMESTAMP   -- ingested_at
FROM default_catalog.default_database.kafka_login_validated
WHERE event_id   IS NOT NULL AND event_id   <> ''
  AND user_id    IS NOT NULL
  AND event_date IS NOT NULL;
