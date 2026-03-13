-- =============================================================================
-- STARROCKS CDP: ICEBERG EXTERNAL CATALOG + CDP SCHEMA
-- =============================================================================
-- Setup cho CDP pipeline (Step 1: Login Events).
-- Mirrors cấu trúc của DW init nhưng cho CDP database/tables.
--
-- "SR Bronze" = iceberg_catalog.bronze.login_events (external, Flink ghi)
-- Silver      = cdp.silver_login      (StarRocks native PRIMARY KEY)
-- Gold        = cdp.gold_user_daily   (dbt tạo/replace, full rebuild)
-- =============================================================================

-- =============================================================================
-- ICEBERG EXTERNAL CATALOG (cùng MinIO instance với DW nếu chạy song song)
-- =============================================================================
CREATE EXTERNAL CATALOG iceberg_catalog
COMMENT 'CDP Bronze layer — Apache Iceberg on MinIO (written by Flink)'
PROPERTIES (
    "type"                           = "iceberg",
    "iceberg.catalog.type"           = "hadoop",
    "iceberg.catalog.warehouse"      = "s3a://lakehouse/warehouse",

    "aws.s3.use_instance_profile"    = "false",
    "aws.s3.access_key"              = "minioadmin",
    "aws.s3.secret_key"              = "minioadmin",
    "aws.s3.endpoint"                = "http://minio:9000",
    "aws.s3.enable_path_style_access"= "true",

    "hadoop.fs.s3a.access.key"                  = "minioadmin",
    "hadoop.fs.s3a.secret.key"                  = "minioadmin",
    "hadoop.fs.s3a.endpoint"                    = "http://minio:9000",
    "hadoop.fs.s3a.path.style.access"           = "true",
    "hadoop.fs.s3a.impl"                        = "org.apache.hadoop.fs.s3a.S3AFileSystem",
    "hadoop.fs.s3a.aws.credentials.provider"    = "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
);

-- =============================================================================
-- CDP DATABASE (Silver + Gold layer)
-- =============================================================================
CREATE DATABASE IF NOT EXISTS cdp
COMMENT 'CDP pipeline: Silver + Gold layers (login events, Step 1)';

USE cdp;

-- =============================================================================
-- SILVER: cdp.silver_login
-- dbt transform từ iceberg_catalog.bronze.login_events
-- PRIMARY KEY (event_id, event_date) → UPSERT (idempotent)
-- =============================================================================
CREATE TABLE IF NOT EXISTS silver_login
(
    event_id   VARCHAR(64) NOT NULL  COMMENT 'UUID',
    event_date DATE        NOT NULL  COMMENT 'Ngày login (partition key)',
    user_id    BIGINT      NOT NULL  COMMENT 'User ID',
    session_id VARCHAR(64) NOT NULL  COMMENT 'Session UUID',
    device_id  VARCHAR(64) NOT NULL  COMMENT 'Device UUID',
    platform   VARCHAR(20) NOT NULL  COMMENT 'Normalized: ios/android/web/unknown',
    country    VARCHAR(10) NOT NULL  COMMENT 'Country code hoặc unknown',
    login_at   DATETIME    NOT NULL  COMMENT 'Timestamp login gốc'
)
PRIMARY KEY (event_id, event_date)
PARTITION BY RANGE(event_date) (
    START ("2025-01-01") END ("2027-01-01") EVERY (INTERVAL 1 MONTH)
)
DISTRIBUTED BY HASH(event_id) BUCKETS 8
PROPERTIES (
    "replication_num"             = "1",
    "dynamic_partition.enable"    = "true",
    "dynamic_partition.time_unit" = "MONTH",
    "dynamic_partition.start"     = "-6",
    "dynamic_partition.end"       = "3",
    "dynamic_partition.prefix"    = "p"
);

-- =============================================================================
-- METADATA LAYER: cdp.event_registry (Step 2)
-- Single source of truth cho tất cả CDP pipeline sources.
-- Sync từ docker/cdp/registry/event_registry.yml khi deploy source mới.
-- =============================================================================
CREATE TABLE IF NOT EXISTS event_registry
(
    source_id            VARCHAR(50)  NOT NULL COMMENT 'login | view | click | payment | search',
    description          VARCHAR(200)          COMMENT 'Mo ta event source',
    kafka_topic          VARCHAR(100) NOT NULL COMMENT 'Kafka ingest topic (sau cdp-ingestor)',
    iceberg_database     VARCHAR(50)  NOT NULL COMMENT 'Iceberg DB: bronze',
    iceberg_table        VARCHAR(100) NOT NULL COMMENT 'Iceberg table: login_events',
    sr_silver_model      VARCHAR(100) NOT NULL COMMENT 'dbt silver model name: silver_login',
    sr_gold_model        VARCHAR(100)          COMMENT 'dbt gold model name (nullable)',
    flink_consumer_group VARCHAR(100)          COMMENT 'Flink Kafka consumer group',
    enabled              BOOLEAN DEFAULT TRUE  COMMENT 'FALSE = skip trong pipeline',
    registered_at        DATETIME              COMMENT 'Khi nao dang ky source nay'
)
PRIMARY KEY (source_id)
DISTRIBUTED BY HASH(source_id) BUCKETS 1
PROPERTIES ("replication_num" = "1");

-- Seed: login source (Step 1)
INSERT INTO event_registry VALUES (
    'login',
    'User login events — first CDP data source',
    'login-events-ingest',
    'bronze', 'login_events',
    'silver_login', 'gold_user_daily',
    'flink-iceberg-cdp-login',
    TRUE, NOW()
);

-- =============================================================================
-- Gold table (cdp.gold_user_daily) do dbt tao/replace — khong can CREATE o day
-- =============================================================================
