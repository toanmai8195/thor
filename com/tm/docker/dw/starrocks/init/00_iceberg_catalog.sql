-- =============================================================================
-- STARROCKS DW: ICEBERG EXTERNAL CATALOG (Bronze Layer)
-- =============================================================================
-- StarRocks đọc trực tiếp từ Apache Iceberg trên MinIO.
-- Iceberg table DO FLINK GHI vào (kafka_to_iceberg.sql).
--
-- "SR Bronze" = iceberg_catalog.bronze.payments (external, không copy data)
--
-- Sau khi Flink write xong, query bằng:
--   SELECT * FROM iceberg_catalog.bronze.payments;
--   SELECT COUNT(*) FROM iceberg_catalog.bronze.payments WHERE payment_date = CURRENT_DATE();
--
-- dbt_starrocks sẽ transform từ iceberg_catalog.bronze → tracking.silver_payments
--
-- Lưu ý:
--   - MinIO phải healthy trước khi tạo catalog (catalog chỉ validate khi query)
--   - Flink phải write ít nhất 1 checkpoint trước khi Iceberg table xuất hiện
-- =============================================================================

CREATE EXTERNAL CATALOG iceberg_catalog
COMMENT 'DW Bronze layer — Apache Iceberg on MinIO (written by Flink)'
PROPERTIES (
    -- Catalog type
    "type"                           = "iceberg",
    "iceberg.catalog.type"           = "hadoop",

    -- Warehouse path trên MinIO (phải khớp với warehouse trong Flink catalog)
    "iceberg.catalog.warehouse"      = "s3a://lakehouse/warehouse",

    -- StarRocks native S3 client (AWS SDK v2) — dùng underscore, không phải dash
    -- BE dùng client này để scan Parquet data files từ MinIO
    "aws.s3.use_instance_profile"    = "false",
    "aws.s3.access_key"              = "minioadmin",
    "aws.s3.secret_key"              = "minioadmin",
    "aws.s3.endpoint"                = "http://minio:9000",
    "aws.s3.enable_path_style_access"= "true",

    -- Hadoop S3A properties (HadoopCatalog dùng Hadoop S3AFileSystem để đọc Iceberg metadata)
    -- Cần thêm vì aws.s3.* không tự map sang fs.s3a.* cho HadoopCatalog
    -- core-site.xml trong BE conf dir cũng cần config này (docker-compose volume mount)
    "hadoop.fs.s3a.access.key"                  = "minioadmin",
    "hadoop.fs.s3a.secret.key"                  = "minioadmin",
    "hadoop.fs.s3a.endpoint"                    = "http://minio:9000",
    "hadoop.fs.s3a.path.style.access"           = "true",
    "hadoop.fs.s3a.impl"                        = "org.apache.hadoop.fs.s3a.S3AFileSystem",
    "hadoop.fs.s3a.aws.credentials.provider"    = "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
);

-- =============================================================================
-- TRACKING DATABASE (Silver + Gold native SR tables — dbt target)
-- =============================================================================
CREATE DATABASE IF NOT EXISTS tracking
COMMENT 'DW pipeline: Silver + Gold layers (populated by dbt from Iceberg Bronze)';

USE tracking;

-- Silver: dbt_starrocks transform từ iceberg_catalog.bronze.payments
-- Tạo sẵn để dbt có thể INSERT mà không cần CREATE
CREATE TABLE IF NOT EXISTS silver_payments
(
    event_id      VARCHAR(64)    NOT NULL COMMENT 'UUID',
    payment_date  DATE           NOT NULL COMMENT 'Ngày thanh toán',
    user_id       BIGINT         NOT NULL COMMENT 'User ID',
    amount        DECIMAL(12, 2) NOT NULL COMMENT 'Số tiền đã validate (> 0)',
    status        VARCHAR(20)    NOT NULL COMMENT 'Normalized: paid/pending/failed/refunded/cancelled/unknown',
    updated_at    DATETIME       NOT NULL COMMENT 'Timestamp gốc'
)
PRIMARY KEY (event_id, payment_date)
PARTITION BY RANGE(payment_date) (
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
