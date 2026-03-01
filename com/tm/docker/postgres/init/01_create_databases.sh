#!/bin/bash
# =============================================================================
# POSTGRESQL INITIALIZATION SCRIPT
# =============================================================================
# Script này được chạy tự động khi PostgreSQL container khởi động lần đầu.
#
# Mục đích:
# 1. Tạo database cho Airflow (workflow orchestration metadata)
# 2. Tạo database cho Superset (BI tool metadata)
#
# PostgreSQL trong hệ thống này đóng vai trò:
# - Lưu trữ metadata (không phải analytics data)
# - Airflow dùng để track DAG runs, task instances, connections
# - Superset dùng để lưu dashboards, charts, users, permissions
#
# Lưu ý: Analytics data được lưu ở ClickHouse và StarRocks!
# =============================================================================

set -e  # Exit ngay nếu có lỗi

# Biến môi trường từ docker-compose
POSTGRES_USER=${POSTGRES_USER:-admin}
POSTGRES_DB=${POSTGRES_DB:-postgres}

echo "=== Creating additional databases ==="

# =============================================================================
# TẠO DATABASE CHO AIRFLOW
# =============================================================================
# Airflow cần database để lưu:
# - DAGs metadata (tên, schedule, owner)
# - DAG runs (start time, end time, state)
# - Task instances (task_id, execution_date, state)
# - Connections (database connections, API keys)
# - Variables (config values)
# - XComs (task communication)
# =============================================================================
echo "Creating database: airflow"
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    -- Tạo database nếu chưa tồn tại
    SELECT 'CREATE DATABASE airflow'
    WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'airflow')\gexec

    -- Grant full permissions cho admin user
    GRANT ALL PRIVILEGES ON DATABASE airflow TO $POSTGRES_USER;
EOSQL

# =============================================================================
# TẠO DATABASE CHO SUPERSET
# =============================================================================
# Superset cần database để lưu:
# - Users và roles (authentication & authorization)
# - Dashboards (layout, filters, refresh settings)
# - Charts (chart type, queries, visualizations)
# - Datasets (tables/views references)
# - Database connections (connection strings to analytics DBs)
# - Saved queries
# =============================================================================
echo "Creating database: superset"
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    SELECT 'CREATE DATABASE superset'
    WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'superset')\gexec

    GRANT ALL PRIVILEGES ON DATABASE superset TO $POSTGRES_USER;
EOSQL

echo "=== Database initialization completed ==="

# =============================================================================
# THÔNG TIN KẾT NỐI
# =============================================================================
# Sau khi khởi tạo, bạn có thể kết nối:
#
# Airflow DB:
#   Host: postgres (trong docker network) hoặc localhost:5432 (từ host)
#   Database: airflow
#   User: admin
#   Password: admin123
#
# Superset DB:
#   Host: postgres (trong docker network) hoặc localhost:5432 (từ host)
#   Database: superset
#   User: admin
#   Password: admin123
#
# Command line test:
#   psql -h localhost -p 5432 -U admin -d airflow
#   psql -h localhost -p 5432 -U admin -d superset
# =============================================================================
