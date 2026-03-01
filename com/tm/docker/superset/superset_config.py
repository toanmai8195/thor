# =============================================================================
# SUPERSET CONFIGURATION
# =============================================================================
# File cấu hình cho Apache Superset - Data Visualization Platform
#
# Superset là gì?
# - Open-source BI (Business Intelligence) tool
# - Tạo dashboards và charts từ SQL queries
# - Connect đến nhiều loại databases (StarRocks, ClickHouse, PostgreSQL...)
# - Web-based, dễ sử dụng, không cần code
#
# File này định nghĩa:
# - Database connections
# - Security settings
# - Caching configuration
# - Feature flags
# =============================================================================

import os
from datetime import timedelta

# =============================================================================
# BASIC CONFIGURATION
# =============================================================================

# Secret key cho session encryption - PHẢI thay đổi trong production!
# Dùng để mã hóa cookies và sensitive data
# Generate: openssl rand -base64 42
SECRET_KEY = os.environ.get('SUPERSET_SECRET_KEY', 'your-secret-key-change-in-production')

# Row limit cho SQL queries (tránh query quá lớn làm crash)
ROW_LIMIT = 100000

# =============================================================================
# DATABASE CONFIGURATION
# =============================================================================
# Superset cần database để lưu metadata (dashboards, users, permissions)
# Đây KHÔNG phải database chứa analytics data!
# =============================================================================

# SQLAlchemy connection string
# Format: dialect+driver://username:password@host:port/database
SQLALCHEMY_DATABASE_URI = os.environ.get(
    'SQLALCHEMY_DATABASE_URI',
    'postgresql+psycopg2://admin:admin123@postgres:5432/superset'
)

# Connection pool settings
# Pool = tập hợp connections được reuse để tránh overhead
SQLALCHEMY_POOL_SIZE = 10  # Số connections thường trực
SQLALCHEMY_POOL_TIMEOUT = 60  # Timeout (seconds) khi wait cho connection
SQLALCHEMY_MAX_OVERFLOW = 20  # Số connections bổ sung khi pool đầy

# =============================================================================
# CACHING CONFIGURATION
# =============================================================================
# Cache giúp giảm load cho database và tăng response time
#
# Cache types:
# - Metadata cache: Schema, tables list, columns info
# - Data cache: Query results
# - Thumbnail cache: Dashboard thumbnails
#
# Redis được dùng làm cache backend vì:
# - Nhanh (in-memory)
# - Persistent (optional)
# - Distributed (có thể share giữa nhiều Superset instances)
# =============================================================================

# Redis connection settings
REDIS_HOST = os.environ.get('REDIS_HOST', 'redis')
REDIS_PORT = int(os.environ.get('REDIS_PORT', 6379))
REDIS_DB = 1  # Database number (Redis có 16 DBs: 0-15)

# Cache config với Redis
CACHE_CONFIG = {
    'CACHE_TYPE': 'RedisCache',
    'CACHE_DEFAULT_TIMEOUT': 60 * 60 * 24,  # 24 hours
    'CACHE_KEY_PREFIX': 'superset_',
    'CACHE_REDIS_HOST': REDIS_HOST,
    'CACHE_REDIS_PORT': REDIS_PORT,
    'CACHE_REDIS_DB': REDIS_DB,
}

# Data cache (cho query results)
DATA_CACHE_CONFIG = {
    'CACHE_TYPE': 'RedisCache',
    'CACHE_DEFAULT_TIMEOUT': 60 * 60,  # 1 hour
    'CACHE_KEY_PREFIX': 'superset_data_',
    'CACHE_REDIS_HOST': REDIS_HOST,
    'CACHE_REDIS_PORT': REDIS_PORT,
    'CACHE_REDIS_DB': REDIS_DB,
}

# Filter state cache
FILTER_STATE_CACHE_CONFIG = {
    'CACHE_TYPE': 'RedisCache',
    'CACHE_DEFAULT_TIMEOUT': 60 * 60 * 24 * 7,  # 7 days
    'CACHE_KEY_PREFIX': 'superset_filter_',
    'CACHE_REDIS_HOST': REDIS_HOST,
    'CACHE_REDIS_PORT': REDIS_PORT,
    'CACHE_REDIS_DB': REDIS_DB,
}

# Explore form data cache
EXPLORE_FORM_DATA_CACHE_CONFIG = {
    'CACHE_TYPE': 'RedisCache',
    'CACHE_DEFAULT_TIMEOUT': 60 * 60 * 24 * 7,  # 7 days
    'CACHE_KEY_PREFIX': 'superset_explore_',
    'CACHE_REDIS_HOST': REDIS_HOST,
    'CACHE_REDIS_PORT': REDIS_PORT,
    'CACHE_REDIS_DB': REDIS_DB,
}

# =============================================================================
# FEATURE FLAGS
# =============================================================================
# Bật/tắt các features của Superset
# Một số features đang ở trạng thái experimental
# =============================================================================

FEATURE_FLAGS = {
    # Dashboard features
    'DASHBOARD_NATIVE_FILTERS': True,  # Native filters (thay vì filter box)
    'DASHBOARD_CROSS_FILTERS': True,  # Click chart để filter charts khác
    'DASHBOARD_NATIVE_FILTERS_SET': True,  # Filter sets (save/load filters)

    # Chart features
    'ENABLE_EXPLORE_DRAG_AND_DROP': True,  # Drag-and-drop trong Explore
    'ENABLE_EXPLORE_JSON_CSRF_PROTECTION': True,  # CSRF protection

    # SQL Lab features
    'ENABLE_TEMPLATE_PROCESSING': True,  # Jinja templates trong SQL
    'ESTIMATE_QUERY_COST': True,  # Estimate query cost trước khi run
    'ENABLE_TEMPLATE_REMOVE_FILTERS': True,

    # Export features
    'VERSIONED_EXPORT': True,  # Export dashboards với versioning
    'EMBEDDED_SUPERSET': True,  # Embed dashboards trong apps khác

    # Alert & Report
    'ALERT_REPORTS': True,  # Scheduled reports và alerts

    # Performance
    'GLOBAL_ASYNC_QUERIES': True,  # Async query execution
}

# =============================================================================
# SQL LAB CONFIGURATION
# =============================================================================
# SQL Lab là feature cho phép users viết và chạy SQL queries trực tiếp
# Hữu ích cho data analysts và debugging
# =============================================================================

# Bật SQL Lab
ENABLE_SQLLAB = True

# Cho phép chạy async queries (queries chạy background)
SQLLAB_ASYNC_TIME_LIMIT_SEC = 60 * 60  # 1 hour timeout

# Số rows tối đa có thể download
SQLLAB_CTAS_NO_LIMIT = True  # Không limit cho CREATE TABLE AS SELECT

# Cho phép save queries
SQL_QUERY_MUTATOR = None

# Default format cho dates
DEFAULT_FEATURE_FLAGS = {
    'SQLLAB_BACKEND_PERSISTENCE': True,  # Persist queries ở backend
}

# =============================================================================
# SECURITY CONFIGURATION
# =============================================================================

# Cho phép CORS (Cross-Origin Resource Sharing) cho embedded dashboards
ENABLE_CORS = True

# CORS origins được phép
CORS_OPTIONS = {
    'supports_credentials': True,
    'allow_headers': ['*'],
    'resources': ['*'],
    'origins': ['*'],  # Trong production, specify domains cụ thể!
}

# Content Security Policy
TALISMAN_ENABLED = False  # Disable trong development

# =============================================================================
# WEBSERVER CONFIGURATION
# =============================================================================

# Webserver timeout (seconds)
SUPERSET_WEBSERVER_TIMEOUT = 120

# Enable proxy fix (nếu run sau reverse proxy)
ENABLE_PROXY_FIX = True

# =============================================================================
# DATABASE DRIVERS
# =============================================================================
# Superset cần drivers để connect đến các databases khác nhau
# Các drivers này cần được install trong Superset container
#
# Để connect đến StarRocks: pip install starrocks
# Để connect đến ClickHouse: pip install clickhouse-connect
#
# Connection strings:
# - StarRocks: mysql+pymysql://user:password@host:9030/database
# - ClickHouse: clickhousedb+connect://user:password@host:8123/database
# =============================================================================

# Allowed database drivers
ALLOWED_DB_DRIVERS = [
    'postgresql',
    'postgresql+psycopg2',
    'mysql',
    'mysql+pymysql',
    'clickhousedb+connect',
    'starrocks',
]

# =============================================================================
# LOGGING CONFIGURATION
# =============================================================================

# Log level
LOG_LEVEL = 'INFO'

# Log format
LOG_FORMAT = '%(asctime)s:%(levelname)s:%(name)s:%(message)s'

# =============================================================================
# MAPBOX (Optional - cho Map visualizations)
# =============================================================================
# Nếu muốn dùng Map charts, cần Mapbox API key
# Register tại: https://www.mapbox.com/
# =============================================================================

# MAPBOX_API_KEY = os.environ.get('MAPBOX_API_KEY', '')
