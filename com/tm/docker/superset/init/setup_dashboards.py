#!/usr/bin/env python3
# =============================================================================
# SUPERSET DASHBOARD SETUP SCRIPT
# =============================================================================
# Script này tạo dashboards và charts trong Superset bằng code (Infrastructure as Code).
#
# TẠI SAO DÙNG CODE ĐỂ TẠO DASHBOARDS?
# - Version control: Track changes, rollback
# - Reproducible: Deploy giống nhau ở mọi environment
# - Automated: CI/CD integration
# - Documentation: Code = documentation
#
# SUPERSET CONCEPTS:
#
# 1. Database Connection:
#    - Kết nối đến data sources (StarRocks, ClickHouse, etc.)
#    - Định nghĩa connection string
#
# 2. Dataset:
#    - Table hoặc SQL query
#    - Metrics và columns definitions
#    - Dùng để tạo charts
#
# 3. Chart (Slice):
#    - Visualization của data
#    - Types: line, bar, pie, table, etc.
#    - Query params + viz params
#
# 4. Dashboard:
#    - Collection of charts
#    - Layout configuration
#    - Filters
#
# USAGE:
# python setup_dashboards.py
#
# Hoặc chạy trong Superset container:
# docker exec superset python /app/init/setup_dashboards.py
# =============================================================================

import json
import logging
from typing import Any, Dict, List, Optional

# Superset imports (available inside Superset container)
try:
    from superset import app, db
    from superset.connectors.sqla.models import SqlaTable, TableColumn, SqlMetric
    from superset.models.core import Database
    from superset.models.dashboard import Dashboard
    from superset.models.slice import Slice
    SUPERSET_AVAILABLE = True
except ImportError:
    SUPERSET_AVAILABLE = False
    print("Warning: Superset modules not available. Running in demo mode.")

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# =============================================================================
# DATABASE CONNECTIONS
# =============================================================================
# Định nghĩa các database connections.
# =============================================================================

DATABASE_CONFIGS = {
    # StarRocks connection (Primary analytics database)
    'starrocks': {
        'database_name': 'StarRocks Analytics',
        'sqlalchemy_uri': 'mysql+pymysql://root:@starrocks-fe:9030/analytics',
        'extra': json.dumps({
            'metadata_params': {},
            'engine_params': {},
            'metadata_cache_timeout': {},
            'schemas_allowed_for_csv_upload': [],
        }),
    },
    # ClickHouse connection (Raw data)
    'clickhouse': {
        'database_name': 'ClickHouse Raw',
        'sqlalchemy_uri': 'clickhousedb+connect://default:@clickhouse:8123/tracking',
        'extra': json.dumps({
            'connect_args': {
                'connect_timeout': 10,
            },
        }),
    },
}

# =============================================================================
# DATASET DEFINITIONS
# =============================================================================
# Datasets là data sources cho charts.
# Có thể là table hoặc SQL query.
# =============================================================================

DATASET_CONFIGS = {
    # -------------------------------------------------------------------------
    # Daily Metrics Dataset
    # -------------------------------------------------------------------------
    'daily_metrics': {
        'table_name': 'fct_daily_metrics',
        'database_name': 'StarRocks Analytics',
        'description': '''
        Daily aggregated metrics cho dropshipping platform.

        **Grain:** 1 row = 1 day + 1 platform + 1 country

        **Use cases:**
        - Daily KPI dashboards
        - Trend analysis
        - Performance monitoring
        ''',
        # Columns to expose in Superset
        'columns': [
            {'column_name': 'date_day', 'type': 'DATE', 'is_dttm': True, 'description': 'Date'},
            {'column_name': 'platform', 'type': 'VARCHAR', 'description': 'Platform: web, ios, android'},
            {'column_name': 'country_code', 'type': 'VARCHAR', 'description': 'ISO country code'},
            {'column_name': 'total_sessions', 'type': 'BIGINT', 'description': 'Total sessions'},
            {'column_name': 'unique_users', 'type': 'BIGINT', 'description': 'Unique registered users'},
            {'column_name': 'page_views', 'type': 'BIGINT', 'description': 'Total page views'},
            {'column_name': 'product_views', 'type': 'BIGINT', 'description': 'Product detail views'},
            {'column_name': 'add_to_cart_events', 'type': 'BIGINT', 'description': 'Add to cart events'},
            {'column_name': 'total_orders', 'type': 'BIGINT', 'description': 'Total orders'},
            {'column_name': 'gross_revenue_cents', 'type': 'BIGINT', 'description': 'Revenue in cents'},
            {'column_name': 'gross_profit_cents', 'type': 'BIGINT', 'description': 'Profit in cents'},
            {'column_name': 'conversion_rate', 'type': 'DOUBLE', 'description': 'Conversion rate'},
            {'column_name': 'aov_cents', 'type': 'DOUBLE', 'description': 'Average order value'},
        ],
        # Pre-defined metrics
        'metrics': [
            {
                'metric_name': 'total_revenue',
                'expression': 'SUM(gross_revenue_cents) / 100',
                'metric_type': 'SUM',
                'description': 'Total revenue in dollars',
            },
            {
                'metric_name': 'total_profit',
                'expression': 'SUM(gross_profit_cents) / 100',
                'metric_type': 'SUM',
                'description': 'Total profit in dollars',
            },
            {
                'metric_name': 'avg_conversion_rate',
                'expression': 'AVG(conversion_rate)',
                'metric_type': 'AVG',
                'description': 'Average conversion rate',
            },
            {
                'metric_name': 'total_sessions_sum',
                'expression': 'SUM(total_sessions)',
                'metric_type': 'SUM',
                'description': 'Total sessions',
            },
        ],
    },

    # -------------------------------------------------------------------------
    # Product Performance Dataset
    # -------------------------------------------------------------------------
    'product_performance': {
        'table_name': 'fct_product_performance',
        'database_name': 'StarRocks Analytics',
        'description': '''
        Product performance metrics cho phân tích sản phẩm.

        **Grain:** 1 row = 1 product + 1 date

        **Use cases:**
        - Product ranking
        - Margin analysis
        - Conversion optimization
        ''',
        'columns': [
            {'column_name': 'date_day', 'type': 'DATE', 'is_dttm': True},
            {'column_name': 'product_id', 'type': 'BIGINT'},
            {'column_name': 'product_name', 'type': 'VARCHAR'},
            {'column_name': 'category_name', 'type': 'VARCHAR'},
            {'column_name': 'brand', 'type': 'VARCHAR'},
            {'column_name': 'supplier_name', 'type': 'VARCHAR'},
            {'column_name': 'views', 'type': 'BIGINT'},
            {'column_name': 'units_sold', 'type': 'BIGINT'},
            {'column_name': 'revenue_cents', 'type': 'BIGINT'},
            {'column_name': 'gross_profit_cents', 'type': 'BIGINT'},
            {'column_name': 'conversion_rate', 'type': 'DOUBLE'},
            {'column_name': 'gross_margin_pct', 'type': 'DOUBLE'},
            {'column_name': 'product_health_score', 'type': 'DOUBLE'},
        ],
        'metrics': [
            {
                'metric_name': 'product_revenue',
                'expression': 'SUM(revenue_cents) / 100',
                'metric_type': 'SUM',
                'description': 'Product revenue in dollars',
            },
            {
                'metric_name': 'product_profit',
                'expression': 'SUM(gross_profit_cents) / 100',
                'metric_type': 'SUM',
                'description': 'Product profit in dollars',
            },
            {
                'metric_name': 'total_units',
                'expression': 'SUM(units_sold)',
                'metric_type': 'SUM',
                'description': 'Total units sold',
            },
        ],
    },

    # -------------------------------------------------------------------------
    # Marketing Attribution Dataset
    # -------------------------------------------------------------------------
    'marketing_attribution': {
        'table_name': 'fct_marketing_attribution',
        'database_name': 'StarRocks Analytics',
        'description': '''
        Marketing channel performance và attribution.

        **Grain:** 1 row = 1 source + 1 medium + 1 campaign + 1 date

        **Use cases:**
        - Channel ROI analysis
        - Campaign performance
        - Budget allocation
        ''',
        'columns': [
            {'column_name': 'date_day', 'type': 'DATE', 'is_dttm': True},
            {'column_name': 'traffic_source', 'type': 'VARCHAR'},
            {'column_name': 'source', 'type': 'VARCHAR'},
            {'column_name': 'medium', 'type': 'VARCHAR'},
            {'column_name': 'campaign', 'type': 'VARCHAR'},
            {'column_name': 'sessions', 'type': 'BIGINT'},
            {'column_name': 'conversions', 'type': 'BIGINT'},
            {'column_name': 'total_revenue_cents', 'type': 'BIGINT'},
            {'column_name': 'conversion_rate', 'type': 'DOUBLE'},
            {'column_name': 'aov_cents', 'type': 'DOUBLE'},
            {'column_name': 'channel_efficiency_score', 'type': 'DOUBLE'},
        ],
        'metrics': [
            {
                'metric_name': 'channel_revenue',
                'expression': 'SUM(total_revenue_cents) / 100',
                'metric_type': 'SUM',
                'description': 'Channel revenue in dollars',
            },
            {
                'metric_name': 'total_conversions',
                'expression': 'SUM(conversions)',
                'metric_type': 'SUM',
                'description': 'Total conversions',
            },
        ],
    },
}

# =============================================================================
# CHART DEFINITIONS
# =============================================================================
# Charts visualize data từ datasets.
# Mỗi chart có:
# - viz_type: Loại chart (line, bar, table, etc.)
# - datasource: Dataset để query
# - params: Query và visualization parameters
# =============================================================================

CHART_CONFIGS = {
    # -------------------------------------------------------------------------
    # Revenue Trend Line Chart
    # -------------------------------------------------------------------------
    'revenue_trend': {
        'slice_name': 'Daily Revenue Trend',
        'viz_type': 'echarts_timeseries_line',
        'datasource_name': 'daily_metrics',
        'description': 'Daily revenue trend over time',
        'params': {
            'datasource': None,  # Will be set dynamically
            'viz_type': 'echarts_timeseries_line',
            'time_column': 'date_day',
            'time_grain_sqla': 'P1D',  # Daily
            'metrics': ['total_revenue'],
            'groupby': [],
            'row_limit': 10000,
            'order_desc': True,
            'show_legend': True,
            'rich_tooltip': True,
            'x_axis_title': 'Date',
            'y_axis_title': 'Revenue ($)',
        },
    },

    # -------------------------------------------------------------------------
    # Conversion Funnel
    # -------------------------------------------------------------------------
    'conversion_funnel': {
        'slice_name': 'Conversion Funnel',
        'viz_type': 'funnel',
        'datasource_name': 'daily_metrics',
        'description': 'User journey funnel: views -> cart -> purchase',
        'params': {
            'viz_type': 'funnel',
            'metric': 'total_sessions_sum',
            'groupby': [],
            'row_limit': 10,
        },
    },

    # -------------------------------------------------------------------------
    # Top Products Table
    # -------------------------------------------------------------------------
    'top_products': {
        'slice_name': 'Top Products by Revenue',
        'viz_type': 'table',
        'datasource_name': 'product_performance',
        'description': 'Top performing products by revenue',
        'params': {
            'viz_type': 'table',
            'query_mode': 'aggregate',
            'groupby': ['product_name', 'category_name', 'brand'],
            'metrics': ['product_revenue', 'product_profit', 'total_units'],
            'order_by_cols': [['product_revenue', False]],  # DESC
            'row_limit': 20,
            'table_timestamp_format': '%Y-%m-%d',
            'page_length': 20,
            'include_search': True,
        },
    },

    # -------------------------------------------------------------------------
    # Revenue by Country Map
    # -------------------------------------------------------------------------
    'revenue_by_country': {
        'slice_name': 'Revenue by Country',
        'viz_type': 'world_map',
        'datasource_name': 'daily_metrics',
        'description': 'Geographic distribution of revenue',
        'params': {
            'viz_type': 'world_map',
            'entity': 'country_code',
            'metric': 'total_revenue',
            'show_bubbles': True,
            'color_scheme': 'supersetColors',
        },
    },

    # -------------------------------------------------------------------------
    # Marketing Channel Performance
    # -------------------------------------------------------------------------
    'channel_performance': {
        'slice_name': 'Marketing Channel Performance',
        'viz_type': 'echarts_bar',
        'datasource_name': 'marketing_attribution',
        'description': 'Revenue by marketing channel',
        'params': {
            'viz_type': 'echarts_bar',
            'groupby': ['traffic_source'],
            'metrics': ['channel_revenue', 'total_conversions'],
            'row_limit': 10,
            'order_desc': True,
            'show_legend': True,
        },
    },

    # -------------------------------------------------------------------------
    # Conversion Rate by Platform
    # -------------------------------------------------------------------------
    'conversion_by_platform': {
        'slice_name': 'Conversion Rate by Platform',
        'viz_type': 'pie',
        'datasource_name': 'daily_metrics',
        'description': 'Conversion rate breakdown by platform',
        'params': {
            'viz_type': 'pie',
            'groupby': ['platform'],
            'metric': 'avg_conversion_rate',
            'show_labels': True,
            'show_legend': True,
            'color_scheme': 'supersetColors',
        },
    },

    # -------------------------------------------------------------------------
    # Profit Margin Trend
    # -------------------------------------------------------------------------
    'margin_trend': {
        'slice_name': 'Gross Margin Trend',
        'viz_type': 'echarts_timeseries_line',
        'datasource_name': 'daily_metrics',
        'description': 'Gross margin percentage over time',
        'params': {
            'viz_type': 'echarts_timeseries_line',
            'time_column': 'date_day',
            'time_grain_sqla': 'P1D',
            'metrics': [{
                'label': 'Gross Margin %',
                'expressionType': 'SQL',
                'sqlExpression': 'SUM(gross_profit_cents) * 100.0 / NULLIF(SUM(gross_revenue_cents), 0)',
            }],
            'groupby': [],
            'y_axis_format': '.1%',
        },
    },
}

# =============================================================================
# DASHBOARD DEFINITIONS
# =============================================================================
# Dashboards là collection of charts với layout.
# =============================================================================

DASHBOARD_CONFIGS = {
    # -------------------------------------------------------------------------
    # Executive KPI Dashboard
    # -------------------------------------------------------------------------
    'executive_kpi': {
        'dashboard_title': 'Executive KPI Dashboard',
        'slug': 'executive-kpi',
        'description': '''
        High-level KPIs cho leadership team.

        **Metrics included:**
        - Revenue & Profit trends
        - Conversion rates
        - Top products
        - Geographic breakdown
        ''',
        'charts': [
            'revenue_trend',
            'conversion_funnel',
            'top_products',
            'revenue_by_country',
            'margin_trend',
        ],
        # Layout positions (grid-based)
        'position_json': {
            'revenue_trend': {'x': 0, 'y': 0, 'w': 8, 'h': 4},
            'conversion_funnel': {'x': 8, 'y': 0, 'w': 4, 'h': 4},
            'margin_trend': {'x': 0, 'y': 4, 'w': 6, 'h': 4},
            'revenue_by_country': {'x': 6, 'y': 4, 'w': 6, 'h': 4},
            'top_products': {'x': 0, 'y': 8, 'w': 12, 'h': 6},
        },
    },

    # -------------------------------------------------------------------------
    # Marketing Performance Dashboard
    # -------------------------------------------------------------------------
    'marketing_performance': {
        'dashboard_title': 'Marketing Performance',
        'slug': 'marketing-performance',
        'description': '''
        Marketing channel và campaign analysis.

        **Insights:**
        - Channel ROI
        - Campaign effectiveness
        - Traffic source breakdown
        ''',
        'charts': [
            'channel_performance',
            'conversion_by_platform',
        ],
        'position_json': {
            'channel_performance': {'x': 0, 'y': 0, 'w': 8, 'h': 6},
            'conversion_by_platform': {'x': 8, 'y': 0, 'w': 4, 'h': 6},
        },
    },
}

# =============================================================================
# SETUP FUNCTIONS
# =============================================================================

def setup_databases():
    """Create database connections."""
    if not SUPERSET_AVAILABLE:
        logger.info("Demo mode: Would create database connections")
        for name, config in DATABASE_CONFIGS.items():
            logger.info(f"  - {config['database_name']}: {config['sqlalchemy_uri']}")
        return {}

    databases = {}
    for name, config in DATABASE_CONFIGS.items():
        db_name = config['database_name']

        # Check if exists
        existing = db.session.query(Database).filter_by(database_name=db_name).first()
        if existing:
            logger.info(f"Database '{db_name}' already exists")
            databases[name] = existing
            continue

        # Create new
        database = Database(
            database_name=db_name,
            sqlalchemy_uri=config['sqlalchemy_uri'],
            extra=config.get('extra', '{}'),
        )
        db.session.add(database)
        db.session.commit()
        logger.info(f"Created database '{db_name}'")
        databases[name] = database

    return databases


def setup_datasets(databases: dict):
    """Create datasets from table definitions."""
    if not SUPERSET_AVAILABLE:
        logger.info("Demo mode: Would create datasets")
        for name, config in DATASET_CONFIGS.items():
            logger.info(f"  - {config['table_name']}")
        return {}

    datasets = {}
    for name, config in DATASET_CONFIGS.items():
        table_name = config['table_name']
        db_name = config['database_name']

        # Find database
        database = None
        for db_key, db_obj in databases.items():
            if db_obj.database_name == db_name:
                database = db_obj
                break

        if not database:
            logger.warning(f"Database '{db_name}' not found for dataset '{name}'")
            continue

        # Check if exists
        existing = db.session.query(SqlaTable).filter_by(
            table_name=table_name,
            database_id=database.id
        ).first()

        if existing:
            logger.info(f"Dataset '{table_name}' already exists")
            datasets[name] = existing
            continue

        # Create dataset
        dataset = SqlaTable(
            table_name=table_name,
            database_id=database.id,
            description=config.get('description', ''),
        )
        db.session.add(dataset)
        db.session.flush()

        # Add columns
        for col_config in config.get('columns', []):
            column = TableColumn(
                column_name=col_config['column_name'],
                type=col_config.get('type', 'VARCHAR'),
                is_dttm=col_config.get('is_dttm', False),
                description=col_config.get('description', ''),
                table=dataset,
            )
            db.session.add(column)

        # Add metrics
        for metric_config in config.get('metrics', []):
            metric = SqlMetric(
                metric_name=metric_config['metric_name'],
                expression=metric_config['expression'],
                metric_type=metric_config.get('metric_type', ''),
                description=metric_config.get('description', ''),
                table=dataset,
            )
            db.session.add(metric)

        db.session.commit()
        logger.info(f"Created dataset '{table_name}'")
        datasets[name] = dataset

    return datasets


def setup_charts(datasets: dict):
    """Create charts from definitions."""
    if not SUPERSET_AVAILABLE:
        logger.info("Demo mode: Would create charts")
        for name, config in CHART_CONFIGS.items():
            logger.info(f"  - {config['slice_name']}")
        return {}

    charts = {}
    for name, config in CHART_CONFIGS.items():
        slice_name = config['slice_name']
        datasource_name = config['datasource_name']

        # Find dataset
        dataset = datasets.get(datasource_name)
        if not dataset:
            logger.warning(f"Dataset '{datasource_name}' not found for chart '{name}'")
            continue

        # Check if exists
        existing = db.session.query(Slice).filter_by(slice_name=slice_name).first()
        if existing:
            logger.info(f"Chart '{slice_name}' already exists")
            charts[name] = existing
            continue

        # Update params with datasource
        params = config['params'].copy()
        params['datasource'] = f'{dataset.id}__table'

        # Create chart
        chart = Slice(
            slice_name=slice_name,
            viz_type=config['viz_type'],
            datasource_id=dataset.id,
            datasource_type='table',
            description=config.get('description', ''),
            params=json.dumps(params),
        )
        db.session.add(chart)
        db.session.commit()
        logger.info(f"Created chart '{slice_name}'")
        charts[name] = chart

    return charts


def setup_dashboards(charts: dict):
    """Create dashboards from definitions."""
    if not SUPERSET_AVAILABLE:
        logger.info("Demo mode: Would create dashboards")
        for name, config in DASHBOARD_CONFIGS.items():
            logger.info(f"  - {config['dashboard_title']}")
        return

    for name, config in DASHBOARD_CONFIGS.items():
        dashboard_title = config['dashboard_title']

        # Check if exists
        existing = db.session.query(Dashboard).filter_by(slug=config['slug']).first()
        if existing:
            logger.info(f"Dashboard '{dashboard_title}' already exists")
            continue

        # Get charts for this dashboard
        dashboard_charts = []
        for chart_name in config['charts']:
            chart = charts.get(chart_name)
            if chart:
                dashboard_charts.append(chart)

        # Build position JSON
        position_json = {}
        for i, chart in enumerate(dashboard_charts):
            chart_name = config['charts'][i]
            pos = config['position_json'].get(chart_name, {'x': 0, 'y': i*4, 'w': 12, 'h': 4})
            position_json[f'CHART-{chart.id}'] = {
                'type': 'CHART',
                'id': f'CHART-{chart.id}',
                'meta': {'chartId': chart.id, 'width': pos['w'], 'height': pos['h']},
                'children': [],
            }

        # Create dashboard
        dashboard = Dashboard(
            dashboard_title=dashboard_title,
            slug=config['slug'],
            description=config.get('description', ''),
            slices=dashboard_charts,
            position_json=json.dumps(position_json),
        )
        db.session.add(dashboard)
        db.session.commit()
        logger.info(f"Created dashboard '{dashboard_title}'")


def main():
    """Main setup function."""
    logger.info("=" * 60)
    logger.info("SUPERSET DASHBOARD SETUP")
    logger.info("=" * 60)

    # Setup databases
    logger.info("\n1. Setting up database connections...")
    databases = setup_databases()

    # Setup datasets
    logger.info("\n2. Setting up datasets...")
    datasets = setup_datasets(databases)

    # Setup charts
    logger.info("\n3. Setting up charts...")
    charts = setup_charts(datasets)

    # Setup dashboards
    logger.info("\n4. Setting up dashboards...")
    setup_dashboards(charts)

    logger.info("\n" + "=" * 60)
    logger.info("SETUP COMPLETE!")
    logger.info("=" * 60)

    if SUPERSET_AVAILABLE:
        logger.info("\nAccess Superset at: http://localhost:8088")
        logger.info("Default credentials: admin / admin123")
    else:
        logger.info("\nDemo mode - no actual changes made")


if __name__ == '__main__':
    main()
