"""
=============================================================================
MEDALLION PIPELINE DAG (DW) — Iceberg Bronze → Silver → Gold
=============================================================================
Full rebuild pipeline chạy daily:

  iceberg_catalog.bronze.payments   (Iceberg, Flink ghi liên tục)
           ↓  dbt_starrocks_silver (incremental UPSERT)
  tracking.silver_payments          (StarRocks PRIMARY KEY)
           ↓  dbt_starrocks_gold (full rebuild)
  analytics.gold_revenue            (StarRocks daily aggregate)
           ↓
  Superset (http://localhost:8088)

Schedule: 0 2 * * *  (daily 02:00 UTC — sau khi data cuối ngày đã commit vào Iceberg)
max_active_runs=1: tránh đồng thời 2 pipeline.
=============================================================================
"""

import os
from datetime import timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.dates import days_ago

default_args = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
}

SR_ENV = {
    **os.environ,
    "STARROCKS_HOST": os.getenv("STARROCKS_HOST", "starrocks"),
    "STARROCKS_MYSQL_PORT": os.getenv("STARROCKS_MYSQL_PORT", "9030"),
}

with DAG(
    dag_id="dw_medallion_pipeline",
    description="DW full rebuild: Iceberg Bronze → Silver (SR) → Gold (SR) → Superset",
    default_args=default_args,
    schedule_interval="0 2 * * *",
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    tags=["medallion", "dw", "payments", "starrocks"],
) as dag:

    # =========================================================================
    # KIỂM TRA BRONZE ICEBERG CÓ DATA KHÔNG
    # Chờ Flink đã commit ít nhất 1 checkpoint vào Iceberg trước khi chạy
    # =========================================================================
    check_bronze = BashOperator(
        task_id="check_bronze_freshness",
        bash_command="""
        COUNT=$(mysql -h ${STARROCKS_HOST} -P ${STARROCKS_MYSQL_PORT} -u root \
          --batch --silent --connect-timeout=10 \
          -e "SELECT COUNT(*) FROM iceberg_catalog.bronze.payments \
              WHERE payment_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)" 2>/dev/null || echo "0")
        echo "Iceberg Bronze rows (yesterday+today): $COUNT"
        [ "${COUNT}" -gt 0 ] || { echo "No Bronze data — Flink chưa commit hoặc chưa có events"; exit 1; }
        """,
        env=SR_ENV,
    )

    # =========================================================================
    # SILVER LAYER — dbt_starrocks_silver
    # iceberg_catalog.bronze.payments → tracking.silver_payments (UPSERT)
    # =========================================================================
    with TaskGroup("silver_layer") as silver_tg:

        dbt_silver_run = BashOperator(
            task_id="dbt_silver_run",
            bash_command="""
            set -e
            cd /opt/analytics/dw/dbt_silver
            dbt run --profiles-dir . --select silver_payments --target dev
            """,
            env=SR_ENV,
            doc_md="""
            silver_payments model (incremental UPSERT):
            - Source: iceberg_catalog.bronze.payments (Iceberg on MinIO)
            - Normalize: status lowercase, invalid → unknown, filter amount ≤ 0
            - UPSERT into tracking.silver_payments (PRIMARY KEY table)
            """,
        )

        dbt_silver_test = BashOperator(
            task_id="dbt_silver_test",
            bash_command="""
            set -e
            cd /opt/analytics/dw/dbt_silver
            dbt test --profiles-dir . --select silver_payments --target dev
            """,
            env=SR_ENV,
        )

        dbt_silver_run >> dbt_silver_test

    # =========================================================================
    # GOLD LAYER — dbt_starrocks_gold
    # tracking.silver_payments → analytics.gold_revenue (full rebuild)
    # =========================================================================
    with TaskGroup("gold_layer") as gold_tg:

        dbt_gold_run = BashOperator(
            task_id="dbt_gold_run",
            bash_command="""
            set -e
            cd /opt/analytics/dw/dbt
            dbt run --profiles-dir . --target dev
            """,
            env=SR_ENV,
            doc_md="""
            gold_revenue model (full table rebuild):
            - Source: tracking.silver_payments
            - Aggregation: total_revenue, total_paid_orders, avg_order_value, unique_users theo ngày
            - Target: analytics.gold_revenue (Superset read từ đây)
            """,
        )

        dbt_gold_test = BashOperator(
            task_id="dbt_gold_test",
            bash_command="""
            set -e
            cd /opt/analytics/dw/dbt
            dbt test --profiles-dir . --target dev
            """,
            env=SR_ENV,
        )

        dbt_gold_run >> dbt_gold_test

    # =========================================================================
    # PIPELINE FLOW
    # =========================================================================
    check_bronze >> silver_tg >> gold_tg
