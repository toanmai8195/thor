"""
=============================================================================
CDP MEDALLION PIPELINE DAG — Iceberg Bronze → Silver → Gold
=============================================================================
Pipeline chạy mỗi 3 phút:

  iceberg_catalog.bronze.login_events   (Iceberg, Flink ghi liên tục)
           ↓  dbt_cdp_silver (incremental UPSERT)
  cdp.silver_login                      (StarRocks PRIMARY KEY)
           ↓  dbt_cdp_gold (full rebuild)
  cdp.gold_user_daily                   (StarRocks aggregate)
           ↓
  Superset (http://localhost:8089)

Schedule: */3 * * * *  (mỗi 3 phút — Flink commit 60s, đủ thời gian có data mới)
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
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
}

SR_ENV = {
    **os.environ,
    "STARROCKS_HOST": os.getenv("STARROCKS_HOST", "starrocks"),
    "STARROCKS_MYSQL_PORT": os.getenv("STARROCKS_MYSQL_PORT", "9030"),
}

with DAG(
    dag_id="cdp_medallion_pipeline",
    description="CDP full rebuild: Iceberg Bronze → Silver (SR cdp) → Gold (SR cdp) → Superset",
    default_args=default_args,
    schedule_interval="*/3 * * * *",
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    tags=["medallion", "cdp", "login", "starrocks"],
) as dag:

    # =========================================================================
    # KIỂM TRA BRONZE ICEBERG CÓ DATA KHÔNG
    # =========================================================================
    check_bronze = BashOperator(
        task_id="check_bronze_freshness",
        bash_command="""
        RC=0
        COUNT=$(mysql -h ${STARROCKS_HOST} -P ${STARROCKS_MYSQL_PORT} -u root \
          --batch --silent --connect-timeout=10 \
          -e "SELECT COUNT(*) FROM iceberg_catalog.bronze.login_events" 2>&1) || RC=$?
        if [ $RC -ne 0 ]; then
          echo "mysql error (RC=$RC): $COUNT"
          exit 1
        fi
        echo "Iceberg Bronze login_events total rows: $COUNT"
        [ "${COUNT}" -gt 0 ] || { echo "No Bronze data — Flink chưa commit hoặc chưa có events"; exit 1; }
        """,
        env=SR_ENV,
    )

    # =========================================================================
    # SILVER LAYER — dbt_cdp_silver
    # iceberg_catalog.bronze.login_events → cdp.silver_login (UPSERT)
    # =========================================================================
    with TaskGroup("silver_layer") as silver_tg:

        dbt_silver_run = BashOperator(
            task_id="dbt_silver_run",
            bash_command="""
            set -e
            cd /opt/analytics/cdp/dbt_silver
            dbt run --profiles-dir . --select silver_login --target dev
            """,
            env=SR_ENV,
            doc_md="""
            silver_login model (incremental UPSERT):
            - Source: iceberg_catalog.bronze.login_events (Iceberg on MinIO)
            - Normalize: platform lowercase, country uppercase, invalid → unknown
            - UPSERT into cdp.silver_login (PRIMARY KEY table)
            """,
        )

        dbt_silver_test = BashOperator(
            task_id="dbt_silver_test",
            bash_command="""
            set -e
            cd /opt/analytics/cdp/dbt_silver
            dbt test --profiles-dir . --select silver_login --target dev
            """,
            env=SR_ENV,
        )

        dbt_silver_run >> dbt_silver_test

    # =========================================================================
    # GOLD LAYER — dbt_cdp_gold
    # cdp.silver_login → cdp.gold_user_daily (full rebuild)
    # =========================================================================
    with TaskGroup("gold_layer") as gold_tg:

        dbt_gold_run = BashOperator(
            task_id="dbt_gold_run",
            bash_command="""
            set -e
            cd /opt/analytics/cdp/dbt_gold
            dbt run --profiles-dir . --target dev
            """,
            env=SR_ENV,
            doc_md="""
            gold_user_daily model (full table rebuild):
            - Source: cdp.silver_login
            - Aggregation: dau, total_logins, platform breakdown, unique_sessions/devices theo ngày
            - Target: cdp.gold_user_daily (Superset read từ đây)
            """,
        )

        dbt_gold_test = BashOperator(
            task_id="dbt_gold_test",
            bash_command="""
            set -e
            cd /opt/analytics/cdp/dbt_gold
            dbt test --profiles-dir . --target dev
            """,
            env=SR_ENV,
        )

        dbt_gold_run >> dbt_gold_test

    # =========================================================================
    # PIPELINE FLOW
    # =========================================================================
    check_bronze >> silver_tg >> gold_tg
