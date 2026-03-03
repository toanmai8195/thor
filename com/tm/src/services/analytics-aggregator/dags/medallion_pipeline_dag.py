"""
=============================================================================
MEDALLION PIPELINE DAG - Payment Revenue
=============================================================================
Pipeline đơn giản:

  [ClickHouse: payments_bronze]
           ↓
  [dbt_clickhouse: silver_payments]  → normalize status/amount
           ↓
  [Sync: ClickHouse → StarRocks]
           ↓
  [dbt_starrocks: gold_revenue]      → total_revenue, total_paid_orders
           ↓
  [Superset]

Schedule: Daily 02:00 UTC
=============================================================================
"""

import os
import sys
from datetime import timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.dates import days_ago

sys.path.insert(0, "/opt/analytics")

default_args = {
    "owner": "data-engineering",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
}

CH_ENV = {
    "CLICKHOUSE_HOST": os.getenv("CLICKHOUSE_HOST", "clickhouse"),
    "CLICKHOUSE_USER": "default",
    "CLICKHOUSE_PASSWORD": "",
}

SR_ENV = {
    "STARROCKS_HOST": os.getenv("STARROCKS_HOST", "starrocks-fe"),
    "STARROCKS_MYSQL_PORT": os.getenv("STARROCKS_MYSQL_PORT", "9030"),
}

with DAG(
    dag_id="medallion_pipeline",
    description="Bronze → Silver (CH) → Sync → Gold (SR) → Superset",
    default_args=default_args,
    schedule_interval="0 2 * * *",
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    tags=["medallion", "payments"],
) as dag:

    # =========================================================================
    # KIỂM TRA DỮ LIỆU BRONZE
    # =========================================================================
    check_bronze = BashOperator(
        task_id="check_bronze_freshness",
        bash_command="""
        COUNT=$(curl -s "http://clickhouse:8123/?query=SELECT+count()+FROM+tracking.payments_bronze+WHERE+updated_at+>+now()-INTERVAL+2+HOUR&user=default")
        echo "Recent events: $COUNT"
        [ "$COUNT" -gt 0 ] || { echo "No recent bronze data"; exit 1; }
        """,
    )

    # =========================================================================
    # SILVER LAYER (dbt_clickhouse)
    # =========================================================================
    with TaskGroup("silver_layer") as silver_tg:

        dbt_ch_run = BashOperator(
            task_id="dbt_ch_run",
            bash_command="""
            cd /opt/analytics/dbt_clickhouse
            dbt deps --profiles-dir . && dbt run --profiles-dir . --target dev
            """,
            env=CH_ENV,
            doc_md="silver_events.sql: normalize status/amount từ payments_bronze",
        )

        dbt_ch_test = BashOperator(
            task_id="dbt_ch_test",
            bash_command="cd /opt/analytics/dbt_clickhouse && dbt test --profiles-dir . --target dev",
            env=CH_ENV,
        )

        dbt_ch_run >> dbt_ch_test

    # =========================================================================
    # SYNC: ClickHouse Silver → StarRocks
    # =========================================================================
    from sync.ch_to_starrocks import run_sync

    sync_silver = PythonOperator(
        task_id="sync_silver_payments",
        python_callable=run_sync,
        doc_md="Copy silver_payments từ ClickHouse → StarRocks qua Stream Load",
    )

    # =========================================================================
    # GOLD LAYER (dbt_starrocks)
    # =========================================================================
    with TaskGroup("gold_layer") as gold_tg:

        dbt_sr_run = BashOperator(
            task_id="dbt_sr_run",
            bash_command="""
            cd /opt/analytics/dbt_starrocks
            dbt deps --profiles-dir . && dbt run --profiles-dir . --target dev
            """,
            env=SR_ENV,
            doc_md="gold_revenue.sql: total_revenue, total_paid_orders theo ngày",
        )

        dbt_sr_test = BashOperator(
            task_id="dbt_sr_test",
            bash_command="cd /opt/analytics/dbt_starrocks && dbt test --profiles-dir . --target dev",
            env=SR_ENV,
        )

        dbt_sr_run >> dbt_sr_test

    # =========================================================================
    # PIPELINE FLOW
    # =========================================================================
    check_bronze >> silver_tg >> sync_silver >> gold_tg
