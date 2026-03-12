"""
=============================================================================
CDP REALTIME PIPELINE DAG — login_bronze → silver_login (ClickHouse)
=============================================================================
Pipeline chạy mỗi 1 phút:

  cdp.login_bronze   (ClickHouse MergeTree, Kafka Engine ghi liên tục)
           ↓  dbt_cdp_realtime (incremental append)
  cdp.silver_login   (ClickHouse ReplacingMergeTree)
           ↓
  Superset (http://localhost:8089) — realtime monitoring

Schedule: */1 * * * *
=============================================================================
"""

import os
from datetime import timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

default_args = {
    "owner": "data-engineering",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

CH_ENV = {
    **os.environ,
    "CLICKHOUSE_HOST": os.getenv("CLICKHOUSE_HOST", "clickhouse"),
    "CLICKHOUSE_HTTP_PORT": os.getenv("CLICKHOUSE_HTTP_PORT", "8123"),
    "CLICKHOUSE_USER": os.getenv("CLICKHOUSE_USER", "default"),
    "CLICKHOUSE_PASSWORD": os.getenv("CLICKHOUSE_PASSWORD", ""),
}

with DAG(
    dag_id="cdp_realtime_pipeline",
    description="CDP Realtime: login_bronze → silver_login (ClickHouse, 1min)",
    default_args=default_args,
    schedule_interval="*/1 * * * *",
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    tags=["realtime", "cdp", "login", "clickhouse"],
) as dag:

    # =========================================================================
    # KIỂM TRA BRONZE CÓ DATA KHÔNG
    # =========================================================================
    check_bronze = BashOperator(
        task_id="check_bronze_freshness",
        bash_command="""
        COUNT=$(curl -sf \
          "http://${CLICKHOUSE_HOST}:${CLICKHOUSE_HTTP_PORT}/?query=SELECT+COUNT()+FROM+cdp.login_bronze&user=${CLICKHOUSE_USER}&password=${CLICKHOUSE_PASSWORD}" \
          2>&1)
        echo "ClickHouse Bronze login_bronze rows: $COUNT"
        [ "${COUNT:-0}" -gt 0 ] || { echo "No Bronze data — CH Kafka Engine chưa consume hoặc chưa có events"; exit 1; }
        """,
        env=CH_ENV,
    )

    # =========================================================================
    # SILVER LAYER — dbt_cdp_realtime
    # cdp.login_bronze → cdp.silver_login (incremental append)
    # =========================================================================
    dbt_silver_run = BashOperator(
        task_id="dbt_silver_run",
        bash_command="""
        set -e
        cd /opt/analytics/cdp/dbt_realtime
        dbt run --profiles-dir . --select silver_login --target dev
        """,
        env=CH_ENV,
        doc_md="""
        silver_login model (incremental append, ReplacingMergeTree):
        - Source: cdp.login_bronze (ClickHouse, Kafka Engine)
        - Normalize: platform lowercase, country uppercase, empty → unknown
        - Append into cdp.silver_login (ReplacingMergeTree handles dedup)
        """,
    )

    check_bronze >> dbt_silver_run
