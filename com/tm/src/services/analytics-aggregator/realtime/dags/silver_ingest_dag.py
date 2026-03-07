"""
=============================================================================
SILVER INGEST DAG - payments_bronze → silver_events
=============================================================================
Chạy dbt_clickhouse mỗi 5 phút để sync data mới từ bronze lên silver.

Flow:
  payments_bronze (Kafka ingested, raw)
      ↓  [incremental append, 1-day overlap]
  silver_events  (normalized: status lowercase, bad amount/id filtered)

Schedule: */5 * * * *  (mỗi 5 phút)
max_active_runs=1: tránh overlap khi dbt run chậm hơn 5 phút.
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
    "retry_delay": timedelta(minutes=2),
}

CH_ENV = {
    **os.environ,
    "CLICKHOUSE_HOST": os.getenv("CLICKHOUSE_HOST", "clickhouse"),
    "CLICKHOUSE_USER": os.getenv("CLICKHOUSE_USER", "default"),
    "CLICKHOUSE_PASSWORD": os.getenv("CLICKHOUSE_PASSWORD", ""),
}

with DAG(
    dag_id="silver_ingest",
    description="payments_bronze → silver_events mỗi 1 phút (dbt incremental append)",
    default_args=default_args,
    schedule_interval="*/1 * * * *",
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,          # tránh 2 dbt run overlap
    tags=["silver", "payments", "dbt"],
) as dag:

    # =========================================================================
    # DỮ LIỆU MỚI TRONG 1 PHÚT QUA?
    # Fast check: nếu không có data mới thì skip dbt run (tiết kiệm resource)
    # =========================================================================
    check_new_data = BashOperator(
        task_id="check_new_bronze_data",
        bash_command="""
        COUNT=$(curl -s "http://clickhouse:8123/?user=default&query=SELECT+count()+FROM+tracking.payments_bronze+WHERE+ingested_at+%3E+now()-INTERVAL+6+MINUTE")
        echo "New bronze rows in last 6min: $COUNT"
        [ "$COUNT" -gt 0 ] || { echo "No new data, skipping"; exit 99; }
        """,
        env=CH_ENV,
        skip_on_exit_code=99,   # exit 99 → SKIPPED (không phải FAILED)
    )

    # =========================================================================
    # DBT RUN - silver_events (incremental append)
    # dbt-clickhouse đã được cài trong image dbt-silver (Dockerfile.dbt)
    # Airflow mount /opt/analytics → dbt nằm ở /opt/analytics/realtime/dbt
    # =========================================================================
    dbt_silver_run = BashOperator(
        task_id="dbt_silver_run",
        bash_command="""
        set -e
        cd /opt/analytics/realtime/dbt
        dbt run --profiles-dir . --select silver_events --target dev
        """,
        env=CH_ENV,
        doc_md="""
        Chạy silver_events model (incremental append):
        - Chỉ insert rows có payment_date >= max(payment_date) - 1 day
        - Normalize status: PAID → paid, invalid_xyz → unknown
        - Filter: amount <= 0, null event_id, user_id = 0 bị loại
        """,
    )

    check_new_data >> dbt_silver_run
