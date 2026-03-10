"""
=============================================================================
SILVER INGEST DAG (DW) — iceberg_catalog.bronze.payments → tracking.silver_payments
=============================================================================
Chạy dbt_starrocks_silver mỗi 15 phút để transform data mới từ Iceberg Bronze
lên StarRocks Silver.

Flow:
  iceberg_catalog.bronze.payments (Flink ghi, commit mỗi 60s)
      ↓  [incremental, 1-day overlap, UPSERT bởi PRIMARY KEY]
  tracking.silver_payments (StarRocks native, normalized)

Schedule: */15 * * * *  (mỗi 15 phút — Flink commit 60s, đủ thời gian có data mới)
max_active_runs=1: tránh overlap khi dbt run chậm.

Source: iceberg_catalog.bronze.payments  (Parquet, MinIO)
Target: tracking.silver_payments         (StarRocks PRIMARY KEY)
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
    "retry_delay": timedelta(minutes=3),
}

SR_ENV = {
    **os.environ,
    "STARROCKS_HOST": os.getenv("STARROCKS_HOST", "starrocks"),
    "STARROCKS_MYSQL_PORT": os.getenv("STARROCKS_MYSQL_PORT", "9030"),
}

with DAG(
    dag_id="dw_silver_ingest",
    description="Iceberg Bronze → StarRocks Silver mỗi 15 phút (dbt incremental UPSERT)",
    default_args=default_args,
    schedule_interval="*/15 * * * *",
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    tags=["silver", "dw", "payments", "dbt", "starrocks"],
) as dag:

    # =========================================================================
    # DỮ LIỆU MỚI TRONG ICEBERG BRONZE?
    # Fast check: query StarRocks xem Iceberg Bronze có data không.
    # Nếu chưa có (Flink chưa commit) thì skip dbt run.
    # =========================================================================
    check_new_data = BashOperator(
        task_id="check_iceberg_bronze",
        bash_command="""
        COUNT=$(mysql -h ${STARROCKS_HOST} -P ${STARROCKS_MYSQL_PORT} -u root \
          --batch --silent --connect-timeout=10 \
          -e "SELECT COUNT(*) FROM iceberg_catalog.bronze.payments \
              WHERE ingested_at >= NOW() - INTERVAL 20 MINUTE" 2>/dev/null || echo "0")
        echo "Iceberg Bronze rows in last 20min: $COUNT"
        [ "${COUNT}" -gt 0 ] || { echo "No new data in Iceberg Bronze, skipping"; exit 99; }
        """,
        env=SR_ENV,
        skip_on_exit_code=99,
    )

    # =========================================================================
    # DBT RUN — silver_payments (incremental UPSERT vào StarRocks PRIMARY KEY table)
    # Airflow mount /opt/analytics → dbt nằm ở /opt/analytics/dw/dbt_silver
    # =========================================================================
    dbt_silver_run = BashOperator(
        task_id="dbt_silver_run",
        bash_command="""
        set -e
        cd /opt/analytics/dw/dbt_silver
        dbt run --profiles-dir . --select silver_payments --target dev
        """,
        env=SR_ENV,
        doc_md="""
        Chạy silver_payments model (incremental UPSERT vào StarRocks):
        - Source: iceberg_catalog.bronze.payments (Iceberg external catalog)
        - Filter: payment_date >= MAX(payment_date) - 1 day (incremental window)
        - Normalize status: PAID → paid, invalid_xyz → unknown
        - Filter bad data: amount <= 0, null event_id, user_id = 0
        - UPSERT: StarRocks PRIMARY KEY table tự dedup theo (event_id, payment_date)
        """,
    )

    check_new_data >> dbt_silver_run
