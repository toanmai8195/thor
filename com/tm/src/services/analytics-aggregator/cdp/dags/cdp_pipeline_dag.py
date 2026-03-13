"""
=============================================================================
CDP MEDALLION PIPELINE DAG — Registry-driven (Step 2: Metadata Layer)
=============================================================================
DAG đọc event_registry.yml, tự động tạo TaskGroup cho từng enabled source.
Thêm source mới: thêm entry vào registry + tạo dbt model — không sửa DAG.

Flow per source:
  iceberg_catalog.bronze.<table>  (Iceberg, Flink ghi liên tục)
           ↓  dbt_cdp_silver (incremental UPSERT, 3min)
  cdp.<silver_model>              (StarRocks PRIMARY KEY)
           ↓  dbt_cdp_gold (full rebuild, 3min) — nếu có gold model
  cdp.<gold_model>

Schedule: */3 * * * *
=============================================================================
"""

import os
import yaml
from datetime import timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.dates import days_ago

# =============================================================================
# Registry — đọc lúc DAG parse, không query DB
# =============================================================================
REGISTRY_PATH = Path("/opt/analytics/cdp/registry/event_registry.yml")

def _load_registry():
    if not REGISTRY_PATH.exists():
        # Fallback: login hardcoded nếu registry chưa mount
        return [{"source_id": "login", "sr_silver_model": "silver_login", "sr_gold_model": "gold_user_daily", "enabled": True}]
    with open(REGISTRY_PATH) as f:
        data = yaml.safe_load(f)
    return [s for s in data.get("sources", []) if s.get("enabled", True)]

ENABLED_SOURCES = _load_registry()

# =============================================================================
# Env / config
# =============================================================================
SR_ENV = {
    **os.environ,
    "STARROCKS_HOST":       os.getenv("STARROCKS_HOST", "starrocks"),
    "STARROCKS_MYSQL_PORT": os.getenv("STARROCKS_MYSQL_PORT", "9030"),
}

default_args = {
    "owner":        "data-engineering",
    "retries":      1,
    "retry_delay":  timedelta(minutes=3),
}

# =============================================================================
# DAG
# =============================================================================
with DAG(
    dag_id="cdp_medallion_pipeline",
    description="CDP Medallion pipeline — registry-driven, all enabled sources",
    default_args=default_args,
    schedule_interval="*/3 * * * *",
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    tags=["medallion", "cdp", "starrocks"],
) as dag:

    # =========================================================================
    # CHECK BRONZE — verify Iceberg có data trước khi transform
    # =========================================================================
    check_bronze = BashOperator(
        task_id="check_bronze_freshness",
        bash_command="""
        set -e
        TOTAL=0
        {% for src in sources %}
        COUNT=$(mysql -h ${STARROCKS_HOST} -P ${STARROCKS_MYSQL_PORT} -u root \
          --batch --silent --connect-timeout=10 \
          -e "SELECT COUNT(*) FROM iceberg_catalog.{{ src['iceberg_database'] }}.{{ src['iceberg_table'] }}" 2>&1)
        echo "Bronze {{ src['source_id'] }}.{{ src['iceberg_table'] }}: $COUNT rows"
        TOTAL=$((TOTAL + COUNT))
        {% endfor %}
        [ "$TOTAL" -gt 0 ] || { echo "No Bronze data across all sources"; exit 1; }
        """.replace("{% for src in sources %}", "\n".join(
            f"""
        COUNT=$(mysql -h ${{STARROCKS_HOST}} -P ${{STARROCKS_MYSQL_PORT}} -u root \\
          --batch --silent --connect-timeout=10 \\
          -e "SELECT COUNT(*) FROM iceberg_catalog.{src['iceberg_database']}.{src['iceberg_table']}" 2>&1) || COUNT=0
        echo "Bronze {src['source_id']}.{src['iceberg_table']}: $COUNT rows"
        TOTAL=$((TOTAL + COUNT))
"""
            for src in ENABLED_SOURCES
        )).replace("{% endfor %}", ""),
        env=SR_ENV,
    )

    # =========================================================================
    # PER-SOURCE TASK GROUPS — Silver + Gold
    # =========================================================================
    prev_group = check_bronze

    for source in ENABLED_SOURCES:
        sid          = source["source_id"]
        silver_model = source.get("sr_silver_model", f"silver_{sid}")
        gold_model   = source.get("sr_gold_model")

        with TaskGroup(f"{sid}_layer") as source_tg:

            # Silver
            with TaskGroup("silver") as silver_tg:
                silver_run = BashOperator(
                    task_id="run",
                    bash_command=f"""
                    set -e
                    cd /opt/analytics/cdp/dbt_silver
                    dbt run --profiles-dir . --select {silver_model} --target dev
                    """,
                    env=SR_ENV,
                )
                silver_test = BashOperator(
                    task_id="test",
                    bash_command=f"""
                    set -e
                    cd /opt/analytics/cdp/dbt_silver
                    dbt test --profiles-dir . --select {silver_model} \
                        --exclude-resource-type unit_test --target dev
                    """,
                    env=SR_ENV,
                )
                silver_run >> silver_test

            # Gold (optional)
            if gold_model:
                with TaskGroup("gold") as gold_tg:
                    gold_run = BashOperator(
                        task_id="run",
                        bash_command=f"""
                        set -e
                        cd /opt/analytics/cdp/dbt_gold
                        dbt run --profiles-dir . --select {gold_model} --target dev
                        """,
                        env=SR_ENV,
                    )
                    gold_test = BashOperator(
                        task_id="test",
                        bash_command=f"""
                        set -e
                        cd /opt/analytics/cdp/dbt_gold
                        dbt test --profiles-dir . --select {gold_model} \
                            --exclude-resource-type unit_test --target dev
                        """,
                        env=SR_ENV,
                    )
                    gold_run >> gold_test

                silver_tg >> gold_tg

        prev_group >> source_tg
        prev_group = source_tg
