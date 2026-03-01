# =============================================================================
# AIRFLOW DAG: Dropshipping Analytics Pipeline
# =============================================================================
# DAG này orchestrates data pipeline từ ClickHouse đến StarRocks.
#
# AIRFLOW LÀ GÌ?
# - Workflow orchestration platform
# - Định nghĩa pipelines bằng Python code
# - Scheduling, monitoring, alerting
# - Dependency management giữa tasks
#
# DAG LÀ GÌ?
# - Directed Acyclic Graph
# - Tập hợp tasks với dependencies
# - Không có cycles (task A -> B -> C, không có C -> A)
#
# CONCEPTS QUAN TRỌNG:
#
# 1. Operators:
#    - PythonOperator: Chạy Python function
#    - BashOperator: Chạy bash command
#    - Sensors: Wait for condition
#    - Custom operators
#
# 2. Dependencies:
#    - task1 >> task2: task2 chạy sau task1
#    - [task1, task2] >> task3: task3 chạy sau cả task1 và task2
#
# 3. XCom:
#    - Cross-communication giữa tasks
#    - Pass data từ task này sang task khác
#
# 4. Variables & Connections:
#    - Variables: Config values
#    - Connections: Database/API connections
#
# PIPELINE FLOW:
# 1. Check data freshness (ClickHouse)
# 2. Run dbt models
# 3. Export to StarRocks
# 4. Refresh Superset datasets
# 5. Send notifications
# =============================================================================

from datetime import datetime, timedelta
from typing import Any

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.sensors.python import PythonSensor
from airflow.utils.task_group import TaskGroup
from airflow.models import Variable

# =============================================================================
# DEFAULT ARGUMENTS
# =============================================================================
# Arguments mặc định cho tất cả tasks trong DAG.
# Có thể override ở từng task.
# =============================================================================

default_args = {
    # Owner của DAG (hiển thị trong UI)
    'owner': 'data-engineering',

    # Email notifications
    'email': ['data-team@example.com'],
    'email_on_failure': True,  # Gửi email khi task fail
    'email_on_retry': False,   # Không gửi email khi retry

    # Retry settings
    # retries: Số lần retry khi fail
    # retry_delay: Thời gian chờ giữa các retries
    # retry_exponential_backoff: Tăng delay theo exponential
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'retry_exponential_backoff': True,
    'max_retry_delay': timedelta(minutes=30),

    # Execution settings
    # depends_on_past: Task chỉ chạy nếu previous run success
    # wait_for_downstream: Wait cho downstream tasks của previous run
    'depends_on_past': False,
    'wait_for_downstream': False,

    # Start date: DAG bắt đầu chạy từ ngày này
    # Nếu start_date là quá khứ và catchup=True, Airflow sẽ backfill
    'start_date': datetime(2024, 1, 1),

    # Execution timeout
    'execution_timeout': timedelta(hours=2),
}

# =============================================================================
# DAG DEFINITION
# =============================================================================

with DAG(
    # DAG ID - unique identifier
    dag_id='dropshipping_analytics',

    # Description hiển thị trong UI
    description='ETL pipeline for dropshipping analytics: ClickHouse -> dbt -> StarRocks',

    # Default arguments (defined above)
    default_args=default_args,

    # Schedule
    # @daily: Chạy mỗi ngày lúc 00:00 UTC
    # @hourly: Chạy mỗi giờ
    # Cron: '0 6 * * *' = 6:00 AM daily
    # None: Manual trigger only
    schedule_interval='@daily',

    # Catchup: Backfill các runs bị miss
    # False = Không backfill, chỉ chạy từ bây giờ
    catchup=False,

    # Tags để categorize DAGs
    tags=['analytics', 'dropshipping', 'daily'],

    # Doc strings
    doc_md="""
    # Dropshipping Analytics Pipeline

    This DAG runs daily to:
    1. Check data freshness in ClickHouse
    2. Run dbt transformations
    3. Export aggregated data to StarRocks
    4. Refresh Superset datasets

    ## Dependencies
    - ClickHouse: Raw events storage
    - dbt: Transformation layer
    - StarRocks: Analytics database
    - Superset: Visualization

    ## Owner
    Data Engineering Team
    """,

    # Max active runs: Số DAG runs có thể chạy đồng thời
    max_active_runs=1,

    # Render templates as native Python objects
    render_template_as_native_obj=True,

) as dag:

    # =========================================================================
    # TASK: START
    # =========================================================================
    # Empty operator đánh dấu điểm bắt đầu
    # Hữu ích cho visualization và debugging
    # =========================================================================
    start = EmptyOperator(
        task_id='start',
        doc='Pipeline start marker'
    )

    # =========================================================================
    # TASK: CHECK DATA FRESHNESS
    # =========================================================================
    # Sensor wait cho đến khi ClickHouse có data mới.
    # Tránh chạy pipeline khi không có data.
    # =========================================================================
    def check_clickhouse_freshness(**context) -> bool:
        """
        Check if ClickHouse has recent data.

        Returns True if data from last 2 hours exists.
        Sensor will keep checking until True.
        """
        # In production, use ClickHouse client
        # from clickhouse_driver import Client
        # client = Client(host='clickhouse', port=9000)

        # Query to check recent data
        query = """
        SELECT count(*)
        FROM tracking.events
        WHERE processed_time >= now() - INTERVAL 2 HOUR
        """

        # For demo, always return True
        # In production:
        # result = client.execute(query)[0][0]
        # return result > 0
        return True

    check_data_freshness = PythonSensor(
        task_id='check_data_freshness',
        python_callable=check_clickhouse_freshness,
        # Timeout: Dừng check sau 1 giờ
        timeout=3600,
        # Poke interval: Check mỗi 5 phút
        poke_interval=300,
        # Mode:
        # - poke: Giữ worker slot, check liên tục
        # - reschedule: Release worker, schedule lại sau poke_interval
        mode='reschedule',
        doc='Wait for fresh data in ClickHouse',
    )

    # =========================================================================
    # TASK GROUP: DBT TRANSFORMS
    # =========================================================================
    # Task group nhóm các dbt tasks lại với nhau.
    # Giúp UI gọn gàng hơn.
    # =========================================================================
    with TaskGroup(
        group_id='dbt_transforms',
        tooltip='Run dbt models to transform raw data'
    ) as dbt_group:

        # ---------------------------------------------------------------------
        # DBT: Install packages
        # ---------------------------------------------------------------------
        dbt_deps = BashOperator(
            task_id='dbt_deps',
            bash_command="""
                cd /opt/dbt && \
                dbt deps --profiles-dir . --project-dir .
            """,
            doc='Install dbt packages (dbt_utils, etc.)',
        )

        # ---------------------------------------------------------------------
        # DBT: Run staging models
        # ---------------------------------------------------------------------
        dbt_run_staging = BashOperator(
            task_id='dbt_run_staging',
            bash_command="""
                cd /opt/dbt && \
                dbt run \
                    --profiles-dir . \
                    --project-dir . \
                    --select staging \
                    --vars '{"execution_date": "{{ ds }}"}'
            """,
            doc='Run dbt staging models (stg_events)',
        )

        # ---------------------------------------------------------------------
        # DBT: Run marts models
        # ---------------------------------------------------------------------
        dbt_run_marts = BashOperator(
            task_id='dbt_run_marts',
            bash_command="""
                cd /opt/dbt && \
                dbt run \
                    --profiles-dir . \
                    --project-dir . \
                    --select marts \
                    --vars '{"execution_date": "{{ ds }}"}'
            """,
            doc='Run dbt marts models (fct_daily_metrics, etc.)',
        )

        # ---------------------------------------------------------------------
        # DBT: Run tests
        # ---------------------------------------------------------------------
        dbt_test = BashOperator(
            task_id='dbt_test',
            bash_command="""
                cd /opt/dbt && \
                dbt test \
                    --profiles-dir . \
                    --project-dir . \
                    --select marts
            """,
            doc='Run dbt tests on marts models',
            # Cho phép fail mà không dừng pipeline
            # trigger_rule='all_done',
        )

        # ---------------------------------------------------------------------
        # DBT: Generate docs
        # ---------------------------------------------------------------------
        dbt_docs = BashOperator(
            task_id='dbt_docs',
            bash_command="""
                cd /opt/dbt && \
                dbt docs generate \
                    --profiles-dir . \
                    --project-dir .
            """,
            doc='Generate dbt documentation',
        )

        # Dependencies trong task group
        dbt_deps >> dbt_run_staging >> dbt_run_marts >> [dbt_test, dbt_docs]

    # =========================================================================
    # TASK GROUP: EXPORT TO STARROCKS
    # =========================================================================
    with TaskGroup(
        group_id='export_to_starrocks',
        tooltip='Export aggregated data to StarRocks for analytics'
    ) as starrocks_group:

        def export_table_to_starrocks(
            source_table: str,
            target_table: str,
            **context
        ):
            """
            Export table from ClickHouse to StarRocks.

            In production, this would:
            1. Read data from ClickHouse
            2. Transform if needed
            3. Insert into StarRocks

            StarRocks supports multiple insert methods:
            - MySQL protocol (INSERT INTO)
            - Stream Load (HTTP API, for bulk)
            - Routine Load (Kafka, for streaming)
            - Broker Load (from S3, HDFS)
            """
            execution_date = context['ds']

            print(f"Exporting {source_table} to StarRocks {target_table}")
            print(f"Execution date: {execution_date}")

            # Example StarRocks Stream Load command:
            # curl -XPUT \
            #     -H "Expect: 100-continue" \
            #     -H "label:load_$(date +%s)" \
            #     -H "column_separator:," \
            #     -T data.csv \
            #     http://starrocks-fe:8030/api/analytics/{target_table}/_stream_load

            # For demo, just log
            print(f"Exported {source_table} successfully")

        # Export daily metrics
        export_daily_metrics = PythonOperator(
            task_id='export_daily_metrics',
            python_callable=export_table_to_starrocks,
            op_kwargs={
                'source_table': 'tracking.fct_daily_metrics',
                'target_table': 'analytics.fct_daily_metrics',
            },
        )

        # Export product performance
        export_product_performance = PythonOperator(
            task_id='export_product_performance',
            python_callable=export_table_to_starrocks,
            op_kwargs={
                'source_table': 'tracking.fct_product_performance',
                'target_table': 'analytics.fct_product_performance',
            },
        )

        # Export marketing attribution
        export_marketing_attribution = PythonOperator(
            task_id='export_marketing_attribution',
            python_callable=export_table_to_starrocks,
            op_kwargs={
                'source_table': 'tracking.fct_marketing_attribution',
                'target_table': 'analytics.fct_marketing_attribution',
            },
        )

        # Export parallel
        [export_daily_metrics, export_product_performance, export_marketing_attribution]

    # =========================================================================
    # TASK: REFRESH SUPERSET
    # =========================================================================
    def refresh_superset_datasets(**context):
        """
        Refresh Superset datasets to pick up new data.

        Superset caches schema và data.
        Cần refresh sau khi data thay đổi.

        API endpoint: POST /api/v1/dataset/{id}/refresh
        """
        print("Refreshing Superset datasets...")

        # In production:
        # import requests
        # superset_url = Variable.get('superset_url')
        # api_token = Variable.get('superset_api_token')
        #
        # datasets = [1, 2, 3]  # Dataset IDs
        # for dataset_id in datasets:
        #     response = requests.post(
        #         f"{superset_url}/api/v1/dataset/{dataset_id}/refresh",
        #         headers={"Authorization": f"Bearer {api_token}"}
        #     )
        #     print(f"Refreshed dataset {dataset_id}: {response.status_code}")

        print("Superset datasets refreshed")

    refresh_superset = PythonOperator(
        task_id='refresh_superset',
        python_callable=refresh_superset_datasets,
        doc='Refresh Superset dataset cache',
    )

    # =========================================================================
    # TASK: SEND NOTIFICATION
    # =========================================================================
    def send_completion_notification(**context):
        """
        Send notification when pipeline completes.

        Can integrate with:
        - Slack
        - Email
        - PagerDuty
        - Webhooks
        """
        execution_date = context['ds']
        dag_id = context['dag'].dag_id

        message = f"""
        ✅ Pipeline completed successfully!

        DAG: {dag_id}
        Execution Date: {execution_date}

        Data is now available in:
        - StarRocks: analytics database
        - Superset: dashboards updated
        """

        print(message)

        # Slack example:
        # from airflow.providers.slack.hooks.slack import SlackHook
        # slack = SlackHook(slack_conn_id='slack_default')
        # slack.send(text=message, channel='#data-alerts')

    notify_success = PythonOperator(
        task_id='notify_success',
        python_callable=send_completion_notification,
        trigger_rule='all_success',  # Chỉ chạy nếu tất cả tasks thành công
    )

    # =========================================================================
    # TASK: END
    # =========================================================================
    end = EmptyOperator(
        task_id='end',
        trigger_rule='all_done',  # Chạy dù các tasks trước fail hay success
    )

    # =========================================================================
    # DEFINE DEPENDENCIES
    # =========================================================================
    # Sử dụng >> operator để định nghĩa thứ tự chạy
    # =========================================================================

    start >> check_data_freshness >> dbt_group >> starrocks_group >> refresh_superset >> notify_success >> end

# =============================================================================
# ADDITIONAL DAGS (Optional)
# =============================================================================
# Trong production, có thể tách thành nhiều DAGs:
#
# 1. hourly_raw_validation_dag:
#    - Chạy mỗi giờ
#    - Check data quality của raw events
#    - Alert nếu có anomalies
#
# 2. weekly_full_refresh_dag:
#    - Chạy weekly
#    - Full refresh tất cả tables
#    - Rebuild indexes
#
# 3. backfill_dag:
#    - Manual trigger
#    - Reprocess historical data
# =============================================================================
