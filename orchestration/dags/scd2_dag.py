from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
import datetime

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email": ["alerts@example.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": datetime.timedelta(minutes=5),
}

with DAG(
    "scd2_duckdb_pipeline",
    default_args=default_args,
    description="Run SCD2 logic using DuckDB on MinIO data",
    schedule_interval="@hourly",
    start_date=days_ago(1),
    catchup=False,
) as dag:

    # Step 1: Sync data from MinIO (using mc or aws-cli)
    sync_minio = BashOperator(
        task_id="sync_minio",
        bash_command="mc alias set localminio http://minio:9000 minioadmin minioadmin && mc cp --recursive localminio/my-cdc-bucket /opt/airflow/duckdb/raw_data"
    )

    # Step 2: Run DuckDB SCD2 SQL
    run_scd2 = BashOperator(
        task_id="run_scd2",
        bash_command="duckdb /opt/airflow/duckdb/scd2.duckdb < /opt/airflow/duckdb/scd2_example.sql"
    )

    # Step 3: Push processed results back to MinIO
    upload_processed = BashOperator(
        task_id="upload_processed",
        bash_command="mc cp --recursive /opt/airflow/duckdb/processed localminio/my-cdc-bucket/processed"
    )

    sync_minio >> run_scd2 >> upload_processed
