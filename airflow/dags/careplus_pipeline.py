from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'careplus',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='careplus_pipeline',
    default_args=default_args,
    description='CarePlus data pipeline — tickets + logs → Snowflake → dbt',
    schedule_interval='0 2 * * *',  # runs every day at 2AM
    start_date=datetime(2026, 2, 28),
    catchup=False,
    tags=['careplus']
) as dag:

    # Task 1 — Extract MySQL tickets → upload to S3 → load into bronze.tickets
    load_tickets = BashOperator(
        task_id='load_tickets_to_snowflake',
        bash_command='python /opt/airflow/ingestion/load_tickets_to_snowflake.py'
    )

    # Task 2 — Parse .log files from S3 → load into bronze.logs
    load_logs = BashOperator(
        task_id='load_logs_to_snowflake',
        bash_command='python /opt/airflow/ingestion/load_logs_to_snowflake.py'
    )

    # Task 3 — Run dbt silver models
    dbt_silver = BashOperator(
        task_id='dbt_run_silver',
        bash_command='dbt run --select silver --project-dir /opt/airflow/dbt/careplus_dbt --profiles-dir /opt/airflow/dbt/careplus_dbt'
    )

    # Task 4 — Run dbt gold models
    dbt_gold = BashOperator(
        task_id='dbt_run_gold',
        bash_command='dbt run --select gold --project-dir /opt/airflow/dbt/careplus_dbt --profiles-dir /opt/airflow/dbt/careplus_dbt'
    )

    # Task dependencies — defines the order
    [load_tickets, load_logs] >> dbt_silver >> dbt_gold


# The last line defines the execution order:
   # load_tickets --|
   #                |-> dbt_silver -> dbt_gold
   # load_logs    --|