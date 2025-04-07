# dags/spacex/tasks.py
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

from spacex.utils.spacex_api import load_to_postgres_pipeline
from spacex.utils.storage_loader import load_to_postgres
from spacex.config import DBT_PATH

def get_extract_and_load_task():
    return PythonOperator(
        task_id="extract_and_load_to_gcs",
        python_callable=load_to_postgres_pipeline,
    )


def get_load_to_postgres_task():
    return PythonOperator(
        task_id="load_to_bigquery",
        python_callable=load_to_postgres,
    )


def get_dbt_run_task():
    return BashOperator(
        task_id="run_dbt_models",
        bash_command=f"""
            cd {DBT_PATH} &&
            dbt deps &&
            dbt run
        """,
    )