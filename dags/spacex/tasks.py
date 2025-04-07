# dags/spacex/tasks.py

from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

from dags.spacex.pipelines.spacex_etl import run_spacex_pipeline
from spacex.config import DBT_PATH


def get_extract_and_load_task():
    """
    Extract and load data from SpaceX API to PostgreSQL.
    """
    return PythonOperator(
        task_id="extract_and_load_to_postgres",
        python_callable=run_spacex_pipeline,
    )


def get_dbt_run_task():
    """
    Run dbt models.
    """
    return BashOperator(
        task_id="run_dbt_models",
        bash_command=f"""
            cd {DBT_PATH} &&
            dbt deps &&
            dbt run
        """,
    )
