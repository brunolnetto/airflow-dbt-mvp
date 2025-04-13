from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.decorators import task

from spacex.pipelines.spacex_etl import (
    extract_spacex_data, 
    transform_spacex_data, 
    load_spacex_data,   
)
from spacex.config import DBT_PATH


logger = LoggingMixin().log

@task()
def extract_task(entity: str) -> str:
    extract_spacex_data(entity)
    return entity

@task()
def transform_task(entity: str) -> str:
    df = transform_spacex_data(entity)
    if df.empty:
        raise ValueError(f"Transformed data for entity '{entity}' is empty.")
    return entity

@task()
def load_task(entity: str) -> None:
    load_spacex_data(entity)

def get_extract_and_load_task(entity: str):
    extracted = extract_task(entity)
    transformed = transform_task(extracted)
    return load_task(transformed)

def get_dbt_run_task(target_env="dev", dbt_select=None):
    dbt_command = f"cd {DBT_PATH} && dbt deps && dbt run --no-write-json --target {target_env}"
    if dbt_select:
        dbt_command += f" --select {dbt_select}"

    return BashOperator(
        task_id=f"dbt_run_{dbt_select or 'all'}",
        bash_command=dbt_command,
    )

def get_dbt_test_task(target_env="dev"):
    dbt_test_command = f"cd {DBT_PATH} && dbt test --target {target_env}"
    return BashOperator(
        task_id="dbt_test",
        bash_command=dbt_test_command,
    )

