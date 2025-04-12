from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.log.logging_mixin import LoggingMixin

from spacex.pipelines.spacex_etl import run_spacex_pipeline
from spacex.config import DBT_PATH

logger = LoggingMixin().log

def get_extract_and_load_task(entity: str):
    """
    Create a task to extract and load a specific entity from SpaceX API to PostgreSQL.
    """
    return PythonOperator(
        task_id=f"extract_and_load_{entity}",
        python_callable=run_spacex_pipeline,
        op_args=[entity],  # Pass entity as argument
    )

from airflow.operators.bash import BashOperator
from airflow.utils.log.logging_mixin import LoggingMixin
from spacex.config import DBT_PATH

logger = LoggingMixin().log

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

