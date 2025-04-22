# tasks.py
import logging
from airflow.operators.bash import BashOperator
from airflow.decorators import task
from .config import DBT_PATH
from .core.pipeline import SpaceXPipeline
from .core.storage import MinIOStorage, MinIOConfig


def get_pipeline_task(entity: str):
    """
    Wrapper to run the SpaceX pipeline for a given entity (e.g., 'launches').
    Used as an Airflow TaskFlow function.
    """
    @task(task_id=f"run_pipeline_{entity}")
    def run_pipeline_task() -> None:
        pipeline = SpaceXPipeline(entity, MinIOStorage(config=MinIOConfig()))
        pipeline.run()

    return run_pipeline_task


def log_dbt_failure(context):
    """
    Airflow failure callback to log DBT command errors.
    """
    task_instance = context['task_instance']
    logging.error(
        f"[DBT ERROR] Task '{task_instance.task_id}' failed.\nException: {context['exception']}"
    )


def get_dbt_run_task(target_env: str = "dev", dbt_select: str = None):
    """
    Creates a BashOperator to run DBT models for a given environment and optional model selector.
    """
    dbt_command = (
        f"cd {DBT_PATH} && "
        f"dbt debug && "
        f"dbt deps && "
        f"dbt run --no-write-json --target {target_env}"
    )
    
    # Adiciona seletor se especificado (ex: staging.spacex)
    if dbt_select:
        dbt_command += f" --select {dbt_select}"

    return BashOperator(
        task_id=f"dbt_run_{dbt_select.replace('.', '_') if dbt_select else 'all'}",
        bash_command=dbt_command,
        on_failure_callback=log_dbt_failure,
    )


def get_dbt_test_task(target_env: str = "dev", dbt_select: str = None):
    """
    Creates a BashOperator to run DBT tests for a given environment, with optional selector.
    """
    dbt_test_command = f"cd {DBT_PATH} && dbt test --target {target_env}"
    if dbt_select:
        dbt_test_command += f" --select {dbt_select}"

    return BashOperator(
        task_id=f"dbt_test_{dbt_select.replace('.', '_') if dbt_select else 'all'}",
        bash_command=dbt_test_command,
        on_failure_callback=log_dbt_failure,
    )
