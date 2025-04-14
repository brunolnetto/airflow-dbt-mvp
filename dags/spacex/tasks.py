# tasks.py
import logging
from airflow.operators.bash import BashOperator
from airflow.decorators import task
from .config import DBT_PATH
from .core.pipeline import SpaceXPipeline
from .core.storage import MinIOStorage, MinIOConfig


def get_pipeline_task(entity: str):
    """Single-task wrapper around pipeline.run() for the given entity."""
    
    @task(task_id=f"run_pipeline_{entity}")
    def run_pipeline_task() -> None:
        pipeline = SpaceXPipeline(entity, MinIOStorage(config=MinIOConfig()))
        pipeline.run()

    return run_pipeline_task


def log_dbt_failure(context):
    """Callback to log DBT task failures."""
    task_instance = context['task_instance']
    logging.error(f"DBT Task Failed: {task_instance.task_id}, Reason: {context['exception']}")


def get_dbt_run_task(target_env="dev", dbt_select=None):
    """Create a DBT run task."""
    dbt_command = f"cd {DBT_PATH} && dbt deps && dbt run --no-write-json --target {target_env}"
    if dbt_select:
        dbt_command += f" --select {dbt_select}"

    dbt_run_task = BashOperator(
        task_id=f"dbt_run_{dbt_select or 'all'}",
        bash_command=dbt_command,
    )

    dbt_run_task.on_failure_callback = log_dbt_failure
    return dbt_run_task


def get_dbt_test_task(target_env="dev"):
    """Create a DBT test task."""
    dbt_test_command = f"cd {DBT_PATH} && dbt test --target {target_env}"
    return BashOperator(
        task_id="dbt_test",
        bash_command=dbt_test_command,
    )
