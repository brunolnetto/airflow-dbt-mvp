from airflow.operators.bash import BashOperator
from airflow.decorators import task, task_group

from core.dbt import get_dbt_run_task, get_dbt_test_task

def get_transform_pipeline_group(project_name: str, target_env: str) -> None:
    """
    Build a task group for the DBT transformation pipeline.
    This function creates a task group with three tasks: dbt_staging, dbt_mart, and dbt_test.
    Each task is responsible for a specific step in the DBT transformation process.
    """
    @task_group(group_id=f"transform_pipeline_{project_name}")
    def dbt_group():
        dbt_staging = get_dbt_run_task(
            target_env=target_env, 
            dbt_select=f"staging.{project_name}"
        )
        dbt_mart = get_dbt_run_task(
            target_env=target_env, 
            dbt_select=f"mart.{project_name}"
        )
        dbt_test = get_dbt_test_task(target_env=target_env)

        dbt_staging >> dbt_mart >> dbt_test

    return dbt_group()