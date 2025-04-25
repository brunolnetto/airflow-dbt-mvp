# tasks.py
import logging
from re import sub

from airflow.operators.bash import BashOperator
from airflow.decorators import task, task_group

from core.config import DBT_PATH
from core.storage import MinIOStorage, MinIOConfig

from projects.spacex.pipeline import SpaceXPipeline

def get_entity_pipeline_task(entity: str):
    """
    Wrapper to run the SpaceX pipeline for a given entity (e.g., 'launches').
    Used as an Airflow TaskFlow function.
    """
    @task(task_id=f"run_pipeline_{entity}")
    def run_entity_pipeline_task() -> None:
        pipeline = SpaceXPipeline(entity, MinIOStorage(config=MinIOConfig()))
        pipeline.run()

    return run_entity_pipeline_task

def get_ingestion_pipeline_group(pipeline_name: str, entities: list):
    @task_group(group_id=pipeline_name)
    def ingestion_group():
        for entity in entities:
            get_entity_pipeline_task(entity)()  # <--- call the returned @task function
    return ingestion_group()

def log_dbt_failure(context):
    """
    Airflow failure callback to log DBT command errors.
    """
    task_instance = context['task_instance']
    logging.error(
        f"[DBT ERROR] Task '{task_instance.task_id}' failed.\n"
        f"Command: {task_instance.bash_command}\n"
        f"Exception: {context['exception']}"
    )

def get_dbt_operator(command: str, task_type: str, target_env: str = "dev", dbt_select: str = None):
    """
    Generic BashOperator for DBT commands.
    """
    VALID_DBT_TASKS = {"run", "test", "seed", "snapshot"}

    if task_type not in VALID_DBT_TASKS:
        raise ValueError(f"Invalid DBT task_type: '{task_type}'. Must be one of {VALID_DBT_TASKS}")

    clean_select = sub(r'[^a-zA-Z0-9_]', '_', dbt_select) if dbt_select else "all"
    full_command = f"cd {DBT_PATH} && {command} --target {target_env}"
    if dbt_select:
        full_command += f" --select {dbt_select}"

    return BashOperator(
        task_id=f"dbt_{task_type}_{clean_select}",
        bash_command=full_command,
        on_failure_callback=log_dbt_failure,
    )


def get_dbt_run_task(target_env: str = "dev", dbt_select: str = None):
    """
    Creates a BashOperator to run DBT models with optional selector.
    Includes debug and deps before run.
    """
    base_command = "dbt debug && dbt deps && dbt run --no-write-json"
    return get_dbt_operator(base_command, "run", target_env, dbt_select)


def get_dbt_test_task(target_env: str = "dev", dbt_select: str = None):
    """
    Creates a BashOperator to run DBT tests with optional selector.
    """
    return get_dbt_operator("dbt test", "test", target_env, dbt_select)

def get_transform_pipeline_group(pipeline_name: str, target_env: str, project_name: str) -> None:
    @task_group(group_id=pipeline_name)
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