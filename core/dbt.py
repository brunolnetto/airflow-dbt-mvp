from re import sub
from hashlib import md5

from airflow.operators.bash import BashOperator

from core.config import DBT_PATH, VALID_DBT_TASKS, logging

def generate_task_id(task_type: str, dbt_select: str) -> str:
    if dbt_select and len(dbt_select) > 30:
        hashed = md5(dbt_select.encode()).hexdigest()[:8]
        return f"dbt_{task_type}_{hashed}"
    clean_select = sub(r'[^a-zA-Z0-9_]', '_', dbt_select) if dbt_select else "all"
    return f"dbt_{task_type}_{clean_select}"


def log_dbt_failure(context) -> None:
    """
    Airflow failure callback to log DBT command errors.
    """
    task_instance = context['task_instance']
    logging.error(
        f"[DBT ERROR] Task '{task_instance.task_id}' failed.\n"
        f"Command: {task_instance.bash_command}\n"
        f"Exception: {context['exception']}"
    )

def get_dbt_operator(
    command: str, task_type: str, target_env: str = "dev", dbt_select: str = None
) -> BashOperator:
    """
    Generic BashOperator for DBT commands.
    """

    if task_type not in VALID_DBT_TASKS:
        raise ValueError(f"Invalid DBT task_type: '{task_type}'. Must be one of {VALID_DBT_TASKS}")

    clean_select = sub(r'[^a-zA-Z0-9_]', '_', dbt_select) if dbt_select else "all"
    full_command = f"cd {DBT_PATH} && {command} --target {target_env}"

    if dbt_select:
        full_command += f" --select {dbt_select}"

    task_id = generate_task_id(task_type, dbt_select)

    return BashOperator(
        task_id=task_id,
        bash_command=full_command,
        on_failure_callback=log_dbt_failure,
    )

def get_dbt_run_task(target_env: str = "dev", dbt_select: str = None) -> BashOperator:
    """
    Creates a BashOperator to run DBT models with optional selector.
    Includes debug and deps before run.
    """
    base_command = "dbt debug && dbt deps && dbt run --no-write-json"
    return get_dbt_operator(base_command, "run", target_env, dbt_select)


def get_dbt_test_task(target_env: str = "dev", dbt_select: str = None) -> BashOperator:
    """
    Creates a BashOperator to run DBT tests with optional selector.
    """
    return get_dbt_operator("dbt test", "test", target_env, dbt_select)

