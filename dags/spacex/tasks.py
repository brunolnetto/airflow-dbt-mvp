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

def get_dbt_run_task():
    dbt_command = f"""
    set -e
    cd {DBT_PATH}
    dbt deps
    dbt run --no-write-json
    """
    logger.info(f"DBT Bash command:\n{dbt_command}")
    
    return BashOperator(
        task_id="run_dbt_models",
        bash_command=dbt_command,
    )
