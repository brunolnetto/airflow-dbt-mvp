# dags/spacex/tasks.py

from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.log.logging_mixin import LoggingMixin

from spacex.pipelines.spacex_etl import run_spacex_pipeline
from spacex.config import DBT_PATH


def get_extract_and_load_task():
    """
    Extract and load data from SpaceX API to PostgreSQL.
    """
    return PythonOperator(
        task_id="extract_and_load_to_postgres",
        python_callable=run_spacex_pipeline,
    )

logger = LoggingMixin().log
def get_dbt_run_task():
    dbt_command = f"""
    set -e
    echo "Running dbt in {DBT_PATH}"
    cd {DBT_PATH}
    which dbt
    dbt --version
    dbt deps
    dbt run --no-write-json
    """
    logger.info(f"DBT Bash command:\n{dbt_command}")
    
    return BashOperator(
        task_id="run_dbt_models",
        bash_command=dbt_command,
    )
