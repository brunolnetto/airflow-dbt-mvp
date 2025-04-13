from airflow import DAG
from spacex.config import DEFAULT_ARGS
from spacex.tasks import (
    get_extract_and_load_task,
    get_dbt_run_task,
    get_dbt_test_task  
)

from os import environ
from dotenv import load_dotenv

load_dotenv()
target_env = environ.get("ENVIRONMENT", "dev")
assert target_env in {"dev", "prod"}, f"Invalid target_env: {target_env}"

ENTITIES = sorted(set([
    "history", "launches", "capsules", "cores", "dragons",  
    "payloads", "rockets", "ships", "landpads"
]))

with DAG(
    dag_id="spacex_etl_dag",
    default_args=DEFAULT_ARGS,
    description="🚀 SpaceX ETL pipeline: API -> postgres -> dbt",
    schedule_interval="@hourly",
    catchup=False,
    tags=["spacex", "etl", "dbt"]
) as dag:

    extract_and_load_tasks = [
        get_extract_and_load_task(entity)
        for entity in ENTITIES
    ]

    run_dbt_staging = get_dbt_run_task(
        target_env=target_env, 
        dbt_select="staging"
    )
    run_dbt_mart = get_dbt_run_task(
        target_env=target_env, 
        dbt_select="mart"
    )
    run_dbt_tests = get_dbt_test_task(target_env=target_env)

    extract_and_load_tasks >> run_dbt_staging >> run_dbt_mart >> run_dbt_tests
