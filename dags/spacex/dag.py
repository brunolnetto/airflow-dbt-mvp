from airflow import DAG
from spacex.config import DEFAULT_ARGS
from spacex.tasks import (
    get_pipeline_task,
    get_dbt_run_task,
    get_dbt_test_task
)
from os import environ
from dotenv import load_dotenv

# Load environment variables
load_dotenv()
target_env = environ.get("ENVIRONMENT", "dev")
assert target_env in {"dev", "prod"}, f"Invalid target_env: {target_env}"

# Define SpaceX data entities
ENTITIES = sorted({
    "history", "launches", "capsules", "cores", "dragons",
    "payloads", "rockets", "ships", "landpads"
})

with DAG(
    dag_id="spacex_etl_dag",
    default_args=DEFAULT_ARGS,
    description="🚀 SpaceX ETL pipeline: API -> Postgres -> DBT",
    schedule_interval="@hourly",
    catchup=False,
    tags=["spacex", "etl", "dbt"]
) as dag:

    # Call the returned function to get task instances
    pipeline_tasks = [get_pipeline_task(entity)() for entity in ENTITIES]

    dbt_staging = get_dbt_run_task(target_env=target_env, dbt_select="staging")
    dbt_mart = get_dbt_run_task(target_env=target_env, dbt_select="mart")
    dbt_test = get_dbt_test_task(target_env=target_env)

    pipeline_tasks >> dbt_staging >> dbt_mart >> dbt_test
