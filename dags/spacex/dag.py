from airflow import DAG
from spacex.config import DEFAULT_ARGS
from spacex.tasks import (
    get_extract_and_load_task,
    get_dbt_run_task
)

ENTITIES = sorted(set([
    "capsules", 
    "cores", 
    "dragons",  
    "history",
    "landpads",
    "launches",
    "payloads",
    "rockets",
    "roadster",
    "ships"
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

    run_dbt = get_dbt_run_task()

    extract_and_load_tasks >> run_dbt
