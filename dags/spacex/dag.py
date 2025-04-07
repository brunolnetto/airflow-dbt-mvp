# dags/spacex/dag.py
from airflow import DAG
from spacex.config import DEFAULT_ARGS
from spacex.tasks import (
    get_extract_and_load_task,
    get_dbt_run_task
)

with DAG(
    dag_id="spacex_etl_dag",
    default_args=DEFAULT_ARGS,
    description="🚀 SpaceX ETL pipeline: API -> GCS -> BQ -> dbt",
    schedule_interval="@daily",
    catchup=False,
    tags=["spacex", "etl", "dbt"]
) as dag:

    extract_and_load = get_extract_and_load_task()
    run_dbt = get_dbt_run_task()

    extract_and_load >> run_dbt
