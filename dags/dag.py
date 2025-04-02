# dags/dag.py

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import sys

# === DAG Config ===
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    dag_id='spacex_dag',
    default_args=default_args,
    description='End-to-end SpaceX ETL pipeline using Python + dbt',
    schedule_interval='@daily',
    catchup=False,
)

# === Path Setup ===
PROJECT_DIR = '/opt/airflow'  # this is the root of your project in the container
DBT_PATH = f"{PROJECT_DIR}/dbt"

# Insert the project root so that 'api' is found as a package under /opt/airflow/api
import sys
sys.path.insert(0, PROJECT_DIR)

# === Now import your local python modules ===
from api import bq_loader, spacex_api

# === Task 1: Extract + Load to GCS ===
extract_and_load_gcs = PythonOperator(
    task_id='extract_and_load_to_gcs',
    python_callable=spacex_api.load_to_gcs_pipeline if hasattr(spacex_api, 'load_to_gcs_pipeline') else spacex_api.load_to_bigquery,  # fallback if api.py has only one function
    dag=dag,
)

# === Task 2: Load from GCS to BigQuery ===
load_to_bq = PythonOperator(
    task_id='load_to_bigquery',
    python_callable=bq_loader.load_to_bigquery,
    dag=dag,
)

# === Task 3: Run dbt ===
dbt_run = BashOperator(
    task_id='run_dbt_models',
    bash_command=f"""
        cd {DBT_PATH} &&
        dbt deps &&
        dbt run
    """,
    dag=dag,
)

# === DAG Flow ===
extract_and_load_gcs >> load_to_bq >> dbt_run
