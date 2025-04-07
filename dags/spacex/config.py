# dags/spacex/config.py
from datetime import datetime, timedelta
import os

PROJECT_DIR = os.getenv("AIRFLOW_HOME", "/opt/airflow")
DBT_PATH = os.path.join(PROJECT_DIR, "dbt")

DEFAULT_ARGS = {
    "owner": "airflow",
    "start_date": datetime(2025, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}