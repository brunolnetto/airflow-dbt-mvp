# dags/spacex/config.py
from datetime import datetime, timedelta
import logging
import os

PROJECT_DIR = os.getenv("AIRFLOW_HOME", "/opt/airflow")
DBT_PATH_DEFAULT = os.path.join(PROJECT_DIR, "dbt")
DBT_PATH = os.getenv("DBT_PATH", DBT_PATH_DEFAULT)

DEFAULT_ARGS = {
    "owner": "airflow",
    "start_date": datetime(2025, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Logging configuration
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

ENVIRONMENT = os.getenv("ENVIRONMENT", "dev")
DEBUG = ENVIRONMENT == "dev"

if DEBUG:
    logging.getLogger().setLevel(logging.DEBUG)