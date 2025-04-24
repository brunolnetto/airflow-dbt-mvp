import os
from pathlib import Path
from datetime import datetime, timedelta
from dotenv import load_dotenv
from typing import Dict
from functools import lru_cache
import logging

# Function to fetch environment variables or raise an error if missing
def get_env_or_fail(var: str, fallback=None) -> str:
    """Fetch the environment variable or raise an error if missing."""
    value = os.getenv(var, fallback)
    if value is None:
        raise RuntimeError(f"❌ Missing required env var: {var}")
    return value

PROJECT_DIR = os.getenv("AIRFLOW_HOME", "/opt/airflow")
DBT_PATH_DEFAULT = os.path.join(PROJECT_DIR, "dbt")
DBT_PATH = os.getenv("DBT_PATH", DBT_PATH_DEFAULT)

DEFAULT_ARGS = {
    "owner": "airflow",
    "start_date": datetime(2025, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

BASE_DIR = Path(__file__).resolve().parent
RAW_DIR = BASE_DIR / "data" / "raw"
PROCESSED_DIR = BASE_DIR / "data" / "processed"

# Constants for database and file paths
DATABASE_NAME = get_env_or_fail("POSTGRES_DB", "spacex_db")
RAW_SCHEMA = "raw"

# Load environment variables from .env file
load_dotenv()

# Logging configuration
LOGGING_FORMAT="%(asctime)s - %(levelname)s - %(message)s"
logging.basicConfig(level=logging.INFO, format=LOGGING_FORMAT)

ENVIRONMENT = os.getenv("ENVIRONMENT", "dev")
DEBUG = ENVIRONMENT == "dev"

if DEBUG:
    logging.getLogger().setLevel(logging.DEBUG)

@lru_cache()
def load_postgres_conn_params() -> Dict[str, str]:
    """Load and return the PostgreSQL connection parameters."""
    return {
        "host": get_env_or_fail("POSTGRES_HOST"),
        "port": get_env_or_fail("POSTGRES_PORT", 5432),
        "user": get_env_or_fail("POSTGRES_USER"),
        "password": get_env_or_fail("POSTGRES_PASSWORD"),
        "dbname": DATABASE_NAME,
    }

