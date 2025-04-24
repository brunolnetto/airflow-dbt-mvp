import os
from pathlib import Path
from dotenv import load_dotenv
from typing import Dict
from functools import lru_cache

# Load environment variables from .env file
load_dotenv()

# Function to fetch environment variables or raise an error if missing
def get_env_or_fail(var: str, fallback=None) -> str:
    """Fetch the environment variable or raise an error if missing."""
    value = os.getenv(var, fallback)
    if value is None:
        raise RuntimeError(f"❌ Missing required env var: {var}")
    return value

# Constants for database and file paths
DATABASE_NAME = get_env_or_fail("POSTGRES_DB", "spacex_db")
RAW_SCHEMA = "raw"

BASE_DIR = Path(__file__).resolve().parent
RAW_DIR = BASE_DIR / "data" / "raw"
PROCESSED_DIR = BASE_DIR / "data" / "processed"

@lru_cache()
def load_conn_params() -> Dict[str, str]:
    """Load and return the PostgreSQL connection parameters."""
    return {
        "host": get_env_or_fail("POSTGRES_HOST"),
        "port": get_env_or_fail("POSTGRES_PORT", 5432),
        "user": get_env_or_fail("POSTGRES_USER"),
        "password": get_env_or_fail("POSTGRES_PASSWORD"),
        "dbname": DATABASE_NAME,
    }


