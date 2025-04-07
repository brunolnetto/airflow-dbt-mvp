import requests
import logging
from datetime import datetime
from typing import List

import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

# ================== Config ==================
TABLE_NAME = "spacex_launches"

# ================== Logging ==================
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# ================ Extract ===================
def extract_from_spacex() -> List[dict]:
    url = "https://api.spacexdata.com/v4/launches"
    try:
        response = requests.get(url)
        response.raise_for_status()
        logging.info("✅ Successfully fetched launch data from SpaceX API")
        return response.json()
    except requests.exceptions.RequestException as e:
        logging.error(f"❌ Failed to fetch data: {e}")
        return []

# ================ Transform ===================
def transform_launch_data(raw_data: List[dict]) -> pd.DataFrame:
    logging.info("🔧 Transforming raw data into DataFrame...")

    df = pd.DataFrame([{
        "id": launch.get("id"),
        "name": launch.get("name"),
        "date_utc": launch.get("date_utc"),
        "success": launch.get("success"),
        "rocket": launch.get("rocket"),
        "details": launch.get("details"),
        "flight_number": launch.get("flight_number")
    } for launch in raw_data])

    df["date_utc"] = pd.to_datetime(df["date_utc"], errors="coerce")
    logging.info(f"📊 Transformed {len(df)} rows.")
    return df

# ================== Load ===================
def upload_to_postgres(df: pd.DataFrame, conn_params: dict, table_name: str = TABLE_NAME) -> None:
    logging.info(f"🐘 Uploading {len(df)} rows to PostgreSQL...")

    create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            id TEXT PRIMARY KEY,
            name TEXT,
            date_utc TIMESTAMP,
            success BOOLEAN,
            rocket TEXT,
            details TEXT,
            flight_number INTEGER
        )
    """

    insert_sql = f"""
        INSERT INTO {table_name} (
            id, name, date_utc, success, rocket, details, flight_number
        )
        VALUES %s
        ON CONFLICT (id) DO NOTHING
    """

    with psycopg2.connect(**conn_params) as conn:
        with conn.cursor() as cur:
            cur.execute(create_table_sql)
            values = df.where(pd.notnull(df), None).values.tolist()
            execute_values(cur, insert_sql, values)
        conn.commit()

    logging.info("✅ Data uploaded to PostgreSQL.")

# ================ ETL Pipeline ===================
def load_to_postgres_pipeline(conn_params: dict) -> None:
    start_time = datetime.now()
    logging.info("🚀 Starting SpaceX ETL process...")

    raw_data = extract_from_spacex()
    if not raw_data:
        logging.warning("⚠️ No data fetched from SpaceX API.")
        return

    df = transform_launch_data(raw_data)
    if df.empty:
        logging.warning("⚠️ Transformed DataFrame is empty.")
        return

    upload_to_postgres(df, conn_params)

    duration = (datetime.now() - start_time).total_seconds()
    logging.info(f"✅ ETL pipeline finished in {duration:.2f} seconds.")

def get_env_or_fail(var: str, fallback=None):
    value = os.getenv(var, fallback)
    if value is None:
        raise RuntimeError(f"Missing required env var: {var}")
    return value

import os

# ================ Run Locally ===================
if __name__ == "__main__":
    try:
        conn_params = {
            "host": get_env_or_fail("POSTGRES_HOST"),
            "port": get_env_or_fail("POSTGRES_USER", 5432),
            "user": get_env_or_fail("POSTGRES_USER"),
            "password": get_env_or_fail("POSTGRES_PASSWORD"),
            "dbname": get_env_or_fail("POSTGRES_DB"),
        }

    except KeyError as e:
        missing = e.args[0]
        raise RuntimeError(f"🚨 Missing required environment variable: {missing}")

    load_to_postgres_pipeline(conn_params)

