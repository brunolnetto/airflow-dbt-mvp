import os
import requests
import logging
from datetime import datetime
from typing import List
from contextlib import closing

import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

# ================ Config =====================
DATABASE_NAME = "spacex_db"
TABLE_NAME = "spacex_launches"
RAW_SCHEMA = "raw"  # para compatibilidade com DBT
FULL_TABLE_NAME = f"{RAW_SCHEMA}.{TABLE_NAME}"

# ================ Environment =================
def get_env_or_fail(var: str, fallback=None):
    value = os.getenv(var, fallback)
    if value is None:
        raise RuntimeError(f"❌ Missing required env var: {var}")
    return value

def load_conn_params():
    from dotenv import load_dotenv
    load_dotenv()

    return {
        "host": get_env_or_fail("POSTGRES_HOST"),
        "port": get_env_or_fail("POSTGRES_PORT", 5432),
        "user": get_env_or_fail("POSTGRES_USER"),
        "password": get_env_or_fail("POSTGRES_PASSWORD"),
        "dbname": DATABASE_NAME,
    }

# ================ Logging ====================
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# ================ Extract ====================
def extract_from_spacex() -> List[dict]:
    url = "https://api.spacexdata.com/v4/launches"
    try:
        response = requests.get(url)
        response.raise_for_status()
        logging.info("✅ SpaceX API data fetched.")
        return response.json()
    except requests.exceptions.RequestException as e:
        logging.error(f"❌ API fetch failed: {e}")
        return []

# ================ Transform ==================
def transform_launch_data(raw_data: List[dict]) -> pd.DataFrame:
    logging.info("🔧 Transforming raw data...")
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

# ================ Load =======================
def ensure_database_exists(db_params: dict):
    dbname = db_params["dbname"]
    logging.info(f"🔍 Checking if database '{dbname}' exists...")

    check_conn_params = db_params.copy()
    check_conn_params["dbname"] = "postgres"

    # criar conexão fora do "with" e setar autocommit antes de usar
    conn = psycopg2.connect(**check_conn_params)
    conn.set_session(autocommit=True)

    try:
        with conn.cursor() as cur:
            cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (dbname,))
            exists = cur.fetchone()
            if not exists:
                logging.info(f"📦 Database '{dbname}' not found. Creating...")
                cur.execute(f'CREATE DATABASE "{dbname}"')
                logging.info(f"✅ Database '{dbname}' created.")
            else:
                logging.info(f"✔️ Database '{dbname}' already exists.")
    finally:
        conn.close()


def upload_to_postgres(df: pd.DataFrame, conn_params: dict) -> None:
    logging.info(f"⬆️ Uploading {len(df)} rows to PostgreSQL...")
    
    create_table_sql = f"""
        CREATE SCHEMA IF NOT EXISTS {RAW_SCHEMA};
        CREATE TABLE IF NOT EXISTS {FULL_TABLE_NAME} (
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
        INSERT INTO {FULL_TABLE_NAME} (
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

    logging.info("✅ Upload complete.")

# ================ Pipeline ===================
def run_spacex_pipeline() -> None:
    start = datetime.now()
    logging.info("🚀 Starting ETL process...")

    logging.info("🔑 Loading connection parameters...")
    conn_params = load_conn_params()

    # ➕ Create database if not exist
    ensure_database_exists(conn_params)

    logging.info("📥 Extracting data from SpaceX API...")
    raw_data = extract_from_spacex()
    if not raw_data:
        logging.warning("⚠️ No data fetched.")
        return
    
    logging.info("🔧 Transforming data...")
    df = transform_launch_data(raw_data)
    if df.empty:
        logging.warning("⚠️ Transformed DataFrame is empty.")
        return

    logging.info("⬆️ Uploading data to PostgreSQL...")
    upload_to_postgres(df, conn_params)
    
    elapsed = (datetime.now() - start).total_seconds()
    logging.info(f"🎯 ETL finished in {elapsed:.2f}s")

if __name__ == "__main__":
    run_spacex_pipeline()