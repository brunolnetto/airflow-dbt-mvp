import os
import requests
import logging
from datetime import datetime
from typing import List, Dict
from contextlib import closing
import json

import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from psycopg2 import extras

# ========== Config ==========
DATABASE_NAME = "spacex_db"
RAW_SCHEMA = "raw"

# ========== Env ==========
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

# ========== Logging ==========
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# ========== Extract ==========
def extract_entity(entity: str) -> List[dict]:
    url = f"https://api.spacexdata.com/v4/{entity}"
    try:
        response = requests.get(url)
        response.raise_for_status()
        logging.info(f"✅ Data fetched for entity: {entity}")
        return response.json()
    except requests.exceptions.RequestException as e:
        logging.error(f"❌ Failed to fetch data from {url}: {e}")
        return []

import json

# ========== Transform ========== 
def transform_generic_data(raw_data: List[dict]) -> pd.DataFrame:
    logging.info("🔧 Stringifying values and preparing DataFrame...")

    stringified_data = []
    for item in raw_data:
        if isinstance(item, dict):
            # Stringify values for each key-value pair in the dictionary
            stringified_item = {k: json.dumps(v) if not isinstance(v, str) else v for k, v in item.items()}
            stringified_data.append(stringified_item)
        else:
            # If the item is already a string, just append it
            stringified_data.append({"json_data": item})  # assuming item is a stringified JSON object

    # Convert the list of dictionaries to a DataFrame
    df = pd.DataFrame(stringified_data)
    
    logging.info(f"📊 Converted {len(df)} rows with {len(df.columns)} columns.")
    
    return df

# ========== Load ==========
def ensure_database_exists(db_params: dict):
    dbname = db_params["dbname"]
    logging.info(f"🔍 Checking if database '{dbname}' exists...")
    check_conn_params = db_params.copy()
    check_conn_params["dbname"] = "postgres"

    conn = psycopg2.connect(**check_conn_params)
    conn.set_session(autocommit=True)

    try:
        with conn.cursor() as cur:
            cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (dbname,))
            exists = cur.fetchone()
            if not exists:
                logging.info(f"📦 Creating database '{dbname}'...")
                cur.execute(f'CREATE DATABASE "{dbname}"')
            else:
                logging.info(f"✔️ Database '{dbname}' already exists.")
    finally:
        conn.close()

def quote_column_name(col: str) -> str:
    """Helper function to safely quote column names to handle reserved keywords and special characters."""
    return f'"{col}"'

def create_table_sql(df: pd.DataFrame, entity: str) -> str:
    """Generate the SQL query to create the table."""
    table_name = f"{RAW_SCHEMA}.{entity}"
    
    # Column types based on inferred data types from DataFrame
    cols_types = {quote_column_name(col): "TEXT" for col in df.columns}
    cols_types[quote_column_name("id")] = "TEXT PRIMARY KEY"  # assuming 'id' is unique for all entities

    create_cols = ",\n".join([f"{col} {type_}" for col, type_ in cols_types.items()])
    
    # Create SQL statement to create table with quoted column names
    create_sql = f"""
        CREATE SCHEMA IF NOT EXISTS {RAW_SCHEMA};
        CREATE TABLE IF NOT EXISTS {table_name} (
            {create_cols}
        )
    """
    return create_sql

def insert_data_sql(df: pd.DataFrame, entity: str) -> str:
    """Generate the SQL query to insert data."""
    table_name = f"{RAW_SCHEMA}.{entity}"
    insert_cols = ", ".join([quote_column_name(col) for col in df.columns])
    
    # Insert SQL with placeholders for batch insertion
    insert_sql = f"""
        INSERT INTO {table_name} ({insert_cols})
        VALUES %s
        ON CONFLICT (id) DO NOTHING
    """
    return insert_sql

def convert_dataframe_to_tuples(df: pd.DataFrame) -> list:
    """Convert DataFrame rows to a list of tuples."""
    return [tuple(row) for row in df.to_numpy()]

def upload_to_postgres(df: pd.DataFrame, conn_params: Dict[str, str], entity: str) -> None:
    """Main function to upload data to PostgreSQL."""
    table_name = f"{RAW_SCHEMA}.{entity}"
    logging.info(f"⬆️ Uploading data to table '{table_name}'...")

    # Step 1: Create table if not exists
    create_sql = create_table_sql(df, entity)
    
    # Step 2: Insert data into table
    insert_sql = insert_data_sql(df, entity)
    
    # Convert DataFrame to list of tuples for batch insert
    values = convert_dataframe_to_tuples(df)
    
    # Step 3: Execute SQL queries and perform insert
    try:
        with psycopg2.connect(**conn_params) as conn:
            with conn.cursor() as cur:
                # Create table if it doesn't exist
                cur.execute(create_sql)
                
                # Insert data into the table
                extras.execute_values(cur, insert_sql, values)
                
            conn.commit()

        logging.info("✅ Upload complete.")

    except Exception as e:
        logging.error(f"❌ Error uploading data: {e}")
        raise

# ========== Pipeline ==========
def run_spacex_pipeline(entity: str) -> None:
    start = datetime.now()
    logging.info(f"🚀 Starting ETL for entity: {entity}")

    conn_params = load_conn_params()
    ensure_database_exists(conn_params)

    raw_data = extract_entity(entity)
    if not raw_data:
        logging.warning("⚠️ No data fetched.")
        return

    df = transform_generic_data(raw_data)
    if df.empty:
        logging.warning("⚠️ Transformed DataFrame is empty.")
        return

    upload_to_postgres(df, conn_params, entity)

    elapsed = (datetime.now() - start).total_seconds()
    logging.info(f"🎯 ETL for '{entity}' finished in {elapsed:.2f}s")


