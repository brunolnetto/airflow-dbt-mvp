import requests
import logging
from datetime import datetime
from typing import List, Dict
from contextlib import closing
from dotenv import load_dotenv
from dataclasses import dataclass
from pathlib import Path
import json
import os

import boto3
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from psycopg2 import extras
from io import BytesIO

# ========== Config ==========
DATABASE_NAME = "spacex_db"
RAW_SCHEMA = "raw"

RAW_DIR = Path("data/raw")
PROCESSED_DIR = Path("data/processed")

load_dotenv()

# ========== Env ==========
def get_env_or_fail(var: str, fallback=None):
    value = os.getenv(var, fallback)
    if value is None:
        raise RuntimeError(f"❌ Missing required env var: {var}")
    return value

@dataclass
class MinIOConfig:
    endpoint_url: str = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
    access_key: str = os.getenv("MINIO_ACCESS_KEY")
    secret_key: str = os.getenv("MINIO_SECRET_KEY")
    region_name: str = os.getenv("MINIO_REGION")
    bucket_name: str = os.getenv("MINIO_BUCKET")

# Global MinIO config
minio_config = MinIOConfig()
s3_client = get_s3_client(minio_config)

def get_s3_client(config: MinIOConfig):
    return boto3.client(
        "s3",
        endpoint_url=config.endpoint_url,
        aws_access_key_id=config.access_key,
        aws_secret_access_key=config.secret_key,
        region_name=config.region_name
    )

def load_conn_params():
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

# ========== Transform ========== 
def transform_generic_data(raw_data: List[dict]) -> pd.DataFrame:
    logging.info("🔧 Stringifying values and preparing DataFrame...")

    stringified_data = []
    for item in raw_data:
        if isinstance(item, dict):
            # Stringify values for each key-value pair in the dictionary
            stringified_item = {
                k: json.dumps(v) if isinstance(v, (dict, list)) else str(v) for k, v in item.items()
            }
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

    with psycopg2.connect(**check_conn_params) as conn:
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

def ensure_bucket_exists(client, bucket_name):
    not_in_bucket=not any(b['Name'] == bucket_name for b in client.list_buckets()['Buckets'])
    if not client.list_buckets() or not_in_bucket:
        client.create_bucket(Bucket=bucket_name)

def quote_column_name(col: str) -> str:
    """Helper function to safely quote column names to handle reserved keywords and special characters."""
    return f'"{col}"'

def create_table_sql(df: pd.DataFrame, entity: str) -> str:
    """Generate the SQL query to create the table."""
    table_name = f"{RAW_SCHEMA.lower()}.{entity.lower()}"
    
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
def extract_spacex_data(entity: str) -> List[dict]:
    logging.info(f"🚀 [Extract] Starting data extraction for entity: {entity}")
    raw_data = extract_entity(entity)

    if not raw_data:
        logging.warning(f"⚠️ No data fetched for entity: {entity}")
        return []

    RAW_DIR.mkdir(parents=True, exist_ok=True)
    json_path = RAW_DIR / f"{entity}.json"
    
    try:
        with open(json_path, "w") as f:
            json.dump(raw_data, f)
        logging.info(f"💾 Raw data saved to {json_path}")
    except Exception as e:
        logging.error(f"❌ Failed to save raw data: {e}")
        raise

    return raw_data

def check_raw_file_exists(entity: str) -> Path:
    json_path = RAW_DIR / f"{entity}.json"
    if not json_path.exists():
        logging.error(f"❌ Raw JSON file not found for entity: {entity}")
        raise FileNotFoundError(json_path)
    return json_path

def load_and_transform_data(json_path: Path) -> pd.DataFrame:
    with open(json_path, "r") as f:
        raw_data = json.load(f)
    return transform_generic_data(raw_data)

def load_and_transform_data(json_path: Path) -> pd.DataFrame:
    with open(json_path, "r") as f:
        raw_data = json.load(f)
    return transform_generic_data(raw_data)

def serialize_to_buffer(df: pd.DataFrame) -> BytesIO:
    buffer = BytesIO()
    df.to_parquet(buffer, index=False)
    buffer.seek(0)
    return buffer


def ensure_bucket_and_upload(df: pd.DataFrame, entity: str) -> None:
    # Ensure the bucket exists before uploading
    try:
        ensure_bucket_exists(s3_client, minio_config.bucket_name)  # Ensure bucket exists
    except Exception as e:
        logging.error(f"❌ Failed to ensure bucket existence: {e}")
        raise

    # Serialize to buffer
    buffer = serialize_to_buffer(df)

    # Upload to MinIO
    object_key = f"processed/{entity}.parquet"
    try:
        s3_client.upload_fileobj(buffer, minio_config.bucket_name, object_key)
        logging.info(f"📤 Uploaded {object_key} to bucket '{minio_config.bucket_name}' on MinIO")
    except Exception as e:
        logging.error(f"❌ Failed to upload to MinIO: {e}")
        raise

def backup_locally(df: pd.DataFrame, entity: str) -> None:
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    local_parquet = PROCESSED_DIR / f"{entity}.parquet"
    df.to_parquet(local_parquet)
    logging.info(f"📦 Local backup saved to {local_parquet}")


# In the transform_spacex_data function, before uploading to MinIO
def transform_spacex_data(entity: str) -> pd.DataFrame:
    logging.info(f"🔧 [Transform] Starting transformation for entity: {entity}")

    try:
        json_path = check_raw_file_exists(entity)
        df = load_and_transform_data(json_path)

        if df.empty:
            logging.warning(f"⚠️ Transformed DataFrame for entity '{entity}' is empty.")
            return df

        # Ensure the bucket exists and upload
        ensure_bucket_and_upload(df, entity)

        # Optional: Local backup
        backup_locally(df, entity)

        return df

    except Exception as e:
        logging.error(f"❌ Transformation failed for entity: {entity}. Error: {e}")
        raise


def load_spacex_data(entity: str) -> None:
    logging.info(f"⬆️ [Load] Loading data for entity: {entity}")
    parquet_path = PROCESSED_DIR / f"{entity}.parquet"

    if not parquet_path.exists():
        logging.error(f"❌ Transformed parquet file not found for entity: {entity}")
        raise FileNotFoundError(parquet_path)

    df = pd.read_parquet(parquet_path)

    conn_params = load_conn_params()
    ensure_database_exists(conn_params)

    upload_to_postgres(df, conn_params, entity)


