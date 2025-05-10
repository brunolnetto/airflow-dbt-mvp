import logging
from typing import Dict

import pandas as pd
import psycopg2
import psycopg2.extras

from .config import RAW_SCHEMA, logging
from .utils import convert_dataframe_to_tuples

logger = logging.getLogger(__name__)

def ensure_database_exists(db_params: dict) -> None:
    dbname = db_params["dbname"]
    logger.info(f"🔍 Checking if database '{dbname}' exists...")
    check_conn_params = db_params.copy()
    check_conn_params["dbname"] = "postgres"

    try:
        with psycopg2.connect(**check_conn_params) as conn:
            conn.set_session(autocommit=True)
            with conn.cursor() as cur:
                cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (dbname,))
                if not cur.fetchone():
                    logger.info(f"📦 Creating database '{dbname}'...")
                    cur.execute(f'CREATE DATABASE "{dbname}"')
                else:
                    logger.info(f"✔️ Database '{dbname}' already exists.")
    except Exception as e:
        logger.exception(f"❌ Failed to ensure database exists: {e}")
        raise

def quote_column_name(col_name: str) -> str:
    """Properly quote column names for SQL compatibility."""
    return f'"{col_name}"'

def create_table_sql(df: pd.DataFrame, entity: str) -> str:
    """Generate SQL to create a table matching the DataFrame's schema."""
    table_name = f"{RAW_SCHEMA.lower()}.{entity.lower()}"
    columns = {
        quote_column_name(col): "TEXT"
        for col in df.columns
    }
    # Enforce primary key on 'id'
    columns[quote_column_name("id")] = "TEXT PRIMARY KEY"

    column_defs = ",\n    ".join(f"{col} {dtype}" for col, dtype in columns.items())

    return f"""
        CREATE SCHEMA IF NOT EXISTS {RAW_SCHEMA};
        CREATE TABLE IF NOT EXISTS {table_name} (
            {column_defs}
        );
    """

def insert_data_sql(df: pd.DataFrame, entity: str) -> str:
    """Generate SQL for inserting data into the table."""
    table_name = f"{RAW_SCHEMA}.{entity}"
    cols = ", ".join(quote_column_name(col) for col in df.columns)

    return f"""
        INSERT INTO {table_name} ({cols})
        VALUES %s
        ON CONFLICT (id) DO NOTHING;
    """

def upload_to_postgres(df: pd.DataFrame, conn_params: Dict[str, str], entity: str) -> None:
    """Upload a DataFrame to PostgreSQL."""
    table_name = f"{RAW_SCHEMA}.{entity}"
    logger.info(f"⬆️ Uploading {len(df)} rows to '{table_name}'...")

    create_sql = create_table_sql(df, entity)
    insert_sql = insert_data_sql(df, entity)
    values = convert_dataframe_to_tuples(df)

    try:
        with psycopg2.connect(**conn_params) as conn:
            with conn.cursor() as cur:
                cur.execute(create_sql)
                psycopg2.extras.execute_values(cur, insert_sql, values)
            conn.commit()
        logger.info(f"✅ Upload to '{table_name}' completed successfully.")
    except Exception as e:
        logger.exception(f"❌ Failed to upload data to '{table_name}': {e}")
        raise
