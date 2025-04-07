# api/storage_loader.py

import os
import logging
from contextlib import closing

from airflow.hooks.postgres_hook import PostgresHook
import pandas as pd


# Configurar logger
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

FILE_PATH = os.getenv("SPACEX_CSV_PATH", "/opt/airflow/data/spacex_data.csv")
POSTGRES_CONN_ID = "postgres_default"
TABLE_NAME = "spacex_launches"

CREATE_TABLE_SQL = f"""
CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
    mission_name TEXT,
    launch_date TIMESTAMP,
    rocket_type TEXT,
    launch_site TEXT,
    details TEXT
)
"""

INSERT_SQL = f"""
INSERT INTO {TABLE_NAME} (mission_name, launch_date, rocket_type, launch_site, details)
VALUES (%s, %s, %s, %s, %s)
"""

def load_to_postgres(file_path: str = FILE_PATH, conn_id: str = POSTGRES_CONN_ID):
    logging.info(f"📂 Read : {file_path}")
    df = pd.read_csv(file_path)

    # Validação simples das colunas esperadas
    expected_columns = {'mission_name', 'launch_date', 'rocket_type', 'launch_site', 'details'}
    if not expected_columns.issubset(df.columns):
        raise ValueError(f"❌ Invalid CSV columns. Esperado: {expected_columns}")

    pg_hook = PostgresHook(postgres_conn_id=conn_id)

    with closing(pg_hook.get_conn()) as conn:
        with closing(conn.cursor()) as cursor:
            logging.info("🛠️ Criando tabela se não existir...")
            cursor.execute(CREATE_TABLE_SQL)

            logging.info(f"⬆️ Inserindo {len(df)} linhas no PostgreSQL...")
            cursor.executemany(INSERT_SQL, df[
                ['mission_name', 'launch_date', 'rocket_type', 'launch_site', 'details']
            ].values.tolist())

        conn.commit()
        logging.info("✅ Data loaded successfully.")

if __name__ == "__main__":
    load_to_postgres()
