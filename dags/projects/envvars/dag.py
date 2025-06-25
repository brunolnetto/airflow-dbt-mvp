from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import os

def print_env():
    print("MINIO_ACCESS_KEY:", os.getenv("MINIO_ACCESS_KEY"))
    print("MINIO_SECRET_KEY:", os.getenv("MINIO_SECRET_KEY"))

with DAG("debug_env_dag", start_date=datetime(2024, 1, 1), schedule=None, catchup=False) as dag:
    t1 = PythonOperator(
        task_id="print_env",
        python_callable=print_env,
    )