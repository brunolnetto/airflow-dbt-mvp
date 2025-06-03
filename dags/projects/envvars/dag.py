from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import os

def print_env():
    print("AWS_ACCESS_KEY_ID:", os.getenv("AWS_ACCESS_KEY_ID"))
    print("AWS_SECRET_ACCESS_KEY:", os.getenv("AWS_SECRET_ACCESS_KEY"))

with DAG("debug_env_dag", start_date=datetime(2024, 1, 1), schedule_interval=None, catchup=False) as dag:
    t1 = PythonOperator(
        task_id="print_env",
        python_callable=print_env,
    )