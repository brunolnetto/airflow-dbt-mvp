#!/bin/bash
set -e

# Inicializa o banco de dados (SQLite por padrão)
airflow db init

# Executa o scheduler uma vez para parsear os DAGs e registrar no banco (DagModel)
airflow scheduler --num-runs 1

# Garante que o DAG está despausado
airflow dags unpause spacex_dag || true

# Dispara o DAG
echo "Triggering DAG spacex_dag..."
airflow dags trigger spacex_dag

# Executa o scheduler para processar as tasks disparadas (usando SequentialExecutor para simplicidade)
export AIRFLOW__CORE__EXECUTOR=SequentialExecutor
airflow scheduler --num-runs 1

echo "DAG spacex_dag execution completed. Exiting."
