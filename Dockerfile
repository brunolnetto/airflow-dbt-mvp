FROM apache/airflow:2.7.2-python3.10

USER root

RUN apt-get update && apt-get install -y gettext && rm -rf /var/lib/apt/lists/*

RUN mkdir -p /opt/airflow/logs/scheduler && \
    mkdir -p /opt/airflow/dbt_profiles && \
    chown -R airflow: /opt/airflow/logs /opt/airflow/dbt_profiles

USER airflow
WORKDIR /opt/airflow

COPY requirements.txt .

RUN pip install --no-cache-dir uv

# Just install with uv pip, without --system
RUN uv venv && uv pip install --no-cache-dir -r requirements.txt
