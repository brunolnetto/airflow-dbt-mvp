FROM apache/airflow:2.7.2-python3.10

USER airflow

# Install required packages in the airflow user's environment
RUN pip install --no-cache-dir \
    dbt-bigquery \
    google-cloud-storage \
    pandas \
    requests

WORKDIR /opt/airflow
