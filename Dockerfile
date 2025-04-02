FROM apache/airflow:2.7.2-python3.10

USER root
WORKDIR /opt/airflow

COPY dags/ /opt/airflow/dags/
COPY api/  /opt/airflow/api/
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

USER airflow
ENTRYPOINT ["/entrypoint.sh"]
