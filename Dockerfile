FROM apache/airflow:2.10.5-python3.10

USER root
WORKDIR /opt/airflow

# Install system dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends gettext curl && \
    rm -rf /var/lib/apt/lists/*

# Download and install uv
COPY --from=ghcr.io/astral-sh/uv:0.6.13 /uv /uvx /bin/

# Change ownership of /opt/airflow to airflow user
RUN chown -R airflow: /opt/airflow

# Copy requirements.txt
COPY requirements.txt .

# Instala as dependências com uv
RUN uv pip install --system -r requirements.txt
RUN uv pip install --upgrade protobuf

# Change ownership of requirements.txt to airflow user
USER airflow

# Copia o restante da aplicação
COPY --chown=airflow:airflow . .

# Change ownership to match Airflow's user (UID 50000 is common for airflow)
RUN chown -R 50000:0 /opt/airflow/dbt