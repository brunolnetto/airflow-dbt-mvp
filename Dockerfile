FROM apache/airflow:2.10.5-python3.10

WORKDIR /opt/airflow

USER root

# Install system dependencies and cleanup
RUN apt-get update && \
    apt-get install -y --no-install-recommends gettext curl sudo git && \
    rm -rf /var/lib/apt/lists/*

# Add airflow user to sudoers with no password prompt (if it doesn't exist)
RUN echo "airflow ALL=(ALL) NOPASSWD:ALL" > /etc/sudoers.d/airflow

# Ensure airflow user exists and belongs to the airflow group
RUN groupadd -f -g 50000 airflow && usermod -aG airflow airflow

# Download and install uv from external source
COPY --from=ghcr.io/astral-sh/uv:0.6.13 /uv /uvx /bin/

# Change ownership of /opt/airflow to airflow user
RUN chown -R airflow:airflow /opt/airflow

# Create necessary directories for logging and set correct permissions
RUN mkdir -p /opt/airflow/logs/scheduler && \
    chown -R airflow:airflow /opt/airflow/logs && \
    chmod -R 777 /opt/airflow/logs && \
    find /opt/airflow/logs -type d -exec chmod 777 {} \;  # Ensure all directories are writable

# Copy and install Python dependencies
COPY requirements.txt .
RUN uv pip install --system -r requirements.txt && uv pip install --upgrade protobuf

# Change ownership of the whole working directory to airflow user
USER airflow
COPY --chown=airflow:airflow . .

# Ensure proper ownership and permissions for dbt files
RUN chown -R airflow:airflow /opt/airflow/dbt && \
    chmod -R 755 /opt/airflow/dbt && \
    find /opt/airflow/dbt -type f -exec chmod u+rw {} \;
