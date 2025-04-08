FROM apache/airflow:2.7.2-python3.10

USER root
WORKDIR /opt/airflow

# Install gettext (used by envsubst)
RUN apt-get update && apt-get install -y --no-install-recommends gettext && \
    rm -rf /var/lib/apt/lists/*

# Create DBT profile directory and fix permissions
RUN mkdir -p /opt/airflow/dbt_profiles && \
    chown -R airflow: /opt/airflow

# Copy only the requirements first to leverage Docker cache
COPY requirements.txt .

USER airflow

# Upgrade pip and install Python dependencies
RUN pip install --upgrade pip && \
    pip install --user -r requirements.txt

# Copy the rest of the code after installing dependencies
COPY --chown=airflow:airflow . .

