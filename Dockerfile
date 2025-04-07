FROM apache/airflow:2.7.2-python3.10

USER root

# Enable bytecode compilation
ENV UV_COMPILE_BYTECODE=1

# Copy from the cache instead of linking since it's a mounted volume
ENV UV_LINK_MODE=copy

RUN apt-get update && apt-get install -y gettext && rm -rf /var/lib/apt/lists/*

# Copie os arquivos antes de mudar permissões
COPY . .

RUN mkdir -p /opt/airflow/dbt_profiles && \
    chown -R airflow: /opt/airflow

USER root
RUN pip install --no-cache-dir \
    dbt-core dbt-postgres apache-airflow 

USER airflow
WORKDIR /opt/airflow

RUN pip install --no-cache-dir uv

RUN uv lock

# Install the project's dependencies using the lockfile and settings
RUN --mount=type=bind,source=pyproject.toml,target=pyproject.toml \
    uv sync --frozen --no-install-project --no-dev

# Place executables in the environment at the front of the path
ENV PATH="/app/.venv/bin:$PATH"

