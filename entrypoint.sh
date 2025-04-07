#!/bin/bash
set -euo pipefail

echo "⌛ Waiting for PostgreSQL to become available..."
until pg_isready -h "$POSTGRES_HOST" -p "${POSTGRES_PORT:-5432}" > /dev/null 2>&1; do
  sleep 2
done
echo "✔️ PostgreSQL is ready. Proceeding with Airflow DB initialization..."

# Generate DBT profile
envsubst < "${AIRFLOW_HOME}/dbt_profiles/profiles.template.yml" > "${AIRFLOW_HOME}/dbt_profiles/profiles.yml"
echo "✔️ DBT profile generated."

# Ensure logs dir ownership
mkdir -p "$AIRFLOW_HOME/logs/scheduler" 2>/dev/null
chown -R airflow: "$AIRFLOW_HOME/logs" 2>/dev/null
echo "✔️ Logs directory ready."

# Initialize Airflow DB
if airflow db migrate > /dev/null 2>&1; then
  echo "✔️ Airflow metadata DB initialized successfully."
else
  echo "❌ Failed to initialize Airflow metadata DB."
  exit 1
fi

# Create admin user
echo "👤 Creating Airflow admin user..."
airflow users create \
  --username "${AIRFLOW_ADMIN_USERNAME:-admin}" \
  --password "${AIRFLOW_ADMIN_PASSWORD:-admin}" \
  --firstname "${AIRFLOW_ADMIN_FIRSTNAME:-Admin}" \
  --lastname "${AIRFLOW_ADMIN_LASTNAME:-User}" \
  --email "${AIRFLOW_ADMIN_EMAIL:-admin@example.com}" \
  --role Admin > /dev/null 2>&1 && echo "✔️ Admin user created." || {
    echo "⚠️ Failed to create admin user (possibly already exists)."
  }


echo "🏁 Airflow initialization completed. Exiting."
