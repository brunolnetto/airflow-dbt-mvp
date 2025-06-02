#!/bin/bash

source /scripts/general_utils.sh

initialize_airflow_db() {
  log_info "Initializing Airflow metadata database..."

  if ! output=$(airflow db migrate 2>&1); then
    log_error "Failed to initialize Airflow metadata DB. Output:"
    echo "$output" >&2
    exit 1
  fi

  log_info "Airflow metadata DB initialized successfully."
}

create_airflow_admin_user() {
  if airflow users list | grep -q "${AIRFLOW_ADMIN_USERNAME:-admin}"; then
    log_info "Admin user already exists on Airflow."
  else
    log_info "Creating Airflow admin user..."
    airflow users create \
      --username "${AIRFLOW_ADMIN_USERNAME:-admin}" \
      --password "${AIRFLOW_ADMIN_PASSWORD:-admin}" \
      --firstname "${AIRFLOW_ADMIN_FIRSTNAME:-Admin}" \
      --lastname "${AIRFLOW_ADMIN_LASTNAME:-User}" \
      --email "${AIRFLOW_ADMIN_EMAIL:-admin@example.com}" \
      --role Admin > /dev/null 2>&1 && \
      log_info "Admin user created."
  fi
}