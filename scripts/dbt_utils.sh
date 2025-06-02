#!/bin/bash

source /scripts/general_utils.sh

generate_dbt_profile() {
  log_info "Generating DBT profile..."
  mkdir -p "${AIRFLOW_HOME}/dbt/profiles" || true

  # Fix permissions if not writable
  if [ ! -w "${AIRFLOW_HOME}/dbt/profiles" ]; then
    log_warn "No write permission to ${AIRFLOW_HOME}/dbt/profiles. Attempting to fix..."
    if ! chown -R "$(id -u):$(id -g)" "${AIRFLOW_HOME}/dbt/profiles"; then
      log_error "Failed to fix permissions for ${AIRFLOW_HOME}/dbt/profiles. Exiting."
    fi
  fi

  if [ ! -w "${AIRFLOW_HOME}/dbt/profiles" ]; then
    log_warn "Still no write permission to ${AIRFLOW_HOME}/dbt/profiles. Skipping DBT profile generation."
    return
  fi

  local output_file="${AIRFLOW_HOME}/dbt/profiles/profiles.yml"
  local temp_file
  temp_file=$(mktemp)

  envsubst < "${AIRFLOW_HOME}/dbt/profiles/profiles.template.yml" > "$temp_file"

  if ! cmp -s "$temp_file" "$output_file"; then
    mv "$temp_file" "$output_file"
    log_info "DBT profile updated."
  else
    rm "$temp_file"
    log_info "DBT profile unchanged."
  fi
}

install_dbt_dependencies() {
  log_info "Installing dbt dependencies..."
  if [ ! -f "${AIRFLOW_HOME}/dbt/packages.yml" ]; then
    log_warn "DBT packages.yml not found at ${AIRFLOW_HOME}/dbt/packages.yml. Skipping dbt deps."
    return 0 # Not a fatal error if packages.yml doesn't exist
  fi

  log_info "Found packages.yml, running dbt deps in ${AIRFLOW_HOME}/dbt..."
  if ! (cd "${AIRFLOW_HOME}/dbt" && dbt deps); then
    log_error "Failed to install dbt dependencies. Check dbt logs for details."
    # Decide if this should be a fatal error. For now, let's make it non-fatal
    # as the core pipeline might function without some optional tests.
    # To make it fatal, use 'return 1' or 'exit 1'.
    return 0 # For now, non-fatal
  fi
  log_info "dbt dependencies installation command completed."
  return 0
}