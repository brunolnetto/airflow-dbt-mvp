#!/bin/bash

source /scripts/general_utils.sh
source /scripts/minio_utils.sh
source /scripts/dbt_utils.sh
source /scripts/airflow_utils.sh
source /scripts/postgres_utils.sh

ensure_var_set "POSTGRES_HOST"
ensure_var_set "AIRFLOW_HOME"
ensure_var_set "MINIO_ROOT_USER"
ensure_var_set "MINIO_ROOT_PASSWORD"
ensure_var_set "MINIO_BUCKET"

main() {
  log_info "Running as user: $(whoami)"

  # Guard: check if sudo is needed (e.g., write to AIRFLOW_HOME)
  if [ ! -w "$AIRFLOW_HOME" ]; then
    log_warn "Current user lacks write permissions to $AIRFLOW_HOME"
    if command -v sudo &> /dev/null; then
      log_warn "You may need to run this script with: sudo $0"
    fi
  fi

  # Execute each step with individual error handling
  steps=(
    wait_for_postgres
    create_postgres_databases
    generate_dbt_profile
    install_dbt_dependencies
    setup_minio
    initialize_airflow_db
    create_airflow_admin_user
  )

  for step in "${steps[@]}"; do
    log_info "▶️ Running step: $step"
    if ! $step; then
      log_error "❌ Step '$step' failed. Check logs above for details."
      exit 1
    fi
  done

  log_info "✅ Airflow initialization completed successfully."
}

# Main execution
if ! main "$@"; then
  log_error "Airflow bootstrap failed"
  exit 1
fi
