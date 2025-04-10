#!/bin/bash
set -euo pipefail

GREEN="\033[0;32m"
YELLOW="\033[1;33m"
RED="\033[0;31m"
RESET="\033[0m"

log_info()  { echo -e "${GREEN}🟢 [INFO] $*${RESET}"; }
log_warn()  { echo -e "${YELLOW}🟡 [WARN] $*${RESET}"; }
log_error() { echo -e "${RED}🔴 [ERROR] $*${RESET}" >&2; }

# Get the directory where the script is located
SCRIPT_DIR=$(dirname "$0")

# Get the current user and group (for ownership)
USER_GROUP=50000

# Fix permissions for Airflow directories (scheduler, dbt, etc.)
fix_airflow_permissions() {
  log_info "Fixing permissions for Airflow directories..."

  # Ensure directories exist
  mkdir -p ${SCRIPT_DIR}/dags
  mkdir -p ${SCRIPT_DIR}/logs
  mkdir -p ${SCRIPT_DIR}/dbt

  # Fix ownership to the current user/group
  chown -R ${USER_GROUP}:${USER_GROUP} ${SCRIPT_DIR}/dags
  chmod -R 777 ${SCRIPT_DIR}/dags

  chown -R ${USER_GROUP}:${USER_GROUP} ${SCRIPT_DIR}/logs
  chmod -R 777 ${SCRIPT_DIR}/logs

  chown -R ${USER_GROUP}:${USER_GROUP} ${SCRIPT_DIR}/dbt
  chmod -R u+rwX,g+rwX ${SCRIPT_DIR}/dbt

  # List permissions
  log_info "Airflow directories permissions:"
  ls -ld ${SCRIPT_DIR}/dags
  ls -ld ${SCRIPT_DIR}/logs
  ls -ld ${SCRIPT_DIR}/dbt

  log_info "Permissions for Airflow directories fixed."
}

# Start Docker Compose and ensure Airflow is up and running
start_airflow() {
  log_info "Starting Docker Compose..."
  docker compose up --build -d
  log_info "Docker Compose started successfully."
}

# Run main setup tasks
main() {
  log_info "Starting Airflow setup..."

  # Fix permissions for Airflow directories
  fix_airflow_permissions

  # Start Docker Compose to bring up Airflow
  start_airflow

  log_info "Airflow setup completed successfully."
}

if ! main "$@"; then
  log_error "Airflow setup failed"
  exit 1
fi
