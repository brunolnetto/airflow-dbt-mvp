#!/bin/bash
set -euo pipefail

# === Colors ===
GREEN="\033[0;32m"
YELLOW="\033[1;33m"
RED="\033[0;31m"
RESET="\033[0m"

# === Logging ===
log_info()  { echo -e "${GREEN}🟢 [INFO] $*${RESET}"; }
log_warn()  { echo -e "${YELLOW}🟡 [WARN] $*${RESET}"; }
log_error() { echo -e "${RED}🔴 [ERROR] $*${RESET}" >&2; }

trap 'log_error "Unexpected error at line $LINENO. Exiting."' ERR

# === Script directory ===
SCRIPT_DIR=$(dirname "$0")

# Default user/group ID (can be overridden)
USER_GROUP=${USER_GROUP:-50000}

# === Functions ===
fix_airflow_permissions() {
  log_info "🔧 Fixing permissions for Airflow directories..."

  for dir in dags logs dbt; do
    full_path="${SCRIPT_DIR}/${dir}"
    mkdir -p "$full_path"

    sudo chown -R "${USER_GROUP}:${USER_GROUP}" "$full_path"
    if [ "$dir" = "dbt" ]; then
      sudo chmod -R u+rwX,g+rwX "$full_path"
    else
      sudo chmod -R 777 "$full_path"
    fi

    ls -ld "$full_path"
  done

  log_info "✅ Permissions for Airflow directories fixed."
}

start_airflow() {
  log_info "🚀 Starting Docker Compose..."
  if ! sudp docker compose up --build -d; then
    log_error "Docker Compose failed to start Airflow containers."
    exit 1
  fi
  log_info "✅ Docker Compose started successfully."
}

main() {
  log_info "👤 Running as user: $(whoami)"
  log_info "⚙️ Starting Airflow setup..."

  fix_airflow_permissions
  start_airflow

  log_info "🎉 Airflow setup completed successfully."
}

# === Run ===
if ! main "$@"; then
  log_error "Airflow setup failed"
  exit 1
fi
