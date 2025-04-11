#!/bin/bash
set -euo pipefail

# Colors for logs
GREEN="\033[0;32m"
YELLOW="\033[1;33m"
RED="\033[0;31m"
RESET="\033[0m"

# Logging functions
log_info()  { echo -e "${GREEN}🟢 [INFO] $*${RESET}"; }
log_warn()  { echo -e "${YELLOW}🟡 [WARN] $*${RESET}"; }
log_error() { echo -e "${RED}🔴 [ERROR] $*${RESET}" >&2; }

trap 'log_error "Unexpected error at line $LINENO. Exiting."' ERR

# Ensure required vars are set
ensure_var_set() {
  : "${1:?$1 is required}"
}
ensure_var_set "POSTGRES_HOST"
ensure_var_set "AIRFLOW_HOME"

wait_for_postgres() {
  log_info "Waiting for PostgreSQL to become available at $POSTGRES_HOST:${POSTGRES_PORT:-5432}..."
  until pg_isready -h "$POSTGRES_HOST" -p "${POSTGRES_PORT:-5432}" > /dev/null 2>&1; do
    sleep 2
  done
  log_info "PostgreSQL is ready."
}

create_postgres_databases() {
  if [ -z "${POSTGRES_DATABASES:-}" ]; then
    log_info "No extra databases requested to be created via POSTGRES_DATABASES."
    return
  fi

  log_info "🔧 Creating extra PostgreSQL databases (if not existing)..."
  
  IFS=',' read -ra DBS <<< "$POSTGRES_DATABASES"
  for db in "${DBS[@]}"; do
    db_trimmed="$(echo "$db" | xargs)"
    if [ -z "$db_trimmed" ]; then
      continue
    fi

    log_info "🔍 Checking existence of database '$db_trimmed'..."
    if PGPASSWORD="$POSTGRES_PASSWORD" psql -U "$POSTGRES_USER" -h "$POSTGRES_HOST" -p "${POSTGRES_PORT:-5432}" -tc "SELECT 1 FROM pg_database WHERE datname = '$db_trimmed'" | grep -q 1; then
      log_warn "Database '$db_trimmed' already exists. Skipping."
    else
      log_info "📦 Creating database '$db_trimmed'..."
      PGPASSWORD="$POSTGRES_PASSWORD" createdb -U "$POSTGRES_USER" -h "$POSTGRES_HOST" -p "${POSTGRES_PORT:-5432}" "$db_trimmed"
      log_info "✅ Database '$db_trimmed' created."
    fi
  done
}

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
