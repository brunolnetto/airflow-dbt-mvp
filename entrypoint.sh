#!/bin/bash
set -euo pipefail


GREEN="\033[0;32m"
YELLOW="\033[1;33m"
RED="\033[0;31m"
RESET="\033[0m"

log_info()  { echo -e "${GREEN}🟢 [INFO] $*${RESET}"; }
log_warn()  { echo -e "${YELLOW}🟡 [WARN] $*${RESET}"; }
log_error() { echo -e "${RED}🔴 [ERROR] $*${RESET}" >&2; }

trap 'log_error "Unexpected error at line $LINENO. Exiting."' ERR

# Ensure required vars are set
: "${POSTGRES_HOST:?POSTGRES_HOST is required}"
: "${AIRFLOW_HOME:?AIRFLOW_HOME is required}"

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

  echo "$POSTGRES_DATABASES" | grep -q ',' || POSTGRES_DATABASES="$POSTGRES_DATABASES,"

  log_info "🔧 Creating extra PostgreSQL databases (if not existing)..."

  IFS=',' read -ra DBS <<< "$POSTGRES_DATABASES"
  for db in "${DBS[@]}"; do
    db_trimmed="$(echo "$db" | xargs)"  # remove espaços extras
    if [ -z "$db_trimmed" ]; then
      continue
    fi

    log_info "🔍 Checking existence of database '$db_trimmed'..."
    if PGPASSWORD="$POSTGRES_PASSWORD" psql -U "$POSTGRES_USER" -h "$POSTGRES_HOST" -p "${POSTGRES_PORT:-5432}" -tc "SELECT 1 FROM pg_database WHERE datname = '$db_trimmed'" | grep -q 1; then
      log_info "✅ Database '$db_trimmed' already exists. Skipping."
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

  # Try to fix permissions if not writable
  if [ ! -w "${AIRFLOW_HOME}/dbt/profiles" ]; then
    log_warn "No write permission to ${AIRFLOW_HOME}/dbt/profiles. Attempting to fix..."
    chmod -R u+w "${AIRFLOW_HOME}/dbt/profiles" 2>/dev/null || true
    chown -R "$(whoami)" "${AIRFLOW_HOME}/dbt/profiles" 2>/dev/null || true
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



prepare_logs_dir() {
  log_info "Ensuring logs directory exists and has correct permissions..."
  mkdir -p "${AIRFLOW_HOME}/logs/scheduler"
  chown -R airflow: "${AIRFLOW_HOME}/logs"
  log_info "Logs directory ready."
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


create_admin_user() {
  if airflow users list | grep -q "${AIRFLOW_ADMIN_USERNAME:-admin}"; then
    log_info "Admin user already exists."
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
  wait_for_postgres
  create_postgres_databases
  generate_dbt_profile
  prepare_logs_dir
  initialize_airflow_db
  create_admin_user
  log_info "Airflow initialization completed successfully. Exiting."
}

if ! main "$@"; then
  log_error "Airflow bootstrap failed"
  exit 1
fi

