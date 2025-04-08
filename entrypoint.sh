#!/bin/bash
set -euo pipefail

GREEN="\033[0;32m"
YELLOW="\033[1;33m"
RED="\033[0;31m"
RESET="\033[0m"

<<<<<<< HEAD
# Generate DBT profile
echo "🧑 Running as user: $(whoami)"
chmod u+w "${AIRFLOW_HOME}/dbt_profiles"
rm -f "${AIRFLOW_HOME}/dbt_profiles/profiles.yml"
envsubst < "${AIRFLOW_HOME}/dbt_profiles/profiles.template.yml" > "${AIRFLOW_HOME}/dbt_profiles/profiles.yml"
echo "✔️ DBT profile generated."
=======
log_info()  { echo -e "${GREEN}🟢 [INFO] $*${RESET}"; }
log_warn()  { echo -e "${YELLOW}🟡 [WARN] $*${RESET}"; }
log_error() { echo -e "${RED}🔴 [ERROR] $*${RESET}" >&2; }
>>>>>>> 5e147f7 (feat: pyproject.toml, dbt pipeline, docker-compose with redis, worker and  flower, profiles.yml fix)

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

generate_dbt_profile() {
  log_info "Generating DBT profile..."
  mkdir -p "${AIRFLOW_HOME}/dbt_profiles"
  [[ -w "${AIRFLOW_HOME}/dbt_profiles" ]] || chmod u+w "${AIRFLOW_HOME}/dbt_profiles"

  local output_file="${AIRFLOW_HOME}/dbt_profiles/profiles.yml"
  local temp_file
  temp_file=$(mktemp)

  envsubst < "${AIRFLOW_HOME}/dbt_profiles/profiles.template.yml" > "$temp_file"

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
  airflow db migrate > /dev/null 2>&1 && \
    log_info "Airflow metadata DB initialized successfully." || {
      log_error "Failed to initialize Airflow metadata DB."
      exit 1
    }
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
  generate_dbt_profile
  prepare_logs_dir
  initialize_airflow_db
  create_admin_user
  log_info "Airflow initialization completed successfully. Exiting."
}

main "$@" || log_error "Airflow bootstrap failed" && exit 1
