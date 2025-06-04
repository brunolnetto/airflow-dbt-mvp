#!/bin/bash

source /scripts/general_utils.sh

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

setup_postgres(){
  wait_for_postgres
  create_postgres_databases
}