#!/bin/bash

source /scripts/general_utils.sh

airflow_db_clean(){
  log_info "Cleaning up Airflow database..." 
  
  airflow db clean
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

# --- AIRFLOW_UID .env Substitution ---
ensure_airflow_uid_in_env_file() {
  local uid_value
  uid_value=$(id -u)
  update_env_file "AIRFLOW_UID" "$uid_value"
  log_info "Ensured AIRFLOW_UID=$uid_value is set in .env file."
}

# --- AIRFLOW_UID Check ---
check_airflow_uid() {
  ensure_airflow_uid_in_env_file
  if [[ -z "${AIRFLOW_UID}" ]]; then
    log_warn "AIRFLOW_UID not set!"
    log_warn "If you are on Linux, you SHOULD set the AIRFLOW_UID environment variable, otherwise files will be owned by root."
    log_warn "For other operating systems you can get rid of the warning with a manually created .env file:"
    log_warn "    See: https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html#setting-the-right-airflow-user"
    export AIRFLOW_UID=$(id -u)
    log_info "Set AIRFLOW_UID to $(id -u)"
  fi
}

# --- Directory Creation ---
ensure_airflow_dirs() {
  log_info "Ensuring Airflow directories exist: /opt/airflow/{logs,dags,plugins,config}"
  mkdir -v -p /opt/airflow/{logs,dags,plugins,config}
}

# --- Ownership and Permissions ---
change_airflow_ownership() {
  log_info "Changing ownership of /opt/airflow and subdirectories to ${AIRFLOW_UID}:0"
  chown -R "${AIRFLOW_UID}:0" /opt/airflow/
  chown -v -R "${AIRFLOW_UID}:0" /opt/airflow/{logs,dags,plugins,config}
}

# --- Volume Listing ---
list_airflow_volumes() {
  log_info "Files in shared volumes:"
  ls -la /opt/airflow/{logs,dags,plugins,config}
}

create_airflow_admin_user() {
  # Check if 'users' command is available in Airflow CLI
  if airflow users --help > /dev/null 2>&1; then
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
  else
    log_warn "'airflow users create' command not available in this Airflow version. Skipping admin user creation."
  fi
}

# List of Airflow services
get_airflow_services() {
  echo "airflow-webserver airflow-worker-1 airflow-worker-2 airflow-scheduler airflow-triggerer"
}

# --- Airflow Version and Config ---
airflow_version_and_config() {
  log_info "Airflow version:"
  airflow version
  log_info "Running 'airflow config list' to create default config file if missing."
  airflow config list >/dev/null
}

export -f airflow_version_and_config

setup_airflow(){
  log_info "Starting check_airflow_uid"
  check_airflow_uid
  log_info "Finished check_airflow_uid"

  log_info "Starting check_system_resources"
  check_system_resources
  log_info "Finished check_system_resources"

  log_info "Starting ensure_airflow_dirs"
  ensure_airflow_dirs
  log_info "Finished ensure_airflow_dirs"

  log_info "Starting list_airflow_volumes (1)"
  list_airflow_volumes
  log_info "Finished list_airflow_volumes (1)"

  log_info "Starting airflow_version_and_config"
  airflow_version_and_config
  log_info "Finished airflow_version_and_config"

  log_info "Starting change_airflow_ownership"
  change_airflow_ownership
  log_info "Finished change_airflow_ownership"

  log_info "Starting list_airflow_volumes (2)"
  list_airflow_volumes
  log_info "Finished list_airflow_volumes (2)"
  initialize_airflow_db
  create_airflow_admin_user
}
