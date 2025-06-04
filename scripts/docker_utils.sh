#!/bin/bash

source /scripts/general_utils.sh
source /scripts/airflow_utils.sh

# Stop and remove Airflow containers to force re-read of updated env
restart_services_env_propagation() {
    local services=($@)

    log_info "🔁 Stopping services..."
    docker compose stop "${services[@]}" >&2

    log_info "🗑️ Removing containers..."
    docker compose rm -f "${services[@]}" >&2

    log_info "🚀 Recreating containers with updated environment..."
    docker compose up -d --force-recreate "${services[@]}" >&2
}

verify_services_env_propagation() {
    local token="$1"
    shift
    local services=("$@")


    # If no services passed, fallback to get_airflow_services()
    if [ ${#services[@]} -eq 0 ]; then
        if declare -f get_airflow_services >/dev/null 2>&1; then
            mapfile -t services < <(get_airflow_services)
        else
            log_error "⚠️  No services provided and get_airflow_services() not defined." 
            return 1
        fi
    fi

    log_info "🔎 Verifying $token* environment variables in containers..."
    local found=0

    for svc in "${services[@]}"; do
    log_info "\n🔍 $svc:"
    if docker exec "$svc" printenv | grep -q "^$token"; then
        docker exec "$svc" printenv | grep "^$token"
        found=1
    else
        log_warn "⚠️  $token* environment variables not found in $svc"
    fi
    done

    return $found
}

refresh_services_env_vars() {
    local token="$1"
    shift
    local services=("$@")
    restart_services_env_propagation "${services[@]}"
    verify_services_env_propagation "$token" "${services[@]}"
}