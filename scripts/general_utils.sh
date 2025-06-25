#!/bin/bash

# Color definitions for log messages
GREEN="\033[0;32m"
YELLOW="\033[1;33m"
RED="\033[0;31m"
BLUE="\033[34m"
RESET="\033[0m"

# Logging functions
log_info()    { echo -e "${BLUE}🔵 [INFO]${RESET} $*" >&2; }
log_success() { echo -e "${GREEN}🟢 [OK]${RESET} $*" >&2; }
log_warn()    { echo -e "${YELLOW}🟡 [WARN]${RESET} $*" >&2; }
log_error()   { echo -e "${RED}🔴 [ERROR]${RESET} $*" >&2; }

# Robust retry function for command execution
retry() {
  local retries=${1:-5}
  local delay=${2:-2}
  shift 2
  local attempt=0
  until "$@"; do
    ((attempt++))
    if (( attempt >= retries )); then
      log_error "Command failed after $attempt attempts: $*"
      return 1
    fi
    log_warn "Command failed, retrying in $delay seconds... ($attempt/$retries)"
    sleep $delay
    delay=$((delay * 2))
  done
}
export -f retry

# Trap to catch unexpected errors
trap 'log_error "Unexpected error at line $LINENO. Exiting."' ERR

# Function to ensure a required variable is set
ensure_var_set() {
  local var_name="$1"
  if [ -z "${!var_name}" ]; then
    log_error "Environment variable '$var_name' is required but not set."
    exit 1
  fi
}

# Function to update or append a key-value pair in the .env file
update_env_file() {
  local key="$1"
  local value="$2"
  local env_file=".env"
  local tmp_file="$(mktemp)"

  touch "$env_file"

  if grep -q "^${key}=" "$env_file"; then
    awk -v k="$key" -v v="$value" -F= '
      $1 == k { print k "=" v; next }
      { print }
    ' "$env_file" > "$tmp_file"
    cp "$tmp_file" "$env_file" && sync && rm -f "$tmp_file"
  else
    echo "${key}=${value}" >> "$env_file"
  fi
}

# Function to generate a random string
generate_random_string() {
  local base64_bytes="$1"   # Amount of entropy, in bytes
  local charset="$2"        # Character set, e.g., A-Z0-9
  local length="$3"         # Final length of the output

  # Generate a base64 string, filter by charset, and truncate to desired length
  openssl rand -base64 "$base64_bytes" | tr -dc "$charset" | head -c"$length"
}

# --- System Resource Checks ---
check_system_resources() {
  local one_meg=1048576
  local mem_available=$(($(getconf _PHYS_PAGES) * $(getconf PAGE_SIZE) / one_meg))
  local cpus_available=$(grep -cE 'cpu[0-9]+' /proc/stat)
  local disk_available=$(df / | tail -1 | awk '{print $4}')
  local warning_resources="false"

  if (( mem_available < 4000 )) ; then
    log_warn "Not enough memory available for Docker. At least 4GB required. You have $(numfmt --to iec $((mem_available * one_meg)))"
    warning_resources="true"
  fi
  if (( cpus_available < 2 )); then
    log_warn "Not enough CPUS available for Docker. At least 2 CPUs recommended. You have ${cpus_available}"
    warning_resources="true"
  fi
  if (( disk_available < one_meg * 10 )); then
    log_warn "Not enough Disk space available for Docker. At least 10 GBs recommended. You have $(numfmt --to iec $((disk_available * 1024 )))"
    warning_resources="true"
  fi
  if [[ ${warning_resources} == "true" ]]; then
    log_warn "You have not enough resources to run Airflow (see above)!"
    log_warn "Please follow the instructions to increase amount of resources available: https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html#before-you-begin"
  fi
}

# Export functions for external use
export -f log_info
export -f log_success
export -f log_warn
export -f log_error
export -f ensure_var_set
export -f update_env_file
export -f generate_random_string
export -f check_system_resources
