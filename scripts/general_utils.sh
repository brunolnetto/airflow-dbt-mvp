#!/bin/bash

# Colors for logs
GREEN="\033[0;32m"
YELLOW="\033[1;33m"
RED="\033[0;31m"
RESET="\033[0m"
BLUE='\033[34m'

# Logging functions
log_info()    { echo -e "${BLUE}🔵 [INFO]${RESET} $*"; }
log_success() { echo -e "${GREEN}🟢 [OK]${RESET} $*"; }
log_warn()    { echo -e "${YELLOW}🟡 [WARN]${RESET} $*"; }
log_error()   { echo -e "${RED}🔴 [ERROR]${RESET} $*" >&2; }

trap 'log_error "Unexpected error at line $LINENO. Exiting."' ERR

# Ensure required vars are set
ensure_var_set() {
  : "${1:?$1 is required}"
}

update_env_file() {
  local key="$1"
  local value="$2"
  local env_file=".env"

  # Ensure .env exists
  touch "$env_file"

  # Update or append
  if grep -q "^${key}=" "$env_file"; then
    sed -i.bak "s|^${key}=.*|${key}=${value}|" "$env_file" && rm -f "${env_file}.bak"
  else
    echo "${key}=${value}" >> "$env_file"
  fi
}

generate_random_string() {
  local base64_bytes="$1"   # Amount of entropy, in bytes
  local charset="$2"        # Character set, e.g., A-Z0-9
  local length="$3"         # Final length of the output

  openssl rand -base64 "$base64_bytes" | tr -dc "$charset" | head -c"$length"
}
