#!/bin/bash

# Testable and modular MinIO bucket setup script
# Dependencies: docker, mc (MinIO client), functions from general_utils.sh

source /scripts/general_utils.sh

# Defaults (can be overridden by env or arguments)
: "${S3_ENDPOINT:=http://minio:9000}"
: "${MC_ALIAS:=admin}"
: "${MC_ALIAS_TMP:=myminio}"

# --- Utility Functions ---
get_minio_container() {
  docker ps --filter "name=minio" -q | head -n1
}

mc_exec() {
  local container="$1"; shift
  docker exec -i "$container" mc "$@"
}

mc_exec_checked() {
  local container="$1"; shift
  if ! docker exec -i "$container" mc "$@"; then
    log_error "mc command failed: mc $*"
    return 1
  fi
}

generate_access_keys() {
  local access_key secret_key
  access_key=$(generate_random_string 12 'A-Z0-9' 16)
  secret_key=$(generate_random_string 24 'A-Za-z0-9' 32)

  echo "$access_key" "$secret_key"
}

write_policy_file() {
  local container="$1" bucket="$2"
  local policy_file="/tmp/public-read-$bucket.json"

  docker exec -i "$container" bash -c "cat > $policy_file <<EOF
{
  \"Version\": \"2012-10-17\",
  \"Statement\": [
    {
      \"Effect\": \"Allow\",
      \"Principal\": \"*\",
      \"Action\": [\"s3:GetBucketLocation\", \"s3:ListBucket\"],
      \"Resource\": [\"arn:aws:s3:::$bucket\"]
    },
    {
      \"Effect\": \"Allow\",
      \"Principal\": \"*\",
      \"Action\": [\"s3:GetObject\", \"s3:PutObject\", \"s3:DeleteObject\"],
      \"Resource\": [\"arn:aws:s3:::$bucket/*\"]
    }
  ]
}
EOF"
}

ensure_mc_alias() {
  local container="$1" user="$2" pass="$3"
  mc_exec_checked "$container" alias set "$MC_ALIAS" "$S3_ENDPOINT" "$user" "$pass"
}

ensure_bucket_exists() {
  local container="$1" bucket="$2"
  if mc_exec "$container" ls "$MC_ALIAS/$bucket" >/dev/null 2>&1; then
    log_warn "Bucket '$bucket' already exists."
  else
    mc_exec_checked "$container" mb "$MC_ALIAS/$bucket" && log_success "Bucket '$bucket' created."
  fi
}

check_user_exists() {
  mc_exec alias list | grep -q "$1"
}

wait_for_user_creation() {
  local username="$1"
  local retries=5
  local delay=1

  for ((i=0; i<retries; i++)); do
    if mc_exec admin user info "$MC_ALIAS" "$username" &>/dev/null; then
      return 0
    fi
    log_info "Waiting for user '$username' to be ready... (${i}/${retries})"
    sleep "$delay"
  done

  log_error "User '$username' not available after ${retries} attempts."
  return 1
}

attach_policy() {
  local username="$1"
  local policy_name="$2"

  if ! mc_exec_checked "$container" admin policy attach "$MC_ALIAS" --user "$username" "$policy_name"; then
    return 1
  fi

  local attached_policies
  attached_policies=$(mc_exec "$container" admin user info "$MC_ALIAS" "$username" | grep -A 5 'Policies' | grep -o '"[^"]\+"' | tr -d '"')

  if echo "$attached_policies" | grep -Fxq "$policy_name"; then
    log_info "Policy '$policy_name' successfully attached to user '$username'."
    return 0
  else
    log_error "Policy '$policy_name' not found in user's attached policies."
    return 1
  fi
}

apply_bucket_policy() {
  local container="$1" bucket="$2" access_key="$3"
  local policy_file="/tmp/public-read-$bucket.json"
  local policy_name="publicread-$bucket"

  mc_exec_checked "$container" admin policy create "$MC_ALIAS" "$policy_name" "$policy_file"

  if ! wait_for_user_creation "$access_key"; then
    return 1
  fi

  attach_policy "$access_key" "$policy_name" || return 1
  mc_exec_checked "$container" anonymous set-json "$policy_file" "$MC_ALIAS/$bucket"
}

create_user_credentials() {
  local container="$1" access_key="$2" secret_key="$3"
  if [[ ${#access_key} -lt 3 || ${#access_key} -gt 20 ]]; then
    log_error "Access key length must be between 3 and 20 characters."
    return 1
  fi
  mc_exec_checked "$container" admin user add "$MC_ALIAS" "$access_key" "$secret_key"
}

setup_temporary_alias() {
  local container="$1" bucket="$2" access_key="$3" secret_key="$4"
  mc_exec_checked "$container" alias set "$MC_ALIAS_TMP" "$S3_ENDPOINT" "$access_key" "$secret_key"
  mc_exec "$container" ls "$MC_ALIAS_TMP/$bucket" || log_warn "Failed to list bucket with new credentials."
}

# --- Entrypoint ---
setup_minio() {
  local bucket="$1" admin_user="$2" admin_pass="$3"

  if [[ -z "$bucket" || -z "$admin_user" || -z "$admin_pass" ]]; then
    log_error "Usage: setup_minio <bucket> <admin_user> <admin_pass>"
    return 1
  fi

  local container
  container=$(get_minio_container)
  if [ -z "$container" ]; then
    log_error "MinIO container not found!"
    return 1
  fi

  log_info "Using MinIO container: $container"

  read -r access_key secret_key < <(generate_access_keys)
  update_env_file "MINIO_ACCESS_KEY" "$access_key"
  update_env_file "MINIO_SECRET_KEY" "$secret_key"

  ensure_mc_alias "$container" "$admin_user" "$admin_pass"
  ensure_bucket_exists "$container" "$bucket"
  write_policy_file "$container" "$bucket"
  create_user_credentials "$container" "$access_key" "$secret_key"
  apply_bucket_policy "$container" "$bucket" "$access_key"
  setup_temporary_alias "$container" "$bucket" "$access_key" "$secret_key"

  echo ""
  log_info "[BUCKET $bucket]"
  echo -e "${YELLOW}S3 Endpoint:${RESET} $S3_ENDPOINT"
  echo -e "${YELLOW}Bucket Name:${RESET} $bucket"
  echo -e "${YELLOW}Access Key:${RESET} $access_key"
  echo -e "${YELLOW}Secret Key:${RESET} $secret_key"
}

export -f get_minio_container
export -f mc_exec
export -f mc_exec_checked
export -f generate_access_keys
export -f write_policy_file
export -f ensure_mc_alias
export -f ensure_bucket_exists
export -f apply_bucket_policy
export -f create_user_credentials
export -f setup_temporary_alias
export -f setup_minio
