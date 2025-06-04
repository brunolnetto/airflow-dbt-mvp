#!/bin/bash
# Robust and modular MinIO bucket setup script
# Dependencies: docker, mc (MinIO client), jq, general_utils.sh

set -euo pipefail

source /scripts/general_utils.sh
source /scripts/airflow_utils.sh
source /scripts/docker_utils.sh

: "${S3_ENDPOINT:=http://minio:9000}"
: "${MC_ALIAS:=admin}"
: "${MC_ALIAS_TMP:=myminio}"

# --- Utility Functions ---

get_minio_container() {
  local container
  container=$(docker ps --filter "name=minio" -q | head -n1)
  if [[ -z "$container" ]]; then
    log_error "No running MinIO container found."
    return 1
  fi
  echo "$container"
}

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

mc_exec() {
  local container="$1"; shift
  retry 5 2 docker exec -i "$container" mc "$@"
}

mc_exec_checked() {
  local container="$1"; shift
  if ! mc_exec "$container" "$@"; then
    log_error "mc command failed: mc $*"
    return 1
  fi
}

generate_access_keys() {
  # Generate random keys with controlled length
  local access_key secret_key
  access_key=$(generate_random_string 16 'A-Z0-9' 16)
  secret_key=$(generate_random_string 32 'A-Za-z0-9' 32)

  log_info "$access_key"
  log_info "$secret_key"

  echo "$access_key" "$secret_key"
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
    mc_exec_checked "$container" mb "$MC_ALIAS/$bucket"
    log_success "Bucket '$bucket' created."
  fi
}

create_user_credentials() {
  local container="$1" access_key="$2" secret_key="$3"
  if (( ${#access_key} < 3 || ${#access_key} > 20 )); then
    log_error "Access key length must be between 3 and 20 characters."
    return 1
  fi

  # Add user before waiting for readiness
  mc_exec_checked "$container" admin user add "$MC_ALIAS" "$access_key" "$secret_key"
}

wait_for_user_creation() {
  local container="$1" username="$2" retries="${3:-5}" delay="${4:-3}"
  local attempt=0

  while (( attempt < retries )); do
    # Use jq to parse JSON user info instead of grep hacks
    if mc_exec "$container" admin user info "$MC_ALIAS" "$username" --json | \
      jq -e '.userStatus == "enabled"' >/dev/null 2>&1; then
      log_info "User '$username' is ready."
      return 0
    fi
    log_info "Waiting for user '$username' to be ready... (attempt $((attempt+1))/$retries)"
    sleep "$delay"
    delay=$(( delay * 2 ))  # exponential backoff
    ((attempt++))
  done

  log_error "User '$username' not available after $retries attempts."
  log_info "Final check output:"
  mc_exec "$container" admin user info "$MC_ALIAS" "$username" || true
  return 1
}

write_policy_file() {
  local container="$1"
  local policy_file="$2"

  # Hardened policy: Public read-only (no Put/Delete)
  docker exec "$container" bash -c "cat > $policy_file <<EOF
{
  \"Version\": \"2012-10-17\",
  \"Statement\": [
    {
      \"Effect\": \"Allow\",
      \"Principal\": {\"AWS\":[\"*\"]},
      \"Action\": [
        \"s3:GetBucketLocation\",
        \"s3:ListBucket\"
      ],
      \"Resource\": [
        \"arn:aws:s3:::*\"
      ]
    },
    {
      \"Effect\": \"Allow\",
      \"Principal\": {\"AWS\":[\"*\"]},
      \"Action\": [
        \"s3:GetObject\"
      ],
      \"Resource\": [
        \"arn:aws:s3:::*/*\"
      ]
    }
  ]
}
EOF"
}

attach_policy() {
  local container="$1"
  local username="$2"
  local policy_name="$3"

  # 1) Attach the policy
  if ! mc_exec_checked "$container" admin policy attach "$MC_ALIAS" --user "$username" "$policy_name"; then
    return 1
  fi

  # 2) Verify via JSON that the user’s policyName matches
  local attached_policies_json
  attached_policies_json=$(
    mc_exec "$container" admin user info "$MC_ALIAS" "$username" --json \
      || echo '{}'
  )

  # 3) Check that policyName == policy_name
  if echo "$attached_policies_json" \
      | jq -e --arg policy "$policy_name" '.policyName == $policy' \
      >/dev/null 2>&1; then
    log_info "Policy '$policy_name' successfully attached to user '$username'."
    return 0
  else
    log_error "Policy '$policy_name' not found on user '$username'."
    return 1
  fi
}


apply_bucket_policy() {
  local container="$1" bucket="$2" access_key="$3" secret_key="$4" policy_name="$5" policy_filename="$6"

  # 1) Write policy inside the container at /tmp/public-read-$bucket.json
  write_policy_file "$container" "$policy_filename"

  # 2) Create the policy from that same file in-container
  mc_exec_checked "$container" admin policy create "$MC_ALIAS" "$policy_name" "$policy_filename"

  # 3) Attach policy to the user
  attach_policy "$container" "$access_key" "$policy_name" || return 1

  # 4) Make the bucket publicly readable
  mc_exec_checked "$container" anonymous set-json "$policy_filename" "$MC_ALIAS/$bucket"

  # 5) Clean up the policy file inside the container (optional)
  docker exec "$container" rm -f "$policy_filename" || true
}

setup_temporary_alias() {
  local container="$1" bucket="$2" access_key="$3" secret_key="$4"
  mc_exec_checked "$container" alias set "$MC_ALIAS_TMP" "$S3_ENDPOINT" "$access_key" "$secret_key"
  if ! mc_exec "$container" ls "$MC_ALIAS_TMP/$bucket" >/dev/null 2>&1; then
    log_warn "Failed to list bucket with new credentials."
  fi
}

refresh_minio_variables_on_airflow() {
  local _access_key _secret_key
  read -r _access_key _secret_key < <(generate_access_keys)
  update_env_file "MINIO_ACCESS_KEY" "$_access_key"
  update_env_file "MINIO_SECRET_KEY" "$_secret_key"s

  local services=()
  if [ ${#services[@]} -eq 0 ]; then
    mapfile -t services < <(get_airflow_services)
  fi

  refresh_services_env_vars "MINIO_" "${services[@]}"s

  # Return values for caller
  echo "$_access_key $_secret_key"
}


setup_minio() {
  local bucket="$1" admin_user="$2" admin_pass="$3"

  if [[ -z "$bucket" || -z "$admin_user" || -z "$admin_pass" ]]; then
    log_error "Usage: setup_minio <bucket> <admin_user> <admin_pass>"
    return 1
  fi

  local container
  container=$(get_minio_container) || return 1
  log_info "Using MinIO container: $container"

  read -r access_key secret_key < <(refresh_minio_variables_on_airflow)

  log_info "${YELLOW}Access Key:${RESET} $access_key"
  log_info "${YELLOW}Secret Key:${RESET} $secret_key"

  ensure_mc_alias "$container" "$admin_user" "$admin_pass"
  ensure_bucket_exists "$container" "$bucket"

  # Create user before waiting for it
  create_user_credentials "$container" "$access_key" "$secret_key"

  # Wait for user to be ready
  if ! wait_for_user_creation "$container" "$access_key"; then
    log_error "User creation verification failed."
    return 1
  fi

  local policy_name="publicread"
  local policy_filename="/tmp/public-read.json"
  apply_bucket_policy "$container" "$bucket" "$access_key" "$secret_key" "$policy_name" "$policy_filename"
  setup_temporary_alias "$container" "$bucket" "$access_key" "$secret_key"

  echo "" >&2
  log_info "[BUCKET $bucket]"
  log_info "${YELLOW}S3 Endpoint:${RESET} $S3_ENDPOINT"
  log_info "${YELLOW}Bucket Name:${RESET} $bucket"
  log_info "${YELLOW}Access Key:${RESET} $access_key"
  log_info "${YELLOW}Secret Key:${RESET} $secret_key"
}

# Export functions if needed
export -f get_minio_container mc_exec mc_exec_checked generate_access_keys write_policy_file \
  ensure_mc_alias ensure_bucket_exists wait_for_user_creation attach_policy apply_bucket_policy \
  create_user_credentials setup_temporary_alias setup_minio
