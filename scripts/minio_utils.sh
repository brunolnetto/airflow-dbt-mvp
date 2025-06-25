#!/bin/bash
# Robust and modular MinIO bucket & user setup script
# Dependencies: mc (MinIO client), jq, general_utils.sh

set -euo pipefail

source /scripts/general_utils.sh

: "${S3_ENDPOINT:=http://minio:9000}"
: "${MC_ALIAS:=admin}"
: "${MC_ALIAS_TMP:=myminio}"
: "${MINIO_BUCKET:?must be set}"
: "${MINIO_ROOT_USER:?must be set}"
: "${MINIO_ROOT_PASSWORD:?must be set}"

##################################
# 1) Wait for MinIO to be alive  #
##################################
wait_for_minio() {
  local health_url="${S3_ENDPOINT%/}/minio/health/live"
  log_info "Waiting for MinIO at $health_url …"
  until curl --silent --fail "$health_url"; do
    log_info "Still waiting for MinIO…"
    sleep 2
  done
  log_info "MinIO is healthy."
}

##################################
# 2) Configure mc alias          #
##################################
ensure_mc_alias() {
  retry 5 2 mc alias set "$MC_ALIAS" "$S3_ENDPOINT" \
    "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD"
}

##################################
# 3) Bucket operations           #
##################################
ensure_bucket_exists() {
  if mc ls "${MC_ALIAS}/${MINIO_BUCKET}" >/dev/null 2>&1; then
    log_warn "Bucket '${MINIO_BUCKET}' already exists."
  else
    log_info "Creating bucket '${MINIO_BUCKET}'…"
    retry 5 2 mc mb "${MC_ALIAS}/${MINIO_BUCKET}"
    log_success "Bucket created."
  fi
}

##################################
# 4) Create admin user           #
##################################
: "${ACCESS_LEN:=16}"
: "${SECRET_LEN:=20}"
generate_access_keys() {
  access_key=$(generate_random_string "$ACCESS_LEN" 'A-Z0-9' "$ACCESS_LEN")
  secret_key=$(generate_random_string "$SECRET_LEN" 'A-Za-z0-9' "$SECRET_LEN")
  echo "$access_key" "$secret_key"
}

create_user_credentials() {
  local access_key="$1" secret_key="$2"

  # Validate MinIO constraints
  if (( ${#access_key} < 3 || ${#access_key} > 20 )); then
    log_error "Access key must be 3–20 chars (was ${#access_key})"; return 1
  fi
  if (( ${#secret_key} < 8 || ${#secret_key} > 40 )); then
    log_error "Secret key must be 8–40 chars (was ${#secret_key})"; return 1
  fi

  log_info "Creating user '$access_key'…"
  retry 5 2 mc admin user add "$MC_ALIAS" "$access_key" "$secret_key"
}

wait_for_user_creation() {
  local user="$1"
  log_info "Waiting for user '$user' to be enabled…"
  retry 5 3 mc admin user info "$MC_ALIAS" "$user" --json | \
    jq -e '.userStatus=="enabled"' >/dev/null
  log_info "User '$user' is ready."
}

##################################
# 5) Policy management           #
##################################
write_policy_file() {
  cat > "$1" <<'EOF'
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {"AWS":["*"]},
      "Action": ["s3:GetBucketLocation","s3:ListBucket"],
      "Resource": ["arn:aws:s3:::*"]
    },
    {
      "Effect": "Allow",
      "Principal": {"AWS":["*"]},
      "Action": ["s3:GetObject"],
      "Resource": ["arn:aws:s3:::*/*"]
    }
  ]
}
EOF
}

apply_bucket_policy() {
  local user="$1" policy_name="publicread" policy_file="/tmp/public-read.json"
  write_policy_file "$policy_file"

  log_info "Uploading policy '$policy_name'…"
  retry 5 2 mc admin policy create "$MC_ALIAS" "$policy_name" "$policy_file"

  log_info "Attaching policy to user '$user'…"
  retry 5 2 mc admin policy attach "$MC_ALIAS" --user "$user" "$policy_name"

  log_info "Making bucket publicly readable…"
  retry 5 2 mc anonymous set-json "$policy_file" "${MC_ALIAS}/${MINIO_BUCKET}"

  rm -f "$policy_file"
  log_success "Policy applied."
}

##################################
# 6) Test credentials & alias    #
##################################
setup_temporary_alias() {
  local user="$1" secret="$2"
  log_info "Testing new credentials…"
  retry 3 2 mc alias set "$MC_ALIAS_TMP" "$S3_ENDPOINT" "$user" "$secret"
  if ! mc ls "${MC_ALIAS_TMP}/${MINIO_BUCKET}" >/dev/null; then
    log_error "New credentials failed to list bucket."
    return 1
  fi
  log_success "New credentials verified."
  mc alias remove "$MC_ALIAS_TMP" || true
}

##################################
# 7) Main orchestration          #
##################################
setup_minio() {
  wait_for_minio
  ensure_mc_alias
  ensure_bucket_exists

  # Generate user creds
  read -r access_key secret_key < <(generate_access_keys)
  log_info "Access Key: $access_key"
  log_info "Secret Key: $secret_key"

  create_user_credentials "$access_key" "$secret_key"
  wait_for_user_creation "$access_key"

  apply_bucket_policy "$access_key"
  setup_temporary_alias "$access_key" "$secret_key"

  echo
  log_info "[MINIO CONFIG]"
  log_info "Endpoint: $S3_ENDPOINT"
  log_info "Bucket:   $MINIO_BUCKET"
  log_info "Access Key: $access_key"
  log_info "Secret Key: $secret_key"
}

# Export functions if needed
export -f setup_minio
