#!/bin/bash

# Utilities for MinIO bucket setup and user management

set -euo pipefail

source /scripts/general_utils.sh

# Globals
S3_ENDPOINT="http://minio:9000"
MC_ALIAS="admin"
MC_ALIAS_TMP="myminio"

# Get MinIO container
get_minio_container() {
  docker ps --filter "name=minio" -q | head -n1
}

# Execute mc in container
mc_exec() {
  local container="$1"
  shift
  docker exec -i "$container" mc "$@"
}

# Generate keys
generate_access_keys() {  
  ACCESS_KEY=$(generate_random_string 12 'A-Z0-9' 16)
  SECRET_KEY=$(generate_random_string 32 'A-Za-z0-9' 32)
  export ACCESS_KEY SECRET_KEY

  update_env_file "MINIO_ACCESS_KEY" "$ACCESS_KEY"
  update_env_file "MINIO_SECRET_KEY" "$SECRET_KEY"
}

# Create public read/write policy
write_policy_file() {
  local container="$1"
  local bucket="$2"
  local policy_file="/tmp/public-read-$bucket.json"

  docker exec -i "$container" bash -c "cat > $policy_file <<EOF
{
  \"Version\": \"2012-10-17\",
  \"Statement\": [
    {
      \"Effect\": \"Allow\",
      \"Principal\": \"*\",
      \"Action\": [
        \"s3:GetBucketLocation\",
        \"s3:ListBucket\"
      ],
      \"Resource\": [
        \"arn:aws:s3:::$bucket\"
      ]
    },
    {
      \"Effect\": \"Allow\",
      \"Principal\": \"*\",
      \"Action\": [
        \"s3:GetObject\",
        \"s3:PutObject\",
        \"s3:DeleteObject\"
      ],
      \"Resource\": [
        \"arn:aws:s3:::$bucket/*\"
      ]
    }
  ]
}
EOF"
}

# Ensure mc alias
ensure_mc_alias() {
  local container="$1" user="$2" pass="$3"
  mc_exec "$container" alias set "$MC_ALIAS" "$S3_ENDPOINT" "$user" "$pass"
}

# Ensure bucket exists
ensure_bucket_exists() {
  local container="$1" bucket="$2"

  if mc_exec "$container" ls "$MC_ALIAS/$bucket" >/dev/null 2>&1; then
    log_warn "Bucket '$bucket' already exists."
  else
    if mc_exec "$container" mb "$MC_ALIAS/$bucket"; then
      log_success "Bucket '$bucket' created."
    else
      log_warn "Failed to create bucket '$bucket', continuing..."
    fi
  fi
}

# Apply bucket policy
apply_bucket_policy() {
  local container="$1" bucket="$2"
  local policy_file="/tmp/public-read-$bucket.json"
  local policy_name="publicread-$bucket"

  mc_exec "$container" admin policy create "$MC_ALIAS" "$policy_name" "$policy_file"
  mc_exec "$container" admin policy attach "$MC_ALIAS" "$policy_name" --user "$ACCESS_KEY"
  mc_exec "$container" anonymous set-json "$policy_file" "$MC_ALIAS/$bucket"
}

# Create user
create_user_credentials() {
  local container="$1"
  mc_exec "$container" admin user add "$MC_ALIAS" "$ACCESS_KEY" "$SECRET_KEY"
}

# Set temporary user alias
setup_temporary_alias() {
  local container="$1"
  local bucket="$2"

  mc_exec "$container" alias set "$MC_ALIAS_TMP" "$S3_ENDPOINT" "$ACCESS_KEY" "$SECRET_KEY"
  mc_exec "$container" ls "$MC_ALIAS_TMP/$bucket" || log_warn "Failed to list bucket with new credentials."
}

# Entrypoint function
setup_minio() {
  local bucket="${MINIO_BUCKET:?MINIO_BUCKET not set}"
  local admin_user="${MINIO_ROOT_USER:?MINIO_ROOT_USER not set}"
  local admin_pass="${MINIO_ROOT_PASSWORD:?MINIO_ROOT_PASSWORD not set}"

  local container
  container=$(get_minio_container)

  if [ -z "$container" ]; then
    log_error "MinIO container not found!"
    exit 1
  fi

  log_info "Using MinIO container: $container"

  generate_access_keys
  ensure_mc_alias "$container" "$admin_user" "$admin_pass"
  ensure_bucket_exists "$container" "$bucket"
  write_policy_file "$container" "$bucket"
  apply_bucket_policy "$container" "$bucket"
  create_user_credentials "$container"
  setup_temporary_alias "$container" "$bucket"

  echo ""
  log_info "[BUCKET $bucket]"
  echo -e "${YELLOW}S3 Endpoint:${RESET} $S3_ENDPOINT"
  echo -e "${YELLOW}Bucket Name:${RESET} $bucket"
  echo -e "${YELLOW}Access Key:${RESET} $ACCESS_KEY"
  echo -e "${YELLOW}Secret Key:${RESET} $SECRET_KEY"
}