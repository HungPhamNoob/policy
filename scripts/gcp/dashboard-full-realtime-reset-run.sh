#!/bin/bash
# Trigger a realtime-only cloud reset from the FastAPI runtime.
#
# This script is intentionally lightweight on the caller:
#   - it does not package the workspace
#   - it only syncs the current .env.cloud file to the VMs
#   - all heavy reset, streaming, and batch work runs on the VMs

set -euo pipefail

PROJECT_ROOT="${PROJECT_ROOT:-/opt/traffic}"
ENV_FILE="${PIPELINE_RUNTIME_ENV_FILE:-/app/.env.cloud}"
SSH_KEY="${SSH_KEY:-/run/secrets/google_compute_engine}"
SSH_USER="${SSH_USER:-${HUNG_SSH_USER:-runner}}"

NODE1_HOST="${NODE1_SSH_HOST:-${NODE1_INTERNAL_IP:-10.128.0.4}}"
NODE2_HOST="${NODE2_SSH_HOST:-${NODE2_INTERNAL_IP:-10.128.0.9}}"
NODE3_HOST="${NODE3_SSH_HOST:-${NODE3_INTERNAL_IP:-10.128.0.8}}"

echo "============================================================"
echo "Dashboard-triggered realtime cloud reset started at $(date -u +%Y-%m-%dT%H:%M:%SZ)"
echo "Project root: ${PROJECT_ROOT}"
echo "Runtime env file: ${ENV_FILE}"
echo "Targets: node1=${NODE1_HOST}, node2=${NODE2_HOST}, node3=${NODE3_HOST}"
echo "============================================================"

if [ ! -f "${SSH_KEY}" ]; then
  echo "ERROR: SSH key not found at ${SSH_KEY}"
  exit 1
fi

if [ -f "${ENV_FILE}" ]; then
  set -a
  . "${ENV_FILE}"
  set +a
else
  echo "WARNING: ${ENV_FILE} is missing. Continuing with runtime environment variables only."
fi

remote_exec() {
  local host="$1"
  shift
  ssh -i "${SSH_KEY}" \
    -o IdentitiesOnly=yes \
    -o StrictHostKeyChecking=no \
    -o UserKnownHostsFile=/dev/null \
    -o ConnectTimeout=20 \
    "${SSH_USER}@${host}" "$*"
}

sync_runtime_env() {
  local host="$1"
  if [ ! -f "${ENV_FILE}" ]; then
    return 0
  fi
  if ! scp -i "${SSH_KEY}" \
    -o IdentitiesOnly=yes \
    -o StrictHostKeyChecking=no \
    -o UserKnownHostsFile=/dev/null \
    -o ConnectTimeout=20 \
    "${ENV_FILE}" "${SSH_USER}@${host}:/tmp/.env.cloud.codex-reset"; then
    echo "WARNING: Failed to sync runtime env to ${host}. Continuing with the VM's existing ${PROJECT_ROOT}/.env.cloud."
    return 0
  fi

  if ! remote_exec "${host}" "
    set -euo pipefail
    sudo mkdir -p '${PROJECT_ROOT}'
    sudo cp /tmp/.env.cloud.codex-reset '${PROJECT_ROOT}/.env.cloud'
    sudo cp '${PROJECT_ROOT}/.env.cloud' '${PROJECT_ROOT}/.env'
    rm -f /tmp/.env.cloud.codex-reset
  "; then
    echo "WARNING: Failed to install synced runtime env on ${host}. Continuing with the VM's existing ${PROJECT_ROOT}/.env.cloud."
  fi
}

echo "Syncing runtime environment to the VMs."
sync_runtime_env "${NODE1_HOST}"
sync_runtime_env "${NODE2_HOST}"
sync_runtime_env "${NODE3_HOST}"

echo "Resetting Node 1 serving tables and generated cloud data."
remote_exec "${NODE1_HOST}" "
  set -euo pipefail
  cd '${PROJECT_ROOT}'
  if docker ps --format '{{.Names}}' | grep -q '^node1-postgres\$'; then
    docker exec -i node1-postgres psql -U '${POSTGRES_USER:-capstone}' -d '${POSTGRES_DB:-capstone_db}' <<'SQL'
DROP TABLE IF EXISTS ${POSTGRES_US_PREDICTION_TABLE:-traffic_risk_predictions} CASCADE;
DROP TABLE IF EXISTS ${POSTGRES_TOMTOM_TABLE:-traffic_tomtom_incidents} CASCADE;
SQL
  fi

  if command -v gsutil >/dev/null 2>&1; then
    gsutil -m rm -r '${FLINK_CHECKPOINT_DIR:-gs://big-data-group-4-backups/checkpoints/flink}'/** 2>/dev/null || true
    gsutil -m rm -r '${SPARK_CHECKPOINT_DIR:-gs://big-data-group-4-backups/checkpoints/spark}'/** 2>/dev/null || true
    gsutil -m rm -r '${SILVER_FEATURES_PATH:-gs://big-data-group-4-silver/process/flink_features}'/** 2>/dev/null || true
    gsutil -m rm -r '${GOLD_RETRAIN_PATH:-gs://big-data-group-4-gold/features/retrain}'/** 2>/dev/null || true
  else
    echo 'WARNING: gsutil is unavailable on Node 1. Skipping GCS cleanup.'
  fi

  IS_TRAIN_OFFLINE=false bash scripts/gcp/run-node1.sh
"

echo "Restarting Node 2 streaming services from a clean state."
remote_exec "${NODE2_HOST}" "
  set -euo pipefail
  cd '${PROJECT_ROOT}'
  docker compose \
    --project-directory deployment/node2-streaming \
    --env-file .env.cloud \
    -f deployment/node2-streaming/docker-compose.yaml \
    down --volumes --remove-orphans 2>/dev/null || true
  docker volume prune -f 2>/dev/null || true
  bash scripts/gcp/run-node2.sh
"

echo "Restarting Node 3 batch services from a clean state."
remote_exec "${NODE3_HOST}" "
  set -euo pipefail
  cd '${PROJECT_ROOT}'
  docker compose \
    --project-directory deployment/node3-batch \
    --env-file .env.cloud \
    -f deployment/node3-batch/docker-compose.yaml \
    down --volumes --remove-orphans 2>/dev/null || true
  docker volume prune -f 2>/dev/null || true
  bash scripts/gcp/run-node3.sh
"

echo "Dashboard-triggered realtime cloud reset completed at $(date -u +%Y-%m-%dT%H:%M:%SZ)"
