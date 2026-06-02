#!/bin/bash
# Reset and run the complete cloud pipeline from the beginning.
#
# This entrypoint is intentionally cloud-first:
#   - the local laptop only packages the current workspace and opens SSH/SCP sessions
#   - the VMs run all heavy services, training jobs, replay jobs, and validation steps
#   - the run can start from a clean realtime state without requiring a git push first

set -euo pipefail

PROJECT_ID="${GCP_PROJECT_ID:-big-data-group-4}"
ZONE="${GCP_ZONE:-us-central1-a}"
NODE1="${NODE1:-node1-control}"
NODE2="${NODE2:-node2-streaming}"
NODE3="${NODE3:-node3-batch}"
BRANCH="${BRANCH:-main}"
PROJECT_ROOT="${PROJECT_ROOT:-/opt/traffic}"
RUN_ID="${RUN_ID:-$(date -u +%Y%m%dT%H%M%SZ)}"
LOG_DIR="${LOG_DIR:-logs/cloud_runs/${RUN_ID}}"
RUNTIME_ENV_FILE="${LOG_DIR}/.env.cloud.${RUN_ID}"
WORKSPACE_ARCHIVE="${LOG_DIR}/traffic-workspace-${RUN_ID}.tar.gz"
RUN_OFFLINE_TRAINING="${RUN_OFFLINE_TRAINING:-true}"
STREAM_MAX_RECORDS="${STREAM_MAX_RECORDS:-0}"
STREAM_THROTTLE_SECONDS="${STREAM_THROTTLE_SECONDS:-0.0}"
NODE3_WAIT_FOR_SILVER_SECONDS="${NODE3_WAIT_FOR_SILVER_SECONDS:-900}"
AIRFLOW_PAUSE_AUTOMATION_DURING_FULL_RUN="${AIRFLOW_PAUSE_AUTOMATION_DURING_FULL_RUN:-true}"
AIRFLOW_RESUME_AUTOMATION_AFTER_FULL_RUN="${AIRFLOW_RESUME_AUTOMATION_AFTER_FULL_RUN:-false}"
CHECK_SERVICES_AFTER_RUN="${CHECK_SERVICES_AFTER_RUN:-true}"
REALTIME_OBSERVE_SECONDS="${REALTIME_OBSERVE_SECONDS:-75}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WORKSPACE_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

mkdir -p "${LOG_DIR}"

cleanup() {
  rm -f "${WORKSPACE_ARCHIVE}"
}
trap cleanup EXIT

SSH_KEY="${SSH_KEY:-~/.ssh/google_compute_engine}"
SSH_USER="${SSH_USER:-${HUNG_SSH_USER:-$(whoami)}}"

get_node_ip() {
  local node="$1"
  gcloud compute instances describe "${node}" \
    --project="${PROJECT_ID}" \
    --zone="${ZONE}" \
    --format='get(networkInterfaces[0].accessConfigs[0].natIP)' \
    --quiet
}

ssh_cmd() {
  local node="$1"
  shift
  local node_ip
  node_ip=$(get_node_ip "${node}")
  ssh -i "${SSH_KEY}" \
    -o IdentitiesOnly=yes \
    -o StrictHostKeyChecking=no \
    -o UserKnownHostsFile=/dev/null \
    -o ConnectTimeout=15 \
    "${SSH_USER}@${node_ip}" "$*"
}

scp_to_node() {
  local source_path="$1"
  local node="$2"
  local target_path="$3"
  local node_ip
  node_ip=$(get_node_ip "${node}")
  scp -i "${SSH_KEY}" \
    -o IdentitiesOnly=yes \
    -o StrictHostKeyChecking=no \
    -o UserKnownHostsFile=/dev/null \
    -o ConnectTimeout=15 \
    "${source_path}" "${SSH_USER}@${node_ip}:${target_path}"
}

prepare_runtime_env() {
  echo "Preparing run-specific cloud environment file."
  cp "${WORKSPACE_ROOT}/.env.cloud" "${RUNTIME_ENV_FILE}"
  {
    printf '\n# Run-specific clean prefixes for %s\n' "${RUN_ID}"
    printf 'FLINK_CHECKPOINT_DIR=gs://big-data-group-4-backups/checkpoints/flink/runs/%s\n' "${RUN_ID}"
    printf 'SPARK_CHECKPOINT_DIR=gs://big-data-group-4-backups/checkpoints/spark/runs/%s\n' "${RUN_ID}"
    printf 'SILVER_FEATURES_PATH=gs://big-data-group-4-silver/process/flink_features/runs/%s\n' "${RUN_ID}"
    printf 'GOLD_RETRAIN_PATH=gs://big-data-group-4-gold/features/retrain/runs/%s\n' "${RUN_ID}"
    printf 'GOLD_RETRAIN_PARQUET_PATH=gs://big-data-group-4-gold/features/retrain/runs/%s/parquet\n' "${RUN_ID}"
    printf 'GOLD_RETRAIN_CSV_PATH=gs://big-data-group-4-gold/features/retrain/runs/%s/csv\n' "${RUN_ID}"
    printf 'STREAM_MAX_RECORDS=%s\n' "${STREAM_MAX_RECORDS}"
    printf 'STREAM_THROTTLE_SECONDS=%s\n' "${STREAM_THROTTLE_SECONDS}"
  } >> "${RUNTIME_ENV_FILE}"
}

prepare_workspace_archive() {
  echo "Packaging the current local workspace for VM deployment."
  tar \
    --exclude=".git" \
    --exclude="dashboard/frontend/.next" \
    --exclude="dashboard/frontend/node_modules" \
    --exclude="dashboard/frontend/baseline_1" \
    --exclude="dashboard/frontend/baseline_2" \
    --exclude="logs" \
    --exclude="data/raw" \
    --exclude="data/process" \
    --exclude="data/split" \
    --exclude="data/simulation" \
    --exclude="data/cloud" \
    --exclude="ml/mlruns" \
    --exclude=".venv" \
    --exclude=".venv-*" \
    --exclude=".venv-node1" \
    --exclude=".venv-node3" \
    --exclude="__pycache__" \
    --exclude=".pytest_cache" \
    --exclude="road_accident_risk_platform.egg-info" \
    --exclude="vendor" \
    -czf "${WORKSPACE_ARCHIVE}" \
    -C "${WORKSPACE_ROOT}" .
}

sync_workspace_to_node() {
  local node="$1"
  local archive_name="traffic-workspace-${RUN_ID}.tar.gz"

  echo "Syncing the current workspace snapshot to ${node}."
  scp_to_node "${WORKSPACE_ARCHIVE}" "${node}" "/tmp/${archive_name}"
  scp_to_node "${RUNTIME_ENV_FILE}" "${node}" "/tmp/.env.cloud.${RUN_ID}"

  ssh_cmd "${node}" "
    set -euo pipefail
    if [ -d ${PROJECT_ROOT} ]; then
      for compose_file in \$(find ${PROJECT_ROOT}/deployment -mindepth 2 -maxdepth 2 -name docker-compose.yaml 2>/dev/null | sort); do
        docker compose --env-file ${PROJECT_ROOT}/.env.cloud -f \${compose_file} down --remove-orphans >/dev/null 2>&1 || true
      done
    fi
    sudo rm -rf ${PROJECT_ROOT}
    sudo install -d -m 0755 -o \$(id -un) -g \$(id -gn) ${PROJECT_ROOT}
    tar --no-same-owner --no-same-permissions -xzf /tmp/${archive_name} -C ${PROJECT_ROOT}
    mkdir -p \
      ${PROJECT_ROOT}/logs \
      ${PROJECT_ROOT}/data/cloud \
      ${PROJECT_ROOT}/data/process \
      ${PROJECT_ROOT}/data/raw \
      ${PROJECT_ROOT}/data/split
    cp /tmp/.env.cloud.${RUN_ID} ${PROJECT_ROOT}/.env.cloud
    cp ${PROJECT_ROOT}/.env.cloud ${PROJECT_ROOT}/.env
    rm -f /tmp/${archive_name} /tmp/.env.cloud.${RUN_ID}
  "
}

echo "Run ID: ${RUN_ID}"
echo "Project: ${PROJECT_ID}, zone: ${ZONE}, branch: ${BRANCH}"
echo "Workspace: ${WORKSPACE_ROOT}"
echo "Logs: ${LOG_DIR}"

prepare_runtime_env
prepare_workspace_archive

sync_workspace_to_node "${NODE1}" | tee "${LOG_DIR}/01-sync-node1.log"
sync_workspace_to_node "${NODE2}" | tee "${LOG_DIR}/02-sync-node2.log"
sync_workspace_to_node "${NODE3}" | tee "${LOG_DIR}/03-sync-node3.log"

echo "Resetting Node 1 PostgreSQL tables and run-specific GCS prefixes."
ssh_cmd "${NODE1}" "
  cd ${PROJECT_ROOT}
  mkdir -p ${PROJECT_ROOT}/logs ${PROJECT_ROOT}/data/cloud ${PROJECT_ROOT}/data/process || sudo mkdir -p ${PROJECT_ROOT}/logs ${PROJECT_ROOT}/data/cloud ${PROJECT_ROOT}/data/process
  RESET_LOCAL_COMPOSE=false RESET_POSTGRES=true RESET_GCS=true bash scripts/gcp/reset-realtime.sh
" | tee "${LOG_DIR}/04-reset-node1.log"

echo "Resetting Node 2 Kafka and Flink state."
ssh_cmd "${NODE2}" "
  cd ${PROJECT_ROOT}
  mkdir -p ${PROJECT_ROOT}/logs ${PROJECT_ROOT}/data/cloud ${PROJECT_ROOT}/data/process || sudo mkdir -p ${PROJECT_ROOT}/logs ${PROJECT_ROOT}/data/cloud ${PROJECT_ROOT}/data/process
  RESET_LOCAL_COMPOSE=true RESET_POSTGRES=false RESET_GCS=false bash scripts/gcp/reset-realtime.sh
" | tee "${LOG_DIR}/05-reset-node2.log"

echo "Resetting Node 3 Spark local state."
ssh_cmd "${NODE3}" "
  cd ${PROJECT_ROOT}
  mkdir -p ${PROJECT_ROOT}/logs ${PROJECT_ROOT}/data/cloud ${PROJECT_ROOT}/data/process || sudo mkdir -p ${PROJECT_ROOT}/logs ${PROJECT_ROOT}/data/cloud ${PROJECT_ROOT}/data/process
  RESET_LOCAL_COMPOSE=true RESET_POSTGRES=false RESET_GCS=false bash scripts/gcp/reset-realtime.sh
" | tee "${LOG_DIR}/06-reset-node3.log"

echo "Starting Node 1 control-plane services and offline bootstrap."
ssh_cmd "${NODE1}" "
  cd ${PROJECT_ROOT}
  IS_TRAIN_OFFLINE=${RUN_OFFLINE_TRAINING} bash scripts/gcp/run-node1.sh
" | tee "${LOG_DIR}/07-run-node1.log"

if [ "${AIRFLOW_PAUSE_AUTOMATION_DURING_FULL_RUN}" = "true" ]; then
  echo "Pausing Airflow automation DAGs during the manual full-cloud run."
  ssh_cmd "${NODE1}" "
    docker exec node1-airflow airflow dags pause streaming_health_check || true
    docker exec node1-airflow airflow dags pause model_retrain_hourly || true
  " | tee "${LOG_DIR}/08-pause-airflow-dags.log"
fi

echo "Starting Node 2 realtime replay and TomTom streaming."
ssh_cmd "${NODE2}" "
  cd ${PROJECT_ROOT}
  STREAM_MAX_RECORDS=${STREAM_MAX_RECORDS} STREAM_THROTTLE_SECONDS=${STREAM_THROTTLE_SECONDS} bash scripts/gcp/run-node2.sh
" | tee "${LOG_DIR}/09-run-node2.log"

echo "Starting Node 3 Spark and H2O retraining from the generated Silver data."
ssh_cmd "${NODE3}" "
  cd ${PROJECT_ROOT}
  NODE3_WAIT_FOR_SILVER_SECONDS=${NODE3_WAIT_FOR_SILVER_SECONDS} bash scripts/gcp/run-node3.sh
" | tee "${LOG_DIR}/10-run-node3.log"

echo "Collecting measured cloud evidence."
RUN_ID="${RUN_ID}" LOG_DIR="logs/cloud_runs" \
  bash "${SCRIPT_DIR}/collect-cloud-metrics.sh" | tee "${LOG_DIR}/11-metrics.log"

if [ "${CHECK_SERVICES_AFTER_RUN}" = "true" ]; then
  echo "Collecting request/response checks for public and internal services."
  RUN_ID="${RUN_ID}" LOG_DIR="logs/cloud_runs" REALTIME_OBSERVE_SECONDS="${REALTIME_OBSERVE_SECONDS}" \
    bash "${SCRIPT_DIR}/check-cloud-services.sh" | tee "${LOG_DIR}/12-service-checks.log"
fi

if [ "${AIRFLOW_PAUSE_AUTOMATION_DURING_FULL_RUN}" = "true" ] && [ "${AIRFLOW_RESUME_AUTOMATION_AFTER_FULL_RUN}" = "true" ]; then
  echo "Resuming Airflow automation DAGs after the manual full-cloud run."
  ssh_cmd "${NODE1}" "
    docker exec node1-airflow airflow dags unpause streaming_health_check || true
    docker exec node1-airflow airflow dags unpause model_retrain_hourly || true
  " | tee "${LOG_DIR}/13-resume-airflow-dags.log"
fi

echo "Full cloud run completed. Evidence is under ${LOG_DIR}."
