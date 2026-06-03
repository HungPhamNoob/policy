#!/bin/bash
# Start Node 3 batch services and run the hourly retraining input job once.
#
# Node 3 responsibilities:
#   - Spark master and worker.
#   - Silver-to-gold batch processing for replay data from 2020 onward.
#   - H2O online retraining against the latest gold retrain dataset.

set -euo pipefail

PROJECT_ROOT="${PROJECT_ROOT:-/opt/traffic}"
ENV_FILE="${ENV_FILE:-${PROJECT_ROOT}/.env.cloud}"
NODE3_COMPOSE_FILE="${PROJECT_ROOT}/deployment/node3-batch/docker-compose.yaml"
NODE3_COMPOSE_DIR="$(dirname "${NODE3_COMPOSE_FILE}")"
NODE3_WAIT_FOR_SILVER_SECONDS="${NODE3_WAIT_FOR_SILVER_SECONDS:-600}"
NODE3_WAIT_FOR_SILVER_INTERVAL_SECONDS="${NODE3_WAIT_FOR_SILVER_INTERVAL_SECONDS:-15}"
NODE3_MIN_SILVER_OBJECTS="${NODE3_MIN_SILVER_OBJECTS:-100}"
NODE3_GCLOUD_STORAGE_TIMEOUT_SECONDS="${NODE3_GCLOUD_STORAGE_TIMEOUT_SECONDS:-300}"
NODE3_RESET_LOCAL_SILVER_SNAPSHOT="${NODE3_RESET_LOCAL_SILVER_SNAPSHOT:-false}"
NODE3_RUN_BATCH_PIPELINE="${NODE3_RUN_BATCH_PIPELINE:-true}"
NODE3_LOG_DIR="${PROJECT_ROOT}/logs"
NODE3_STATE_DIR="${NODE3_LOG_DIR}/retrain_state"
NODE3_LOCK_DIR="${NODE3_LOG_DIR}/.node3-run.lock"
NODE3_LOCK_PID_FILE="${NODE3_LOCK_DIR}/pid"
NODE3_LOCK_OWNED=0
NODE3_LOCK_BUSY_EXIT_CODE="${NODE3_LOCK_BUSY_EXIT_CODE:-75}"
APT_CACHE_UPDATED=0
NODE3_TEMP_DIR="$(mktemp -d /tmp/node3-run-XXXXXX)"
NODE3_SILVER_LS_STDOUT="${NODE3_TEMP_DIR}/silver-ls.txt"
NODE3_SILVER_LS_STDERR="${NODE3_TEMP_DIR}/silver-ls.err"
NODE3_CURRENT_SILVER_DELTA_MANIFEST="${NODE3_STATE_DIR}/current-silver-delta-manifest.txt"
NODE3_CURRENT_SILVER_WATERMARK_FILE="${NODE3_STATE_DIR}/current-silver-watermark.txt"
NODE3_LAST_SUCCESSFUL_SILVER_WATERMARK_FILE="${NODE3_STATE_DIR}/last-successful-silver-watermark.txt"
NODE3_CURRENT_SILVER_PENDING_TOTAL_FILE="${NODE3_STATE_DIR}/current-silver-pending-total.txt"
NODE3_RESET_LOCAL_GOLD_SNAPSHOT="${NODE3_RESET_LOCAL_GOLD_SNAPSHOT:-false}"
NODE3_STOP_SPARK_FOR_H2O="${NODE3_STOP_SPARK_FOR_H2O:-true}"
NODE3_LOCAL_PENDING_DELTA_COUNT=0
NODE3_LOCAL_PENDING_TOTAL_COUNT=0
NODE3_CURRENT_BATCH_ID=""

cleanup_node3_temp() {
  rm -rf "${NODE3_TEMP_DIR}" 2>/dev/null || true
}

release_node3_lock() {
  if [ "${NODE3_LOCK_OWNED}" -eq 1 ]; then
    rm -rf "${NODE3_LOCK_DIR}" 2>/dev/null || sudo rm -rf "${NODE3_LOCK_DIR}" 2>/dev/null || true
  fi
}

acquire_node3_lock() {
  mkdir -p "${NODE3_LOG_DIR}" 2>/dev/null || sudo mkdir -p "${NODE3_LOG_DIR}"
  sudo chown "$(id -u):$(id -g)" "${NODE3_LOG_DIR}" 2>/dev/null || true

  if mkdir "${NODE3_LOCK_DIR}" 2>/dev/null; then
    echo "$$" > "${NODE3_LOCK_PID_FILE}"
    NODE3_LOCK_OWNED=1
    return 0
  fi

  if [ -f "${NODE3_LOCK_PID_FILE}" ]; then
    local existing_pid
    existing_pid="$(cat "${NODE3_LOCK_PID_FILE}" 2>/dev/null || true)"
    if [ -n "${existing_pid}" ] && kill -0 "${existing_pid}" 2>/dev/null; then
      echo "Another Node 3 batch/retraining run is already active (PID ${existing_pid}). Exiting without interrupting it."
      exit "${NODE3_LOCK_BUSY_EXIT_CODE}"
    fi
  fi

  echo "Detected a stale Node 3 lock. Removing it before continuing."
  rm -rf "${NODE3_LOCK_DIR}" 2>/dev/null || sudo rm -rf "${NODE3_LOCK_DIR}"
  if mkdir "${NODE3_LOCK_DIR}" 2>/dev/null; then
    echo "$$" > "${NODE3_LOCK_PID_FILE}"
    NODE3_LOCK_OWNED=1
    return 0
  fi

  echo "Another Node 3 batch/retraining run acquired the lock first. Exiting cleanly."
  exit "${NODE3_LOCK_BUSY_EXIT_CODE}"
}

trap 'cleanup_node3_temp; release_node3_lock' EXIT

echo "Node 3 run script started at $(date -u +%Y-%m-%dT%H:%M:%SZ)"
echo "Project root: ${PROJECT_ROOT}"
echo "Environment file: ${ENV_FILE}"

acquire_node3_lock
echo "Node 3 execution lock acquired by PID $$."
echo "NODE3_RUN_BATCH_PIPELINE: ${NODE3_RUN_BATCH_PIPELINE}"

cd "${PROJECT_ROOT}"
  echo "ERROR: ${ENV_FILE} does not exist."
  exit 1
fi

# RETRAIN_MIN_US_ROWS gate is intentionally disabled.
# The retrain loop now checks for *new* Silver data (timestamp-based rsync diff)
# instead of a minimum row-count threshold. Set RETRAIN_MIN_US_ROWS=0 in .env.cloud
# to skip the row-count gate entirely; set it to a positive integer to re-enable.
RETRAIN_MIN_US_ROWS="${RETRAIN_MIN_US_ROWS:-0}"
RETRAIN_ALLOW_IF_COUNT_UNAVAILABLE="${RETRAIN_ALLOW_IF_COUNT_UNAVAILABLE:-false}"
RETRAIN_ROWCOUNT_ENDPOINT="${RETRAIN_ROWCOUNT_ENDPOINT:-http://${NODE1_INTERNAL_IP:-10.128.0.4}:8000/api/v1/pipeline/replay-health}"
RETRAIN_ROWCOUNT_TABLE="${POSTGRES_US_PREDICTION_TABLE:-${POSTGRES_PREDICTION_TABLE:-traffic_risk_predictions}}"

apt_install_if_missing() {
  if [ "${APT_CACHE_UPDATED}" -eq 0 ]; then
    sudo apt-get update
    APT_CACHE_UPDATED=1
  fi
  sudo DEBIAN_FRONTEND=noninteractive apt-get install -y "$@"
}

ensure_command() {
  local command_name="$1"
  local package_name="$2"
  if command -v "${command_name}" >/dev/null 2>&1; then
    return 0
  fi
  echo "Installing missing dependency for '${command_name}': ${package_name}"
  apt_install_if_missing "${package_name}"
}

ensure_docker_compose() {
  if docker compose version >/dev/null 2>&1; then
    return 0
  fi
  if command -v docker-compose >/dev/null 2>&1; then
    return 0
  fi
  echo "Installing missing dependency for 'docker compose': docker-compose-plugin"
  apt_install_if_missing docker-compose-plugin
}

ensure_gcloud_cli() {
  if command -v gcloud >/dev/null 2>&1; then
    return 0
  fi
  echo "Installing missing dependency for 'gcloud': google-cloud-cli tarball"
  local installer_tgz="/tmp/google-cloud-cli-460.0.0-linux-x86_64.tar.gz"
  curl -fsSL "https://dl.google.com/dl/cloudsdk/channels/rapid/downloads/google-cloud-cli-460.0.0-linux-x86_64.tar.gz" -o "${installer_tgz}"
  rm -rf "${HOME}/google-cloud-sdk"
  tar -xf "${installer_tgz}" -C "${HOME}"
  "${HOME}/google-cloud-sdk/install.sh" --quiet
  export PATH="${PATH}:${HOME}/google-cloud-sdk/bin"
}

compose_cmd() {
  if docker compose version >/dev/null 2>&1; then
    docker compose "$@"
    return
  fi
  if command -v docker-compose >/dev/null 2>&1; then
    docker-compose "$@"
    return
  fi
  ensure_docker_compose
  docker compose "$@"
}

echo "Checking host dependencies required for Node 3 services."
ensure_command docker docker.io
ensure_docker_compose
ensure_command curl curl
ensure_gcloud_cli
ensure_command java openjdk-17-jre-headless
ensure_command python3 python3
if ! python3 -m venv --help >/dev/null 2>&1; then
  echo "Installing missing dependency for Python virtual environments: python3-venv"
  apt_install_if_missing python3-venv
fi

configure_cloud_sdk_runtime() {
  # Some VM images have /home/<user>/.config/gcloud owned by root after startup
  # scripts run with sudo. Use a writable runtime config directory so gcloud
  # and gsutil commands can use the VM service account without touching HOME.
  export CLOUDSDK_CONFIG="${CLOUDSDK_CONFIG:-/tmp/gcloud-config-$(id -u)}"
  mkdir -p "${CLOUDSDK_CONFIG}"
  chmod 700 "${CLOUDSDK_CONFIG}"
  echo "Cloud SDK runtime config: ${CLOUDSDK_CONFIG}"
}

fetch_replay_row_count() {
  python3 - <<'PY'
import json
import os
import sys
import urllib.request

endpoint = os.environ.get("RETRAIN_ROWCOUNT_ENDPOINT", "")
table = os.environ.get("RETRAIN_ROWCOUNT_TABLE", "")
if not endpoint or not table:
    sys.exit(0)

try:
    with urllib.request.urlopen(endpoint, timeout=10) as response:
        payload = json.load(response)
    for source in payload.get("sources", []) or []:
        if str(source.get("table")) == table:
            count = source.get("row_count")
            if count is None:
                break
            print(int(count))
            sys.exit(0)
except Exception:
    pass

sys.exit(0)
PY
}

replay_row_count_below_retrain_gate() {
  if [ "${RETRAIN_MIN_US_ROWS}" -le 0 ]; then
    return 1
  fi

  echo "Checking replay row count before batch/retrain work (min required: ${RETRAIN_MIN_US_ROWS})."
  local replay_row_count
  replay_row_count="$(fetch_replay_row_count || true)"
  replay_row_count="${replay_row_count//$'\r'/}"
  replay_row_count="${replay_row_count//$'\n'/}"
  if [ -z "${replay_row_count}" ]; then
    if [ "${RETRAIN_ALLOW_IF_COUNT_UNAVAILABLE}" = "true" ]; then
      echo "WARNING: Could not determine replay row count from ${RETRAIN_ROWCOUNT_ENDPOINT}. Continuing batch/retrain work."
      return 1
    fi
    echo "WARNING: Could not determine replay row count from ${RETRAIN_ROWCOUNT_ENDPOINT}. Skipping batch/retrain work."
    return 0
  fi

  echo "Replay row count for ${RETRAIN_ROWCOUNT_TABLE}: ${replay_row_count}"
  if [ "${replay_row_count}" -lt "${RETRAIN_MIN_US_ROWS}" ]; then
    echo "Replay rows below threshold (${RETRAIN_MIN_US_ROWS}). Skipping batch/retrain work."
    return 0
  fi
  return 1
}

wait_for_silver_data() {
  # Node 2 writes Silver objects asynchronously. Node 3 must wait until at
  # least one feature object is visible before taking a local snapshot for
  # Spark, otherwise the batch job succeeds with an empty dataset.
  local silver_prefix="${SILVER_FEATURES_PATH%/}/"
  local waited_seconds=0

  echo "Waiting for Silver feature objects before running Spark."
  echo "Silver prefix: ${silver_prefix}"

  while [ "${waited_seconds}" -le "${NODE3_WAIT_FOR_SILVER_SECONDS}" ]; do
    : > "${NODE3_SILVER_LS_STDOUT}"
    : > "${NODE3_SILVER_LS_STDERR}"
    if timeout "${NODE3_GCLOUD_STORAGE_TIMEOUT_SECONDS}" \
      gcloud storage ls "${silver_prefix}" >"${NODE3_SILVER_LS_STDOUT}" 2>"${NODE3_SILVER_LS_STDERR}"; then
      if [ -s "${NODE3_SILVER_LS_STDOUT}" ]; then
        local entry_count
        entry_count="$(wc -l < "${NODE3_SILVER_LS_STDOUT}" | tr -d ' ')"
        echo "Silver data prefix is available with ${entry_count} top-level entries. Sample entries:"
        head -20 "${NODE3_SILVER_LS_STDOUT}"
        return 0
      fi
    else
      local ls_exit_code=$?
      if [ "${ls_exit_code}" -eq 124 ]; then
        echo "Timed out after ${NODE3_GCLOUD_STORAGE_TIMEOUT_SECONDS}s while listing the Silver prefix. Retrying."
      else
        echo "Silver prefix listing failed with exit code ${ls_exit_code}. Retrying."
      fi
      if [ -s "${NODE3_SILVER_LS_STDERR}" ]; then
        echo "Last gcloud storage ls stderr:"
        cat "${NODE3_SILVER_LS_STDERR}"
      fi
    fi

    echo "No Silver data visible yet after ${waited_seconds}s. Waiting ${NODE3_WAIT_FOR_SILVER_INTERVAL_SECONDS}s."
    sleep "${NODE3_WAIT_FOR_SILVER_INTERVAL_SECONDS}"
    waited_seconds=$((waited_seconds + NODE3_WAIT_FOR_SILVER_INTERVAL_SECONDS))
  done

  echo "ERROR: No Silver data found after ${NODE3_WAIT_FOR_SILVER_SECONDS}s."
  echo "ERROR: Start Node 2 first and verify flink-python-job writes to ${SILVER_FEATURES_PATH}."
  if [ -s "${NODE3_SILVER_LS_STDERR}" ]; then
    echo "Last gcloud storage ls error:"
    cat "${NODE3_SILVER_LS_STDERR}"
  fi
  exit 1
}

build_local_silver_delta_manifest() {
  mkdir -p "${NODE3_STATE_DIR}"
  : > "${NODE3_CURRENT_SILVER_DELTA_MANIFEST}"
  rm -f "${NODE3_CURRENT_SILVER_WATERMARK_FILE}"
  rm -f "${NODE3_CURRENT_SILVER_PENDING_TOTAL_FILE}"

  if ! python3 - \
    "${LOCAL_SILVER_FEATURES_PATH}" \
    "${SPARK_LOCAL_SILVER_FEATURES_PATH}" \
    "${NODE3_CURRENT_SILVER_DELTA_MANIFEST}" \
    "${NODE3_CURRENT_SILVER_WATERMARK_FILE}" \
    "${NODE3_LAST_SUCCESSFUL_SILVER_WATERMARK_FILE}" \
    "${NODE3_CURRENT_SILVER_PENDING_TOTAL_FILE}" \
    "${NODE3_DELTA_BATCH_MAX_FILES:-${SPARK_INCREMENTAL_MAX_FILES:-15000}}" <<'PY'; then
import sys
import re
from pathlib import Path

local_root = Path(sys.argv[1])
spark_root = sys.argv[2].rstrip("/")
delta_manifest = Path(sys.argv[3])
watermark_file = Path(sys.argv[4])
last_successful_watermark_file = Path(sys.argv[5])
pending_total_file = Path(sys.argv[6])
batch_limit = int(sys.argv[7])

last_successful_watermark = ""
if last_successful_watermark_file.exists():
    last_successful_watermark = (
        last_successful_watermark_file.read_text(encoding="utf-8").strip()
    )

pattern = re.compile(r"features-(\d{8}T\d+Z)\.jsonl$")
entries = []
max_stamp = ""

if local_root.exists():
    for path in local_root.rglob("features-*.jsonl"):
        match = pattern.match(path.name)
        if not match:
            continue
        stamp = match.group(1)
        if last_successful_watermark and stamp <= last_successful_watermark:
            continue
        relative_path = path.relative_to(local_root).as_posix()
        entries.append((stamp, f"{spark_root}/{relative_path}"))
        if stamp > max_stamp:
            max_stamp = stamp

entries.sort()
total_pending = len(entries)
selected_entries = entries[:batch_limit] if batch_limit > 0 else entries
delta_manifest.write_text(
    "\n".join(local_path for _, local_path in selected_entries)
    + ("\n" if selected_entries else ""),
    encoding="utf-8",
)
pending_total_file.write_text(str(total_pending) + "\n", encoding="utf-8")
if selected_entries:
    watermark_file.write_text(selected_entries[-1][0] + "\n", encoding="utf-8")
print(len(selected_entries))
PY
    echo "WARNING: Failed to compute the local Silver delta manifest."
    return 1
  fi

  NODE3_LOCAL_PENDING_DELTA_COUNT="$(wc -l < "${NODE3_CURRENT_SILVER_DELTA_MANIFEST}" | tr -d ' ')"
  if [ -f "${NODE3_CURRENT_SILVER_PENDING_TOTAL_FILE}" ]; then
    NODE3_LOCAL_PENDING_TOTAL_COUNT="$(tr -d ' \n\r' < "${NODE3_CURRENT_SILVER_PENDING_TOTAL_FILE}")"
  else
    NODE3_LOCAL_PENDING_TOTAL_COUNT="${NODE3_LOCAL_PENDING_DELTA_COUNT}"
  fi
  if [ "${NODE3_LOCAL_PENDING_DELTA_COUNT}" -gt 0 ]; then
    echo "Silver delta manifest selected ${NODE3_LOCAL_PENDING_DELTA_COUNT} files for this retrain tick out of ${NODE3_LOCAL_PENDING_TOTAL_COUNT} pending local files newer than the last successful retrain watermark."
  else
    echo "No local Silver backlog exists beyond the last successful retrain watermark."
  fi
}

silver_snapshot_unchanged_since_last_success() {
  if ! build_local_silver_delta_manifest; then
    echo "Silver delta check failed before sync. Continuing with an incremental rsync so this schedule tick can still recover."
    return 1
  fi

  if [ "${NODE3_LOCAL_PENDING_DELTA_COUNT}" -gt 0 ]; then
    echo "Detected local Silver backlog that has not yet been retrained. Continuing with Spark and H2O."
    return 1
  fi

  echo "No local Silver backlog is waiting. A remote incremental rsync will confirm whether new data arrived for this schedule tick."
  return 0
}

mark_silver_watermark_processed() {
  if [ ! -s "${NODE3_CURRENT_SILVER_WATERMARK_FILE}" ]; then
    echo "No current Silver watermark was recorded for this run. Skipping watermark update."
    return 0
  fi
  mkdir -p "${NODE3_STATE_DIR}"
  cp "${NODE3_CURRENT_SILVER_WATERMARK_FILE}" "${NODE3_LAST_SUCCESSFUL_SILVER_WATERMARK_FILE}"
  date -u +%Y-%m-%dT%H:%M:%SZ > "${NODE3_STATE_DIR}/last-successful-retrain-at.txt"
  echo "Recorded the Silver watermark for the successful retrain."
}

stop_stale_h2o_processes() {
  local stale_h2o_pids
  stale_h2o_pids="$(pgrep -f '/h2o\\.jar' || true)"
  if [ -z "${stale_h2o_pids}" ]; then
    echo "No stale H2O JVMs detected before retraining."
    return 0
  fi

  echo "Stopping stale H2O JVMs before retraining: ${stale_h2o_pids}"
  sudo kill ${stale_h2o_pids} 2>/dev/null || true
  sleep 5

  stale_h2o_pids="$(pgrep -f '/h2o\\.jar' || true)"
  if [ -n "${stale_h2o_pids}" ]; then
    echo "Force-stopping stubborn H2O JVMs: ${stale_h2o_pids}"
    sudo kill -9 ${stale_h2o_pids} 2>/dev/null || true
  fi
}

bootstrap_local_gold_snapshot() {
  mkdir -p "${LOCAL_GOLD_RETRAIN_PARQUET_PATH}" "${LOCAL_GOLD_RETRAIN_CSV_PATH}"

  if [ "${NODE3_RESET_LOCAL_GOLD_SNAPSHOT}" = "true" ]; then
    echo "Resetting the local Gold snapshot because NODE3_RESET_LOCAL_GOLD_SNAPSHOT=true."
    sudo rm -rf "${LOCAL_GOLD_RETRAIN_PARQUET_PATH}" "${LOCAL_GOLD_RETRAIN_CSV_PATH}"
    mkdir -p "${LOCAL_GOLD_RETRAIN_PARQUET_PATH}" "${LOCAL_GOLD_RETRAIN_CSV_PATH}"
  fi

  if find "${LOCAL_GOLD_RETRAIN_PARQUET_PATH}" -type f -name "*.parquet" -print -quit | grep -q .; then
    echo "Local Gold Parquet snapshot already exists. Reusing it for cumulative retraining."
    return 0
  fi

  echo "Bootstrapping the local Gold snapshot from GCS so retraining can use old + new data."
  gcloud storage rsync -r "${GOLD_RETRAIN_PARQUET_PATH}" "${LOCAL_GOLD_RETRAIN_PARQUET_PATH}" || true
  gcloud storage rsync -r "${GOLD_RETRAIN_CSV_PATH}" "${LOCAL_GOLD_RETRAIN_CSV_PATH}" || true

  if find "${LOCAL_GOLD_RETRAIN_PARQUET_PATH}" -type f -name "*.parquet" -print -quit | grep -q .; then
    echo "Local Gold snapshot bootstrapped from GCS."
  else
    echo "No existing Gold Parquet snapshot was found in GCS. The next Spark run will rebuild Gold from the full Silver snapshot."
  fi
}

sync_silver_snapshot() {
  echo "Running an incremental Silver rsync from GCS into the local snapshot."

  if ! gcloud storage rsync -r "${SILVER_FEATURES_PATH}" "${LOCAL_SILVER_FEATURES_PATH}"; then
    echo "WARNING: Silver rsync reported transient copy errors while streaming was active."
    echo "WARNING: Continuing with the files that were copied into the local snapshot."
  fi
}

stop_spark_services_for_h2o() {
  if [ "${NODE3_STOP_SPARK_FOR_H2O}" != "true" ]; then
    echo "Keeping Spark services running during H2O retraining."
    return 0
  fi

  echo "Stopping Spark services to free RAM for H2O retraining."
  compose_cmd \
    --project-directory "${NODE3_COMPOSE_DIR}" \
    --env-file "${ENV_FILE}" \
    -f "${NODE3_COMPOSE_FILE}" \
    stop spark-worker-1 spark-worker-2 spark-worker-3 spark-master || true
}

configure_cloud_sdk_runtime

echo "Starting Spark services..."
echo "Removing stale Node 3 containers from previous Compose project names..."
docker rm -f \
  node3-spark-master \
  node3-spark-worker-1 \
  node3-spark-worker-2 \
  node3-spark-worker-3 \
  2>/dev/null || true

echo "Ensuring the shared Docker network exists before Compose starts."
docker network inspect capstone-net >/dev/null 2>&1 || docker network create capstone-net >/dev/null

compose_cmd \
  --project-directory "${NODE3_COMPOSE_DIR}" \
  --env-file "${ENV_FILE}" \
  -f "${NODE3_COMPOSE_FILE}" \
  up -d

echo "Verifying that the Spark master container is mounted from ${PROJECT_ROOT}."
SPARK_MOUNT_SOURCE="$(docker inspect node3-spark-master --format '{{range .Mounts}}{{if eq .Destination "/opt/traffic"}}{{.Source}}{{end}}{{end}}' 2>/dev/null || true)"
if [ "${SPARK_MOUNT_SOURCE}" != "${PROJECT_ROOT}" ]; then
  echo "ERROR: node3-spark-master is mounted from '${SPARK_MOUNT_SOURCE}', expected '${PROJECT_ROOT}'."
  echo "ERROR: Aborting so the operator does not accidentally run an outdated checkout."
  exit 1
fi

echo "Waiting for Spark master to accept jobs..."
sleep 20

if [ "${NODE3_RUN_BATCH_PIPELINE}" != "true" ]; then
  echo "NODE3_RUN_BATCH_PIPELINE=${NODE3_RUN_BATCH_PIPELINE}. Spark services were started without running Silver -> Gold or H2O retraining."
  exit 0
fi

if replay_row_count_below_retrain_gate; then
  echo "Skipping Silver -> Gold and H2O retraining until enough US replay rows are available."
  compose_cmd \
    --project-directory "${NODE3_COMPOSE_DIR}" \
    --env-file "${ENV_FILE}" \
    -f "${NODE3_COMPOSE_FILE}" \
    ps
  exit 0
fi

LOCAL_CLOUD_DATA_DIR="${LOCAL_CLOUD_DATA_DIR:-${PROJECT_ROOT}/data/cloud}"
LOCAL_SILVER_FEATURES_PATH="${LOCAL_SILVER_FEATURES_PATH:-${LOCAL_CLOUD_DATA_DIR}/silver/flink_features}"
LOCAL_GOLD_RETRAIN_PATH="${LOCAL_GOLD_RETRAIN_PATH:-${LOCAL_CLOUD_DATA_DIR}/gold/features/retrain}"
LOCAL_GOLD_RETRAIN_PARQUET_PATH="${LOCAL_GOLD_RETRAIN_PARQUET_PATH:-${LOCAL_GOLD_RETRAIN_PATH}/parquet}"
LOCAL_GOLD_RETRAIN_CSV_PATH="${LOCAL_GOLD_RETRAIN_CSV_PATH:-${LOCAL_GOLD_RETRAIN_PATH}/csv}"
SPARK_LOCAL_SILVER_FEATURES_PATH="${SPARK_LOCAL_SILVER_FEATURES_PATH:-/data/cloud/silver/flink_features}"
export LOCAL_SILVER_FEATURES_PATH
export SPARK_LOCAL_SILVER_FEATURES_PATH
export NODE3_CURRENT_SILVER_DELTA_MANIFEST
export NODE3_CURRENT_SILVER_WATERMARK_FILE
export NODE3_LAST_SUCCESSFUL_SILVER_WATERMARK_FILE

wait_for_silver_data

mkdir -p "${LOCAL_SILVER_FEATURES_PATH}" "${LOCAL_GOLD_RETRAIN_PATH}"

echo "Syncing Silver data from GCS to local disk for Spark processing."
echo "GCS Silver:   ${SILVER_FEATURES_PATH}"
echo "Local Silver: ${LOCAL_SILVER_FEATURES_PATH}"

# IMPORTANT: Reset the local Silver snapshot FIRST, before computing the delta
# manifest. Otherwise the manifest is calculated from files that are about to be
# deleted, causing Spark to run on an empty local directory.
if [ "${NODE3_RESET_LOCAL_SILVER_SNAPSHOT}" = "true" ]; then
  echo "Resetting the local Silver snapshot before sync because NODE3_RESET_LOCAL_SILVER_SNAPSHOT=true."
  sudo rm -rf "${LOCAL_SILVER_FEATURES_PATH}"
  mkdir -p "${LOCAL_SILVER_FEATURES_PATH}"
  # When resetting, we MUST re-sync from GCS to have data for Spark
  echo "Full re-sync from GCS after local reset..."
  gcloud storage rsync -r "${SILVER_FEATURES_PATH}" "${LOCAL_SILVER_FEATURES_PATH}" || {
    echo "WARNING: Full Silver rsync after reset reported errors. Continuing with what was downloaded."
  }
  # Clear the last successful watermark so ALL data gets processed
  rm -f "${NODE3_LAST_SUCCESSFUL_SILVER_WATERMARK_FILE}"
  echo "Cleared last-successful watermark to ensure full dataset retraining."
else
  echo "Preserving the existing local Silver snapshot so rsync can continue incrementally."
  # Only run incremental delta check when NOT resetting
  if silver_snapshot_unchanged_since_last_success; then
    echo "No unretrained local backlog was found before sync."
  fi
  if [ "${NODE3_LOCAL_PENDING_DELTA_COUNT}" -gt 0 ]; then
    echo "Skipping the remote Silver rsync for this tick because a large unretrained local backlog already exists."
    echo "The next scheduled retrain will pull additional remote Silver files after this backlog is consumed."
  else
    sync_silver_snapshot
  fi
fi

# Always rebuild the delta manifest after syncing (or after reset+full sync)
build_local_silver_delta_manifest

if [ "${NODE3_LOCAL_PENDING_DELTA_COUNT}" -eq 0 ]; then
  echo "No new Silver data since the last successful retrain. Skipping Spark and H2O for this schedule tick."
  compose_cmd \
    --project-directory "${NODE3_COMPOSE_DIR}" \
    --env-file "${ENV_FILE}" \
    -f "${NODE3_COMPOSE_FILE}" \
    ps
  exit 0
fi

if [ -s "${NODE3_CURRENT_SILVER_WATERMARK_FILE}" ]; then
  NODE3_CURRENT_BATCH_ID="$(tr -d ' \n\r' < "${NODE3_CURRENT_SILVER_WATERMARK_FILE}")"
else
  NODE3_CURRENT_BATCH_ID="$(date -u +%Y%m%dT%H%M%SZ)"
fi
echo "Current retrain batch ID: ${NODE3_CURRENT_BATCH_ID}"

LOCAL_SILVER_SAMPLE_FILE="$(find "${LOCAL_SILVER_FEATURES_PATH}" -type f -print -quit)"
if [ -z "${LOCAL_SILVER_SAMPLE_FILE}" ]; then
  echo "ERROR: Local Silver snapshot is empty after rsync."
  echo "ERROR: Check Node 2 Flink logs and GCS permissions before running Node 3."
  exit 1
fi
echo "Local Silver snapshot is ready. Sample file: ${LOCAL_SILVER_SAMPLE_FILE}"

echo "Preparing local Spark output directories with container-writable permissions."
bootstrap_local_gold_snapshot
mkdir -p "${LOCAL_GOLD_RETRAIN_PARQUET_PATH}" "${LOCAL_GOLD_RETRAIN_CSV_PATH}"
sudo chown -R "$(id -u):$(id -g)" "${LOCAL_CLOUD_DATA_DIR}"
sudo chmod -R a+rwX "${LOCAL_CLOUD_DATA_DIR}"

echo "Running Spark silver-to-gold job once. Existing checkpoints/data are preserved."
compose_cmd \
  --project-directory "${NODE3_COMPOSE_DIR}" \
  --env-file "${ENV_FILE}" \
  -f "${NODE3_COMPOSE_FILE}" \
  exec -T --user root spark-master \
  sh -c 'mkdir -p /home/spark/.ivy2/cache /home/spark/.ivy2/jars && chown -R spark:spark /home/spark/.ivy2'

compose_cmd \
  --project-directory "${NODE3_COMPOSE_DIR}" \
  --env-file "${ENV_FILE}" \
  -f "${NODE3_COMPOSE_FILE}" \
  exec -T spark-master \
  env \
  SILVER_FEATURES_PATH=/data/cloud/silver/flink_features \
  SILVER_DELTA_MANIFEST_PATH=/opt/traffic/logs/retrain_state/current-silver-delta-manifest.txt \
  SPARK_BATCH_ID="${NODE3_CURRENT_BATCH_ID}" \
  EXISTING_GOLD_PARQUET_PATH=/data/cloud/gold/features/retrain/parquet \
  GOLD_RETRAIN_PATH=/data/cloud/gold/features/retrain \
  GOLD_RETRAIN_PARQUET_PATH=/data/cloud/gold/features/retrain/parquet \
  GOLD_RETRAIN_CSV_PATH=/data/cloud/gold/features/retrain/csv \
  SPARK_INCREMENTAL_MAX_FILES="${SPARK_INCREMENTAL_MAX_FILES:-10000}" \
  /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --driver-memory "${SPARK_DRIVER_MEMORY:-1536m}" \
  /opt/traffic/processing/spark_batch.py

echo "Syncing Gold Parquet and CSV outputs back to GCS."
gcloud storage rsync -r "${LOCAL_GOLD_RETRAIN_PARQUET_PATH}" "${GOLD_RETRAIN_PARQUET_PATH}"
gcloud storage rsync -r "${LOCAL_GOLD_RETRAIN_CSV_PATH}" "${GOLD_RETRAIN_CSV_PATH}"
stop_spark_services_for_h2o

echo "Running online H2O retraining once from the latest gold data."
SKIP_H2O_RETRAIN=0
if ! find "${LOCAL_GOLD_RETRAIN_CSV_PATH}" -type f -name "*.csv" -size +0 -print -quit | grep -q .; then
  echo "No Gold CSV files found at ${LOCAL_GOLD_RETRAIN_CSV_PATH}. Skipping H2O retraining."
  SKIP_H2O_RETRAIN=1
fi

if [ "${SKIP_H2O_RETRAIN}" -eq 0 ] && [ "${RETRAIN_MIN_US_ROWS}" -gt 0 ]; then
  echo "Checking replay row count before retraining (min required: ${RETRAIN_MIN_US_ROWS})."
  replay_row_count="$(fetch_replay_row_count || true)"
  replay_row_count="${replay_row_count//$'\r'/}"
  replay_row_count="${replay_row_count//$'\n'/}"
  if [ -z "${replay_row_count}" ]; then
    if [ "${RETRAIN_ALLOW_IF_COUNT_UNAVAILABLE}" = "true" ]; then
      echo "WARNING: Could not determine replay row count from ${RETRAIN_ROWCOUNT_ENDPOINT}. Continuing retrain."
    else
      echo "WARNING: Could not determine replay row count from ${RETRAIN_ROWCOUNT_ENDPOINT}. Skipping retrain."
      SKIP_H2O_RETRAIN=1
    fi
  else
    echo "Replay row count for ${RETRAIN_ROWCOUNT_TABLE}: ${replay_row_count}"
    if [ "${replay_row_count}" -lt "${RETRAIN_MIN_US_ROWS}" ]; then
      echo "Replay rows below threshold (${RETRAIN_MIN_US_ROWS}). Skipping H2O retraining."
      SKIP_H2O_RETRAIN=1
    fi
  fi
fi

if [ "${SKIP_H2O_RETRAIN}" -eq 1 ]; then
  echo "Skipping H2O retraining for this run."
  compose_cmd \
    --project-directory "${NODE3_COMPOSE_DIR}" \
    --env-file "${ENV_FILE}" \
    -f "${NODE3_COMPOSE_FILE}" \
    ps
  exit 0
fi

stop_stale_h2o_processes

RETRAINING_VENV="${PROJECT_ROOT}/.venv-node3"
RETRAINING_PYTHON="${RETRAINING_VENV}/bin/python"
if [ ! -x "${RETRAINING_PYTHON}" ]; then
  python3 -m venv "${RETRAINING_VENV}"
fi

MISSING_RETRAIN_SPECS=()
if ! "${RETRAINING_PYTHON}" -c "import h2o" >/dev/null 2>&1; then
  MISSING_RETRAIN_SPECS+=("h2o==3.46.0.6")
fi
if ! "${RETRAINING_PYTHON}" -c "import mlflow" >/dev/null 2>&1; then
  MISSING_RETRAIN_SPECS+=("mlflow==2.12.1")
fi
if ! "${RETRAINING_PYTHON}" -c "import pandas" >/dev/null 2>&1; then
  MISSING_RETRAIN_SPECS+=("pandas==2.2.2")
fi
if ! "${RETRAINING_PYTHON}" -c "import numpy" >/dev/null 2>&1; then
  MISSING_RETRAIN_SPECS+=("numpy==1.26.4")
fi
if ! "${RETRAINING_PYTHON}" -c "import sklearn" >/dev/null 2>&1; then
  MISSING_RETRAIN_SPECS+=("scikit-learn==1.4.2")
fi
if ! "${RETRAINING_PYTHON}" -c "import pyarrow" >/dev/null 2>&1; then
  MISSING_RETRAIN_SPECS+=("pyarrow==11.0.0")
fi
if ! "${RETRAINING_PYTHON}" -c "import dotenv" >/dev/null 2>&1; then
  MISSING_RETRAIN_SPECS+=("python-dotenv==1.0.1")
fi
if ! "${RETRAINING_PYTHON}" -c "import gcsfs" >/dev/null 2>&1; then
  MISSING_RETRAIN_SPECS+=("gcsfs==2024.3.1")
fi
if ! "${RETRAINING_PYTHON}" -c "import google.auth" >/dev/null 2>&1; then
  MISSING_RETRAIN_SPECS+=("google-auth>=2.23.0")
fi
if ! "${RETRAINING_PYTHON}" -c "import google.cloud.storage" >/dev/null 2>&1; then
  MISSING_RETRAIN_SPECS+=("google-cloud-storage>=2.14.0")
fi

if [ "${#MISSING_RETRAIN_SPECS[@]}" -gt 0 ]; then
  echo "Installing missing Python retraining dependencies into ${RETRAINING_VENV}."
  "${RETRAINING_PYTHON}" -m pip install --upgrade pip
  "${RETRAINING_PYTHON}" -m pip install "${MISSING_RETRAIN_SPECS[@]}"
else
  echo "Python retraining dependencies already exist in ${RETRAINING_VENV}."
fi

H2O_MAX_RUNTIME="${NODE3_H2O_MAX_RUNTIME:-${H2O_MAX_RUNTIME:-3600}}" \
  H2O_NTHREADS="${NODE3_H2O_NTHREADS:-${H2O_NTHREADS:-2}}" \
  RETRAIN_DATA_PATH="${LOCAL_GOLD_RETRAIN_CSV_PATH}" \
  "${RETRAINING_PYTHON}" ml/training/h2o_after_2020.py

mark_silver_watermark_processed

echo "Node 3 services:"
compose_cmd \
  --project-directory "${NODE3_COMPOSE_DIR}" \
  --env-file "${ENV_FILE}" \
  -f "${NODE3_COMPOSE_FILE}" \
  ps

echo "Node 3 run script completed at $(date -u +%Y-%m-%dT%H:%M:%SZ)"
