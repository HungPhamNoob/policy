#!/bin/bash

set -euo pipefail

PROJECT_ROOT="${PROJECT_ROOT:-/opt/traffic}"
STATE_DIR="${PROJECT_ROOT}/logs/retrain_state"
STATUS_FILE="${STATE_DIR}/node3-retrain-status.json"
LAUNCH_METADATA_FILE="${STATE_DIR}/node3-retrain-launch.json"
PID_FILE="${STATE_DIR}/node3-retrain.pid"
LOG_FILE="${PROJECT_ROOT}/logs/node3-retrain-runner.log"

mkdir -p "${STATE_DIR}" "$(dirname "${LOG_FILE}")"
sudo chown -R "$(id -u):$(id -g)" "${STATE_DIR}" "$(dirname "${LOG_FILE}")"
sudo chmod u+rwX,g+rwX "${STATE_DIR}" "$(dirname "${LOG_FILE}")"
sudo touch "${LOG_FILE}"
sudo chown "$(id -u):$(id -g)" "${LOG_FILE}"
sudo chmod u+rw,g+rw "${LOG_FILE}"

write_launch_metadata() {
  local pid="$1"
  local launched_at="$2"
  python3 - "${LAUNCH_METADATA_FILE}" "${pid}" "${launched_at}" "${LOG_FILE}" <<'PY'
import json
import sys

payload = {
    "pid": int(sys.argv[2]),
    "launched_at": sys.argv[3],
    "log_file": sys.argv[4],
}
with open(sys.argv[1], "w", encoding="utf-8") as handle:
    json.dump(payload, handle, ensure_ascii=True, indent=2)
PY
}

if [ -f "${PID_FILE}" ]; then
  existing_pid="$(tr -d ' \n\r' < "${PID_FILE}" 2>/dev/null || true)"
  if [ -n "${existing_pid}" ] && kill -0 "${existing_pid}" 2>/dev/null; then
    echo "Node3 retrain worker is already running with PID ${existing_pid}."
    if [ -f "${STATUS_FILE}" ]; then
      cat "${STATUS_FILE}"
    fi
    exit 0
  fi
  rm -f "${PID_FILE}"
fi

launched_at="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
# Keep the retrain worker bounded by the Node3-specific defaults from `.env.cloud`
# so one run can fit inside the 20-minute Airflow cadence instead of forcing a
# one-hour H2O budget on every launch.
nohup bash -lc "cd '${PROJECT_ROOT}' && RETRAIN_MIN_US_ROWS=0 NODE3_LOCK_BUSY_EXIT_CODE=0 bash scripts/gcp/run-node3.sh" \
  >>"${LOG_FILE}" 2>&1 < /dev/null &
worker_pid=$!
echo "${worker_pid}" > "${PID_FILE}"
write_launch_metadata "${worker_pid}" "${launched_at}"

sleep 2
if ! kill -0 "${worker_pid}" 2>/dev/null; then
  echo "Node3 retrain worker exited immediately after launch. Inspect ${LOG_FILE}."
  if [ -f "${STATUS_FILE}" ]; then
    cat "${STATUS_FILE}"
  fi
  exit 1
fi

echo "Node3 retrain worker launched in background with PID ${worker_pid}."
if [ -f "${STATUS_FILE}" ]; then
  cat "${STATUS_FILE}"
fi
