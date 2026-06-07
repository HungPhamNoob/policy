#!/bin/bash

set -euo pipefail

PROJECT_ROOT="${PROJECT_ROOT:-/opt/traffic}"
STATE_DIR="${PROJECT_ROOT}/logs/retrain_state"
STATUS_FILE="${STATE_DIR}/node3-retrain-status.json"
LAUNCH_METADATA_FILE="${STATE_DIR}/node3-retrain-launch.json"
PID_FILE="${STATE_DIR}/node3-retrain.pid"
LOCK_PID_FILE="${PROJECT_ROOT}/logs/.node3-run.lock/pid"
LOG_FILE="${PROJECT_ROOT}/logs/node3-retrain-runner.log"
STALE_SECONDS="${NODE3_RETRAIN_STALE_SECONDS:-4200}"

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

status_payload_age_seconds() {
  local payload_file="$1"
  if [ ! -f "${payload_file}" ]; then
    return 1
  fi
  python3 - "${payload_file}" <<'PY'
import json
import sys
from datetime import datetime, timezone

payload_path = sys.argv[1]
with open(payload_path, "r", encoding="utf-8") as handle:
    payload = json.load(handle)

stamp = (
    payload.get("updated_at")
    or payload.get("launched_at")
    or payload.get("started_at")
)
if not stamp:
    raise SystemExit(1)

normalized = str(stamp).strip().replace("Z", "+00:00")
parsed = datetime.fromisoformat(normalized)
if parsed.tzinfo is None:
    parsed = parsed.replace(tzinfo=timezone.utc)

age_seconds = int((datetime.now(timezone.utc) - parsed).total_seconds())
print(max(age_seconds, 0))
PY
}

stop_stale_worker() {
  local pid="$1"
  echo "Detected a stale Node3 retrain worker (PID ${pid}). Stopping it before relaunch."
  kill "${pid}" 2>/dev/null || true
  sleep 10
  if kill -0 "${pid}" 2>/dev/null; then
    echo "Force-stopping stale Node3 retrain worker PID ${pid}."
    kill -9 "${pid}" 2>/dev/null || true
  fi
}

pid_is_active() {
  local pid="$1"
  if [ -z "${pid}" ]; then
    return 1
  fi
  if ! kill -0 "${pid}" 2>/dev/null; then
    return 1
  fi
  local proc_state=""
  proc_state="$(ps -o stat= -p "${pid}" 2>/dev/null | tr -d '[:space:]' || true)"
  if [ -z "${proc_state}" ]; then
    return 1
  fi
  case "${proc_state}" in
    Z*|*Z*)
      return 1
      ;;
  esac
  return 0
}

resolve_existing_worker_pid() {
  local candidate_pid=""
  if [ -f "${PID_FILE}" ]; then
    candidate_pid="$(tr -d ' \n\r' < "${PID_FILE}" 2>/dev/null || true)"
    if pid_is_active "${candidate_pid}"; then
      printf '%s\n' "${candidate_pid}"
      return 0
    fi
  fi
  if [ -f "${LOCK_PID_FILE}" ]; then
    candidate_pid="$(tr -d ' \n\r' < "${LOCK_PID_FILE}" 2>/dev/null || true)"
    if pid_is_active "${candidate_pid}"; then
      printf '%s\n' "${candidate_pid}"
      return 0
    fi
  fi
  return 1
}

find_active_retrain_worker_pid() {
  python3 - "$$" "${PPID:-0}" <<'PY'
import os
import sys

current_pid = int(sys.argv[1])
parent_pid = int(sys.argv[2])

run_targets = {
    "scripts/gcp/run-node3.sh",
    "/opt/traffic/scripts/gcp/run-node3.sh",
}
h2o_targets = {
    "ml/training/h2o_after_2020.py",
    "/opt/traffic/ml/training/h2o_after_2020.py",
}

for entry in sorted(os.listdir("/proc"), key=lambda value: int(value) if value.isdigit() else 0):
    if not entry.isdigit():
        continue
    pid = int(entry)
    if pid in {current_pid, parent_pid}:
        continue
    try:
        with open(f"/proc/{pid}/cmdline", "rb") as handle:
            raw = handle.read()
    except OSError:
        continue
    if not raw:
        continue
    argv = [part.decode("utf-8", errors="ignore") for part in raw.split(b"\0") if part]
    if not argv:
        continue
    argv0 = os.path.basename(argv[0])
    if argv0 == "bash" and len(argv) >= 2 and argv[1] in run_targets:
        print(pid)
        raise SystemExit(0)
    if argv0.startswith("python") and any(arg in h2o_targets for arg in argv[1:]):
        print(pid)
        raise SystemExit(0)
PY
}

existing_pid="$(resolve_existing_worker_pid || true)"
if [ -n "${existing_pid}" ]; then
  if pid_is_active "${existing_pid}"; then
    payload_age=""
    payload_age="$(status_payload_age_seconds "${STATUS_FILE}" 2>/dev/null || true)"
    if [ -z "${payload_age}" ]; then
      payload_age="$(status_payload_age_seconds "${LAUNCH_METADATA_FILE}" 2>/dev/null || true)"
    fi
    echo "Node3 retrain worker is already running with PID ${existing_pid}."
    if [ -n "${payload_age}" ]; then
      echo "Worker status age: ${payload_age}s (stale threshold: ${STALE_SECONDS}s)."
      if [ "${payload_age}" -ge "${STALE_SECONDS}" ]; then
        stop_stale_worker "${existing_pid}"
        rm -f "${PID_FILE}"
        existing_pid=""
      fi
    fi
    if [ -n "${existing_pid}" ] && [ -f "${STATUS_FILE}" ]; then
      cat "${STATUS_FILE}"
    fi
    if [ -n "${existing_pid}" ]; then
      exit 0
    fi
  fi
fi
rm -f "${PID_FILE}"

active_worker_pid="$(find_active_retrain_worker_pid || true)"
if pid_is_active "${active_worker_pid}"; then
  echo "${active_worker_pid}" > "${PID_FILE}"
  echo "Node3 retrain worker is already active with PID ${active_worker_pid}. Reusing it instead of launching a duplicate."
  if [ -f "${STATUS_FILE}" ]; then
    cat "${STATUS_FILE}"
  fi
  exit 0
fi

launched_at="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
# Keep the retrain worker bounded by the Node3-specific defaults from `.env.cloud`
# so one run can fit inside the 20-minute Airflow cadence instead of forcing a
# one-hour H2O budget on every launch.
nohup bash -lc "cd '${PROJECT_ROOT}' && RETRAIN_MIN_US_ROWS=0 NODE3_LOCK_BUSY_EXIT_CODE=0 NODE3_H2O_WALL_TIMEOUT_SECONDS=\${NODE3_H2O_WALL_TIMEOUT_SECONDS:-3300} bash scripts/gcp/run-node3.sh" \
  >>"${LOG_FILE}" 2>&1 < /dev/null &
worker_pid=$!
echo "${worker_pid}" > "${PID_FILE}"
write_launch_metadata "${worker_pid}" "${launched_at}"

sleep 2
if ! kill -0 "${worker_pid}" 2>/dev/null; then
  echo "Node3 retrain worker exited immediately after launch. Inspect ${LOG_FILE}."
  if [ -f "${STATUS_FILE}" ]; then
    cat "${STATUS_FILE}"
    status_value="$(python3 - "${STATUS_FILE}" <<'PY'
import json
import sys
from pathlib import Path

payload = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
print(str(payload.get("status", "continue")).strip().lower())
PY
)"
    if [ "${status_value}" != "failed" ]; then
      exit 0
    fi
  fi
  exit 1
fi

echo "Node3 retrain worker launched in background with PID ${worker_pid}."
if [ -f "${STATUS_FILE}" ]; then
  cat "${STATUS_FILE}"
fi
