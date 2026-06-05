#!/bin/bash

set -euo pipefail

JOBMANAGER_URL="${FLINK_JOBMANAGER_URL:-http://flink-jobmanager:8081}"
POLL_SECONDS="${FLINK_JOB_GUARD_POLL_SECONDS:-20}"
SUBMIT_COOLDOWN_SECONDS="${FLINK_JOB_GUARD_SUBMIT_COOLDOWN_SECONDS:-30}"
LAST_SUBMIT_EPOCH=0

submit_job() {
  local now
  now="$(date +%s)"
  if [ $((now - LAST_SUBMIT_EPOCH)) -lt "${SUBMIT_COOLDOWN_SECONDS}" ]; then
    echo "Flink job submit skipped because cooldown is still active."
    return 0
  fi

  echo "Submitting unified Flink streaming job at $(date -u +%Y-%m-%dT%H:%M:%SZ)"
  /opt/flink/bin/flink run -d \
    -m flink-jobmanager:8081 \
    -pyclientexec /opt/flink/pyenv/bin/python \
    -pyexec /opt/flink/pyenv/bin/python \
    -pyfs processing \
    -py processing/flink_streaming.py
  LAST_SUBMIT_EPOCH="${now}"
}

job_count() {
  local payload
  payload="$(curl -fsS "${JOBMANAGER_URL}/jobs/overview" 2>/dev/null || true)"
  if [ -z "${payload}" ]; then
    echo 0
    return 0
  fi

  python3 - <<'PY' "${payload}"
import json
import sys

try:
    payload = json.loads(sys.argv[1])
except Exception:
    print(0)
    raise SystemExit(0)

jobs = payload.get("jobs") or []
active = [
    job for job in jobs
    if str(job.get("state") or "").upper() not in {"FINISHED", "FAILED", "CANCELED"}
]
print(len(active))
PY
}

echo "Starting Flink job guard against ${JOBMANAGER_URL}"

until curl -fsS "${JOBMANAGER_URL}/taskmanagers" >/dev/null 2>&1; do
  echo "Waiting for Flink JobManager REST endpoint..."
  sleep 5
done

while true; do
  active_jobs="$(job_count)"
  if [ "${active_jobs}" -eq 0 ]; then
    submit_job || true
  fi
  sleep "${POLL_SECONDS}"
done
