#!/bin/bash
# Wait for the active Node3 retrain to finish, then reset only US replay.
# This script is intended to run unattended (nohup/cron), so it must be
# resilient to transient gcloud/ssh failures.
set -u -o pipefail

PROJECT_ROOT="${PROJECT_ROOT:-/home/hung/YEAR 3/Big Data/BTL}"
LOG_FILE="${LOG_FILE:-/tmp/us-replay-reset-after-retrain.log}"
PROJECT_ID="${GCP_PROJECT_ID:-bigdata1-490302}"
ZONE="${GCP_ZONE:-us-central1-a}"
# Keep the post-reset replay pace close to the cloud default so the
# 2020+ replay dataset drains in about one day while the rest of the stack
# stays responsive.
THROTTLE_SECONDS="${US_REPLAY_THROTTLE_SECONDS:-0.068}"
LOOP_FOREVER="${US_REPLAY_LOOP_FOREVER:-false}"
START_ROW="${US_REPLAY_START_ROW:-0}"

mkdir -p "$(dirname "${LOG_FILE}")"
cd "${PROJECT_ROOT}"

echo "[$(date -u +%Y-%m-%dT%H:%M:%SZ)] waiting for current node3 retrain to finish" >> "${LOG_FILE}"
while true; do
  if gcloud compute ssh node3-batch \
      --quiet \
      --zone="${ZONE}" \
      --project="${PROJECT_ID}" \
      --command="ps -eo pid=,args= | grep -F '/home/runner/.venv-traffic-node3/bin/python ml/training/h2o_after_2020.py' | grep -v 'grep -F' >/dev/null" \
      >/dev/null 2>&1; then
    echo "[$(date -u +%Y-%m-%dT%H:%M:%SZ)] retrain still running" >> "${LOG_FILE}"
    sleep 30
    continue
  fi

  # If SSH fails transiently, keep waiting; if it succeeds and the process is
  # absent, the loop should break and trigger the reset.
  if gcloud compute ssh node3-batch \
      --quiet \
      --zone="${ZONE}" \
      --project="${PROJECT_ID}" \
      --command="true" \
      >/dev/null 2>&1; then
    break
  fi

  echo "[$(date -u +%Y-%m-%dT%H:%M:%SZ)] WARN: gcloud ssh failed; retrying in 30s" >> "${LOG_FILE}"
  sleep 30
done

echo "[$(date -u +%Y-%m-%dT%H:%M:%SZ)] node3 retrain idle; now waiting for offline bootstrap completion" >> "${LOG_FILE}"
while true; do
  if gcloud compute ssh node1-control \
      --quiet \
      --zone="${ZONE}" \
      --project="${PROJECT_ID}" \
      --command="
        python3 - <<'PY'
import json
import sys
import urllib.parse
import urllib.request

api_base = 'http://localhost:5000/api/2.0/mlflow'
experiment_name = 'traffic-risk-assessment'
run_name = 'h2o_automl'

experiment_url = (
    f\"{api_base}/experiments/get-by-name?experiment_name=\"
    f\"{urllib.parse.quote(experiment_name, safe='')}\"
)
with urllib.request.urlopen(experiment_url, timeout=10) as response:
    experiment_payload = json.load(response)
experiment_id = experiment_payload['experiment']['experiment_id']

body = json.dumps(
    {
        'experiment_ids': [experiment_id],
        'filter': (
            f\"attributes.run_name = '{run_name}' \"
            \"and attributes.status = 'FINISHED'\"
        ),
        'max_results': 1,
        'order_by': ['attributes.start_time DESC'],
    }
).encode('utf-8')
request = urllib.request.Request(
    f'{api_base}/runs/search',
    data=body,
    headers={'Content-Type': 'application/json'},
    method='POST',
)
with urllib.request.urlopen(request, timeout=15) as response:
    runs_payload = json.load(response)

sys.exit(0 if (runs_payload.get('runs') or []) else 1)
PY
      " \
      >/dev/null 2>&1; then
    break
  fi

  echo "[$(date -u +%Y-%m-%dT%H:%M:%SZ)] offline bootstrap still running or unavailable; retrying in 60s" >> "${LOG_FILE}"
  sleep 60
done

echo "[$(date -u +%Y-%m-%dT%H:%M:%SZ)] starting replay-only reset after offline bootstrap completion" >> "${LOG_FILE}"
while true; do
  if US_REPLAY_THROTTLE_SECONDS="${THROTTLE_SECONDS}" \
    US_REPLAY_LOOP_FOREVER="${LOOP_FOREVER}" \
    US_REPLAY_START_ROW="${START_ROW}" \
    bash scripts/gcp/reset-us-replay-only.sh >> "${LOG_FILE}" 2>&1; then
    break
  fi

  echo "[$(date -u +%Y-%m-%dT%H:%M:%SZ)] WARN: replay reset failed transiently; retrying in 60s" >> "${LOG_FILE}"
  sleep 60
done

echo "[$(date -u +%Y-%m-%dT%H:%M:%SZ)] reset script completed" >> "${LOG_FILE}"
