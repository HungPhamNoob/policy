#!/bin/bash
# Restart the node2 Flink Python streaming job in a controlled way.
#
# This script is intentionally narrower than scripts/gcp/run-node2.sh:
# it does not touch Kafka, Redis, producers, or the Flink JM/TM containers.
# It cancels the currently running Flink job via the REST API, waits for the
# cluster to go idle, then recreates only the submitter container so a single
# replacement job is launched with the latest runtime env values.

set -euo pipefail

PROJECT_ROOT="${PROJECT_ROOT:-/opt/traffic}"
ENV_FILE="${ENV_FILE:-${PROJECT_ROOT}/.env.cloud}"
NODE2_COMPOSE_FILE="${PROJECT_ROOT}/deployment/node2-streaming/docker-compose.yaml"
NODE2_COMPOSE_DIR="$(dirname "${NODE2_COMPOSE_FILE}")"
NODE2_COMPOSE_PROJECT_NAME="${NODE2_COMPOSE_PROJECT_NAME:-node2-streaming}"
FLINK_REST_URL="${FLINK_REST_URL:-http://localhost:8081}"
FLINK_JOB_NAME="${FLINK_JOB_NAME:-Unified Traffic Streaming - US Replay + TomTom Live}"
FLINK_CANCEL_TIMEOUT_SECONDS="${FLINK_CANCEL_TIMEOUT_SECONDS:-180}"
FLINK_RESTART_TIMEOUT_SECONDS="${FLINK_RESTART_TIMEOUT_SECONDS:-180}"
FLINK_POLL_SECONDS="${FLINK_POLL_SECONDS:-5}"

echo "Controlled Flink restart started at $(date -u +%Y-%m-%dT%H:%M:%SZ)"
echo "Project root: ${PROJECT_ROOT}"
echo "Environment file: ${ENV_FILE}"
echo "Flink REST URL: ${FLINK_REST_URL}"

cd "${PROJECT_ROOT}"

if [ -f "${ENV_FILE}" ]; then
  while IFS= read -r line; do
    case "${line}" in
      ''|\#*) continue ;;
      *=*) export "${line}" ;;
    esac
  done < "${ENV_FILE}"
else
  echo "ERROR: ${ENV_FILE} does not exist."
  exit 1
fi

compose_cmd() {
  if docker compose version >/dev/null 2>&1; then
    COMPOSE_PROJECT_NAME="${NODE2_COMPOSE_PROJECT_NAME}" docker compose "$@"
    return
  fi
  if command -v docker-compose >/dev/null 2>&1; then
    COMPOSE_PROJECT_NAME="${NODE2_COMPOSE_PROJECT_NAME}" docker-compose "$@"
    return
  fi
  echo "ERROR: docker compose is unavailable on this host."
  exit 1
}

flink_jobs_json() {
  curl -fsS "${FLINK_REST_URL}/jobs/overview"
}

running_job_summary() {
  python3 -c '
import json
import os
import sys

payload = json.load(sys.stdin)
target_name = os.environ.get("FLINK_JOB_NAME", "")
running = []
for job in payload.get("jobs", []):
    if job.get("state") != "RUNNING":
        continue
    if target_name and job.get("name") != target_name:
        continue
    running.append({"jid": job.get("jid"), "name": job.get("name"), "state": job.get("state")})
print(json.dumps(running, ensure_ascii=True))
'
}

current_running_job_id() {
  local summary_json="$1"
  python3 -c '
import json
import sys

jobs = json.loads(sys.argv[1])
if len(jobs) != 1:
    raise SystemExit(1)
print(jobs[0]["jid"])
' "${summary_json}"
}

count_running_jobs() {
  local summary_json="$1"
  python3 -c '
import json
import sys

jobs = json.loads(sys.argv[1])
print(len(jobs))
' "${summary_json}"
}

wait_for_running_job_count() {
  local expected_count="$1"
  local timeout_seconds="$2"
  local deadline=$(( $(date +%s) + timeout_seconds ))

  while true; do
    local jobs_json summary_json current_count
    jobs_json="$(flink_jobs_json)"
    summary_json="$(printf '%s' "${jobs_json}" | running_job_summary)"
    current_count="$(count_running_jobs "${summary_json}")"
    echo "Observed ${current_count} running matching Flink job(s). Expected ${expected_count}."
    if [ "${current_count}" = "${expected_count}" ]; then
      printf '%s\n' "${summary_json}"
      return 0
    fi
    if [ "$(date +%s)" -ge "${deadline}" ]; then
      echo "ERROR: Timed out waiting for ${expected_count} running matching Flink job(s)."
      echo "Last observed summary: ${summary_json}"
      return 1
    fi
    sleep "${FLINK_POLL_SECONDS}"
  done
}

echo "Checking the current Flink job state before restart."
initial_jobs_json="$(flink_jobs_json)"
initial_summary_json="$(printf '%s' "${initial_jobs_json}" | running_job_summary)"
initial_count="$(count_running_jobs "${initial_summary_json}")"
echo "Initial matching RUNNING jobs: ${initial_summary_json}"

if [ "${initial_count}" -ne 1 ]; then
  echo "ERROR: Expected exactly one RUNNING Flink job named '${FLINK_JOB_NAME}' before restart."
  exit 1
fi

job_id="$(current_running_job_id "${initial_summary_json}")"
echo "Cancelling current Flink job: ${job_id}"
curl -fsS -X PATCH "${FLINK_REST_URL}/jobs/${job_id}" >/dev/null

echo "Waiting for the old Flink job to disappear from RUNNING jobs."
wait_for_running_job_count 0 "${FLINK_CANCEL_TIMEOUT_SECONDS}" >/dev/null

echo "Recreating only the Flink Python submitter container."
compose_cmd \
  --project-directory "${NODE2_COMPOSE_DIR}" \
  --env-file "${ENV_FILE}" \
  -f "${NODE2_COMPOSE_FILE}" \
  up -d --no-deps --force-recreate flink-python-job

echo "Waiting for the replacement Flink job to come up."
final_summary_json="$(wait_for_running_job_count 1 "${FLINK_RESTART_TIMEOUT_SECONDS}")"
echo "Replacement RUNNING job summary: ${final_summary_json}"

echo "Controlled Flink restart completed at $(date -u +%Y-%m-%dT%H:%M:%SZ)"
