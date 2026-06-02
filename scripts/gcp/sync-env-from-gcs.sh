#!/bin/bash
# Refresh /opt/traffic/.env.cloud from the shared GCS copy when available.

set -euo pipefail

ENV_FILE="${1:-${ENV_FILE:-/opt/traffic/.env.cloud}}"
PROJECT_ROOT="${PROJECT_ROOT:-$(dirname "${ENV_FILE}")}"
DEFAULT_GCS_ENV_PATH="${DEFAULT_GCS_ENV_PATH:-gs://big-data-group-4-bronze/env/.env.cloud}"
TMP_ENV_FILE="$(mktemp /tmp/traffic-env-XXXXXX)"

cleanup() {
  rm -f "${TMP_ENV_FILE}" 2>/dev/null || true
}

trap cleanup EXIT

detect_gcs_env_path() {
  local env_path="${DEFAULT_GCS_ENV_PATH}"
  if [ -f "${ENV_FILE}" ]; then
    local detected
    detected="$(awk -F= '/^GCS_ENV_PATH=/{print $2; exit}' "${ENV_FILE}" 2>/dev/null || true)"
    if [ -n "${detected}" ]; then
      env_path="${detected}"
    fi
  fi
  printf '%s\n' "${env_path}"
}

if ! command -v gcloud >/dev/null 2>&1; then
  echo "gcloud is unavailable; keeping the existing ${ENV_FILE}."
  exit 0
fi

GCS_ENV_PATH="$(detect_gcs_env_path)"
echo "Attempting to refresh ${ENV_FILE} from ${GCS_ENV_PATH}."

if ! gcloud storage cp "${GCS_ENV_PATH}" "${TMP_ENV_FILE}" >/dev/null 2>&1; then
  echo "Shared cloud env is unavailable; keeping the existing ${ENV_FILE}."
  exit 0
fi

if [ ! -s "${TMP_ENV_FILE}" ]; then
  echo "Shared cloud env downloaded empty content; keeping the existing ${ENV_FILE}."
  exit 0
fi

if ! install -m 600 "${TMP_ENV_FILE}" "${ENV_FILE}" 2>/dev/null; then
  sudo install -m 600 "${TMP_ENV_FILE}" "${ENV_FILE}"
  sudo chown "$(id -u):$(id -g)" "${ENV_FILE}" 2>/dev/null || true
fi

if ! cp "${ENV_FILE}" "${PROJECT_ROOT}/.env" 2>/dev/null; then
  sudo cp "${ENV_FILE}" "${PROJECT_ROOT}/.env" 2>/dev/null || true
  sudo chown "$(id -u):$(id -g)" "${PROJECT_ROOT}/.env" 2>/dev/null || true
fi
echo "Refreshed ${ENV_FILE} from ${GCS_ENV_PATH}."
