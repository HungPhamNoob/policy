#!/bin/bash
# Start Node 1 control-plane services and bootstrap the offline model.
#
# Node 1 responsibilities:
#   - PostgreSQL/PostGIS for prediction storage.
#   - MLflow tracking and model serving.
#   - FastAPI dashboard backend and Next.js dashboard frontend.
#   - Airflow scheduler/webserver.
#   - Offline H2O training on pre-2020 data before stream/batch nodes start.

set -euo pipefail

PROJECT_ROOT="${PROJECT_ROOT:-/opt/traffic}"
ENV_FILE="${ENV_FILE:-${PROJECT_ROOT}/.env.cloud}"
NODE1_COMPOSE_FILE="${PROJECT_ROOT}/deployment/node1-control/docker-compose.yaml"
NODE1_COMPOSE_DIR="$(dirname "${NODE1_COMPOSE_FILE}")"
TRAINING_PID_FILE="${PROJECT_ROOT}/logs/cloud_h2o_before_2020.pid"
TRAINING_TMP_CLEANUP_HOURS="${TRAINING_TMP_CLEANUP_HOURS:-12}"
NODE1_BOOTSTRAP_MODEL="${NODE1_BOOTSTRAP_MODEL:-true}"
APT_CACHE_UPDATED=0

# Capture incoming environment variables to prevent them from being overwritten by sourcing env files
INCOMING_IS_TRAIN_OFFLINE="${IS_TRAIN_OFFLINE:-}"

echo "Node 1 run script started at $(date -u +%Y-%m-%dT%H:%M:%SZ)"
echo "Project root: ${PROJECT_ROOT}"
echo "Environment file: ${ENV_FILE}"

cd "${PROJECT_ROOT}"

if [ -f "${ENV_FILE}" ]; then
  set -a
  . "${ENV_FILE}"
  set +a
else
  echo "ERROR: ${ENV_FILE} does not exist."
  exit 1
fi

if [ -n "${INCOMING_IS_TRAIN_OFFLINE}" ]; then
  IS_TRAIN_OFFLINE="${INCOMING_IS_TRAIN_OFFLINE}"
fi

export MLFLOW_TRACKING_URI="${NODE1_MLFLOW_TRACKING_URI:-http://localhost:5000}"
IS_TRAIN_OFFLINE="${IS_TRAIN_OFFLINE:-false}"
echo "Node 1 MLflow tracking URI: ${MLFLOW_TRACKING_URI}"
echo "IS_TRAIN_OFFLINE: ${IS_TRAIN_OFFLINE}"
echo "NODE1_BOOTSTRAP_MODEL: ${NODE1_BOOTSTRAP_MODEL}"

NODE1_CANONICAL_NAME_PATTERN='^node1-(postgres|airflow-db|airflow|airflow-scheduler|blackbox-exporter|prometheus|grafana|mlflow|mlflow-serving|fastapi|dashboard-frontend)$'
NODE1_TRANSIENT_NAME_PATTERN='^.+_node1-(postgres|airflow-db|airflow|airflow-scheduler|blackbox-exporter|prometheus|grafana|mlflow|mlflow-serving|fastapi|dashboard-frontend)$'

remove_matching_node1_containers() {
  # Remove containers that match the supplied pattern. The caller decides whether
  # canonical service names or Compose-generated transient names should be cleaned up.
  local container_name_pattern="$1"
  local stale_containers=()

  while IFS= read -r container_name; do
    if [ -n "${container_name}" ]; then
      stale_containers+=("${container_name}")
    fi
  done < <(docker ps -a --format '{{.Names}}' | grep -E "${container_name_pattern}" || true)

  if [ "${#stale_containers[@]}" -eq 0 ]; then
    echo "No stale Node 1 containers were found."
    return 0
  fi

  echo "Removing stale Node 1 containers detected from previous Compose reconciliations:"
  printf '  - %s\n' "${stale_containers[@]}"
  docker rm -f "${stale_containers[@]}" >/dev/null
}

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

wait_for_postgres() {
  echo "Waiting for PostgreSQL before applying runtime index guards..."
  for attempt in $(seq 1 30); do
    if docker exec node1-postgres pg_isready -U "${POSTGRES_USER:-capstone}" -d "${POSTGRES_DB:-capstone_db}" >/dev/null 2>&1; then
      echo "PostgreSQL is reachable."
      return 0
    fi
    echo "PostgreSQL is not ready yet (${attempt}/30)."
    sleep 2
  done
  echo "WARNING: PostgreSQL did not become ready in time; skipping runtime index guards."
  return 1
}

ensure_prediction_indexes() {
  if ! wait_for_postgres; then
    return 0
  fi

  local prediction_table
  prediction_table="$(
    docker exec node1-postgres psql -U "${POSTGRES_USER:-capstone}" -d "${POSTGRES_DB:-capstone_db}" -At \
      -c "SELECT to_regclass('public.traffic_risk_predictions');" 2>/dev/null || true
  )"
  if [ "${prediction_table}" != "traffic_risk_predictions" ]; then
    echo "Prediction table does not exist yet; skipping runtime index guards."
    return 0
  fi

  table_has_column() {
    local table_name="$1"
    local column_name="$2"
    docker exec node1-postgres psql -U "${POSTGRES_USER:-capstone}" -d "${POSTGRES_DB:-capstone_db}" -At \
      -c "SELECT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = 'public' AND table_name = '${table_name}' AND column_name = '${column_name}');" 2>/dev/null || true
  }

  ensure_index() {
    local table_name="$1"
    local index_column="$2"
    local index_name="$3"
    local index_expression="$4"
    local is_valid

    if [ "$(table_has_column "${table_name}" "${index_column}")" != "t" ]; then
      echo "Column ${table_name}.${index_column} does not exist yet; skipping index ${index_name}."
      return 0
    fi

    is_valid="$(
      docker exec node1-postgres psql -U "${POSTGRES_USER:-capstone}" -d "${POSTGRES_DB:-capstone_db}" -At \
        -c "SELECT COALESCE((SELECT i.indisvalid FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid WHERE c.relname = '${index_name}'), false);" 2>/dev/null || true
    )"

    if [ "${is_valid}" = "t" ]; then
      echo "Index ${index_name} is already valid."
      return 0
    fi

    echo "Rebuilding dashboard query index ${index_name}."
    docker exec node1-postgres psql -U "${POSTGRES_USER:-capstone}" -d "${POSTGRES_DB:-capstone_db}" \
      -c "DROP INDEX CONCURRENTLY IF EXISTS public.${index_name};" >/dev/null
    docker exec node1-postgres psql -U "${POSTGRES_USER:-capstone}" -d "${POSTGRES_DB:-capstone_db}" \
      -c "CREATE INDEX CONCURRENTLY ${index_name} ON public.${table_name} (${index_expression});" >/dev/null
  }

  echo "Ensuring dashboard query indexes exist on traffic_risk_predictions."
  ensure_index "traffic_risk_predictions" "event_time" "idx_traffic_risk_predictions_event_time" "event_time DESC NULLS LAST"
  ensure_index "traffic_risk_predictions" "processed_time" "idx_traffic_risk_predictions_processed_time" "processed_time DESC NULLS LAST"

  # Also ensure indexes on the TomTom incidents table used by live/full mode queries.
  local tomtom_table
  tomtom_table="$(
    docker exec node1-postgres psql -U "${POSTGRES_USER:-capstone}" -d "${POSTGRES_DB:-capstone_db}" -At \
      -c "SELECT to_regclass('public.traffic_tomtom_incidents');" 2>/dev/null || true
  )"
  if [ "${tomtom_table}" = "traffic_tomtom_incidents" ]; then
    echo "Ensuring dashboard query indexes exist on traffic_tomtom_incidents."
    ensure_index "traffic_tomtom_incidents" "event_time" "idx_traffic_tomtom_incidents_event_time" "event_time DESC NULLS LAST"
    ensure_index "traffic_tomtom_incidents" "severity" "idx_traffic_tomtom_incidents_severity" "severity"
  else
    echo "TomTom incidents table does not exist yet; skipping its index guards."
  fi
}

prepare_runtime_directories() {
  echo "Preparing writable runtime directories for containers."
  mkdir -p orchestration/logs/scheduler
  sudo chown -R 50000:0 orchestration/logs 2>/dev/null || true
  sudo chmod -R 2775 orchestration/logs 2>/dev/null || true
}

mark_training_active() {
  mkdir -p "${PROJECT_ROOT}/logs"
  echo "$$" > "${TRAINING_PID_FILE}"
}

clear_training_active() {
  rm -f "${TRAINING_PID_FILE}" 2>/dev/null || true
}

cleanup_stale_training_temp_files() {
  echo "Cleaning stale offline-training temp files older than ${TRAINING_TMP_CLEANUP_HOURS} hours."
  sudo find /tmp \
    \( \
      -name 'tmp*_us_train_offline_before_2020.csv' -o \
      -name 'tmp*_us_train_offline_before_2020_features_for_h2o.csv' -o \
      -name 'us_train_offline_before_2020.csv' -o \
      -name 'us_train_offline_before_2020_features_for_h2o.csv' -o \
      -name 'traffic-workspace-*.tar.gz' \
    \) \
    -type f \
    -mmin "+$((TRAINING_TMP_CLEANUP_HOURS * 60))" \
    -print -delete 2>/dev/null || true

  sudo find /tmp \
    -maxdepth 1 \
    -type d \
    \( -name 'tmp*' -o -name 'hsperfdata_*' \) \
    -mmin "+$((TRAINING_TMP_CLEANUP_HOURS * 60))" \
    -print0 2>/dev/null | xargs -0 -r sudo rm -rf || true
}

training_already_running() {
  if [ ! -f "${TRAINING_PID_FILE}" ]; then
    return 1
  fi

  local existing_pid
  existing_pid="$(cat "${TRAINING_PID_FILE}" 2>/dev/null || true)"
  if [ -n "${existing_pid}" ] && kill -0 "${existing_pid}" 2>/dev/null; then
    echo "Offline baseline training is already active under PID ${existing_pid}. Reusing the current run."
    return 0
  fi

  echo "Removing stale offline-training PID file."
  clear_training_active
  return 1
}

echo "Checking host dependencies required for offline H2O training."
ensure_command curl curl
ensure_command docker docker.io
ensure_docker_compose
ensure_command java openjdk-17-jre-headless
ensure_command python3 python3

if ! python3 -m venv --help >/dev/null 2>&1; then
  echo "python3-venv is not installed. Installing it before creating training environments."
  apt_install_if_missing python3-venv
fi

echo "Starting Node 1 Docker services from the current workspace snapshot..."
prepare_runtime_directories

echo "Removing stale Node 1 containers from previous Compose project names..."
remove_matching_node1_containers "${NODE1_CANONICAL_NAME_PATTERN}"
remove_matching_node1_containers "${NODE1_TRANSIENT_NAME_PATTERN}"

echo "Ensuring the shared Docker network exists before Compose starts."
docker network inspect capstone-net >/dev/null 2>&1 || docker network create capstone-net >/dev/null

compose_cmd \
  --project-directory "${NODE1_COMPOSE_DIR}" \
  --env-file "${ENV_FILE}" \
  -f "${NODE1_COMPOSE_FILE}" \
  up -d --build --remove-orphans

ensure_prediction_indexes

prepare_runtime_directories
compose_cmd \
  --project-directory "${NODE1_COMPOSE_DIR}" \
  --env-file "${ENV_FILE}" \
  -f "${NODE1_COMPOSE_FILE}" \
  restart airflow airflow-scheduler >/dev/null 2>&1 || true

echo "Waiting for MLflow tracking server before checking model registry..."
for attempt in $(seq 1 30); do
  if curl --max-time 5 -fsS "${MLFLOW_TRACKING_URI}/health" >/dev/null 2>&1; then
    echo "MLflow tracking server is reachable."
    break
  fi
  echo "MLflow is not ready yet (${attempt}/30)."
  sleep 5
done

echo "Checking whether a registered MLflow model already exists..."
MODEL_EXISTS="false"
MODEL_NAME="${ML_MODEL_NAME:-traffic-risk-model}"
MODEL_REGISTRY_URL="${MLFLOW_TRACKING_URI}/api/2.0/mlflow/registered-models/get?name=${MODEL_NAME}"
if curl --max-time 10 -fsS "${MODEL_REGISTRY_URL}" >/dev/null 2>&1; then
  MODEL_EXISTS="true"
fi

run_offline_training() {
  if training_already_running; then
    return 0
  fi

  echo "Running offline feature engineering and H2O training."
  cleanup_stale_training_temp_files
  mark_training_active
  local exit_code=0
  local training_venv="${PROJECT_ROOT}/.venv-node1"
  local training_python="${training_venv}/bin/python"
  {
    if [ ! -x "${training_python}" ]; then
      python3 -m venv "${training_venv}"
    fi

    local missing_specs=()
    if ! "${training_python}" -c "import h2o" >/dev/null 2>&1; then
      missing_specs+=("h2o==3.46.0.6")
    fi
    if ! "${training_python}" -c "import mlflow" >/dev/null 2>&1; then
      missing_specs+=("mlflow==2.12.1")
    fi
    if ! "${training_python}" -c "import pandas" >/dev/null 2>&1; then
      missing_specs+=("pandas==2.2.2")
    fi
    if ! "${training_python}" -c "import numpy" >/dev/null 2>&1; then
      missing_specs+=("numpy==1.26.4")
    fi
    if ! "${training_python}" -c "import sklearn" >/dev/null 2>&1; then
      missing_specs+=("scikit-learn==1.4.2")
    fi
    if ! "${training_python}" -c "import dotenv" >/dev/null 2>&1; then
      missing_specs+=("python-dotenv==1.0.1")
    fi
    if ! "${training_python}" -c "import gcsfs" >/dev/null 2>&1; then
      missing_specs+=("gcsfs==2024.3.1")
    fi
    if ! "${training_python}" -c "import google.auth" >/dev/null 2>&1; then
      missing_specs+=("google-auth>=2.23.0")
    fi
    if ! "${training_python}" -c "import google.cloud.storage" >/dev/null 2>&1; then
      missing_specs+=("google-cloud-storage>=2.14.0")
    fi

    if [ "${#missing_specs[@]}" -gt 0 ]; then
      echo "Installing missing Python training dependencies into ${training_venv}."
      "${training_python}" -m pip install --upgrade pip
      "${training_python}" -m pip install "${missing_specs[@]}"
    else
      echo "Python training dependencies already exist in ${training_venv}."
    fi
    if [[ "${US_TRAIN_OFFLINE_PATH:-}" != gs://* ]]; then
      "${training_python}" ml/dataset/dataset_offline.py
    else
      echo "US_TRAIN_OFFLINE_PATH is a GCS feature CSV. Skipping local feature generation."
    fi
    "${training_python}" ml/training/h2o_before_2020.py
  } || exit_code=$?

  clear_training_active
  return "${exit_code}"
}

if [ "${NODE1_BOOTSTRAP_MODEL}" != "true" ]; then
  echo "NODE1_BOOTSTRAP_MODEL=${NODE1_BOOTSTRAP_MODEL}. Skipping offline model bootstrap."
elif [ "${IS_TRAIN_OFFLINE}" = "true" ]; then
  echo "IS_TRAIN_OFFLINE=true. Forcing offline feature engineering and H2O training."
  run_offline_training
elif [ "${MODEL_EXISTS}" = "true" ]; then
  echo "MLflow already has a registered model. Offline training bootstrap is skipped."
else
  echo "No registered model found in MLflow. Offline training is required."
  run_offline_training
fi

echo "Restarting MLflow model serving after model bootstrap..."
echo "Removing any transient Node 1 containers that could block the serving restart."
remove_matching_node1_containers "${NODE1_TRANSIENT_NAME_PATTERN}"
compose_cmd \
  --project-directory "${NODE1_COMPOSE_DIR}" \
  --env-file "${ENV_FILE}" \
  -f "${NODE1_COMPOSE_FILE}" \
  up -d --build mlflow-serving fastapi

echo "Node 1 services:"
compose_cmd \
  --project-directory "${NODE1_COMPOSE_DIR}" \
  --env-file "${ENV_FILE}" \
  -f "${NODE1_COMPOSE_FILE}" \
  ps

echo "Node 1 run script completed at $(date -u +%Y-%m-%dT%H:%M:%SZ)"
