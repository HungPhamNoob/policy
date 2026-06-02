#!/bin/bash
# =============================================================================
# Full Realtime-Only Reset — Traffic Risk Assessment Platform
# =============================================================================
#
# Purpose:
#   Reset ALL realtime data sources, state, and serving tables so that the
#   platform can start streaming from scratch. This script does NOT run
#   pre-2020 offline training; it only resets the post-2020 replay and
#   TomTom live branches.
#
# What gets reset:
#   1. Kafka topics (traffic.us.raw, traffic.tomtom.raw)
#   2. Flink checkpoints (GCS + local state)
#   3. Spark checkpoints (GCS)
#   4. Silver/Gold GCS data
#   5. PostgreSQL serving tables (predictions + tomtom)
#   6. Docker volumes for Kafka, Flink, Spark
#   7. Redis cache if present
#
# Usage (local laptop → targets all 3 VMs):
#   bash scripts/gcp/reset-realtime.sh
#
# Usage (on a specific VM node):
#   PROJECT_ROOT=/opt/traffic bash scripts/gcp/reset-realtime.sh --node [1|2|3]
# =============================================================================

set -euo pipefail

PROJECT_ROOT="${PROJECT_ROOT:-/opt/traffic}"
ENV_FILE="${ENV_FILE:-${PROJECT_ROOT}/.env.cloud}"
LOG_DIR="${PROJECT_ROOT}/logs/cloud_runs"
TIMESTAMP="$(date -u +%Y%m%dT%H%M%SZ)"
RUN_DIR="${LOG_DIR}/${TIMESTAMP}"
RESET_LOG="${RUN_DIR}/reset-realtime.log"

NODE1_IP="${NODE1_IP:-35.224.149.110}"
NODE2_IP="${NODE2_IP:-34.46.107.159}"
NODE3_IP="${NODE3_IP:-34.63.78.147}"
SSH_KEY="${SSH_KEY:-~/.ssh/hung_vm_key}"
SSH_USER="${SSH_USER:-${HUNG_SSH_USER:-$(whoami)}}"
RESET_LOCAL_COMPOSE="${RESET_LOCAL_COMPOSE:-}"
RESET_POSTGRES="${RESET_POSTGRES:-}"
RESET_GCS="${RESET_GCS:-}"

TARGET_NODE="${1:-all}"
if [[ "${TARGET_NODE}" == --node ]]; then
  TARGET_NODE="${2:-all}"
fi

mkdir -p "${RUN_DIR}" 2>/dev/null || sudo mkdir -p "${RUN_DIR}" 2>/dev/null || true
touch "${RESET_LOG}" 2>/dev/null || sudo touch "${RESET_LOG}" 2>/dev/null || true
exec > >(tee -a "${RESET_LOG}" 2>/dev/null) 2>&1

echo "============================================================================"
echo "Realtime-Only Reset — Started at ${TIMESTAMP}"
echo "Run directory: ${RUN_DIR}"
echo "Target: ${TARGET_NODE}"
echo "============================================================================"

# ---------------------------------------------------------------------------
# Load environment
# ---------------------------------------------------------------------------
if [ -f "${ENV_FILE}" ]; then
  set -a
  . "${ENV_FILE}"
  set +a
else
  echo "[WARN] ${ENV_FILE} not found. Using defaults."
fi

POSTGRES_USER="${POSTGRES_USER:-capstone}"
POSTGRES_DB="${POSTGRES_DB:-capstone_db}"
POSTGRES_PREDICTION_TABLE="${POSTGRES_US_PREDICTION_TABLE:-traffic_risk_predictions}"
POSTGRES_TOMTOM_TABLE="${POSTGRES_TOMTOM_TABLE:-traffic_tomtom_incidents}"
FLINK_CHECKPOINT_DIR="${FLINK_CHECKPOINT_DIR:-gs://big-data-group-4-backups/checkpoints/flink}"
SPARK_CHECKPOINT_DIR="${SPARK_CHECKPOINT_DIR:-gs://big-data-group-4-backups/checkpoints/spark}"
SILVER_FEATURES_PATH="${SILVER_FEATURES_PATH:-gs://big-data-group-4-silver/features}"
GOLD_RETRAIN_PATH="${GOLD_RETRAIN_PATH:-gs://big-data-group-4-gold/features/retrain}"
KAFKA_TOPIC_RAW="${KAFKA_TOPIC_RAW:-traffic.us.raw}"
KAFKA_TOPIC_TOMTOM="${KAFKA_TOPIC_TOMTOM:-traffic.tomtom.raw}"

bool_enabled() {
  case "${1:-}" in
    true|TRUE|1|yes|YES|y|Y) return 0 ;;
    *) return 1 ;;
  esac
}

compose_down_if_present() {
  local compose_dir="$1"
  local compose_file="$2"
  local compose_project_name="${3:-}"
  if [ ! -f "${compose_file}" ]; then
    return 0
  fi
  if [ -n "${compose_project_name}" ]; then
    COMPOSE_PROJECT_NAME="${compose_project_name}" docker compose \
      --project-directory "${compose_dir}" \
      --env-file "${ENV_FILE}" \
      -f "${compose_file}" \
      down --volumes --remove-orphans 2>/dev/null || true
    return 0
  fi
  docker compose \
    --project-directory "${compose_dir}" \
    --env-file "${ENV_FILE}" \
    -f "${compose_file}" \
    down --volumes --remove-orphans 2>/dev/null || true
}

delete_generated_gcs_data() {
  echo "  Deleting Flink checkpoints: ${FLINK_CHECKPOINT_DIR}"
  gsutil -m rm -r "${FLINK_CHECKPOINT_DIR%/}/**" 2>/dev/null || echo "    (already empty)"

  echo "  Deleting Spark checkpoints: ${SPARK_CHECKPOINT_DIR}"
  gsutil -m rm -r "${SPARK_CHECKPOINT_DIR%/}/**" 2>/dev/null || echo "    (already empty)"

  echo "  Deleting Silver features: ${SILVER_FEATURES_PATH}"
  gsutil -m rm -r "${SILVER_FEATURES_PATH%/}/**" 2>/dev/null || echo "    (already empty)"

  echo "  Deleting Gold retrain data: ${GOLD_RETRAIN_PATH}"
  gsutil -m rm -r "${GOLD_RETRAIN_PATH%/}/**" 2>/dev/null || echo "    (already empty)"
}

run_local_reset_if_requested() {
  if [ -z "${RESET_LOCAL_COMPOSE}${RESET_POSTGRES}${RESET_GCS}" ]; then
    return 1
  fi

  echo ""
  echo "=== Local VM reset mode ==="
  echo "RESET_LOCAL_COMPOSE=${RESET_LOCAL_COMPOSE:-false}"
  echo "RESET_POSTGRES=${RESET_POSTGRES:-false}"
  echo "RESET_GCS=${RESET_GCS:-false}"

  if bool_enabled "${RESET_LOCAL_COMPOSE}"; then
    echo "  Stopping local Node 2/3 streaming and batch Compose stacks if present..."
    compose_down_if_present \
      "${PROJECT_ROOT}/deployment/node2-streaming" \
      "${PROJECT_ROOT}/deployment/node2-streaming/docker-compose.yaml" \
      "${NODE2_COMPOSE_PROJECT_NAME:-node2-streaming}"
    compose_down_if_present \
      "${PROJECT_ROOT}/deployment/node3-batch" \
      "${PROJECT_ROOT}/deployment/node3-batch/docker-compose.yaml"
    docker volume prune -f 2>/dev/null || true
  fi

  if bool_enabled "${RESET_POSTGRES}"; then
    if ! docker ps --format '{{.Names}}' | grep -q '^node1-postgres$' && [ -f "${PROJECT_ROOT}/deployment/node1-control/docker-compose.yaml" ]; then
      echo "  node1-postgres is not running. Starting PostgreSQL only so serving tables can be reset..."
      docker compose \
        --project-directory "${PROJECT_ROOT}/deployment/node1-control" \
        --env-file "${ENV_FILE}" \
        -f "${PROJECT_ROOT}/deployment/node1-control/docker-compose.yaml" \
        up -d postgres >/dev/null 2>&1 || true
      for attempt in $(seq 1 30); do
        if docker ps --format '{{.Names}}' | grep -q '^node1-postgres$' \
          && docker exec node1-postgres pg_isready -U "${POSTGRES_USER}" -d "${POSTGRES_DB}" >/dev/null 2>&1; then
          break
        fi
        sleep 2
      done
    fi

    if docker ps --format '{{.Names}}' | grep -q '^node1-postgres$'; then
      echo "  Dropping realtime PostgreSQL serving tables on this VM..."
      docker exec -i node1-postgres psql -U "${POSTGRES_USER}" -d "${POSTGRES_DB}" <<SQL
DROP TABLE IF EXISTS ${POSTGRES_PREDICTION_TABLE} CASCADE;
DROP TABLE IF EXISTS ${POSTGRES_TOMTOM_TABLE} CASCADE;
SQL
    else
      echo "  node1-postgres is not running on this VM. Skipping table reset."
    fi
  fi

  if bool_enabled "${RESET_GCS}"; then
    if command -v gsutil >/dev/null 2>&1; then
      echo "  Deleting generated GCS checkpoints and Silver/Gold data..."
      delete_generated_gcs_data
    else
      echo "  WARNING: gsutil is unavailable. Skipping GCS cleanup."
    fi
  fi

  echo "Local VM reset mode completed at $(date -u +%Y-%m-%dT%H:%M:%SZ)."
  return 0
}

if run_local_reset_if_requested; then
  exit 0
fi

# ===========================================================================
# Helper: SSH exec on a remote node
# ===========================================================================
ssh_node() {
  local node_ip="$1"
  local command="$2"
  ssh -i "${SSH_KEY}" \
    -o IdentitiesOnly=yes \
    -o StrictHostKeyChecking=no \
    -o UserKnownHostsFile=/dev/null \
    -o ConnectTimeout=15 \
    "${SSH_USER}@${node_ip}" "${command}"
}

# ===========================================================================
# Step 1: Stop Node 2 streaming services (Kafka, Flink, producers)
# ===========================================================================
step1_stop_node2() {
  echo ""
  echo "=== Step 1: Stopping Node 2 streaming services ==="
  if [[ "${TARGET_NODE}" == "all" || "${TARGET_NODE}" == "2" ]]; then
    ssh_node "${NODE2_IP}" "
      set -e
      cd /opt/traffic || exit 1
      echo '  Stopping Node 2 Docker Compose services...'
      docker compose \
        --project-directory deployment/node2-streaming \
        --env-file .env.cloud \
        -f deployment/node2-streaming/docker-compose.yaml \
        down --volumes --remove-orphans 2>/dev/null || true
      echo '  Node 2 services stopped.'
    "
  else
    echo "  Skipping Node 2 (target: ${TARGET_NODE})"
  fi
}

# ===========================================================================
# Step 2: Stop Node 3 batch services (Spark)
# ===========================================================================
step2_stop_node3() {
  echo ""
  echo "=== Step 2: Stopping Node 3 batch services ==="
  if [[ "${TARGET_NODE}" == "all" || "${TARGET_NODE}" == "3" ]]; then
    ssh_node "${NODE3_IP}" "
      set -e
      cd /opt/traffic || exit 1
      echo '  Stopping Node 3 Docker Compose services...'
      docker compose \
        --project-directory deployment/node3-batch \
        --env-file .env.cloud \
        -f deployment/node3-batch/docker-compose.yaml \
        down --volumes --remove-orphans 2>/dev/null || true
      echo '  Node 3 services stopped.'
    "
  else
    echo "  Skipping Node 3 (target: ${TARGET_NODE})"
  fi
}

# ===========================================================================
# Step 3: Reset PostgreSQL serving tables on Node 1
# ===========================================================================
step3_reset_postgres() {
  echo ""
  echo "=== Step 3: Resetting PostgreSQL serving tables ==="
  if [[ "${TARGET_NODE}" == "all" || "${TARGET_NODE}" == "1" ]]; then
    ssh_node "${NODE1_IP}" "
      set -e
      if docker ps --format '{{.Names}}' | grep -q '^node1-postgres\$'; then
        echo '  Dropping realtime prediction tables...'
        docker exec -i node1-postgres psql -U ${POSTGRES_USER} -d ${POSTGRES_DB} <<SQL
DROP TABLE IF EXISTS ${POSTGRES_PREDICTION_TABLE} CASCADE;
DROP TABLE IF EXISTS ${POSTGRES_TOMTOM_TABLE} CASCADE;
SQL
        echo '  Tables dropped successfully.'
      else
        echo '  Node 1 PostgreSQL container not running. Skipping table reset.'
      fi
    "
  else
    echo "  Skipping PostgreSQL reset (target: ${TARGET_NODE})"
  fi
}

# ===========================================================================
# Step 4: Reset Kafka topics on Node 2
# ===========================================================================
step4_reset_kafka() {
  echo ""
  echo "=== Step 4: Resetting Kafka topics ==="
  if [[ "${TARGET_NODE}" == "all" || "${TARGET_NODE}" == "2" ]]; then
    ssh_node "${NODE2_IP}" "
      set -e
      echo '  Deleting Kafka topics...'
      # Kafka topics are auto-created, but we delete to clear committed offsets
      for topic in ${KAFKA_TOPIC_RAW} ${KAFKA_TOPIC_TOMTOM}; do
        docker exec node2-kafka-1 kafka-topics.sh --delete --topic \"\${topic}\" \
          --bootstrap-server localhost:9092 2>/dev/null || echo \"    (Topic \${topic} may not exist yet)\"
      done
      echo '  Kafka topics reset.'
    "
  else
    echo "  Skipping Kafka reset (target: ${TARGET_NODE})"
  fi
}

# ===========================================================================
# Step 5: Delete GCS checkpoints and data
# ===========================================================================
step5_reset_gcs() {
  echo ""
  echo "=== Step 5: Deleting GCS checkpoints and generated data ==="

  delete_generated_gcs_data

  echo "  GCS reset completed."
}

# ===========================================================================
# Step 6: Clear local Docker volumes on all nodes
# ===========================================================================
step6_clear_volumes() {
  echo ""
  echo "=== Step 6: Clearing orphaned Docker volumes ==="

  for node_ip in "${NODE1_IP}" "${NODE2_IP}" "${NODE3_IP}"; do
    local node_name
    case "${node_ip}" in
      "${NODE1_IP}") node_name="node1-control" ;;
      "${NODE2_IP}") node_name="node2-streaming" ;;
      "${NODE3_IP}") node_name="node3-batch" ;;
      *) node_name="${node_ip}" ;;
    esac

    if [[ "${TARGET_NODE}" != "all" ]]; then
      local target_idx
      case "${TARGET_NODE}" in
        1) target_idx="node1" ;;
        2) target_idx="node2" ;;
        3) target_idx="node3" ;;
        *) target_idx="${TARGET_NODE}" ;;
      esac
      if [[ "${node_name}" != "${target_idx}"* ]] && [[ "${node_name}" != "${target_idx}" ]]; then
        echo "  Skipping ${node_name} (target: ${TARGET_NODE})"
        continue
      fi
    fi

    echo "  Pruning volumes on ${node_name}..."
    ssh_node "${node_ip}" "
      docker volume prune -f 2>/dev/null || true
      echo '    Done.'
    " || echo "    (Connection failed, skipping)"
  done
}

# ===========================================================================
# Step 7: Restart Node 2 + Node 3 services
# ===========================================================================
step7_restart_nodes() {
  echo ""
  echo "=== Step 7: Restarting Node 2 (streaming) and Node 3 (batch) ==="
  if [[ "${TARGET_NODE}" == "all" || "${TARGET_NODE}" == "2" ]]; then
    ssh_node "${NODE2_IP}" "
      set -e
      cd /opt/traffic || exit 1
      echo '  Starting Node 2 services...'
      docker compose \
        --project-directory deployment/node2-streaming \
        --env-file .env.cloud \
        -f deployment/node2-streaming/docker-compose.yaml \
        up -d --remove-orphans
      echo '  Node 2 restarted.'
    "
  fi

  if [[ "${TARGET_NODE}" == "all" || "${TARGET_NODE}" == "3" ]]; then
    ssh_node "${NODE3_IP}" "
      set -e
      cd /opt/traffic || exit 1
      echo '  Starting Node 3 services...'
      docker compose \
        --project-directory deployment/node3-batch \
        --env-file .env.cloud \
        -f deployment/node3-batch/docker-compose.yaml \
        up -d --remove-orphans
      echo '  Node 3 restarted.'
    "
  fi
}

# ===========================================================================
# Main execution
# ===========================================================================
step1_stop_node2
step2_stop_node3
step3_reset_postgres
step4_reset_kafka
step5_reset_gcs
step6_clear_volumes
step7_restart_nodes

echo ""
echo "============================================================================"
echo "Realtime-Only Reset — Completed at $(date -u +%Y-%m-%dT%H:%M:%SZ)"
echo "Log file: ${RESET_LOG}"
echo "============================================================================"
echo ""
echo "Next steps:"
echo "  1. Verify Kafka topics recreated:"
echo "     ssh ${SSH_USER}@${NODE2_IP} 'docker exec node2-kafka-1 kafka-topics.sh --list --bootstrap-server localhost:9092'"
echo ""
echo "  2. Verify PostgreSQL tables recreated by Flink on first event:"
echo "     curl -fsS http://${NODE1_IP}:8000/api/v1/overview/summary"
echo ""
echo "  3. Monitor dashboard at http://${NODE1_IP}:3001"
echo ""
echo "  The pipeline will begin processing data as producers reconnect and Flink"
echo "  reads from fresh Kafka offsets. PostgreSQL tables are created automatically"
echo "  by Flink on the first incoming event."
