#!/bin/bash
# Collect real cloud pipeline metrics after an end-to-end run.

set -euo pipefail

PROJECT_ID="${GCP_PROJECT_ID:-big-data-group-4}"
ZONE="${GCP_ZONE:-us-central1-a}"
NODE1="${NODE1:-node1-control}"
NODE2="${NODE2:-node2-streaming}"
NODE3="${NODE3:-node3-batch}"
PROJECT_ROOT="${PROJECT_ROOT:-/opt/traffic}"
LOG_DIR="${LOG_DIR:-logs/cloud_runs}"
RUN_ID="${RUN_ID:-$(date -u +%Y%m%dT%H%M%SZ)}"
OUTPUT_DIR="${LOG_DIR}/${RUN_ID}"

mkdir -p "${OUTPUT_DIR}"

ssh_cmd() {
  local node="$1"
  shift
  gcloud compute ssh "${node}" \
    --project="${PROJECT_ID}" \
    --zone="${ZONE}" \
    --quiet \
    --command="$*"
}

node_external_ip() {
  local node="$1"
  gcloud compute instances describe "${node}" \
    --project="${PROJECT_ID}" \
    --zone="${ZONE}" \
    --format='get(networkInterfaces[0].accessConfigs[0].natIP)'
}

NODE1_IP="$(node_external_ip "${NODE1}")"
NODE2_IP="$(node_external_ip "${NODE2}")"
NODE3_IP="$(node_external_ip "${NODE3}")"

{
  echo "# Traffic Risk Cloud Metrics"
  echo
  echo "Collected at: $(date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo
  echo "## Public Service URLs"
  echo
  echo "- Dashboard: http://${NODE1_IP}:3001"
  echo "- FastAPI: http://${NODE1_IP}:8000/docs"
  echo "- Airflow: http://${NODE1_IP}:8080"
  echo "- MLflow: http://${NODE1_IP}:5000"
  echo "- Grafana: http://${NODE1_IP}:3000"
  echo "- Prometheus: http://${NODE1_IP}:9090"
  echo "- Flink: http://${NODE2_IP}:8081"
  echo "- Spark: http://${NODE3_IP}:8080"
  echo
  echo "## FastAPI Pipeline Metrics"
  ssh_cmd "${NODE1}" "curl -fsS http://localhost:8000/api/v1/pipeline/throughput?window=15m | python3 -m json.tool"
  echo
  ssh_cmd "${NODE1}" "curl -fsS http://localhost:8000/api/v1/pipeline/latency?metric=avg | python3 -m json.tool"
  echo
  ssh_cmd "${NODE1}" "curl -fsS http://localhost:8000/api/v1/pipeline/replay-health | python3 -m json.tool"
  echo
  echo "## PostgreSQL Table Metrics"
  ssh_cmd "${NODE1}" "cd ${PROJECT_ROOT} && . .env.cloud && for table_name in \"\${POSTGRES_US_PREDICTION_TABLE:-traffic_risk_predictions}\" \"\${POSTGRES_TOMTOM_TABLE:-traffic_tomtom_incidents}\"; do
    if docker exec node1-postgres psql -U \"\$POSTGRES_USER\" -d \"\$POSTGRES_DB\" -At -c \"select to_regclass('public.' || '\$table_name');\" | grep -qv '^$'; then
      docker exec node1-postgres psql -U \"\$POSTGRES_USER\" -d \"\$POSTGRES_DB\" -P pager=off -c \"
SELECT
  '\$table_name' AS table_name,
  COUNT(*)::bigint AS rows,
  MIN(event_time) AS min_event_time,
  MAX(event_time) AS max_event_time,
  AVG(end_to_end_latency_ms)::numeric(12,2) AS avg_e2e_ms,
  percentile_cont(0.95) WITHIN GROUP (ORDER BY end_to_end_latency_ms)::numeric(12,2) AS p95_e2e_ms
FROM \$table_name;
\"
    else
      echo \"Table \$table_name does not exist yet.\"
    fi
  done"
  echo
  echo "## Kafka Topic Offsets"
  ssh_cmd "${NODE2}" "docker exec node2-kafka-1 kafka-run-class kafka.tools.GetOffsetShell --broker-list kafka-1:29092,kafka-2:29092,kafka-3:29092 --topic traffic.us.raw --time -1 || true"
  ssh_cmd "${NODE2}" "docker exec node2-kafka-1 kafka-run-class kafka.tools.GetOffsetShell --broker-list kafka-1:29092,kafka-2:29092,kafka-3:29092 --topic traffic.tomtom.raw --time -1 || true"
  echo
  echo "## Producer Log Rate Samples"
  ssh_cmd "${NODE2}" "docker logs --tail=300 node2-producer-0 2>&1 | grep -E 'rate=|Done\\.' | tail -20 || true"
  ssh_cmd "${NODE2}" "docker logs --tail=300 node2-producer-1 2>&1 | grep -E 'rate=|Done\\.' | tail -20 || true"
  ssh_cmd "${NODE2}" "docker logs --tail=300 node2-producer-2 2>&1 | grep -E 'rate=|Done\\.' | tail -20 || true"
  ssh_cmd "${NODE2}" "docker logs --tail=300 node2-tomtom-producer 2>&1 | grep -E 'TomTom poll summary|Done\\.' | tail -20 || true"
  echo
  echo "## Prometheus Samples"
  ssh_cmd "${NODE1}" "curl -fsS 'http://localhost:9090/api/v1/query?query=sum(rate(traffic_api_requests_total%5B5m%5D))' | python3 -m json.tool"
  ssh_cmd "${NODE1}" "curl -fsS 'http://localhost:9090/api/v1/query?query=histogram_quantile(0.95,sum(rate(traffic_api_request_latency_seconds_bucket%5B5m%5D))%20by%20(le))' | python3 -m json.tool"
  echo
  echo "## Docker Service Status"
  ssh_cmd "${NODE1}" "cd ${PROJECT_ROOT}/deployment/node1-control && docker compose --env-file ${PROJECT_ROOT}/.env.cloud ps"
  ssh_cmd "${NODE2}" "cd ${PROJECT_ROOT}/deployment/node2-streaming && COMPOSE_PROJECT_NAME=node2-streaming docker compose --env-file ${PROJECT_ROOT}/.env.cloud ps"
  ssh_cmd "${NODE3}" "cd ${PROJECT_ROOT}/deployment/node3-batch && docker compose --env-file ${PROJECT_ROOT}/.env.cloud ps"
} | tee "${OUTPUT_DIR}/cloud-metrics.md"

echo "Metrics written to ${OUTPUT_DIR}/cloud-metrics.md"
