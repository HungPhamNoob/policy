#!/bin/bash
# Reset only the US after-2020 replay path back to row 0 while preserving
# TomTom live ingestion and the rest of the serving stack.

set -euo pipefail

PROJECT_ID="${GCP_PROJECT_ID:-big-data-group-4}"
ZONE="${GCP_ZONE:-us-central1-a}"
NODE1="${NODE1:-node1-control}"
NODE2="${NODE2:-node2-streaming}"

US_REPLAY_THROTTLE_SECONDS="${US_REPLAY_THROTTLE_SECONDS:-0.072}"
US_REPLAY_LOOP_FOREVER="${US_REPLAY_LOOP_FOREVER:-false}"
US_REPLAY_START_ROW="${US_REPLAY_START_ROW:-0}"
POSTGRES_TABLE="${POSTGRES_US_PREDICTION_TABLE:-traffic_risk_predictions}"
KAFKA_TOPIC_RAW="${KAFKA_TOPIC_RAW:-traffic.us.raw}"
KAFKA_REPLICATION_FACTOR="${KAFKA_REPLICATION_FACTOR:-3}"
KAFKA_PARTITIONS="${KAFKA_PARTITIONS:-3}"
FLINK_CHECKPOINT_DIR="${FLINK_CHECKPOINT_DIR:-gs://big-data-group-4-backups/checkpoints/flink}"

echo "=============================================="
echo "US Replay Only Reset - $(date -u +%Y-%m-%dT%H:%M:%SZ)"
echo "=============================================="
echo "Replay throttle: ${US_REPLAY_THROTTLE_SECONDS}s"
echo "Replay loop forever: ${US_REPLAY_LOOP_FOREVER}"

echo ""
echo "Step 1: Pause retrain DAG during the replay reset window..."
gcloud compute ssh "${NODE1}" --project="${PROJECT_ID}" --zone="${ZONE}" --command="
  sudo docker exec node1-airflow airflow dags pause model_retrain_hourly 2>/dev/null || true
  echo 'model_retrain_hourly paused'
"

echo ""
echo "Step 2: Stop only US replay + unified Flink services on Node 2..."
gcloud compute ssh "${NODE2}" --project="${PROJECT_ID}" --zone="${ZONE}" --command="
  cd /opt/traffic
  COMPOSE_PROJECT_NAME=node2-streaming sudo docker compose \
    -f deployment/node2-streaming/docker-compose.yaml \
    stop producer-1 producer-2 producer-3 flink-python-job flink-jobmanager flink-taskmanager
"

echo ""
echo "Step 3: Reset the served US replay table to zero rows..."
gcloud compute ssh "${NODE1}" --project="${PROJECT_ID}" --zone="${ZONE}" --command="
  sudo docker exec node1-postgres psql -U capstone -d capstone_db -c 'TRUNCATE TABLE ${POSTGRES_TABLE};'
"

echo ""
echo "Step 4: Recreate only the Kafka topic for US replay..."
gcloud compute ssh "${NODE2}" --project="${PROJECT_ID}" --zone="${ZONE}" --command="
  sudo docker exec node2-kafka-1 bash -lc '
    kafka-topics --bootstrap-server kafka-1:29092,kafka-2:29092,kafka-3:29092 --delete --if-exists --topic ${KAFKA_TOPIC_RAW} || true
    sleep 5
    kafka-topics --bootstrap-server kafka-1:29092,kafka-2:29092,kafka-3:29092 --create --if-not-exists --topic ${KAFKA_TOPIC_RAW} --partitions ${KAFKA_PARTITIONS} --replication-factor ${KAFKA_REPLICATION_FACTOR}
    kafka-topics --bootstrap-server kafka-1:29092,kafka-2:29092,kafka-3:29092 --describe --topic ${KAFKA_TOPIC_RAW}
  '
"

echo ""
echo "Step 5: Clear Flink checkpoints so the US consumer restarts from row 0..."
gcloud compute ssh "${NODE2}" --project="${PROJECT_ID}" --zone="${ZONE}" --command="
  sudo rm -rf /opt/flink/checkpoints/unified-traffic-streaming/* /opt/flink/tmp/* 2>/dev/null || true
  gsutil -m rm -r '${FLINK_CHECKPOINT_DIR%/}/**' 2>/dev/null || true
"

echo ""
echo "Step 6: Restart Flink + US replay producers with the new pacing..."
gcloud compute ssh "${NODE2}" --project="${PROJECT_ID}" --zone="${ZONE}" --command="
  cd /opt/traffic
  sudo env \
    NODE2_REFRESH_US_PRODUCERS=true \
    US_REPLAY_START_ROW=${US_REPLAY_START_ROW} \
    STREAM_THROTTLE_SECONDS=${US_REPLAY_THROTTLE_SECONDS} \
    STREAM_LOOP_FOREVER=${US_REPLAY_LOOP_FOREVER} \
    COMPOSE_PROJECT_NAME=node2-streaming \
    docker compose \
    --env-file .env.cloud \
    -f deployment/node2-streaming/docker-compose.yaml \
    up -d flink-jobmanager flink-taskmanager flink-python-job producer-1 producer-2 producer-3
"

echo ""
echo "Step 7: Resume retrain DAG..."
gcloud compute ssh "${NODE1}" --project="${PROJECT_ID}" --zone="${ZONE}" --command="
  sudo docker exec node1-airflow airflow dags unpause model_retrain_hourly 2>/dev/null || true
  echo 'model_retrain_hourly resumed'
"

echo ""
echo "Step 8: Verify replay starts from zero and grows again..."
gcloud compute ssh "${NODE1}" --project="${PROJECT_ID}" --zone="${ZONE}" --command="
  curl -fsS http://localhost:8000/api/v1/pipeline/replay-health
"

echo ""
echo "US replay reset complete. TomTom live was left running."
