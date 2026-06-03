#!/bin/bash
# =============================================================================
# Full US Replay Reset - Reset to events=0, replay US after-2020 from scratch
# =============================================================================
# Steps:
#   1. Pause Airflow automation DAGs
#   2. Stop Node 2 producers + Flink
#   3. Drop PostgreSQL prediction tables
#   4. Delete GCS Silver + Gold + checkpoints
#   5. Restart Node 2 (fresh Kafka topics, Flink, producers from row 0)
#   6. Restart Node 3 Spark + H2O with fresh local snapshots
#   7. Resume Airflow DAGs
#   8. Verify dashboard shows events=0 then grows over time
# =============================================================================
set -euo pipefail

PROJECT_ID="${GCP_PROJECT_ID:-big-data-group-4}"
ZONE="${GCP_ZONE:-us-central1-a}"
N1="${NODE1:-node1-control}"
N2="${NODE2:-node2-streaming}"
N3="${NODE3:-node3-batch}"

echo "=============================================="
echo "Full US Replay Reset - $(date -u +%Y-%m-%dT%H:%M:%SZ)"
echo "=============================================="

# Step 1: Pause Airflow
echo "Step 1: Pausing Airflow DAGs..."
gcloud compute ssh ${N1} --project=${PROJECT_ID} --zone=${ZONE} --command="
  sudo docker exec node1-airflow airflow dags pause streaming_health_check 2>/dev/null || true
  sudo docker exec node1-airflow airflow dags pause model_retrain_hourly 2>/dev/null || true
  echo 'DAGs paused'
" 2>&1 || echo "(pause skipped)"

# Step 2: Stop Node 2
echo ""
echo "Step 2: Stopping Node 2 services..."
gcloud compute ssh ${N2} --project=${PROJECT_ID} --zone=${ZONE} --command="
  cd /opt/traffic
  sudo docker compose \
    --project-directory deployment/node2-streaming \
    --env-file .env.cloud \
    -f deployment/node2-streaming/docker-compose.yaml \
    down --volumes --remove-orphans 2>/dev/null || true
  echo 'Node 2 stopped'
" 2>&1

# Step 3: Drop PostgreSQL tables
echo ""
echo "Step 3: Dropping prediction tables..."
gcloud compute ssh ${N1} --project=${PROJECT_ID} --zone=${ZONE} --command="
  sudo docker exec -i node1-postgres psql -U capstone -d capstone_db <<'SQL'
DROP TABLE IF EXISTS traffic_risk_predictions CASCADE;
DROP TABLE IF EXISTS traffic_tomtom_incidents CASCADE;
SQL
  echo 'Tables dropped'
" 2>&1

# Step 4: Delete GCS data
echo ""
echo "Step 4: Deleting GCS Silver/Gold/Checkpoints..."
gcloud compute ssh ${N1} --project=${PROJECT_ID} --zone=${ZONE} --command="
  gsutil -m rm -r gs://big-data-group-4-silver/process/flink_features/** 2>/dev/null || echo '(silver empty)'
  gsutil -m rm -r gs://big-data-group-4-gold/features/retrain/** 2>/dev/null || echo '(gold empty)'
  gsutil -m rm -r gs://big-data-group-4-backups/checkpoints/** 2>/dev/null || echo '(checkpoints empty)'
" 2>&1

# Step 5: Restart Node 2 with fresh producers from row 0
echo ""
echo "Step 5: Starting Node 2 (fresh replay from row 0)..."
gcloud compute ssh ${N2} --project=${PROJECT_ID} --zone=${ZONE} --command="
  cd /opt/traffic
  export NODE2_REFRESH_US_PRODUCERS=true
  export US_REPLAY_START_ROW=0
  export STREAM_THROTTLE_SECONDS=0.0
  bash scripts/gcp/run-node2.sh
" 2>&1

# Step 6: Restart Node 3 with fresh snapshots
echo ""
echo "Step 6: Starting Node 3 (fresh Silver/Gold snapshots)..."
gcloud compute ssh ${N3} --project=${PROJECT_ID} --zone=${ZONE} --command="
  cd /opt/traffic
  export NODE3_RESET_LOCAL_SILVER_SNAPSHOT=true
  export NODE3_RESET_LOCAL_GOLD_SNAPSHOT=true
  export H2O_MAX_RUNTIME=1200
  bash scripts/gcp/run-node3.sh
" 2>&1

# Step 7: Resume Airflow
echo ""
echo "Step 7: Resuming Airflow DAGs..."
gcloud compute ssh ${N1} --project=${PROJECT_ID} --zone=${ZONE} --command="
  sudo docker exec node1-airflow airflow dags unpause streaming_health_check 2>/dev/null || true
  sudo docker exec node1-airflow airflow dags unpause model_retrain_hourly 2>/dev/null || true
  echo 'DAGs resumed'
" 2>&1 || echo "(resume skipped)"

echo ""
echo "=============================================="
echo "Full US Replay Reset - Completed at $(date -u +%Y-%m-%dT%H:%M:%SZ)"
echo "=============================================="
echo "Dashboard: http://35.224.149.110:3001/pipeline"
echo "Prediction rows should show 0 and grow over ~1 day"
echo "Retrain loop should show CONTINUE (not FINISHED)"