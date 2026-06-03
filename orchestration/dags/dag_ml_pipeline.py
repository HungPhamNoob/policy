#!/usr/bin/env python3
"""
orchestration/dags/dag_ml_pipeline.py
Airflow DAG: model_retrain_hourly

Triggers the US accident severity model retraining pipeline every 20 minutes.
Uses internal SSH to start run-node3.sh on node3-batch (10.128.0.8).
"""

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "traffic-risk-platform",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="model_retrain_hourly",
    default_args=default_args,
    description="H2O AutoML retraining every 20 min via internal SSH",
    schedule_interval=os.getenv("AIRFLOW_MODEL_RETRAIN_SCHEDULE", "*/20 * * * *"),
    start_date=datetime(2026, 5, 1),
    catchup=False,
    max_active_runs=1,
    tags=["ml", "retrain", "batch", "spark", "h2o"],
) as dag:

    spark_and_h2o = BashOperator(
        task_id="spark_and_h2o_retrain_on_node3",
        bash_command="""
            set -euo pipefail
            echo "=== [Airflow DAG] Triggering Node3 retrain (Spark + H2O) ==="
            echo "Node3 target: ${NODE3_INTERNAL_IP:-10.128.0.8}"
            SSH_KEY_PATH="${SSH_KEY:-/run/secrets/google_compute_engine}"
            if [ ! -f "$SSH_KEY_PATH" ]; then
                echo "=== SSH key not found at $SSH_KEY_PATH, trying gcloud ==="
                gcloud compute ssh node3-batch --project=big-data-group-4 --zone=us-central1-a --command="cd /opt/traffic && RETRAIN_MIN_US_ROWS=0 NODE3_RESET_LOCAL_SILVER_SNAPSHOT=true NODE3_RESET_LOCAL_GOLD_SNAPSHOT=true H2O_MAX_RUNTIME=1800 NODE3_LOCK_BUSY_EXIT_CODE=0 bash scripts/gcp/run-node3.sh" -- -o StrictHostKeyChecking=no -o ConnectTimeout=60
            else
                echo "Using SSH key: $SSH_KEY_PATH"
                ssh -i "$SSH_KEY_PATH" -o StrictHostKeyChecking=no -o ConnectTimeout=60 "${HUNG_SSH_USER:-hung}@${NODE3_INTERNAL_IP:-10.128.0.8}" "cd /opt/traffic && RETRAIN_MIN_US_ROWS=0 NODE3_RESET_LOCAL_SILVER_SNAPSHOT=true NODE3_RESET_LOCAL_GOLD_SNAPSHOT=true H2O_MAX_RUNTIME=1800 NODE3_LOCK_BUSY_EXIT_CODE=0 bash scripts/gcp/run-node3.sh"
            fi
            echo "=== [Airflow DAG] Retrain completed at $(date -u) ==="
        """,
        env={
            "HUNG_SSH_USER": "{{ var.value.get('hung_ssh_user', 'hung') }}",
            "SSH_KEY": "{{ var.value.get('ssh_key', '/run/secrets/google_compute_engine') }}",
            "NODE3_INTERNAL_IP": "{{ var.value.get('node3_internal_ip', '10.128.0.8') }}",
        },
        execution_timeout=timedelta(hours=3),
    )

    notify = BashOperator(
        task_id="notify_success",
        bash_command="echo 'Retrain pipeline finished at $(date -u)'",
    )

    spark_and_h2o >> notify