#!/usr/bin/env python3
"""
orchestration/dags/dag_ml_pipeline.py
Airflow DAG: model_retrain_hourly

Triggers the US accident severity model retraining pipeline every 20 minutes.
Uses gcloud compute ssh to start run-node3.sh on node3-batch.
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
    description="H2O AutoML retraining every 20 min via gcloud compute ssh",
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
            gcloud compute ssh node3-batch \
                --project=big-data-group-4 \
                --zone=us-central1-a \
                --command="cd /opt/traffic && NODE3_RESET_LOCAL_SILVER_SNAPSHOT=true NODE3_RESET_LOCAL_GOLD_SNAPSHOT=true H2O_MAX_RUNTIME=1200 NODE3_LOCK_BUSY_EXIT_CODE=0 bash scripts/gcp/run-node3.sh" \
                -- -o StrictHostKeyChecking=no -o ConnectTimeout=30
            echo "=== [Airflow DAG] Retrain completed at $(date -u) ==="
        """,
        execution_timeout=timedelta(hours=3),
    )

    notify = BashOperator(
        task_id="notify_success",
        bash_command="echo 'Retrain pipeline finished at $(date -u)'",
    )

    spark_and_h2o >> notify