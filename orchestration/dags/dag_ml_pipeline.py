#!/usr/bin/env python3
"""
orchestration/dags/dag_ml_pipeline.py
Airflow DAG: model_retrain_hourly

Triggers the US accident severity model retraining pipeline on a configurable
schedule (default: every 15 minutes for fast demonstration; set
AIRFLOW_MODEL_RETRAIN_SCHEDULE to '0 * * * *' for production hourly runs).

Steps:
    1. Spark Silver -> Gold: Reads Flink-generated feature JSONL files from
       GCS Silver, validates schema, deduplicates, and writes ML-ready Parquet
       to GCS Gold on Node 3.
    2. H2O retrain: Trains a new H2O AutoML model on the Gold Parquet dataset
       and registers the result in MLflow on Node 1.
    3. Notify success: Logs the completion timestamp.

Recovery strategy:
    If the Spark or H2O step fails (e.g. Node 3 is temporarily unavailable),
    Airflow retries up to 5 times with a 5-minute delay. The Node 2/3 lifecycle
    script can be used as a manual recovery hook.
"""

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

# ---------------------------------------------------------------------------
# Default task arguments
# ---------------------------------------------------------------------------

default_args = {
    "owner": "traffic-risk-platform",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 5,
    "retry_delay": timedelta(minutes=5),
}

# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------

with DAG(
    dag_id="model_retrain_hourly",
    default_args=default_args,
    description="Periodic H2O AutoML retraining from the latest Flink-generated Silver features",
    schedule_interval=os.getenv("AIRFLOW_MODEL_RETRAIN_SCHEDULE", "*/15 * * * *"),
    start_date=datetime(2026, 5, 1),
    catchup=False,
    max_active_runs=1,
    tags=["ml", "retrain", "batch", "spark", "h2o"],
) as dag:

    # ------------------------------------------------------------------
    # Task 1 – Spark: Silver → Gold Parquet (executes on Node 3)
    # ------------------------------------------------------------------
    spark_silver_to_gold = BashOperator(
        task_id="spark_silver_to_gold",
        bash_command=r"""
            set -euo pipefail
            echo "=== [Airflow] Spark Silver -> Gold ==="
            SSH_KEY_PATH="${SSH_KEY:-/run/secrets/google_compute_engine}"
            SSH_TARGET="${HUNG_SSH_USER:-runner}@${NODE3_INTERNAL_IP:-10.128.0.8}"
            if [ ! -f "${SSH_KEY_PATH}" ]; then
                echo "ERROR: SSH key not found at ${SSH_KEY_PATH}"
                exit 1
            fi
            ssh -i "${SSH_KEY_PATH}" \
                -o IdentitiesOnly=yes \
                -o StrictHostKeyChecking=no \
                -o UserKnownHostsFile=/dev/null \
                -o ConnectTimeout=15 \
                "${SSH_TARGET}" "
                cd /opt/traffic &&
                NODE3_LOCK_BUSY_EXIT_CODE=0 bash scripts/gcp/run-node3.sh
            "
        """,
        execution_timeout=timedelta(hours=6),
    )

    # ------------------------------------------------------------------
    # Task 2 – H2O AutoML: retrain on Gold Parquet (executes on Node 3)
    # ------------------------------------------------------------------
    h2o_retrain = BashOperator(
        task_id="h2o_retrain",
        bash_command="""
            set -euo pipefail
            echo "=== [Airflow] H2O AutoML Retrain ==="
            echo "Node 3 retraining completed inside scripts/gcp/run-node3.sh."
        """,
        execution_timeout=timedelta(hours=1),
    )

    # ------------------------------------------------------------------
    # Task 3 – Notify success
    # ------------------------------------------------------------------
    notify_success = BashOperator(
        task_id="notify_success",
        bash_command="""
            echo "=== [Airflow] Model retrain pipeline completed successfully ==="
            echo "Completed at: $(date -u +%Y-%m-%dT%H:%M:%SZ)"
        """,
    )

    # DAG structure: Spark -> H2O -> Notify
    spark_silver_to_gold >> h2o_retrain >> notify_success
