#!/usr/bin/env python3
"""
orchestration/dags/dag_ml_pipeline.py
Airflow DAG: model_retrain_hourly

Triggers the US accident severity model retraining pipeline every 60 minutes.
Uses internal SSH to launch run-node3.sh on node3-batch (10.128.0.8) in the
background, then reads Node 3's status file.
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

RETRAIN_INTERVAL_MINUTES = int(
    os.getenv("AIRFLOW_MODEL_RETRAIN_INTERVAL_MINUTES", "60")
)

with DAG(
    dag_id="model_retrain_hourly",
    default_args=default_args,
    description="H2O AutoML retraining every 60 min via internal SSH",
    # Use a real 60-minute cadence between scheduled runs so the next tick
    # waits for a full interval instead of depending on a cron string.
    schedule_interval=timedelta(minutes=RETRAIN_INTERVAL_MINUTES),
    start_date=datetime(2026, 5, 1),
    catchup=False,
    max_active_runs=1,
    tags=["ml", "retrain", "batch", "spark", "h2o"],
) as dag:

    trigger_retrain = BashOperator(
        task_id="trigger_node3_retrain_on_node3",
        bash_command="""
            set -euo pipefail
            echo "=== [Airflow DAG] Launching Node3 retrain worker in background ==="
            SSH_KEY_PATH="${SSH_KEY:-/opt/airflow/.ssh/traffic-inter-node.key}"
            SSH_USER="${HUNG_SSH_USER:-runner}"
            NODE3_IP="${NODE3_INTERNAL_IP:-10.128.0.8}"
            resolve_ssh_key() {
                local candidate
                for candidate in "$@"; do
                    if [ -f "${candidate}" ] && [ -r "${candidate}" ]; then
                        printf '%s\n' "${candidate}"
                        return 0
                    fi
                    if [ -d "${candidate}" ]; then
                        local discovered
                        discovered="$(find "${candidate}" -maxdepth 2 -type f -readable | head -n 1 || true)"
                        if [ -n "${discovered}" ]; then
                            printf '%s\n' "${discovered}"
                            return 0
                        fi
                    fi
                done
                return 1
            }
            RESOLVED_SSH_KEY="$(resolve_ssh_key \
                "${SSH_KEY_PATH}" \
                "/opt/airflow/.ssh/traffic-inter-node.key" \
                "/run/secrets/google_compute_engine/traffic-inter-node.key" \
                "/run/secrets/google_compute_engine/google_compute_engine" \
                "/run/secrets/google_compute_engine" \
                "/run/secrets")" || true
            if [ -n "${RESOLVED_SSH_KEY}" ]; then
                SSH_KEY_PATH="${RESOLVED_SSH_KEY}"
            fi
            echo "Node3 target: ${SSH_USER}@${NODE3_IP}"
            echo "SSH key path: ${SSH_KEY_PATH}"

            if [ ! -f "${SSH_KEY_PATH}" ] || [ ! -r "${SSH_KEY_PATH}" ]; then
                echo "ERROR: SSH key is unavailable or unreadable at ${SSH_KEY_PATH}. Cannot trigger retrain."
                exit 1
            fi

            echo "Using SSH key: ${SSH_KEY_PATH}"
            ssh -i "${SSH_KEY_PATH}" \
                -o StrictHostKeyChecking=no \
                -o ConnectTimeout=20 \
                -o ServerAliveInterval=15 \
                -o ServerAliveCountMax=2 \
                "${SSH_USER}@${NODE3_IP}" \
                "cd /opt/traffic && bash scripts/gcp/launch-node3-retrain.sh"

            echo "=== [Airflow DAG] Node3 retrain worker launched at $(date -u) ==="
        """,
        env={
            "HUNG_SSH_USER": "runner",
            "SSH_KEY": "/opt/airflow/.ssh/traffic-inter-node.key",
            "NODE3_INTERNAL_IP": "10.128.0.8",
        },
        execution_timeout=timedelta(minutes=5),
    )

    check_retrain_status = BashOperator(
        task_id="check_node3_retrain_status",
        bash_command="""
            set -euo pipefail
            echo "=== [Airflow DAG] Reading Node3 retrain status ==="
            SSH_KEY_PATH="${SSH_KEY:-/opt/airflow/.ssh/traffic-inter-node.key}"
            SSH_USER="${HUNG_SSH_USER:-runner}"
            NODE3_IP="${NODE3_INTERNAL_IP:-10.128.0.8}"
            resolve_ssh_key() {
                local candidate
                for candidate in "$@"; do
                    if [ -f "${candidate}" ] && [ -r "${candidate}" ]; then
                        printf '%s\n' "${candidate}"
                        return 0
                    fi
                    if [ -d "${candidate}" ]; then
                        local discovered
                        discovered="$(find "${candidate}" -maxdepth 2 -type f -readable | head -n 1 || true)"
                        if [ -n "${discovered}" ]; then
                            printf '%s\n' "${discovered}"
                            return 0
                        fi
                    fi
                done
                return 1
            }
            RESOLVED_SSH_KEY="$(resolve_ssh_key \
                "${SSH_KEY_PATH}" \
                "/opt/airflow/.ssh/traffic-inter-node.key" \
                "/run/secrets/google_compute_engine/traffic-inter-node.key" \
                "/run/secrets/google_compute_engine/google_compute_engine" \
                "/run/secrets/google_compute_engine" \
                "/run/secrets")" || true
            if [ -n "${RESOLVED_SSH_KEY}" ]; then
                SSH_KEY_PATH="${RESOLVED_SSH_KEY}"
            fi

            if [ ! -f "${SSH_KEY_PATH}" ] || [ ! -r "${SSH_KEY_PATH}" ]; then
                echo "ERROR: SSH key is unavailable or unreadable at ${SSH_KEY_PATH}. Cannot inspect Node3 retrain status."
                exit 1
            fi

            STATUS_PAYLOAD="$(ssh -i "${SSH_KEY_PATH}" \
                -o StrictHostKeyChecking=no \
                -o ConnectTimeout=20 \
                -o ServerAliveInterval=15 \
                -o ServerAliveCountMax=2 \
                "${SSH_USER}@${NODE3_IP}" \
                "python3 - <<'PY'\nimport json\nfrom pathlib import Path\nstatus_path = Path('/opt/traffic/logs/retrain_state/node3-retrain-status.json')\npid_path = Path('/opt/traffic/logs/retrain_state/node3-retrain.pid')\npayload = {'status': 'continue', 'phase': 'unknown', 'message': 'No Node3 retrain status file exists yet.'}\nif status_path.exists():\n    payload = json.loads(status_path.read_text(encoding='utf-8'))\npid = None\nif pid_path.exists():\n    try:\n        pid = int(pid_path.read_text(encoding='utf-8').strip())\n    except ValueError:\n        pid = None\npayload['running'] = False\npayload['pid'] = pid\nif pid is not None:\n    try:\n        import os\n        os.kill(pid, 0)\n        payload['running'] = True\n    except OSError:\n        payload['running'] = False\nprint(json.dumps(payload, ensure_ascii=True))\nPY")"

            echo "${STATUS_PAYLOAD}"
            export STATUS_PAYLOAD
            python3 - <<'PY'
import json
import os
import sys

payload = json.loads(os.environ["STATUS_PAYLOAD"])
status = str(payload.get("status", "continue")).lower()
running = bool(payload.get("running"))

if status == "failed":
    raise SystemExit("Node3 retrain status is FAILED. Inspect node3-retrain-runner.log on node3.")

if running:
    print("Node3 retrain worker is still running in background. Airflow will allow it to continue.")
else:
    print(f"Node3 retrain worker is idle with status={status}.")
PY
        """,
        env={
            "HUNG_SSH_USER": "runner",
            "SSH_KEY": "/opt/airflow/.ssh/traffic-inter-node.key",
            "NODE3_INTERNAL_IP": "10.128.0.8",
        },
        execution_timeout=timedelta(minutes=5),
    )

    notify = BashOperator(
        task_id="notify_success",
        bash_command="echo 'Retrain pipeline finished at $(date -u)'",
    )

    trigger_retrain >> check_retrain_status >> notify
