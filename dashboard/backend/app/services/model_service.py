"""MLflow model history helpers for the dashboard backend."""

from __future__ import annotations

import json
import math
import os
import subprocess
from typing import Any

from app.core.config import get_settings
from app.services.model_history_seed import SEEDED_RETRAIN_HISTORY


TRACKED_METRICS = [
    "accuracy",
    "macro_precision",
    "macro_recall",
    "macro_f1",
    "weighted_precision",
    "weighted_recall",
    "weighted_f1",
    "logloss",
]
ACTIVE_NODE3_PHASES = {
    "starting",
    "bootstrap",
    "syncing_silver",
    "spark",
    "h2o",
    "logging_models",
    "registering_model",
}


def _coerce_int(value: Any) -> int | None:
    value = _clean_value(value)
    if value is None:
        return None
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return None


def _expected_model_count(row: Any) -> int | None:
    return _coerce_int(
        row.get("params.top_k_models")
        or row.get("params.expected_top_models")
        or row.get("params.H2O_TOP_K_MODELS")
    )


def _logged_model_count(row: Any) -> int | None:
    return _coerce_int(
        row.get("metrics.models_logged") or row.get("params.models_logged")
    )


def _is_complete_retrain_batch(row: Any) -> bool | None:
    run_role = str(_clean_value(row.get("tags.run_role")) or "")
    if run_role != "retrain_parent":
        return None

    status = str(_clean_value(row.get("status")) or "").upper()
    expected = _expected_model_count(row)
    logged = _logged_model_count(row)
    if status != "FINISHED" or expected is None or logged is None:
        return False
    return logged >= expected


def _is_retrain_run(row: Any) -> bool:
    run_name = str(_clean_value(row.get("tags.mlflow.runName")) or "")
    run_type_tag = str(_clean_value(row.get("tags.run_type")) or "")
    run_type_param = str(_clean_value(row.get("params.run_type")) or "")
    run_role = str(_clean_value(row.get("tags.run_role")) or "")
    parent_id = _clean_value(row.get("tags.mlflow.parentRunId"))
    if run_role == "retrain_parent":
        return True
    if parent_id:
        return False
    return (
        run_name == "h2o_retrain_online"
        or run_type_tag == "retrain_online"
        or run_type_param == "retrain_online"
    )


def _clean_value(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, float) and math.isnan(value):
        return None
    return value


def _mlflow_unavailable(exc: Exception) -> dict[str, Any]:
    return {
        "status": "unavailable",
        "runs": [],
        "metrics": TRACKED_METRICS,
        "error": str(exc)[:200],
    }


def _seed_history(limit: int, experiment_name: str) -> dict[str, Any]:
    runs = SEEDED_RETRAIN_HISTORY[: max(0, limit)]
    return {
        "status": "seeded" if runs else "not_enough_data",
        "experiment": experiment_name,
        "runs": runs,
        "metrics": TRACKED_METRICS,
        "source": "seed_fallback",
    }


def _backfill_seed_runs(output: list[dict[str, Any]], limit: int) -> list[dict[str, Any]]:
    if len(output) >= limit:
        return output[:limit]

    seen_run_ids = {str(run.get("run_id")) for run in output if run.get("run_id")}
    for seeded_run in SEEDED_RETRAIN_HISTORY:
        if len(output) >= limit:
            break
        seeded_run_id = str(seeded_run.get("run_id"))
        if seeded_run_id in seen_run_ids:
            continue
        output.append(seeded_run)
        seen_run_ids.add(seeded_run_id)
    return output


def _leaderboard_child_payload(row: Any) -> dict[str, Any]:
    metrics = {
        metric: _clean_value(row.get(f"metrics.{metric}"))
        for metric in TRACKED_METRICS
        if _clean_value(row.get(f"metrics.{metric}")) is not None
    }
    return {
        "run_id": _clean_value(row.get("run_id")),
        "run_name": _clean_value(row.get("tags.mlflow.runName")),
        "status": _clean_value(row.get("status")),
        "rank": _coerce_int(row.get("params.rank") or row.get("tags.rank")),
        "model_id": _clean_value(row.get("params.model_id"))
        or _clean_value(row.get("tags.model_id")),
        "algo": _clean_value(row.get("params.algo"))
        or _clean_value(row.get("tags.algo")),
        "leaderboard_source": _clean_value(row.get("params.leaderboard_source"))
        or _clean_value(row.get("tags.leaderboard_source")),
        "start_time": (
            row.get("start_time").isoformat()
            if row.get("start_time") is not None
            else None
        ),
        "end_time": (
            row.get("end_time").isoformat()
            if row.get("end_time") is not None
            else None
        ),
        "metrics": metrics,
    }


def _active_node3_retrain_run() -> dict[str, Any] | None:
    settings = get_settings()
    ssh_key = str(os.getenv("SSH_KEY") or "/run/secrets/google_compute_engine").strip()
    ssh_users = []
    for candidate in (
        os.getenv("SSH_USER"),
        os.getenv("HUNG_SSH_USER"),
        "runner",
    ):
        value = str(candidate or "").strip()
        if value and value not in ssh_users:
            ssh_users.append(value)
    node3_host = str(os.getenv("NODE3_INTERNAL_IP") or "10.128.0.8").strip()
    if not ssh_key or not os.path.exists(ssh_key) or not ssh_users or not node3_host:
        return None

    remote_script = r"""
python3 - <<'PY'
import json
import os
from pathlib import Path

status_path = Path('/opt/traffic/logs/retrain_state/node3-retrain-status.json')
if not status_path.exists():
    raise SystemExit(1)

payload = json.loads(status_path.read_text(encoding='utf-8'))
pid = int(payload.get('pid') or 0)
running = False
if pid > 0:
    try:
        os.kill(pid, 0)
        running = True
    except OSError:
        running = False
payload['running'] = running
print(json.dumps(payload, ensure_ascii=True))
PY
""".strip()

    try:
        payload = None
        for ssh_user in ssh_users:
            try:
                result = subprocess.run(
                    [
                        "ssh",
                        "-i",
                        ssh_key,
                        "-o",
                        "StrictHostKeyChecking=no",
                        "-o",
                        "ConnectTimeout=20",
                        f"{ssh_user}@{node3_host}",
                        remote_script,
                    ],
                    check=True,
                    capture_output=True,
                    text=True,
                    timeout=30,
                )
                payload = json.loads(result.stdout.strip())
                break
            except Exception:
                continue
        if payload is None:
            return None
    except Exception:
        return None

    if not payload.get("running"):
        return None

    phase = str(payload.get("phase") or "").strip().lower()
    if phase not in ACTIVE_NODE3_PHASES:
        return None

    batch_id = _clean_value(payload.get("batch_id"))
    started_at = _clean_value(payload.get("started_at"))
    models_logged = _coerce_int(payload.get("models_logged")) or 0
    return {
        "run_id": f"node3-active-{batch_id or started_at or 'unknown'}",
        "run_name": "h2o_retrain_online",
        "status": "RUNNING",
        "run_type": "retrain_online",
        "run_role": "retrain_parent",
        "retrain_batch_id": batch_id,
        "expected_models": _coerce_int(os.getenv("H2O_TOP_K_MODELS")) or 10,
        "models_logged": models_logged,
        "is_complete_batch": False,
        "start_time": started_at,
        "end_time": None,
        "metrics": {},
        "leaderboard_models": [],
        "phase": phase,
        "message": _clean_value(payload.get("message")),
    }


def retrain_history(limit: int = 30) -> dict[str, Any]:
    """Return recent MLflow training runs with core classification metrics."""
    settings = get_settings()
    try:
        import mlflow

        mlflow.set_tracking_uri(settings.mlflow_tracking_uri)
        experiment = mlflow.get_experiment_by_name(settings.mlflow_experiment_name)
        if experiment is None:
            return _seed_history(limit, settings.mlflow_experiment_name)

        runs = mlflow.search_runs(
            experiment_ids=[experiment.experiment_id],
            max_results=max(limit * 12, 200),
            order_by=["start_time DESC"],
        )
        parent_runs: list[dict[str, Any]] = []
        leaderboard_children: dict[str, list[dict[str, Any]]] = {}
        run_count = 0
        for _, row in runs.iterrows():
            run_count += 1
            parent_run_id = _clean_value(row.get("tags.mlflow.parentRunId"))
            run_role = str(_clean_value(row.get("tags.run_role")) or "")
            if parent_run_id and run_role == "leaderboard_model":
                child_payload = _leaderboard_child_payload(row)
                leaderboard_children.setdefault(str(parent_run_id), []).append(child_payload)
                continue
            if not _is_retrain_run(row):
                continue
            metrics = {
                metric: _clean_value(row.get(f"metrics.{metric}"))
                for metric in TRACKED_METRICS
                if _clean_value(row.get(f"metrics.{metric}")) is not None
            }
            parent_runs.append(
                {
                    "run_id": _clean_value(row.get("run_id")),
                    "run_name": _clean_value(row.get("tags.mlflow.runName")),
                    "status": _clean_value(row.get("status")),
                    "run_type": _clean_value(row.get("tags.run_type"))
                    or _clean_value(row.get("params.run_type")),
                    "run_role": _clean_value(row.get("tags.run_role")),
                    "retrain_batch_id": _clean_value(row.get("tags.retrain_batch_id"))
                    or _clean_value(row.get("params.retrain_batch_id")),
                    "expected_models": _expected_model_count(row),
                    "models_logged": _logged_model_count(row),
                    "is_complete_batch": _is_complete_retrain_batch(row),
                    "start_time": (
                        row.get("start_time").isoformat()
                        if row.get("start_time") is not None
                        else None
                    ),
                    "end_time": (
                        row.get("end_time").isoformat()
                        if row.get("end_time") is not None
                        else None
                    ),
                    "metrics": metrics,
                }
            )
            if len(parent_runs) >= limit:
                break

        for run in parent_runs:
            child_runs = leaderboard_children.get(str(run.get("run_id")), [])
            child_runs.sort(
                key=lambda child: (
                    child.get("rank") is None,
                    child.get("rank") or 0,
                    str(child.get("run_name") or ""),
                )
            )
            run["leaderboard_models"] = child_runs

        active_run = _active_node3_retrain_run()
        if active_run is not None:
            duplicate_active = any(
                str(run.get("status") or "").upper() == "RUNNING"
                and (
                    (run.get("retrain_batch_id") and run.get("retrain_batch_id") == active_run.get("retrain_batch_id"))
                    or (run.get("start_time") and run.get("start_time") == active_run.get("start_time"))
                )
                for run in parent_runs
            )
            if not duplicate_active:
                parent_runs.insert(0, active_run)

        output = _backfill_seed_runs(parent_runs, limit)
        for run in output:
            run.setdefault("leaderboard_models", [])
        return {
            "status": "ok" if output else "not_enough_data",
            "experiment": settings.mlflow_experiment_name,
            "runs": output,
            "metrics": TRACKED_METRICS,
            "source": "mlflow_with_seed_backfill"
            if len(output) > len(parent_runs)
            else "mlflow",
        }
    except Exception as exc:
        fallback = _seed_history(limit, settings.mlflow_experiment_name)
        if fallback.get("runs"):
            fallback["error"] = str(exc)[:200]
            return fallback
        return _mlflow_unavailable(exc)


def performance_trend(limit: int = 20) -> dict[str, Any]:
    """Return metric series across recent MLflow runs."""
    history = retrain_history(limit=limit)
    if history.get("status") == "unavailable":
        return history

    series = []
    for run in reversed(history.get("runs", [])):
        point = {
            "run_id": run.get("run_id"),
            "run_name": run.get("run_name"),
            "start_time": run.get("start_time"),
        }
        point.update(run.get("metrics", {}))
        series.append(point)
    return {
        "status": "ok" if series else "not_enough_data",
        "experiment": history.get("experiment"),
        "series": series,
        "metrics": TRACKED_METRICS,
    }
