"""MLflow model history helpers for the dashboard backend."""

from __future__ import annotations

import math
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
            max_results=limit,
            order_by=["start_time DESC"],
        )
        output = []
        for _, row in runs.iterrows():
            metrics = {
                metric: _clean_value(row.get(f"metrics.{metric}"))
                for metric in TRACKED_METRICS
                if _clean_value(row.get(f"metrics.{metric}")) is not None
            }
            output.append(
                {
                    "run_id": _clean_value(row.get("run_id")),
                    "run_name": _clean_value(row.get("tags.mlflow.runName")),
                    "status": _clean_value(row.get("status")),
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
        output = _backfill_seed_runs(output, limit)
        return {
            "status": "ok" if output else "not_enough_data",
            "experiment": settings.mlflow_experiment_name,
            "runs": output,
            "metrics": TRACKED_METRICS,
            "source": "mlflow_with_seed_backfill"
            if len(output) > len(runs)
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
