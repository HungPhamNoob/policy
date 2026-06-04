"""System and model status endpoints."""

import os

from fastapi import APIRouter

from app.core.config import get_settings
from app.services.pipeline_service import gold_last_update

router = APIRouter()


@router.get("/status")
def get_system_status() -> dict:
    """Return pipeline configuration and lightweight status metadata."""
    settings = get_settings()
    tomtom_api_key = str(
        os.getenv("TOMTOM_API_KEY") or os.getenv("TOMTOM_API") or ""
    ).strip()
    tomtom_poll_seconds = int(float(os.getenv("TOMTOM_POLL_SECONDS", "60") or "60"))
    tomtom_flush_interval_seconds = int(
        float(os.getenv("TOMTOM_FLUSH_INTERVAL_SECONDS", "15") or "15")
    )
    tomtom_live_status = "configured" if tomtom_api_key else "missing_credentials"
    return {
        "environment": settings.environment,
        "kafka": {
            "us_topic": settings.kafka_topic_raw,
            "tomtom_topic": settings.kafka_topic_tomtom_raw,
            "status": "configured",
        },
        "tomtom_live": {
            "status": tomtom_live_status,
            "credentials_configured": bool(tomtom_api_key),
            "poll_seconds": tomtom_poll_seconds,
            "flush_interval_seconds": tomtom_flush_interval_seconds,
            "note": (
                "TomTom producer can poll live incidents."
                if tomtom_api_key
                else "TomTom producer is idle until TOMTOM_API_KEY is configured."
            ),
        },
        "flink": {
            "job_name": "Flink Traffic Risk Prediction",
            "status": "configured",
            "checkpoint_dir": settings.flink_local_checkpoint_dir
            or settings.flink_checkpoint_dir,
            "checkpoint_dir_remote": settings.flink_checkpoint_dir,
            "checkpoint_interval_ms": settings.flink_checkpoint_interval_ms,
        },
        "spark": {
            "last_gold_update": gold_last_update(),
            "gold_path": settings.gold_retrain_path,
        },
        "mlflow": {
            "model_name": settings.model_name,
            "serving_endpoint": settings.mlflow_serving_endpoint,
            "latest_version": settings.model_version or "latest",
        },
        "airflow": {
            "executor": "LocalExecutor",
            "model_retrain_schedule": settings.airflow_model_retrain_schedule,
            "stream_health_schedule": settings.airflow_stream_health_schedule,
        },
        "pipeline": {
            "full_realtime_reset_script": settings.pipeline_reset_script,
            "reset_log_dir": settings.pipeline_reset_log_dir,
        },
        "postgres": {
            "us_prediction_table": settings.us_prediction_table,
            "tomtom_events_table": settings.tomtom_events_table,
            "row_count": None,
        },
    }


@router.get("/model/info")
def get_model_info() -> dict:
    """Return deprecated model configuration alias used by older clients."""
    settings = get_settings()
    return {
        "model_name": settings.model_name,
        "model_version": settings.model_version or "latest",
        "tracking_uri": settings.mlflow_tracking_uri,
        "serving_endpoint": settings.mlflow_serving_endpoint,
        "deprecated": True,
        "canonical_endpoint": "/api/v1/model/info",
    }
