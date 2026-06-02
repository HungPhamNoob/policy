"""Model metadata endpoints."""

from fastapi import APIRouter

from app.core.config import get_settings
from app.services.model_service import performance_trend, retrain_history

router = APIRouter()


@router.get("/info")
def get_model_info() -> dict:
    """Return model registry and serving configuration used by the backend."""
    settings = get_settings()
    return {
        "model_name": settings.model_name,
        "model_version": settings.model_version or "latest",
        "tracking_uri": settings.mlflow_tracking_uri,
        "serving_endpoint": settings.mlflow_serving_endpoint,
    }


@router.get("/retrain-history")
def get_retrain_history(limit: int = 30) -> dict:
    """Return recent MLflow retraining runs and core metrics."""
    return retrain_history(limit=limit)


@router.get("/performance-trend")
def get_performance_trend(limit: int = 20) -> dict:
    """Return model performance metrics over recent MLflow runs."""
    return performance_trend(limit=limit)
