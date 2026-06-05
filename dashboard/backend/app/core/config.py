"""Environment-backed settings for the dashboard backend."""

from functools import lru_cache
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Runtime settings loaded from `.env`, `.env.cloud`, or process environment."""

    environment: str = Field(default="local", alias="ENV")

    postgres_user: str = Field(default="capstone", alias="POSTGRES_USER")
    postgres_password: str = Field(default="changeme123", alias="POSTGRES_PASSWORD")
    postgres_db: str = Field(default="capstone_db", alias="POSTGRES_DB")
    postgres_host: str = Field(default="localhost", alias="POSTGRES_HOST")
    postgres_port: int = Field(default=5432, alias="POSTGRES_PORT")
    postgres_pool_min_conn: int = Field(default=1, alias="PG_POOL_MIN_CONN")
    postgres_pool_max_conn: int = Field(default=8, alias="PG_POOL_MAX_CONN")
    postgres_statement_timeout_ms: int = Field(
        default=30_000, alias="PG_STATEMENT_TIMEOUT_MS"
    )
    prediction_table: str = Field(
        default="traffic_risk_predictions",
        alias="POSTGRES_PREDICTION_TABLE",
    )
    us_prediction_table: str = Field(
        default="traffic_risk_predictions",
        alias="POSTGRES_US_PREDICTION_TABLE",
    )
    tomtom_events_table: str = Field(
        default="traffic_tomtom_incidents",
        alias="POSTGRES_TOMTOM_TABLE",
    )

    kafka_topic_raw: str = Field(default="traffic.us.raw", alias="KAFKA_TOPIC_RAW")
    kafka_topic_tomtom_raw: str = Field(
        default="traffic.tomtom.raw",
        alias="KAFKA_TOPIC_TOMTOM_RAW",
    )
    flink_checkpoint_dir: str = Field(
        default="gs://big-data-group-4-backups/checkpoints/flink",
        alias="FLINK_CHECKPOINT_DIR",
    )
    flink_local_checkpoint_dir: str = Field(
        default="file:///opt/flink/checkpoints/unified-traffic-streaming",
        alias="FLINK_LOCAL_CHECKPOINT_DIR",
    )
    flink_checkpoint_interval_ms: int = Field(
        default=30000, alias="FLINK_CHECKPOINT_INTERVAL"
    )
    gold_retrain_path: str = Field(
        default="gs://big-data-group-4-gold/features/retrain",
        alias="GOLD_RETRAIN_PATH",
    )
    airflow_model_retrain_schedule: str = Field(
        default="*/45 * * * *",
        alias="AIRFLOW_MODEL_RETRAIN_SCHEDULE",
    )
    airflow_model_retrain_interval_minutes: int = Field(
        default=45,
        alias="AIRFLOW_MODEL_RETRAIN_INTERVAL_MINUTES",
    )
    airflow_stream_health_schedule: str = Field(
        default="*/2 * * * *",
        alias="AIRFLOW_STREAM_HEALTH_SCHEDULE",
    )
    retrain_min_us_rows: int = Field(
        default=0,
        alias="RETRAIN_MIN_US_ROWS",
    )
    pipeline_reset_script: str = Field(
        default="/opt/traffic/scripts/gcp/full-cloud-realtime-reset-run.sh",
        alias="PIPELINE_RESET_SCRIPT",
    )
    pipeline_reset_log_dir: str = Field(
        default="/tmp/traffic-reset-jobs",
        alias="PIPELINE_RESET_LOG_DIR",
    )
    mlflow_tracking_uri: str = Field(
        default="http://localhost:5000", alias="MLFLOW_TRACKING_URI"
    )
    mlflow_experiment_name: str = Field(
        default="traffic-risk-assessment", alias="MLFLOW_EXPERIMENT_NAME"
    )
    mlflow_serving_endpoint: str = Field(
        default="http://localhost:5001/invocations",
        alias="MLFLOW_SERVING_ENDPOINT",
    )
    model_name: str = Field(default="traffic-risk-model", alias="ML_MODEL_NAME")
    model_version: str = Field(default="latest", alias="ML_MODEL_VERSION")

    model_config = SettingsConfigDict(
        env_file=(".env.cloud", ".env"),
        env_file_encoding="utf-8",
        extra="ignore",
        populate_by_name=True,
        protected_namespaces=("settings_",),
    )


@lru_cache
def get_settings() -> Settings:
    """Return cached settings so each request does not reread environment files."""
    return Settings()
