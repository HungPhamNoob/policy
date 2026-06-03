#!/usr/bin/env python3
"""
Unified Flink streaming job for US replay and TomTom live incident processing.

Data flows:

    US replay flow (before-2020 model, post-2020 data):
        Kafka traffic.us.raw
        -> feature engineering  (processing.feature_engineering.build_features)
        -> Silver GCS write     (batched JSONL per partition)
        -> MLflow / H2O inference  (micro-batch: ML_BATCH_SIZE events per HTTP call)
        -> PostgreSQL table:    traffic_risk_predictions

    TomTom live flow (rule-based, no H2O):
        Kafka traffic.tomtom.raw
        -> enrichment           (processing.streaming_enrichment.enrich_tomtom_event)
        -> feature engineering  (shared with US)
        -> rule-based severity  (magnitudeOfDelay + iconCategory)
        -> PostgreSQL table:    traffic_tomtom_incidents

    TomTom events are intentionally excluded from Spark, MLflow, and H2O.
    The severity label is derived from TomTom delay and icon signals, which
    are not compatible with the US Accidents H2O model contract.
"""

import json
import logging
import os
import re
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import psycopg2
import psycopg2.extras
import requests
from dotenv import load_dotenv
from psycopg2.pool import SimpleConnectionPool
from pyflink.common import Types
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.functions import SinkFunction
from pyflink.datastream.connectors.kafka import KafkaOffsetsInitializer, KafkaSource
from processing.feature_engineering import build_features
from processing.streaming_enrichment import enrich_tomtom_event
from shared.risk_scoring import (
    clamp_severity,
    compute_unified_risk_score,
    infer_severity_from_prediction,
    to_float,
    to_int,
)

try:
    from pyflink.datastream.checkpoint_storage import FileSystemCheckpointStorage
except Exception:  # pragma: no cover - depends on the PyFlink distribution.
    FileSystemCheckpointStorage = None


# Load environment variables from .env file when running outside Docker.
load_dotenv()

logging.basicConfig(
    level=os.getenv("STREAMING_LOG_LEVEL", "INFO"),
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger("flink-dual-stream")


# ---------------------------------------------------------------------------
# Configuration (resolved from environment variables set by Docker Compose)
# ---------------------------------------------------------------------------
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC_US_RAW = os.getenv(
    "KAFKA_TOPIC_US_RAW",
    os.getenv("KAFKA_TOPIC_RAW", "traffic.us.raw"),
)
KAFKA_TOPIC_TOMTOM_RAW = os.getenv("KAFKA_TOPIC_TOMTOM_RAW", "traffic.tomtom.raw")
FLINK_INFERENCE_GROUP = os.getenv("FLINK_INFERENCE_GROUP", "flink-dual-inference")
FLINK_TOMTOM_GROUP = os.getenv("FLINK_TOMTOM_GROUP", "flink-dual-tomtom")
FLINK_PARALLELISM = int(os.getenv("FLINK_PARALLELISM", "2"))
FLINK_CHECKPOINT_INTERVAL = int(os.getenv("FLINK_CHECKPOINT_INTERVAL", "30000"))
FLINK_CHECKPOINT_TIMEOUT = int(
    os.getenv("FLINK_CHECKPOINT_TIMEOUT", "1800000")
)
FLINK_CHECKPOINT_MIN_PAUSE = int(
    os.getenv("FLINK_CHECKPOINT_MIN_PAUSE", "120000")
)
FLINK_TOLERABLE_FAILED_CHECKPOINTS = int(
    os.getenv("FLINK_TOLERABLE_FAILED_CHECKPOINTS", "10")
)
FLINK_CHECKPOINT_DIR = os.getenv(
    "FLINK_LOCAL_CHECKPOINT_DIR",
    os.getenv("FLINK_CHECKPOINT_DIR", "file:///tmp/flink-checkpoints/traffic-risk"),
)
FLINK_KAFKA_CONNECTOR_JAR = os.getenv("FLINK_KAFKA_CONNECTOR_JAR", "")

MLFLOW_SERVING_ENDPOINT = os.getenv(
    "MLFLOW_SERVING_ENDPOINT",
    "http://10.128.0.4:5001/invocations",
)
ML_TIMEOUT_SECONDS = float(os.getenv("ML_TIMEOUT_SECONDS", "5"))
ML_FALLBACK_RISK_SCORE = float(os.getenv("ML_FALLBACK_RISK_SCORE", "-1"))
ML_BATCH_SIZE = int(os.getenv("ML_BATCH_SIZE", "100"))
ML_MAX_BUFFER_AGE_MS = int(os.getenv("ML_MAX_BUFFER_AGE_MS", "750"))

SILVER_FEATURES_PATH = os.getenv(
    "SILVER_FEATURES_PATH",
    "gs://big-data-group-4-silver/process/flink_features",
)
SILVER_WRITE_ENABLED = os.getenv("SILVER_WRITE_ENABLED", "true").lower() in {
    "1",
    "true",
    "yes",
}
SILVER_FLUSH_EVERY_N = int(
    os.getenv("SILVER_FLUSH_EVERY_N", os.getenv("SILVER_BATCH_SIZE", "100"))
)
SILVER_MAX_BUFFER_AGE_MS = int(os.getenv("SILVER_MAX_BUFFER_AGE_MS", "1500"))

PG_HOST = os.getenv("POSTGRES_HOST", "10.128.0.4")
PG_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
PG_DB = os.getenv("POSTGRES_DB", "capstone_db")
PG_USER = os.getenv("POSTGRES_USER", "capstone")
PG_PASSWORD = os.getenv("POSTGRES_PASSWORD", "123")
PG_POOL_MIN = int(os.getenv("PG_POOL_MIN_CONN", "1"))
PG_POOL_MAX = int(os.getenv("PG_POOL_MAX_CONN", "4"))
PG_BATCH_SIZE = int(os.getenv("PG_BATCH_SIZE", "200"))
PG_MAX_BUFFER_AGE_MS = int(os.getenv("PG_MAX_BUFFER_AGE_MS", "750"))
PG_US_TABLE = os.getenv(
    "POSTGRES_US_PREDICTION_TABLE",
    os.getenv("POSTGRES_PREDICTION_TABLE", "traffic_risk_predictions"),
)
PG_TOMTOM_TABLE = os.getenv("POSTGRES_TOMTOM_TABLE", "traffic_tomtom_incidents")
FLINK_PRINT_EACH_EVENT = os.getenv("FLINK_PRINT_EACH_EVENT", "false").lower() in {
    "1",
    "true",
    "yes",
}


MODEL_FEATURE_COLUMNS = [
    "lat",
    "lon",
    "hour",
    "day_of_week",
    "is_weekend",
    "is_rush_hour",
    "weather_code",
    "temperature_f",
    "humidity",
    "wind_speed_mph",
    "visibility_mi",
    "road_type_code",
    "is_junction",
    "has_traffic_signal",
    "is_crossing",
    "is_roundabout",
    "is_stop",
    "is_station",
    "is_railway",
    "is_night",
]

# Connection pool and batch buffers — one per Flink Python worker/subtask.
PG_POOL: Optional[SimpleConnectionPool] = None
US_BATCH_BUFFER: List[Dict[str, Any]] = []
TOMTOM_BATCH_BUFFER: List[Dict[str, Any]] = []
_SCHEMA_INITIALIZED = False
US_BATCH_BUFFER_STARTED_AT: Optional[float] = None
TOMTOM_BATCH_BUFFER_STARTED_AT: Optional[float] = None

# Micro-batch MLflow inference buffer
_ML_INFERENCE_BUFFER: List[Tuple[Dict[str, Any], str, float]] = []
_ML_INFERENCE_BUFFER_STARTED_AT: Optional[float] = None


# ---------------------------------------------------------------------------
# Connection pool management
# ---------------------------------------------------------------------------


def get_pg_pool() -> SimpleConnectionPool:
    """Return the global PostgreSQL connection pool, creating it lazily."""
    global PG_POOL
    if PG_POOL is None:
        PG_POOL = SimpleConnectionPool(
            minconn=PG_POOL_MIN,
            maxconn=PG_POOL_MAX,
            host=PG_HOST,
            port=PG_PORT,
            dbname=PG_DB,
            user=PG_USER,
            password=PG_PASSWORD,
        )
        logger.info(
            "PostgreSQL connection pool created (min=%d, max=%d) -> %s:%d/%s",
            PG_POOL_MIN,
            PG_POOL_MAX,
            PG_HOST,
            PG_PORT,
            PG_DB,
        )
    return PG_POOL


def release_pg_connection(connection) -> None:
    """Return a connection to the pool, ignoring errors from already-closed handles."""
    try:
        get_pg_pool().putconn(connection)
    except Exception:
        try:
            connection.close()
        except Exception:
            pass


# ---------------------------------------------------------------------------
# Schema helpers (run once at startup, not on the hot path)
# ---------------------------------------------------------------------------


def table_name(value: str) -> str:
    """Extract a safe public-schema table name from a possibly-qualified string."""
    selected = value.split(".")[-1]
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", selected):
        raise ValueError(f"Invalid PostgreSQL table name: {value}")
    return selected


PG_US_TABLE = table_name(PG_US_TABLE)
PG_TOMTOM_TABLE = table_name(PG_TOMTOM_TABLE)


def ensure_us_schema(cursor) -> None:
    """Create or evolve the US replay prediction table (idempotent)."""
    cursor.execute("CREATE EXTENSION IF NOT EXISTS postgis;")
    cursor.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {PG_US_TABLE} (
            event_id VARCHAR PRIMARY KEY,
            event_year INT,
            event_time TIMESTAMP,
            lat DOUBLE PRECISION,
            lon DOUBLE PRECISION,
            true_severity INT,
            predicted_severity INT,
            risk_score DOUBLE PRECISION,
            weather_code INT,
            temperature_f DOUBLE PRECISION,
            humidity DOUBLE PRECISION,
            wind_speed_mph DOUBLE PRECISION,
            visibility_mi DOUBLE PRECISION,
            road_type_code INT,
            hour INT,
            day_of_week INT,
            is_weekend INT,
            is_rush_hour INT,
            is_junction INT,
            has_traffic_signal INT,
            is_crossing INT,
            is_roundabout INT,
            is_stop INT,
            is_station INT,
            is_railway INT,
            is_night INT,
            model_status VARCHAR(32),
            inference_latency_ms DOUBLE PRECISION,
            ingestion_time TIMESTAMP,
            processed_time TIMESTAMP,
            end_to_end_latency_ms DOUBLE PRECISION,
            geom GEOMETRY(Point, 4326),
            updated_at TIMESTAMP DEFAULT NOW(),
            created_at TIMESTAMP DEFAULT NOW()
        );
        """
    )
    for statement in [
        "event_year INT",
        "event_time TIMESTAMP",
        "lat DOUBLE PRECISION",
        "lon DOUBLE PRECISION",
        "true_severity INT",
        "predicted_severity INT",
        "risk_score DOUBLE PRECISION",
        "weather_code INT",
        "temperature_f DOUBLE PRECISION",
        "humidity DOUBLE PRECISION",
        "wind_speed_mph DOUBLE PRECISION",
        "visibility_mi DOUBLE PRECISION",
        "road_type_code INT",
        "hour INT",
        "day_of_week INT",
        "is_weekend INT",
        "is_rush_hour INT",
        "is_junction INT",
        "has_traffic_signal INT",
        "is_crossing INT",
        "is_roundabout INT",
        "is_stop INT",
        "is_station INT",
        "is_railway INT",
        "is_night INT",
        "model_status VARCHAR(32)",
        "inference_latency_ms DOUBLE PRECISION",
        "ingestion_time TIMESTAMP",
        "processed_time TIMESTAMP",
        "end_to_end_latency_ms DOUBLE PRECISION",
        "geom GEOMETRY(Point, 4326)",
        "updated_at TIMESTAMP DEFAULT NOW()",
        "created_at TIMESTAMP DEFAULT NOW()",
    ]:
        cursor.execute(
            f"ALTER TABLE {PG_US_TABLE} ADD COLUMN IF NOT EXISTS {statement};"
        )


def ensure_tomtom_schema(cursor) -> None:
    """Create or evolve the TomTom live incident table (idempotent)."""
    cursor.execute("CREATE EXTENSION IF NOT EXISTS postgis;")
    cursor.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {PG_TOMTOM_TABLE} (
            event_id VARCHAR PRIMARY KEY,
            incident_id VARCHAR,
            event_time TIMESTAMP,
            lat DOUBLE PRECISION,
            lon DOUBLE PRECISION,
            severity INT,
            risk_score DOUBLE PRECISION,
            icon_category INT,
            delay_magnitude INT,
            delay_seconds DOUBLE PRECISION,
            length_meters DOUBLE PRECISION,
            weather_code INT,
            temperature_f DOUBLE PRECISION,
            humidity DOUBLE PRECISION,
            wind_speed_mph DOUBLE PRECISION,
            visibility_mi DOUBLE PRECISION,
            road_type_code INT,
            hour INT,
            day_of_week INT,
            is_weekend INT,
            is_rush_hour INT,
            is_junction INT,
            has_traffic_signal INT,
            is_crossing INT,
            is_roundabout INT,
            is_stop INT,
            is_station INT,
            is_railway INT,
            is_night INT,
            state_or_region VARCHAR,
            city VARCHAR,
            from_road TEXT,
            to_road TEXT,
            geometry_wkt TEXT,
            model_status VARCHAR(32),
            ingestion_time TIMESTAMP,
            processed_time TIMESTAMP,
            end_to_end_latency_ms DOUBLE PRECISION,
            geom GEOMETRY(Point, 4326),
            updated_at TIMESTAMP DEFAULT NOW(),
            created_at TIMESTAMP DEFAULT NOW()
        );
        """
    )
    for statement in [
        "incident_id VARCHAR",
        "event_time TIMESTAMP",
        "lat DOUBLE PRECISION",
        "lon DOUBLE PRECISION",
        "severity INT",
        "risk_score DOUBLE PRECISION",
        "icon_category INT",
        "delay_magnitude INT",
        "delay_seconds DOUBLE PRECISION",
        "length_meters DOUBLE PRECISION",
        "weather_code INT",
        "temperature_f DOUBLE PRECISION",
        "humidity DOUBLE PRECISION",
        "wind_speed_mph DOUBLE PRECISION",
        "visibility_mi DOUBLE PRECISION",
        "road_type_code INT",
        "hour INT",
        "day_of_week INT",
        "is_weekend INT",
        "is_rush_hour INT",
        "is_junction INT",
        "has_traffic_signal INT",
        "is_crossing INT",
        "is_roundabout INT",
        "is_stop INT",
        "is_station INT",
        "is_railway INT",
        "is_night INT",
        "state_or_region VARCHAR",
        "city VARCHAR",
        "from_road TEXT",
        "to_road TEXT",
        "geometry_wkt TEXT",
        "model_status VARCHAR(32)",
        "ingestion_time TIMESTAMP",
        "processed_time TIMESTAMP",
        "end_to_end_latency_ms DOUBLE PRECISION",
        "geom GEOMETRY(Point, 4326)",
        "updated_at TIMESTAMP DEFAULT NOW()",
        "created_at TIMESTAMP DEFAULT NOW()",
    ]:
        cursor.execute(
            f"ALTER TABLE {PG_TOMTOM_TABLE} ADD COLUMN IF NOT EXISTS {statement};"
        )


def initialize_schemas() -> None:
    """Run schema DDL once at job startup, before the hot path starts."""
    global _SCHEMA_INITIALIZED
    if _SCHEMA_INITIALIZED:
        return
    # Use a one-off connection here so the global pool stays uninitialized
    # until the worker functions run inside Flink. Pre-creating the pool in
    # the driver makes PyFlink attempt to pickle a live psycopg2 connection.
    connection = psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        dbname=PG_DB,
        user=PG_USER,
        password=PG_PASSWORD,
    )
    try:
        with connection:
            with connection.cursor() as cursor:
                ensure_us_schema(cursor)
                ensure_tomtom_schema(cursor)
        _SCHEMA_INITIALIZED = True
        logger.info("PostgreSQL schemas initialized for %s and %s", PG_US_TABLE, PG_TOMTOM_TABLE)
    finally:
        connection.close()


# ---------------------------------------------------------------------------
# GCS Silver writer (batched, not per-event)
# ---------------------------------------------------------------------------

_GCS_FS = None
_SILVER_BATCH_BUFFERS: Dict[str, List[Dict[str, Any]]] = {}
_SILVER_BATCH_STARTED_AT: Dict[str, float] = {}


def _buffer_age_ms(started_at: Optional[float]) -> float:
    """Return the current age for a buffer based on monotonic time."""
    if started_at is None:
        return 0.0
    return (time.monotonic() - started_at) * 1000.0


def _get_gcs_fs():
    """Return a lazily-initialized GCS filesystem client (reused across events)."""
    global _GCS_FS
    if _GCS_FS is None:
        import gcsfs
        _GCS_FS = gcsfs.GCSFileSystem()
    return _GCS_FS


def _silver_prefix(features: Dict[str, Any]) -> str:
    """Return the date partition used for Silver feature batches."""
    event_time = features.get("event_time")
    if event_time:
        dt = datetime.fromisoformat(str(event_time).replace("Z", "+00:00"))
        return f"{dt.year}/{dt.month:02d}/{dt.day:02d}"
    return "unknown_date"


def flush_silver_prefix(prefix: str) -> None:
    """Flush one buffered Silver batch to a JSONL object in GCS."""
    rows = _SILVER_BATCH_BUFFERS.get(prefix) or []
    if not rows:
        return

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    path = (
        f"{SILVER_FEATURES_PATH.rstrip('/')}/{prefix}/batches/"
        f"features-{timestamp}.jsonl"
    )
    payload = "\n".join(json.dumps(row, ensure_ascii=False) for row in rows) + "\n"

    try:
        fs = _get_gcs_fs()
        with fs.open(path, "wb") as file_obj:
            file_obj.write(payload.encode("utf-8"))
        logger.info("Flushed %d Silver features to %s", len(rows), path)
        _SILVER_BATCH_BUFFERS[prefix] = []
        _SILVER_BATCH_STARTED_AT.pop(prefix, None)
    except Exception:
        logger.exception("Failed to flush Silver feature batch: %s", path)


def flush_all_silver_batches() -> None:
    """Flush all buffered Silver feature batches before shutdown."""
    for prefix in list(_SILVER_BATCH_BUFFERS):
        flush_silver_prefix(prefix)


def write_to_gcs_silver(features: Dict[str, Any]) -> None:
    """Buffer US replay features and flush them to GCS as JSONL batches."""
    if not SILVER_WRITE_ENABLED:
        return

    prefix = _silver_prefix(features)
    buffer = _SILVER_BATCH_BUFFERS.setdefault(prefix, [])
    if prefix not in _SILVER_BATCH_STARTED_AT:
        _SILVER_BATCH_STARTED_AT[prefix] = time.monotonic()
    buffer.append(features)
    if (
        len(buffer) >= SILVER_FLUSH_EVERY_N
        or _buffer_age_ms(_SILVER_BATCH_STARTED_AT.get(prefix)) >= SILVER_MAX_BUFFER_AGE_MS
    ):
        flush_silver_prefix(prefix)


# ---------------------------------------------------------------------------
# Risk score computation (unified formula for both US and TomTom)
# ---------------------------------------------------------------------------


def compute_risk_score(
    severity: Any,
    delay_seconds: Any = None,
    length_meters: Any = None,
    is_night: Any = 0,
    is_weekend: Any = 0,
    road_type_code: Any = 0,
    weather_code: Any = 0,
) -> Optional[float]:
    """Compute unified continuous risk score (0.0-1.0) using the shared formula."""
    return compute_unified_risk_score(
        severity=severity,
        delay_seconds=delay_seconds,
        length_meters=length_meters,
        is_night=is_night,
        is_weekend=is_weekend,
        road_type_code=road_type_code,
        weather_code=weather_code,
    )


# ---------------------------------------------------------------------------
# MLflow micro-batch inference helpers
# ---------------------------------------------------------------------------


def _extract_prediction(prediction: Any) -> Tuple[Optional[int], Optional[float]]:
    """Normalize MLflow output into (severity, risk_score) tuple."""
    severity = infer_severity_from_prediction(prediction)
    if severity is None:
        return None, None
    risk = compute_risk_score(severity=severity)
    return severity, risk


def _call_mlflow_batch(
    batch: List[Tuple[Dict[str, Any], Optional[str], float]]
) -> List[Tuple[Optional[int], Optional[float]]]:
    """Call MLflow Serving with a micro-batch of features for throughput.

    Args:
        batch: List of tuples (features_dict, ingestion_time_str, start_ts).

    Returns:
        List of (severity, risk_score) tuples, one per input row.
    """
    rows_data = []
    valid_indices = []
    for i, (features, _ingestion, _start) in enumerate(batch):
        row = []
        try:
            for column in MODEL_FEATURE_COLUMNS:
                if column not in features:
                    raise ValueError(f"Missing feature column: {column}")
                row.append(features[column])
            rows_data.append(row)
            valid_indices.append(i)
        except Exception:
            logger.warning(
                "Skipping row %s in micro-batch — missing feature columns",
                features.get("event_id", "?"),
            )

    if not rows_data:
        return [(None, None)] * len(batch)

    payload = {
        "dataframe_split": {
            "columns": MODEL_FEATURE_COLUMNS,
            "data": rows_data,
        }
    }

    try:
        response = requests.post(
            MLFLOW_SERVING_ENDPOINT,
            json=payload,
            timeout=max(ML_TIMEOUT_SECONDS, len(rows_data) * 0.1 + 2),
            headers={"Content-Type": "application/json"},
        )
        response.raise_for_status()
        predictions = response.json().get("predictions", [])
    except Exception as exc:
        logger.warning(
            "MLflow micro-batch inference unavailable for %d rows; using fallback scoring: %s",
            len(rows_data),
            exc,
        )
        return [(None, None)] * len(batch)

    # Map predictions back to original batch indices
    results = [(None, None)] * len(batch)
    for pred_idx, idx in enumerate(valid_indices):
        if pred_idx < len(predictions):
            results[idx] = _extract_prediction(predictions[pred_idx])

    return results


def flush_ml_batch() -> None:
    """Flush the buffered MLflow inference micro-batch."""
    global _ML_INFERENCE_BUFFER, _ML_INFERENCE_BUFFER_STARTED_AT
    if not _ML_INFERENCE_BUFFER:
        return

    batch = _ML_INFERENCE_BUFFER
    _ML_INFERENCE_BUFFER = []
    _ML_INFERENCE_BUFFER_STARTED_AT = None

    batch_start = time.time()
    predictions = _call_mlflow_batch(batch)
    batch_elapsed_ms = (time.time() - batch_start) * 1000

    batch_size = len(batch)
    per_event_ms = batch_elapsed_ms / batch_size if batch_size > 0 else 0

    logger.info(
        "MLflow micro-batch flush: %d events in %.1f ms (%.2f ms/event)",
        batch_size,
        batch_elapsed_ms,
        per_event_ms,
    )

    for (features, ingestion_time, start_ts), (predicted_severity, _ml_risk_score) in zip(
        batch, predictions
    ):
        inference_latency_ms = (time.time() - start_ts) * 1000

        model_status = "ok"
        effective_severity = predicted_severity

        if effective_severity is not None:
            risk_score = compute_risk_score(
                severity=effective_severity,
                is_night=features.get("is_night"),
                is_weekend=features.get("is_weekend"),
                road_type_code=features.get("road_type_code"),
                weather_code=features.get("weather_code"),
            )
        else:
            fallback_severity = features.get("true_severity")
            if fallback_severity is not None:
                effective_severity = fallback_severity
                risk_score = compute_risk_score(
                    severity=fallback_severity,
                    is_night=features.get("is_night"),
                    is_weekend=features.get("is_weekend"),
                    road_type_code=features.get("road_type_code"),
                    weather_code=features.get("weather_code"),
                )
            elif ML_FALLBACK_RISK_SCORE >= 0:
                risk_score = round(max(0.0, min(1.0, ML_FALLBACK_RISK_SCORE)), 4)
            else:
                risk_score = 0.25
                effective_severity = 2
            model_status = "heuristic_fallback"

        processed_time = datetime.now(timezone.utc).isoformat()
        e2e_latency = parse_latency_ms(ingestion_time, processed_time)

        row = {
            **features,
            "predicted_severity": effective_severity,
            "risk_score": risk_score,
            "model_status": model_status,
            "inference_latency_ms": inference_latency_ms,
            "ingestion_time": ingestion_time,
            "processed_time": processed_time,
            "end_to_end_latency_ms": e2e_latency,
        }
        buffer_us_row(row)


def call_mlflow_model(
    features: Dict[str, Any]
) -> Tuple[Optional[int], Optional[float]]:
    """Buffer a single event into micro-batch for MLflow inference.

    When the buffer reaches ML_BATCH_SIZE, the batch is flushed to the
    MLflow Serving endpoint as a single HTTP request with multiple rows.
    """
    start = time.time()
    ingestion_time = normalize_optional_timestamp(
        features.get("ingestion_time", features.get("_ingested_at_utc"))
    )

    global _ML_INFERENCE_BUFFER_STARTED_AT
    if _ML_INFERENCE_BUFFER_STARTED_AT is None:
        _ML_INFERENCE_BUFFER_STARTED_AT = time.monotonic()
    _ML_INFERENCE_BUFFER.append((features, ingestion_time, start))

    if (
        len(_ML_INFERENCE_BUFFER) >= ML_BATCH_SIZE
        or _buffer_age_ms(_ML_INFERENCE_BUFFER_STARTED_AT) >= ML_MAX_BUFFER_AGE_MS
    ):
        flush_ml_batch()

    # Return a placeholder — actual values are filled by flush_ml_batch
    return None, None


# ---------------------------------------------------------------------------
# Latency helper
# ---------------------------------------------------------------------------


def parse_latency_ms(ingestion_time: Any, processed_time: str) -> Optional[float]:
    """Compute end-to-end latency in milliseconds from producer ingestion to sink write."""
    if not ingestion_time:
        return None
    try:
        ingestion_dt = datetime.fromisoformat(
            str(ingestion_time).replace("Z", "+00:00")
        )
        processed_dt = datetime.fromisoformat(processed_time.replace("Z", "+00:00"))
        return (processed_dt - ingestion_dt).total_seconds() * 1000
    except ValueError:
        return None


def normalize_optional_timestamp(value: Any) -> Optional[str]:
    """Return an ISO timestamp string or None for blank/invalid timestamp values."""
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.isoformat()

    text = str(value).strip()
    if not text:
        return None

    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00")).isoformat()
    except ValueError:
        return None


def replay_instance_event_id(original_event_id: Any, raw_row: Dict[str, Any]) -> str:
    """Return an append-only event id for each replay emission."""
    base_id = str(original_event_id or raw_row.get("ID") or "unknown").strip()
    if not base_id:
        base_id = "unknown"

    emission_id = str(raw_row.get("_replay_emission_id") or "").strip()
    if not emission_id:
        cycle = raw_row.get("_replay_cycle")
        producer = raw_row.get("_replay_producer_index")
        row_index = raw_row.get("_replay_row_index")
        if cycle is not None and producer is not None and row_index is not None:
            emission_id = f"c{cycle}-p{producer}-r{row_index}"

    if not emission_id:
        ingestion_marker = str(raw_row.get("_ingested_at_utc") or "").strip()
        if ingestion_marker:
            emission_id = re.sub(r"[^0-9A-Za-z]+", "", ingestion_marker)

    return f"{base_id}@{emission_id}" if emission_id else base_id


# ---------------------------------------------------------------------------
# Batch insert helpers (connection pooling + execute_values)
# ---------------------------------------------------------------------------


US_COLUMNS = [
    "event_id", "event_year", "event_time", "lat", "lon",
    "true_severity", "predicted_severity", "risk_score",
    "weather_code", "temperature_f", "humidity", "wind_speed_mph",
    "visibility_mi", "road_type_code", "hour", "day_of_week",
    "is_weekend", "is_rush_hour", "is_junction", "has_traffic_signal",
    "is_crossing", "is_roundabout", "is_stop", "is_station", "is_railway",
    "is_night", "model_status", "inference_latency_ms", "ingestion_time",
    "processed_time", "end_to_end_latency_ms",
]

TOMTOM_COLUMNS = [
    "event_id", "incident_id", "event_time", "lat", "lon", "severity",
    "risk_score", "icon_category", "delay_magnitude", "delay_seconds",
    "length_meters", "weather_code", "temperature_f", "humidity",
    "wind_speed_mph", "visibility_mi", "road_type_code", "hour",
    "day_of_week", "is_weekend", "is_rush_hour", "is_junction",
    "has_traffic_signal", "is_crossing", "is_roundabout", "is_stop",
    "is_station", "is_railway", "is_night", "state_or_region", "city",
    "from_road", "to_road", "geometry_wkt", "model_status",
    "ingestion_time", "processed_time", "end_to_end_latency_ms",
]


def _dedupe_batch_by_event_id(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Keep one row per event_id so PostgreSQL upserts cannot hit a row twice."""
    deduped: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        event_id = str(row.get("event_id") or "").strip()
        if event_id:
            deduped[event_id] = row
    return list(deduped.values())


def _batch_insert_us(rows: List[Dict[str, Any]]) -> None:
    """Batch insert US predictions using execute_values for performance."""
    if not rows:
        return
    rows = _dedupe_batch_by_event_id(rows)
    if not rows:
        return
    pool = get_pg_pool()
    connection = pool.getconn()
    try:
        with connection:
            with connection.cursor() as cursor:
                template = "(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, ST_SetSRID(ST_MakePoint(%s, %s), 4326))"
                values = []
                for r in rows:
                    event_time = normalize_optional_timestamp(r.get("event_time"))
                    ingestion_time = normalize_optional_timestamp(
                        r.get("ingestion_time")
                    )
                    processed_time = normalize_optional_timestamp(
                        r.get("processed_time")
                    )
                    values.append((
                        r.get("event_id"), r.get("event_year"), event_time,
                        r.get("lat"), r.get("lon"), r.get("true_severity"),
                        r.get("predicted_severity"), r.get("risk_score"),
                        r.get("weather_code"), r.get("temperature_f"),
                        r.get("humidity"), r.get("wind_speed_mph"),
                        r.get("visibility_mi"), r.get("road_type_code"),
                        r.get("hour"), r.get("day_of_week"), r.get("is_weekend"),
                        r.get("is_rush_hour"), r.get("is_junction"),
                        r.get("has_traffic_signal"), r.get("is_crossing"),
                        r.get("is_roundabout"), r.get("is_stop"),
                        r.get("is_station"), r.get("is_railway"),
                        r.get("is_night"), r.get("model_status"),
                        r.get("inference_latency_ms"), ingestion_time,
                        processed_time, r.get("end_to_end_latency_ms"),
                        r.get("lon"), r.get("lat"),
                    ))
                psycopg2.extras.execute_values(
                    cursor,
                    f"INSERT INTO {PG_US_TABLE} ({', '.join(US_COLUMNS)}, geom) VALUES %s "
                    f"ON CONFLICT (event_id) DO UPDATE SET "
                    + ", ".join(f"{col} = EXCLUDED.{col}" for col in US_COLUMNS)
                    + f", geom = EXCLUDED.geom, updated_at = NOW()",
                    values,
                    template=template,
                )
    except Exception:
        logger.exception("Batch insert failed for %d US rows", len(rows))
    finally:
        release_pg_connection(connection)


def _batch_insert_tomtom(rows: List[Dict[str, Any]]) -> None:
    """Batch insert TomTom incidents using execute_values for performance."""
    if not rows:
        return
    rows = _dedupe_batch_by_event_id(rows)
    if not rows:
        return
    pool = get_pg_pool()
    connection = pool.getconn()
    try:
        with connection:
            with connection.cursor() as cursor:
                all_columns = TOMTOM_COLUMNS + ["geom"]
                template = "(" + ", ".join(["%s"] * (len(TOMTOM_COLUMNS))) + ", ST_SetSRID(ST_MakePoint(%s, %s), 4326))"
                values = []
                for r in rows:
                    row = dict(r)
                    row["event_time"] = normalize_optional_timestamp(
                        row.get("event_time")
                    )
                    row["ingestion_time"] = normalize_optional_timestamp(
                        row.get("ingestion_time")
                    )
                    row["processed_time"] = normalize_optional_timestamp(
                        row.get("processed_time")
                    )
                    vals = tuple(row.get(col) for col in TOMTOM_COLUMNS) + (
                        row.get("lon"),
                        row.get("lat"),
                    )
                    values.append(vals)
                psycopg2.extras.execute_values(
                    cursor,
                    f"INSERT INTO {PG_TOMTOM_TABLE} ({', '.join(all_columns)}) VALUES %s "
                    f"ON CONFLICT (event_id) DO UPDATE SET "
                    + ", ".join(f"{col} = EXCLUDED.{col}" for col in TOMTOM_COLUMNS)
                    + f", geom = EXCLUDED.geom, updated_at = NOW()",
                    values,
                    template=template,
                )
    except Exception:
        logger.exception("Batch insert failed for %d TomTom rows", len(rows))
    finally:
        release_pg_connection(connection)


def flush_us_batch() -> None:
    """Flush the US batch buffer to PostgreSQL."""
    global US_BATCH_BUFFER, US_BATCH_BUFFER_STARTED_AT
    if US_BATCH_BUFFER:
        _batch_insert_us(US_BATCH_BUFFER)
        US_BATCH_BUFFER = []
        US_BATCH_BUFFER_STARTED_AT = None


def flush_tomtom_batch() -> None:
    """Flush the TomTom batch buffer to PostgreSQL."""
    global TOMTOM_BATCH_BUFFER, TOMTOM_BATCH_BUFFER_STARTED_AT
    if TOMTOM_BATCH_BUFFER:
        _batch_insert_tomtom(TOMTOM_BATCH_BUFFER)
        TOMTOM_BATCH_BUFFER = []
        TOMTOM_BATCH_BUFFER_STARTED_AT = None


def buffer_us_row(row: Dict[str, Any]) -> None:
    """Add a US prediction row to the batch buffer; flush when full."""
    global US_BATCH_BUFFER_STARTED_AT
    if US_BATCH_BUFFER_STARTED_AT is None:
        US_BATCH_BUFFER_STARTED_AT = time.monotonic()
    US_BATCH_BUFFER.append(row)
    if (
        len(US_BATCH_BUFFER) >= PG_BATCH_SIZE
        or _buffer_age_ms(US_BATCH_BUFFER_STARTED_AT) >= PG_MAX_BUFFER_AGE_MS
    ):
        flush_us_batch()


def buffer_tomtom_row(row: Dict[str, Any]) -> None:
    """Add a TomTom incident row to the batch buffer; flush when full."""
    global TOMTOM_BATCH_BUFFER_STARTED_AT
    if TOMTOM_BATCH_BUFFER_STARTED_AT is None:
        TOMTOM_BATCH_BUFFER_STARTED_AT = time.monotonic()
    TOMTOM_BATCH_BUFFER.append(row)
    if (
        len(TOMTOM_BATCH_BUFFER) >= PG_BATCH_SIZE
        or _buffer_age_ms(TOMTOM_BATCH_BUFFER_STARTED_AT) >= PG_MAX_BUFFER_AGE_MS
    ):
        flush_tomtom_batch()


def flush_stale_buffers_if_needed() -> None:
    """Bound end-to-end latency by flushing aged buffers even when they are not full."""
    if _ML_INFERENCE_BUFFER and _buffer_age_ms(_ML_INFERENCE_BUFFER_STARTED_AT) >= ML_MAX_BUFFER_AGE_MS:
        flush_ml_batch()
    if US_BATCH_BUFFER and _buffer_age_ms(US_BATCH_BUFFER_STARTED_AT) >= PG_MAX_BUFFER_AGE_MS:
        flush_us_batch()
    if TOMTOM_BATCH_BUFFER and _buffer_age_ms(TOMTOM_BATCH_BUFFER_STARTED_AT) >= PG_MAX_BUFFER_AGE_MS:
        flush_tomtom_batch()
    for prefix in list(_SILVER_BATCH_STARTED_AT):
        if _buffer_age_ms(_SILVER_BATCH_STARTED_AT.get(prefix)) >= SILVER_MAX_BUFFER_AGE_MS:
            flush_silver_prefix(prefix)


# ---------------------------------------------------------------------------
# Message processing (hot-path functions called by Flink map operators)
# ---------------------------------------------------------------------------


def process_us_message(raw_message: str) -> str:
    """Process one US replay message from Kafka.

    Steps: parse JSON -> feature engineering -> Silver GCS write
           -> buffer for micro-batch MLflow inference
           -> unified risk score -> batch PostgreSQL insert.
    """
    try:
        flush_stale_buffers_if_needed()
        raw_row = json.loads(raw_message)
        ingestion_time = normalize_optional_timestamp(raw_row.get("_ingested_at_utc"))
        features = build_features(raw_row)
        if features is None:
            raise ValueError("US feature engineering returned no record")
        features["event_id"] = replay_instance_event_id(
            features.get("event_id"),
            raw_row,
        )
        features["ingestion_time"] = ingestion_time

        # Silver layer write (optional, can be disabled for throughput testing)
        write_to_gcs_silver(features)

        # Buffer for micro-batch MLflow inference.
        # call_mlflow_model stores the event; when buffer hits ML_BATCH_SIZE
        # it flushes all events to MLflow in a single HTTP request.
        call_mlflow_model(features)

        return f"US_BUFFERED: {features.get('event_id')}"
    except Exception as exc:
        logger.exception("US message processing failed: %s", str(raw_message)[:200])
        return f"US_FAIL: {exc}"


def process_tomtom_message(raw_message: str) -> str:
    """Process one TomTom live incident message from Kafka.

    Steps: parse JSON -> enrichment -> feature engineering -> unified risk score
           -> buffer for batch PostgreSQL insert.
    No MLflow inference is used for TomTom events.
    """
    try:
        flush_stale_buffers_if_needed()
        raw_row = json.loads(raw_message)
        ingestion_time = normalize_optional_timestamp(
            raw_row.get("_ingested_at_utc") or raw_row.get("ingestion_time")
        )
        enriched = enrich_tomtom_event(raw_row)
        if enriched is None:
            raise ValueError("TomTom enrichment returned no record")

        features = build_features(enriched)
        if features is None:
            raise ValueError("TomTom feature engineering returned no record")

        severity = features.get("true_severity")

        # Unified risk score using TomTom delay/length + context features
        risk_score = compute_risk_score(
            severity=severity,
            delay_seconds=raw_row.get("delay_seconds"),
            length_meters=raw_row.get("length_meters"),
            is_night=features.get("is_night"),
            is_weekend=features.get("is_weekend"),
            road_type_code=features.get("road_type_code"),
            weather_code=features.get("weather_code"),
        )

        processed_time = datetime.now(timezone.utc).isoformat()
        e2e_latency = parse_latency_ms(ingestion_time, processed_time)

        row = {
            **features,
            "incident_id": raw_row.get("incident_id"),
            "severity": severity,
            "risk_score": risk_score,
            "icon_category": to_int(raw_row.get("icon_category")),
            "delay_magnitude": to_int(raw_row.get("delay_magnitude")),
            "delay_seconds": to_float(raw_row.get("delay_seconds")),
            "length_meters": to_float(raw_row.get("length_meters")),
            "state_or_region": raw_row.get("state_or_region"),
            "city": raw_row.get("city"),
            "from_road": raw_row.get("from_road"),
            "to_road": raw_row.get("to_road"),
            "geometry_wkt": raw_row.get("geometry_wkt"),
            "model_status": "rule_based",
            "ingestion_time": ingestion_time,
            "processed_time": processed_time,
            "end_to_end_latency_ms": e2e_latency,
        }
        buffer_tomtom_row(row)
        return f"TOMTOM_OK: {features.get('event_id')}"
    except Exception as exc:
        logger.exception("TomTom message processing failed: %s", str(raw_message)[:200])
        return f"TOMTOM_FAIL: {exc}"


# ---------------------------------------------------------------------------
# Kafka source builder
# ---------------------------------------------------------------------------


def build_kafka_source(topic: str, group_id: str, source_name: str):
    """Create a Kafka source for a single raw topic."""
    source = (
        KafkaSource.builder()
        .set_bootstrap_servers(KAFKA_BOOTSTRAP_SERVERS)
        .set_topics(topic)
        .set_group_id(group_id)
        .set_starting_offsets(KafkaOffsetsInitializer.earliest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )
    return source, source_name


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------


def main() -> None:
    """Build and start the unified Flink job."""
    logger.info("=" * 80)
    logger.info("Starting unified Flink job for US replay and TomTom live streams")
    logger.info("Kafka bootstrap: %s", KAFKA_BOOTSTRAP_SERVERS)
    logger.info("US topic: %s -> PostgreSQL table: %s", KAFKA_TOPIC_US_RAW, PG_US_TABLE)
    logger.info(
        "TomTom topic: %s -> PostgreSQL table: %s",
        KAFKA_TOPIC_TOMTOM_RAW,
        PG_TOMTOM_TABLE,
    )
    logger.info("US MLflow endpoint: %s", MLFLOW_SERVING_ENDPOINT)
    logger.info("ML micro-batch size: %d events per HTTP call", ML_BATCH_SIZE)
    logger.info("US Silver path: %s", SILVER_FEATURES_PATH)
    logger.info(
        "PG pool: min=%d max=%d batch_size=%d",
        PG_POOL_MIN,
        PG_POOL_MAX,
        PG_BATCH_SIZE,
    )
    logger.info(
        "Silver write: enabled=%s batch_size=%d",
        SILVER_WRITE_ENABLED,
        SILVER_FLUSH_EVERY_N,
    )
    logger.info(
        "Buffer age limits: ml=%dms pg=%dms silver=%dms",
        ML_MAX_BUFFER_AGE_MS,
        PG_MAX_BUFFER_AGE_MS,
        SILVER_MAX_BUFFER_AGE_MS,
    )
    logger.info("=" * 80)

    # Initialize database schemas once before the hot path.
    initialize_schemas()

    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(FLINK_PARALLELISM)
    env.enable_checkpointing(FLINK_CHECKPOINT_INTERVAL)

    checkpoint_config = env.get_checkpoint_config()
    if hasattr(checkpoint_config, "set_checkpoint_timeout"):
        checkpoint_config.set_checkpoint_timeout(FLINK_CHECKPOINT_TIMEOUT)
    if hasattr(checkpoint_config, "set_min_pause_between_checkpoints"):
        checkpoint_config.set_min_pause_between_checkpoints(FLINK_CHECKPOINT_MIN_PAUSE)
    if hasattr(checkpoint_config, "set_tolerable_checkpoint_failure_number"):
        checkpoint_config.set_tolerable_checkpoint_failure_number(
            FLINK_TOLERABLE_FAILED_CHECKPOINTS
        )
    if FileSystemCheckpointStorage is not None:
        checkpoint_config.set_checkpoint_storage(
            FileSystemCheckpointStorage(FLINK_CHECKPOINT_DIR)
        )
    else:
        logger.warning("Checkpoint storage configuration is using PyFlink defaults.")

    connector_jars = [
        jar.strip() for jar in FLINK_KAFKA_CONNECTOR_JAR.split(",") if jar.strip()
    ]
    if connector_jars:
        env.add_jars(*connector_jars)

    us_source, us_source_name = build_kafka_source(
        KAFKA_TOPIC_US_RAW,
        FLINK_INFERENCE_GROUP,
        "kafka-us-raw-source",
    )
    tomtom_source, tomtom_source_name = build_kafka_source(
        KAFKA_TOPIC_TOMTOM_RAW,
        FLINK_TOMTOM_GROUP,
        "kafka-tomtom-raw-source",
    )

    us_stream = env.from_source(
        source=us_source,
        watermark_strategy=WatermarkStrategy.no_watermarks(),
        source_name=us_source_name,
    ).map(process_us_message, output_type=Types.STRING())

    tomtom_stream = env.from_source(
        source=tomtom_source,
        watermark_strategy=WatermarkStrategy.no_watermarks(),
        source_name=tomtom_source_name,
    ).map(process_tomtom_message, output_type=Types.STRING())

    if FLINK_PRINT_EACH_EVENT:
        us_stream.union(tomtom_stream).print()
    else:
        us_stream.add_sink(
            SinkFunction("org.apache.flink.streaming.api.functions.sink.DiscardingSink")
        )
        tomtom_stream.add_sink(
            SinkFunction("org.apache.flink.streaming.api.functions.sink.DiscardingSink")
        )

    # Flush any remaining buffered rows before the job exits.
    import atexit
    atexit.register(flush_all_silver_batches)
    atexit.register(flush_ml_batch)
    atexit.register(flush_us_batch)
    atexit.register(flush_tomtom_batch)

    env.execute("Unified Traffic Streaming - US Replay + TomTom Live")


if __name__ == "__main__":
    main()
