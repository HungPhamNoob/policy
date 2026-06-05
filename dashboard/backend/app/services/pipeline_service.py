"""Best-effort operational metrics for the dashboard pipeline health page."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import os
import re
import shutil
import subprocess
from typing import Any
import uuid

from psycopg2 import sql

from app.core.config import get_settings
from app.core.database import fetch_all, fetch_one
from app.services.prediction_service import table_identifier

LATENCY_SANITY_MAX_MS = 3_600_000.0
EXPENSIVE_SCAN_ROW_THRESHOLD = 500_000
_RETRAIN_LOOP_CACHE: dict[str, Any] = {}
_RETRAIN_LOOP_CACHE_TS: float = 0.0
_RETRAIN_LOOP_CACHE_TTL_S = 30.0
_REPLAY_PROGRESS_CACHE: dict[str, int] = {}


def _is_root_retrain_run(row: Any) -> bool:
    run_name = str(row.get("tags.mlflow.runName") or "")
    run_type_tag = str(row.get("tags.run_type") or "")
    run_type_param = str(row.get("params.run_type") or "")
    run_role = str(row.get("tags.run_role") or "")
    parent_id = row.get("tags.mlflow.parentRunId")
    if run_role == "retrain_parent":
        return True
    if parent_id:
        return False
    return (
        run_name == "h2o_retrain_online"
        or run_type_tag == "retrain_online"
        or run_type_param == "retrain_online"
    )


def _coerce_metric_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return None


def _expected_models_for_root_run(row: Any) -> int | None:
    return _coerce_metric_int(
        row.get("params.top_k_models")
        or row.get("params.expected_top_models")
        or row.get("params.H2O_TOP_K_MODELS")
    )


def _logged_models_for_root_run(row: Any) -> int | None:
    return _coerce_metric_int(
        row.get("metrics.models_logged") or row.get("params.models_logged")
    )


def _root_run_is_complete(row: Any) -> bool:
    if str(row.get("tags.run_role") or "") != "retrain_parent":
        return False
    if str(row.get("status") or "").upper() != "FINISHED":
        return False
    expected_models = _expected_models_for_root_run(row)
    logged_models = _logged_models_for_root_run(row)
    if expected_models is None or logged_models is None:
        return False
    return logged_models >= expected_models


def _prediction_table_name() -> str:
    return get_settings().prediction_table.split(".")[-1]


def _prediction_table_names() -> list[str]:
    settings = get_settings()
    names = [
        settings.us_prediction_table.split(".")[-1],
        settings.tomtom_events_table.split(".")[-1],
    ]
    return list(dict.fromkeys(names))


def _table_columns(table_name: str | None = None) -> set[str]:
    query = """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = %(table_name)s
    """
    rows = fetch_all(query, {"table_name": table_name or _prediction_table_name()})
    return {str(row["column_name"]) for row in rows}


def _columns_for_table(table_name: str) -> set[str]:
    """Return columns for a table while keeping older tests easy to monkeypatch."""
    try:
        return _table_columns(table_name)
    except TypeError:
        return _table_columns()


def _parse_window_seconds(window: str) -> int:
    match = re.fullmatch(r"\s*(\d+)\s*([smhd])\s*", window or "5m")
    if not match:
        return 300
    value = int(match.group(1))
    unit = match.group(2)
    multipliers = {"s": 1, "m": 60, "h": 3600, "d": 86400}
    return max(1, value * multipliers[unit])


def _time_column(columns: set[str]) -> str | None:
    if "processed_time" in columns:
        return "processed_time"
    if "updated_at" in columns:
        return "updated_at"
    if "created_at" in columns:
        return "created_at"
    if "ingestion_time" in columns:
        return "ingestion_time"
    if "event_time" in columns:
        return "event_time"
    return None


def _coerce_utc_timestamp(value: Any) -> datetime | None:
    if not isinstance(value, datetime):
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _latest_table_timestamp(table_name: str, column: str) -> datetime | None:
    query = sql.SQL(
        """
        SELECT {time_column} AS latest_time
        FROM {table}
        WHERE {time_column} IS NOT NULL
        ORDER BY {time_column} DESC NULLS LAST
        LIMIT 1
        """
    ).format(
        table=table_identifier(table_name),
        time_column=sql.Identifier(column),
    )
    row = fetch_one(query) or {}
    return _coerce_utc_timestamp(row.get("latest_time"))


def _latest_source_timestamp(table_name: str, column: str | None) -> datetime | None:
    if not column:
        return None
    if _skip_expensive_time_scan(table_name, column):
        return None
    return _latest_table_timestamp(table_name, column)


def _latest_replay_row_index(
    table_name: str,
    column: str | None,
) -> int | None:
    if not column:
        return None
    query = sql.SQL(
        """
        SELECT (regexp_match(event_id, '@c[0-9]+-p[0-9]+-r([0-9]+)$'))[1]::BIGINT AS replay_row_index
        FROM {table}
        WHERE event_id ~ '@c[0-9]+-p[0-9]+-r[0-9]+$'
        ORDER BY {time_column} DESC NULLS LAST
        LIMIT 1
        """
    ).format(
        table=table_identifier(table_name),
        time_column=sql.Identifier(column),
    )
    row = fetch_one(query) or {}
    value = row.get("replay_row_index")
    if value is None:
        return _REPLAY_PROGRESS_CACHE.get(table_name)
    replay_row_index = int(value)
    replay_row_index = max(
        replay_row_index,
        _REPLAY_PROGRESS_CACHE.get(table_name, replay_row_index),
    )
    _REPLAY_PROGRESS_CACHE[table_name] = replay_row_index
    return replay_row_index


def _table_row_estimate(table_name: str) -> int:
    row = fetch_one(
        """
        SELECT COALESCE(c.reltuples, 0)::BIGINT AS row_estimate
        FROM pg_class AS c
        JOIN pg_namespace AS n ON n.oid = c.relnamespace
        WHERE n.nspname = 'public' AND c.relname = %(table_name)s
        """,
        {"table_name": table_name},
    )
    return int(row.get("row_estimate") or 0) if row else 0


def _exact_table_row_count(table_name: str) -> int:
    query = sql.SQL("SELECT COUNT(*)::BIGINT AS row_count FROM {table}").format(
        table=table_identifier(table_name)
    )
    row = fetch_one(query) or {}
    return int(row.get("row_count") or 0)


def _index_valid(index_name: str) -> bool:
    row = fetch_one(
        """
        SELECT COALESCE(
            (
                SELECT i.indisvalid
                FROM pg_index AS i
                JOIN pg_class AS c ON c.oid = i.indexrelid
                WHERE c.relname = %(index_name)s
            ),
            FALSE
        ) AS is_valid
        """,
        {"index_name": index_name},
    )
    return bool(row and row.get("is_valid"))


def _time_index_name(table_name: str, column: str) -> str | None:
    if table_name == "traffic_risk_predictions" and column == "event_time":
        return "idx_traffic_risk_predictions_event_time"
    if table_name == "traffic_risk_predictions" and column == "processed_time":
        return "idx_traffic_risk_predictions_processed_time"
    if table_name == "traffic_tomtom_incidents" and column == "event_time":
        return "idx_traffic_tomtom_incidents_event_time"
    return None


def _skip_expensive_time_scan(table_name: str, column: str) -> bool:
    index_name = _time_index_name(table_name, column)
    if index_name and _index_valid(index_name):
        return False
    return _table_row_estimate(table_name) >= EXPENSIVE_SCAN_ROW_THRESHOLD


def _window_anchor(
    latest_times: list[datetime],
    window_seconds: int,
) -> tuple[datetime, bool]:
    now = datetime.now(timezone.utc)
    if not latest_times:
        return now, True
    latest_time = max(latest_times)
    if latest_time >= now - timedelta(seconds=window_seconds):
        return now, True
    return latest_time, False


def throughput(window: str) -> dict[str, Any]:
    """Return event throughput over the requested lookback window."""
    window_seconds = _parse_window_seconds(window)
    sources = []
    latest_times: list[datetime] = []

    for table_name in _prediction_table_names():
        columns = _columns_for_table(table_name)
        column = _time_column(columns)
        if not column:
            sources.append(
                {
                    "table": table_name,
                    "status": "unavailable",
                    "event_count": 0,
                    "time_column": None,
                    "latest_timestamp": None,
                }
            )
            continue
        if _skip_expensive_time_scan(table_name, column):
            sources.append(
                {
                    "table": table_name,
                    "status": "degraded",
                    "event_count": 0,
                    "time_column": None,
                    "latest_timestamp": None,
                }
            )
            continue
        latest_timestamp = _latest_table_timestamp(table_name, column)
        if latest_timestamp:
            latest_times.append(latest_timestamp)
        sources.append(
            {
                "table": table_name,
                "time_column": column,
                "latest_timestamp": (
                    latest_timestamp.isoformat() if latest_timestamp else None
                ),
            }
        )

    if not sources or all(
        source.get("status") == "unavailable" for source in sources
    ):
        return {
            "status": "unavailable",
            "window": window,
            "event_count": 0,
            "events_per_minute": 0.0,
            "events_per_second": 0.0,
            "sources": [],
        }

    window_anchor, anchored_to_now = _window_anchor(latest_times, window_seconds)
    anchor_params = {
        "window_seconds": window_seconds,
        "window_anchor": window_anchor.replace(tzinfo=None),
    }
    total_count = 0
    for source in sources:
        if not source.get("time_column"):
            continue
        query = sql.SQL(
            """
            SELECT COUNT(*)::BIGINT AS event_count
            FROM {table}
            WHERE {time_column} >= (%(window_anchor)s - (%(window_seconds)s * INTERVAL '1 second'))
              AND {time_column} <= %(window_anchor)s
            """
        ).format(
            table=table_identifier(str(source["table"])),
            time_column=sql.Identifier(str(source["time_column"])),
        )
        row = fetch_one(query, anchor_params) or {}
        count = int(row.get("event_count") or 0)
        total_count += count
        source["event_count"] = count
        source["status"] = (
            "ok"
            if count and anchored_to_now
            else "stale"
            if count
            else "not_enough_data"
        )

    return {
        "status": (
            "ok"
            if total_count and anchored_to_now
            else "stale"
            if total_count
            else "not_enough_data"
        ),
        "window": window,
        "window_seconds": window_seconds,
        "window_anchor": window_anchor.isoformat(),
        "is_live_window": anchored_to_now,
        "event_count": total_count,
        "events_per_minute": round(total_count / (window_seconds / 60.0), 4),
        "events_per_second": round(total_count / window_seconds, 4),
        "sources": sources,
    }


def latency(metric: str, window: str = "5m") -> dict[str, Any]:
    """Return latency percentiles over a recent window."""
    window_seconds = _parse_window_seconds(window)
    selects: list[sql.Composable] = []
    source_columns: dict[str, dict[str, Any]] = {}
    latest_times: list[datetime] = []
    for table_name in _prediction_table_names():
        columns = _columns_for_table(table_name)
        latency_columns = [
            column
            for column in (
                "processing_latency_ms",
                "inference_latency_ms",
                "end_to_end_latency_ms",
            )
            if column in columns
        ]
        time_column = _time_column(columns)
        if not latency_columns or not time_column:
            continue
        if _skip_expensive_time_scan(table_name, time_column):
            continue
        column = latency_columns[0]
        latest_timestamp = _latest_table_timestamp(table_name, time_column)
        if latest_timestamp:
            latest_times.append(latest_timestamp)
        source_columns[table_name] = {
            "latency_columns": latency_columns,
            "selected_latency_column": column,
            "time_column": time_column,
            "latest_timestamp": (
                latest_timestamp.isoformat() if latest_timestamp else None
            ),
        }

    if not source_columns:
        return {"status": "unavailable", "metric": metric, "window": window, "columns": []}

    window_anchor, anchored_to_now = _window_anchor(latest_times, window_seconds)
    for table_name, metadata in source_columns.items():
        selects.append(
            sql.SQL(
                """
                SELECT {latency_column} AS latency_ms
                FROM {table}
                WHERE {latency_column} IS NOT NULL
                  AND {latency_column} >= 0
                  AND {latency_column} <= %(latency_sanity_max_ms)s
                  AND {time_column} >= (%(window_anchor)s - (%(window_seconds)s * INTERVAL '1 second'))
                  AND {time_column} <= %(window_anchor)s
                """
            ).format(
                table=table_identifier(table_name),
                latency_column=sql.Identifier(str(metadata["latency_columns"][0])),
                time_column=sql.Identifier(str(metadata["time_column"])),
            )
        )

    query = sql.SQL(
        """
        SELECT
            percentile_cont(0.5) WITHIN GROUP (ORDER BY latency_ms)::DOUBLE PRECISION AS p50,
            percentile_cont(0.95) WITHIN GROUP (ORDER BY latency_ms)::DOUBLE PRECISION AS p95,
            percentile_cont(0.99) WITHIN GROUP (ORDER BY latency_ms)::DOUBLE PRECISION AS p99,
            AVG(latency_ms)::DOUBLE PRECISION AS avg,
            COUNT(latency_ms)::BIGINT AS sample_count
        FROM ({union_query}) AS latency_samples
        """
    ).format(union_query=sql.SQL(" UNION ALL ").join(selects))
    row = fetch_one(
        query,
        {
            "latency_sanity_max_ms": LATENCY_SANITY_MAX_MS,
            "window_seconds": window_seconds,
            "window_anchor": window_anchor.replace(tzinfo=None),
        },
    ) or {}
    source_latency: dict[str, Any] = {}
    for table_name, metadata in source_columns.items():
        source_query = sql.SQL(
            """
            SELECT
                percentile_cont(0.5) WITHIN GROUP (ORDER BY latency_ms)::DOUBLE PRECISION AS p50,
                percentile_cont(0.95) WITHIN GROUP (ORDER BY latency_ms)::DOUBLE PRECISION AS p95,
                percentile_cont(0.99) WITHIN GROUP (ORDER BY latency_ms)::DOUBLE PRECISION AS p99,
                AVG(latency_ms)::DOUBLE PRECISION AS avg,
                COUNT(latency_ms)::BIGINT AS sample_count
            FROM (
                SELECT {latency_column} AS latency_ms
                FROM {table}
                WHERE {latency_column} IS NOT NULL
                  AND {latency_column} >= 0
                  AND {latency_column} <= %(latency_sanity_max_ms)s
                  AND {time_column} >= (%(window_anchor)s - (%(window_seconds)s * INTERVAL '1 second'))
                  AND {time_column} <= %(window_anchor)s
            ) AS source_latency_samples
            """
        ).format(
            table=table_identifier(table_name),
            latency_column=sql.Identifier(str(metadata["selected_latency_column"])),
            time_column=sql.Identifier(str(metadata["time_column"])),
        )
        source_row = fetch_one(
            source_query,
            {
                "latency_sanity_max_ms": LATENCY_SANITY_MAX_MS,
                "window_seconds": window_seconds,
                "window_anchor": window_anchor.replace(tzinfo=None),
            },
        ) or {}
        source_latency[table_name] = {
            "latency_column": metadata["selected_latency_column"],
            "p50": source_row.get("p50"),
            "p95": source_row.get("p95"),
            "p99": source_row.get("p99"),
            "avg": source_row.get("avg"),
            "sample_count": source_row.get("sample_count") or 0,
        }
    allowed = {"p50", "p95", "p99", "avg"}
    metric_key = metric if metric in allowed else "p95"
    return {
        "status": (
            "ok"
            if row.get("sample_count") and anchored_to_now
            else "stale"
            if row.get("sample_count")
            else "not_enough_data"
        ),
        "metric": metric_key,
        "window": window,
        "window_seconds": window_seconds,
        "window_anchor": window_anchor.isoformat(),
        "is_live_window": anchored_to_now,
        "value_ms": row.get(metric_key),
        "latency_ms": {
            "p50": row.get("p50"),
            "p95": row.get("p95"),
            "p99": row.get("p99"),
            "avg": row.get("avg"),
        },
        "sample_count": row.get("sample_count") or 0,
        "columns": source_columns,
        "sources": source_latency,
    }


def _compute_retrain_loop_status(
    source_health: list[dict[str, Any]],
    replay_rows: int,
    settings: Any,
) -> dict[str, Any]:
    """Determine retrain loop CONTINUE / FINISHED / FAILED from live conditions.

    CONTINUE = there is data in the system AND either:
        - Events are still arriving (within schedule window), OR
        - TomTom incidents are still streaming, OR
        - Less than 5 successful retrain batches completed
    FINISHED = ALL conditions true:
        1. No new events in 3x schedule window (minimum 60 min idle)
        2. At least 5 successful retrain batches completed
        3. TomTom incident stream is also idle for 3x schedule window
    FAILED = most recent retrain run errored AND no successful runs recently
    """
    global _RETRAIN_LOOP_CACHE, _RETRAIN_LOOP_CACHE_TS
    now_ts = datetime.now(timezone.utc).timestamp()
    if _RETRAIN_LOOP_CACHE and (now_ts - _RETRAIN_LOOP_CACHE_TS) < _RETRAIN_LOOP_CACHE_TTL_S:
        return _RETRAIN_LOOP_CACHE

    MIN_RETRAIN_BATCHES_BEFORE_FINISHED = int(
        os.getenv("MIN_RETRAIN_BATCHES_BEFORE_FINISHED", "5")
    )

    # Parse Airflow schedule window
    schedule_str = os.getenv("AIRFLOW_MODEL_RETRAIN_SCHEDULE", "*/45 * * * *")
    match = re.fullmatch(r"\*/(\d+)\s+\*\s+\*\s+\*\s+\*", schedule_str)
    airflow_schedule_minutes = int(match.group(1)) if match else 45
    # Use 3x schedule window as the idle threshold while still reporting the real
    # Airflow cadence separately to the dashboard.
    idle_window_minutes = airflow_schedule_minutes * 3
    data_stale_window_seconds = idle_window_minutes * 60

    # Latest pipeline activity across ALL sources (prediction + tomtom).
    # For replay datasets, event_time is historical and should not be used to
    # decide whether the pipeline is still making progress.
    latest_prediction_time: datetime | None = None
    latest_tomtom_time: datetime | None = None
    latest_prediction_event_time: datetime | None = None
    latest_tomtom_event_time: datetime | None = None
    for source in source_health:
        event_time = None
        activity_time = None
        et_str = source.get("latest_event_time")
        activity_str = source.get("latest_created_at")
        try:
            if et_str:
                event_time = datetime.fromisoformat(et_str)
        except (ValueError, TypeError):
            event_time = None
        try:
            if activity_str:
                activity_time = datetime.fromisoformat(activity_str)
        except (ValueError, TypeError):
            activity_time = None
        table = source.get("table", "")
        if "tomtom" in table.lower():
            if event_time is not None and (
                latest_tomtom_event_time is None or event_time > latest_tomtom_event_time
            ):
                latest_tomtom_event_time = event_time
            candidate = activity_time or event_time
            if candidate is not None and (
                latest_tomtom_time is None or candidate > latest_tomtom_time
            ):
                latest_tomtom_time = candidate
        else:
            if event_time is not None and (
                latest_prediction_event_time is None
                or event_time > latest_prediction_event_time
            ):
                latest_prediction_event_time = event_time
            candidate = activity_time or event_time
            if candidate is not None and (
                latest_prediction_time is None or candidate > latest_prediction_time
            ):
                latest_prediction_time = candidate

    now = datetime.now(timezone.utc)
    has_prediction_data = replay_rows > 0

    # Determine staleness for each stream independently
    prediction_is_stale = False
    if latest_prediction_time is not None:
        prediction_is_stale = (now - latest_prediction_time).total_seconds() > data_stale_window_seconds
    elif has_prediction_data:
        prediction_is_stale = False  # Has data, just no recent timestamp column
    else:
        prediction_is_stale = True  # No data at all

    tomtom_is_stale = False
    if latest_tomtom_time is not None:
        tomtom_is_stale = (now - latest_tomtom_time).total_seconds() > data_stale_window_seconds

    # BOTH streams must be stale for the system to be considered idle
    all_streams_stale = prediction_is_stale and (tomtom_is_stale or latest_tomtom_time is None)

    # Count MLflow retrain runs (cached)
    recent_failed = False
    recent_incomplete = False
    recent_finished_count = 0
    recent_total_count = 0
    mlflow_available = False
    try:
        import mlflow
        mlflow.set_tracking_uri(settings.mlflow_tracking_uri)
        experiment = mlflow.get_experiment_by_name(settings.mlflow_experiment_name)
        if experiment is not None:
            runs = mlflow.search_runs(
                experiment_ids=[experiment.experiment_id],
                max_results=100,
                order_by=["start_time DESC"],
            )
            latest_root_status = None
            for _, row in runs.iterrows():
                if not _is_root_retrain_run(row):
                    continue
                recent_total_count += 1
                if latest_root_status is None:
                    latest_root_status = str(row.get("status", "")).upper()
                    if latest_root_status == "FAILED":
                        recent_failed = True
                    elif latest_root_status == "FINISHED" and not _root_run_is_complete(
                        row
                    ):
                        recent_incomplete = True
                status = str(row.get("status", "")).upper()
                if status == "FINISHED" and _root_run_is_complete(row):
                    recent_finished_count += 1
            mlflow_available = True
    except Exception:
        pass

    # Build the latest event timestamp for reporting
    times = []
    if latest_prediction_time:
        times.append(latest_prediction_time)
    if latest_tomtom_time:
        times.append(latest_tomtom_time)
    latest_event_time = max(times) if times else None

    # ---- Determine loop status ----
    if recent_failed and recent_finished_count == 0:
        loop_status = "failed"
        loop_reason = "Most recent retrain run FAILED and no successful retrain completed. Airflow will retry on next tick."
    elif recent_incomplete and recent_finished_count == 0:
        loop_status = "failed"
        loop_reason = (
            "Most recent retrain run finished without logging the full top-k model set. "
            "Treating it as FAILED until a complete retrain batch succeeds."
        )
    elif not has_prediction_data and recent_total_count == 0:
        loop_status = "continue"
        loop_reason = "Waiting for initial data replay to begin. Replay events = 0."
    elif all_streams_stale and recent_finished_count >= MIN_RETRAIN_BATCHES_BEFORE_FINISHED:
        loop_status = "finished"
        loop_reason = (
            f"All data streams are idle for {idle_window_minutes} minutes "
            f"(prediction idle: {prediction_is_stale}, tomtom idle: {tomtom_is_stale}). "
            f"{recent_finished_count} retrain batches completed successfully. "
            f"Latest event: {latest_event_time.isoformat() if latest_event_time else 'N/A'}."
        )
    elif has_prediction_data and prediction_is_stale and latest_tomtom_time is not None and not tomtom_is_stale:
        loop_status = "stalled"
        loop_reason = (
            "US replay/prediction ingestion has stopped advancing while TomTom live traffic is still flowing. "
            f"Latest prediction activity: {latest_prediction_time.isoformat() if latest_prediction_time else 'N/A'}. "
            f"Latest prediction event timestamp: {latest_prediction_event_time.isoformat() if latest_prediction_event_time else 'N/A'}. "
            f"Latest TomTom activity: {latest_tomtom_time.isoformat()}."
        )
    else:
        loop_status = "continue"
        parts = []
        if has_prediction_data and not prediction_is_stale:
            parts.append(f"Prediction data is actively flowing (latest: {latest_prediction_time.isoformat() if latest_prediction_time else 'N/A'})")
        if latest_tomtom_time is not None and not tomtom_is_stale:
            parts.append(f"TomTom incidents are streaming (latest: {latest_tomtom_time.isoformat()})")
        if recent_finished_count < MIN_RETRAIN_BATCHES_BEFORE_FINISHED:
            parts.append(f"Retrain batches completed: {recent_finished_count}/{MIN_RETRAIN_BATCHES_BEFORE_FINISHED} (minimum required before FINISHED)")
        if not parts:
            parts.append(f"Retrains scheduled every {airflow_schedule_minutes} min. Events: {replay_rows:,}")
        loop_reason = ". ".join(parts) + "."

    result = {
        "status": loop_status,
        "reason": loop_reason,
        "latest_event_time": latest_event_time.isoformat() if latest_event_time else None,
        "latest_prediction_time": latest_prediction_time.isoformat() if latest_prediction_time else None,
        "latest_tomtom_time": latest_tomtom_time.isoformat() if latest_tomtom_time else None,
        "latest_prediction_event_time": (
            latest_prediction_event_time.isoformat()
            if latest_prediction_event_time
            else None
        ),
        "latest_tomtom_event_time": (
            latest_tomtom_event_time.isoformat() if latest_tomtom_event_time else None
        ),
        "schedule_interval_minutes": airflow_schedule_minutes,
        "schedule_window_minutes": airflow_schedule_minutes,
        "idle_window_minutes": idle_window_minutes,
        "mlflow_available": mlflow_available,
        "has_data": has_prediction_data,
        "prediction_is_stale": prediction_is_stale,
        "tomtom_is_stale": tomtom_is_stale,
        "recent_retrain_runs": recent_total_count,
        "recent_retrain_finished": recent_finished_count,
        "min_retrain_batches_required": MIN_RETRAIN_BATCHES_BEFORE_FINISHED,
        "latest_retrain_incomplete": recent_incomplete,
    }
    _RETRAIN_LOOP_CACHE = result
    _RETRAIN_LOOP_CACHE_TS = now_ts
    return result


def replay_health() -> dict[str, Any]:
    """Return recent replay and model-status metadata from the prediction table."""
    settings = get_settings()
    source_health = []
    total_rows = 0
    replay_rows = 0
    for table_name in _prediction_table_names():
        columns = _columns_for_table(table_name)
        if not columns:
            source_health.append(
                {"table": table_name, "status": "unavailable", "row_count": 0}
            )
            continue

        latest_event_time = None
        activity_column = _time_column(columns)
        latest_insert_time = _latest_source_timestamp(table_name, activity_column)
        latest_replay_row_index = None
        if table_name == settings.us_prediction_table.split(".")[-1]:
            latest_replay_row_index = _latest_replay_row_index(
                table_name,
                activity_column,
            )
        if "event_time" in columns:
            latest_event_time = _latest_source_timestamp(table_name, "event_time")
        row_estimate = _table_row_estimate(table_name)
        if (
            table_name == settings.us_prediction_table.split(".")[-1]
            or row_estimate <= EXPENSIVE_SCAN_ROW_THRESHOLD
        ):
            row_count = _exact_table_row_count(table_name)
        else:
            row_count = row_estimate
        stored_row_count = row_count
        compatibility_latest_event_time = latest_event_time
        compatibility_row_count = row_count
        if latest_replay_row_index is not None:
            compatibility_row_count = latest_replay_row_index + 1
            compatibility_latest_event_time = latest_insert_time or latest_event_time
        total_rows += stored_row_count
        if table_name == settings.us_prediction_table.split(".")[-1]:
            replay_rows = compatibility_row_count

        source_health.append(
            {
                "table": table_name,
                "status": "ok" if compatibility_row_count else "not_enough_data",
                "row_count": compatibility_row_count,
                "stored_row_count": stored_row_count,
                "row_estimate": row_estimate,
                "latest_event_time": (
                    compatibility_latest_event_time.isoformat()
                    if compatibility_latest_event_time
                    else None
                ),
                "latest_created_at": (
                    latest_insert_time.isoformat() if latest_insert_time else None
                ),
                "latest_replay_row_index": latest_replay_row_index,
                "model_status": [],
            }
        )

    if not source_health:
        return {
            "status": "unavailable",
            "row_count": 0,
            "latest_event_time": None,
            "latest_created_at": None,
            "model_status": [],
            "retrain_min_us_rows": settings.retrain_min_us_rows,
            "retrain_ready": False,
            "retrain_policy": "new_silver_data_only",
            "retrain_loop": {
                "status": "continue",
                "reason": "No source tables available yet.",
                "latest_event_time": None,
                "schedule_interval_minutes": 45,
                "schedule_window_minutes": 45,
                "idle_window_minutes": 135,
                "mlflow_available": False,
                "has_data": False,
                "data_is_stale": False,
                "recent_retrain_runs": 0,
            },
            "sources": source_health,
        }

    retrain_loop = _compute_retrain_loop_status(source_health, replay_rows, settings)

    return {
        "status": "ok" if total_rows else "not_enough_data",
        "row_count": replay_rows,
        "total_row_count": total_rows,
        "retrain_min_us_rows": settings.retrain_min_us_rows,
        "retrain_ready": replay_rows >= settings.retrain_min_us_rows,
        "retrain_policy": "new_silver_data_only",
        "retrain_loop": retrain_loop,
        "sources": source_health,
    }


def _reset_state_file() -> Path:
    settings = get_settings()
    state_dir = Path(settings.pipeline_reset_log_dir)
    state_dir.mkdir(parents=True, exist_ok=True)
    return state_dir / "full_realtime_reset_state.json"


def _tail_lines(path: Path, line_count: int = 20) -> list[str]:
    if not path.exists():
        return []
    lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
    return lines[-line_count:]


def _pid_running(pid: int) -> bool:
    try:
        os.kill(pid, 0)
        return True
    except ProcessLookupError:
        return False
    except PermissionError:
        return True


def reset_job_status() -> dict[str, Any]:
    """Return status metadata for the most recently launched full realtime reset."""
    state_file = _reset_state_file()
    if not state_file.exists():
        return {"status": "not_started"}

    payload = json.loads(state_file.read_text(encoding="utf-8"))
    pid = int(payload.get("pid", 0) or 0)
    log_path = Path(str(payload.get("log_path", "")))
    running = pid > 0 and _pid_running(pid)
    return {
        "status": "running" if running else "finished",
        "pid": pid,
        "run_id": payload.get("run_id"),
        "script": payload.get("script"),
        "started_at": payload.get("started_at"),
        "log_path": str(log_path) if log_path else None,
        "last_log_lines": _tail_lines(log_path, line_count=20) if log_path else [],
    }


def trigger_full_realtime_reset(force: bool = False) -> dict[str, Any]:
    """
    Launch the full realtime reset shell script as a background process.

    This endpoint is designed for operator-triggered restarts from the dashboard.
    """
    current = reset_job_status()
    if current.get("status") == "running" and not force:
        return {
            "status": "already_running",
            "message": "A reset job is already running. Set force=true to start another run.",
            **current,
        }

    settings = get_settings()
    script_path = Path(settings.pipeline_reset_script)
    if not script_path.exists():
        return {
            "status": "script_missing",
            "script": str(script_path),
            "message": "Reset script path does not exist on this runtime host.",
        }

    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "-" + uuid.uuid4().hex[:8]
    log_dir = Path(settings.pipeline_reset_log_dir)
    log_dir.mkdir(parents=True, exist_ok=True)
    log_path = log_dir / f"full_realtime_reset_{run_id}.log"

    with log_path.open("a", encoding="utf-8") as log_file:
        process = subprocess.Popen(
            ["bash", str(script_path)],
            stdout=log_file,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )

    state_file = _reset_state_file()
    state_file.write_text(
        json.dumps(
            {
                "pid": process.pid,
                "run_id": run_id,
                "script": str(script_path),
                "log_path": str(log_path),
                "started_at": datetime.now(timezone.utc).isoformat(),
                "force": force,
            },
            ensure_ascii=True,
        ),
        encoding="utf-8",
    )

    return {
        "status": "started",
        "pid": process.pid,
        "run_id": run_id,
        "script": str(script_path),
        "log_path": str(log_path),
    }


def _path_status(path_value: str | None) -> dict[str, Any]:
    if not path_value:
        return {"path": None, "status": "unavailable", "last_modified": None}
    if path_value.startswith("gs://"):
        return _gcs_path_status(path_value)

    local_path = path_value.replace("file://", "", 1)
    path = Path(local_path)
    if not path.exists():
        return {"path": path_value, "status": "missing", "last_modified": None}

    candidates = [path]
    if path.is_dir():
        candidates = [item for item in path.rglob("*") if item.is_file()]
    latest_mtime = max((item.stat().st_mtime for item in candidates), default=None)
    last_modified = (
        datetime.fromtimestamp(latest_mtime, tz=timezone.utc).isoformat()
        if latest_mtime
        else None
    )
    return {
        "path": path_value,
        "status": "ok",
        "last_modified": last_modified,
        "file_count": len(candidates) if path.is_dir() else 1,
    }


def checkpoints() -> dict[str, Any]:
    """Return configured checkpoint and Gold output paths with best-effort timestamps."""
    settings = get_settings()
    primary_flink_path = (
        settings.flink_local_checkpoint_dir or settings.flink_checkpoint_dir
    )
    return {
        "flink": _path_status(primary_flink_path),
        "flink_remote": _path_status(settings.flink_checkpoint_dir),
        "gold": _path_status(settings.gold_retrain_path),
        "environment": settings.environment,
        "cwd": os.getcwd(),
    }


def gold_last_update() -> str | None:
    """Return the best-effort Gold dataset last modified timestamp."""
    return _path_status(get_settings().gold_retrain_path).get("last_modified")


def _gcs_path_status(path_value: str) -> dict[str, Any]:
    """Return best-effort freshness metadata for a GCS prefix."""
    try:
        from google.cloud import storage
    except Exception as exc:
        return {
            "path": path_value,
            "status": "configured_remote",
            "last_modified": None,
            "note": f"GCS client unavailable: {exc}",
        }

    match = re.fullmatch(r"gs://([^/]+)(?:/(.*))?", path_value.rstrip("/"))
    if not match:
        return {
            "path": path_value,
            "status": "invalid",
            "last_modified": None,
            "note": "Invalid GCS path format.",
        }

    bucket_name, prefix = match.group(1), (match.group(2) or "").rstrip("/")
    prefix = f"{prefix}/" if prefix else ""

    try:
        client = storage.Client()
        blobs = list(
            client.list_blobs(bucket_name, prefix=prefix, max_results=2000)  # type: ignore[arg-type]
        )
    except Exception as exc:
        return {
            "path": path_value,
            "status": "configured_remote",
            "last_modified": None,
            "note": f"GCS lookup failed: {exc}",
        }

    if not blobs:
        return {
            "path": path_value,
            "status": "empty",
            "last_modified": None,
            "file_count": 0,
        }

    latest_blob = max(
        (blob for blob in blobs if blob.updated is not None),
        key=lambda blob: blob.updated,  # type: ignore[arg-type]
        default=None,
    )
    result = {
        "path": path_value,
        "status": "ok",
        "last_modified": latest_blob.updated.isoformat() if latest_blob else None,
        "file_count": len(blobs),
        "sample_blob": blobs[0].name,
    }
    if len(blobs) == 2000:
        result["note"] = "Freshness is based on the first 2000 blobs under the prefix."
    return result
