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
    }


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
        if "event_time" in columns and not _skip_expensive_time_scan(
            table_name, "event_time"
        ):
            latest_event_time = _latest_table_timestamp(table_name, "event_time")
        row_estimate = _table_row_estimate(table_name)
        if (
            table_name == settings.us_prediction_table.split(".")[-1]
            or row_estimate <= EXPENSIVE_SCAN_ROW_THRESHOLD
        ):
            row_count = _exact_table_row_count(table_name)
        else:
            row_count = row_estimate
        total_rows += row_count
        if table_name == settings.us_prediction_table.split(".")[-1]:
            replay_rows = row_count

        source_health.append(
            {
                "table": table_name,
                "status": "ok" if row_count else "not_enough_data",
                "row_count": row_count,
                "row_estimate": row_estimate,
                "latest_event_time": (
                    latest_event_time.isoformat() if latest_event_time else None
                ),
                "latest_created_at": None,
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
        }

    return {
        "status": "ok" if total_rows else "not_enough_data",
        "row_count": replay_rows,
        "total_row_count": total_rows,
        "retrain_min_us_rows": settings.retrain_min_us_rows,
        "retrain_ready": replay_rows >= settings.retrain_min_us_rows,
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
