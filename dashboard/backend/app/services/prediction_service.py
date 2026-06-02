"""Prediction query service backed by PostgreSQL/PostGIS."""

from typing import Any, Literal

from fastapi import HTTPException
from psycopg2 import sql

from app.core.config import get_settings
from app.core.database import fetch_all, fetch_one
from app.core.runtime_cache import cached_result
from app.services.risk_sql import (
    effective_tomtom_risk_score_expr,
    effective_us_risk_score_expr,
)


def risk_level(score: float | None) -> str:
    """Map a risk score to a dashboard label."""
    value = float(score or 0.0)
    if value >= 0.7:
        return "high"
    if value >= 0.4:
        return "medium"
    return "low"


MapMode = Literal["replay", "live", "full"]
OVERVIEW_CACHE_TTL_SECONDS = 20.0
MAP_POINTS_CACHE_TTL_SECONDS = 15.0
LATEST_PREDICTIONS_CACHE_TTL_SECONDS = 15.0
OVERVIEW_RISK_SAMPLE_LIMIT = 5_000


def _public_table_name(value: str) -> str:
    """Return the unqualified table name used by the public schema."""
    return value.split(".")[-1]


def table_identifier(table_name: str | None = None) -> sql.Identifier:
    """Return a safely quoted table identifier."""
    selected_table = table_name or get_settings().prediction_table
    return sql.Identifier(_public_table_name(selected_table))


def us_table_identifier() -> sql.Identifier:
    """Return the US replay prediction table identifier."""
    settings = get_settings()
    return table_identifier(settings.us_prediction_table or settings.prediction_table)


def tomtom_table_identifier() -> sql.Identifier:
    """Return the TomTom live incident table identifier."""
    return table_identifier(get_settings().tomtom_events_table)


def _table_exists(table_name: str) -> bool:
    query = """
        SELECT EXISTS (
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema = 'public' AND table_name = %(table_name)s
        ) AS exists
    """
    row = fetch_one(query, {"table_name": _public_table_name(table_name)})
    return bool(row and row.get("exists"))


def _table_row_estimate(table_name: str) -> int:
    row = fetch_one(
        """
        SELECT COALESCE(c.reltuples, 0)::BIGINT AS row_estimate
        FROM pg_class AS c
        JOIN pg_namespace AS n ON n.oid = c.relnamespace
        WHERE n.nspname = 'public' AND c.relname = %(table_name)s
        """,
        {"table_name": _public_table_name(table_name)},
    )
    return max(0, int(row.get("row_estimate") or 0)) if row else 0


def _load_overview_source(
    *,
    table_name: str,
    table: sql.Identifier,
    risk_score: sql.Composable,
    prefer_exact_total: bool = False,
) -> dict[str, Any]:
    """Return fast source metrics without full-table scans on large replay tables."""
    total_estimate = _table_row_estimate(table_name)
    if prefer_exact_total or total_estimate <= OVERVIEW_RISK_SAMPLE_LIMIT:
        total_row = fetch_one(
            sql.SQL("SELECT COUNT(*)::BIGINT AS total_events FROM {table}").format(
                table=table
            )
        )
        total_events = int(total_row.get("total_events") or 0) if total_row else 0
    else:
        total_events = total_estimate

    sample_row = fetch_one(
        sql.SQL(
            """
            WITH recent AS (
                SELECT
                    event_time,
                    {risk_score} AS risk_score
                FROM {table}
                WHERE event_time IS NOT NULL
                ORDER BY event_time DESC NULLS LAST
                LIMIT %(sample_limit)s
            )
            SELECT
                COUNT(*)::BIGINT AS sample_events,
                COALESCE(SUM(CASE WHEN risk_score >= 0.7 THEN 1 ELSE 0 END), 0)::BIGINT AS high_risk_events,
                COALESCE(SUM(risk_score), 0)::DOUBLE PRECISION AS risk_score_sum,
                MAX(event_time) AS latest_event_time
            FROM recent
            WHERE risk_score IS NOT NULL
            """
        ).format(risk_score=risk_score, table=table),
        {"sample_limit": OVERVIEW_RISK_SAMPLE_LIMIT},
    )
    sample_events = int(sample_row.get("sample_events") or 0) if sample_row else 0
    sample_high_risk = (
        int(sample_row.get("high_risk_events") or 0) if sample_row else 0
    )
    sample_risk_sum = (
        float(sample_row.get("risk_score_sum") or 0.0) if sample_row else 0.0
    )
    avg_risk_score = sample_risk_sum / sample_events if sample_events else 0.0
    high_risk_events = (
        int(round((sample_high_risk / sample_events) * total_events))
        if sample_events
        else 0
    )

    return {
        "total_events": total_events,
        "high_risk_events": high_risk_events,
        "avg_risk_score": avg_risk_score,
        "latest_event_time": (
            sample_row.get("latest_event_time") if sample_row else None
        ),
    }


def _normalize_mode(mode: str | None) -> MapMode:
    if mode in {"replay", "live", "full"}:
        return mode  # type: ignore[return-value]
    return "full"


def _limit_full_mode_sources(
    selects: list[sql.Composable],
    normalized_mode: MapMode,
    limit: int,
) -> list[sql.Composable]:
    """Apply the requested limit independently to each source in full mode."""
    if normalized_mode != "full" or len(selects) <= 1:
        return selects
    return [
        sql.SQL(
            """
            (
                SELECT *
                FROM ({source_query}) AS mode_source
                ORDER BY event_time DESC NULLS LAST
                LIMIT {per_source_limit}
            )
            """
        ).format(
            source_query=source_query,
            per_source_limit=sql.Literal(limit),
        )
        for source_query in selects
    ]


def _union_limit(normalized_mode: MapMode, source_count: int, limit: int) -> int:
    """Return the outer result limit after each full-mode source is capped."""
    if normalized_mode == "full" and source_count > 1:
        return limit * source_count
    return limit


def mode_table_identifier(mode: str | None) -> sql.Identifier:
    """Return the appropriate table identifier based on mode."""
    normalized = _normalize_mode(mode)
    if normalized == "replay":
        return us_table_identifier()
    if normalized == "live":
        return tomtom_table_identifier()
    return table_identifier()


def overview_summary(mode: str | None = None) -> dict[str, Any]:
    """Aggregate high-level metrics for the selected dashboard mode."""
    normalized_mode = _normalize_mode(mode)

    def load_summary() -> dict[str, Any]:
        return _load_overview_summary(normalized_mode)

    return cached_result(
        "overview_summary",
        (normalized_mode,),
        OVERVIEW_CACHE_TTL_SECONDS,
        load_summary,
    )


def _load_overview_summary(normalized_mode: MapMode) -> dict[str, Any]:
    """Load high-level metrics for the selected dashboard mode."""
    settings = get_settings()
    source_metrics: list[dict[str, Any]] = []
    us_risk_score = effective_us_risk_score_expr()
    tomtom_risk_score = effective_tomtom_risk_score_expr()

    if normalized_mode in {"replay", "full"} and _table_exists(
        settings.us_prediction_table
    ):
        source_metrics.append(
            _load_overview_source(
                table_name=settings.us_prediction_table,
                table=us_table_identifier(),
                risk_score=us_risk_score,
                prefer_exact_total=True,
            )
        )
    if normalized_mode in {"live", "full"} and _table_exists(
        settings.tomtom_events_table
    ):
        source_metrics.append(
            _load_overview_source(
                table_name=settings.tomtom_events_table,
                table=tomtom_table_identifier(),
                risk_score=tomtom_risk_score,
                prefer_exact_total=True,
            )
        )

    total_events = sum(int(row["total_events"]) for row in source_metrics)
    high_risk_events = sum(int(row["high_risk_events"]) for row in source_metrics)
    avg_risk_score = (
        sum(
            float(row["avg_risk_score"]) * int(row["total_events"])
            for row in source_metrics
        )
        / total_events
        if total_events
        else 0.0
    )
    latest_event_time = max(
        (row["latest_event_time"] for row in source_metrics if row["latest_event_time"]),
        default=None,
    )

    # Fetch latest model performance metrics from MLflow.
    model_metrics = _fetch_latest_model_metrics()

    return {
        "total_events": total_events,
        "high_risk_events": high_risk_events,
        "avg_risk_score": round(avg_risk_score, 4),
        "latest_event_time": (
            latest_event_time.isoformat() if latest_event_time else None
        ),
        "latest_model_version": (
            "TomTom rule-based severity"
            if normalized_mode == "live"
            else (
                "US H2O + TomTom rule-based"
                if normalized_mode == "full"
                else settings.model_version or "latest"
            )
        ),
        "mode": normalized_mode,
        "model_performance": model_metrics,
    }


def _fetch_latest_model_metrics() -> dict[str, Any]:
    """Return the best available run metrics ranked by weighted_f1."""
    try:
        from app.services.model_service import retrain_history

        history = retrain_history(limit=30)
        runs = history.get("runs", [])
        if not runs:
            return {}

        # Find the best model from the most recent successful retrain batch
        # The best model for each batch is logged with rank=1 (top1)
        best_run = next((run for run in runs if "top1_" in run.get("run_name", "")), None)

        if not best_run:
            return {}

        metrics = dict(best_run.get("metrics") or {})
        metrics["selected_run_id"] = best_run.get("run_id")
        metrics["selected_run_name"] = best_run.get("run_name")
        metrics["selection_metric"] = "latest_top1"
        return metrics
    except Exception:
        return {}


def map_points(
    bbox: str | None,
    min_risk: float,
    start_time: str | None,
    end_time: str | None,
    limit: int,
    mode: str | None = None,
) -> dict[str, list[dict[str, Any]]]:
    """Return replay, live, or combined points for map rendering."""
    normalized_mode = _normalize_mode(mode)

    def load_points() -> dict[str, list[dict[str, Any]]]:
        return _load_map_points(
            bbox=bbox,
            min_risk=min_risk,
            start_time=start_time,
            end_time=end_time,
            limit=limit,
            normalized_mode=normalized_mode,
        )

    return cached_result(
        "map_points",
        (bbox, min_risk, start_time, end_time, limit, normalized_mode),
        MAP_POINTS_CACHE_TTL_SECONDS,
        load_points,
    )


def _load_map_points(
    *,
    bbox: str | None,
    min_risk: float,
    start_time: str | None,
    end_time: str | None,
    limit: int,
    normalized_mode: MapMode,
) -> dict[str, list[dict[str, Any]]]:
    """Load replay, live, or combined points for map rendering."""
    us_risk_score = effective_us_risk_score_expr()
    tomtom_risk_score = effective_tomtom_risk_score_expr()
    where_clauses = ["{risk_score} >= %(min_risk)s"]
    params: dict[str, Any] = {"min_risk": min_risk, "limit": limit}
    use_postgis_bbox = False

    if bbox:
        parts = [float(value) for value in bbox.split(",")]
        if len(parts) != 4:
            raise HTTPException(
                status_code=400, detail="bbox must be min_lon,min_lat,max_lon,max_lat"
            )
        params.update(
            {
                "min_lon": parts[0],
                "min_lat": parts[1],
                "max_lon": parts[2],
                "max_lat": parts[3],
            }
        )
        use_postgis_bbox = True
        where_clauses.append(
            """
            geom IS NOT NULL
            AND ST_Intersects(
                geom,
                ST_MakeEnvelope(%(min_lon)s, %(min_lat)s, %(max_lon)s, %(max_lat)s, 4326)
            )
            """
        )
    if start_time:
        params["start_time"] = start_time
        where_clauses.append("event_time >= %(start_time)s")
    if end_time:
        params["end_time"] = end_time
        where_clauses.append("event_time <= %(end_time)s")

    us_select = sql.SQL(
        """
        SELECT
            event_id,
            lat,
            lon,
            {risk_score} AS risk_score,
            predicted_severity,
            true_severity,
            event_time,
            model_status,
            'us_replay' AS data_source,
            'circle' AS marker_shape
        FROM {table}
        WHERE {where_clause}
        """
    ).format(
        risk_score=us_risk_score,
        table=us_table_identifier(),
        where_clause=sql.SQL(" AND ").join(
            sql.SQL(clause).format(risk_score=us_risk_score) for clause in where_clauses
        ),
    )
    tomtom_select = sql.SQL(
        """
        SELECT
            event_id,
            lat,
            lon,
            {risk_score} AS risk_score,
            severity AS predicted_severity,
            severity AS true_severity,
            event_time,
            model_status,
            'tomtom_live' AS data_source,
            'triangle' AS marker_shape
        FROM {table}
        WHERE {where_clause}
        """
    ).format(
        risk_score=tomtom_risk_score,
        table=tomtom_table_identifier(),
        where_clause=sql.SQL(" AND ").join(
            sql.SQL(clause).format(risk_score=tomtom_risk_score)
            for clause in where_clauses
        ),
    )

    selects: list[sql.Composable] = []
    settings = get_settings()
    if normalized_mode in {"replay", "full"} and _table_exists(
        settings.us_prediction_table
    ):
        selects.append(us_select)
    if normalized_mode in {"live", "full"} and _table_exists(
        settings.tomtom_events_table
    ):
        selects.append(tomtom_select)
    if not selects:
        return {"points": []}
    union_limit = _union_limit(normalized_mode, len(selects), limit)
    params["union_limit"] = union_limit
    selects = _limit_full_mode_sources(selects, normalized_mode, limit)

    union_query = sql.SQL(" UNION ALL ").join(selects)
    query = sql.SQL(
        """
        SELECT *
        FROM ({union_query}) AS map_points
        ORDER BY event_time DESC NULLS LAST
        LIMIT %(union_limit)s
        """
    ).format(union_query=union_query)
    try:
        rows = fetch_all(query, params)
    except Exception:
        if not use_postgis_bbox:
            raise
        fallback_clauses = [
            clause
            for clause in where_clauses
            if "ST_Intersects" not in clause and "geom IS NOT NULL" not in clause
        ]
        fallback_clauses.append("lon BETWEEN %(min_lon)s AND %(max_lon)s")
        fallback_clauses.append("lat BETWEEN %(min_lat)s AND %(max_lat)s")
        fallback_query = sql.SQL(
            """
            SELECT *
            FROM ({union_query}) AS map_points
            WHERE lon BETWEEN %(min_lon)s AND %(max_lon)s
              AND lat BETWEEN %(min_lat)s AND %(max_lat)s
            ORDER BY event_time DESC NULLS LAST
            LIMIT %(union_limit)s
            """
        ).format(
            union_query=sql.SQL(" UNION ALL ").join(selects),
        )
        rows = fetch_all(fallback_query, params)
    for row in rows:
        if row.get("event_time"):
            row["event_time"] = row["event_time"].isoformat()
        row["risk_level"] = risk_level(row.get("risk_score"))
        row["model_status"] = row.get("model_status") or "unknown"
        row["data_source"] = row.get("data_source") or "us_replay"
        row["marker_shape"] = row.get("marker_shape") or "circle"
    return {"points": rows}


def prediction_detail(event_id: str) -> dict[str, Any]:
    """Return the stored feature and prediction data for one event."""
    settings = get_settings()
    selects: list[sql.Composable] = []
    if _table_exists(settings.us_prediction_table):
        selects.append(
            sql.SQL(
                """
                SELECT
                    event_id,
                    event_time,
                    lat,
                    lon,
                    {risk_score} AS risk_score,
                    predicted_severity,
                    true_severity,
                    model_status,
                    created_at,
                    'us_replay' AS data_source,
                    'circle' AS marker_shape
                FROM {table}
                WHERE event_id = %(event_id)s
                """
            ).format(
                risk_score=effective_us_risk_score_expr(),
                table=us_table_identifier(),
            )
        )
    if _table_exists(settings.tomtom_events_table):
        selects.append(
            sql.SQL(
                """
                SELECT
                    event_id,
                    event_time,
                    lat,
                    lon,
                    {risk_score} AS risk_score,
                    severity AS predicted_severity,
                    severity AS true_severity,
                    model_status,
                    created_at,
                    'tomtom_live' AS data_source,
                    'triangle' AS marker_shape
                FROM {table}
                WHERE event_id = %(event_id)s
                """
            ).format(
                risk_score=effective_tomtom_risk_score_expr(),
                table=tomtom_table_identifier(),
            )
        )
    if not selects:
        raise HTTPException(status_code=404, detail="Prediction event not found")

    query = sql.SQL(
        """
        SELECT *
        FROM ({union_query}) AS prediction_detail
        LIMIT 1
        """
    ).format(union_query=sql.SQL(" UNION ALL ").join(selects))
    row = fetch_one(query, {"event_id": event_id})
    if not row:
        raise HTTPException(status_code=404, detail="Prediction event not found")
    if row.get("event_time"):
        row["event_time"] = row["event_time"].isoformat()
    if row.get("created_at"):
        row["created_at"] = row["created_at"].isoformat()
    row["risk_level"] = risk_level(row.get("risk_score"))
    row["model_status"] = row.get("model_status") or "unknown"
    row.pop("geom", None)
    return row


def latest_predictions(
    limit: int, mode: str | None = None
) -> dict[str, list[dict[str, Any]]]:
    """Return the most recent replay, live, or combined records."""
    normalized_mode = _normalize_mode(mode)

    def load_latest() -> dict[str, list[dict[str, Any]]]:
        return _load_latest_predictions(limit, normalized_mode)

    return cached_result(
        "latest_predictions",
        (limit, normalized_mode),
        LATEST_PREDICTIONS_CACHE_TTL_SECONDS,
        load_latest,
    )


def _load_latest_predictions(
    limit: int, normalized_mode: MapMode
) -> dict[str, list[dict[str, Any]]]:
    """Load the most recent replay, live, or combined records."""
    selects: list[sql.Composable] = []
    settings = get_settings()
    if normalized_mode in {"replay", "full"} and _table_exists(
        settings.us_prediction_table
    ):
        selects.append(
            sql.SQL(
                """
                SELECT
                    event_id,
                    event_time,
                    lat,
                    lon,
                    {risk_score} AS risk_score,
                    predicted_severity,
                    true_severity,
                    model_status,
                    'us_replay' AS data_source,
                    'circle' AS marker_shape
                FROM {table}
                """
            ).format(
                risk_score=effective_us_risk_score_expr(),
                table=us_table_identifier(),
            )
        )
    if normalized_mode in {"live", "full"} and _table_exists(
        settings.tomtom_events_table
    ):
        selects.append(
            sql.SQL(
                """
                SELECT
                    event_id,
                    event_time,
                    lat,
                    lon,
                    {risk_score} AS risk_score,
                    severity AS predicted_severity,
                    severity AS true_severity,
                    model_status,
                    'tomtom_live' AS data_source,
                    'triangle' AS marker_shape
                FROM {table}
                """
            ).format(
                risk_score=effective_tomtom_risk_score_expr(),
                table=tomtom_table_identifier(),
            )
        )
    if not selects:
        return {"predictions": []}
    union_limit = _union_limit(normalized_mode, len(selects), limit)
    selects = _limit_full_mode_sources(selects, normalized_mode, limit)

    query = sql.SQL(
        """
        SELECT *
        FROM ({union_query}) AS latest_events
        ORDER BY event_time DESC NULLS LAST
        LIMIT %(union_limit)s
        """
    ).format(union_query=sql.SQL(" UNION ALL ").join(selects))
    rows = fetch_all(query, {"limit": limit, "union_limit": union_limit})
    for row in rows:
        if row.get("event_time"):
            row["event_time"] = row["event_time"].isoformat()
        row["risk_level"] = risk_level(row.get("risk_score"))
        row["model_status"] = row.get("model_status") or "unknown"
    return {"predictions": rows}
