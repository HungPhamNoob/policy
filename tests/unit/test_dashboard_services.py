from datetime import date
from datetime import datetime, timezone
import sys
from pathlib import Path

BACKEND_PATH = Path(__file__).resolve().parents[2] / "dashboard" / "backend"
if str(BACKEND_PATH) not in sys.path:
    sys.path.insert(0, str(BACKEND_PATH))

from psycopg2 import sql  # noqa: E402

from app.services import analytics_service, pipeline_service, prediction_service  # noqa: E402


def test_timeseries_metric_count_is_returned(monkeypatch):
    def fake_fetch_all(query, params=None):
        return [
            {
                "time": date(2024, 1, 1),
                "value": 12.0,
                "avg_risk_score": 0.5,
                "accident_count": 12,
                "high_risk_count": 3,
            }
        ]

    monkeypatch.setattr(analytics_service, "fetch_all", fake_fetch_all)

    result = analytics_service.timeseries("day", "count", None, None)

    assert result["metric"] == "count"
    assert result["series"][0]["time"] == "2024-01-01"
    assert result["series"][0]["value"] == 12.0


def test_analytics_empty_sources_return_empty_payload(monkeypatch):
    monkeypatch.setattr(analytics_service, "_mode_sources", lambda mode=None: [])

    severity = analytics_service.severity_distribution("full")
    risk_by_hour = analytics_service.risk_by_hour("full")
    weather = analytics_service.weather_histogram("full")

    assert severity == {"distribution": []}
    assert risk_by_hour == {"data": []}
    assert weather == {
        "histogram": {"temperature": [], "humidity": [], "wind_speed": []}
    }


def test_severity_distribution_returns_empty_when_query_fails(monkeypatch):
    monkeypatch.setattr(
        analytics_service,
        "_mode_sources",
        lambda mode=None: [
            {
                "table": sql.Identifier("traffic_risk_predictions"),
                "severity_column": sql.Identifier("true_severity"),
            }
        ],
    )
    monkeypatch.setattr(
        analytics_service,
        "fetch_all",
        lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("boom")),
    )

    result = analytics_service.severity_distribution("replay")

    assert result == {"distribution": []}


def test_throughput_handles_missing_prediction_table(monkeypatch):
    monkeypatch.setattr(pipeline_service, "_table_columns", lambda: set())

    result = pipeline_service.throughput("5m")

    assert result["status"] == "unavailable"
    assert result["event_count"] == 0
    assert result["events_per_minute"] == 0.0


def test_latency_handles_missing_latency_columns(monkeypatch):
    monkeypatch.setattr(pipeline_service, "_table_columns", lambda: {"event_id"})

    result = pipeline_service.latency("p95")

    assert result["status"] == "unavailable"
    assert result["columns"] == []


def test_throughput_uses_latest_active_window_when_stream_is_stale(monkeypatch):
    monkeypatch.setattr(
        pipeline_service, "_prediction_table_names", lambda: ["traffic_risk_predictions"]
    )
    monkeypatch.setattr(
        pipeline_service, "_columns_for_table", lambda table_name: {"created_at"}
    )
    monkeypatch.setattr(
        pipeline_service,
        "_latest_table_timestamp",
        lambda table_name, column: datetime(2024, 1, 1, tzinfo=timezone.utc),
    )
    monkeypatch.setattr(
        pipeline_service, "fetch_one", lambda *args, **kwargs: {"event_count": 25}
    )

    result = pipeline_service.throughput("5m")

    assert result["status"] == "stale"
    assert result["event_count"] == 25
    assert result["is_live_window"] is False


def test_time_column_prefers_processed_time_over_created_at():
    result = pipeline_service._time_column(
        {"created_at", "processed_time", "event_time"}
    )

    assert result == "processed_time"


def test_replay_health_returns_exact_replay_count_and_threshold(monkeypatch):
    monkeypatch.setattr(
        pipeline_service,
        "_prediction_table_names",
        lambda: ["traffic_risk_predictions", "traffic_tomtom_incidents"],
    )
    monkeypatch.setattr(
        pipeline_service,
        "_columns_for_table",
        lambda table_name: {"event_time"},
    )
    monkeypatch.setattr(
        pipeline_service,
        "_skip_expensive_time_scan",
        lambda table_name, column: False,
    )
    monkeypatch.setattr(
        pipeline_service,
        "_latest_table_timestamp",
        lambda table_name, column: datetime(2024, 1, 1, tzinfo=timezone.utc),
    )
    monkeypatch.setattr(
        pipeline_service,
        "_table_row_estimate",
        lambda table_name: 300,
    )
    monkeypatch.setattr(
        pipeline_service,
        "_exact_table_row_count",
        lambda table_name: 2_175_600 if table_name == "traffic_risk_predictions" else 8_948,
    )

    settings = pipeline_service.get_settings()
    monkeypatch.setattr(settings, "retrain_min_us_rows", 3_000_000)
    monkeypatch.setattr(
        pipeline_service,
        "_compute_retrain_loop_status",
        lambda source_health, replay_rows, settings: {
            "status": "continue",
            "reason": "test stub",
            "recent_retrain_runs": 0,
            "recent_retrain_finished": 0,
        },
    )

    result = pipeline_service.replay_health()

    assert result["row_count"] == 2_175_600
    assert result["total_row_count"] == 2_184_548
    assert result["retrain_ready"] is False
    assert result["sources"][0]["row_count"] == 2_175_600
    assert result["sources"][1]["row_count"] == 8_948


def test_retrain_loop_counts_finished_root_runs_from_param(monkeypatch):
    pipeline_service._RETRAIN_LOOP_CACHE = {}
    pipeline_service._RETRAIN_LOOP_CACHE_TS = 0.0

    class FakeMlflow:
        @staticmethod
        def set_tracking_uri(uri):
            return None

        @staticmethod
        def get_experiment_by_name(name):
            return type("Experiment", (), {"experiment_id": "1"})()

        @staticmethod
        def search_runs(experiment_ids, max_results, order_by):
            class FakeDataFrame:
                def iterrows(self):
                    rows = [
                        {
                            "tags.mlflow.runName": "h2o_retrain_online",
                            "params.run_type": "retrain_online",
                            "tags.run_role": "retrain_parent",
                            "status": "FINISHED",
                        },
                        {
                            "tags.mlflow.runName": "retrain_top1_xgboost",
                            "tags.mlflow.parentRunId": "abc",
                            "status": "FINISHED",
                        },
                    ]
                    for idx, row in enumerate(rows):
                        yield idx, row

            return FakeDataFrame()

    monkeypatch.setitem(sys.modules, "mlflow", FakeMlflow)

    settings = pipeline_service.get_settings()
    source_health = [
        {
            "table": "traffic_risk_predictions",
            "latest_event_time": datetime.now(timezone.utc).isoformat(),
        },
        {
            "table": "traffic_tomtom_incidents",
            "latest_event_time": datetime.now(timezone.utc).isoformat(),
        },
    ]

    result = pipeline_service._compute_retrain_loop_status(
        source_health,
        replay_rows=100,
        settings=settings,
    )

    assert result["status"] == "continue"
    assert result["recent_retrain_runs"] == 1
    assert result["recent_retrain_finished"] == 1


def test_retrain_history_filters_non_retrain_runs(monkeypatch):
    from app.services import model_service

    class FakeMlflow:
        @staticmethod
        def set_tracking_uri(uri):
            return None

        @staticmethod
        def get_experiment_by_name(name):
            return type("Experiment", (), {"experiment_id": "1"})()

        @staticmethod
        def search_runs(experiment_ids, max_results, order_by):
            class FakeTimestamp:
                def isoformat(self):
                    return "2026-06-04T00:00:00+00:00"

            class FakeDataFrame:
                def __len__(self):
                    return 3

                def iterrows(self):
                    rows = [
                        {
                            "run_id": "parent-1",
                            "tags.mlflow.runName": "h2o_retrain_online",
                            "tags.run_type": "retrain_online",
                            "tags.run_role": "retrain_parent",
                            "status": "FINISHED",
                            "start_time": FakeTimestamp(),
                            "end_time": FakeTimestamp(),
                            "metrics.accuracy": 0.9,
                        },
                        {
                            "run_id": "child-1",
                            "tags.mlflow.runName": "retrain_top1_xgboost",
                            "status": "FINISHED",
                            "start_time": FakeTimestamp(),
                            "end_time": FakeTimestamp(),
                            "metrics.weighted_f1": 0.8,
                        },
                        {
                            "run_id": "bootstrap-1",
                            "tags.mlflow.runName": "bootstrap_heuristic_serving",
                            "status": "FINISHED",
                            "start_time": FakeTimestamp(),
                            "end_time": FakeTimestamp(),
                        },
                    ]
                    for idx, row in enumerate(rows):
                        yield idx, row

            return FakeDataFrame()

    monkeypatch.setitem(sys.modules, "mlflow", FakeMlflow)

    result = model_service.retrain_history(limit=10)

    assert result["status"] == "ok"
    assert [run["run_id"] for run in result["runs"][:2]] == ["parent-1", "child-1"]
    assert all(run["run_id"] != "bootstrap-1" for run in result["runs"])


def test_full_map_uses_limit_per_source(monkeypatch):
    captured_params = {}

    monkeypatch.setattr(prediction_service, "_table_exists", lambda table_name: True)
    monkeypatch.setattr(
        prediction_service,
        "fetch_all",
        lambda query, params=None: captured_params.update(params or {}) or [],
    )

    result = prediction_service._load_map_points(
        bbox=None,
        min_risk=0.0,
        start_time=None,
        end_time=None,
        limit=5_000,
        normalized_mode="full",
    )

    assert result == {"points": []}
    assert captured_params["limit"] == 5_000
    assert captured_params["union_limit"] == 10_000


def test_full_latest_uses_limit_per_source(monkeypatch):
    captured_params = {}

    monkeypatch.setattr(prediction_service, "_table_exists", lambda table_name: True)
    monkeypatch.setattr(
        prediction_service,
        "fetch_all",
        lambda query, params=None: captured_params.update(params or {}) or [],
    )

    result = prediction_service._load_latest_predictions(100, "full")

    assert result == {"predictions": []}
    assert captured_params["limit"] == 100
    assert captured_params["union_limit"] == 200
