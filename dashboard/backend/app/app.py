"""FastAPI entrypoint for the Traffic Risk Assessment dashboard backend."""

import logging
import time
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from prometheus_client import CONTENT_TYPE_LATEST, Counter, Histogram, generate_latest
from starlette.responses import Response

from app.routes import (
    analytics,
    docs_aliases,
    hotspots,
    model,
    overview,
    pipeline,
    predictions,
    scenarios,
    system,
)

REQUEST_COUNT = Counter(
    "traffic_api_requests_total",
    "Total number of HTTP requests handled by the Traffic Risk API.",
    ["method", "path", "status_code"],
)
REQUEST_LATENCY = Histogram(
    "traffic_api_request_latency_seconds",
    "HTTP request latency in seconds for the Traffic Risk API.",
    ["method", "path"],
)
REQUEST_DEBUG_LOG = logging.getLogger("traffic.api.request_debug")
DEBUG_PATH_PREFIXES = (
    "/api/v1/overview/summary",
    "/api/v1/predictions/latest",
    "/api/v1/pipeline/replay-health",
)
API_RESPONSE_VERSION = "api-20260605-0226"


app = FastAPI(
    title="Traffic Risk Assessment API",
    version="1.0.0",
    description="Backend API for realtime traffic risk predictions and pipeline status.",
)

# CORS middleware must be added FIRST so it handles preflight OPTIONS
# before custom http middleware sees the request.
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:3000",
        "http://localhost:3001",
        "http://localhost:8000",
        "http://34.61.176.172:5173",
        "http://35.224.149.110:5173",
        "http://35.224.149.110:3001",
    ],
    allow_origin_regex=r"https?://.*",
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["*"],
    max_age=600,
)


@app.middleware("http")
async def collect_request_metrics(request, call_next):
    """Record request count and latency for Prometheus scraping."""
    start_time = time.perf_counter()
    response = await call_next(request)
    elapsed = time.perf_counter() - start_time
    path = request.url.path
    if path.startswith("/api/v1/"):
        response.headers["Cache-Control"] = "no-store, no-cache, max-age=0, must-revalidate"
        response.headers["Pragma"] = "no-cache"
        response.headers["Expires"] = "0"
        response.headers["X-TrafficRisk-API-Version"] = API_RESPONSE_VERSION
    if any(path.startswith(prefix) for prefix in DEBUG_PATH_PREFIXES):
        REQUEST_DEBUG_LOG.warning(
            "dashboard_request path=%s query=%s status=%s client=%s origin=%s referer=%s ua=%s",
            path,
            request.url.query,
            response.status_code,
            request.client.host if request.client else "unknown",
            request.headers.get("origin", ""),
            request.headers.get("referer", ""),
            request.headers.get("user-agent", ""),
        )
    REQUEST_COUNT.labels(request.method, path, str(response.status_code)).inc()
    REQUEST_LATENCY.labels(request.method, path).observe(elapsed)
    return response


@app.get("/health")
def health() -> dict[str, str]:
    """Return a lightweight process health response for load balancers and CI smoke tests."""
    return {"status": "ok"}


@app.get("/metrics")
def metrics() -> Response:
    """Expose Prometheus metrics for local and cloud Grafana dashboards."""
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


app.include_router(overview.router, prefix="/api/v1/overview", tags=["overview"])
app.include_router(
    predictions.router, prefix="/api/v1/predictions", tags=["predictions"]
)
app.include_router(hotspots.router, prefix="/api/v1/hotspots", tags=["hotspots"])
app.include_router(scenarios.router, prefix="/api/v1/scenarios", tags=["scenarios"])
app.include_router(analytics.router, prefix="/api/v1/analytics", tags=["analytics"])
app.include_router(pipeline.router, prefix="/api/v1/pipeline", tags=["pipeline"])
app.include_router(system.router, prefix="/api/v1/system", tags=["system"])
app.include_router(model.router, prefix="/api/v1/model", tags=["model"])
app.include_router(
    docs_aliases.router, prefix="/api/v1", tags=["docs-compatible-aliases"]
)
