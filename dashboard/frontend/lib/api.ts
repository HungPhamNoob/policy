import type { MapMode, ScenarioInput } from "./types";

export const API_BASE_URL =
  process.env.NEXT_PUBLIC_API_BASE_URL ||
  process.env.VITE_API_BASE_URL ||
  "";

type QueryValue = string | number | boolean | null | undefined;

function buildUrl(path: string, params?: Record<string, QueryValue>) {
  const baseUrl =
    API_BASE_URL ||
    (typeof window !== "undefined"
      ? (window.location.hostname === "localhost" ||
          window.location.hostname === "127.0.0.1"
          ? "/api-proxy"
          : `${window.location.protocol}//${window.location.hostname}:8000`)
      : "http://localhost:3001/api-proxy");
  const resolvedBaseUrl = baseUrl.startsWith("http")
    ? baseUrl
    : typeof window !== "undefined"
      ? `${window.location.origin}${baseUrl}`
      : `http://localhost:3001${baseUrl}`;
  const normalizedBaseUrl = resolvedBaseUrl.endsWith("/")
    ? resolvedBaseUrl
    : `${resolvedBaseUrl}/`;
  const normalizedPath = path.replace(/^\/+/, "");
  const url = new URL(normalizedPath, normalizedBaseUrl);
  Object.entries(params || {}).forEach(([key, value]) => {
    if (value !== undefined && value !== null && value !== "") {
      url.searchParams.set(key, String(value));
    }
  });
  return url.toString();
}

async function request<T>(
  path: string,
  options?: RequestInit & { params?: Record<string, QueryValue> }
): Promise<T> {
  const response = await fetch(buildUrl(path, options?.params), {
    ...options,
    headers: {
      "Content-Type": "application/json",
      ...(options?.headers || {})
    }
  });
  if (!response.ok) {
    throw new Error(`${response.status} ${response.statusText}`);
  }
  return response.json() as Promise<T>;
}

export const api = {
  health: () => request<{ status: string }>("/health"),
  overview: (mode: MapMode = "full") =>
    request("/api/v1/overview/summary", { params: { mode } }),
  mapPoints: (params: Record<string, QueryValue>) =>
    request<{ points: unknown[] }>("/api/v1/predictions/map", { params }),
  latest: (limit = 100, mode: MapMode = "full") =>
    request<{ predictions: unknown[] }>("/api/v1/predictions/latest", {
      params: { limit, mode }
    }),
  predictionDetail: (eventId: string) =>
    request(`/api/v1/predictions/${encodeURIComponent(eventId)}`),
  hotspots: (params: Record<string, QueryValue>) =>
    request<{ hotspots: unknown[] }>("/api/v1/hotspots", { params }),
  nearby: (params: Record<string, QueryValue>) =>
    request("/api/v1/hotspots/nearby", { params }),
  riskByHour: (mode: string = "full") =>
    request<{ data: unknown[] }>("/api/v1/analytics/risk-by-hour", { params: { mode } }),
  severityDistribution: (mode: string = "full") =>
    request<{ distribution: unknown[] }>("/api/v1/analytics/severity-distribution", { params: { mode } }),
  weatherHistogram: (mode: string = "full") =>
    request<{ histogram: unknown }>("/api/v1/analytics/weather-histogram", { params: { mode } }),
  timeseries: (params: Record<string, QueryValue>) =>
    request<{ series: unknown[] }>("/api/v1/analytics/timeseries", { params }),
  scenarioPredict: (body: ScenarioInput) =>
    request("/api/v1/scenarios/predict", {
      method: "POST",
      body: JSON.stringify(body)
    }),
  scenarioCompare: (baseline: ScenarioInput, scenario: ScenarioInput) =>
    request("/api/v1/scenarios/compare", {
      method: "POST",
      body: JSON.stringify({ baseline, scenario })
    }),
  systemStatus: () => request("/api/v1/system/status"),
  modelInfo: () => request("/api/v1/model/info"),
  throughput: (window = "5m") =>
    request("/api/v1/pipeline/throughput", { params: { window } }),
  latency: (metric = "p95", window = "5m") =>
    request("/api/v1/pipeline/latency", { params: { metric, window } }),
  checkpoints: () => request("/api/v1/pipeline/checkpoints"),
  replayHealth: () => request("/api/v1/pipeline/replay-health"),
  fullRealtimeReset: (force = false) =>
    request("/api/v1/pipeline/full-realtime-reset", {
      method: "POST",
      params: { force }
    }),
  fullRealtimeResetStatus: () =>
    request("/api/v1/pipeline/full-realtime-reset"),
  retrainHistory: (limit = 30) =>
    request("/api/v1/model/retrain-history", { params: { limit } }),
  performanceTrend: () => request("/api/v1/model/performance-trend")
};
