import { DashboardClient } from "@/components/DashboardClient";
import type { Hotspot, OverviewSummary, PredictionPoint } from "@/lib/types";

export const dynamic = "force-dynamic";
export const fetchCache = "force-no-store";

const API_BASE = "http://35.224.149.110:8000";

async function loadJson<T>(path: string) {
  const response = await fetch(`${API_BASE}${path}`, {
    cache: "no-store",
    next: { revalidate: 0 }
  });
  if (!response.ok) {
    return undefined;
  }
  return response.json() as Promise<T>;
}

export default async function DashboardPage() {
  const [
    overview,
    points,
    latest,
    hotspots,
    replayHealth,
    riskByHour,
    severity,
    weather
  ] = await Promise.all([
    loadJson<OverviewSummary>("/api/v1/overview/summary?mode=replay"),
    loadJson<{ points?: PredictionPoint[] }>("/api/v1/predictions/map?limit=1500&min_risk=0&mode=replay"),
    loadJson<{ predictions?: PredictionPoint[] }>("/api/v1/predictions/latest?limit=30&mode=replay"),
    loadJson<{ hotspots?: Hotspot[] }>("/api/v1/hotspots?limit=10&min_events=1&mode=replay"),
    loadJson<Record<string, unknown>>("/api/v1/pipeline/replay-health"),
    loadJson<{ data?: Array<Record<string, number>> }>("/api/v1/analytics/risk-by-hour?mode=replay"),
    loadJson<{ distribution?: Array<Record<string, number>> }>("/api/v1/analytics/severity-distribution?mode=replay"),
    loadJson<{ histogram?: Record<string, Array<{ bin: string; count: number }>> }>("/api/v1/analytics/weather-histogram?mode=replay")
  ]);

  return (
    <DashboardClient
      bootstrap={{
        overview,
        points,
        latest,
        hotspots,
        replayHealth,
        riskByHour,
        severity,
        weather
      }}
    />
  );
}
