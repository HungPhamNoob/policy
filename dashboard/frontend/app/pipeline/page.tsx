"use client";

import { useQuery } from "@tanstack/react-query";
import {
  Bar,
  BarChart,
  CartesianGrid,
  Line,
  LineChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis
} from "recharts";
import { Activity, Database, GitBranch, RadioTower, ServerCog } from "lucide-react";
import { api } from "@/lib/api";
import { formatVietnamTimestamp, formatVietnamTimestampLabel } from "@/lib/time";
import { KpiCard } from "@/components/DataState";

type AnyRecord = Record<string, any>;

const SERVICE_URLS: Record<string, string> = {
  kafka: "",
  flink: "http://35.225.231.57:8081",
  spark: "http://34.63.78.147:8080",
  postgres: "",
  mlflow: "http://35.224.149.110:5000",
  airflow: "http://35.224.149.110:8080",
  fastapi: "http://35.224.149.110:8000/docs",
  grafana: "http://35.224.149.110:3000"
};

function statusText(data: unknown) {
  if (!data || typeof data !== "object") return "unavailable";
  return String((data as AnyRecord).status || "configured");
}

function formatPercent(value: unknown) {
  if (typeof value !== "number" || Number.isNaN(value)) {
    return "N/A";
  }
  return `${(value * 100).toFixed(2)}%`;
}

export default function PipelinePage() {
  const health = useQuery({ queryKey: ["health"], queryFn: api.health });
  const system = useQuery({
    queryKey: ["system"],
    queryFn: api.systemStatus,
    refetchInterval: 30_000
  });
  const model = useQuery({ queryKey: ["model"], queryFn: api.modelInfo });
  const throughput = useQuery({
    queryKey: ["throughput"],
    queryFn: () => api.throughput("5m"),
    refetchInterval: 10_000
  });
  const latency = useQuery({
    queryKey: ["latency", "5m"],
    queryFn: () => api.latency("p95", "5m"),
    refetchInterval: 10_000
  });
  const checkpoints = useQuery({
    queryKey: ["checkpoints"],
    queryFn: api.checkpoints,
    refetchInterval: 60_000
  });
  const replay = useQuery({
    queryKey: ["replay"],
    queryFn: api.replayHealth,
    refetchInterval: 10_000
  });
  const retrain = useQuery({
    queryKey: ["retrain"],
    queryFn: () => api.retrainHistory(30),
    refetchInterval: 60_000
  });
  const trend = useQuery({
    queryKey: ["performance"],
    queryFn: api.performanceTrend,
    refetchInterval: 60_000
  });

  const throughputData = throughput.data as AnyRecord | undefined;
  const latencyData = latency.data as AnyRecord | undefined;
  const systemData = system.data as AnyRecord | undefined;
  const modelData = model.data as AnyRecord | undefined;
  const checkpointData = checkpoints.data as AnyRecord | undefined;
  const replayData = replay.data as AnyRecord | undefined;
  const retrainData = retrain.data as AnyRecord | undefined;
  const trendData = trend.data as AnyRecord | undefined;

  const latencyChart = latencyData?.latency_ms
    ? Object.entries(latencyData.latency_ms).map(([metric, value]) => ({
        metric,
        value
      }))
    : [];
  const trendSeries = (trendData?.series as AnyRecord[] | undefined) || [];
  const replaySources = (replayData?.sources as AnyRecord[] | undefined) || [];
  const latestRetrainRun = ((retrainData?.runs as AnyRecord[] | undefined) || [])[0];

  const serviceRows = [
    [
      "Kafka",
      "kafka",
      systemData?.kafka?.status,
      [systemData?.kafka?.us_topic, systemData?.kafka?.tomtom_topic]
        .filter(Boolean)
        .join(" | ")
    ],
    ["Flink", "flink", systemData?.flink?.status, systemData?.flink?.checkpoint_dir],
    ["Spark", "spark", "configured", systemData?.spark?.gold_path],
    [
      "Postgres",
      "postgres",
      "configured",
      [systemData?.postgres?.us_prediction_table, systemData?.postgres?.tomtom_events_table]
        .filter(Boolean)
        .join(" | ")
    ],
    ["MLflow", "mlflow", "configured", systemData?.mlflow?.serving_endpoint],
    [
      "Airflow",
      "airflow",
      "configured",
      `retrain: ${systemData?.airflow?.model_retrain_schedule || "n/a"} | stream: ${systemData?.airflow?.stream_health_schedule || "n/a"}`
    ],
    ["FastAPI", "fastapi", "configured", "REST API & docs"],
    ["Grafana", "grafana", "configured", "Monitoring dashboards"]
  ];
  const throughputDetail =
    throughputData?.status === "stale" && throughputData?.window_anchor
      ? formatVietnamTimestampLabel("Latest active window ended", throughputData.window_anchor)
      : `${throughputData?.event_count || 0} events / ${throughputData?.window || "5m"}`;
  const flowState =
    throughputData?.status === "ok"
      ? "Flowing"
      : throughputData?.status === "stale"
        ? "Stalled"
        : throughputData?.event_count
          ? "Draining"
          : "Blocked";
  const flowDetail =
    (throughputData?.sources as AnyRecord[] | undefined)
      ?.map((source) => `${String(source.table)}: ${Number(source.event_count || 0).toLocaleString()}`)
      .join(" | ") || "No recent source activity";
  const latencyDetail =
    latencyData?.status === "stale" && latencyData?.window_anchor
      ? formatVietnamTimestampLabel("Latest active window ended", latencyData.window_anchor)
      : statusText(latencyData);
  const avgLatencyDetail =
    latencyData?.status === "stale" && latencyData?.window_anchor
      ? formatVietnamTimestampLabel("Latest active window ended", latencyData.window_anchor)
      : `Recent window: ${String(latencyData?.window || "5m")}`;
  const retrainState = String(latestRetrainRun?.status || "unavailable");
  const retrainDetail = latestRetrainRun?.start_time
    ? formatVietnamTimestampLabel("Last run", latestRetrainRun.start_time)
    : "No retrain run metadata yet";

  return (
    <div className="page-stack">
      <div className="page-title">
        <div>
          <h1>Pipeline Health</h1>
          <p>Operational view for Kafka, Flink, Spark, PostGIS, MLflow and retrain runs.</p>
        </div>
        <span className="status-pill">
          <Activity size={14} />
          {health.data?.status || "unavailable"}
        </span>
      </div>

      <section className="grid kpi-grid">
        <KpiCard
          label="Throughput"
          value={`${Number(throughputData?.events_per_second || 0).toFixed(2)} msg/s`}
          detail={throughputDetail}
        />
        <KpiCard
          label="P95 latency"
          value={
            latencyData?.value_ms === null || latencyData?.value_ms === undefined
              ? "N/A"
              : `${Number(latencyData.value_ms).toFixed(1)}ms`
          }
          detail={latencyDetail}
        />
        <KpiCard
          label="Avg latency"
          value={
            latencyData?.latency_ms?.avg === null || latencyData?.latency_ms?.avg === undefined
              ? "N/A"
              : `${Number(latencyData.latency_ms.avg).toFixed(1)}ms`
          }
          detail={avgLatencyDetail}
        />
        <KpiCard
          label="Prediction rows"
          value={Number(replayData?.row_count || 0).toLocaleString()}
          detail={statusText(replayData)}
        />
        <KpiCard
          label="Model"
          value={modelData?.model_version || "latest"}
          detail={modelData?.model_name || "traffic-risk-model"}
        />
        <KpiCard
          label="Stream flow"
          value={flowState}
          detail={flowDetail}
        />
        <KpiCard
          label="Retrain loop"
          value={retrainState}
          detail={retrainDetail}
        />
      </section>

      <section className="grid pipeline-grid">
        <div className="card">
          <div className="card-header">
            <h2 className="card-title">System topology</h2>
            <ServerCog size={18} />
          </div>
          <div className="side-list">
            {serviceRows.map(([name, key, status, detail]) => {
              const url = SERVICE_URLS[key];
              const content = (
                <>
                  <div className="row-top">
                    <strong>{name}</strong>
                    <span className="status-pill">{status || "unavailable"}</span>
                  </div>
                  <span className="muted">{detail || "No metadata"}</span>
                </>
              );
              if (url) {
                return (
                  <a
                    className="row-item"
                    href={url}
                    key={String(name)}
                    rel="noreferrer"
                    target="_blank"
                    style={{ textDecoration: "none", color: "inherit", display: "block" }}
                  >
                    {content}
                  </a>
                );
              }
              return (
                <div className="row-item" key={String(name)}>
                  {content}
                </div>
              );
            })}
          </div>
        </div>

        <div className="card">
          <div className="card-header">
            <h2 className="card-title">Source freshness</h2>
            <Database size={18} />
          </div>
          <div className="side-list">
            {replaySources.length
              ? replaySources.map((item) => (
                  <div className="row-item" key={String(item.table)}>
                    <div className="row-top">
                      <strong>{String(item.table)}</strong>
                      <span className="status-pill">{String(item.status || "unavailable")}</span>
                    </div>
                    <span className="muted">
                      rows: {Number(item.row_count || 0).toLocaleString()} | latest event: {formatVietnamTimestamp(item.latest_event_time, "n/a")}
                    </span>
                    <span className="muted">
                      latest insert: {formatVietnamTimestamp(item.latest_created_at, "n/a")}
                    </span>
                  </div>
                ))
              : ["flink", "gold"].map((key) => {
                  const item = checkpointData?.[key] || {};
                  return (
                    <div className="row-item" key={key}>
                      <div className="row-top">
                        <strong>{key.toUpperCase()}</strong>
                        <span className="status-pill">{item.status || "unavailable"}</span>
                      </div>
                      <span className="muted">{item.path || "Not configured"}</span>
                      <span className="muted">{item.last_modified || item.note || ""}</span>
                    </div>
                  );
                })}
          </div>
        </div>
      </section>

      <section className="grid pipeline-grid">
        <div className="card">
          <div className="card-header">
            <h2 className="card-title">Latency distribution</h2>
            <RadioTower size={18} />
          </div>
          <div className="chart-box">
            <ResponsiveContainer>
              <BarChart data={latencyChart}>
                <CartesianGrid stroke="rgba(148,163,184,0.14)" />
                <XAxis dataKey="metric" stroke="#94a3b8" />
                <YAxis stroke="#94a3b8" />
                <Tooltip />
                <Bar dataKey="value" fill="#38bdf8" radius={[4, 4, 0, 0]} />
              </BarChart>
            </ResponsiveContainer>
          </div>
        </div>

        <div className="card">
          <div className="card-header">
            <h2 className="card-title">Model performance trend</h2>
            <GitBranch size={18} />
          </div>
          <div className="chart-box">
            <ResponsiveContainer>
              <LineChart data={trendSeries}>
                <CartesianGrid stroke="rgba(148,163,184,0.14)" />
                <XAxis dataKey="run_name" stroke="#94a3b8" hide />
                <YAxis stroke="#94a3b8" />
                <Tooltip />
                <Line dataKey="accuracy" stroke="#22c55e" strokeWidth={2} />
                <Line dataKey="weighted_f1" stroke="#38bdf8" strokeWidth={2} />
                <Line dataKey="weighted_recall" stroke="#f59e0b" strokeWidth={2} />
                <Line dataKey="weighted_precision" stroke="#ef4444" strokeWidth={2} />
              </LineChart>
            </ResponsiveContainer>
          </div>
        </div>
      </section>

      <section className="card">
        <h2 className="card-title">Retrain history</h2>
        <table className="table">
          <thead>
            <tr>
              <th>Run</th>
              <th>Status</th>
              <th>Start</th>
              <th>Accuracy</th>
              <th>F1</th>
              <th>Recall</th>
              <th>Precision</th>
            </tr>
          </thead>
          <tbody>
            {((retrainData?.runs as AnyRecord[] | undefined) || []).map((run) => (
              <tr key={run.run_id}>
                <td className="mono">{run.run_name || run.run_id}</td>
                <td>{run.status}</td>
                <td>{formatVietnamTimestamp(run.start_time, "-")}</td>
                <td>{formatPercent(run.metrics?.accuracy)}</td>
                <td>{formatPercent(run.metrics?.weighted_f1 ?? run.metrics?.f1)}</td>
                <td>{formatPercent(run.metrics?.weighted_recall ?? run.metrics?.recall)}</td>
                <td>{formatPercent(run.metrics?.weighted_precision ?? run.metrics?.precision)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </section>
    </div>
  );
}
