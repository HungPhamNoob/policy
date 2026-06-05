"use client";

import { useEffect } from "react";

const API_BASE = "http://35.224.149.110:8000";

function formatTs(value: unknown): string {
  if (!value) return "-";
  const text = String(value).trim();
  if (!text) return "-";
  const normalized =
    /(?:Z|[+-]\d{2}:\d{2})$/.test(text) || text.includes("GMT") ? text : `${text}Z`;
  const parsed = new Date(normalized);
  if (Number.isNaN(parsed.getTime())) return "-";
  return new Intl.DateTimeFormat("vi-VN", {
    timeZone: "Asia/Ho_Chi_Minh",
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    hour12: false
  }).format(parsed);
}

function fmtNum(value: unknown): string {
  return Number(value || 0).toLocaleString("en-US");
}

function findCardByLabels(labels: string[]) {
  const cards = Array.from(document.querySelectorAll(".card"));
  return (
    cards.find((card) => {
      const title = card.querySelector(".card-title");
      return title && labels.includes(title.textContent?.trim() || "");
    }) || null
  );
}

function setCard(
  card: Element | null,
  label: string,
  value: string,
  detail?: string
) {
  if (!card) return;
  const title = card.querySelector(".card-title");
  const cardValue = card.querySelector(".card-value");
  const muted = card.querySelector(".muted");
  if (title) title.textContent = label;
  if (cardValue) cardValue.textContent = value;
  if (detail !== undefined) {
    if (muted) {
      muted.textContent = detail;
    } else if (detail) {
      const div = document.createElement("div");
      div.className = "muted";
      div.textContent = detail;
      card.appendChild(div);
    }
  }
}

function patchPredictionsTable(predictions: any[]) {
  const section = Array.from(document.querySelectorAll(".card")).find((card) => {
    const title = card.querySelector(".card-title");
    return title && title.textContent?.trim() === "Latest predictions";
  });
  if (!section) return;
  const tbody = section.querySelector("tbody");
  if (!tbody) return;
  tbody.innerHTML = "";
  for (const point of predictions.slice(0, 6)) {
    const tr = document.createElement("tr");
    const status =
      point.model_status === "success" || point.model_status === "failed"
        ? point.model_status
        : point.model_status
          ? "success"
          : "failed";
    const eventId = String(point.event_id || "").split("@")[0] || "-";
    tr.innerHTML = [
      '<td class="mono"></td>',
      "<td></td>",
      "<td></td>",
      "<td></td>",
      "<td></td>"
    ].join("");
    tr.children[0].textContent = eventId;
    tr.children[1].textContent = Number(point.risk_score || 0).toFixed(4);
    tr.children[2].textContent = String(
      point.predicted_severity ?? point.true_severity ?? "-"
    );
    tr.children[3].textContent = formatTs(point.processed_time || point.event_time);
    tr.children[4].textContent = status;
    tbody.appendChild(tr);
  }
}

function patchPipelineSourceFreshness(replayHealth: any) {
  const cards = Array.from(document.querySelectorAll(".card"));
  const sourceCard = cards.find((card) => {
    const title = card.querySelector(".card-title");
    return title && title.textContent?.trim() === "Source freshness";
  });
  if (!sourceCard) return;
  const predictionSource = (replayHealth.sources || []).find(
    (item: any) => String(item.table) === "traffic_risk_predictions"
  );
  if (!predictionSource) return;
  const rows = Array.from(sourceCard.querySelectorAll(".row-item"));
  const target = rows.find((row) => {
    const strong = row.querySelector("strong");
    return strong && strong.textContent?.trim() === "traffic_risk_predictions";
  });
  if (!target) return;
  const muted = target.querySelectorAll(".muted");
  if (muted[0]) {
    muted[0].textContent =
      `processed replay rows: ${fmtNum(predictionSource.row_count)}` +
      ` | stored rows: ${fmtNum(predictionSource.stored_row_count || predictionSource.row_count)}` +
      ` | latest processed: ${formatTs(predictionSource.latest_created_at || predictionSource.latest_event_time)}`;
  }
  if (muted[1]) {
    muted[1].textContent = `latest insert: ${formatTs(predictionSource.latest_created_at || predictionSource.latest_event_time)}`;
  }
}

async function patchDom() {
  const path = window.location.pathname;
  const [summaryRes, latestRes, replayRes] = await Promise.all([
    fetch(`${API_BASE}/api/v1/overview/summary?mode=replay`, { cache: "no-store" }),
    fetch(`${API_BASE}/api/v1/predictions/latest?limit=6&mode=replay`, {
      cache: "no-store"
    }),
    fetch(`${API_BASE}/api/v1/pipeline/replay-health`, { cache: "no-store" })
  ]);
  if (!summaryRes.ok || !latestRes.ok || !replayRes.ok) return;
  const summary = await summaryRes.json();
  const latest = await latestRes.json();
  const replayHealth = await replayRes.json();
  const predictionSource =
    (replayHealth.sources || []).find(
      (item: any) => String(item.table) === "traffic_risk_predictions"
    ) || {};

  if (path === "/") {
    setCard(
      findCardByLabels(["Total events", "Replay progress"]),
      "Replay progress",
      fmtNum(summary.replay_progress_events ?? summary.total_events),
      `processed replay rows: ${fmtNum(summary.replay_progress_events ?? summary.total_events)}` +
        ` | stored rows: ${fmtNum(predictionSource.stored_row_count || replayHealth.total_row_count || summary.total_events)}`
    );
    setCard(
      findCardByLabels(["Latest event"]),
      "Latest event",
      summary.latest_event_time ? "Online" : "No data",
      summary.latest_event_time ? formatTs(summary.latest_event_time) : "Waiting for replay"
    );
    patchPredictionsTable(latest.predictions || []);
  }

  if (path.startsWith("/pipeline")) {
    setCard(
      findCardByLabels(["Prediction rows", "Replay progress"]),
      "Replay progress",
      fmtNum(replayHealth.row_count),
      `processed replay rows: ${fmtNum(replayHealth.row_count)}` +
        ` | stored rows: ${fmtNum(predictionSource.stored_row_count || replayHealth.row_count)}` +
        ` | latest insert: ${formatTs(predictionSource.latest_created_at)}`
    );
    setCard(
      findCardByLabels(["Retrain loop"]),
      "Retrain loop",
      String((replayHealth.retrain_loop || {}).status || "unavailable").toUpperCase(),
      String((replayHealth.retrain_loop || {}).reason || "")
    );
    patchPipelineSourceFreshness(replayHealth);
  }
}

export function CompatibilityPatch() {
  useEffect(() => {
    let cancelled = false;

    const run = async () => {
      try {
        if (!cancelled) {
          await patchDom();
        }
      } catch (error) {
        console.warn("TrafficRisk client compatibility patch skipped", error);
      }
    };

    run();
    const timer = window.setInterval(run, 15_000);
    return () => {
      cancelled = true;
      window.clearInterval(timer);
    };
  }, []);

  return null;
}
