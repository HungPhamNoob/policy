import type { Metadata } from "next";
import Link from "next/link";
import Script from "next/script";
import { Activity, BarChart3, GitBranch, Map, ShieldAlert } from "lucide-react";
import { CompatibilityPatch } from "@/components/CompatibilityPatch";
import "./globals.css";
import { Providers } from "./providers";

export const metadata: Metadata = {
  title: "TrafficRisk AI",
  description: "Realtime road accident risk dashboard"
};

const EARLY_COMPATIBILITY_SCRIPT = `
(() => {
  const API_BASE = "http://35.224.149.110:8000";
  const VERSION = "ui-20260605-0258";
  const RELOAD_KEY = "trafficrisk_ui_reload_once";

  function fmtNum(value) {
    const num = Number(value || 0);
    return Number.isFinite(num) ? num.toLocaleString("en-US") : "0";
  }

  function fmtTs(value) {
    if (!value) return "Waiting for replay";
    const text = String(value).trim();
    if (!text) return "Waiting for replay";
    const normalized =
      /(?:Z|[+-]\\d{2}:\\d{2})$/.test(text) || text.includes("GMT") ? text : text + "Z";
    const parsed = new Date(normalized);
    if (Number.isNaN(parsed.getTime())) return "Waiting for replay";
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

  function cards() {
    return Array.from(document.querySelectorAll(".card"));
  }

  function findCard(labels) {
    return cards().find((card) => {
      const title = card.querySelector(".card-title");
      const text = title && title.textContent ? title.textContent.trim() : "";
      return labels.includes(text);
    }) || null;
  }

  function setCard(card, label, value, detail) {
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

  function patchPredictionsTable(predictions) {
    const section = cards().find((card) => {
      const title = card.querySelector(".card-title");
      return title && title.textContent && title.textContent.trim() === "Latest predictions";
    });
    if (!section) return;
    const tbody = section.querySelector("tbody");
    if (!tbody) return;
    tbody.innerHTML = "";
    (predictions || []).slice(0, 6).forEach((point) => {
      const tr = document.createElement("tr");
      const eventId = String(point.event_id || "").split("@")[0] || "-";
      const status = point.model_status === "failed" ? "failed" : "success";
      tr.innerHTML = '<td class="mono"></td><td></td><td></td><td></td><td></td>';
      tr.children[0].textContent = eventId;
      tr.children[1].textContent = Number(point.risk_score || 0).toFixed(4);
      tr.children[2].textContent = String(point.predicted_severity ?? point.true_severity ?? "-");
      tr.children[3].textContent = fmtTs(point.processed_time || point.event_time);
      tr.children[4].textContent = status;
      tbody.appendChild(tr);
    });
  }

  function patchSourceFreshness(replayHealth) {
    const sourceCard = cards().find((card) => {
      const title = card.querySelector(".card-title");
      return title && title.textContent && title.textContent.trim() === "Source freshness";
    });
    if (!sourceCard) return;
    const predictionSource = (replayHealth.sources || []).find(
      (item) => String(item.table) === "traffic_risk_predictions"
    );
    if (!predictionSource) return;
    const rows = Array.from(sourceCard.querySelectorAll(".row-item"));
    const target = rows.find((row) => {
      const strong = row.querySelector("strong");
      return strong && strong.textContent && strong.textContent.trim() === "traffic_risk_predictions";
    });
    if (!target) return;
    const muted = target.querySelectorAll(".muted");
    if (muted[0]) {
      muted[0].textContent =
        "processed replay rows: " + fmtNum(predictionSource.row_count) +
        " | stored rows: " + fmtNum(predictionSource.stored_row_count || predictionSource.row_count) +
        " | latest processed: " + fmtTs(predictionSource.latest_created_at || predictionSource.latest_event_time);
    }
    if (muted[1]) {
      muted[1].textContent =
        "latest insert: " + fmtTs(predictionSource.latest_created_at || predictionSource.latest_event_time);
    }
  }

  async function patchFromApi() {
    const path = window.location.pathname;
    const [summaryRes, latestRes, replayRes] = await Promise.all([
      fetch(API_BASE + "/api/v1/overview/summary?mode=replay", { cache: "no-store" }),
      fetch(API_BASE + "/api/v1/predictions/latest?limit=6&mode=replay", { cache: "no-store" }),
      fetch(API_BASE + "/api/v1/pipeline/replay-health", { cache: "no-store" })
    ]);
    if (!summaryRes.ok || !latestRes.ok || !replayRes.ok) return;
    const summary = await summaryRes.json();
    const latest = await latestRes.json();
    const replayHealth = await replayRes.json();
    const predictionSource = (replayHealth.sources || []).find(
      (item) => String(item.table) === "traffic_risk_predictions"
    ) || {};

    if (path === "/") {
      setCard(
        findCard(["Total events", "Replay progress"]),
        "Replay progress",
        fmtNum(summary.replay_progress_events ?? summary.total_events),
        "processed replay rows: " + fmtNum(summary.replay_progress_events ?? summary.total_events) +
          " | stored rows: " + fmtNum(predictionSource.stored_row_count || replayHealth.total_row_count || summary.total_events)
      );
      setCard(
        findCard(["Latest event"]),
        "Latest event",
        summary.latest_event_time ? "Online" : "No data",
        fmtTs(summary.latest_event_time)
      );
      patchPredictionsTable(latest.predictions || []);
    }

    if (path.startsWith("/pipeline")) {
      setCard(
        findCard(["Prediction rows", "Replay progress"]),
        "Replay progress",
        fmtNum(replayHealth.row_count),
        "processed replay rows: " + fmtNum(replayHealth.row_count) +
          " | stored rows: " + fmtNum(predictionSource.stored_row_count || replayHealth.row_count) +
          " | latest insert: " + fmtTs(predictionSource.latest_created_at || replayHealth.retrain_loop?.latest_prediction_time)
      );
      setCard(
        findCard(["Retrain loop"]),
        "Retrain loop",
        String((replayHealth.retrain_loop || {}).status || "unavailable").toUpperCase(),
        String((replayHealth.retrain_loop || {}).reason || "")
      );
      patchSourceFreshness(replayHealth);
    }

    const stalePredictionLabel = document.body.textContent && document.body.textContent.includes("Prediction rows");
    const staleHistoricalTime = document.body.textContent && document.body.textContent.includes("01/04/2023");
    if ((stalePredictionLabel || staleHistoricalTime) && sessionStorage.getItem(RELOAD_KEY) !== VERSION) {
      sessionStorage.setItem(RELOAD_KEY, VERSION);
      window.setTimeout(() => window.location.reload(), 250);
    }
  }

  function start() {
    patchFromApi().catch(() => {});
    window.setInterval(() => {
      patchFromApi().catch(() => {});
    }, 10000);
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", start, { once: true });
  } else {
    start();
  }
})();
`;

const navItems = [
  { href: "/", label: "Dashboard", icon: Map },
  { href: "/scenario", label: "Scenario", icon: ShieldAlert },
  { href: "/pipeline", label: "Pipeline", icon: GitBranch }
];

export default function RootLayout({ children }: { children: React.ReactNode }) {
  return (
    <html lang="en">
      <body>
        <Script id="trafficrisk-early-compatibility" strategy="beforeInteractive">
          {EARLY_COMPATIBILITY_SCRIPT}
        </Script>
        <Providers>
          <CompatibilityPatch />
          <div className="app-shell">
            <aside className="sidebar">
              <div className="brand">
                <div className="brand-mark">
                  <Activity size={22} />
                </div>
                <div>
                  <strong>TrafficRisk AI</strong>
                  <span>Risk intelligence</span>
                </div>
              </div>
              <nav className="nav-list">
                {navItems.map((item) => {
                  const Icon = item.icon;
                  return (
                    <Link className="nav-item" href={item.href} key={item.href}>
                      <Icon size={18} />
                      {item.label}
                    </Link>
                  );
                })}
              </nav>
              <div className="sidebar-footer">
                <BarChart3 size={18} />
                <span>US Accidents pipeline</span>
              </div>
            </aside>
            <main className="main-content">{children}</main>
          </div>
        </Providers>
      </body>
    </html>
  );
}
