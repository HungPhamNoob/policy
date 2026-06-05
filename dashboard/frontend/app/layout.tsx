import type { Metadata } from "next";
import Script from "next/script";
import { Activity, BarChart3 } from "lucide-react";
import { SidebarNav } from "@/components/SidebarNav";
import "./globals.css";
import { Providers } from "./providers";

export const metadata: Metadata = {
  title: "TrafficRisk AI",
  description: "Realtime road accident risk dashboard"
};

export const dynamic = "force-dynamic";
export const fetchCache = "force-no-store";

const EARLY_COMPATIBILITY_SCRIPT = `
(() => {
  const VERSION = "ui-20260605-0510";
  const ROUTE_VERSION_KEY = "trafficrisk_ui_route_version_once";

  function ensureVersionedLocation() {
    const url = new URL(window.location.href);
    if (url.searchParams.get("v") === VERSION) return false;
    if (sessionStorage.getItem(ROUTE_VERSION_KEY) === VERSION) return false;
    sessionStorage.setItem(ROUTE_VERSION_KEY, VERSION);
    url.searchParams.set("v", VERSION);
    window.location.replace(url.toString());
    return true;
  }

  function start() {
    if (ensureVersionedLocation()) {
      return;
    }
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", start, { once: true });
  } else {
    start();
  }
})();
`;

const navItems = [
  { href: "/", label: "Dashboard", icon: "map" as const },
  { href: "/scenario", label: "Scenario", icon: "shield-alert" as const },
  { href: "/pipeline", label: "Pipeline", icon: "git-branch" as const }
];

export default function RootLayout({ children }: { children: React.ReactNode }) {
  return (
    <html lang="en">
      <body>
        <Script id="trafficrisk-early-compatibility" strategy="beforeInteractive">
          {EARLY_COMPATIBILITY_SCRIPT}
        </Script>
        <Providers>
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
              <SidebarNav items={navItems} />
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
