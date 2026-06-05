import type { Metadata } from "next";
import Link from "next/link";
import { Activity, BarChart3, GitBranch, Map, ShieldAlert } from "lucide-react";
import { CompatibilityPatch } from "@/components/CompatibilityPatch";
import "./globals.css";
import { Providers } from "./providers";

export const metadata: Metadata = {
  title: "TrafficRisk AI",
  description: "Realtime road accident risk dashboard"
};

const navItems = [
  { href: "/", label: "Dashboard", icon: Map },
  { href: "/scenario", label: "Scenario", icon: ShieldAlert },
  { href: "/pipeline", label: "Pipeline", icon: GitBranch }
];

export default function RootLayout({ children }: { children: React.ReactNode }) {
  return (
    <html lang="en">
      <body>
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
