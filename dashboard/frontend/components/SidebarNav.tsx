"use client";

import { usePathname } from "next/navigation";
import { GitBranch, Map, ShieldAlert } from "lucide-react";

type NavItem = {
  href: string;
  label: string;
  icon: "map" | "shield-alert" | "git-branch";
};

const icons = {
  map: Map,
  "shield-alert": ShieldAlert,
  "git-branch": GitBranch
} as const;
const NAV_BUILD_VERSION = "ui-20260605-0340";

export function SidebarNav({ items }: { items: NavItem[] }) {
  const pathname = usePathname();

  return (
    <nav className="nav-list">
      {items.map((item) => {
        const Icon = icons[item.icon];
        const isActive = item.href === "/"
          ? pathname === "/"
          : pathname.startsWith(item.href);

        return (
          <a
            aria-current={isActive ? "page" : undefined}
            className={isActive ? "nav-item active" : "nav-item"}
            href={`${item.href}${item.href.includes("?") ? "&" : "?"}v=${NAV_BUILD_VERSION}`}
            key={item.href}
          >
            <Icon size={18} />
            {item.label}
          </a>
        );
      })}
    </nav>
  );
}
