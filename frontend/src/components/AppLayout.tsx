import type { PropsWithChildren } from "react";
import { NavLink, useLocation } from "react-router-dom";

const navItems = [
  { to: "/", label: "Dashboard", icon: "▦" },
  { to: "/guide", label: "Guide", icon: "◫" },
  { to: "/runs", label: "Runs", icon: "◉" },
  { to: "/watchlists", label: "Watchlists", icon: "◌" },
  { to: "/rrg/sector", label: "Sector Rotation", icon: "◎" },
  { to: "/rrg/industry", label: "Industry Rotation", icon: "◍" },
  { to: "/rrg/theme", label: "Theme Rotation", icon: "◐" },
  { to: "/overlap", label: "Overlap", icon: "◈" },
  { to: "/backtests", label: "Backtests", icon: "▥" },
  { to: "/admin", label: "Admin", icon: "◔" },
];

export function AppLayout({ children }: PropsWithChildren) {
  const location = useLocation();

  return (
    <div className="app-shell">
      <aside className="sidebar">
        <div className="brand-block">
          <div className="brand-title">Ticker Screener</div>
          <div className="brand-subtitle">Clinical Analytics</div>
        </div>
        <nav className="sidebar-nav">
          {navItems.map((item) => (
            <NavLink
              key={item.to}
              to={item.to}
              end={item.to === "/"}
              className={({ isActive }) =>
                `nav-item${isActive || (item.to !== "/" && location.pathname.startsWith(item.to)) ? " is-active" : ""}`
              }
            >
              <span className="nav-icon">{item.icon}</span>
              <span>{item.label}</span>
            </NavLink>
          ))}
        </nav>
        <div className="sidebar-footer">
          <a className="footer-link" href="#">
            Settings
          </a>
          <a className="footer-link" href="#">
            Support
          </a>
        </div>
      </aside>
      <div className="main-shell">
        <header className="topbar">
          <div className="search-box">CMD + K to search…</div>
          <div className="topbar-status">
            <span className="status-chip">WEB: HEALTHY</span>
            <span className="status-chip">DB: CONNECTED</span>
            <span className="status-chip">ARTIFACTS: REACHABLE</span>
          </div>
        </header>
        <main className="page-shell">{children}</main>
      </div>
    </div>
  );
}
