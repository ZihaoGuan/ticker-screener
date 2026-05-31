import { useEffect, useState, type PropsWithChildren } from "react";
import { NavLink, useLocation } from "react-router-dom";

const navItems = [
  { to: "/", label: "Dashboard", icon: "▦" },
  { to: "/guide", label: "Guide", icon: "◫" },
  { to: "/runs", label: "Runs", icon: "◉" },
  { to: "/watchlists", label: "Watchlists", icon: "◌" },
  { to: "/rotation/sector", label: "Rotation", icon: "◎" },
  { to: "/overlap", label: "Overlap", icon: "◈" },
  { to: "/backtests", label: "Backtests", icon: "▥" },
  { to: "/admin", label: "Admin", icon: "◔" },
];

export function AppLayout({ children }: PropsWithChildren) {
  const location = useLocation();
  const [isMobileNavOpen, setIsMobileNavOpen] = useState(false);

  useEffect(() => {
    setIsMobileNavOpen(false);
  }, [location.pathname]);

  return (
    <div className="app-shell">
      <aside className={`sidebar${isMobileNavOpen ? " is-open" : ""}`}>
        <div className="brand-block">
          <div className="brand-title">Ticker Screener</div>
          <div className="brand-subtitle">Clinical Analytics</div>
        </div>
        <nav id="primary-navigation" className="sidebar-nav">
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
          <div className="topbar-main">
            <button
              className="mobile-nav-toggle"
              type="button"
              onClick={() => setIsMobileNavOpen((current) => !current)}
              aria-expanded={isMobileNavOpen}
              aria-controls="primary-navigation"
            >
              {isMobileNavOpen ? "Close" : "Menu"}
            </button>
            <div className="search-box">CMD + K to search…</div>
          </div>
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
