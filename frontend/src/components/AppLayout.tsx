import { useEffect, useState, type PropsWithChildren } from "react";
import { NavLink, useLocation, useNavigate } from "react-router-dom";
import { useAuth } from "../auth/AuthContext";
import type { CapabilityName, RoleName } from "../lib/types";

const navItems = [
  { to: "/", label: "Dashboard", icon: "▦", roles: ["premium", "admin"] as RoleName[] },
  { to: "/guide", label: "Guide", icon: "◫", roles: ["premium", "admin"] as RoleName[] },
  { to: "/charts", label: "Charts", icon: "◍" },
  { to: "/earnings", label: "Earnings", icon: "◒" },
  { to: "/watchlists", label: "Watchlists", icon: "◌", roles: ["premium", "admin"] as RoleName[] },
  { to: "/rotation/sector", label: "Rotation", icon: "◎", roles: ["premium", "admin"] as RoleName[] },
  { to: "/report", label: "Report", icon: "◈", roles: ["premium", "admin"] as RoleName[] },
  { to: "/backtests", label: "Backtests", icon: "▥", roles: ["premium", "admin"] as RoleName[] },
  { to: "/screeners", label: "Screeners", icon: "◉", capability: "run_screeners" as CapabilityName },
  { to: "/portfolio", label: "Portfolio", icon: "◐", capability: "manage_exclusions" as CapabilityName },
  { to: "/admin", label: "Admin", icon: "◔", capability: "manage_exclusions" as CapabilityName },
];

export function AppLayout({ children }: PropsWithChildren) {
  const location = useLocation();
  const navigate = useNavigate();
  const auth = useAuth();
  const [isMobileNavOpen, setIsMobileNavOpen] = useState(false);

  useEffect(() => {
    setIsMobileNavOpen(false);
  }, [location.pathname]);

  const visibleNavItems = navItems.filter(
    (item) => (!item.roles || item.roles.includes(auth.role)) && (!item.capability || auth.hasCapability(item.capability)),
  );

  const handleLogout = async () => {
    await auth.logout();
    navigate("/", { replace: true });
  };

  return (
    <div className="app-shell">
      <aside className={`sidebar${isMobileNavOpen ? " is-open" : ""}`}>
        <div className="brand-block">
          <div className="brand-title">Ticker Screener</div>
          <div className="brand-subtitle">Clinical Analytics</div>
        </div>
        <nav id="primary-navigation" className="sidebar-nav">
          {visibleNavItems.map((item) => (
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
          {auth.authenticated ? (
            <>
              <span className="footer-link">{auth.user?.email}</span>
              <span className="footer-link">{auth.role.toUpperCase()}</span>
              <button className="footer-link button-link" type="button" onClick={() => void handleLogout()}>
                Sign Out
              </button>
            </>
          ) : (
            <NavLink className="footer-link" to="/login">
              Sign In
            </NavLink>
          )}
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
            <span className="status-chip">{auth.authenticated ? `ROLE: ${auth.role.toUpperCase()}` : "ROLE: VISITOR"}</span>
          </div>
        </header>
        <main className="page-shell">{children}</main>
      </div>
    </div>
  );
}
