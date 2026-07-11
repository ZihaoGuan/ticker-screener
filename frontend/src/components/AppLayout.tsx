import { useEffect, useState, type PropsWithChildren } from "react";
import { NavLink, useLocation, useNavigate } from "react-router-dom";
import { useAuth } from "../auth/AuthContext";
import type { CapabilityName, RoleName } from "../lib/types";

const navItems = [
  { to: "/", label: "Dashboard", icon: "D" },
  { to: "/scanner", label: "Scanner", icon: "S" },
  { to: "/ratings", label: "Ratings", icon: "R" },
  { to: "/guide", label: "Guide", icon: "G", roles: ["premium", "admin"] as RoleName[] },
  { to: "/charts", label: "Charts", icon: "C" },
  { to: "/earnings", label: "Earnings", icon: "E" },
  { to: "/watchlists", label: "Watchlists", icon: "W", roles: ["admin"] as RoleName[] },
  { to: "/pair-trades", label: "Pair Trades", icon: "T", roles: ["admin"] as RoleName[], capability: "run_screeners" as CapabilityName },
  { to: "/my-picks", label: "My Picks", icon: "M", roles: ["admin"] as RoleName[], expo: true, capability: "manage_exclusions" as CapabilityName },
  { to: "/rotation/sector", label: "Rotation", icon: "O" },
  { to: "/report", label: "Report", icon: "P", roles: ["admin"] as RoleName[] },
  { to: "/screeners", label: "Screeners", icon: "N", roles: ["admin"] as RoleName[], capability: "run_screeners" as CapabilityName },
  { to: "/warmup", label: "Warmup", icon: "U", roles: ["admin"] as RoleName[], expo: true, capability: "run_screeners" as CapabilityName },
  { to: "/backtests", label: "Backtests", icon: "B", roles: ["admin"] as RoleName[], expo: true, capability: "run_screeners" as CapabilityName },
  { to: "/portfolio", label: "Portfolio", icon: "F", roles: ["admin"] as RoleName[], expo: true, capability: "manage_exclusions" as CapabilityName },
  { to: "/tiger-positions", label: "Tiger", icon: "I", roles: ["premium", "admin"] as RoleName[] },
  { to: "/admin", label: "Admin", icon: "A", capability: "manage_exclusions" as CapabilityName },
];

export function AppLayout({ children }: PropsWithChildren) {
  const location = useLocation();
  const navigate = useNavigate();
  const auth = useAuth();
  const [isMobileNavOpen, setIsMobileNavOpen] = useState(false);
  const showInfraStatus = auth.role === "admin";
  const infraSummary = showInfraStatus ? "INFRA: WEB HEALTHY / DB CONNECTED / ARTIFACTS REACHABLE" : null;

  useEffect(() => {
    setIsMobileNavOpen(false);
  }, [location.pathname]);

  useEffect(() => {
    if (!isMobileNavOpen) {
      document.body.style.overflow = "";
      return;
    }
    document.body.style.overflow = "hidden";
    return () => {
      document.body.style.overflow = "";
    };
  }, [isMobileNavOpen]);

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
        <div className="sidebar-head">
          <div className="brand-block">
            <div className="brand-title">Ticker Screener</div>
            <div className="brand-subtitle">Clinical Analytics</div>
          </div>
          <button
            className="sidebar-close"
            type="button"
            onClick={() => setIsMobileNavOpen(false)}
            aria-label="Close navigation menu"
          >
            Close
          </button>
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
              {item.expo ? <span className="nav-tag">EXPO</span> : null}
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
      <button
        type="button"
        className={`app-shell-backdrop${isMobileNavOpen ? " is-visible" : ""}`}
        aria-label="Close navigation drawer"
        onClick={() => setIsMobileNavOpen(false)}
      />
      <div className="main-shell">
        <header className="topbar">
          <div className="topbar-main">
            <button
              className="mobile-nav-toggle"
              type="button"
              onClick={() => setIsMobileNavOpen((current) => !current)}
              aria-expanded={isMobileNavOpen}
              aria-controls="primary-navigation"
              aria-label={isMobileNavOpen ? "Close navigation menu" : "Open navigation menu"}
            >
              {isMobileNavOpen ? "Close" : "Menu"}
            </button>
            <div className="topbar-brand">Ticker Screener</div>
          </div>
          <div className="topbar-status topbar-status-compact">
            <span className="status-chip status-chip-compact">{infraSummary ?? (auth.authenticated ? `ROLE: ${auth.role.toUpperCase()}` : "ROLE: VISITOR")}</span>
          </div>
          <div className="topbar-status topbar-status-full">
            {showInfraStatus ? <span className="status-chip">WEB: HEALTHY</span> : null}
            {showInfraStatus ? <span className="status-chip">DB: CONNECTED</span> : null}
            {showInfraStatus ? <span className="status-chip">ARTIFACTS: REACHABLE</span> : null}
            <span className="status-chip">{auth.authenticated ? `ROLE: ${auth.role.toUpperCase()}` : "ROLE: VISITOR"}</span>
          </div>
        </header>
        <main className="page-shell">{children}</main>
      </div>
    </div>
  );
}
