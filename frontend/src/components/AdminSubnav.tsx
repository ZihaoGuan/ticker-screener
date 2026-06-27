import { NavLink } from "react-router-dom";

type AdminSubnavProps = {
  title: string;
  description: string;
};

export function AdminSubnav({ title, description }: AdminSubnavProps) {
  return (
    <section className="panel screeners-subnav-panel">
      <div className="screeners-subnav-copy">
        <span className="eyebrow">Admin</span>
        <h1>{title}</h1>
        <p className="panel-copy">{description}</p>
      </div>
      <div className="screeners-subnav-links" role="tablist" aria-label="Admin sections">
        <NavLink to="/admin" end className={({ isActive }) => `screeners-subnav-link${isActive ? " is-active" : ""}`}>
          Admin Overview
        </NavLink>
        <NavLink to="/admin/missing-sectors" className={({ isActive }) => `screeners-subnav-link${isActive ? " is-active" : ""}`}>
          Missing Sectors
        </NavLink>
        <NavLink to="/admin/finviz-missing-tickers" className={({ isActive }) => `screeners-subnav-link${isActive ? " is-active" : ""}`}>
          Finviz Missing
        </NavLink>
        <NavLink to="/admin/ticker-ratings-health" className={({ isActive }) => `screeners-subnav-link${isActive ? " is-active" : ""}`}>
          Ticker Ratings Health
        </NavLink>
        <NavLink to="/admin/discord-notifications" className={({ isActive }) => `screeners-subnav-link${isActive ? " is-active" : ""}`}>
          Discord Alerts
        </NavLink>
      </div>
    </section>
  );
}
