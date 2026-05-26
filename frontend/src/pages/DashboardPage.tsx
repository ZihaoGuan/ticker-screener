import { useEffect, useState } from "react";
import { StatusPill } from "../components/StatusPill";
import { Panel } from "../components/Panel";
import { fetchJson } from "../lib/api";
import type { DashboardResponse, JobsResponse } from "../lib/types";

export function DashboardPage() {
  const [dashboard, setDashboard] = useState<DashboardResponse | null>(null);
  const [jobs, setJobs] = useState<JobsResponse["jobs"]>([]);

  useEffect(() => {
    void fetchJson<DashboardResponse>("/api/dashboard").then(setDashboard).catch(() => setDashboard(null));
    void fetchJson<JobsResponse>("/api/jobs").then((payload) => setJobs(payload.jobs)).catch(() => setJobs([]));
  }, []);

  const strategyCards = dashboard?.strategy_cards ?? [];
  const watchlistFiles = dashboard?.recent_watchlists ?? [];

  return (
    <div className="page-grid">
      <Panel title="Key Strategy Metrics" aside={<span className="eyebrow">Last 24 hours</span>}>
        <div className="card-grid">
          {strategyCards.map((card) => (
            <article key={card.id} className="metric-card">
              <div className="metric-card-head">
                <h3>{card.label}</h3>
                <span className={`accent-mark accent-${card.accent ?? "neutral"}`} />
              </div>
              <p className="card-meta">{card.description}</p>
              <div className="metric-value">
                {String(card.hits ?? 0).padStart(2, "0")} <span>tickers found</span>
              </div>
            </article>
          ))}
        </div>
      </Panel>

      <div className="split-grid">
        <Panel title="Recent Screening Activity" aside={<button className="ghost-button">View all runs</button>}>
          <table className="data-table">
            <thead>
              <tr>
                <th>Screener</th>
                <th>Timestamp</th>
                <th>Status</th>
              </tr>
            </thead>
            <tbody>
              {jobs.map((job) => (
                <tr key={job.job_id}>
                  <td>{job.label}</td>
                  <td>{job.started_at || "-"}</td>
                  <td>
                    <StatusPill status={job.status} />
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </Panel>

        <Panel title="Recent Watchlists">
          <div className="file-list">
            {watchlistFiles.map((file) => (
              <div key={file.stem} className="file-row">
                <div>
                  <div className="file-name">{file.name}</div>
                  <div className="file-meta">{file.stem}</div>
                </div>
              </div>
            ))}
          </div>
        </Panel>
      </div>
    </div>
  );
}
