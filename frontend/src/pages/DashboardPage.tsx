import { useEffect, useState } from "react";
import { Link } from "react-router-dom";
import { LoadingBlock } from "../components/LoadingBlock";
import { StatusPill } from "../components/StatusPill";
import { Panel } from "../components/Panel";
import { fetchJson } from "../lib/api";
import { formatLocalDateTime } from "../lib/format";
import type { DashboardResponse, JobsResponse } from "../lib/types";

export function DashboardPage() {
  const [dashboard, setDashboard] = useState<DashboardResponse | null>(null);
  const [jobs, setJobs] = useState<JobsResponse["jobs"]>([]);
  const [isLoading, setIsLoading] = useState(true);

  useEffect(() => {
    Promise.all([
      fetchJson<DashboardResponse>("/api/dashboard").then(setDashboard).catch(() => setDashboard(null)),
      fetchJson<JobsResponse>("/api/jobs").then((payload) => setJobs(payload.jobs)).catch(() => setJobs([])),
    ]).finally(() => setIsLoading(false));
  }, []);

  const strategyCards = dashboard?.strategy_cards ?? [];
  const watchlistFiles = dashboard?.recent_watchlists ?? [];

  return (
    <div className="page-grid">
      <Panel title="Key Strategy Metrics" aside={<span className="eyebrow">Last 24 hours</span>}>
        {isLoading ? <LoadingBlock label="Loading dashboard metrics…" compact /> : null}
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
        <Panel title="Recent Screening Activity" aside={<Link className="ghost-button" to="/screeners">View all screeners</Link>}>
          <div className="data-table-responsive">
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
                    <td data-label="Screener">{job.label}</td>
                    <td data-label="Timestamp">{formatLocalDateTime(job.started_at)}</td>
                    <td data-label="Status">
                      <StatusPill status={job.status} />
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </Panel>

        <Panel title="Recent Watchlists">
          {isLoading ? <LoadingBlock label="Loading recent watchlists…" compact /> : null}
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
