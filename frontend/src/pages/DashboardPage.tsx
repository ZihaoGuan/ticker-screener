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
  const spyExtension = dashboard?.market_health?.spy_extension ?? null;
  const spyLatest = spyExtension?.latest ?? null;

  return (
    <div className="page-grid">
      <Panel title="Market Health" aside={<span className="eyebrow">SPY timing check</span>}>
        {isLoading ? <LoadingBlock label="Loading market health…" compact /> : null}
        <div className="card-grid">
          <article className="metric-card">
            <div className="metric-card-head">
              <h3>{spyExtension?.ticker ?? "SPY"}</h3>
              <span className={`accent-mark accent-${spyLatest?.state === "extreme" ? "down" : spyLatest?.state === "warning" ? "neutral" : "up"}`} />
            </div>
            <p className="card-meta">{spyExtension?.label ?? "10W SMA"} extension from weekly trend.</p>
            <div className="metric-value">
              {spyLatest ? `${spyLatest.extension_pct.toFixed(2)}%` : "--"} <span>{formatSpyExtensionState(spyLatest?.state)}</span>
            </div>
            <p className="card-meta">
              {spyLatest
                ? `Dist ${formatPrice(spyLatest.distance)} · Close ${formatPrice(spyLatest.close)} · MA ${formatPrice(spyLatest.moving_average)}`
                : "No SPY market-health data available."}
            </p>
            <p className="card-meta">
              {spyLatest ? `As of ${spyLatest.time}` : "Waiting for market data."}
              {spyExtension?.data_source ? ` · Source ${spyExtension.data_source}` : ""}
            </p>
          </article>
        </div>
      </Panel>

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

function formatSpyExtensionState(state: "normal" | "warning" | "extreme" | null | undefined) {
  if (state === "warning") {
    return "Overextended";
  }
  if (state === "extreme") {
    return "Extreme";
  }
  if (state === "normal") {
    return "Normal";
  }
  return "Unavailable";
}

function formatPrice(value: number | null | undefined) {
  return value == null ? "--" : `$${value.toFixed(2)}`;
}
