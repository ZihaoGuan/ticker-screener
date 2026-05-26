import { Panel } from "../components/Panel";
import { strategyCards, screenerJobs, watchlistFiles } from "../lib/mock-data";
import { StatusPill } from "../components/StatusPill";

export function DashboardPage() {
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
              <p className="card-meta">Last Run: {card.lastRun}</p>
              <div className="metric-value">
                {String(card.hits).padStart(2, "0")} <span>tickers found</span>
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
              {screenerJobs.map((job) => (
                <tr key={job.jobId}>
                  <td>{job.label}</td>
                  <td>{job.startedAt}</td>
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
                  <div className="file-name">{file.label}</div>
                  <div className="file-meta">
                    {file.dateLabel} • {file.sizeLabel}
                  </div>
                </div>
              </div>
            ))}
          </div>
        </Panel>
      </div>
    </div>
  );
}
