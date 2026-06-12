import { useEffect, useState } from "react";
import { Link } from "react-router-dom";
import { LoadingBlock } from "../components/LoadingBlock";
import { StatusPill } from "../components/StatusPill";
import { Panel } from "../components/Panel";
import { fetchJson } from "../lib/api";
import { formatLocalDateTime } from "../lib/format";
import type { DashboardResponse, JobsResponse } from "../lib/types";

type MarketRegime = "healthy_pullback" | "healthy_uptrend" | "chaos" | "caution";

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
  const regime = dashboard?.market_health?.regime ?? null;
  const regimeLatest = regime?.latest ?? null;
  const rsiDivergence = dashboard?.market_health?.rsi_divergence ?? null;
  const rsiLatest = rsiDivergence?.latest ?? null;
  const spyExtension = dashboard?.market_health?.spy_extension ?? null;
  const spyLatest = spyExtension?.latest ?? null;

  return (
    <div className="page-grid">
      <Panel title="Market Health" aside={<span className="eyebrow">SPY timing check</span>}>
        {isLoading ? <LoadingBlock label="Loading market health…" compact /> : null}
        <div className="card-grid">
          <article className="metric-card">
            <div className="metric-card-head">
              <h3>{regime?.ticker ?? spyExtension?.ticker ?? "SPY"}</h3>
              <span className={`accent-mark accent-${regimeAccent(regimeLatest?.regime)}`} />
            </div>
            <p className="card-meta">Weekly 21EMA sets primary trend. Daily 21EMA shows short-term pressure.</p>
            <div className="metric-value">
              {regimeLatest?.regime_label ?? "Unavailable"} <span>{regimeSubLabel(regimeLatest)}</span>
            </div>
            <p className="card-meta">
              {regimeLatest
                ? `Weekly 21EMA: ${regimeLatest.weekly_uptrend ? "Uptrending" : "Below trend"} · Daily 21EMA: ${regimeLatest.daily_downtrend ? "Short-term downtrend" : "Above short-term trend"}`
                : "No SPY market-health data available."}
            </p>
            <p className="card-meta">
              {regimeLatest
                ? `Daily ${formatPrice(regimeLatest.daily_close)} vs 21EMA ${formatPrice(regimeLatest.daily_ema21)} (${formatPercentSigned(regimeLatest.daily_distance_pct)})`
                : "Waiting for market data."}
            </p>
            <p className="card-meta">
              {regimeLatest
                ? `Weekly ${formatPrice(regimeLatest.weekly_close)} vs 21EMA ${formatPrice(regimeLatest.weekly_ema21)} (${formatPercentSigned(regimeLatest.weekly_distance_pct)})`
                : ""}
            </p>
            <p className="card-meta">{regimeLatest?.explanation ?? ""}</p>
            <p className="card-meta">
              {regimeLatest ? `As of ${regimeLatest.date}` : "Waiting for market data."}
              {regime?.data_source ? ` · Source ${regime.data_source}` : ""}
            </p>
          </article>

          <article className="metric-card">
            <div className="metric-card-head">
              <h3>{rsiDivergence?.ticker ?? "SPY"} RSI Div</h3>
              <span className={`accent-mark accent-${rsiLatest?.state === "overbought_warning" ? "down" : rsiLatest ? "neutral" : "up"}`} />
            </div>
            <p className="card-meta">Regular bearish RSI divergence only. Hidden and ghost divergences ignored.</p>
            <div className="metric-value">
              {rsiLatest?.label ?? "No Signal"} <span>{rsiSignalSubLabel(rsiLatest)}</span>
            </div>
            <p className="card-meta">
              {rsiLatest
                ? `Signal ${rsiLatest.signal_date} · RSI ${rsiLatest.signal_rsi.toFixed(2)} vs prev ${rsiLatest.previous_signal_rsi.toFixed(2)}`
                : "No recent filtered regular bearish divergence."}
            </p>
            <p className="card-meta">
              {rsiLatest
                ? `High ${formatPrice(rsiLatest.signal_price)} vs prev ${formatPrice(rsiLatest.previous_signal_price)} · ${rsiLatest.price_change_pct.toFixed(2)}% move · ${rsiLatest.bars_apart} bars`
                : ""}
            </p>
            <p className="card-meta">{rsiLatest?.explanation ?? ""}</p>
            <p className="card-meta">
              {rsiDivergence?.data_source ? `Source ${rsiDivergence.data_source}` : ""}
            </p>
          </article>

          <article className="metric-card">
            <div className="metric-card-head">
              <h3>{spyExtension?.ticker ?? "SPY"} Extension</h3>
              <span className={`accent-mark accent-${spyLatest?.state === "extreme" ? "down" : spyLatest?.state === "warning" ? "neutral" : "up"}`} />
            </div>
            <p className="card-meta">{spyExtension?.label ?? "10W SMA"} stretch from weekly trend.</p>
            <div className="metric-value">
              {spyLatest ? `${spyLatest.extension_pct.toFixed(2)}%` : "--"} <span>{formatSpyExtensionState(spyLatest?.state)}</span>
            </div>
            <p className="card-meta">
              {spyLatest
                ? `Dist ${formatPrice(spyLatest.distance)} · Close ${formatPrice(spyLatest.close)} · MA ${formatPrice(spyLatest.moving_average)}`
                : "No SPY extension data available."}
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

function formatPercentSigned(value: number | null | undefined) {
  if (value == null) {
    return "--";
  }
  const prefix = value > 0 ? "+" : "";
  return `${prefix}${value.toFixed(2)}%`;
}

function regimeAccent(regime: MarketRegime | null | undefined) {
  if (regime === "chaos") {
    return "down";
  }
  if (regime === "caution") {
    return "neutral";
  }
  return "up";
}

function regimeSubLabel(latest: DashboardResponse["market_health"]["regime"]["latest"] | null | undefined) {
  if (!latest) {
    return "Unavailable";
  }
  if (latest.regime === "healthy_pullback") {
    return "Weekly uptrend, daily reset";
  }
  if (latest.regime === "healthy_uptrend") {
    return "Weekly and daily aligned";
  }
  if (latest.regime === "chaos") {
    return "Weekly + daily weak";
  }
  return "Bounce attempt";
}

function rsiSignalSubLabel(latest: DashboardResponse["market_health"]["rsi_divergence"]["latest"] | null | undefined) {
  if (!latest) {
    return "Normal";
  }
  if (latest.state === "overbought_warning") {
    return `RSI >= ${latest.overbought_threshold}`;
  }
  return "Watch closely";
}
