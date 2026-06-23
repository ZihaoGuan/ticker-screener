import { useEffect, useState } from "react";
import { LoadingBlock } from "../components/LoadingBlock";
import { Panel } from "../components/Panel";
import { fetchJson } from "../lib/api";
import type { DashboardResponse } from "../lib/types";

type MarketRegime =
  | "healthy_chaos"
  | "perfect_convergence_bull"
  | "perfect_convergence_bear"
  | "bear_market_rally";

export function DashboardPage() {
  const [dashboard, setDashboard] = useState<DashboardResponse | null>(null);
  const [isLoading, setIsLoading] = useState(true);

  useEffect(() => {
    fetchJson<DashboardResponse>("/api/dashboard").then(setDashboard).catch(() => setDashboard(null)).finally(() => setIsLoading(false));
  }, []);

  const regime = dashboard?.market_health?.regime ?? null;
  const regimeLatest = regime?.latest ?? null;
  const rsiDivergence = dashboard?.market_health?.rsi_divergence ?? null;
  const rsiLatest = rsiDivergence?.latest ?? null;
  const bearishTd9 = dashboard?.market_health?.bearish_td9 ?? null;
  const td9Latest = bearishTd9?.latest ?? null;
  const optionsPositioning = dashboard?.market_health?.options_positioning ?? null;
  const optionsLatest = optionsPositioning?.latest ?? null;
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
              {regimeLatest?.regime_label ?? "Unavailable"} <span>{regimeLatest?.summary ?? "Unavailable"}</span>
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

          <article className="metric-card market-matrix-card">
            <div className="metric-card-head">
              <h3>Operational Matrix</h3>
              <span className={`accent-mark accent-${regimeAccent(regimeLatest?.regime)}`} />
            </div>
            <p className="card-meta">Use weekly 21EMA as anchor, daily 21EMA as pulse. Active cell shows current market state.</p>
            <div className="market-matrix">
              {MARKET_MATRIX_CELLS.map((cell) => {
                const isActive = regimeLatest?.regime === cell.regime;
                return (
                  <div
                    key={cell.regime}
                    className={`market-matrix-cell market-matrix-${cell.tone}${isActive ? " is-active" : ""}`}
                  >
                    <div className="market-matrix-axis">{cell.axis}</div>
                    <strong>{cell.title}</strong>
                    <p>{cell.copy}</p>
                  </div>
                );
              })}
            </div>
            <p className="card-meta">
              {regimeLatest ? `Active setup: ${regimeLatest.regime_label}.` : "Waiting for market data."}
            </p>
          </article>

          <article className="metric-card">
            <div className="metric-card-head">
              <h3>{rsiDivergence?.ticker ?? "SPY"} RSI Top</h3>
              <span className={`accent-mark accent-${rsiSignalAccent(rsiLatest?.state)}`} />
            </div>
            <p className="card-meta">Daily bearish RSI divergence top using Charles Edwards style pivot logic.</p>
            <div className="metric-value">
              {rsiLatest?.label ?? "No Signal"} <span>{rsiSignalSubLabel(rsiLatest)}</span>
            </div>
            <p className="card-meta">
              {rsiLatest
                ? `Signal ${rsiLatest.signal_date} · ${rsiLatest.bars_since_signal} bars ago · RSI ${rsiLatest.signal_rsi.toFixed(2)} vs prev ${rsiLatest.previous_signal_rsi.toFixed(2)}`
                : "No recent daily bearish RSI divergence top."}
            </p>
            <p className="card-meta">
              {rsiLatest
                ? `Top ${formatPrice(rsiLatest.signal_price)} vs prev ${formatPrice(rsiLatest.previous_signal_price)} · Now ${formatPrice(rsiLatest.current_close)} · ${formatPercentSigned(rsiLatest.distance_from_signal_pct)} from top`
                : ""}
            </p>
            <p className="card-meta">
              {rsiLatest
                ? `Current RSI ${rsiLatest.current_rsi.toFixed(2)} · Daily 21EMA ${formatPrice(rsiLatest.current_ema21)}`
                : ""}
            </p>
            <p className="card-meta">
              {rsiLatest
                ? `E = lift after enough closes below daily 21EMA · R = lift after RSI resets below ${rsiLatest.reset_rsi_threshold}`
                : "E = lift after enough closes below daily 21EMA · R = lift after RSI reset below 45"}
            </p>
            <p className="card-meta">{rsiLiftDetail(rsiLatest)}</p>
            <p className="card-meta">{rsiLatest?.explanation ?? ""}</p>
            <p className="card-meta">
              {rsiDivergence?.data_source ? `Source ${rsiDivergence.data_source}` : ""}
            </p>
          </article>

          <article className={`metric-card${optionsLatest?.plots ? " metric-card-wide" : ""}`}>
            <div className="metric-card-head">
              <h3>{optionsPositioning?.ticker ?? "SPX"} Options Positioning</h3>
              <span className={`accent-mark accent-${optionsLatest?.gex_regime === "negative" ? "down" : "up"}`} />
            </div>
            <p className="card-meta">
              {optionsLatest?.plots
                ? "Admin-refreshed SPX gamma exposure cache. Dashboard reads stored plot artifacts only."
                : "CBOE-delayed close snapshot loaded from persisted DB summary, not live-fetched on dashboard load."}
            </p>
            <div className="metric-value">
              {optionsLatest?.gex_label ?? "Unavailable"} <span>{optionsLatest ? `Flip ${formatPrice(optionsLatest.gamma_flip)}` : "No options snapshot"}</span>
            </div>
            <p className="card-meta">
              {optionsLatest
                ? `Net GEX ${formatCompactNumber(optionsLatest.net_gex)} · Spot ${formatPrice(optionsLatest.spot)} · ${formatFlipDistance(optionsLatest.distance_to_flip_pct)}`
                : "No GEX data available."}
            </p>
            <p className="card-meta">
              {optionsLatest
                ? `Call wall ${formatPrice(optionsLatest.call_wall)} · Put wall ${formatPrice(optionsLatest.put_wall)} · ATM pin ${formatPrice(optionsLatest.atm_pin_strike)}`
                : ""}
            </p>
            <p className="card-meta">
              {optionsLatest
                ? `Tracked strikes ${optionsLatest.strike_count ?? "--"} · Put/Call OI ${formatRatio(optionsLatest.put_call_oi_ratio)}`
                : ""}
            </p>
            <p className="card-meta">
              {optionsLatest
                ? `Next expiry ${optionsLatest.next_expiry || "--"} · Next monthly ${optionsLatest.next_monthly_expiry || "--"}`
                : ""}
            </p>
            <p className="card-meta">{optionsLatest?.summary ?? ""}</p>
            <p className="card-meta">{optionsLatest?.methodology ?? ""}</p>
            {optionsLatest?.plots ? (
              <div className="gex-admin-plot-grid">
                <div className="gex-admin-plot" dangerouslySetInnerHTML={{ __html: optionsLatest.plots.absolute }} />
                <div className="gex-admin-plot" dangerouslySetInnerHTML={{ __html: optionsLatest.plots.by_option_type }} />
                <div className="gex-admin-plot gex-admin-plot-wide" dangerouslySetInnerHTML={{ __html: optionsLatest.plots.profile }} />
              </div>
            ) : null}
            <p className="card-meta">
              {optionsLatest ? `As of ${optionsLatest.as_of}` : "Waiting for market data."}
              {optionsPositioning?.data_source ? ` · Source ${optionsPositioning.data_source}` : ""}
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

          <article className="metric-card">
            <div className="metric-card-head">
              <h3>{bearishTd9?.ticker ?? "SPY"} TD9</h3>
              <span className={`accent-mark accent-${td9Latest ? "down" : "up"}`} />
            </div>
            <p className="card-meta">Bearish TD Sequential setup on daily bars.</p>
            <div className="metric-value">
              {td9Latest?.label ?? "No Signal"} <span>{td9SignalSubLabel(td9Latest)}</span>
            </div>
            <p className="card-meta">
              {td9Latest
                ? `Signal ${td9Latest.signal_date} · Setup ${td9Latest.setup_count}/9 · Close ${formatPrice(td9Latest.signal_close)}`
                : "No active bearish TD9 on latest SPY daily bar."}
            </p>
            <p className="card-meta">
              {td9Latest
                ? `Compare close[4] ${formatPrice(td9Latest.comparison_close)} · Stretch ${formatPercentSigned(td9Latest.distance_from_compare_pct)}`
                : ""}
            </p>
            <p className="card-meta">{td9Latest?.explanation ?? ""}</p>
            <p className="card-meta">
              {bearishTd9?.data_source ? `Source ${bearishTd9.data_source}` : ""}
            </p>
          </article>
        </div>
      </Panel>
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

function formatCompactNumber(value: number | null | undefined) {
  if (value == null) {
    return "--";
  }
  const absValue = Math.abs(value);
  if (absValue >= 1_000_000_000) {
    return `${value < 0 ? "-" : ""}$${(absValue / 1_000_000_000).toFixed(2)}B`;
  }
  if (absValue >= 1_000_000) {
    return `${value < 0 ? "-" : ""}$${(absValue / 1_000_000).toFixed(2)}M`;
  }
  if (absValue >= 1_000) {
    return `${value < 0 ? "-" : ""}$${(absValue / 1_000).toFixed(2)}K`;
  }
  return `${value < 0 ? "-" : ""}$${absValue.toFixed(2)}`;
}

function formatFlipDistance(value: number | null | undefined) {
  if (value == null) {
    return "Flip dist --";
  }
  const side = value >= 0 ? "Above flip" : "Below flip";
  return `${side} ${formatPercentSigned(value)}`;
}

function formatRatio(value: number | null | undefined) {
  if (value == null) {
    return "--";
  }
  return `${value.toFixed(2)}x`;
}

function regimeAccent(regime: MarketRegime | null | undefined) {
  if (regime === "perfect_convergence_bear") {
    return "down";
  }
  if (regime === "bear_market_rally") {
    return "neutral";
  }
  return "up";
}

function rsiSignalSubLabel(latest: DashboardResponse["market_health"]["rsi_divergence"]["latest"] | null | undefined) {
  if (!latest) {
    return "Normal";
  }
  if (latest.state === "fresh_top_warning") {
    return `Fresh <= ${latest.fresh_bars} bars`;
  }
  if (latest.state === "active_top_warning") {
    return `Active <= ${latest.active_bars} bars`;
  }
  if (latest.state === "invalidated") {
    return "Breakout proved top wrong";
  }
  if (latest.lift_reason === "rsi_reset") {
    return `Lifted: RSI < ${latest.reset_rsi_threshold}`;
  }
  if (latest.lift_reason === "below_ema21") {
    return "Lifted: below daily 21EMA";
  }
  if (latest.lift_reason === "expired") {
    return "Lifted: stale signal";
  }
  return "Lifted";
}

function rsiSignalAccent(
  state: DashboardResponse["market_health"]["rsi_divergence"]["latest"] extends infer T
    ? T extends { state: infer S }
      ? S | null | undefined
      : null | undefined
    : null | undefined,
) {
  if (state === "fresh_top_warning") {
    return "down";
  }
  if (state === "active_top_warning") {
    return "neutral";
  }
  if (state === "invalidated") {
    return "up";
  }
  if (state === "lifted") {
    return "up";
  }
  return "up";
}

function rsiLiftDetail(latest: DashboardResponse["market_health"]["rsi_divergence"]["latest"] | null | undefined) {
  if (!latest) {
    return "";
  }
  if (latest.lift_reason === "below_ema21") {
    return `E active: top warning lifted because price spent enough time below daily 21EMA.`;
  }
  if (latest.lift_reason === "rsi_reset") {
    return `R active: top warning lifted because RSI reset below ${latest.reset_rsi_threshold}.`;
  }
  if (latest.lift_reason === "expired") {
    return "Signal expired: warning aged out of active window.";
  }
  if (latest.state === "invalidated") {
    return "I logic active: price and RSI both pushed above the old top, so divergence failed.";
  }
  if (latest.state === "fresh_top_warning" || latest.state === "active_top_warning") {
    return "No lift yet: warning still active until E or R clears it, or breakout invalidates it.";
  }
  return "";
}

function td9SignalSubLabel(latest: DashboardResponse["market_health"]["bearish_td9"]["latest"] | null | undefined) {
  if (!latest) {
    return "Normal";
  }
  return "Exhaustion watch";
}

const MARKET_MATRIX_CELLS: Array<{
  regime: MarketRegime;
  axis: string;
  title: string;
  copy: string;
  tone: "up" | "neutral" | "down";
}> = [
  {
    regime: "perfect_convergence_bull",
    axis: "Above weekly 21EMA + above daily 21EMA",
    title: "Perfect Convergence (Bull Market)",
    copy: "Trend your friend. Ride wave, hold, or add with discipline.",
    tone: "up",
  },
  {
    regime: "bear_market_rally",
    axis: "Below weekly 21EMA + above daily 21EMA",
    title: "Bear Market Rally",
    copy: "Short-term euphoria inside structural downtrend. Bull-trap risk high.",
    tone: "neutral",
  },
  {
    regime: "healthy_chaos",
    axis: "Above weekly 21EMA + below daily 21EMA",
    title: "Healthy Chaos",
    copy: "Short-term pain inside macro uptrend. Look for controlled buy-the-dip entries.",
    tone: "up",
  },
  {
    regime: "perfect_convergence_bear",
    axis: "Below weekly 21EMA + below daily 21EMA",
    title: "Perfect Convergence (Bear Market)",
    copy: "Maximum chaos. Defense first, cash king until structure improves.",
    tone: "down",
  },
];
