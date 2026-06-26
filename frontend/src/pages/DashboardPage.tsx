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
  const breadthScore = dashboard?.market_health?.breadth_score ?? null;
  const breadthLatest = breadthScore?.latest ?? null;
  const uptrendScore = dashboard?.market_health?.uptrend_score ?? null;
  const uptrendLatest = uptrendScore?.latest ?? null;
  const ibdDistribution = dashboard?.market_health?.ibd_distribution ?? null;
  const ibdLatest = ibdDistribution?.latest ?? null;
  const exposurePosture = dashboard?.market_health?.exposure_posture ?? null;
  const exposureLatest = exposurePosture?.latest ?? null;

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
              <h3>{breadthScore?.ticker ?? "S&P 500 Breadth"}</h3>
              <span className={`accent-mark accent-${breadthAccent(breadthLatest?.zone_color)}`} />
            </div>
            <p className="card-meta">TraderMonty 6-component breadth composite from persisted artifact cache.</p>
            <div className="metric-value">
              {breadthLatest?.composite_score != null ? `${breadthLatest.composite_score.toFixed(1)}/100` : "Unavailable"}{" "}
              <span>{breadthLatest?.zone ?? "No breadth artifact"}</span>
            </div>
            <p className="card-meta">
              {breadthLatest
                ? `Exposure ${breadthLatest.exposure_guidance ?? "--"} · Trend ${formatBreadthTrend(breadthLatest.trend_direction, breadthLatest.trend_delta, breadthLatest.trend_observations)}`
                : "Run breadth analyzer and persist its JSON artifact to surface this card."}
            </p>
            <p className="card-meta">
              {breadthLatest
                ? `Strongest ${breadthLatest.strongest_label ?? "--"} (${formatBreadthScore(breadthLatest.strongest_score)}) · Weakest ${breadthLatest.weakest_label ?? "--"} (${formatBreadthScore(breadthLatest.weakest_score)})`
                : ""}
            </p>
            <p className="card-meta">
              {breadthLatest
                ? `${breadthLatest.data_quality_label ?? "Data quality unavailable"}${formatBreadthComponentCoverage(breadthLatest.available_components, breadthLatest.total_components)}`
                : ""}
            </p>
            <p className="card-meta">{breadthLatest?.guidance ?? ""}</p>
            <p className="card-meta">
              {breadthLatest?.freshness_warning
                ? breadthLatest.freshness_warning
                : breadthLatest
                  ? `Data date ${breadthLatest.data_date ?? "--"} · ${formatBreadthAge(breadthLatest.latest_data_days_old)}`
                  : ""}
              {breadthScore?.data_source ? ` · Source ${breadthScore.data_source}` : ""}
            </p>
          </article>

          <article className="metric-card">
            <div className="metric-card-head">
              <h3>{uptrendScore?.ticker ?? "Monty Uptrend Ratio"}</h3>
              <span className={`accent-mark accent-${breadthAccent(uptrendLatest?.zone_color)}`} />
            </div>
            <p className="card-meta">Monty uptrend ratio composite from persisted artifact cache.</p>
            <div className="metric-value">
              {uptrendLatest?.composite_score != null ? `${uptrendLatest.composite_score.toFixed(1)}/100` : "Unavailable"}{" "}
              <span>{uptrendLatest?.zone_detail ?? uptrendLatest?.zone ?? "No uptrend artifact"}</span>
            </div>
            <p className="card-meta">
              {uptrendLatest
                ? `Exposure ${uptrendLatest.exposure_guidance ?? "--"} · Ratio ${formatPercentValue(uptrendLatest.ratio_pct)} · Trend ${formatUptrendTrend(uptrendLatest.trend_direction, uptrendLatest.slope_smoothed, uptrendLatest.acceleration_label)}`
                : "Run uptrend analyzer and persist its JSON artifact to surface this card."}
            </p>
            <p className="card-meta">
              {uptrendLatest
                ? `Sectors up ${formatUptrendSectorBreadth(uptrendLatest.sector_uptrend_count, uptrendLatest.sector_total)} · Cyclical minus defensive ${formatPercentSigned(uptrendLatest.cyclical_minus_defensive_pct)} · Historical ${formatPercentile(uptrendLatest.historical_percentile)}`
                : ""}
            </p>
            <p className="card-meta">
              {uptrendLatest
                ? `Strongest ${uptrendLatest.strongest_label ?? "--"} (${formatBreadthScore(uptrendLatest.strongest_score)}) · Weakest ${uptrendLatest.weakest_label ?? "--"} (${formatBreadthScore(uptrendLatest.weakest_score)})`
                : ""}
            </p>
            <p className="card-meta">
              {uptrendLatest
                ? `${uptrendLatest.data_quality_label ?? "Data quality unavailable"}${formatBreadthComponentCoverage(uptrendLatest.available_components, uptrendLatest.total_components)} · Confidence ${uptrendLatest.confidence_label ?? "--"}`
                : ""}
            </p>
            <p className="card-meta">{formatUptrendWarnings(uptrendLatest?.warning_labels, uptrendLatest?.warning_penalty)}</p>
            <p className="card-meta">{uptrendLatest?.guidance ?? ""}</p>
            <p className="card-meta">
              {uptrendLatest ? `Data date ${uptrendLatest.data_date ?? "--"} · ${formatBreadthAge(uptrendLatest.latest_data_days_old)}` : ""}
              {uptrendScore?.data_source ? ` · Source ${uptrendScore.data_source}` : ""}
            </p>
          </article>

          <article className="metric-card">
            <div className="metric-card-head">
              <h3>{exposurePosture?.ticker ?? "Exposure Coach"}</h3>
              <span className={`accent-mark accent-${exposureAccent(exposureLatest?.recommendation)}`} />
            </div>
            <p className="card-meta">Synthesized posture from latest breadth, uptrend, and top-risk style signals.</p>
            <div className="metric-value">
              {exposureLatest?.exposure_ceiling_pct != null ? `${exposureLatest.exposure_ceiling_pct}%` : "Unavailable"}{" "}
              <span>{exposureLatest?.recommendation ?? "No posture artifact"}</span>
            </div>
            <p className="card-meta">
              {exposureLatest
                ? `Bias ${exposureLatest.bias ?? "--"} · Participation ${exposureLatest.participation ?? "--"} · Confidence ${exposureLatest.confidence ?? "--"}`
                : "Run Exposure Coach to surface recommended exposure ceiling."}
            </p>
            <p className="card-meta">
              {exposureLatest
                ? `Composite ${formatBreadthScore(exposureLatest.composite_score)} · Breadth ${formatBreadthScore(exposureLatest.breadth_score)} · Uptrend ${formatBreadthScore(exposureLatest.uptrend_score)} · Top Risk ${formatBreadthScore(exposureLatest.top_risk_score)}`
                : ""}
            </p>
            <p className="card-meta">
              {exposureLatest
                ? `Inputs ${formatExposureInputs(exposureLatest.inputs_provided, exposureLatest.inputs_missing)}`
                : ""}
            </p>
            <p className="card-meta">{exposureLatest?.rationale ?? ""}</p>
            <p className="card-meta">
              {exposureLatest ? `${formatBreadthAge(exposureLatest.latest_data_days_old)}` : ""}
              {exposurePosture?.data_source ? ` · Source ${exposurePosture.data_source}` : ""}
            </p>
          </article>

          <article className="metric-card">
            <div className="metric-card-head">
              <h3>{ibdDistribution?.ticker ?? "IBD Distribution Day Monitor"}</h3>
              <span className={`accent-mark accent-${ibdRiskAccent(ibdLatest?.overall_risk_level)}`} />
            </div>
            <p className="card-meta">QQQ/SPY distribution-day cluster monitor with exposure action.</p>
            <div className="metric-value">
              {ibdLatest?.overall_risk_level ?? "Unavailable"} <span>{ibdLatest?.recommended_action ?? "No IBD artifact"}</span>
            </div>
            <p className="card-meta">
              {ibdLatest
                ? `QQQ d5/d15/d25 ${formatIbdCluster(ibdLatest.qqq_d5_count, ibdLatest.qqq_d15_count, ibdLatest.qqq_d25_count)} · SPY ${formatIbdCluster(ibdLatest.spy_d5_count, ibdLatest.spy_d15_count, ibdLatest.spy_d25_count)}`
                : "Run IBD Distribution Day Monitor to surface cluster risk."}
            </p>
            <p className="card-meta">
              {ibdLatest
                ? `Primary ${ibdLatest.primary_signal_symbol ?? "--"} · Today DD ${formatBoolLabel(ibdLatest.primary_is_distribution_day_today)} · Below 21EMA/50SMA ${formatBoolLabel(ibdLatest.market_below_21ema_or_50ma)}`
                : ""}
            </p>
            <p className="card-meta">
              {ibdLatest
                ? `Exposure ${formatPercentInt(ibdLatest.current_exposure_pct)} -> ${formatPercentInt(ibdLatest.target_exposure_pct)} · Trail ${formatPercentInt(ibdLatest.trailing_stop_pct)}`
                : ""}
            </p>
            <p className="card-meta">
              {ibdLatest
                ? `${ibdLatest.alternative_action ? `Alt ${ibdLatest.alternative_action} · ` : ""}${formatAuditFlags(ibdLatest.audit_flags)}`
                : ""}
            </p>
            <p className="card-meta">{ibdLatest?.rationale ?? ""}</p>
            <p className="card-meta">
              {ibdLatest ? `As of ${ibdLatest.as_of ?? "--"} · ${formatBreadthAge(ibdLatest.latest_data_days_old)}` : ""}
              {ibdDistribution?.data_source ? ` · Source ${ibdDistribution.data_source}` : ""}
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

          {optionsLatest?.plots?.v2 ? (
            <article className="metric-card metric-card-wide">
              <div className="metric-card-head">
                <h3>{optionsPositioning?.ticker ?? "SPX"} GEX Plot Chart V2</h3>
                <span className={`accent-mark accent-${optionsLatest.gex_regime === "negative" ? "down" : "up"}`} />
              </div>
              <p className="card-meta">Explainer-style net GEX by strike with gamma flip, call wall, and put wall.</p>
              <div className="gex-v2-card" dangerouslySetInnerHTML={{ __html: optionsLatest.plots.v2 }} />
            </article>
          ) : null}

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

function formatBreadthScore(value: number | null | undefined) {
  return value == null ? "--" : `${value.toFixed(0)}/100`;
}

function formatBreadthTrend(direction: string | null | undefined, delta: number | null | undefined, observations: number) {
  if (!direction) {
    return "Unavailable";
  }
  const deltaLabel = delta == null ? "--" : `${delta > 0 ? "+" : ""}${delta.toFixed(1)}`;
  return `${direction} (${deltaLabel}, ${observations} obs)`;
}

function formatBreadthAge(daysOld: number | null | undefined) {
  if (daysOld == null) {
    return "Age unknown";
  }
  return `${daysOld} day${daysOld === 1 ? "" : "s"} old`;
}

function formatBreadthComponentCoverage(available: number | null | undefined, total: number | null | undefined) {
  if (available == null || total == null) {
    return "";
  }
  return ` (${available}/${total})`;
}

function formatPercentValue(value: number | null | undefined) {
  return value == null ? "--" : `${value.toFixed(1)}%`;
}

function formatPercentile(value: number | null | undefined) {
  return value == null ? "--" : `${value.toFixed(1)}th pct`;
}

function formatUptrendTrend(
  direction: string | null | undefined,
  slopeSmoothed: number | null | undefined,
  accelerationLabel: string | null | undefined,
) {
  if (!direction) {
    return "Unavailable";
  }
  const slopeLabel = slopeSmoothed == null ? "--" : `${slopeSmoothed > 0 ? "+" : ""}${slopeSmoothed.toFixed(4)}`;
  const accelLabel = accelerationLabel ? accelerationLabel.replace(/_/g, " ") : "--";
  return `${direction} (${slopeLabel}, ${accelLabel})`;
}

function formatUptrendSectorBreadth(upCount: number | null | undefined, total: number | null | undefined) {
  if (upCount == null || total == null) {
    return "--";
  }
  return `${upCount}/${total}`;
}

function formatUptrendWarnings(labels: string[] | null | undefined, warningPenalty: number | null | undefined) {
  if (!labels || labels.length === 0) {
    return "";
  }
  const penaltyLabel = warningPenalty == null || warningPenalty === 0 ? "" : ` · Penalty ${warningPenalty}`;
  return `Warnings ${labels.join(", ")}${penaltyLabel}`;
}

function formatExposureInputs(provided: string[] | null | undefined, missing: string[] | null | undefined) {
  const providedLabel = provided && provided.length > 0 ? provided.join(", ") : "--";
  const missingLabel = missing && missing.length > 0 ? missing.join(", ") : "none";
  return `Provided ${providedLabel} · Missing ${missingLabel}`;
}

function formatIbdCluster(
  d5: number | null | undefined,
  d15: number | null | undefined,
  d25: number | null | undefined,
) {
  return `${d5 ?? "--"}/${d15 ?? "--"}/${d25 ?? "--"}`;
}

function formatBoolLabel(value: boolean | null | undefined) {
  if (value == null) {
    return "--";
  }
  return value ? "yes" : "no";
}

function formatPercentInt(value: number | null | undefined) {
  return value == null ? "--" : `${value}%`;
}

function formatAuditFlags(flags: string[] | null | undefined) {
  if (!flags || flags.length === 0) {
    return "Audit clean";
  }
  return `Audit ${flags.join(", ")}`;
}

function exposureAccent(recommendation: string | null | undefined) {
  if (recommendation === "NEW_ENTRY_ALLOWED") {
    return "up";
  }
  if (recommendation === "REDUCE_ONLY") {
    return "neutral";
  }
  if (recommendation === "CASH_PRIORITY") {
    return "down";
  }
  return "neutral";
}

function ibdRiskAccent(riskLevel: string | null | undefined) {
  if (riskLevel === "NORMAL") {
    return "up";
  }
  if (riskLevel === "CAUTION") {
    return "neutral";
  }
  if (riskLevel === "HIGH" || riskLevel === "SEVERE") {
    return "down";
  }
  return "neutral";
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

function breadthAccent(zoneColor: string | null | undefined) {
  if (zoneColor === "green" || zoneColor === "blue") {
    return "up";
  }
  if (zoneColor === "yellow") {
    return "neutral";
  }
  if (zoneColor === "orange" || zoneColor === "red") {
    return "down";
  }
  return "neutral";
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
