import { useEffect, useState } from "react";
import { LoadingBlock } from "../components/LoadingBlock";
import { fetchJson } from "../lib/api";
import type { DashboardResponse } from "../lib/types";

type MarketRegime =
  | "healthy_chaos"
  | "perfect_convergence_bull"
  | "perfect_convergence_bear"
  | "bear_market_rally";

type WatchlistRow = DashboardResponse["recent_watchlists"][number];

export function DashboardPage() {
  const [dashboard, setDashboard] = useState<DashboardResponse | null>(null);
  const [isLoading, setIsLoading] = useState(true);

  useEffect(() => {
    fetchJson<DashboardResponse>("/api/dashboard").then(setDashboard).catch(() => setDashboard(null)).finally(() => setIsLoading(false));
  }, []);

  const overview = dashboard?.overview ?? null;
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
  const themeDetector = dashboard?.market_health?.theme_detector ?? null;
  const themeLatest = themeDetector?.latest ?? null;
  const recentWatchlists = dashboard?.recent_watchlists ?? [];
  const strategyCards = dashboard?.strategy_cards ?? [];

  const systemStatus = buildSystemStatus(overview);
  const gexPlot = optionsLatest?.plots?.v2 ?? optionsLatest?.plots?.profile ?? optionsLatest?.plots?.absolute ?? null;

  return (
    <div className="page-grid dashboard-page">
      <section className="dashboard-command-shell">
        <div className="dashboard-command-head">
          <div className="dashboard-command-copy">
            <div className="dashboard-terminal-kicker">
              <span className="dashboard-terminal-dot" />
              Market Tape Command Center
            </div>
            <h1>Decision Dashboard</h1>
            <p>
              A terminal-style market posture board driven by persisted snapshots. Read the tape first, decide risk,
              then branch into scans, charts, and watchlists.
            </p>
          </div>
          <div className="dashboard-command-status">
            <StatusTile label="Feed" value={regime?.data_source ? regime.data_source.toUpperCase() : "UNAVAILABLE"} accent="up" />
            <StatusTile label="Artifacts" value={systemStatus.artifactStatus} accent={systemStatus.artifactAccent} />
            <StatusTile label="Database" value={systemStatus.databaseStatus} accent={systemStatus.databaseAccent} />
            <StatusTile label="Last Sync" value={systemStatus.latestSyncLabel} accent="neutral" />
          </div>
        </div>

        {isLoading ? <LoadingBlock label="Loading command board..." compact /> : null}

        <div className="dashboard-grid">
          <article className="dashboard-tile dashboard-span-4 dashboard-regime-tile">
            <TileHeader
              title="Market Regime / SPY Context"
              meta={regimeLatest?.date ? `REF ${regimeLatest.date}` : "Awaiting snapshot"}
              accent={regimeAccent(regimeLatest?.regime)}
            />
            <div className={`dashboard-hero-value accent-${regimeAccent(regimeLatest?.regime)}`}>
              {regimeLatest?.regime_label ?? "UNAVAILABLE"}
            </div>
            <div className="dashboard-hero-subtitle">{regimeLatest?.summary ?? "No SPY market-health data available."}</div>
            <div className="dashboard-callout">
              {regimeLatest?.explanation ?? "Primary trend and short-term pressure will populate here when benchmark history is available."}
            </div>
            <div className="dashboard-mini-grid">
              <MiniStat label="Weekly Trend" value={regimeLatest ? (regimeLatest.weekly_uptrend ? "Bullish" : "Below Trend") : "--"} />
              <MiniStat
                label="ST Pressure"
                value={regimeLatest ? (regimeLatest.daily_downtrend ? "Pullback" : "Constructive") : "--"}
              />
            </div>
            <div className="dashboard-meta-list">
              <div>{regimeLatest ? `Daily ${formatPrice(regimeLatest.daily_close)} vs 21EMA ${formatPrice(regimeLatest.daily_ema21)} (${formatPercentSigned(regimeLatest.daily_distance_pct)})` : "Waiting for market data."}</div>
              <div>{regimeLatest ? `Weekly ${formatPrice(regimeLatest.weekly_close)} vs 21EMA ${formatPrice(regimeLatest.weekly_ema21)} (${formatPercentSigned(regimeLatest.weekly_distance_pct)})` : ""}</div>
            </div>
          </article>

          <article className="dashboard-tile dashboard-span-4">
            <TileHeader
              title="Operational Matrix"
              meta={regimeLatest ? `Posture ${operationalPosture(regimeLatest.regime)}` : "Posture pending"}
              accent={regimeAccent(regimeLatest?.regime)}
            />
            <div className="dashboard-matrix">
              {MARKET_MATRIX_CELLS.map((cell) => {
                const isActive = regimeLatest?.regime === cell.regime;
                return (
                  <div
                    key={cell.regime}
                    className={`dashboard-matrix-cell dashboard-matrix-${cell.tone}${isActive ? " is-active" : ""}`}
                  >
                    <span className="dashboard-matrix-label">{cell.label}</span>
                    <strong>{cell.title}</strong>
                    <div className="dashboard-matrix-value">{isActive ? cell.activeValue : cell.inactiveValue}</div>
                  </div>
                );
              })}
            </div>
            <div className="dashboard-footer-note">
              {regimeLatest ? `Guidance: ${operationalGuidance(regimeLatest.regime)}` : "Use weekly 21EMA as anchor and daily 21EMA as pulse."}
            </div>
          </article>

          <article className="dashboard-tile dashboard-span-4 dashboard-exposure-tile">
            <TileHeader
              title="Exposure Coach"
              meta={exposureLatest ? `Updated ${formatBreadthAge(exposureLatest.latest_data_days_old)}` : "No posture artifact"}
              accent={exposureAccent(exposureLatest?.recommendation)}
            />
            <div className={`dashboard-exposure-value accent-${exposureAccent(exposureLatest?.recommendation)}`}>
              {formatExposureRange(exposureLatest?.exposure_ceiling_pct)}
            </div>
            <div className="dashboard-exposure-label">{normalizeDisplayLabel(exposureLatest?.recommendation ?? "No posture artifact")}</div>
            <div className="dashboard-split-row">
              <div>
                <span className="dashboard-inline-label">Bias</span>
                <strong>{normalizeDisplayLabel(exposureLatest?.bias ?? "--")}</strong>
              </div>
              <div>
                <span className="dashboard-inline-label">Participation</span>
                <strong>{normalizeDisplayLabel(exposureLatest?.participation ?? "--")}</strong>
              </div>
              <div>
                <span className="dashboard-inline-label">Confidence</span>
                <strong>{normalizeDisplayLabel(exposureLatest?.confidence ?? "--")}</strong>
              </div>
            </div>
            <div className="dashboard-kpi-grid">
              <KpiStat label="Composite" value={formatBreadthScore(exposureLatest?.composite_score)} accent="up" />
              <KpiStat label="Breadth" value={formatBreadthScore(exposureLatest?.breadth_score)} accent="neutral" />
              <KpiStat label="Uptrend" value={formatBreadthScore(exposureLatest?.uptrend_score)} accent="neutral" />
              <KpiStat label="Top Risk" value={formatBreadthScore(exposureLatest?.top_risk_score)} accent="down" />
            </div>
            <div className="dashboard-footer-note">{exposureLatest?.rationale ?? "Run Exposure Coach to surface recommended risk ceiling."}</div>
          </article>

          <article className="dashboard-tile dashboard-span-5">
            <TileHeader
              title="Breadth & Participation"
              meta={breadthLatest?.data_date ? `Data ${breadthLatest.data_date}` : "Composite cache"}
              accent={breadthAccent(breadthLatest?.zone_color ?? uptrendLatest?.zone_color)}
            />
            <div className="dashboard-dual-metric">
              <MetricBand
                label={breadthScore?.ticker ?? "S&P 500 Breadth"}
                value={formatCompositeScoreValue(breadthLatest?.composite_score)}
                secondary={breadthLatest?.zone ?? "No breadth artifact"}
                percent={breadthLatest?.composite_score}
                accent={breadthAccent(breadthLatest?.zone_color)}
                detail={
                  breadthLatest
                    ? `${breadthLatest.exposure_guidance ?? "--"} · ${formatBreadthTrend(breadthLatest.trend_direction, breadthLatest.trend_delta, breadthLatest.trend_observations)}`
                    : "Run breadth analyzer and persist its JSON artifact to surface this card."
                }
              />
              <MetricBand
                label={uptrendScore?.ticker ?? "Monty Uptrend Ratio"}
                value={formatCompositeScoreValue(uptrendLatest?.composite_score)}
                secondary={uptrendLatest?.zone_detail ?? uptrendLatest?.zone ?? "No uptrend artifact"}
                percent={uptrendLatest?.composite_score}
                accent={breadthAccent(uptrendLatest?.zone_color)}
                detail={
                  uptrendLatest
                    ? `${formatPercentValue(uptrendLatest.ratio_pct)} · ${formatUptrendTrend(uptrendLatest.trend_direction, uptrendLatest.slope_smoothed, uptrendLatest.acceleration_label)}`
                    : "Run uptrend analyzer and persist its JSON artifact to surface this card."
                }
              />
            </div>
            <div className="dashboard-meta-columns">
              <div>
                <span className="dashboard-inline-label">Breadth</span>
                <div className="dashboard-meta-copy">
                  {breadthLatest
                    ? `Strongest ${breadthLatest.strongest_label ?? "--"} (${formatBreadthScore(breadthLatest.strongest_score)}) · Weakest ${breadthLatest.weakest_label ?? "--"} (${formatBreadthScore(breadthLatest.weakest_score)})`
                    : "No breadth artifact yet."}
                </div>
              </div>
              <div>
                <span className="dashboard-inline-label">Uptrend</span>
                <div className="dashboard-meta-copy">
                  {uptrendLatest
                    ? `Sectors ${formatUptrendSectorBreadth(uptrendLatest.sector_uptrend_count, uptrendLatest.sector_total)} · Cyclical minus defensive ${formatPercentSigned(uptrendLatest.cyclical_minus_defensive_pct)}`
                    : "No uptrend artifact yet."}
                </div>
              </div>
            </div>
            <div className="dashboard-footer-note">
              {breadthLatest?.guidance ?? uptrendLatest?.guidance ?? "Participation guidance appears here when the cached composites are present."}
            </div>
          </article>

          <article className="dashboard-tile dashboard-span-3">
            <TileHeader
              title="IBD Distribution Count"
              meta={ibdLatest?.as_of ? `Window 25D · ${ibdLatest.as_of}` : "Cluster monitor"}
              accent={ibdRiskAccent(ibdLatest?.overall_risk_level)}
            />
            <div className="dashboard-count-grid">
              <CountBox label="SPY Days" value={formatCountBox(ibdLatest?.spy_d25_count)} accent="neutral" />
              <CountBox label="QQQ Days" value={formatCountBox(ibdLatest?.qqq_d25_count)} accent={ibdRiskAccent(ibdLatest?.overall_risk_level)} />
            </div>
            <div className={`dashboard-banner accent-${ibdRiskAccent(ibdLatest?.overall_risk_level)}`}>
              {ibdLatest?.recommended_action ?? "Run IBD Distribution Day Monitor to surface cluster risk."}
            </div>
            <div className="dashboard-meta-list">
              <div>{ibdLatest ? `Primary ${ibdLatest.primary_signal_symbol ?? "--"} · Today DD ${formatBoolLabel(ibdLatest.primary_is_distribution_day_today)}` : ""}</div>
              <div>{ibdLatest ? `Exposure ${formatPercentInt(ibdLatest.current_exposure_pct)} -> ${formatPercentInt(ibdLatest.target_exposure_pct)} · Trail ${formatPercentInt(ibdLatest.trailing_stop_pct)}` : ""}</div>
            </div>
          </article>

          <article className="dashboard-tile dashboard-span-4">
            <TileHeader
              title="Theme Detector"
              meta={themeLatest ? `${themeLatest.total_themes ?? 0} themes` : "RRG leadership"}
              accent={themeAccent(themeLatest?.bullish_count, themeLatest?.bearish_count)}
            />
            <div className="dashboard-theme-stack">
              <ThemeRow label={themeLatest?.top_bullish_name ?? "No bullish leader"} value={formatThemeHeat(themeLatest?.top_bullish_heat)} accent="up" />
              <ThemeRow label={themeLatest?.top_theme_names[1] ?? themeLatest?.top_theme_names[0] ?? "No secondary leader"} value={themeLatest?.top_bullish_stage ?? "--"} accent="neutral" />
              <ThemeRow label={themeLatest?.top_bearish_name ?? "No bearish laggard"} value={formatThemeHeat(themeLatest?.top_bearish_heat)} accent="down" />
            </div>
            <div className="dashboard-meta-list">
              <div>{themeLatest ? `${themeLatest.bullish_count ?? 0} bullish · ${themeLatest.bearish_count ?? 0} bearish · Uptrend sectors ${themeLatest.uptrend_sectors ?? "--"}` : "Run Theme Detector to surface thematic market summary."}</div>
              <div>{themeLatest?.top_theme_names.length ? `Top stack ${themeLatest.top_theme_names.join(" · ")}` : ""}</div>
            </div>
          </article>

          <article className="dashboard-tile dashboard-span-8">
            <TileHeader
              title="Dealer Gamma Exposure (GEX) Matrix"
              meta={optionsLatest ? `${optionsLatest.source_symbol} · ${optionsLatest.as_of}` : "Persisted close snapshot"}
              accent={optionsLatest?.gex_regime === "negative" ? "down" : "up"}
            />
            <div className="dashboard-gex-layout">
              <div className="dashboard-gex-plot">
                {gexPlot ? (
                  <div dangerouslySetInnerHTML={{ __html: gexPlot }} />
                ) : (
                  <div className="dashboard-empty-panel">
                    CBOE-delayed close snapshot loaded from persisted DB summary, not live-fetched on dashboard load.
                  </div>
                )}
              </div>
              <div className="dashboard-gex-stats">
                <KpiStat label="Net GEX" value={formatCompactNumber(optionsLatest?.net_gex)} accent={optionsLatest?.gex_regime === "negative" ? "down" : "up"} />
                <KpiStat label="Flip Level" value={formatPrice(optionsLatest?.gamma_flip)} accent="neutral" />
                <KpiStat label="Call Wall" value={formatPrice(optionsLatest?.call_wall)} accent="up" />
                <KpiStat label="Put Wall" value={formatPrice(optionsLatest?.put_wall)} accent="down" />
                <KpiStat label="Spot" value={formatPrice(optionsLatest?.spot)} accent="neutral" />
                <KpiStat label="ATM Pin" value={formatPrice(optionsLatest?.atm_pin_strike)} accent="neutral" />
              </div>
            </div>
            <div className="dashboard-footer-note">
              {optionsLatest?.summary ?? "No GEX data available."}
              {optionsLatest?.methodology ? ` ${optionsLatest.methodology}` : ""}
            </div>
          </article>

          <div className="dashboard-span-4 dashboard-stack">
            <article className="dashboard-tile dashboard-compact-tile">
              <TileHeader
                title="Tactical RSI (14D)"
                meta={rsiLatest?.signal_date ? `Signal ${rsiLatest.signal_date}` : "No active warning"}
                accent={rsiSignalAccent(rsiLatest?.state)}
              />
              <div className="dashboard-inline-metric">
                <span className="dashboard-inline-number">{rsiLatest?.current_rsi?.toFixed(2) ?? "--"}</span>
                <div className="dashboard-inline-meter">
                  <div className="dashboard-inline-meter-track" />
                  <div
                    className={`dashboard-inline-meter-fill accent-${rsiSignalAccent(rsiLatest?.state)}`}
                    style={{ width: `${clampPercent(rsiLatest?.current_rsi)}` }}
                  />
                </div>
              </div>
              <div className="dashboard-footer-note">
                {rsiLatest?.label ?? "No Signal"} · {rsiSignalSubLabel(rsiLatest)}
              </div>
              <div className="dashboard-meta-copy">{rsiLiftDetail(rsiLatest) || rsiLatest?.explanation || "Daily bearish RSI divergence top using Charles Edwards style pivot logic."}</div>
            </article>

            <article className="dashboard-tile dashboard-compact-tile">
              <TileHeader
                title="TD9 Sequential Count"
                meta={td9Latest?.signal_date ? `Signal ${td9Latest.signal_date}` : "No active exhaustion"}
                accent={td9Latest ? "down" : "up"}
              />
              <div className={`dashboard-sequential-value accent-${td9Latest ? "down" : "up"}`}>{td9Latest?.setup_count ? `${td9Latest.setup_count} SELL` : "NO SIGNAL"}</div>
              <div className="dashboard-footer-note">{td9Latest?.label ?? "Bearish TD Sequential setup on daily bars."}</div>
              <div className="dashboard-step-track">
                {Array.from({ length: 9 }).map((_, index) => (
                  <span key={index} className={`dashboard-step${td9Latest && index < td9Latest.setup_count ? " is-active" : ""}`} />
                ))}
              </div>
            </article>
          </div>

          <article className="dashboard-tile dashboard-span-4">
            <TileHeader
              title="SPY Extension Analysis"
              meta={spyLatest?.time ? `As of ${spyLatest.time}` : "10W trend"}
              accent={spyLatest?.state === "extreme" ? "down" : spyLatest?.state === "warning" ? "neutral" : "up"}
            />
            <MetricBand
              label={`${spyExtension?.ticker ?? "SPY"} vs ${spyExtension?.label ?? "10W SMA"}`}
              value={spyLatest ? `${spyLatest.extension_pct.toFixed(2)}%` : "--"}
              secondary={formatSpyExtensionState(spyLatest?.state)}
              percent={spyLatest?.extension_pct != null ? (spyLatest.extension_pct / (spyExtension?.extreme_pct ?? 15)) * 100 : null}
              accent={spyLatest?.state === "extreme" ? "down" : spyLatest?.state === "warning" ? "neutral" : "up"}
              detail={
                spyLatest
                  ? `Close ${formatPrice(spyLatest.close)} · MA ${formatPrice(spyLatest.moving_average)} · Dist ${formatPrice(spyLatest.distance)}`
                  : "No SPY extension data available."
              }
            />
            <div className="dashboard-meta-list">
              <div>{spyExtension ? `Warning ${spyExtension.warning_pct.toFixed(1)}% · Extreme ${spyExtension.extreme_pct.toFixed(1)}%` : ""}</div>
            </div>
          </article>

          <article className="dashboard-tile dashboard-span-4">
            <TileHeader
              title="System Snapshot"
              meta={overview?.latest_sync_at ? `Synced ${formatDateTimeCompact(overview.latest_sync_at)}` : "Artifact-driven"}
              accent={systemStatus.databaseAccent}
            />
            <div className="dashboard-system-grid">
              <MiniStat label="Database" value={systemStatus.databaseStatus} />
              <MiniStat label="Artifacts" value={systemStatus.artifactStatus} />
              <MiniStat label="Runs" value={overview?.screen_run_count != null ? String(overview.screen_run_count) : "--"} />
              <MiniStat label="Sync" value={overview?.latest_sync_at ? formatDateTimeCompact(overview.latest_sync_at) : "--"} />
            </div>
            <div className="dashboard-module-list">
              {strategyCards.length > 0 ? (
                strategyCards.map((card) => (
                  <span key={card.id} className="dashboard-module-chip">
                    {card.label}
                  </span>
                ))
              ) : (
                <span className="dashboard-module-chip">No strategy cards</span>
              )}
            </div>
            <div className="dashboard-footer-note">Dashboard remains DB/artifact first. When a snapshot is missing, modules degrade to cached truth or explicit unavailable state.</div>
          </article>

          <article className="dashboard-tile dashboard-span-4">
            <TileHeader
              title="Recent Watchlists"
              meta={recentWatchlists.length ? `${recentWatchlists.length} latest artifacts` : "No recent artifacts"}
              accent="neutral"
            />
            <div className="dashboard-watchlist-table">
              {recentWatchlists.length > 0 ? (
                recentWatchlists.slice(0, 6).map((item) => <WatchlistTableRow key={item.path} item={item} />)
              ) : (
                <div className="dashboard-empty-panel">No recent watchlists available.</div>
              )}
            </div>
          </article>
        </div>
      </section>
    </div>
  );
}

function TileHeader({
  title,
  meta,
  accent,
}: {
  title: string;
  meta: string;
  accent: "up" | "neutral" | "down";
}) {
  return (
    <div className="dashboard-tile-head">
      <span className="dashboard-tile-title">{title}</span>
      <span className={`dashboard-tile-meta accent-${accent}`}>{meta}</span>
    </div>
  );
}

function MiniStat({ label, value }: { label: string; value: string }) {
  return (
    <div className="dashboard-mini-stat">
      <span>{label}</span>
      <strong>{value}</strong>
    </div>
  );
}

function KpiStat({
  label,
  value,
  accent,
}: {
  label: string;
  value: string;
  accent: "up" | "neutral" | "down";
}) {
  return (
    <div className={`dashboard-kpi-card accent-${accent}`}>
      <span>{label}</span>
      <strong>{value}</strong>
    </div>
  );
}

function CountBox({
  label,
  value,
  accent,
}: {
  label: string;
  value: string;
  accent: "up" | "neutral" | "down";
}) {
  return (
    <div className={`dashboard-count-box accent-${accent}`}>
      <strong>{value}</strong>
      <span>{label}</span>
    </div>
  );
}

function StatusTile({
  label,
  value,
  accent,
}: {
  label: string;
  value: string;
  accent: "up" | "neutral" | "down";
}) {
  return (
    <div className={`dashboard-status-tile accent-${accent}`}>
      <span>{label}</span>
      <strong>{value}</strong>
    </div>
  );
}

function ThemeRow({
  label,
  value,
  accent,
}: {
  label: string;
  value: string;
  accent: "up" | "neutral" | "down";
}) {
  return (
    <div className={`dashboard-theme-row accent-${accent}`}>
      <span>{label}</span>
      <strong>{value}</strong>
    </div>
  );
}

function MetricBand({
  label,
  value,
  secondary,
  percent,
  accent,
  detail,
}: {
  label: string;
  value: string;
  secondary: string;
  percent: number | null | undefined;
  accent: "up" | "neutral" | "down";
  detail: string;
}) {
  return (
    <div className="dashboard-band">
      <div className="dashboard-band-head">
        <span>{label}</span>
        <strong>{value}</strong>
      </div>
      <div className="dashboard-band-track">
        <div className={`dashboard-band-fill accent-${accent}`} style={{ width: `${clampPercent(percent)}` }} />
      </div>
      <div className="dashboard-band-foot">
        <span>{secondary}</span>
        <span>{detail}</span>
      </div>
    </div>
  );
}

function WatchlistTableRow({ item }: { item: WatchlistRow }) {
  return (
    <div className="dashboard-watchlist-row">
      <div>
        <strong>{item.name}</strong>
        <span>{item.group_label}</span>
      </div>
      <div>
        <strong>{item.sort_date ?? "--"}</strong>
        <span>{formatDateTimeCompact(item.captured_at)}</span>
      </div>
    </div>
  );
}

function buildSystemStatus(overview: DashboardResponse["overview"] | null) {
  const databaseStatus = overview?.database_configured ? "CONNECTED" : "OFFLINE";
  const artifactStatus = overview?.artifacts_dir ? "REACHABLE" : "UNKNOWN";
  return {
    databaseStatus,
    artifactStatus,
    databaseAccent: overview?.database_configured ? ("up" as const) : ("down" as const),
    artifactAccent: overview?.artifacts_dir ? ("up" as const) : ("neutral" as const),
    latestSyncLabel: overview?.latest_sync_at ? formatDateTimeCompact(overview.latest_sync_at) : "PENDING",
  };
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

function formatCompositeScoreValue(value: number | null | undefined) {
  return value == null ? "--" : `${value.toFixed(1)}/100`;
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

function formatPercentValue(value: number | null | undefined) {
  return value == null ? "--" : `${value.toFixed(1)}%`;
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

function formatThemeHeat(value: number | null | undefined) {
  if (value == null || !Number.isFinite(value)) {
    return "--";
  }
  return `${value.toFixed(1)} heat`;
}

function themeAccent(bullishCount: number | null | undefined, bearishCount: number | null | undefined) {
  const bulls = bullishCount ?? 0;
  const bears = bearishCount ?? 0;
  if (bulls > bears) {
    return "up";
  }
  if (bears > bulls) {
    return "down";
  }
  return "neutral";
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
    return "E active: warning lifted after enough closes below daily 21EMA.";
  }
  if (latest.lift_reason === "rsi_reset") {
    return `R active: warning lifted because RSI reset below ${latest.reset_rsi_threshold}.`;
  }
  if (latest.lift_reason === "expired") {
    return "Signal expired: warning aged out of active window.";
  }
  if (latest.state === "invalidated") {
    return "I logic active: price and RSI both pushed above the old top, so divergence failed.";
  }
  if (latest.state === "fresh_top_warning" || latest.state === "active_top_warning") {
    return "No lift yet: warning remains active until E or R clears it, or breakout invalidates it.";
  }
  return "";
}

function normalizeDisplayLabel(value: string) {
  return value.replace(/_/g, " ");
}

function formatExposureRange(exposureCeilingPct: number | null | undefined) {
  if (exposureCeilingPct == null) {
    return "--";
  }
  if (exposureCeilingPct >= 100) {
    return "75-100%";
  }
  if (exposureCeilingPct >= 75) {
    return "50-75%";
  }
  if (exposureCeilingPct >= 50) {
    return "25-50%";
  }
  return `0-${exposureCeilingPct}%`;
}

function operationalPosture(regime: MarketRegime) {
  if (regime === "perfect_convergence_bull") {
    return "ACTIVE";
  }
  if (regime === "healthy_chaos") {
    return "SELECTIVE";
  }
  if (regime === "bear_market_rally") {
    return "CAUTION";
  }
  return "DEFENSIVE";
}

function operationalGuidance(regime: MarketRegime) {
  if (regime === "perfect_convergence_bull") {
    return "Aggressive buying permitted when setups confirm.";
  }
  if (regime === "healthy_chaos") {
    return "Controlled dip buying only while macro structure stays intact.";
  }
  if (regime === "bear_market_rally") {
    return "Treat strength as tactical, not structural.";
  }
  return "Defense first. Cash and risk reduction take priority.";
}

function clampPercent(value: number | null | undefined) {
  if (value == null || !Number.isFinite(value)) {
    return 0;
  }
  return Math.max(0, Math.min(100, value));
}

function formatDateTimeCompact(value: string) {
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return value;
  }
  return new Intl.DateTimeFormat(undefined, {
    month: "short",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
  }).format(date);
}

function formatCountBox(value: number | null | undefined) {
  return value == null ? "--" : String(value);
}

const MARKET_MATRIX_CELLS: Array<{
  regime: MarketRegime;
  label: string;
  title: string;
  activeValue: string;
  inactiveValue: string;
  tone: "up" | "neutral" | "down";
}> = [
  {
    regime: "perfect_convergence_bull",
    label: "Accumulation",
    title: "Above weekly 21EMA + above daily 21EMA",
    activeValue: "RUN",
    inactiveValue: "WAIT",
    tone: "up",
  },
  {
    regime: "healthy_chaos",
    label: "Constructive Pullback",
    title: "Above weekly 21EMA + below daily 21EMA",
    activeValue: "BUY DIP",
    inactiveValue: "WAIT",
    tone: "up",
  },
  {
    regime: "bear_market_rally",
    label: "Bear Rally",
    title: "Below weekly 21EMA + above daily 21EMA",
    activeValue: "TRAP",
    inactiveValue: "VOID",
    tone: "neutral",
  },
  {
    regime: "perfect_convergence_bear",
    label: "Markdown",
    title: "Below weekly 21EMA + below daily 21EMA",
    activeValue: "DEFEND",
    inactiveValue: "VOID",
    tone: "down",
  },
];
