import { useEffect, useMemo, useState } from "react";
import { Link, useParams } from "react-router-dom";
import { LoadingBlock } from "../components/LoadingBlock";
import { PriceChart, type ChartVisibility } from "../components/PriceChart";
import { RebasedComparisonChart } from "../components/RebasedComparisonChart";
import { ScannerMiniChart } from "../components/ScannerMiniChart";
import { fetchJson } from "../lib/api";
import { formatLocalDate, formatLocalDateTime } from "../lib/format";
import type { CandlePoint, SectorLeaderboardHolding, SectorLeaderboardResponse, SectorLeaderboardRow, WatchlistChartResponse } from "../lib/types";

type ViewMode = "list" | "chart";
type HoldingViewMode = "list" | "chart";
type HoldingSortKey = "weight" | "dailyRs" | "weeklyRs" | "leadership" | "rsDays" | "redRsDays" | "dcr" | "relVol" | "ticker" | "change";
type SortDirection = "asc" | "desc";

const BENCHMARK_TICKER = "SPY";
const HOLDING_CHART_LIMIT = 40;

const CHART_VISIBILITY: ChartVisibility = {
  ema8: true,
  ema21: true,
  sma50: true,
  sma200: true,
  bollingerBands: false,
  weeklyEma8: true,
  ipoVwap: false,
  anchoredVwap52wLow: false,
  marketExtension: true,
  fibOverlay: false,
  gapZones: true,
  htfBox: false,
  rsLine: false,
  rsSignals: false,
  sellSignals: false,
  wyckoffSignals: false,
  wyckoffHoldSignals: false,
  flexSr: false,
  channelLines: false,
};

export function SectorLeaderboardPage() {
  const { ticker } = useParams();
  const requestedTicker = (ticker ?? "").trim().toUpperCase();
  const [payload, setPayload] = useState<SectorLeaderboardResponse | null>(null);
  const [chartPayloads, setChartPayloads] = useState<Record<string, WatchlistChartResponse | null | undefined>>({});
  const [chartErrors, setChartErrors] = useState<Record<string, string>>({});
  const [chartLoadingTickers, setChartLoadingTickers] = useState<Record<string, boolean>>({});
  const [isLoading, setIsLoading] = useState(true);
  const [notice, setNotice] = useState("");
  const [viewMode, setViewMode] = useState<ViewMode>("list");
  const [holdingViewMode, setHoldingViewMode] = useState<HoldingViewMode>("list");
  const [holdingSortBy, setHoldingSortBy] = useState<HoldingSortKey>("weight");
  const [holdingSortDirection, setHoldingSortDirection] = useState<SortDirection>("desc");

  useEffect(() => {
    setIsLoading(true);
    setNotice("");
    void fetchJson<SectorLeaderboardResponse>("/api/sector-leaderboard")
      .then(setPayload)
      .catch((error) => {
        setPayload(null);
        setNotice(error instanceof Error ? error.message : "Failed to load sector leaderboard.");
      })
      .finally(() => setIsLoading(false));
  }, []);

  const rows = payload?.rows ?? [];
  const detailRow = requestedTicker ? rows.find((row) => row.ticker === requestedTicker) ?? null : null;
  const chartTickers = useMemo(() => {
    if (requestedTicker && detailRow) {
      const holdingChartTickers = holdingViewMode === "chart"
        ? [...detailRow.top_holdings]
            .sort((left, right) => compareHoldings(left, right, holdingSortBy, holdingSortDirection))
            .slice(0, HOLDING_CHART_LIMIT)
            .map((holding) => holding.ticker)
        : [];
      return [detailRow.ticker, ...holdingChartTickers];
    }
    if (!requestedTicker && viewMode === "chart") {
      return [...rows.map((row) => row.ticker), BENCHMARK_TICKER];
    }
    return [];
  }, [detailRow, holdingSortBy, holdingSortDirection, holdingViewMode, requestedTicker, rows, viewMode]);
  const chartTickerKey = chartTickers.join("|");

  useEffect(() => {
    if (chartTickers.length === 0) {
      return;
    }
    const missingTickers = chartTickers.filter((symbol) => chartPayloads[symbol] === undefined && !chartLoadingTickers[symbol]);
    if (missingTickers.length === 0) {
      return;
    }
    let ignore = false;
    setChartLoadingTickers((current) => {
      const next = { ...current };
      for (const symbol of missingTickers) {
        next[symbol] = true;
      }
      return next;
    });
    void Promise.allSettled(
      missingTickers.map(async (symbol) => {
        const payload = await fetchJson<WatchlistChartResponse>(`/api/charts/${symbol}?period=18mo`);
        return { ticker: symbol, payload };
      }),
    ).then((results) => {
      if (ignore) {
        return;
      }
      setChartPayloads((current) => {
        const next = { ...current };
        for (const result of results) {
          if (result.status === "fulfilled") {
            next[result.value.ticker] = result.value.payload;
          }
        }
        return next;
      });
      setChartErrors((current) => {
        const next = { ...current };
        results.forEach((result, index) => {
          if (result.status === "fulfilled") {
            delete next[result.value.ticker];
            return;
          }
          const failedTicker = missingTickers[index];
          next[failedTicker] = result.reason instanceof Error ? result.reason.message : "Failed to load chart.";
        });
        return next;
      });
      setChartLoadingTickers((current) => {
        const next = { ...current };
        for (const symbol of missingTickers) {
          delete next[symbol];
        }
        return next;
      });
    });
    return () => {
      ignore = true;
    };
  }, [chartTickerKey]);

  if (isLoading) {
    return <LoadingBlock label="Loading sector leaderboard..." />;
  }

  if (requestedTicker) {
    return (
      <SectorDetailPage
        row={detailRow}
        requestedTicker={requestedTicker}
        chartPayload={chartPayloads[requestedTicker]}
        chartError={chartErrors[requestedTicker]}
        isChartLoading={Boolean(chartLoadingTickers[requestedTicker])}
        holdingViewMode={holdingViewMode}
        onHoldingViewModeChange={setHoldingViewMode}
        holdingSortBy={holdingSortBy}
        holdingSortDirection={holdingSortDirection}
        chartPayloads={chartPayloads}
        chartErrors={chartErrors}
        chartLoadingTickers={chartLoadingTickers}
        onSortHoldings={(key) => {
          if (holdingSortBy === key) {
            setHoldingSortDirection((current) => (current === "asc" ? "desc" : "asc"));
          } else {
            setHoldingSortBy(key);
            setHoldingSortDirection(key === "ticker" ? "asc" : "desc");
          }
        }}
      />
    );
  }

  return (
    <div className="page-grid sector-leaderboard-page">
      <SectorHeader payload={payload} rows={rows} viewMode={viewMode} onViewModeChange={setViewMode} />

      {notice ? <div className="notice-banner">{notice}</div> : null}

      {viewMode === "list" ? (
        <SectorLeaderboardTable rows={rows} />
      ) : (
        <SectorChartGrid
          rows={rows}
          chartPayloads={chartPayloads}
          chartErrors={chartErrors}
          chartLoadingTickers={chartLoadingTickers}
          benchmarkPayload={chartPayloads[BENCHMARK_TICKER]}
          benchmarkError={chartErrors[BENCHMARK_TICKER]}
          isBenchmarkLoading={Boolean(chartLoadingTickers[BENCHMARK_TICKER])}
        />
      )}
    </div>
  );
}

function SectorHeader({
  payload,
  rows,
  viewMode,
  onViewModeChange,
}: {
  payload: SectorLeaderboardResponse | null;
  rows: SectorLeaderboardRow[];
  viewMode: ViewMode;
  onViewModeChange: (mode: ViewMode) => void;
}) {
  return (
    <>
      <section className="scanner-result-hero panel">
        <div className="scanner-result-breadcrumbs">
          <Link to="/">Dashboard</Link>
          <span>›</span>
          <span>Sector Leaderboard</span>
        </div>
        <div className="scanner-result-title-row">
          <div>
            <span className="scanner-result-kicker">ETF Rotation</span>
            <h1>U.S. ETF Sector Leaderboard</h1>
          </div>
          <span className={`scanner-result-status${rows.length > 0 ? " is-live" : ""}`}>{rows.length > 0 ? "Data Ready" : "No Data"}</span>
        </div>
        <p className="scanner-result-copy">
          Sector and industry ETF leaderboard ranked by daily performance, with cached OHLCV metrics and top holdings from the
          default State Street ETF catalog.
        </p>
        <SectorMetricStrip payload={payload} rows={rows} />
      </section>

      <section className="scanner-result-filter-grid sector-leaderboard-controls">
        <div className="scanner-result-filter panel scanner-result-filter-actions">
          <span className="eyebrow">Views</span>
          <div className="scanner-result-view-actions" role="tablist" aria-label="Sector leaderboard view">
            <button
              className={`scanner-result-view-chip${viewMode === "list" ? " is-active" : ""}`}
              type="button"
              onClick={() => onViewModeChange("list")}
            >
              List
            </button>
            <button
              className={`scanner-result-view-chip${viewMode === "chart" ? " is-active" : ""}`}
              type="button"
              onClick={() => onViewModeChange("chart")}
            >
              Charts
            </button>
          </div>
        </div>
        <div className="scanner-result-filter panel scanner-result-filter-actions">
          <span className="eyebrow">Page Data</span>
          <div className="scanner-result-view-actions">
            <button className="ghost-button sector-refresh-button" type="button" onClick={() => window.location.reload()}>
              Refresh
            </button>
          </div>
        </div>
      </section>
    </>
  );
}

function SectorMetricStrip({ payload, rows }: { payload: SectorLeaderboardResponse | null; rows: SectorLeaderboardRow[] }) {
  return (
    <div className="scanner-result-metrics">
      <div className="scanner-result-metric">
        <span className="eyebrow">Latest Data</span>
        <strong>{formatLocalDate(payload?.latest_data_date)}</strong>
      </div>
      <div className="scanner-result-metric">
        <span className="eyebrow">ETF Catalog</span>
        <strong>{rows.length} ETFs</strong>
      </div>
      <div className="scanner-result-metric">
        <span className="eyebrow">Holdings Cache</span>
        <strong>{formatLocalDateTime(payload?.source.holdings_cache_generated_at)}</strong>
      </div>
      <div className="scanner-result-metric">
        <span className="eyebrow">Source</span>
        <strong>
          <a href={payload?.source.fund_finder_url} target="_blank" rel="noreferrer">
            SSGA
          </a>
        </strong>
      </div>
    </div>
  );
}

function SectorLeaderboardTable({ rows }: { rows: SectorLeaderboardRow[] }) {
  return (
    <section className="scanner-result-table-shell panel">
      <div className="data-table-responsive sector-table-wrap">
        <table className="data-table sector-leaderboard-table">
          <thead>
            <tr>
              <th>Ticker</th>
              <th>Description</th>
              <th>Price</th>
              <th>DY%</th>
              <th>WK%</th>
              <th>MO%</th>
              <th>1Y%</th>
              <th>RS 1M</th>
              <th>RS 3M</th>
              <th>RS Mom</th>
              <th>RS Days</th>
              <th>Red RS</th>
              <th>DCR</th>
              <th>RS NH</th>
              <th>Vol+</th>
              <th>ATR%</th>
              <th>Avg Vol</th>
              <th>Rel Vol</th>
              <th>Top Holdings</th>
            </tr>
          </thead>
          <tbody>
            {rows.map((row) => (
              <tr key={row.ticker}>
                <td data-label="Ticker">
                  <Link className="scanner-result-symbol" to={`/sector-leaderboard/${encodeURIComponent(row.ticker)}`}>
                    <span>{row.ticker}</span>
                  </Link>
                </td>
                <td data-label="Description">{row.description}</td>
                <td data-label="Price">{formatMoney(row.price)}</td>
                <td data-label="DY%" className={valueClass(row.day_change_pct)}>{formatPercent(row.day_change_pct)}</td>
                <td data-label="WK%" className={valueClass(row.week_change_pct)}>{formatPercent(row.week_change_pct)}</td>
                <td data-label="MO%" className={valueClass(row.month_change_pct)}>{formatPercent(row.month_change_pct)}</td>
                <td data-label="1Y%" className={valueClass(row.year_change_pct)}>{formatPercent(row.year_change_pct)}</td>
                <td data-label="RS 1M" className={valueClass(row.rs_vs_spy_1m_pct)}>{formatPercent(row.rs_vs_spy_1m_pct)}</td>
                <td data-label="RS 3M" className={valueClass(row.rs_vs_spy_3m_pct)}>{formatPercent(row.rs_vs_spy_3m_pct)}</td>
                <td data-label="RS Mom">
                  <span className={`scanner-score-pill ${toneForMomentumScore(row.rs_momentum_score)}`}>{formatRating(row.rs_momentum_score)}</span>
                </td>
                <td data-label="RS Days">{formatCountWithPercent(row.rs_days_21d, row.rs_days_21d_pct)}</td>
                <td data-label="Red RS">{formatCountWithPercent(row.red_rs_days_21d, row.red_rs_days_21d_pct)}</td>
                <td data-label="DCR">{formatPercent(row.avg_dcr_21d, { signed: false })}</td>
                <td data-label="RS NH">
                  <span className={`scanner-score-pill ${toneForBoolean(row.rs_new_high_63d)}`}>{formatFlag(row.rs_new_high_63d)}</span>
                </td>
                <td data-label="Vol+">
                  <span className={`scanner-score-pill ${toneForBoolean(row.volume_confirmation)}`}>{formatFlag(row.volume_confirmation)}</span>
                </td>
                <td data-label="ATR%">{formatPercent(row.atr_pct, { signed: false })}</td>
                <td data-label="Avg Vol">{formatVolume(row.avg_volume_20d)}</td>
                <td data-label="Rel Vol">{formatMultiple(row.relative_volume_20d)}</td>
                <td data-label="Top Holdings">
                  <HoldingPills row={row} />
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </section>
  );
}

function SectorChartGrid({
  rows,
  chartPayloads,
  chartErrors,
  chartLoadingTickers,
  benchmarkPayload,
  benchmarkError,
  isBenchmarkLoading,
}: {
  rows: SectorLeaderboardRow[];
  chartPayloads: Record<string, WatchlistChartResponse | null | undefined>;
  chartErrors: Record<string, string>;
  chartLoadingTickers: Record<string, boolean>;
  benchmarkPayload: WatchlistChartResponse | null | undefined;
  benchmarkError: string | undefined;
  isBenchmarkLoading: boolean;
}) {
  const benchmarkCandles = buildChartCandles(benchmarkPayload);
  return (
    <section className="scanner-result-table-shell panel">
      <div className="scanner-result-chart-grid is-3-col sector-all-chart-grid">
        {rows.map((row) => {
          const chartPayload = chartPayloads[row.ticker];
          const chartCandles = buildChartCandles(chartPayload);
          const chartError = chartErrors[row.ticker];
          const isChartLoading = Boolean(chartLoadingTickers[row.ticker]);
          return (
            <article key={row.ticker} className="scanner-chart-card">
              <div className="scanner-chart-card-header">
                <div className="scanner-chart-card-heading">
                  <div className="scanner-chart-card-symbol-row">
                    <Link className="scanner-result-symbol" to={`/sector-leaderboard/${encodeURIComponent(row.ticker)}`}>
                      <span>{row.ticker}</span>
                    </Link>
                    <span className="scanner-inline-badge">{row.description}</span>
                  </div>
                  <strong>{formatMoney(row.price)}</strong>
                  <span>{row.provider}</span>
                </div>
                <div className="scanner-chart-card-price">{renderChange(row.day_change_pct)}</div>
              </div>
              <div className="scanner-chart-card-score-row">
                <span className="scanner-score-pill">WK {formatPercent(row.week_change_pct)}</span>
                <span className="scanner-score-pill">MO {formatPercent(row.month_change_pct)}</span>
                <span className={`scanner-score-pill ${toneForBoolean(row.rs_new_high_63d)}`}>RS NH {formatFlag(row.rs_new_high_63d)}</span>
                <span className="scanner-score-pill">DCR {formatPercent(row.avg_dcr_21d, { signed: false })}</span>
                <span className="scanner-chart-card-volume">Rel Vol {formatMultiple(row.relative_volume_20d)}</span>
              </div>
              <div className="scanner-chart-card-body">
                {isChartLoading || isBenchmarkLoading ? <LoadingBlock label={`Loading ${row.ticker} vs ${BENCHMARK_TICKER} chart...`} /> : null}
                {!isChartLoading && chartError ? <p className="panel-copy">{chartError}</p> : null}
                {!isBenchmarkLoading && benchmarkError ? <p className="panel-copy">{benchmarkError}</p> : null}
                {!isChartLoading && !chartError && chartCandles.length === 0 ? <p className="panel-copy">No chart data.</p> : null}
                {!isBenchmarkLoading && !benchmarkError && benchmarkCandles.length === 0 ? <p className="panel-copy">No SPY chart data.</p> : null}
                {!isChartLoading && !isBenchmarkLoading && !chartError && !benchmarkError && chartCandles.length > 0 && benchmarkCandles.length > 0 ? (
                  <RebasedComparisonChart
                    sectorTicker={row.ticker}
                    benchmarkTicker={BENCHMARK_TICKER}
                    sectorCandles={chartCandles}
                    benchmarkCandles={benchmarkCandles}
                  />
                ) : null}
              </div>
              <div className="scanner-chart-card-footer">
                <span>{chartPayload?.resolved_as_of_date ? `Sector as of ${chartPayload.resolved_as_of_date}` : "Latest"}</span>
                <Link to={`/sector-leaderboard/${encodeURIComponent(row.ticker)}`}>Open Details</Link>
              </div>
            </article>
          );
        })}
      </div>
    </section>
  );
}

function SectorDetailPage({
  row,
  requestedTicker,
  chartPayload,
  chartError,
  isChartLoading,
  holdingViewMode,
  onHoldingViewModeChange,
  holdingSortBy,
  holdingSortDirection,
  chartPayloads,
  chartErrors,
  chartLoadingTickers,
  onSortHoldings,
}: {
  row: SectorLeaderboardRow | null;
  requestedTicker: string;
  chartPayload: WatchlistChartResponse | null | undefined;
  chartError: string | undefined;
  isChartLoading: boolean;
  holdingViewMode: HoldingViewMode;
  onHoldingViewModeChange: (mode: HoldingViewMode) => void;
  holdingSortBy: HoldingSortKey;
  holdingSortDirection: SortDirection;
  chartPayloads: Record<string, WatchlistChartResponse | null | undefined>;
  chartErrors: Record<string, string>;
  chartLoadingTickers: Record<string, boolean>;
  onSortHoldings: (key: HoldingSortKey) => void;
}) {
  const chartCandles = useMemo(() => buildChartCandles(chartPayload), [chartPayload]);
  const sortedHoldings = useMemo(
    () => [...(row?.top_holdings ?? [])].sort((left, right) => compareHoldings(left, right, holdingSortBy, holdingSortDirection)),
    [holdingSortBy, holdingSortDirection, row?.top_holdings],
  );
  const holdingBreadth = useMemo(() => buildHoldingBreadth(row?.top_holdings ?? []), [row?.top_holdings]);
  const chartHoldings = sortedHoldings.slice(0, HOLDING_CHART_LIMIT);

  if (!row) {
    return (
      <div className="page-grid sector-leaderboard-page">
        <section className="panel">
          <p className="panel-copy">Unknown sector ETF: {requestedTicker}</p>
          <Link className="ghost-button" to="/sector-leaderboard">
            Back To Leaderboard
          </Link>
        </section>
      </div>
    );
  }

  return (
    <div className="page-grid sector-leaderboard-page">
      <section className="scanner-result-hero panel">
        <div className="scanner-result-breadcrumbs">
          <Link to="/">Dashboard</Link>
          <span>›</span>
          <Link to="/sector-leaderboard">Sector Leaderboard</Link>
          <span>›</span>
          <span>{row.ticker}</span>
        </div>
        <div className="scanner-result-title-row">
          <div>
            <span className="scanner-result-kicker">{row.provider}</span>
            <h1>{row.ticker} {row.description}</h1>
          </div>
          <span className={`scanner-result-status${row.day_change_pct && row.day_change_pct > 0 ? " is-live" : ""}`}>{formatPercent(row.day_change_pct)}</span>
        </div>
        <p className="scanner-result-copy">ETF detail view with a full candle chart and sortable holding momentum table.</p>
        <div className="scanner-result-metrics">
          <div className="scanner-result-metric">
            <span className="eyebrow">Price</span>
            <strong>{formatMoney(row.price)}</strong>
          </div>
          <div className="scanner-result-metric">
            <span className="eyebrow">Week</span>
            <strong className={valueClass(row.week_change_pct)}>{formatPercent(row.week_change_pct)}</strong>
          </div>
          <div className="scanner-result-metric">
            <span className="eyebrow">Month</span>
            <strong className={valueClass(row.month_change_pct)}>{formatPercent(row.month_change_pct)}</strong>
          </div>
          <div className="scanner-result-metric">
            <span className="eyebrow">1 Year</span>
            <strong className={valueClass(row.year_change_pct)}>{formatPercent(row.year_change_pct)}</strong>
          </div>
        </div>
      </section>

      <section className="scanner-result-metrics">
        <div className="scanner-result-metric">
          <span className="eyebrow">Leader Weight</span>
          <strong>{formatPercent(holdingBreadth.leaderWeightPct, { signed: false })}</strong>
        </div>
        <div className="scanner-result-metric">
          <span className="eyebrow">Daily RS &gt; 80</span>
          <strong>{holdingBreadth.dailyRs80} / {holdingBreadth.total}</strong>
        </div>
        <div className="scanner-result-metric">
          <span className="eyebrow">Red RS Avg</span>
          <strong>{formatPercent(holdingBreadth.avgRedRsDaysPct, { signed: false })}</strong>
        </div>
        <div className="scanner-result-metric">
          <span className="eyebrow">Avg DCR</span>
          <strong>{formatPercent(holdingBreadth.avgDcr, { signed: false })}</strong>
        </div>
        <div className="scanner-result-metric">
          <span className="eyebrow">Vol Confirmed</span>
          <strong>{holdingBreadth.volumeConfirmed} / {holdingBreadth.total}</strong>
        </div>
        <div className="scanner-result-metric">
          <span className="eyebrow">RS NH 63D</span>
          <strong>{holdingBreadth.rsNewHigh63} / {holdingBreadth.total}</strong>
        </div>
      </section>

      <section className="panel sector-chart-panel">
        <div className="sector-chart-header">
          <div>
            <span className="eyebrow">Candle Chart</span>
            <h2>{row.ticker}</h2>
          </div>
          <Link className="ghost-button" to={`/charts?ticker=${encodeURIComponent(row.ticker)}`}>
            Full Chart
          </Link>
        </div>
        {isChartLoading ? <LoadingBlock label={`Loading ${row.ticker} chart...`} /> : null}
        {!isChartLoading && chartError ? <div className="notice-banner">{chartError}</div> : null}
        {!isChartLoading && !chartError && chartCandles.length === 0 ? <div className="empty-state">No cached candle data found for {row.ticker}.</div> : null}
        {!isChartLoading && !chartError && chartCandles.length > 0 ? (
          <div className="sector-price-chart">
            <PriceChart ticker={row.ticker} candles={chartCandles} overlays={chartPayload ?? undefined} visibility={CHART_VISIBILITY} />
          </div>
        ) : null}
      </section>

      <section className="scanner-result-table-shell panel">
        <div className="panel-head sector-holdings-panel-head">
          <div>
            <span className="eyebrow">Holdings</span>
            <h2>Top holdings momentum</h2>
          </div>
          <SectorHoldingsControls
            viewMode={holdingViewMode}
            onViewModeChange={onHoldingViewModeChange}
            sortBy={holdingSortBy}
            sortDirection={holdingSortDirection}
            onSort={onSortHoldings}
          />
        </div>
        {holdingViewMode === "list" ? (
          <SectorHoldingsTable
            row={row}
            sortedHoldings={sortedHoldings}
            holdingSortBy={holdingSortBy}
            holdingSortDirection={holdingSortDirection}
            onSortHoldings={onSortHoldings}
          />
        ) : (
          <SectorHoldingsChartGrid
            sectorTicker={row.ticker}
            holdings={chartHoldings}
            chartPayloads={chartPayloads}
            chartErrors={chartErrors}
            chartLoadingTickers={chartLoadingTickers}
            totalHoldings={sortedHoldings.length}
          />
        )}
      </section>
    </div>
  );
}

function SectorHoldingsControls({
  viewMode,
  onViewModeChange,
  sortBy,
  sortDirection,
  onSort,
}: {
  viewMode: HoldingViewMode;
  onViewModeChange: (mode: HoldingViewMode) => void;
  sortBy: HoldingSortKey;
  sortDirection: SortDirection;
  onSort: (key: HoldingSortKey) => void;
}) {
  return (
    <div className="sector-holdings-controls">
      <div className="sector-holdings-control-group">
        <span className="eyebrow">View</span>
        <div className="scanner-result-view-actions">
          <button className={`scanner-result-view-chip${viewMode === "list" ? " is-active" : ""}`} type="button" onClick={() => onViewModeChange("list")}>
            List
          </button>
          <button className={`scanner-result-view-chip${viewMode === "chart" ? " is-active" : ""}`} type="button" onClick={() => onViewModeChange("chart")}>
            Charts
          </button>
        </div>
      </div>
      <div className="sector-holdings-control-group">
        <span className="eyebrow">Sort</span>
        <div className="sector-holdings-sort-actions">
          {(["weight", "dailyRs", "weeklyRs", "leadership", "rsDays", "redRsDays", "dcr", "relVol", "change", "ticker"] as HoldingSortKey[]).map((key) => (
            <button className={`scanner-result-view-chip${sortBy === key ? " is-active" : ""}`} key={key} type="button" onClick={() => onSort(key)}>
              {labelForHoldingSort(key)}{sortBy === key ? ` ${sortDirection === "asc" ? "Asc" : "Desc"}` : ""}
            </button>
          ))}
        </div>
      </div>
    </div>
  );
}

function SectorHoldingsTable({
  row,
  sortedHoldings,
  holdingSortBy,
  holdingSortDirection,
  onSortHoldings,
}: {
  row: SectorLeaderboardRow;
  sortedHoldings: SectorLeaderboardHolding[];
  holdingSortBy: HoldingSortKey;
  holdingSortDirection: SortDirection;
  onSortHoldings: (key: HoldingSortKey) => void;
}) {
  return (
    <div className="data-table-responsive sector-table-wrap">
      <table className="data-table sector-leaderboard-table sector-holdings-table">
        <thead>
          <tr>
            <th>{renderHoldingSortHeader("Ticker", "ticker", holdingSortBy, holdingSortDirection, onSortHoldings)}</th>
            <th>Name</th>
            <th>{renderHoldingSortHeader("Weight", "weight", holdingSortBy, holdingSortDirection, onSortHoldings)}</th>
            <th>{renderHoldingSortHeader("DY%", "change", holdingSortBy, holdingSortDirection, onSortHoldings)}</th>
            <th>{renderHoldingSortHeader("Daily RS", "dailyRs", holdingSortBy, holdingSortDirection, onSortHoldings)}</th>
            <th>{renderHoldingSortHeader("Weekly RS", "weeklyRs", holdingSortBy, holdingSortDirection, onSortHoldings)}</th>
            <th>{renderHoldingSortHeader("Leader", "leadership", holdingSortBy, holdingSortDirection, onSortHoldings)}</th>
            <th>{renderHoldingSortHeader("RS Days", "rsDays", holdingSortBy, holdingSortDirection, onSortHoldings)}</th>
            <th>{renderHoldingSortHeader("Red RS", "redRsDays", holdingSortBy, holdingSortDirection, onSortHoldings)}</th>
            <th>{renderHoldingSortHeader("DCR", "dcr", holdingSortBy, holdingSortDirection, onSortHoldings)}</th>
            <th>RS NH</th>
            <th>HV63</th>
            <th>Vol+</th>
            <th>Avg Vol</th>
            <th>{renderHoldingSortHeader("Rel Vol", "relVol", holdingSortBy, holdingSortDirection, onSortHoldings)}</th>
            <th>Chart</th>
          </tr>
        </thead>
        <tbody>
          {sortedHoldings.map((holding) => (
            <tr key={`${row.ticker}-${holding.ticker}`}>
              <td data-label="Ticker">
                <Link className="scanner-result-symbol" to={`/charts?ticker=${encodeURIComponent(holding.ticker)}`}>
                  <span>{holding.ticker}</span>
                </Link>
              </td>
              <td data-label="Name">{holding.name || holding.ticker}</td>
              <td data-label="Weight">{formatPercent(holding.weight, { signed: false })}</td>
              <td data-label="DY%" className={valueClass(holding.day_change_pct)}>{formatPercent(holding.day_change_pct)}</td>
              <td data-label="Daily RS">{formatRating(holding.daily_rs_rating)}</td>
              <td data-label="Weekly RS">{formatRating(holding.weekly_rs_rating)}</td>
              <td data-label="Leader">{formatRating(holding.leadership_score)}</td>
              <td data-label="RS Days">{formatCountWithPercent(holding.rs_days_21d, holding.rs_days_21d_pct)}</td>
              <td data-label="Red RS">{formatCountWithPercent(holding.red_rs_days_21d, holding.red_rs_days_21d_pct)}</td>
              <td data-label="DCR">{formatPercent(holding.avg_dcr_21d, { signed: false })}</td>
              <td data-label="RS NH">
                <span className={`scanner-score-pill ${toneForBoolean(holding.rs_new_high_63d)}`}>{formatFlag(holding.rs_new_high_63d)}</span>
              </td>
              <td data-label="HV63">
                <span className={`scanner-score-pill ${toneForBoolean(holding.hv63)}`}>{formatFlag(holding.hv63)}</span>
              </td>
              <td data-label="Vol+">
                <span className={`scanner-score-pill ${toneForBoolean(holding.volume_confirmation)}`}>{formatFlag(holding.volume_confirmation)}</span>
              </td>
              <td data-label="Avg Vol">{formatVolume(holding.avg_volume_20d)}</td>
              <td data-label="Rel Vol">{formatMultiple(holding.relative_volume_20d)}</td>
              <td data-label="Chart">
                <Link className="ghost-button compact-table-action" to={`/charts?ticker=${encodeURIComponent(holding.ticker)}`}>
                  Analyze
                </Link>
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

function SectorHoldingsChartGrid({
  sectorTicker,
  holdings,
  chartPayloads,
  chartErrors,
  chartLoadingTickers,
  totalHoldings,
}: {
  sectorTicker: string;
  holdings: SectorLeaderboardHolding[];
  chartPayloads: Record<string, WatchlistChartResponse | null | undefined>;
  chartErrors: Record<string, string>;
  chartLoadingTickers: Record<string, boolean>;
  totalHoldings: number;
}) {
  return (
    <div className="sector-holdings-chart-section">
      <div className="scanner-result-toolbar sector-holdings-chart-toolbar">
        <div className="scanner-result-toolbar-left">
          <strong>Showing {Math.min(holdings.length, HOLDING_CHART_LIMIT)} charted holdings</strong>
          <span>{totalHoldings > HOLDING_CHART_LIMIT ? `Limited to first ${HOLDING_CHART_LIMIT} by current sort` : `All ${totalHoldings} holdings`}</span>
        </div>
      </div>
      <div className="scanner-result-chart-grid is-3-col sector-holdings-chart-grid">
        {holdings.map((holding) => {
          const chartPayload = chartPayloads[holding.ticker];
          const chartCandles = buildChartCandles(chartPayload);
          const chartError = chartErrors[holding.ticker];
          const isChartLoading = Boolean(chartLoadingTickers[holding.ticker]);
          return (
            <article className="scanner-chart-card" key={`${sectorTicker}-${holding.ticker}-chart`}>
              <div className="scanner-chart-card-header">
                <div className="scanner-chart-card-heading">
                  <div className="scanner-chart-card-symbol-row">
                    <Link className="scanner-result-symbol" to={`/charts?ticker=${encodeURIComponent(holding.ticker)}`}>
                      <span>{holding.ticker}</span>
                    </Link>
                    <span className="scanner-inline-badge">{formatPercent(holding.weight, { signed: false })}</span>
                  </div>
                  <strong>{holding.name || holding.ticker}</strong>
                  <span>Holding in {sectorTicker}</span>
                </div>
                <div className="scanner-chart-card-price">{renderChange(holding.day_change_pct)}</div>
              </div>
              <div className="scanner-chart-card-score-row">
                <span className={`scanner-score-pill ${toneForMomentumScore(holding.daily_rs_rating)}`}>RS {formatRating(holding.daily_rs_rating)}</span>
                <span className={`scanner-score-pill ${toneForMomentumScore(holding.weekly_rs_rating)}`}>WRS {formatRating(holding.weekly_rs_rating)}</span>
                <span className="scanner-score-pill">RS Days {formatPercent(holding.rs_days_21d_pct, { signed: false })}</span>
                <span className="scanner-score-pill">Red RS {formatPercent(holding.red_rs_days_21d_pct, { signed: false })}</span>
                <span className="scanner-score-pill">DCR {formatPercent(holding.avg_dcr_21d, { signed: false })}</span>
                <span className={`scanner-score-pill ${toneForBoolean(holding.volume_confirmation)}`}>Vol+ {formatFlag(holding.volume_confirmation)}</span>
                <span className="scanner-chart-card-volume">Rel Vol {formatMultiple(holding.relative_volume_20d)}</span>
                <span className="scanner-chart-card-volume">Weight {formatPercent(holding.weight, { signed: false })}</span>
              </div>
              <div className="scanner-chart-card-body">
                {isChartLoading ? <LoadingBlock label={`Loading ${holding.ticker} chart...`} /> : null}
                {!isChartLoading && chartError ? <p className="panel-copy">{chartError}</p> : null}
                {!isChartLoading && !chartError && chartCandles.length === 0 ? <p className="panel-copy">No chart data.</p> : null}
                {!isChartLoading && !chartError && chartCandles.length > 0 ? <ScannerMiniChart ticker={holding.ticker} candles={chartCandles} /> : null}
              </div>
              <div className="scanner-chart-card-footer">
                <span>{chartPayload?.resolved_as_of_date ? `As of ${chartPayload.resolved_as_of_date}` : "Latest"}</span>
                <Link to={`/charts?ticker=${encodeURIComponent(holding.ticker)}`}>Analyze</Link>
              </div>
            </article>
          );
        })}
      </div>
    </div>
  );
}

function HoldingPills({ row }: { row: SectorLeaderboardRow }) {
  return (
    <div className="sector-holding-list">
      {row.top_holdings.slice(0, 5).map((holding) => (
        <span className="sector-holding-pill" key={`${row.ticker}-${holding.ticker}`}>
          <span className={`holding-dot ${valueClass(holding.day_change_pct)}`} />
          <span>{holding.ticker}</span>
          <span>{formatPercent(holding.weight, { signed: false })}</span>
        </span>
      ))}
    </div>
  );
}

function buildHoldingBreadth(holdings: SectorLeaderboardHolding[]) {
  const dailyRsValues = holdings.map((holding) => holding.daily_rs_rating).filter(isFiniteNumber);
  const rsDaysValues = holdings.map((holding) => holding.rs_days_21d_pct).filter(isFiniteNumber);
  const redRsDaysValues = holdings.map((holding) => holding.red_rs_days_21d_pct).filter(isFiniteNumber);
  const dcrValues = holdings.map((holding) => holding.avg_dcr_21d).filter(isFiniteNumber);
  const leaderWeightPct = holdings
    .filter((holding) => isFiniteNumber(holding.daily_rs_rating) && holding.daily_rs_rating > 80)
    .reduce((sum, holding) => sum + holding.weight, 0);
  return {
    total: holdings.length,
    dailyRs80: holdings.filter((holding) => isFiniteNumber(holding.daily_rs_rating) && holding.daily_rs_rating > 80).length,
    dailyRs90: holdings.filter((holding) => isFiniteNumber(holding.daily_rs_rating) && holding.daily_rs_rating > 90).length,
    avgDailyRs: average(dailyRsValues),
    avgRsDaysPct: average(rsDaysValues),
    avgRedRsDaysPct: average(redRsDaysValues),
    avgDcr: average(dcrValues),
    leaderWeightPct,
    relVol15: holdings.filter((holding) => isFiniteNumber(holding.relative_volume_20d) && holding.relative_volume_20d > 1.5).length,
    volumeConfirmed: holdings.filter((holding) => holding.volume_confirmation === true).length,
    rsNewHigh63: holdings.filter((holding) => holding.rs_new_high_63d === true).length,
  };
}

function buildChartCandles(chartPayload: WatchlistChartResponse | null | undefined): CandlePoint[] {
  const volumeByTime = new Map((chartPayload?.volume ?? []).map((item) => [item.time, item.value]));
  return (chartPayload?.candles ?? []).map((item) => ({
    ...item,
    volume: volumeByTime.get(item.time) ?? 0,
  }));
}

function compareHoldings(left: SectorLeaderboardHolding, right: SectorLeaderboardHolding, key: HoldingSortKey, direction: SortDirection): number {
  const multiplier = direction === "asc" ? 1 : -1;
  if (key === "ticker") {
    return left.ticker.localeCompare(right.ticker) * multiplier;
  }
  const leftValue = holdingSortValue(left, key);
  const rightValue = holdingSortValue(right, key);
  return (numericSortValue(leftValue) - numericSortValue(rightValue)) * multiplier;
}

function holdingSortValue(holding: SectorLeaderboardHolding, key: HoldingSortKey): number | null | undefined {
  if (key === "weight") {
    return holding.weight;
  }
  if (key === "dailyRs") {
    return holding.daily_rs_rating;
  }
  if (key === "weeklyRs") {
    return holding.weekly_rs_rating;
  }
  if (key === "leadership") {
    return holding.leadership_score;
  }
  if (key === "rsDays") {
    return holding.rs_days_21d_pct;
  }
  if (key === "redRsDays") {
    return holding.red_rs_days_21d_pct;
  }
  if (key === "dcr") {
    return holding.avg_dcr_21d;
  }
  if (key === "relVol") {
    return holding.relative_volume_20d;
  }
  return holding.day_change_pct;
}

function renderHoldingSortHeader(
  label: string,
  key: HoldingSortKey,
  activeKey: HoldingSortKey,
  direction: SortDirection,
  onSort: (key: HoldingSortKey) => void,
) {
  const isActive = key === activeKey;
  return (
    <button className={`sector-sort-button${isActive ? " is-active" : ""}`} type="button" onClick={() => onSort(key)}>
      {label}
      {isActive ? <span>{direction === "asc" ? " Asc" : " Desc"}</span> : null}
    </button>
  );
}

function labelForHoldingSort(key: HoldingSortKey): string {
  if (key === "dailyRs") {
    return "Daily RS";
  }
  if (key === "weeklyRs") {
    return "Weekly RS";
  }
  if (key === "rsDays") {
    return "RS Days";
  }
  if (key === "redRsDays") {
    return "Red RS";
  }
  if (key === "dcr") {
    return "DCR";
  }
  if (key === "relVol") {
    return "Rel Vol";
  }
  if (key === "change") {
    return "DY%";
  }
  if (key === "leadership") {
    return "Leader";
  }
  return key.charAt(0).toUpperCase() + key.slice(1);
}

function renderChange(value: number | null | undefined) {
  return <span className={valueClass(value)}>{formatPercent(value)}</span>;
}

function toneForMomentumScore(value: number | null | undefined): string {
  if (value == null || !Number.isFinite(value)) {
    return "is-neutral";
  }
  if (value >= 65) {
    return "is-strong";
  }
  if (value >= 50) {
    return "is-warm";
  }
  return "is-neutral";
}

function toneForBoolean(value: boolean | null | undefined): string {
  if (value === true) {
    return "is-strong";
  }
  if (value === false) {
    return "is-neutral";
  }
  return "is-neutral";
}

function formatMoney(value: number | null | undefined): string {
  if (value == null || !Number.isFinite(value)) {
    return "-";
  }
  return `$${value.toFixed(2)}`;
}

function formatPercent(value: number | null | undefined, options: { signed?: boolean } = {}): string {
  if (value == null || !Number.isFinite(value)) {
    return "-";
  }
  const signed = options.signed ?? true;
  const prefix = signed && value > 0 ? "+" : "";
  return `${prefix}${value.toFixed(1)}%`;
}

function formatRating(value: number | null | undefined): string {
  if (value == null || !Number.isFinite(value)) {
    return "-";
  }
  return value.toFixed(1);
}

function formatFlag(value: boolean | null | undefined): string {
  if (value == null) {
    return "-";
  }
  return value ? "Yes" : "No";
}

function formatMultiple(value: number | null | undefined): string {
  if (value == null || !Number.isFinite(value)) {
    return "-";
  }
  return `${value.toFixed(2)}x`;
}

function formatCountWithPercent(count: number | null | undefined, percent: number | null | undefined): string {
  if (count == null || !Number.isFinite(count) || percent == null || !Number.isFinite(percent)) {
    return "-";
  }
  return `${count} (${percent.toFixed(0)}%)`;
}

function formatVolume(value: number | null | undefined): string {
  if (value == null || !Number.isFinite(value)) {
    return "-";
  }
  if (value >= 1_000_000) {
    return `${(value / 1_000_000).toFixed(1)}M`;
  }
  if (value >= 1_000) {
    return `${(value / 1_000).toFixed(1)}K`;
  }
  return `${value}`;
}

function average(values: number[]): number | null {
  if (values.length === 0) {
    return null;
  }
  return values.reduce((sum, value) => sum + value, 0) / values.length;
}

function isFiniteNumber(value: number | null | undefined): value is number {
  return value != null && Number.isFinite(value);
}

function valueClass(value: number | null | undefined): string {
  if (value == null || !Number.isFinite(value) || value === 0) {
    return "is-flat";
  }
  return value > 0 ? "is-positive" : "is-negative";
}

function numericSortValue(value: number | null | undefined): number {
  return value == null || !Number.isFinite(value) ? -9999 : value;
}
