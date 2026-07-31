import { useEffect, useMemo, useState } from "react";
import { Link, useParams } from "react-router-dom";
import { LoadingBlock } from "../components/LoadingBlock";
import { PriceChart, type ChartVisibility } from "../components/PriceChart";
import { ScannerMiniChart } from "../components/ScannerMiniChart";
import { fetchJson } from "../lib/api";
import { formatLocalDate, formatLocalDateTime } from "../lib/format";
import type { CandlePoint, SectorLeaderboardHolding, SectorLeaderboardResponse, SectorLeaderboardRow, WatchlistChartResponse } from "../lib/types";

type ViewMode = "list" | "chart";
type HoldingSortKey = "weight" | "dailyRs" | "ticker" | "change";
type SortDirection = "asc" | "desc";

const BENCHMARK_TICKER = "SPY";

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
      return [detailRow.ticker];
    }
    if (!requestedTicker && viewMode === "chart") {
      return [...rows.map((row) => row.ticker), BENCHMARK_TICKER];
    }
    return [];
  }, [detailRow, requestedTicker, rows, viewMode]);
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
        holdingSortBy={holdingSortBy}
        holdingSortDirection={holdingSortDirection}
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
              <th>ATR%</th>
              <th>Vol</th>
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
                <td data-label="ATR%">{formatPercent(row.atr_pct, { signed: false })}</td>
                <td data-label="Vol">{formatVolume(row.volume)}</td>
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
      <div className="scanner-result-chart-grid is-2-col sector-all-chart-grid">
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
                <span className="scanner-chart-card-volume">Vol {formatVolume(row.volume)}</span>
              </div>
              <div className="scanner-chart-card-body">
                <div className="sector-chart-comparison">
                  <div className="sector-mini-chart-panel">
                    <div className="sector-mini-chart-label">
                      <strong>{row.ticker}</strong>
                      <span>{formatPercent(row.day_change_pct)}</span>
                    </div>
                    {isChartLoading ? <LoadingBlock label={`Loading ${row.ticker} chart...`} /> : null}
                    {!isChartLoading && chartError ? <p className="panel-copy">{chartError}</p> : null}
                    {!isChartLoading && !chartError && chartCandles.length === 0 ? <p className="panel-copy">No chart data.</p> : null}
                    {!isChartLoading && !chartError && chartCandles.length > 0 ? <ScannerMiniChart ticker={row.ticker} candles={chartCandles} /> : null}
                  </div>
                  <div className="sector-mini-chart-panel">
                    <div className="sector-mini-chart-label">
                      <strong>{BENCHMARK_TICKER}</strong>
                      <span>Benchmark</span>
                    </div>
                    {isBenchmarkLoading ? <LoadingBlock label={`Loading ${BENCHMARK_TICKER} chart...`} /> : null}
                    {!isBenchmarkLoading && benchmarkError ? <p className="panel-copy">{benchmarkError}</p> : null}
                    {!isBenchmarkLoading && !benchmarkError && benchmarkCandles.length === 0 ? <p className="panel-copy">No SPY chart data.</p> : null}
                    {!isBenchmarkLoading && !benchmarkError && benchmarkCandles.length > 0 ? (
                      <ScannerMiniChart ticker={BENCHMARK_TICKER} candles={benchmarkCandles} />
                    ) : null}
                  </div>
                </div>
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
  holdingSortBy,
  holdingSortDirection,
  onSortHoldings,
}: {
  row: SectorLeaderboardRow | null;
  requestedTicker: string;
  chartPayload: WatchlistChartResponse | null | undefined;
  chartError: string | undefined;
  isChartLoading: boolean;
  holdingSortBy: HoldingSortKey;
  holdingSortDirection: SortDirection;
  onSortHoldings: (key: HoldingSortKey) => void;
}) {
  const chartCandles = useMemo(() => buildChartCandles(chartPayload), [chartPayload]);
  const sortedHoldings = useMemo(
    () => [...(row?.top_holdings ?? [])].sort((left, right) => compareHoldings(left, right, holdingSortBy, holdingSortDirection)),
    [holdingSortBy, holdingSortDirection, row?.top_holdings],
  );

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
        <div className="panel-head">
          <div>
            <span className="eyebrow">Holdings</span>
            <h2>Top holdings momentum</h2>
          </div>
        </div>
        <div className="data-table-responsive sector-table-wrap">
          <table className="data-table sector-leaderboard-table sector-holdings-table">
            <thead>
              <tr>
                <th>{renderHoldingSortHeader("Ticker", "ticker", holdingSortBy, holdingSortDirection, onSortHoldings)}</th>
                <th>Name</th>
                <th>{renderHoldingSortHeader("Weight", "weight", holdingSortBy, holdingSortDirection, onSortHoldings)}</th>
                <th>{renderHoldingSortHeader("DY%", "change", holdingSortBy, holdingSortDirection, onSortHoldings)}</th>
                <th>{renderHoldingSortHeader("Daily RS", "dailyRs", holdingSortBy, holdingSortDirection, onSortHoldings)}</th>
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
      </section>
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
  const leftValue = key === "weight" ? left.weight : key === "dailyRs" ? left.daily_rs_rating : left.day_change_pct;
  const rightValue = key === "weight" ? right.weight : key === "dailyRs" ? right.daily_rs_rating : right.day_change_pct;
  return (numericSortValue(leftValue) - numericSortValue(rightValue)) * multiplier;
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

function renderChange(value: number | null | undefined) {
  return <span className={valueClass(value)}>{formatPercent(value)}</span>;
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

function valueClass(value: number | null | undefined): string {
  if (value == null || !Number.isFinite(value) || value === 0) {
    return "is-flat";
  }
  return value > 0 ? "is-positive" : "is-negative";
}

function numericSortValue(value: number | null | undefined): number {
  return value == null || !Number.isFinite(value) ? -9999 : value;
}
