import { useEffect, useMemo, useState } from "react";
import { Link } from "react-router-dom";
import { LoadingBlock } from "../components/LoadingBlock";
import { PriceChart, type ChartVisibility } from "../components/PriceChart";
import { fetchJson } from "../lib/api";
import { formatLocalDate } from "../lib/format";
import type { CandlePoint, SectorLeaderboardResponse, SectorLeaderboardRow, WatchlistChartResponse } from "../lib/types";

type ViewMode = "list" | "chart";

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
  const [payload, setPayload] = useState<SectorLeaderboardResponse | null>(null);
  const [chartPayload, setChartPayload] = useState<WatchlistChartResponse | null>(null);
  const [isLoading, setIsLoading] = useState(true);
  const [isChartLoading, setIsChartLoading] = useState(false);
  const [notice, setNotice] = useState("");
  const [chartNotice, setChartNotice] = useState("");
  const [viewMode, setViewMode] = useState<ViewMode>("list");
  const [selectedTicker, setSelectedTicker] = useState("XLC");

  useEffect(() => {
    setIsLoading(true);
    setNotice("");
    void fetchJson<SectorLeaderboardResponse>("/api/sector-leaderboard")
      .then((response) => {
        setPayload(response);
        setSelectedTicker((current) => response.rows.some((row) => row.ticker === current) ? current : response.rows[0]?.ticker ?? "XLC");
      })
      .catch((error) => {
        setPayload(null);
        setNotice(error instanceof Error ? error.message : "Failed to load sector leaderboard.");
      })
      .finally(() => setIsLoading(false));
  }, []);

  useEffect(() => {
    if (viewMode !== "chart" || !selectedTicker) {
      return;
    }
    const controller = new AbortController();
    setIsChartLoading(true);
    setChartNotice("");
    void fetchJson<WatchlistChartResponse>(`/api/charts/${selectedTicker}?period=18mo`, { signal: controller.signal })
      .then(setChartPayload)
      .catch((error) => {
        if (controller.signal.aborted) {
          return;
        }
        setChartPayload(null);
        setChartNotice(error instanceof Error ? error.message : `Failed to load ${selectedTicker} chart.`);
      })
      .finally(() => {
        if (!controller.signal.aborted) {
          setIsChartLoading(false);
        }
      });
    return () => controller.abort();
  }, [selectedTicker, viewMode]);

  const rows = payload?.rows ?? [];
  const selectedRow = rows.find((row) => row.ticker === selectedTicker) ?? rows[0] ?? null;
  const chartCandles = useMemo<CandlePoint[]>(
    () =>
      (chartPayload?.candles ?? []).map((item) => ({
        ...item,
        volume: chartPayload?.volume.find((volumePoint) => volumePoint.time === item.time)?.value ?? 0,
      })),
    [chartPayload],
  );

  if (isLoading) {
    return <LoadingBlock label="Loading sector leaderboard..." />;
  }

  return (
    <div className="page-grid sector-leaderboard-page">
      <section className="sector-leaderboard-hero">
        <div>
          <span className="eyebrow">ETF Rotation</span>
          <h1>U.S. ETF Sector Leaderboard</h1>
          <p>
            Select Sector SPDR leaderboard ranked by daily performance, with cached OHLCV metrics and top holdings from the default
            State Street sector ETF catalog.
          </p>
        </div>
        <div className="sector-leaderboard-actions">
          <div className="segmented-control" role="tablist" aria-label="Sector leaderboard view">
            <button className={viewMode === "list" ? "is-active" : ""} type="button" onClick={() => setViewMode("list")}>
              List
            </button>
            <button className={viewMode === "chart" ? "is-active" : ""} type="button" onClick={() => setViewMode("chart")}>
              Chart
            </button>
          </div>
          <button className="ghost-button" type="button" onClick={() => window.location.reload()}>
            Refresh
          </button>
        </div>
      </section>

      {notice ? <div className="notice-banner">{notice}</div> : null}

      <section className="sector-leaderboard-meta">
        <div>
          <span>Latest Data</span>
          <strong>{formatLocalDate(payload?.latest_data_date)}</strong>
        </div>
        <div>
          <span>Catalog</span>
          <strong>{rows.length} ETFs</strong>
        </div>
        <div>
          <span>Source</span>
          <a href={payload?.source.fund_finder_url} target="_blank" rel="noreferrer">
            SSGA Fund Finder
          </a>
        </div>
      </section>

      {viewMode === "list" ? (
        <SectorLeaderboardTable
          rows={rows}
          selectedTicker={selectedTicker}
          onSelectTicker={(ticker) => {
            setSelectedTicker(ticker);
            setViewMode("chart");
          }}
        />
      ) : (
        <section className="sector-chart-view">
          <div className="sector-chart-sidebar">
            <div className="sector-chart-picker">
              {rows.map((row) => (
                <button
                  key={row.ticker}
                  className={row.ticker === selectedTicker ? "is-active" : ""}
                  type="button"
                  onClick={() => setSelectedTicker(row.ticker)}
                >
                  <span>{row.ticker}</span>
                  <span className={valueClass(row.day_change_pct)}>{formatPercent(row.day_change_pct)}</span>
                </button>
              ))}
            </div>
          </div>
          <div className="sector-chart-panel">
            <div className="sector-chart-header">
              <div>
                <span className="eyebrow">{selectedRow?.description ?? selectedTicker}</span>
                <h2>{selectedTicker} Candle Chart</h2>
              </div>
              <Link className="ghost-button" to={`/charts?ticker=${selectedTicker}`}>
                Full Chart
              </Link>
            </div>
            {isChartLoading ? <LoadingBlock label={`Loading ${selectedTicker} chart...`} /> : null}
            {chartNotice ? <div className="notice-banner">{chartNotice}</div> : null}
            {!isChartLoading && chartCandles.length > 0 ? (
              <div className="sector-price-chart">
                <PriceChart ticker={selectedTicker} candles={chartCandles} overlays={chartPayload ?? undefined} visibility={CHART_VISIBILITY} />
              </div>
            ) : null}
            {!isChartLoading && !chartNotice && chartCandles.length === 0 ? (
              <div className="empty-state">No cached candle data found for {selectedTicker}.</div>
            ) : null}
          </div>
        </section>
      )}
    </div>
  );
}

function SectorLeaderboardTable({
  rows,
  selectedTicker,
  onSelectTicker,
}: {
  rows: SectorLeaderboardRow[];
  selectedTicker: string;
  onSelectTicker: (ticker: string) => void;
}) {
  return (
    <section className="sector-table-wrap">
      <table className="sector-leaderboard-table">
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
            <tr key={row.ticker} className={row.ticker === selectedTicker ? "is-selected" : ""}>
              <td>
                <button className="ticker-link-button" type="button" onClick={() => onSelectTicker(row.ticker)}>
                  {row.ticker}
                </button>
              </td>
              <td>{row.description}</td>
              <td>{formatMoney(row.price)}</td>
              <td className={valueClass(row.day_change_pct)}>{formatPercent(row.day_change_pct)}</td>
              <td className={valueClass(row.week_change_pct)}>{formatPercent(row.week_change_pct)}</td>
              <td className={valueClass(row.month_change_pct)}>{formatPercent(row.month_change_pct)}</td>
              <td className={valueClass(row.year_change_pct)}>{formatPercent(row.year_change_pct)}</td>
              <td>{formatPercent(row.atr_pct, { signed: false })}</td>
              <td>{formatVolume(row.volume)}</td>
              <td>
                <div className="sector-holding-list">
                  {row.top_holdings.map((holding) => (
                    <span className="sector-holding-pill" key={`${row.ticker}-${holding.ticker}`}>
                      <span className={`holding-dot ${valueClass(holding.day_change_pct)}`} />
                      <span>{holding.ticker}</span>
                      <span>{formatPercent(holding.weight, { signed: false })}</span>
                    </span>
                  ))}
                </div>
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </section>
  );
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
