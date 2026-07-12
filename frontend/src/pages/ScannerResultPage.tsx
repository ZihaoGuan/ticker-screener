import { useEffect, useMemo, useState } from "react";
import { Link, useParams } from "react-router-dom";
import { LoadingBlock } from "../components/LoadingBlock";
import { PaginationControls } from "../components/PaginationControls";
import { ScannerMiniChart } from "../components/ScannerMiniChart";
import { fetchJson } from "../lib/api";
import { formatCount, formatLocalDate, formatLocalDateTime } from "../lib/format";
import type {
  CandlePoint,
  ScannerBoardCard,
  ScannerBoardResponse,
  TechnicalIndicatorRatingCell,
  TopRatingEntry,
  TopTechnicalIndicatorRatingEntry,
  TopTechnicalIndicatorRatingsResponse,
  TopRatingsResponse,
  TopTechnicalRatingEntry,
  TopTechnicalRatingsResponse,
  WatchlistChartResponse,
  WatchlistDetailResponse,
} from "../lib/types";

type SortKey = "als" | "ta" | "fa" | "ars" | "ticker" | "company" | "sector" | "volume" | "change";
type SortDirection = "asc" | "desc";
type ScannerViewMode = "charts" | "list";
type ChartColumnCount = 2 | 3;

type ScannerRow = {
  ticker: string;
  company: string;
  sector: string;
  industry: string;
  summary: string;
  setupLabel: string;
  chartHref: string;
  dayVolume: number | null;
  changePct: number | null;
  taScore: number | null;
  faScore: number | null;
  perfYearPct: number | null;
  perfYtdPct: number | null;
  canslimScore: number | null;
  canslimMaxScore: number | null;
  accelScore: number | null;
  accelLabel: string;
  arsScore: number | null;
  dailyRsRating: number | null;
  alsScore: number | null;
  technicalIndicator1d: string;
  technicalIndicator1w: string;
  isNew: boolean;
};

const MAX_RATINGS_ROWS = 500;
const LIST_PAGE_SIZE = 50;
const CHART_PAGE_SIZE_BY_COLUMN: Record<ChartColumnCount, number> = {
  2: 10,
  3: 12,
};

export function ScannerResultPage() {
  const { scannerId = "" } = useParams();
  const [board, setBoard] = useState<ScannerBoardResponse | null>(null);
  const [card, setCard] = useState<ScannerBoardCard | null>(null);
  const [detail, setDetail] = useState<WatchlistDetailResponse | null>(null);
  const [fundamentalPayload, setFundamentalPayload] = useState<TopRatingsResponse | null>(null);
  const [technicalPayload, setTechnicalPayload] = useState<TopTechnicalRatingsResponse | null>(null);
  const [technicalIndicatorPayload, setTechnicalIndicatorPayload] = useState<TopTechnicalIndicatorRatingsResponse | null>(null);
  const [isLoading, setIsLoading] = useState(true);
  const [notice, setNotice] = useState("");
  const [search, setSearch] = useState("");
  const [sectorFilter, setSectorFilter] = useState("all");
  const [industryFilter, setIndustryFilter] = useState("all");
  const [sortBy, setSortBy] = useState<SortKey>("als");
  const [sortDirection, setSortDirection] = useState<SortDirection>("desc");
  const [viewMode, setViewMode] = useState<ScannerViewMode>("list");
  const [chartColumns, setChartColumns] = useState<ChartColumnCount>(2);
  const [currentPage, setCurrentPage] = useState(1);
  const [chartPayloads, setChartPayloads] = useState<Record<string, WatchlistChartResponse | null | undefined>>({});
  const [chartErrors, setChartErrors] = useState<Record<string, string>>({});
  const [chartLoadingTickers, setChartLoadingTickers] = useState<Record<string, boolean>>({});

  useEffect(() => {
    let ignore = false;

    async function load() {
      setIsLoading(true);
      setNotice("");
      try {
        const boardPayload = await fetchJson<ScannerBoardResponse>("/api/scanner-board");
        if (ignore) {
          return;
        }
        setBoard(boardPayload);

        const selectedCard =
          boardPayload.cards.find((item) => item.id === scannerId) ??
          (scannerId === "weekly_rs" ? boardPayload.cards.find((item) => item.id === "weekly_rs_before_price") ?? null : null);
        setCard(selectedCard);
        if (!selectedCard) {
          setDetail(null);
          setNotice(`Unknown scanner: ${scannerId}`);
          return;
        }
        if (!selectedCard.stem) {
          setDetail(null);
          setNotice("Scanner has no persisted watchlist yet.");
          return;
        }
        const detailPayload = await fetchJson<WatchlistDetailResponse>(`/api/watchlists/${encodeURIComponent(selectedCard.stem)}`);
        if (ignore) {
          return;
        }
        setDetail(detailPayload);
      } catch (error) {
        if (ignore) {
          return;
        }
        setBoard(null);
        setCard(null);
        setDetail(null);
        setFundamentalPayload(null);
        setTechnicalPayload(null);
        setTechnicalIndicatorPayload(null);
        setNotice(error instanceof Error ? error.message : "Failed to load scanner list.");
      } finally {
        if (!ignore) {
          setIsLoading(false);
        }
      }
    }

    void load();
    return () => {
      ignore = true;
    };
  }, [scannerId]);

  useEffect(() => {
    let ignore = false;

    void Promise.all([
      fetchJson<TopRatingsResponse>(`/api/ratings/top?limit=${MAX_RATINGS_ROWS}`),
      fetchJson<TopTechnicalRatingsResponse>(`/api/ratings/technical/top?limit=${MAX_RATINGS_ROWS}`),
      fetchJson<TopTechnicalIndicatorRatingsResponse>(`/api/ratings/technical-indicator/top?limit=${MAX_RATINGS_ROWS}`),
    ])
      .then(([fundamentalRows, technicalRows, technicalIndicatorRows]) => {
        if (ignore) {
          return;
        }
        setFundamentalPayload(fundamentalRows);
        setTechnicalPayload(technicalRows);
        setTechnicalIndicatorPayload(technicalIndicatorRows);
      })
      .catch(() => {
        if (ignore) {
          return;
        }
        setFundamentalPayload(null);
        setTechnicalPayload(null);
        setTechnicalIndicatorPayload(null);
      });

    return () => {
      ignore = true;
    };
  }, []);

  const fundamentalMap = useMemo(() => {
    return new Map((fundamentalPayload?.rows ?? []).map((row) => [row.ticker.toUpperCase(), row] satisfies [string, TopRatingEntry]));
  }, [fundamentalPayload?.rows]);

  const technicalMap = useMemo(() => {
    return new Map((technicalPayload?.rows ?? []).map((row) => [row.ticker.toUpperCase(), row] satisfies [string, TopTechnicalRatingEntry]));
  }, [technicalPayload?.rows]);

  const technicalIndicatorMap = useMemo(() => {
    return new Map((technicalIndicatorPayload?.rows ?? []).map((row) => [row.ticker.toUpperCase(), row] satisfies [string, TopTechnicalIndicatorRatingEntry]));
  }, [technicalIndicatorPayload?.rows]);

  const rows = useMemo(() => {
    return (detail?.entries ?? []).map((entry) => buildScannerRow(entry, card?.stem ?? "", fundamentalMap, technicalMap, technicalIndicatorMap));
  }, [card?.stem, detail?.entries, fundamentalMap, technicalMap, technicalIndicatorMap]);

  const newTickerCount = useMemo(() => rows.filter((row) => row.isNew).length, [rows]);

  const sectors = useMemo(() => {
    return Array.from(new Set(rows.map((row) => row.sector).filter((sector) => sector && sector !== "Unknown sector"))).sort();
  }, [rows]);

  const industries = useMemo(() => {
    const sourceRows = sectorFilter === "all" ? rows : rows.filter((row) => row.sector === sectorFilter);
    return Array.from(new Set(sourceRows.map((row) => row.industry).filter((industry) => industry && industry !== "Unknown industry"))).sort();
  }, [rows, sectorFilter]);

  const filteredRows = useMemo(() => {
    const query = search.trim().toLowerCase();
    let nextRows = rows;
    if (sectorFilter !== "all") {
      nextRows = nextRows.filter((row) => row.sector === sectorFilter);
    }
    if (industryFilter !== "all") {
      nextRows = nextRows.filter((row) => row.industry === industryFilter);
    }
    if (query) {
      nextRows = nextRows.filter((row) =>
        [row.ticker, row.company, row.sector, row.industry, row.summary, row.setupLabel].join(" ").toLowerCase().includes(query),
      );
    }
    return [...nextRows].sort((left, right) => compareScannerRows(left, right, sortBy, sortDirection));
  }, [rows, search, sectorFilter, industryFilter, sortBy, sortDirection]);

  useEffect(() => {
    if (industryFilter !== "all" && !industries.includes(industryFilter)) {
      setIndustryFilter("all");
    }
  }, [industries, industryFilter]);

  useEffect(() => {
    setCurrentPage(1);
  }, [scannerId, search, sectorFilter, industryFilter, sortBy, sortDirection, viewMode, chartColumns]);

  const pageSize = viewMode === "charts" ? CHART_PAGE_SIZE_BY_COLUMN[chartColumns] : LIST_PAGE_SIZE;
  const totalPages = Math.max(1, Math.ceil(filteredRows.length / pageSize));
  const normalizedPage = Math.min(currentPage, totalPages);
  const pagedRows = useMemo(() => {
    const startIndex = (normalizedPage - 1) * pageSize;
    return filteredRows.slice(startIndex, startIndex + pageSize);
  }, [filteredRows, normalizedPage, pageSize]);
  const pagedTickerKey = useMemo(() => pagedRows.map((row) => row.ticker).join("|"), [pagedRows]);

  useEffect(() => {
    if (currentPage !== normalizedPage) {
      setCurrentPage(normalizedPage);
    }
  }, [currentPage, normalizedPage]);

  useEffect(() => {
    if (viewMode !== "charts" || pagedRows.length === 0) {
      return;
    }
    const missingTickers = pagedRows
      .map((row) => row.ticker)
      .filter((ticker) => chartPayloads[ticker] === undefined && !chartLoadingTickers[ticker]);
    if (missingTickers.length === 0) {
      return;
    }
    let ignore = false;
    setChartLoadingTickers((current) => {
      const next = { ...current };
      for (const ticker of missingTickers) {
        next[ticker] = true;
      }
      return next;
    });
    void Promise.allSettled(
      missingTickers.map(async (ticker) => {
        const query = new URLSearchParams({ period: "18mo" });
        const payload = await fetchJson<WatchlistChartResponse>(`/api/charts/${ticker}?${query.toString()}`);
        return { ticker, payload };
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
        for (const ticker of missingTickers) {
          delete next[ticker];
        }
        return next;
      });
    });
    return () => {
      ignore = true;
    };
  }, [pagedTickerKey, viewMode]);

  const topNote = useMemo(() => {
    const first = detail?.entries.find((entry) => typeof entry.master_note === "string" && entry.master_note.trim());
    return typeof first?.master_note === "string" ? first.master_note.trim() : card?.description ?? "";
  }, [card?.description, detail?.entries]);

  const handleExportCsv = () => {
    if (filteredRows.length === 0) {
      return;
    }
    const lines = [
      ["Ticker", "Company", "Sector", "Industry", "Day Volume", "Change %", "1Y %", "YTD %", "TA", "1D", "1W", "FA", "CANSLIM", "Accel", "ARS", "Daily RS", "ALS Score", "Setup", "Summary"].join(","),
      ...filteredRows.map((row) =>
        [
          row.ticker,
          csvValue(row.company),
          csvValue(row.sector),
          csvValue(row.industry),
          row.dayVolume == null ? "" : row.dayVolume.toString(),
          row.changePct == null ? "" : row.changePct.toFixed(2),
          row.perfYearPct == null ? "" : row.perfYearPct.toFixed(2),
          row.perfYtdPct == null ? "" : row.perfYtdPct.toFixed(2),
          row.taScore == null ? "" : row.taScore.toFixed(1),
          csvValue(row.technicalIndicator1d),
          csvValue(row.technicalIndicator1w),
          row.faScore == null ? "" : row.faScore.toFixed(1),
          formatCanslimScore(row.canslimScore, row.canslimMaxScore),
          formatAccelerationScore(row.accelScore, row.accelLabel),
          row.arsScore == null ? "" : Math.round(row.arsScore).toString(),
          row.dailyRsRating == null ? "" : row.dailyRsRating.toFixed(1),
          row.alsScore == null ? "" : Math.round(row.alsScore).toString(),
          csvValue(row.setupLabel),
          csvValue(row.summary),
        ].join(","),
      ),
    ];
    const blob = new Blob([lines.join("\n")], { type: "text/csv;charset=utf-8" });
    const url = URL.createObjectURL(blob);
    const anchor = document.createElement("a");
    anchor.href = url;
    anchor.download = `${card?.stem || scannerId}-scanner-list.csv`;
    document.body.append(anchor);
    anchor.click();
    anchor.remove();
    URL.revokeObjectURL(url);
  };

  return (
    <div className="page-grid scanner-result-page">
      <section className="scanner-result-hero panel">
        <div className="scanner-result-breadcrumbs">
          <Link to="/">Dashboard</Link>
          <span>›</span>
          <Link to="/scanner">Stock Scanner</Link>
          <span>›</span>
          <span>{card?.label ?? "Scanner Detail"}</span>
        </div>
        <div className="scanner-result-title-row">
          <div>
            <span className="scanner-result-kicker">Precision Utility</span>
            <h1>{card?.label ?? "Scanner Detail"}</h1>
          </div>
          <span className={`scanner-result-status${rows.length > 0 ? " is-live" : ""}`}>{rows.length > 0 ? "Pattern Detected" : "No Active Hits"}</span>
        </div>
        <p className="scanner-result-copy">{card?.description ?? "Open scanner to review latest persisted result list."}</p>
        <div className="scanner-result-metrics">
          <div className="scanner-result-metric">
            <span className="eyebrow">Target Trading Day</span>
            <strong>{formatLocalDate(board?.target_trading_date)}</strong>
          </div>
          <div className="scanner-result-metric">
            <span className="eyebrow">Captured</span>
            <strong>{formatLocalDateTime(card?.captured_at)}</strong>
          </div>
          <div className="scanner-result-metric">
            <span className="eyebrow">Signals</span>
            <strong>{formatCount(filteredRows.length)}</strong>
          </div>
          <div className="scanner-result-metric">
            <span className="eyebrow">New Vs Prev</span>
            <strong>{detail?.has_previous_scan ? formatCount(newTickerCount) : "--"}</strong>
          </div>
          <div className="scanner-result-metric">
            <span className="eyebrow">Dataset</span>
            <strong>{card?.stem ?? "--"}</strong>
          </div>
        </div>
      </section>

      <section className="scanner-result-filter-grid">
        <label className="scanner-result-filter panel">
          <span className="eyebrow">Search</span>
          <input type="text" value={search} onChange={(event) => setSearch(event.target.value)} placeholder="Find ticker, company, sector, industry" />
        </label>
        <label className="scanner-result-filter panel">
          <span className="eyebrow">Sector</span>
          <select value={sectorFilter} onChange={(event) => setSectorFilter(event.target.value)}>
            <option value="all">All sectors</option>
            {sectors.map((sector) => (
              <option key={sector} value={sector}>
                {sector}
              </option>
            ))}
          </select>
        </label>
        <label className="scanner-result-filter panel">
          <span className="eyebrow">Industry</span>
          <select value={industryFilter} onChange={(event) => setIndustryFilter(event.target.value)}>
            <option value="all">All industries</option>
            {industries.map((industry) => (
              <option key={industry} value={industry}>
                {industry}
              </option>
            ))}
          </select>
        </label>
        <div className="scanner-result-filter panel scanner-result-filter-actions">
          <span className="eyebrow">Views</span>
          <div className="scanner-result-view-actions">
            <button
              type="button"
              className={`scanner-result-view-chip${viewMode === "charts" ? " is-active" : ""}`}
              onClick={() => setViewMode("charts")}
            >
              Charts
            </button>
            <button
              type="button"
              className={`scanner-result-view-chip${viewMode === "list" ? " is-active" : ""}`}
              onClick={() => setViewMode("list")}
            >
              List
            </button>
          </div>
        </div>
        <div className="scanner-result-filter panel scanner-result-filter-actions">
          <span className="eyebrow">Chart Density</span>
          <div className="scanner-result-view-actions">
            <button
              type="button"
              className={`scanner-result-view-chip${chartColumns === 2 ? " is-active" : ""}`}
              onClick={() => setChartColumns(2)}
            >
              2 per line
            </button>
            <button
              type="button"
              className={`scanner-result-view-chip${chartColumns === 3 ? " is-active" : ""}`}
              onClick={() => setChartColumns(3)}
            >
              3 per line
            </button>
          </div>
        </div>
      </section>

      <section className="scanner-result-toolbar">
        <div className="scanner-result-toolbar-left">
          <strong>Showing {formatCount(filteredRows.length)} results</strong>
          <span>View: {viewMode === "charts" ? "Charts" : "List"}</span>
          <span>Sorted by: {labelForSort(sortBy)} ({sortDirection})</span>
          <span>Page {normalizedPage} / {formatCount(totalPages)}</span>
        </div>
        <div className="scanner-result-toolbar-right">
          <button className="ghost-button scanner-result-export" type="button" onClick={handleExportCsv} disabled={filteredRows.length === 0}>
            Export CSV
          </button>
        </div>
      </section>

      <section className="scanner-result-table-shell panel">
        {isLoading ? <LoadingBlock label="Loading scanner result list…" /> : null}
        {!isLoading && notice ? <p className="panel-copy">{notice}</p> : null}
        {!isLoading && !notice && filteredRows.length === 0 ? <p className="panel-copy">No tickers match current scanner filters.</p> : null}
        {!isLoading && filteredRows.length > 0 ? (
          <>
            <PaginationControls
              currentPage={normalizedPage}
              totalItems={filteredRows.length}
              totalPages={totalPages}
              pageSize={pageSize}
              onPageChange={setCurrentPage}
            />
            {viewMode === "charts" ? (
              <div className={`scanner-result-chart-grid is-${chartColumns}-col`}>
                {pagedRows.map((row, index) => {
                  const chartPayload = chartPayloads[row.ticker];
                  const chartCandles = buildMiniChartCandles(chartPayload);
                  const isChartLoading = Boolean(chartLoadingTickers[row.ticker]);
                  const chartError = chartErrors[row.ticker];
                  const latestCandle = chartCandles[chartCandles.length - 1] ?? null;
                  const resolvedChange = row.changePct ?? computeDayChangePct(chartCandles);
                  return (
                    <article key={`${row.ticker}-${index}`} className="scanner-chart-card">
                      <div className="scanner-chart-card-header">
                        <div className="scanner-chart-card-heading">
                          <div className="scanner-chart-card-symbol-row">
                            <Link className="scanner-result-symbol" to={row.chartHref}>
                              <span>{row.ticker}</span>
                            </Link>
                            {row.isNew ? <span className="scanner-inline-badge is-new">New</span> : null}
                            {row.setupLabel ? <span className="scanner-inline-badge">{row.setupLabel}</span> : null}
                          </div>
                          <strong>{row.company || row.ticker}</strong>
                          <span>{row.sector}</span>
                        </div>
                        <div className="scanner-chart-card-price">
                          <strong>{latestCandle ? latestCandle.close.toFixed(2) : "--"}</strong>
                          {renderChange(resolvedChange)}
                        </div>
                      </div>
                      <div className="scanner-chart-card-score-row">
                        <span className={`scanner-score-pill ${toneForScore(row.alsScore, 100)}`}>ALS {formatIntegerScore(row.alsScore)}</span>
                        <span className={`scanner-score-pill ${toneForScore(row.taScore, 10)}`}>TA {formatTenPointScore(row.taScore)}</span>
                        <span className="scanner-chart-card-volume">Vol {formatVolume(row.dayVolume)}</span>
                      </div>
                      <div className="scanner-chart-card-body">
                        {isChartLoading ? <LoadingBlock label={`Loading ${row.ticker} chart…`} /> : null}
                        {!isChartLoading && chartError ? <p className="panel-copy">{chartError}</p> : null}
                        {!isChartLoading && !chartError && chartCandles.length === 0 ? <p className="panel-copy">No chart data.</p> : null}
                        {!isChartLoading && !chartError && chartCandles.length > 0 ? <ScannerMiniChart ticker={row.ticker} candles={chartCandles} /> : null}
                      </div>
                      <div className="scanner-chart-card-footer">
                        <span>{chartPayload?.resolved_as_of_date ? `As of ${chartPayload.resolved_as_of_date}` : "Latest"}</span>
                        <Link to={row.chartHref}>Analyze Full Chart</Link>
                      </div>
                    </article>
                  );
                })}
              </div>
            ) : (
              <div className="data-table-responsive scanner-result-table-wrap">
                <table className="data-table scanner-result-table">
                  <thead>
                    <tr>
                      <th>#</th>
                      <th>{renderSortHeader("Symbol", "ticker", sortBy, sortDirection, setSortBy, setSortDirection)}</th>
                      <th>{renderSortHeader("Company", "company", sortBy, sortDirection, setSortBy, setSortDirection)}</th>
                      <th>{renderSortHeader("Sector", "sector", sortBy, sortDirection, setSortBy, setSortDirection)}</th>
                      <th>{renderSortHeader("Day Vol", "volume", sortBy, sortDirection, setSortBy, setSortDirection)}</th>
                      <th>{renderSortHeader("Change %", "change", sortBy, sortDirection, setSortBy, setSortDirection)}</th>
                      <th>1Y %</th>
                      <th>YTD %</th>
                      <th>{renderSortHeader("TA", "ta", sortBy, sortDirection, setSortBy, setSortDirection)}</th>
                      <th>1D</th>
                      <th>1W</th>
                      <th>{renderSortHeader("FA", "fa", sortBy, sortDirection, setSortBy, setSortDirection)}</th>
                      <th>CANSLIM</th>
                      <th>Accel</th>
                      <th>{renderSortHeader("ARS", "ars", sortBy, sortDirection, setSortBy, setSortDirection)}</th>
                      <th>Daily RS</th>
                      <th>{renderSortHeader("ALS Score", "als", sortBy, sortDirection, setSortBy, setSortDirection)}</th>
                    </tr>
                  </thead>
                  <tbody>
                    {pagedRows.map((row, index) => (
                      <tr key={`${row.ticker}-${index}`}>
                        <td data-label="#">{(normalizedPage - 1) * pageSize + index + 1}</td>
                        <td data-label="Symbol">
                          <Link className="scanner-result-symbol" to={row.chartHref}>
                            <span>{row.ticker}</span>
                            {row.isNew ? <span className="scanner-inline-badge is-new">New</span> : null}
                            {row.setupLabel ? <span className="scanner-inline-badge">{row.setupLabel}</span> : null}
                          </Link>
                        </td>
                        <td data-label="Company">
                          <div className="scanner-result-company">
                            <strong>{row.company || row.ticker}</strong>
                            {row.summary ? <span>{row.summary}</span> : null}
                          </div>
                        </td>
                        <td data-label="Sector">
                          <div className="scanner-result-sector">
                            <strong>{row.sector}</strong>
                            {row.industry && row.industry !== row.sector ? <span>{row.industry}</span> : null}
                          </div>
                        </td>
                        <td data-label="Day Vol">{formatVolume(row.dayVolume)}</td>
                        <td data-label="Change %">{renderChange(row.changePct)}</td>
                        <td data-label="1Y %">{renderChange(row.perfYearPct)}</td>
                        <td data-label="YTD %">{renderChange(row.perfYtdPct)}</td>
                        <td data-label="TA">
                          <span className={`scanner-score-pill ${toneForScore(row.taScore, 10)}`}>{formatTenPointScore(row.taScore)}</span>
                        </td>
                        <td data-label="1D">{row.technicalIndicator1d || "--"}</td>
                        <td data-label="1W">{row.technicalIndicator1w || "--"}</td>
                        <td data-label="FA">
                          <span className={`scanner-score-pill ${toneForScore(row.faScore, 10)}`}>{formatTenPointScore(row.faScore)}</span>
                        </td>
                        <td data-label="CANSLIM">{formatCanslimScore(row.canslimScore, row.canslimMaxScore)}</td>
                        <td data-label="Accel">{formatAccelerationScore(row.accelScore, row.accelLabel)}</td>
                        <td data-label="ARS">{formatPercentScore(row.arsScore)}</td>
                        <td data-label="Daily RS">{formatPercentScore(row.dailyRsRating)}</td>
                        <td data-label="ALS Score" className="scanner-result-als-cell">
                          {formatIntegerScore(row.alsScore)}
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            )}
            <PaginationControls
              currentPage={normalizedPage}
              totalItems={filteredRows.length}
              totalPages={totalPages}
              pageSize={pageSize}
              onPageChange={setCurrentPage}
            />
            <div className="scanner-result-footer">
              <span>Page {normalizedPage} shows {formatCount(pagedRows.length)} stocks</span>
              <div className="scanner-result-preview-pills">
                {pagedRows.slice(0, 5).map((row) => (
                  <span key={`preview-${row.ticker}`} className="scanner-card-pill">
                    {row.ticker}
                  </span>
                ))}
              </div>
            </div>
          </>
        ) : null}
      </section>

      <section className="scanner-result-note panel">
        <div className="scanner-result-note-icon">i</div>
        <div>
          <h2>Scanner Logic Note</h2>
          <p className="panel-copy">{topNote || "Scanner detail uses latest persisted watchlist plus latest rating tables when available."}</p>
        </div>
      </section>
    </div>
  );
}

function buildScannerRow(
  entry: Record<string, unknown>,
  stem: string,
  fundamentalMap: Map<string, TopRatingEntry>,
  technicalMap: Map<string, TopTechnicalRatingEntry>,
  technicalIndicatorMap: Map<string, TopTechnicalIndicatorRatingEntry>,
): ScannerRow {
  const ticker = String(entry.ticker ?? "").trim().toUpperCase();
  const fundamental = fundamentalMap.get(ticker);
  const technical = technicalMap.get(ticker);
  const technicalIndicator = technicalIndicatorMap.get(ticker);
  const directPerfYear = typeof entry.perf_year_pct === "number" ? entry.perf_year_pct : null;
  const directPerfYtd = typeof entry.perf_ytd_pct === "number" ? entry.perf_ytd_pct : null;
  const directCanslimScore = typeof entry.canslim_score === "number" ? entry.canslim_score : null;
  const directCanslimMaxScore = typeof entry.canslim_max_score === "number" ? entry.canslim_max_score : null;
  const directTechnicalOverall = typeof entry.ta_rating === "number" ? entry.ta_rating : null;
  const directFundamentalOverall = typeof entry.fa_rating === "number" ? entry.fa_rating : null;
  const directLeadershipOverall = typeof entry.rs_rating === "number" ? entry.rs_rating : null;
  const directDailyRsRating = typeof entry.daily_rs_rating === "number" ? entry.daily_rs_rating : null;
  const directAccelerationScore = typeof entry.growth_acceleration_score === "number" ? entry.growth_acceleration_score : typeof entry.acceleration_score === "number" ? entry.acceleration_score : null;
  const directAccelerationLabel = typeof entry.growth_acceleration_label === "string" ? entry.growth_acceleration_label : typeof entry.acceleration_label === "string" ? entry.acceleration_label : "";
  const technicalOverall = directTechnicalOverall ?? technical?.overall_rating ?? null;
  const fundamentalOverall = directFundamentalOverall ?? fundamental?.overall_rating ?? null;
  const leadershipOverall = directLeadershipOverall ?? technical?.leadership_score ?? null;
  const directTechnicalIndicator = normalizeDirectTechnicalIndicatorRatings(entry.technical_indicator_ratings);
  const dailyIndicator = directTechnicalIndicator["1d"] ?? technicalIndicator?.daily;
  const weeklyIndicator = directTechnicalIndicator["1w"] ?? technicalIndicator?.weekly;
  return {
    ticker,
    company: String(entry.company_name ?? ""),
    sector: String(entry.sector ?? fundamental?.sector ?? "Unknown sector"),
    industry: String(entry.industry ?? fundamental?.industry ?? ""),
    summary: String(entry.summary ?? ""),
    setupLabel: String(entry.setup_label ?? ""),
    chartHref: buildChartHref(ticker, stem),
    dayVolume: resolveDisplayVolume(entry),
    changePct: resolveDisplayChangePct(entry),
    perfYearPct: directPerfYear ?? fundamental?.perf_year_pct ?? null,
    perfYtdPct: directPerfYtd ?? fundamental?.perf_ytd_pct ?? null,
    canslimScore: directCanslimScore,
    canslimMaxScore: directCanslimMaxScore,
    accelScore: directAccelerationScore,
    accelLabel: directAccelerationLabel,
    taScore: technicalOverall != null ? technicalOverall / 10 : null,
    faScore: fundamentalOverall != null ? fundamentalOverall / 10 : null,
    arsScore: leadershipOverall,
    dailyRsRating: directDailyRsRating ?? technical?.daily_rs_rating ?? null,
    alsScore: averagePresent([technicalOverall, fundamentalOverall, leadershipOverall]),
    technicalIndicator1d: dailyIndicator?.rating_label ?? "",
    technicalIndicator1w: weeklyIndicator?.rating_label ?? "",
    isNew: Boolean(entry.is_new),
  };
}

function normalizeDirectTechnicalIndicatorRatings(value: unknown) {
  if (!value || typeof value !== "object") {
    return {} as Record<string, TechnicalIndicatorRatingCell>;
  }
  return value as Record<string, TechnicalIndicatorRatingCell>;
}

function buildChartHref(ticker: string, stem: string) {
  const params = new URLSearchParams();
  params.set("ticker", ticker);
  if (stem) {
    params.set("stem", stem);
  }
  return `/charts?${params.toString()}`;
}

function coerceOptionalNumber(value: unknown) {
  if (value === null || value === undefined || value === "") {
    return null;
  }
  const numeric = Number(value);
  return Number.isFinite(numeric) ? numeric : null;
}

function averagePresent(values: Array<number | null | undefined>) {
  const numbers = values.filter((value): value is number => typeof value === "number" && Number.isFinite(value));
  if (numbers.length === 0) {
    return null;
  }
  return numbers.reduce((sum, value) => sum + value, 0) / numbers.length;
}

function resolveDisplayChangePct(entry: Record<string, unknown> | null | undefined): number | null {
  const candidates = [entry?.price_change_pct, entry?.daily_change_pct, entry?.change_pct, entry?.pct_change];
  for (const candidate of candidates) {
    const value = coerceOptionalNumber(candidate);
    if (value != null) {
      return value;
    }
  }
  return null;
}

function resolveDisplayVolume(entry: Record<string, unknown> | null | undefined): number | null {
  const candidates = [entry?.current_volume, entry?.breakout_day_volume, entry?.day_volume, entry?.volume];
  for (const candidate of candidates) {
    const value = coerceOptionalNumber(candidate);
    if (value != null) {
      return value;
    }
  }
  return null;
}

function compareScannerRows(left: ScannerRow, right: ScannerRow, sortBy: SortKey, sortDirection: SortDirection) {
  if (sortBy === "ticker") {
    return compareText(left.ticker, right.ticker, sortDirection);
  }
  if (sortBy === "company") {
    return compareText(left.company, right.company, sortDirection) || left.ticker.localeCompare(right.ticker);
  }
  if (sortBy === "sector") {
    return compareText(left.sector, right.sector, sortDirection) || left.ticker.localeCompare(right.ticker);
  }
  if (sortBy === "volume") {
    return compareNullableNumber(left.dayVolume, right.dayVolume, sortDirection) || left.ticker.localeCompare(right.ticker);
  }
  if (sortBy === "change") {
    return compareNullableNumber(left.changePct, right.changePct, sortDirection) || left.ticker.localeCompare(right.ticker);
  }
  if (sortBy === "ta") {
    return compareNullableNumber(left.taScore, right.taScore, sortDirection) || left.ticker.localeCompare(right.ticker);
  }
  if (sortBy === "fa") {
    return compareNullableNumber(left.faScore, right.faScore, sortDirection) || left.ticker.localeCompare(right.ticker);
  }
  if (sortBy === "ars") {
    return compareNullableNumber(left.arsScore, right.arsScore, sortDirection) || left.ticker.localeCompare(right.ticker);
  }
  return compareNullableNumber(left.alsScore, right.alsScore, sortDirection) || left.ticker.localeCompare(right.ticker);
}

function compareNullableNumber(left: number | null, right: number | null, sortDirection: SortDirection) {
  const missingSentinel = sortDirection === "asc" ? Number.POSITIVE_INFINITY : Number.NEGATIVE_INFINITY;
  const normalizedLeft = typeof left === "number" ? left : missingSentinel;
  const normalizedRight = typeof right === "number" ? right : missingSentinel;
  return sortDirection === "asc" ? normalizedLeft - normalizedRight : normalizedRight - normalizedLeft;
}

function compareText(left: string, right: string, sortDirection: SortDirection) {
  return sortDirection === "asc" ? left.localeCompare(right) : right.localeCompare(left);
}

function formatTenPointScore(value: number | null) {
  return value == null ? "--" : value.toFixed(1);
}

function formatVolume(value: number | null) {
  if (value == null) {
    return "--";
  }
  const absolute = Math.abs(value);
  if (absolute >= 1_000_000_000) {
    return `${(value / 1_000_000_000).toFixed(2)}B`;
  }
  if (absolute >= 1_000_000) {
    return `${(value / 1_000_000).toFixed(2)}M`;
  }
  if (absolute >= 1_000) {
    return `${(value / 1_000).toFixed(1)}K`;
  }
  return value.toFixed(0);
}

function renderChange(value: number | null) {
  if (value == null) {
    return <span className="ticker-change neutral">--</span>;
  }
  const tone = value >= 0 ? "up" : "down";
  return (
    <span className={`ticker-change ${tone}`}>
      {value >= 0 ? "+" : ""}
      {value.toFixed(2)}%
    </span>
  );
}

function formatPercentScore(value: number | null) {
  return value == null ? "--" : `${Math.round(value)}%`;
}

function formatIntegerScore(value: number | null) {
  return value == null ? "--" : `${Math.round(value)}`;
}

function formatCanslimScore(score: number | null, maxScore: number | null) {
  if (score == null) {
    return "--";
  }
  return `${score}/${maxScore ?? 14}`;
}

function formatAccelerationScore(score: number | null, label: string) {
  if (score == null) {
    return "--";
  }
  return label ? `${Math.round(score)} ${label}` : `${Math.round(score)}`;
}

function toneForScore(value: number | null, max: number) {
  if (value == null) {
    return "is-neutral";
  }
  if (value >= max * 0.8) {
    return "is-strong";
  }
  if (value >= max * 0.6) {
    return "is-warm";
  }
  return "is-neutral";
}

function labelForSort(sortBy: SortKey) {
  switch (sortBy) {
    case "company":
      return "Company";
    case "sector":
      return "Sector";
    case "volume":
      return "Day Vol";
    case "change":
      return "Change %";
    case "ta":
      return "TA";
    case "fa":
      return "FA";
    case "ars":
      return "ARS";
    case "ticker":
      return "Ticker";
    default:
      return "ALS Score";
  }
}

function renderSortHeader(
  label: string,
  key: SortKey,
  sortBy: SortKey,
  sortDirection: SortDirection,
  setSortBy: (value: SortKey) => void,
  setSortDirection: (value: SortDirection) => void,
) {
  const isActive = sortBy === key;
  const indicator = isActive ? (sortDirection === "asc" ? " ↑" : " ↓") : "";
  return (
    <button
      type="button"
      className={`ghost-button scanner-result-sort-button${isActive ? " is-active" : ""}`}
      onClick={() => {
        if (isActive) {
          setSortDirection(sortDirection === "asc" ? "desc" : "asc");
          return;
        }
        setSortBy(key);
        setSortDirection(key === "ticker" || key === "company" || key === "sector" ? "asc" : "desc");
      }}
    >
      {label}
      {indicator}
    </button>
  );
}

function csvValue(value: string) {
  const normalized = String(value ?? "");
  if (!normalized.includes(",") && !normalized.includes('"') && !normalized.includes("\n")) {
    return normalized;
  }
  return `"${normalized.split('"').join('""')}"`;
}

function buildMiniChartCandles(payload: WatchlistChartResponse | null | undefined): CandlePoint[] {
  if (!payload) {
    return [];
  }
  return payload.candles.map((item, index) => ({
    ...item,
    volume: payload.volume[index]?.value ?? 0,
  }));
}

function computeDayChangePct(candles: CandlePoint[]) {
  const latest = candles[candles.length - 1];
  const previous = candles[candles.length - 2];
  if (!latest || !previous || previous.close <= 0) {
    return null;
  }
  return ((latest.close - previous.close) / previous.close) * 100;
}
