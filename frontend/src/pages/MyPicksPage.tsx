import { useEffect, useMemo, useState, type FormEvent, type ReactNode } from "react";
import { Link, useSearchParams } from "react-router-dom";
import { LoadingBlock } from "../components/LoadingBlock";
import { PaginationControls } from "../components/PaginationControls";
import { ScannerMiniChart } from "../components/ScannerMiniChart";
import { fetchJson } from "../lib/api";
import { aggregateCandlesWeekly, buildChartCandles, buildExponentialMovingAverage } from "../lib/chartData";
import { formatCount, formatLocalDate, formatLocalDateTime, humanizePositionAction, humanizePositionExtension, humanizePositionTrend, toneForPositionAction } from "../lib/format";
import type { FundamentalChecklistItem, MyPickRow, MyPicksContextResponse, WatchlistChartResponse } from "../lib/types";

const EMPTY_CONTEXT: MyPicksContextResponse = {
  database_configured: false,
  total_count: 0,
  rows: [],
  available_added_dates: [],
  fundamental_checklist: [],
  fundamental_summary: [],
};
const LIST_PAGE_SIZE = 50;
const CHART_PAGE_SIZE = 9;
const CARD_PAGE_SIZE = 30;
type MyPicksViewMode = "list" | "charts" | "cards" | "sectors" | "position";
type MyPicksChartRange = "3m" | "6m" | "1y";
type MyPicksChartType = "candles" | "bars" | "line";
type MyPicksChartTimeframe = "daily" | "weekly";
type MyPicksSortKey =
  | "added_at"
  | "ticker"
  | "sector_industry"
  | "latest_close"
  | "change_1d_pct"
  | "perf_ytd_pct"
  | "change_since_added_pct"
  | "change_from_52wk_low_pct"
  | "bollinger_band_status"
  | "ema9_tested_since_added"
  | "ema21_tested_since_added"
  | "sma50_tested_since_added"
  | "price_above_sma50"
  | "daily_ema9"
  | "distance_to_ema9_pct"
  | "daily_ema21"
  | "distance_to_ema21_pct"
  | "fundamental_rating"
  | "trend_template"
  | "leadership_score"
  | "canslim_score"
  | "vcp_score"
  | "technical_indicator_1d"
  | "technical_indicator_1w"
  | "position_action"
  | "position_action_score"
  | "recent_signal_count"
  | "latest_signal_date";
type SortDirection = "desc" | "asc";

export function MyPicksPage() {
  const [searchParams, setSearchParams] = useSearchParams();
  const [context, setContext] = useState<MyPicksContextResponse>(EMPTY_CONTEXT);
  const [isLoading, setIsLoading] = useState(true);
  const [isSaving, setIsSaving] = useState(false);
  const [notice, setNotice] = useState("");
  const [ticker, setTicker] = useState("");
  const [notes, setNotes] = useState("");
  const [search, setSearch] = useState("");
  const [groupByDate, setGroupByDate] = useState(false);
  const [sortBy, setSortBy] = useState<MyPicksSortKey>("added_at");
  const [sortDirection, setSortDirection] = useState<SortDirection>("desc");
  const [viewMode, setViewMode] = useState<MyPicksViewMode>("list");
  const [currentPage, setCurrentPage] = useState(1);
  const [chartPayloads, setChartPayloads] = useState<Record<string, WatchlistChartResponse | null | undefined>>({});
  const [chartErrors, setChartErrors] = useState<Record<string, string>>({});
  const [chartLoadingTickers, setChartLoadingTickers] = useState<Record<string, boolean>>({});
  const [checklistSaving, setChecklistSaving] = useState<Record<string, boolean>>({});
  const [visibleSectorCounts, setVisibleSectorCounts] = useState<Record<string, number>>({});
  const [chartRange, setChartRange] = useState<MyPicksChartRange>("6m");
  const [chartType, setChartType] = useState<MyPicksChartType>("bars");
  const [chartTimeframe, setChartTimeframe] = useState<MyPicksChartTimeframe>("daily");
  const [showChartVolume, setShowChartVolume] = useState(true);
  const selectedBoardTicker = searchParams.get("ticker")?.trim().toUpperCase() || "";
  const isColumnView = viewMode === "sectors" || viewMode === "position";

  const selectBoardTicker = (nextTicker: string) => {
    setSearchParams((current) => {
      const next = new URLSearchParams(current);
      next.set("ticker", nextTicker.trim().toUpperCase());
      return next;
    }, { replace: true });
  };

  const loadPicks = () => {
    setIsLoading(true);
    void fetchJson<MyPicksContextResponse>("/api/admin/my-picks")
      .then((payload) => {
        setContext({
          ...EMPTY_CONTEXT,
          ...payload,
          rows: Array.isArray(payload.rows) ? payload.rows : [],
          available_added_dates: Array.isArray(payload.available_added_dates) ? payload.available_added_dates : [],
          fundamental_checklist: Array.isArray(payload.fundamental_checklist) ? payload.fundamental_checklist : [],
          fundamental_summary: Array.isArray(payload.fundamental_summary) ? payload.fundamental_summary : [],
        });
      })
      .catch((error) => {
        setContext(EMPTY_CONTEXT);
        setNotice(error instanceof Error ? error.message : "Failed to load My Picks.");
      })
      .finally(() => setIsLoading(false));
  };

  useEffect(() => {
    loadPicks();
  }, []);

  const filteredRows = useMemo(() => {
    const query = search.trim().toLowerCase();
    const rows = context.rows.filter((row) => {
      if (!query) {
        return true;
      }
      return [
        row.ticker,
        row.sector ?? "",
        row.industry ?? "",
        row.notes ?? "",
        row.recent_signals.map((item) => item.strategy_id).join(" "),
      ]
        .join(" ")
        .toLowerCase()
        .includes(query);
    });
    return [...rows].sort((left, right) => compareMyPickRows(left, right, sortBy, sortDirection));
  }, [context.rows, search, sortBy, sortDirection]);

  const groupedRows = useMemo(() => {
    const groups = new Map<string, MyPickRow[]>();
    filteredRows.forEach((row) => {
      const key = row.added_date || "Unknown date";
      groups.set(key, [...(groups.get(key) ?? []), row]);
    });
    return Array.from(groups.entries()).map(([label, rows]) => ({ label, rows }));
  }, [filteredRows]);

  useEffect(() => {
    setCurrentPage(1);
  }, [groupByDate, search, sortBy, sortDirection, viewMode]);

  const pageSize = viewMode === "charts" ? CHART_PAGE_SIZE : viewMode === "cards" ? CARD_PAGE_SIZE : LIST_PAGE_SIZE;
  const totalPages = Math.max(1, Math.ceil(filteredRows.length / pageSize));
  const normalizedPage = Math.min(currentPage, totalPages);
  const pagedRows = useMemo(() => {
    const startIndex = (normalizedPage - 1) * pageSize;
    return filteredRows.slice(startIndex, startIndex + pageSize);
  }, [filteredRows, normalizedPage, pageSize]);
  const groupedPagedRows = useMemo(() => {
    const groups = new Map<string, MyPickRow[]>();
    pagedRows.forEach((row) => {
      const key = row.added_date || "Unknown date";
      groups.set(key, [...(groups.get(key) ?? []), row]);
    });
    return Array.from(groups.entries()).map(([label, rows]) => ({ label, rows }));
  }, [pagedRows]);
  const pagedTickerKey = useMemo(() => pagedRows.map((row) => row.ticker).join("|"), [pagedRows]);
  const selectedBoardRow = useMemo(
    () => filteredRows.find((row) => row.ticker === selectedBoardTicker) ?? null,
    [filteredRows, selectedBoardTicker],
  );

  useEffect(() => {
    if (currentPage !== normalizedPage) {
      setCurrentPage(normalizedPage);
    }
  }, [currentPage, normalizedPage]);

  useEffect(() => {
    const requestedRows = viewMode === "charts"
      ? pagedRows
      : isColumnView && selectedBoardRow
        ? [selectedBoardRow]
        : [];
    if (requestedRows.length === 0) {
      return;
    }
    const missingTickers = requestedRows
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
        const payload = await fetchJson<WatchlistChartResponse>(`/api/charts/${ticker}?period=18mo`);
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
  }, [isColumnView, pagedTickerKey, selectedBoardTicker, viewMode]);

  useEffect(() => {
    if (!isColumnView || filteredRows.length === 0) return;
    if (!filteredRows.some((row) => row.ticker === selectedBoardTicker)) {
      selectBoardTicker(filteredRows[0].ticker);
    }
  }, [filteredRows, isColumnView, selectedBoardTicker]);

  const handleSubmit = async (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    setIsSaving(true);
    setNotice("");
    try {
      const payload = await fetchJson<{ ok: boolean; pick: MyPickRow }>("/api/admin/my-picks", {
        method: "POST",
        body: JSON.stringify({ ticker, notes }),
      });
      setNotice(`Added ${payload.pick.ticker} to My Picks.`);
      setTicker("");
      setNotes("");
      setContext((current) => {
        const rows = [payload.pick, ...current.rows.filter((entry) => entry.id !== payload.pick.id)];
        return {
          ...current,
          rows,
          total_count: rows.length,
          available_added_dates: Array.from(new Set(rows.map((entry) => entry.added_date).filter((value): value is string => Boolean(value)))),
        };
      });
    } catch (error) {
      setNotice(error instanceof Error ? error.message : "Failed to add pick.");
    } finally {
      setIsSaving(false);
    }
  };


  const handleChecklistToggle = async (row: MyPickRow, item: FundamentalChecklistItem, checked: boolean) => {
    const savingKey = `${row.id}:${item.key}`;
    setChecklistSaving((current) => ({ ...current, [savingKey]: true }));
    setNotice("");
    try {
      const payload = await fetchJson<{ ok: boolean; pick: MyPickRow }>(`/api/admin/my-picks/${row.id}/checklist`, {
        method: "POST",
        body: JSON.stringify({ key: item.key, checked }),
      });
      setContext((current) => ({
        ...current,
        rows: current.rows.map((entry) => (entry.id === row.id ? payload.pick : entry)),
      }));
    } catch (error) {
      setNotice(error instanceof Error ? error.message : "Failed to update checklist item.");
    } finally {
      setChecklistSaving((current) => {
        const next = { ...current };
        delete next[savingKey];
        return next;
      });
    }
  };

  const handleDelete = async (row: MyPickRow) => {
    if (!window.confirm(`Delete ${row.ticker} from My Picks?`)) {
      return;
    }
    setIsSaving(true);
    setNotice("");
    try {
      await fetchJson<{ ok: boolean }>(`/api/admin/my-picks/${row.id}/delete`, {
        method: "POST",
      });
      setContext((current) => {
        const rows = current.rows.filter((entry) => entry.id !== row.id);
        return {
          ...current,
          rows,
          total_count: rows.length,
          available_added_dates: Array.from(new Set(rows.map((entry) => entry.added_date).filter((value): value is string => Boolean(value)))),
        };
      });
      setNotice(`Deleted ${row.ticker}.`);
    } catch (error) {
      setNotice(error instanceof Error ? error.message : "Failed to delete pick.");
    } finally {
      setIsSaving(false);
    }
  };

  if (isLoading) {
    return <LoadingBlock label="Loading My Picks..." />;
  }

  return (
    <div className="page-grid earnings-board weekly-watchlist-board">
      <section className="earnings-board-hero">
        <div className="earnings-board-hero-copy">
          <span className="earnings-board-kicker">Admin Watch Board</span>
          <h1>My Picks</h1>
          <p className="panel-copy">Personal admin list for tickers worth tracking. Default view shows every pick sorted by added time, with ratings and recent screener signal context inline.</p>
        </div>
        <div className="earnings-board-metrics">
          <div className="earnings-metric">
            <span className="eyebrow">Rows</span>
            <strong>{formatCount(filteredRows.length)}</strong>
          </div>
          <div className="earnings-metric">
            <span className="eyebrow">Groups</span>
            <strong>{groupByDate ? formatCount(groupedRows.length) : "Off"}</strong>
          </div>
          <div className="earnings-metric">
            <span className="eyebrow">Sort</span>
            <strong>{sortBy === "ticker" ? (sortDirection === "asc" ? "Ticker A-Z" : "Ticker Z-A") : sortDirection === "desc" ? "Newest first" : "Oldest first"}</strong>
          </div>
          <div className="earnings-metric">
            <span className="eyebrow">View</span>
            <strong>{viewMode === "charts" ? "Charts" : viewMode === "cards" ? "Cards" : viewMode === "sectors" ? "Sectors" : viewMode === "position" ? "Position" : "List"}</strong>
          </div>
          <div className="earnings-metric">
            <span className="eyebrow">Latest Added</span>
            <strong>{formatLocalDateTime(filteredRows[0]?.added_at)}</strong>
          </div>
        </div>
      </section>

      <section className="panel earnings-filter-console">
        <form className="earnings-filter-console-row weekly-watchlist-console-row" onSubmit={(event) => void handleSubmit(event)}>
          <label className="field">
            <span>Ticker</span>
            <input value={ticker} onChange={(event) => setTicker(event.target.value)} placeholder="NVDA" />
          </label>
          <label className="field" style={{ minWidth: "18rem", flex: 1 }}>
            <span>Notes</span>
            <input value={notes} onChange={(event) => setNotes(event.target.value)} placeholder="Why this name belongs here" />
          </label>
          <div className="weekly-watchlist-actions">
            <button className="primary-button" type="submit" disabled={isSaving}>
              {isSaving ? "Saving..." : "Add Pick"}
            </button>
          </div>
        </form>
        <div className="earnings-filter-console-row weekly-watchlist-console-row">
          <label className="field">
            <span>Search</span>
            <input value={search} onChange={(event) => setSearch(event.target.value)} placeholder="Ticker, sector, signal, note" />
          </label>
          <label className="field">
            <span>Group</span>
            <select value={groupByDate ? "date" : "flat"} onChange={(event) => setGroupByDate(event.target.value === "date")}>
              <option value="flat">All rows</option>
              <option value="date">By added date</option>
            </select>
          </label>
          <label className="field">
            <span>Sort By</span>
            <select
              value={sortBy}
              onChange={(event) => {
                const nextSortBy = event.target.value as MyPicksSortKey;
                setSortBy(nextSortBy);
                setSortDirection(defaultSortDirection(nextSortBy));
              }}
            >
              <option value="added_at">Added time</option>
              <option value="ticker">Ticker name</option>
              <option value="sector_industry">Sector / Industry</option>
              <option value="latest_close">Close</option>
              <option value="change_1d_pct">1D %</option>
              <option value="perf_ytd_pct">YTD %</option>
              <option value="change_since_added_pct">Since Add %</option>
              <option value="change_from_52wk_low_pct">From 52W Low %</option>
              <option value="bollinger_band_status">Bollinger</option>
              <option value="ema9_tested_since_added">EMA9 Test</option>
              <option value="ema21_tested_since_added">EMA21 Test</option>
              <option value="sma50_tested_since_added">Retest 50 SMA</option>
              <option value="price_above_sma50">Above 50 SMA</option>
              <option value="daily_ema9">EMA9</option>
              <option value="distance_to_ema9_pct">vs EMA9</option>
              <option value="daily_ema21">EMA21</option>
              <option value="distance_to_ema21_pct">vs EMA21</option>
              <option value="fundamental_rating">FA</option>
              <option value="trend_template">Trend Template</option>
              <option value="leadership_score">RS Rating</option>
              <option value="canslim_score">CAN V2</option>
              <option value="vcp_score">VCP</option>
              <option value="technical_indicator_1d">1D</option>
              <option value="technical_indicator_1w">1W</option>
              <option value="position_action">Decision</option>
              <option value="position_action_score">Decision Score</option>
              <option value="recent_signal_count">Signals</option>
              <option value="latest_signal_date">Latest Signal</option>
            </select>
          </label>
          <label className="field">
            <span>Order</span>
            <select value={sortDirection} onChange={(event) => setSortDirection(event.target.value as SortDirection)}>
              {isAlphabeticalSort(sortBy) ? (
                <>
                  <option value="asc">A to Z</option>
                  <option value="desc">Z to A</option>
                </>
              ) : isDateSort(sortBy) ? (
                <>
                  <option value="desc">Newest first</option>
                  <option value="asc">Oldest first</option>
                </>
              ) : (
                <>
                  <option value="desc">High to low</option>
                  <option value="asc">Low to high</option>
                </>
              )}
            </select>
          </label>
          <label className="field">
            <span>View</span>
            <select value={viewMode} onChange={(event) => setViewMode(event.target.value as MyPicksViewMode)}>
              <option value="list">List</option>
              <option value="charts">Charts</option>
              <option value="cards">Cards</option>
              <option value="sectors">Sectors</option>
              <option value="position">Position Map</option>
            </select>
          </label>
          <div className="weekly-watchlist-actions">
            <Link className="ghost-button" to="/ratings">
              Open Ratings
            </Link>
          </div>
        </div>
        {notice ? <p className="panel-copy earnings-console-note">{notice}</p> : null}
        {!context.database_configured ? <p className="panel-copy earnings-console-note">Database is not configured for My Picks storage.</p> : null}
      </section>


      <section className="panel earnings-filter-console">
        <div className="panel-head earnings-calendar-head">
          <div>
            <h2>Fundamental Checklist</h2>
            <span className="eyebrow">Manual admin review guide</span>
          </div>
        </div>
        <p className="panel-copy">Use this checklist to manually confirm whether each ticker matches the transcript's fundamental-analysis process before you keep sizing it up technically.</p>
        <div className="detail-subsection">
          {(context.fundamental_summary ?? []).map((item) => (
            <p key={item} className="panel-copy">- {item}</p>
          ))}
        </div>
        <div className="data-table-responsive">
          <table className="data-table">
            <thead>
              <tr>
                <th>Checklist Item</th>
                <th>Instruction</th>
              </tr>
            </thead>
            <tbody>
              {(context.fundamental_checklist ?? []).map((item) => (
                <tr key={item.key}>
                  <td>{item.label}</td>
                  <td>{item.description}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </section>

      <section className="panel earnings-calendar-panel">
        <div className="panel-head earnings-calendar-head">
          <div>
            <h2>{viewMode === "charts" ? "Chart View" : viewMode === "cards" ? "Card View" : viewMode === "sectors" ? "Sector View" : viewMode === "position" ? "Position Map" : groupByDate ? "Grouped Picks" : "All Picks"}</h2>
            <span className="eyebrow">{formatCount(filteredRows.length)} names</span>
          </div>
        </div>
        {!isColumnView && filteredRows.length > 0 ? (
          <PaginationControls
            currentPage={normalizedPage}
            totalItems={filteredRows.length}
            totalPages={totalPages}
            pageSize={pageSize}
            onPageChange={setCurrentPage}
          />
        ) : null}
        {viewMode === "charts" && filteredRows.length === 0 ? <p className="panel-copy">No picks match current filter.</p> : null}
        {viewMode === "charts" && filteredRows.length > 0 ? (
          <div className="scanner-result-chart-grid is-3-col">
            {pagedRows.map((row) => {
              const chartPayload = chartPayloads[row.ticker];
              const chartCandles = buildChartCandles(chartPayload);
              const isChartLoading = Boolean(chartLoadingTickers[row.ticker]);
              const chartError = chartErrors[row.ticker];
              const latestCandle = chartCandles[chartCandles.length - 1] ?? null;
              return (
                <article key={row.id} className="scanner-chart-card scanner-top-hit-chart-card">
                  <div className="scanner-chart-card-header">
                    <div className="scanner-chart-card-heading">
                      <div className="scanner-chart-card-symbol-row">
                        <Link className="scanner-result-symbol" to={`/charts?ticker=${encodeURIComponent(row.ticker)}`}>
                          <span>{row.ticker}</span>
                        </Link>
                      </div>
                      <strong>{[row.sector, row.industry].filter(Boolean).join(" / ") || "Watch name"}</strong>
                      <span>Added {formatLocalDateTime(row.added_at)}</span>
                    </div>
                    <div className="scanner-chart-card-price">
                      <strong>{latestCandle ? latestCandle.close.toFixed(2) : formatPrice(row.latest_close)}</strong>
                      <span className={toneForPercent(row.distance_to_ema21_pct)}>
                        {formatSignedPercent(row.distance_to_ema21_pct)} vs EMA21
                      </span>
                    </div>
                  </div>
                  <div className="scanner-chart-card-score-row">
                    <span className={`scanner-score-pill ${toneForScore(row.fundamental_rating, 100)}`}>FA {formatScoreInteger(row.fundamental_rating)}</span>
                    <span className={`scanner-score-pill ${toneForTrendTemplate(row)}`}>TT {formatTrendTemplate(row)}</span>
                    <span className={`scanner-score-pill ${toneForScore(row.leadership_score, 100)}`}>RS {formatScoreInteger(row.leadership_score)}</span>
                    <span className={`scanner-score-pill ${toneForScore(row.canslim_score, row.canslim_max_score ?? 14)}`}>CAN V2 {formatScoreFraction(row.canslim_score, row.canslim_max_score)}</span>
                    <span className={`scanner-score-pill ${toneForScore(row.vcp_score, 100)}`}>VCP {formatScore(row.vcp_score)}</span>
                  </div>
                  <div className="scanner-chart-card-body">
                    {isChartLoading ? <LoadingBlock label={`Loading ${row.ticker} chart...`} /> : null}
                    {!isChartLoading && chartError ? <p className="panel-copy">{chartError}</p> : null}
                    {!isChartLoading && !chartError && chartCandles.length === 0 ? <p className="panel-copy">No chart data.</p> : null}
                    {!isChartLoading && !chartError && chartCandles.length > 0 ? (
                      <ScannerMiniChart
                        ticker={row.ticker}
                        candles={chartCandles}
                        ema9={buildExponentialMovingAverage(chartCandles, 9)}
                        ema21={chartPayload?.ema21 ?? buildExponentialMovingAverage(chartCandles, 21)}
                      />
                    ) : null}
                  </div>
                  <div className="scanner-chart-card-footer">
                    <span>
                      EMA9 {formatPrice(row.daily_ema9)} | EMA21 {formatPrice(row.daily_ema21)}
                    </span>
                    <Link to={`/charts?ticker=${encodeURIComponent(row.ticker)}`}>Analyze Full Chart</Link>
                  </div>
                </article>
              );
            })}
          </div>
        ) : null}
        {viewMode === "cards" && filteredRows.length === 0 ? <p className="panel-copy">No picks match current filter.</p> : null}
        {viewMode === "cards" && filteredRows.length > 0 ? (
          <div className="my-picks-card-grid">
            {pagedRows.map((row) => (
              <MyPickGuruCard key={row.id} row={row} isSaving={isSaving} onDelete={handleDelete} />
            ))}
          </div>
        ) : null}
        {viewMode === "sectors" && filteredRows.length === 0 ? <p className="panel-copy">No picks match current filter.</p> : null}
        {viewMode === "sectors" && filteredRows.length > 0 ? (
          <MyPicksBoardChartWorkspace
            row={selectedBoardRow}
            chartPayload={chartPayloads[selectedBoardTicker]}
            chartError={chartErrors[selectedBoardTicker]}
            isChartLoading={Boolean(chartLoadingTickers[selectedBoardTicker])}
            range={chartRange}
            chartType={chartType}
            timeframe={chartTimeframe}
            showVolume={showChartVolume}
            onRangeChange={setChartRange}
            onChartTypeChange={setChartType}
            onTimeframeChange={setChartTimeframe}
            onShowVolumeChange={setShowChartVolume}
          >
            <MyPicksSectorBoard
              rows={filteredRows}
              isSaving={isSaving}
              selectedTicker={selectedBoardTicker}
              visibleCounts={visibleSectorCounts}
              onDelete={handleDelete}
              onSelectTicker={selectBoardTicker}
              onLoadMore={(sector) => setVisibleSectorCounts((current) => ({ ...current, [sector]: (current[sector] ?? CARD_PAGE_SIZE) + CARD_PAGE_SIZE }))}
            />
          </MyPicksBoardChartWorkspace>
        ) : null}
        {viewMode === "position" && filteredRows.length === 0 ? <p className="panel-copy">No picks match current filter.</p> : null}
        {viewMode === "position" && filteredRows.length > 0 ? (
          <MyPicksBoardChartWorkspace
            row={selectedBoardRow}
            chartPayload={chartPayloads[selectedBoardTicker]}
            chartError={chartErrors[selectedBoardTicker]}
            isChartLoading={Boolean(chartLoadingTickers[selectedBoardTicker])}
            range={chartRange}
            chartType={chartType}
            timeframe={chartTimeframe}
            showVolume={showChartVolume}
            onRangeChange={setChartRange}
            onChartTypeChange={setChartType}
            onTimeframeChange={setChartTimeframe}
            onShowVolumeChange={setShowChartVolume}
          >
            <MyPicksPositionBoard
              rows={filteredRows}
              isSaving={isSaving}
              selectedTicker={selectedBoardTicker}
              onDelete={handleDelete}
              onSelectTicker={selectBoardTicker}
            />
          </MyPicksBoardChartWorkspace>
        ) : null}
        {viewMode === "list" && !groupByDate && filteredRows.length === 0 ? <p className="panel-copy">No picks match current filter.</p> : null}
        {viewMode === "list" && !groupByDate && filteredRows.length > 0 ? <PicksTable rows={pagedRows} checklistItems={context.fundamental_checklist ?? []} checklistSaving={checklistSaving} onToggleChecklist={handleChecklistToggle} onDelete={handleDelete} isSaving={isSaving} sortBy={sortBy} sortDirection={sortDirection} setSortBy={setSortBy} setSortDirection={setSortDirection} /> : null}
        {viewMode === "list" && groupByDate && groupedRows.length === 0 ? <p className="panel-copy">No grouped picks match current filter.</p> : null}
        {viewMode === "list" && groupByDate
          ? groupedPagedRows.map((group) => (
              <div key={group.label} className="detail-subsection">
                <div className="panel-head earnings-calendar-head">
                  <div>
                    <h3>{formatLocalDate(group.label)}</h3>
                    <span className="eyebrow">{formatCount(group.rows.length)} names</span>
                  </div>
                </div>
                <PicksTable rows={group.rows} checklistItems={context.fundamental_checklist ?? []} checklistSaving={checklistSaving} onToggleChecklist={handleChecklistToggle} onDelete={handleDelete} isSaving={isSaving} sortBy={sortBy} sortDirection={sortDirection} setSortBy={setSortBy} setSortDirection={setSortDirection} />
              </div>
            ))
          : null}
        {!isColumnView && filteredRows.length > 0 ? (
          <PaginationControls
            currentPage={normalizedPage}
            totalItems={filteredRows.length}
            totalPages={totalPages}
            pageSize={pageSize}
            onPageChange={setCurrentPage}
          />
        ) : null}
      </section>
    </div>
  );
}

const MY_PICKS_SECTOR_ACCENTS = ["amber", "yellow", "teal", "blue", "sky", "cyan", "gold"];
const MY_PICKS_POSITION_BUCKETS = [
  ["extended", "Extended"],
  ["above_ema10", "Above EMA10"],
  ["ema10_ema21", "EMA10–EMA21"],
  ["ema21_sma50", "EMA21–SMA50"],
  ["below_sma50_above_sma200", "Below SMA50 · Above SMA200"],
  ["below_sma200", "Below SMA200"],
  ["no_data", "No Data"],
] as const;

function MyPicksBoardChartWorkspace({
  children,
  row,
  chartPayload,
  chartError,
  isChartLoading,
  range,
  chartType,
  timeframe,
  showVolume,
  onRangeChange,
  onChartTypeChange,
  onTimeframeChange,
  onShowVolumeChange,
}: {
  children: ReactNode;
  row: MyPickRow | null;
  chartPayload: WatchlistChartResponse | null | undefined;
  chartError: string | undefined;
  isChartLoading: boolean;
  range: MyPicksChartRange;
  chartType: MyPicksChartType;
  timeframe: MyPicksChartTimeframe;
  showVolume: boolean;
  onRangeChange: (range: MyPicksChartRange) => void;
  onChartTypeChange: (chartType: MyPicksChartType) => void;
  onTimeframeChange: (timeframe: MyPicksChartTimeframe) => void;
  onShowVolumeChange: (showVolume: boolean) => void;
}) {
  const dailyCandles = buildChartCandles(chartPayload);
  const allCandles = timeframe === "weekly" ? aggregateCandlesWeekly(dailyCandles) : dailyCandles;
  const rangeSize = range === "3m" ? (timeframe === "weekly" ? 14 : 66) : range === "6m" ? (timeframe === "weekly" ? 27 : 132) : (timeframe === "weekly" ? 53 : 264);
  const chartCandles = allCandles.slice(-rangeSize);
  const firstChartTime = chartCandles[0]?.time;
  const ema9 = buildExponentialMovingAverage(allCandles, 9).filter((point) => !firstChartTime || point.time >= firstChartTime);
  const ema21 = buildExponentialMovingAverage(allCandles, 21).filter((point) => !firstChartTime || point.time >= firstChartTime);
  return (
    <div className="board-chart-split">
      <div className="board-chart-split-board">{children}</div>
      <aside className="board-chart-panel" aria-label="Selected My Pick chart">
        {!row ? <p className="panel-copy">Select a ticker card to review its chart.</p> : <>
          <div className="board-chart-panel-head">
            <div>
              <Link to={`/charts?ticker=${encodeURIComponent(row.ticker)}`}>{row.ticker}</Link>
              <strong title={[row.sector, row.industry].filter(Boolean).join(" / ")}>{[row.sector, row.industry].filter(Boolean).join(" / ") || "My Pick"}</strong>
            </div>
            <div>
              <strong>{formatPrice(row.latest_close)}</strong>
              {renderChange(row.change_1d_pct)}
            </div>
          </div>
          <div className="scanner-chart-card-score-row board-chart-panel-scores">
            <span className={`scanner-score-pill ${toneForScore(row.daily_rs_rating ?? row.leadership_score, 100)}`}>RS {formatScoreInteger(row.daily_rs_rating ?? row.leadership_score)}</span>
            <span className={`scanner-score-pill ${toneForScore(row.fundamental_rating, 100)}`}>FA {formatScoreInteger(row.fundamental_rating)}</span>
            <span className={`scanner-score-pill ${guruToneForPositionAction(row.position_action?.action)}`}>⚾ {humanizePositionAction(row.position_action?.action)}</span>
          </div>
          <div className="board-chart-panel-controls" role="group" aria-label="Selected chart controls">
            {(["daily", "weekly"] as MyPicksChartTimeframe[]).map((item) => (
              <button key={item} type="button" title={item === "daily" ? "Daily" : "Weekly"} aria-pressed={timeframe === item} className={`scanner-result-view-chip${timeframe === item ? " is-active" : ""}`} onClick={() => onTimeframeChange(item)}>{item === "daily" ? "D" : "W"}</button>
            ))}
            {(["3m", "6m", "1y"] as MyPicksChartRange[]).map((item) => (
              <button key={item} type="button" className={`scanner-result-view-chip${range === item ? " is-active" : ""}`} onClick={() => onRangeChange(item)}>{item.toUpperCase()}</button>
            ))}
            <select aria-label="Selected chart style" value={chartType} onChange={(event) => onChartTypeChange(event.target.value as MyPicksChartType)}>
              <option value="candles">Candles</option>
              <option value="bars">Bars</option>
              <option value="line">Line</option>
            </select>
            <label className="scanner-chart-toggle"><input type="checkbox" checked={showVolume} onChange={(event) => onShowVolumeChange(event.target.checked)} /><span>Volume</span></label>
          </div>
          <div className="board-chart-panel-chart">
            {isChartLoading ? <LoadingBlock label={`Loading ${row.ticker} chart...`} /> : null}
            {!isChartLoading && chartError ? <p className="panel-copy">{chartError}</p> : null}
            {!isChartLoading && !chartError && chartCandles.length === 0 ? <p className="panel-copy">No chart data.</p> : null}
            {!isChartLoading && !chartError && chartCandles.length > 0 ? <ScannerMiniChart ticker={row.ticker} candles={chartCandles} chartType={chartType} height={330} showVolume={showVolume} ema9={ema9} ema21={ema21} /> : null}
          </div>
          <div className="board-chart-panel-context">
            <span>{positionBucketLabel(row.position_bucket)}</span>
            <span>{row.recent_signal_count} recent scanner hit{row.recent_signal_count === 1 ? "" : "s"}</span>
            {row.notes ? <span title={row.notes}>{row.notes}</span> : null}
          </div>
          <Link className="ghost-button board-chart-panel-link" to={`/charts?ticker=${encodeURIComponent(row.ticker)}`}>Open full chart</Link>
        </>}
      </aside>
    </div>
  );
}

function MyPicksPositionBoard({ rows, isSaving, selectedTicker, onDelete, onSelectTicker }: {
  rows: MyPickRow[];
  isSaving: boolean;
  selectedTicker: string;
  onDelete: (row: MyPickRow) => void;
  onSelectTicker: (ticker: string) => void;
}) {
  return (
    <div className="guru-board" aria-label="My Picks position map">
      <div className="guru-board-summary">
        <span>Each pick appears once, based on its latest moving-average position.</span>
        <span><strong>{formatCount(rows.length)}</strong> names shown</span>
      </div>
      <div className="guru-board-scroll position-map-scroll">
        {MY_PICKS_POSITION_BUCKETS.map(([id, label]) => {
          const bucketRows = rows.filter((row) => (row.position_bucket || "no_data") === id);
          return (
            <article className="guru-column position-map-column" key={id}>
              <header><strong>{formatCount(bucketRows.length)}</strong><span>{label}</span></header>
              <div className="guru-column-cards">
                {bucketRows.map((row) => <MyPickGuruCard key={row.id} row={row} isSaving={isSaving} isSelected={row.ticker === selectedTicker} onDelete={onDelete} onSelect={onSelectTicker} />)}
              </div>
            </article>
          );
        })}
      </div>
    </div>
  );
}

function MyPicksSectorBoard({
  rows,
  isSaving,
  selectedTicker,
  visibleCounts,
  onDelete,
  onSelectTicker,
  onLoadMore,
}: {
  rows: MyPickRow[];
  isSaving: boolean;
  selectedTicker: string;
  visibleCounts: Record<string, number>;
  onDelete: (row: MyPickRow) => void;
  onSelectTicker: (ticker: string) => void;
  onLoadMore: (sector: string) => void;
}) {
  const sectors = useMemo(() => {
    const grouped = new Map<string, MyPickRow[]>();
    for (const row of rows) {
      const sector = String(row.sector || "").trim() || "Unclassified";
      grouped.set(sector, [...(grouped.get(sector) ?? []), row]);
    }
    return Array.from(grouped.entries())
      .map(([sector, sectorRows]) => ({ sector, rows: sectorRows }))
      .sort((left, right) => right.rows.length - left.rows.length || left.sector.localeCompare(right.sector));
  }, [rows]);
  return (
    <div className="guru-board my-picks-sector-board" aria-label="My Picks by sector">
      <div className="guru-board-summary">
        <span><strong>{formatCount(rows.length)}</strong> picks across <strong>{formatCount(sectors.length)}</strong> sectors</span>
        <span>Cards retain the selected My Picks sort order within each sector.</span>
      </div>
      <div className="guru-board-scroll">
        {sectors.map(({ sector, rows: sectorRows }, index) => {
          const visibleCount = visibleCounts[sector] ?? CARD_PAGE_SIZE;
          const remainingCount = Math.max(0, sectorRows.length - visibleCount);
          return (
            <article className={`guru-column is-${MY_PICKS_SECTOR_ACCENTS[index % MY_PICKS_SECTOR_ACCENTS.length]}`} key={sector}>
              <header title={sector}>
                <strong>{formatCount(sectorRows.length)}</strong>
                <span>{sector}</span>
              </header>
              <div className="guru-column-cards">
                {sectorRows.slice(0, visibleCount).map((row) => (
                  <MyPickGuruCard key={row.id} row={row} isSaving={isSaving} isSelected={row.ticker === selectedTicker} onDelete={onDelete} onSelect={onSelectTicker} />
                ))}
              </div>
              {remainingCount > 0 ? <button className="ghost-button guru-column-load-more" type="button" disabled={isSaving} onClick={() => onLoadMore(sector)}>
                Load 30 more ({formatCount(remainingCount)} remaining)
              </button> : null}
            </article>
          );
        })}
      </div>
    </div>
  );
}

function MyPickGuruCard({
  row,
  isSaving,
  isSelected = false,
  onDelete,
  onSelect,
}: {
  row: MyPickRow;
  isSaving: boolean;
  isSelected?: boolean;
  onDelete: (row: MyPickRow) => void;
  onSelect?: (ticker: string) => void;
}) {
  const latestSignal = row.recent_signals[0];
  const signalTitle = row.recent_signals.length > 0
    ? [`${row.recent_signal_count} recent scanner hits`, ...row.recent_signals.map((signal) => `• ${humanizeSignalId(signal.strategy_id)}${signal.signal_date ? ` · ${formatLocalDate(signal.signal_date)}` : ""}`)].join("\n")
    : "No recent scanner hits";
  const trendTemplate = row.trend_template_match
    ? "TT ✓"
    : row.trend_template_criteria_passed != null
      ? `TT ${row.trend_template_criteria_passed}/${row.trend_template_criteria_total ?? 10}`
      : "TT --";
  return (
    <article className={`guru-ticker-card my-pick-guru-card${isSelected ? " is-selected" : ""}`}>
      <Link
        className="guru-ticker-card-link"
        to={`/charts?ticker=${encodeURIComponent(row.ticker)}`}
        title={onSelect ? `Select ${row.ticker}` : `${row.ticker} chart`}
        onClick={onSelect ? (event) => { event.preventDefault(); onSelect(row.ticker); } : undefined}
      >
        <div className="guru-ticker-main">
          <strong>{row.ticker}</strong>
          {renderChange(row.change_1d_pct)}
        </div>
        <div className="guru-ticker-badges">
          <span title={signalTitle}>{row.recent_signal_count}×</span>
          {row.sector ? <span className="guru-sector-tag" title={`Sector: ${row.sector}`}>{row.sector}</span> : null}
          <span title={row.trend_template_label || "Minervini Trend Template"}>{trendTemplate}</span>
          <span title="Daily relative strength rating">RS {formatScoreInteger(row.daily_rs_rating ?? row.leadership_score)}</span>
          <span title="Fundamental rating">FA {formatScoreInteger(row.fundamental_rating)}</span>
          {row.vcp_score != null ? <span title={row.vcp_rating || "VCP score"}>VCP {formatScore(row.vcp_score)}</span> : null}
        </div>
        <div className="guru-ticker-context">
          <span title="Latest close">{row.latest_close == null ? "--" : `$${formatPrice(row.latest_close)}`}</span>
          <span title={`Added ${formatLocalDateTime(row.added_at)}`}>📅 {formatLocalDate(row.added_date)}</span>
          <span className={`guru-strike ${guruToneForPositionAction(row.position_action?.action)}`} title={row.position_action?.reason_summary || "Position guidance unavailable"}>
            ⚾ {humanizePositionAction(row.position_action?.action)}
          </span>
          {latestSignal ? <span className="guru-primary-signal" title={latestSignal.signal_date ? `Latest signal · ${formatLocalDate(latestSignal.signal_date)}` : "Latest signal"}>⚡ {humanizeSignalId(latestSignal.strategy_id)}</span> : null}
          {row.notes ? <span className="my-pick-card-note" title={row.notes}>📝 {row.notes}</span> : null}
        </div>
      </Link>
      <button
        type="button"
        className="guru-my-pick-toggle is-selected"
        aria-label={`Remove ${row.ticker} from My Picks`}
        aria-pressed="true"
        disabled={isSaving}
        title="Remove from My Picks"
        onClick={() => onDelete(row)}
      >
        ★
      </button>
    </article>
  );
}

function positionBucketLabel(bucket: string | null | undefined) {
  return MY_PICKS_POSITION_BUCKETS.find(([id]) => id === bucket)?.[1] || "No position data";
}

function PicksTable({
  rows,
  checklistItems,
  checklistSaving,
  onToggleChecklist,
  onDelete,
  isSaving,
  sortBy,
  sortDirection,
  setSortBy,
  setSortDirection,
}: {
  rows: MyPickRow[];
  checklistItems: FundamentalChecklistItem[];
  checklistSaving: Record<string, boolean>;
  onToggleChecklist: (row: MyPickRow, item: FundamentalChecklistItem, checked: boolean) => void;
  onDelete: (row: MyPickRow) => void;
  isSaving: boolean;
  sortBy: MyPicksSortKey;
  sortDirection: SortDirection;
  setSortBy: (value: MyPicksSortKey) => void;
  setSortDirection: (value: SortDirection) => void;
}) {
  return (
    <div className="data-table-responsive">
      <table className="data-table">
        <thead>
          <tr>
            <th>{renderSortHeader("Added", "added_at", sortBy, sortDirection, setSortBy, setSortDirection)}</th>
            <th className="pinned-ticker">{renderSortHeader("Ticker", "ticker", sortBy, sortDirection, setSortBy, setSortDirection)}</th>
            <th>{renderSortHeader("Sector / Industry", "sector_industry", sortBy, sortDirection, setSortBy, setSortDirection)}</th>
            <th>{renderSortHeader("Close", "latest_close", sortBy, sortDirection, setSortBy, setSortDirection)}</th>
            <th>{renderSortHeader("1D %", "change_1d_pct", sortBy, sortDirection, setSortBy, setSortDirection)}</th>
            <th>{renderSortHeader("YTD %", "perf_ytd_pct", sortBy, sortDirection, setSortBy, setSortDirection)}</th>
            <th>{renderSortHeader("Since Add %", "change_since_added_pct", sortBy, sortDirection, setSortBy, setSortDirection)}</th>
            <th>{renderSortHeader("From 52W Low %", "change_from_52wk_low_pct", sortBy, sortDirection, setSortBy, setSortDirection)}</th>
            <th>{renderSortHeader("Bollinger", "bollinger_band_status", sortBy, sortDirection, setSortBy, setSortDirection)}</th>
            <th>{renderSortHeader("EMA9 Test", "ema9_tested_since_added", sortBy, sortDirection, setSortBy, setSortDirection)}</th>
            <th>{renderSortHeader("EMA21 Test", "ema21_tested_since_added", sortBy, sortDirection, setSortBy, setSortDirection)}</th>
            <th>{renderSortHeader("Retest 50 SMA", "sma50_tested_since_added", sortBy, sortDirection, setSortBy, setSortDirection)}</th>
            <th>{renderSortHeader("Above 50 SMA", "price_above_sma50", sortBy, sortDirection, setSortBy, setSortDirection)}</th>
            <th>{renderSortHeader("EMA9", "daily_ema9", sortBy, sortDirection, setSortBy, setSortDirection)}</th>
            <th>{renderSortHeader("vs EMA9", "distance_to_ema9_pct", sortBy, sortDirection, setSortBy, setSortDirection)}</th>
            <th>{renderSortHeader("EMA21", "daily_ema21", sortBy, sortDirection, setSortBy, setSortDirection)}</th>
            <th>{renderSortHeader("vs EMA21", "distance_to_ema21_pct", sortBy, sortDirection, setSortBy, setSortDirection)}</th>
            <th>{renderSortHeader("FA", "fundamental_rating", sortBy, sortDirection, setSortBy, setSortDirection)}</th>
            <th>{renderSortHeader("Trend Template", "trend_template", sortBy, sortDirection, setSortBy, setSortDirection)}</th>
            <th>{renderSortHeader("RS Rating", "leadership_score", sortBy, sortDirection, setSortBy, setSortDirection)}</th>
            <th>Daily RS</th>
            <th>{renderSortHeader("CAN V2", "canslim_score", sortBy, sortDirection, setSortBy, setSortDirection)}</th>
            <th>{renderSortHeader("VCP", "vcp_score", sortBy, sortDirection, setSortBy, setSortDirection)}</th>
            <th>{renderSortHeader("1D", "technical_indicator_1d", sortBy, sortDirection, setSortBy, setSortDirection)}</th>
            <th>{renderSortHeader("1W", "technical_indicator_1w", sortBy, sortDirection, setSortBy, setSortDirection)}</th>
            <th>{renderSortHeader("Decision", "position_action", sortBy, sortDirection, setSortBy, setSortDirection)}</th>
            <th>{renderSortHeader("Decision Score", "position_action_score", sortBy, sortDirection, setSortBy, setSortDirection)}</th>
            <th>{renderSortHeader("Signals", "recent_signal_count", sortBy, sortDirection, setSortBy, setSortDirection)}</th>
            <th>{renderSortHeader("Latest Signal", "latest_signal_date", sortBy, sortDirection, setSortBy, setSortDirection)}</th>
            <th>Notes</th>
            {checklistItems.map((item) => (
              <th key={item.key} title={item.description}>{item.short_label}</th>
            ))}
            <th>Action</th>
          </tr>
        </thead>
        <tbody>
          {rows.map((row) => (
            <tr key={row.id}>
              <td data-label="Added">{formatLocalDateTime(row.added_at)}</td>
              <td className="pinned-ticker" data-label="Ticker">
                <Link to={`/charts?ticker=${encodeURIComponent(row.ticker)}`}>{row.ticker}</Link>
              </td>
              <td data-label="Sector / Industry">{[row.sector, row.industry].filter(Boolean).join(" / ") || "-"}</td>
              <td data-label="Close">{formatPrice(row.latest_close)}</td>
              <td data-label="1D %">{renderChange(row.change_1d_pct)}</td>
              <td data-label="YTD %">{renderChange(row.perf_ytd_pct)}</td>
              <td data-label="Since Add %">{renderChange(row.change_since_added_pct)}</td>
              <td data-label="From 52W Low %">{renderChange(row.change_from_52wk_low_pct)}</td>
              <td data-label="Bollinger">{renderBollingerBandStatus(row.bollinger_band_status)}</td>
              <td data-label="EMA9 Test">{renderTestFlag(row.ema9_tested_since_added)}</td>
              <td data-label="EMA21 Test">{renderTestFlag(row.ema21_tested_since_added)}</td>
              <td data-label="Retest 50 SMA">{renderTestFlag(row.sma50_tested_since_added)}</td>
              <td data-label="Above 50 SMA">{renderAboveSmaFlag(row.price_above_sma50)}</td>
              <td data-label="EMA9">{formatPrice(row.daily_ema9)}</td>
              <td data-label="vs EMA9">
                <span className={toneForPercent(row.distance_to_ema9_pct)}>{formatSignedPercent(row.distance_to_ema9_pct)}</span>
              </td>
              <td data-label="EMA21">{formatPrice(row.daily_ema21)}</td>
              <td data-label="vs EMA21">
                <span className={toneForPercent(row.distance_to_ema21_pct)}>{formatSignedPercent(row.distance_to_ema21_pct)}</span>
              </td>
              <td data-label="FA">{formatScore(row.fundamental_rating)}</td>
              <td data-label="Trend Template">{renderTrendTemplateCell(row)}</td>
              <td data-label="RS Rating">{formatScore(row.leadership_score)}</td>
              <td data-label="Daily RS">{formatScore(row.daily_rs_rating)}</td>
              <td data-label="CAN V2">{formatScoreFraction(row.canslim_score, row.canslim_max_score)}</td>
              <td data-label="VCP">{formatScoreWithLabel(row.vcp_score, row.vcp_rating)}</td>
              <td data-label="1D">{row.technical_indicator_ratings?.["1d"]?.rating_label ?? "-"}</td>
              <td data-label="1W">{row.technical_indicator_ratings?.["1w"]?.rating_label ?? "-"}</td>
              <td data-label="Decision">{renderPositionActionCell(row.position_action)}</td>
              <td data-label="Decision Score">{formatDecisionScore(row.position_action?.action_score)}</td>
              <td data-label="Signals">
                {row.recent_signal_count}
                {row.recent_signals.length > 0 ? ` | ${row.recent_signals.slice(0, 2).map((item) => item.strategy_id).join(", ")}` : ""}
              </td>
              <td data-label="Latest Signal">{formatLocalDate(row.latest_signal_date)}</td>
              <td data-label="Notes">{row.notes || "-"}</td>
              {checklistItems.map((item) => {
                const savingKey = `${row.id}:${item.key}`;
                return (
                  <td key={item.key} data-label={item.label}>
                    <input
                      type="checkbox"
                      checked={Boolean(row.checklist?.[item.key])}
                      disabled={isSaving || Boolean(checklistSaving[savingKey])}
                      title={item.description}
                      onChange={(event) => void onToggleChecklist(row, item, event.target.checked)}
                    />
                  </td>
                );
              })}
              <td data-label="Action">
                <button className="table-action-button" type="button" disabled={isSaving} onClick={() => void onDelete(row)}>
                  Delete
                </button>
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

function compareAdded(left: string | null, right: string | null, direction: "desc" | "asc") {
  const leftTime = left ? Date.parse(left) : 0;
  const rightTime = right ? Date.parse(right) : 0;
  return direction === "desc" ? rightTime - leftTime : leftTime - rightTime;
}

function compareNullableNumber(left: number | null | undefined, right: number | null | undefined, direction: SortDirection) {
  const leftValue = left == null || Number.isNaN(left) ? Number.NEGATIVE_INFINITY : left;
  const rightValue = right == null || Number.isNaN(right) ? Number.NEGATIVE_INFINITY : right;
  return direction === "desc" ? rightValue - leftValue : leftValue - rightValue;
}

function compareNullableText(left: string | null | undefined, right: string | null | undefined, direction: SortDirection) {
  const leftValue = String(left ?? "").toLowerCase();
  const rightValue = String(right ?? "").toLowerCase();
  return direction === "desc" ? rightValue.localeCompare(leftValue) : leftValue.localeCompare(rightValue);
}

function compareNullableBoolean(left: boolean | null | undefined, right: boolean | null | undefined, direction: SortDirection) {
  const toRank = (value: boolean | null | undefined) => {
    if (value == null) {
      return -1;
    }
    return value ? 1 : 0;
  };
  const leftValue = toRank(left);
  const rightValue = toRank(right);
  return direction === "desc" ? rightValue - leftValue : leftValue - rightValue;
}

function trendTemplateSortValue(row: MyPickRow) {
  if ((row.trend_template_criteria_total ?? 0) <= 0 || row.trend_template_criteria_passed == null) {
    return null;
  }
  return row.trend_template_criteria_passed / (row.trend_template_criteria_total ?? 1);
}

function compareMyPickRows(left: MyPickRow, right: MyPickRow, sortBy: MyPicksSortKey, sortDirection: SortDirection) {
  let comparison = 0;
  switch (sortBy) {
    case "ticker":
      comparison = compareNullableText(left.ticker, right.ticker, sortDirection);
      break;
    case "sector_industry":
      comparison = compareNullableText(
        [left.sector, left.industry].filter(Boolean).join(" / "),
        [right.sector, right.industry].filter(Boolean).join(" / "),
        sortDirection,
      );
      break;
    case "latest_close":
      comparison = compareNullableNumber(left.latest_close, right.latest_close, sortDirection);
      break;
    case "change_1d_pct":
      comparison = compareNullableNumber(left.change_1d_pct, right.change_1d_pct, sortDirection);
      break;
    case "perf_ytd_pct":
      comparison = compareNullableNumber(left.perf_ytd_pct, right.perf_ytd_pct, sortDirection);
      break;
    case "change_since_added_pct":
      comparison = compareNullableNumber(left.change_since_added_pct, right.change_since_added_pct, sortDirection);
      break;
    case "change_from_52wk_low_pct":
      comparison = compareNullableNumber(left.change_from_52wk_low_pct, right.change_from_52wk_low_pct, sortDirection);
      break;
    case "bollinger_band_status":
      comparison = compareNullableText(left.bollinger_band_status, right.bollinger_band_status, sortDirection);
      break;
    case "ema9_tested_since_added":
      comparison = compareNullableBoolean(left.ema9_tested_since_added, right.ema9_tested_since_added, sortDirection);
      break;
    case "ema21_tested_since_added":
      comparison = compareNullableBoolean(left.ema21_tested_since_added, right.ema21_tested_since_added, sortDirection);
      break;
    case "sma50_tested_since_added":
      comparison = compareNullableBoolean(left.sma50_tested_since_added, right.sma50_tested_since_added, sortDirection);
      break;
    case "price_above_sma50":
      comparison = compareNullableBoolean(left.price_above_sma50, right.price_above_sma50, sortDirection);
      break;
    case "daily_ema9":
      comparison = compareNullableNumber(left.daily_ema9, right.daily_ema9, sortDirection);
      break;
    case "distance_to_ema9_pct":
      comparison = compareNullableNumber(left.distance_to_ema9_pct, right.distance_to_ema9_pct, sortDirection);
      break;
    case "daily_ema21":
      comparison = compareNullableNumber(left.daily_ema21, right.daily_ema21, sortDirection);
      break;
    case "distance_to_ema21_pct":
      comparison = compareNullableNumber(left.distance_to_ema21_pct, right.distance_to_ema21_pct, sortDirection);
      break;
    case "fundamental_rating":
      comparison = compareNullableNumber(left.fundamental_rating, right.fundamental_rating, sortDirection);
      break;
    case "trend_template":
      comparison =
        compareNullableNumber(trendTemplateSortValue(left), trendTemplateSortValue(right), sortDirection) ||
        compareNullableText(left.trend_template_label, right.trend_template_label, sortDirection);
      break;
    case "leadership_score":
      comparison = compareNullableNumber(left.leadership_score, right.leadership_score, sortDirection);
      break;
    case "canslim_score":
      comparison = compareNullableNumber(left.canslim_score, right.canslim_score, sortDirection);
      break;
    case "vcp_score":
      comparison = compareNullableNumber(left.vcp_score, right.vcp_score, sortDirection);
      break;
    case "technical_indicator_1d":
      comparison = compareNullableText(left.technical_indicator_ratings?.["1d"]?.rating_label, right.technical_indicator_ratings?.["1d"]?.rating_label, sortDirection);
      break;
    case "technical_indicator_1w":
      comparison = compareNullableText(left.technical_indicator_ratings?.["1w"]?.rating_label, right.technical_indicator_ratings?.["1w"]?.rating_label, sortDirection);
      break;
    case "position_action":
      comparison = compareNullableText(left.position_action?.action, right.position_action?.action, sortDirection);
      break;
    case "position_action_score":
      comparison = compareNullableNumber(left.position_action?.action_score, right.position_action?.action_score, sortDirection);
      break;
    case "recent_signal_count":
      comparison = compareNullableNumber(left.recent_signal_count, right.recent_signal_count, sortDirection);
      break;
    case "latest_signal_date":
      comparison = compareAdded(left.latest_signal_date, right.latest_signal_date, sortDirection);
      break;
    case "added_at":
    default:
      comparison = compareAdded(left.added_at, right.added_at, sortDirection);
      break;
  }
  return comparison || left.ticker.localeCompare(right.ticker);
}

function isAlphabeticalSort(sortBy: MyPicksSortKey) {
  return sortBy === "ticker" || sortBy === "sector_industry" || sortBy === "technical_indicator_1d" || sortBy === "technical_indicator_1w" || sortBy === "position_action";
}

function isDateSort(sortBy: MyPicksSortKey) {
  return sortBy === "added_at" || sortBy === "latest_signal_date";
}

function defaultSortDirection(sortBy: MyPicksSortKey): SortDirection {
  return isAlphabeticalSort(sortBy) ? "asc" : "desc";
}

function renderSortHeader(
  label: string,
  key: MyPicksSortKey,
  sortBy: MyPicksSortKey,
  sortDirection: SortDirection,
  setSortBy: (value: MyPicksSortKey) => void,
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
        setSortDirection(defaultSortDirection(key));
      }}
    >
      {label}
      {indicator}
    </button>
  );
}

function formatScore(value: number | null | undefined) {
  if (value == null || Number.isNaN(value)) {
    return "--";
  }
  return value.toFixed(2);
}

function formatScoreInteger(value: number | null | undefined) {
  if (value == null || Number.isNaN(value)) {
    return "--";
  }
  return Math.round(value).toString();
}

function formatScoreFraction(value: number | null | undefined, maxValue: number | null | undefined) {
  if (value == null || Number.isNaN(value)) {
    return "--";
  }
  if (maxValue == null || Number.isNaN(maxValue)) {
    return Math.round(value).toString();
  }
  return `${Math.round(value)}/${Math.round(maxValue)}`;
}

function formatScoreWithLabel(value: number | null | undefined, label: string | null | undefined) {
  if (value == null || Number.isNaN(value)) {
    return "--";
  }
  const score = value.toFixed(1);
  return label ? `${score} ${label}` : score;
}

function formatPrice(value: number | null | undefined) {
  if (value == null || Number.isNaN(value)) {
    return "--";
  }
  return value.toFixed(2);
}

function formatDecisionScore(value: number | null | undefined) {
  if (value == null || Number.isNaN(value)) {
    return "--";
  }
  return value.toFixed(1);
}

function renderPositionActionCell(positionAction: MyPickRow["position_action"]) {
  if (!positionAction?.action) {
    return <span className="panel-copy">--</span>;
  }
  return (
    <div title={positionAction.reason_summary ?? undefined}>
      <span className={`scanner-score-pill ${toneForPositionAction(positionAction.action)}`}>{humanizePositionAction(positionAction.action)}</span>
      <div className="ticker-company-inline">
        {humanizePositionTrend(positionAction.trend_state)} / {humanizePositionExtension(positionAction.extension_state)}
      </div>
    </div>
  );
}

function renderBollingerBandStatus(status: string | null | undefined) {
  if (!status) {
    return <span className="panel-copy">--</span>;
  }
  switch (status.trim().toLowerCase()) {
    case "above_upper_band":
      return <span className="scanner-score-pill is-caution">Above</span>;
    case "within_bands":
      return <span className="scanner-score-pill is-neutral">Within</span>;
    case "below_lower_band":
      return <span className="scanner-score-pill is-negative">Below</span>;
    default:
      return <span className="panel-copy">{status}</span>;
  }
}

function formatSignedPercent(value: number | null | undefined) {
  if (value == null || Number.isNaN(value)) {
    return "--";
  }
  const prefix = value > 0 ? "+" : "";
  return `${prefix}${value.toFixed(2)}%`;
}

function humanizeSignalId(value: string) {
  return value
    .split("_")
    .filter(Boolean)
    .map((part) => part.length <= 3 ? part.toUpperCase() : part[0].toUpperCase() + part.slice(1))
    .join(" ");
}

function guruToneForPositionAction(action: string | null | undefined) {
  if (action === "add_position") return "is-active";
  if (action === "hold_position") return "is-ready";
  if (action === "avoid_new" || action === "trim_reduce") return "is-avoid";
  return "is-context";
}

function renderChange(value: number | null | undefined) {
  if (value == null || Number.isNaN(value)) {
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

function renderTestFlag(value: boolean | null | undefined) {
  if (value == null) {
    return <span className="ticker-change neutral">--</span>;
  }
  return <span className={`ticker-change ${value ? "down" : "up"}`}>{value ? "Tested" : "No"}</span>;
}

function renderAboveSmaFlag(value: boolean | null | undefined) {
  if (value == null) {
    return <span className="ticker-change neutral">--</span>;
  }
  return <span className={`ticker-change ${value ? "up" : "down"}`}>{value ? "Above" : "Below"}</span>;
}

function formatTrendTemplate(row: MyPickRow) {
  return row.trend_template_label ?? "--";
}

function toneForTrendTemplate(row: MyPickRow) {
  if (row.trend_template_criteria_passed == null || row.trend_template_criteria_total == null || row.trend_template_criteria_total <= 0) {
    return "is-neutral";
  }
  return toneForScore(row.trend_template_criteria_passed, row.trend_template_criteria_total);
}

function renderTrendTemplateCell(row: MyPickRow) {
  if (row.trend_template_criteria_passed == null || row.trend_template_criteria_total == null) {
    return <span className="ticker-change neutral">--</span>;
  }
  return (
    <span className={`ticker-change ${row.trend_template_match ? "up" : "neutral"}`}>
      {formatTrendTemplate(row)}
      {row.trend_template_match ? " Match" : ""}
    </span>
  );
}

function toneForPercent(value: number | null | undefined) {
  if (value == null || Number.isNaN(value)) {
    return "";
  }
  if (value > 0) {
    return "my-picks-percent is-positive";
  }
  if (value < 0) {
    return "my-picks-percent is-negative";
  }
  return "my-picks-percent";
}

function toneForScore(value: number | null | undefined, maxValue: number) {
  if (value == null || Number.isNaN(value)) {
    return "is-neutral";
  }
  const ratio = maxValue > 0 ? value / maxValue : 0;
  if (ratio >= 0.75) {
    return "is-strong";
  }
  if (ratio >= 0.5) {
    return "is-warm";
  }
  return "is-neutral";
}
