import { useEffect, useMemo, useState, type FormEvent } from "react";
import { Link } from "react-router-dom";
import { LoadingBlock } from "../components/LoadingBlock";
import { PaginationControls } from "../components/PaginationControls";
import { ScannerMiniChart } from "../components/ScannerMiniChart";
import { fetchJson } from "../lib/api";
import { formatCount, formatLocalDate, formatLocalDateTime } from "../lib/format";
import type { CandlePoint, FundamentalChecklistItem, MyPickRow, MyPicksContextResponse, WatchlistChartResponse } from "../lib/types";

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
type MyPicksViewMode = "list" | "charts";
type MyPicksSortKey =
  | "added_at"
  | "ticker"
  | "sector_industry"
  | "latest_close"
  | "change_1d_pct"
  | "perf_ytd_pct"
  | "change_since_added_pct"
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
  | "recent_signal_count"
  | "latest_signal_date";
type SortDirection = "desc" | "asc";

export function MyPicksPage() {
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

  const pageSize = viewMode === "charts" ? CHART_PAGE_SIZE : LIST_PAGE_SIZE;
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
  }, [pagedTickerKey, viewMode]);

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
      loadPicks();
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
      setNotice(`Deleted ${row.ticker}.`);
      loadPicks();
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
            <strong>{viewMode === "charts" ? "Charts" : "List"}</strong>
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
            <h2>{viewMode === "charts" ? "Chart View" : groupByDate ? "Grouped Picks" : "All Picks"}</h2>
            <span className="eyebrow">{formatCount(filteredRows.length)} names</span>
          </div>
        </div>
        {filteredRows.length > 0 ? (
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
              const chartCandles = buildMiniChartCandles(chartPayload);
              const isChartLoading = Boolean(chartLoadingTickers[row.ticker]);
              const chartError = chartErrors[row.ticker];
              const latestCandle = chartCandles[chartCandles.length - 1] ?? null;
              return (
                <article key={row.id} className="scanner-chart-card">
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
        {filteredRows.length > 0 ? (
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
            <th>{renderSortHeader("Ticker", "ticker", sortBy, sortDirection, setSortBy, setSortDirection)}</th>
            <th>{renderSortHeader("Sector / Industry", "sector_industry", sortBy, sortDirection, setSortBy, setSortDirection)}</th>
            <th>{renderSortHeader("Close", "latest_close", sortBy, sortDirection, setSortBy, setSortDirection)}</th>
            <th>{renderSortHeader("1D %", "change_1d_pct", sortBy, sortDirection, setSortBy, setSortDirection)}</th>
            <th>{renderSortHeader("YTD %", "perf_ytd_pct", sortBy, sortDirection, setSortBy, setSortDirection)}</th>
            <th>{renderSortHeader("Since Add %", "change_since_added_pct", sortBy, sortDirection, setSortBy, setSortDirection)}</th>
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
            <th>{renderSortHeader("CAN V2", "canslim_score", sortBy, sortDirection, setSortBy, setSortDirection)}</th>
            <th>{renderSortHeader("VCP", "vcp_score", sortBy, sortDirection, setSortBy, setSortDirection)}</th>
            <th>{renderSortHeader("1D", "technical_indicator_1d", sortBy, sortDirection, setSortBy, setSortDirection)}</th>
            <th>{renderSortHeader("1W", "technical_indicator_1w", sortBy, sortDirection, setSortBy, setSortDirection)}</th>
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
              <td data-label="Ticker">
                <Link to={`/charts?ticker=${encodeURIComponent(row.ticker)}`}>{row.ticker}</Link>
              </td>
              <td data-label="Sector / Industry">{[row.sector, row.industry].filter(Boolean).join(" / ") || "-"}</td>
              <td data-label="Close">{formatPrice(row.latest_close)}</td>
              <td data-label="1D %">{renderChange(row.change_1d_pct)}</td>
              <td data-label="YTD %">{renderChange(row.perf_ytd_pct)}</td>
              <td data-label="Since Add %">{renderChange(row.change_since_added_pct)}</td>
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
              <td data-label="CAN V2">{formatScoreFraction(row.canslim_score, row.canslim_max_score)}</td>
              <td data-label="VCP">{formatScoreWithLabel(row.vcp_score, row.vcp_rating)}</td>
              <td data-label="1D">{row.technical_indicator_ratings?.["1d"]?.rating_label ?? "-"}</td>
              <td data-label="1W">{row.technical_indicator_ratings?.["1w"]?.rating_label ?? "-"}</td>
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
  return sortBy === "ticker" || sortBy === "sector_industry" || sortBy === "technical_indicator_1d" || sortBy === "technical_indicator_1w";
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

function formatSignedPercent(value: number | null | undefined) {
  if (value == null || Number.isNaN(value)) {
    return "--";
  }
  const prefix = value > 0 ? "+" : "";
  return `${prefix}${value.toFixed(2)}%`;
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

function buildMiniChartCandles(payload: WatchlistChartResponse | null | undefined): CandlePoint[] {
  if (!payload) {
    return [];
  }
  return (payload.candles ?? []).map((item, index) => ({
    ...item,
    volume: payload.volume[index]?.value ?? 0,
  }));
}

function buildExponentialMovingAverage(candles: CandlePoint[], length: number): Array<{ time: string; value: number }> {
  if (candles.length === 0 || length <= 0) {
    return [];
  }
  const alpha = 2 / (length + 1);
  let ema = candles[0].close;
  const points = [{ time: candles[0].time, value: Number(ema.toFixed(2)) }];
  for (let index = 1; index < candles.length; index += 1) {
    ema = (candles[index].close * alpha) + (ema * (1 - alpha));
    points.push({ time: candles[index].time, value: Number(ema.toFixed(2)) });
  }
  return points;
}
