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
type MyPicksSortKey = "added_at" | "ticker";

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
  const [sortDirection, setSortDirection] = useState<"desc" | "asc">("desc");
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
        setContext(payload);
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
    return [...rows].sort((left, right) => {
      if (sortBy === "ticker") {
        const tickerCompare =
          sortDirection === "asc" ? left.ticker.localeCompare(right.ticker) : right.ticker.localeCompare(left.ticker);
        return tickerCompare || compareAdded(left.added_at, right.added_at, "desc");
      }
      return compareAdded(left.added_at, right.added_at, sortDirection) || left.ticker.localeCompare(right.ticker);
    });
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
            <select value={sortBy} onChange={(event) => setSortBy(event.target.value as MyPicksSortKey)}>
              <option value="added_at">Added time</option>
              <option value="ticker">Ticker name</option>
            </select>
          </label>
          <label className="field">
            <span>Order</span>
            <select value={sortDirection} onChange={(event) => setSortDirection(event.target.value as "desc" | "asc")}>
              {sortBy === "ticker" ? (
                <>
                  <option value="asc">A to Z</option>
                  <option value="desc">Z to A</option>
                </>
              ) : (
                <>
                  <option value="desc">Newest first</option>
                  <option value="asc">Oldest first</option>
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
          {context.fundamental_summary.map((item) => (
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
              {context.fundamental_checklist.map((item) => (
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
                    <span className={`scanner-score-pill ${toneForScore(row.als_score, 100)}`}>ALS {formatScoreInteger(row.als_score)}</span>
                    <span className={`scanner-score-pill ${toneForScore(row.fundamental_rating, 100)}`}>FA {formatScoreInteger(row.fundamental_rating)}</span>
                    <span className={`scanner-score-pill ${toneForScore(row.technical_rating, 10)}`}>TA {formatScore(row.technical_rating)}</span>
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
        {viewMode === "list" && !groupByDate && filteredRows.length > 0 ? <PicksTable rows={pagedRows} checklistItems={context.fundamental_checklist} checklistSaving={checklistSaving} onToggleChecklist={handleChecklistToggle} onDelete={handleDelete} isSaving={isSaving} /> : null}
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
                <PicksTable rows={group.rows} checklistItems={context.fundamental_checklist} checklistSaving={checklistSaving} onToggleChecklist={handleChecklistToggle} onDelete={handleDelete} isSaving={isSaving} />
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
}: {
  rows: MyPickRow[];
  checklistItems: FundamentalChecklistItem[];
  checklistSaving: Record<string, boolean>;
  onToggleChecklist: (row: MyPickRow, item: FundamentalChecklistItem, checked: boolean) => void;
  onDelete: (row: MyPickRow) => void;
  isSaving: boolean;
}) {
  return (
    <div className="data-table-responsive">
      <table className="data-table">
        <thead>
          <tr>
            <th>Added</th>
            <th>Ticker</th>
            <th>Sector / Industry</th>
            <th>Close</th>
            <th>1D %</th>
            <th>YTD %</th>
            <th>Since Add %</th>
            <th>EMA9 Test</th>
            <th>EMA21 Test</th>
            <th>EMA9</th>
            <th>vs EMA9</th>
            <th>EMA21</th>
            <th>vs EMA21</th>
            <th>ALS</th>
            <th>FA</th>
            <th>TA</th>
            <th>RS Rating</th>
            <th>CAN V2</th>
            <th>VCP</th>
            <th>1D</th>
            <th>1W</th>
            <th>Signals</th>
            <th>Latest Signal</th>
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
              <td data-label="EMA9">{formatPrice(row.daily_ema9)}</td>
              <td data-label="vs EMA9">
                <span className={toneForPercent(row.distance_to_ema9_pct)}>{formatSignedPercent(row.distance_to_ema9_pct)}</span>
              </td>
              <td data-label="EMA21">{formatPrice(row.daily_ema21)}</td>
              <td data-label="vs EMA21">
                <span className={toneForPercent(row.distance_to_ema21_pct)}>{formatSignedPercent(row.distance_to_ema21_pct)}</span>
              </td>
              <td data-label="ALS">{formatScore(row.als_score)}</td>
              <td data-label="FA">{formatScore(row.fundamental_rating)}</td>
              <td data-label="TA">{formatScore(row.technical_rating)}</td>
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
