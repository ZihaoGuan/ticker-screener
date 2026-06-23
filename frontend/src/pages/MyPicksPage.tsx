import { useEffect, useMemo, useState, type FormEvent } from "react";
import { Link } from "react-router-dom";
import { LoadingBlock } from "../components/LoadingBlock";
import { fetchJson } from "../lib/api";
import { formatCount, formatLocalDate, formatLocalDateTime } from "../lib/format";
import type { MyPickRow, MyPicksContextResponse } from "../lib/types";

const EMPTY_CONTEXT: MyPicksContextResponse = {
  database_configured: false,
  total_count: 0,
  rows: [],
  available_added_dates: [],
};

export function MyPicksPage() {
  const [context, setContext] = useState<MyPicksContextResponse>(EMPTY_CONTEXT);
  const [isLoading, setIsLoading] = useState(true);
  const [isSaving, setIsSaving] = useState(false);
  const [notice, setNotice] = useState("");
  const [ticker, setTicker] = useState("");
  const [notes, setNotes] = useState("");
  const [search, setSearch] = useState("");
  const [groupByDate, setGroupByDate] = useState(false);
  const [sortDirection, setSortDirection] = useState<"desc" | "asc">("desc");

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
    return [...rows].sort((left, right) => compareAdded(left.added_at, right.added_at, sortDirection) || left.ticker.localeCompare(right.ticker));
  }, [context.rows, search, sortDirection]);

  const groupedRows = useMemo(() => {
    const groups = new Map<string, MyPickRow[]>();
    filteredRows.forEach((row) => {
      const key = row.added_date || "Unknown date";
      groups.set(key, [...(groups.get(key) ?? []), row]);
    });
    return Array.from(groups.entries()).map(([label, rows]) => ({ label, rows }));
  }, [filteredRows]);

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
            <strong>{sortDirection === "desc" ? "Newest first" : "Oldest first"}</strong>
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
            <span>Order</span>
            <select value={sortDirection} onChange={(event) => setSortDirection(event.target.value as "desc" | "asc")}>
              <option value="desc">Newest first</option>
              <option value="asc">Oldest first</option>
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

      <section className="panel earnings-calendar-panel">
        <div className="panel-head earnings-calendar-head">
          <div>
            <h2>{groupByDate ? "Grouped Picks" : "All Picks"}</h2>
            <span className="eyebrow">{formatCount(filteredRows.length)} names</span>
          </div>
        </div>
        {!groupByDate && filteredRows.length === 0 ? <p className="panel-copy">No picks match current filter.</p> : null}
        {!groupByDate && filteredRows.length > 0 ? <PicksTable rows={filteredRows} onDelete={handleDelete} isSaving={isSaving} /> : null}
        {groupByDate && groupedRows.length === 0 ? <p className="panel-copy">No grouped picks match current filter.</p> : null}
        {groupByDate
          ? groupedRows.map((group) => (
              <div key={group.label} className="detail-subsection">
                <div className="panel-head earnings-calendar-head">
                  <div>
                    <h3>{formatLocalDate(group.label)}</h3>
                    <span className="eyebrow">{formatCount(group.rows.length)} names</span>
                  </div>
                </div>
                <PicksTable rows={group.rows} onDelete={handleDelete} isSaving={isSaving} />
              </div>
            ))
          : null}
      </section>
    </div>
  );
}

function PicksTable({
  rows,
  onDelete,
  isSaving,
}: {
  rows: MyPickRow[];
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
            <th>ALS</th>
            <th>FA</th>
            <th>TA</th>
            <th>ARS</th>
            <th>1D</th>
            <th>1W</th>
            <th>Signals</th>
            <th>Latest Signal</th>
            <th>Notes</th>
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
              <td data-label="ALS">{formatScore(row.als_score)}</td>
              <td data-label="FA">{formatScore(row.fundamental_rating)}</td>
              <td data-label="TA">{formatScore(row.technical_rating)}</td>
              <td data-label="ARS">{formatScore(row.leadership_score)}</td>
              <td data-label="1D">{row.technical_indicator_ratings?.["1d"]?.rating_label ?? "-"}</td>
              <td data-label="1W">{row.technical_indicator_ratings?.["1w"]?.rating_label ?? "-"}</td>
              <td data-label="Signals">
                {row.recent_signal_count}
                {row.recent_signals.length > 0 ? ` | ${row.recent_signals.slice(0, 2).map((item) => item.strategy_id).join(", ")}` : ""}
              </td>
              <td data-label="Latest Signal">{formatLocalDate(row.latest_signal_date)}</td>
              <td data-label="Notes">{row.notes || "-"}</td>
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
