import { useEffect, useMemo, useState } from "react";
import { useSearchParams } from "react-router-dom";
import type { MartinForwardBook, MartinForwardResponse, MartinForwardRow } from "../lib/types";

type View = "today" | "positions" | "performance";
const views: View[] = ["today", "positions", "performance"];

export default function MartinForwardTestPage() {
  const [params, setParams] = useSearchParams();
  const view = views.includes(params.get("view") as View) ? params.get("view") as View : "today";
  const requestedBook = params.get("book");
  const bookKey = requestedBook === "sector_opportunity" || requestedBook === "daily_top5" ? requestedBook : "account";
  const [data, setData] = useState<MartinForwardResponse | null>(null);
  const [error, setError] = useState("");
  const [filter, setFilter] = useState("open");
  const [expanded, setExpanded] = useState<string | null>(null);
  const [copyMessage, setCopyMessage] = useState("");
  useEffect(() => {
    let active = true;
    fetch("/api/admin/martin-forward-test", { credentials: "include" })
      .then(async (response) => { if (!response.ok) throw new Error(`Database request failed (${response.status})`); return response.json() as Promise<MartinForwardResponse>; })
      .then((payload) => { if (active) setData(payload); })
      .catch((reason: Error) => { if (active) setError(reason.message); });
    return () => { active = false; };
  }, []);

  const book = data?.books?.[bookKey];
  const latest = book?.runs?.[0];
  const candidateRows = useMemo(() => (book?.candidates ?? []).map(unpack), [book]);
  const positionRows = useMemo(() => (book?.positions ?? []).map(unpack), [book]);
  const attention = candidateRows.filter((row) => row.status === "blocked_data" || ["error", "cancelled", "expired"].includes(String(row.status)) || hasMissingReason(row.reasons_json ?? row.reasons ?? object(row.payload_json).reasons));
  const summary = object(latest?.summary_json);
  const sessionDate = String(latest?.session_date ?? "");
  const stale = sessionDate ? (Date.now() - new Date(`${sessionDate}T00:00:00`).getTime()) > 3 * 86400000 : false;
  const setQuery = (key: string, value: string) => setParams((previous) => { const next = new URLSearchParams(previous); next.set(key, value); return next; });

  return <main className="page-shell martin-page">
    <header className="martin-header">
      <div><h1>Martin Focus Forward Test</h1>
        <p>Paper-trade results from saved Martin Focus snapshots. No broker orders are placed.</p></div>
      <span className="martin-simulation-badge">SIMULATED · NO BROKER ORDERS</span>
    </header>
    {error ? <StatePanel title="Database error" detail={error} /> : !data ? <StatePanel title="Loading forward test" detail="Loading saved sessions and positions…" /> : !data.configured ? <StatePanel title="Database unavailable" detail="Forward test persistence is not configured." /> : <>
      <div className="martin-toolbar">
        <div className="martin-book-switch" aria-label="Strategy book">
          <button type="button" aria-pressed={bookKey === "account"} onClick={() => setQuery("book", "account")}>Account book</button>
          <button type="button" aria-pressed={bookKey === "sector_opportunity"} onClick={() => setQuery("book", "sector_opportunity")}>Sector opportunity</button>
          <button type="button" aria-pressed={bookKey === "daily_top5"} onClick={() => setQuery("book", "daily_top5")}>Daily Top 5</button>
        </div>
        <p className="martin-book-description">{bookKey === "account" ? "Account book: fixed $100,000 sizing base, 0.5% risk per trade, 20% allocation cap, and up to five open or pending positions." : bookKey === "sector_opportunity" ? "Sector opportunity: up to ten sector-ranked candidates, simulated at 100 shares each without an account-capital limit." : "Daily Top 5: up to five new 100-share orders each session, with no concurrent holding or capital limit."}</p>
        <div className="martin-freshness"><span className={stale ? "martin-dot stale" : "martin-dot"} />
          {latest ? `${stale ? "Latest session may be stale" : "Latest session"} · ${sessionDate}` : "No session processed"}
          {latest && summary.source_run_id ? ` · source run ${String(summary.source_run_id)}` : ""}</div>
      </div>
      {!latest ? <StatePanel title="No session yet" detail="Run the existing Martin forward test job after a persisted snapshot is available." /> : <>
        {stale ? <p className="martin-inline-state" role="status">The latest stored session is more than three calendar days old.</p> : null}
        <nav className="martin-tabs" aria-label="Forward test views" role="tablist">
          {views.map((item, index) => <button id={`martin-tab-${item}`} key={item} role="tab" tabIndex={view === item ? 0 : -1} aria-selected={view === item}
            onClick={() => setQuery("view", item)} onKeyDown={(event) => {
              if (!["ArrowRight", "ArrowLeft", "Home", "End"].includes(event.key)) return;
              event.preventDefault();
              const nextIndex = event.key === "Home" ? 0 : event.key === "End" ? views.length - 1 : (index + (event.key === "ArrowRight" ? 1 : views.length - 1)) % views.length;
              const next = views[nextIndex];
              setQuery("view", next);
              document.getElementById(`martin-tab-${next}`)?.focus();
            }}>{item === "today" ? "Today" : item === "positions" ? "Positions" : "Performance"}</button>)}
        </nav>
        {summary.partial ? <p className="martin-inline-state" role="status">Partial session: market bars were unavailable for {format(object(summary.action_counts).missing_bars)} pending orders or open positions.</p> : null}
        {view === "today" ? <Today key={bookKey} book={book!} bookLabel={bookKey === "account" ? "Account book" : bookKey === "sector_opportunity" ? "Sector opportunity" : "Daily Top 5"} candidates={candidateRows} outstandingOrders={(book?.outstanding_orders ?? []).map(unpack)} attention={attention} /> : null}
        {view === "positions" ? <Positions positions={positionRows} filter={filter} setFilter={setFilter} expanded={expanded} setExpanded={setExpanded} /> : null}
        {view === "performance" ? <Performance book={book!} books={data.books} /> : null}
        <details className="martin-audit"><summary>Data &amp; audit</summary>
          <button type="button" onClick={async () => { try { await navigator.clipboard.writeText(JSON.stringify({ session: latest, candidates: book!.candidates, positions: book!.positions, events: book!.events, metrics: book!.metrics, history: book!.history, attribution: book!.attribution }, null, 2)); setCopyMessage("Session data copied."); } catch { setCopyMessage("Copy failed. Select the raw JSON below to copy it."); } }}>{copyMessage ? "Copy again" : "Copy session JSON"}</button>{copyMessage ? <span role="status"> {copyMessage}</span> : null}
          <p>Session {sessionDate} · snapshot fingerprint {String(summary.source_fingerprint ?? latest.source_fingerprint ?? "—")}</p>
          <h3>Stored runs</h3><SimpleTable rows={book!.runs} columns={["session_date", "source_run_id", "created_at"]} />
          <h3>Recent events</h3><SimpleTable rows={book!.events} columns={["event_date", "ticker", "event_type", "payload_json"]} />
          <h3>Raw session data</h3><pre>{JSON.stringify({ candidates: book!.candidates, positions: book!.positions }, null, 2)}</pre>
        </details>
      </>}
    </>}
  </main>;
}

function Today({ book, bookLabel, candidates, outstandingOrders, attention }: { book: MartinForwardBook; bookLabel: string; candidates: MartinForwardRow[]; outstandingOrders: MartinForwardRow[]; attention: MartinForwardRow[] }) {
  type LifecycleGroup = "selected" | "pending" | "waiting" | "open" | "closed" | "all";
  const selected = candidates.filter(isSelected);
  const pendingRows = candidates.filter((row) => row.status === "pending");
  const waitingRows = candidates.filter((row) => row.status === "waiting_capacity");
  const openRows = book.positions.filter((row) => row.status === "open");
  const closedRows = book.positions.filter((row) => row.status === "closed");
  const defaultGroup: LifecycleGroup = pendingRows.length ? "pending" : selected.length ? "selected" : openRows.length ? "open" : "closed";
  const [activeGroup, setActiveGroup] = useState<LifecycleGroup>(defaultGroup);
  const [detailTicker, setDetailTicker] = useState<string | null>(null);
  const rows = activeGroup === "all" ? candidates : activeGroup === "selected" ? selected : activeGroup === "pending" ? pendingRows : activeGroup === "waiting" ? waitingRows : [];
  const excludedRows = candidates.filter((row) => !isSelected(row) && row.status !== "pending" && !attention.includes(row));
  const groupLabel = activeGroup === "all" ? "All candidates" : activeGroup[0].toUpperCase() + activeGroup.slice(1);
  return <div className="martin-view">
    <section className="martin-lifecycle" aria-label="Session lifecycle">
      {[{ group: "selected" as const, label: "Selected · latest session", value: selected.length }, { group: "pending" as const, label: "Pending · latest session", value: pendingRows.length }, { group: "waiting" as const, label: "Waiting capacity", value: waitingRows.length }, { group: "open" as const, label: "Open · all sessions", value: openRows.length }, { group: "closed" as const, label: "Closed · all sessions", value: closedRows.length }].map((item) => <button type="button" className="martin-lifecycle-card" key={item.group} aria-pressed={activeGroup === item.group} onClick={() => { setActiveGroup(item.group); setDetailTicker(null); }}><span>{item.label}</span><strong>{String(item.value)}</strong></button>)}
    </section>
    {outstandingOrders.some((row) => row.market_data_state === "awaiting_data") ? <section className="martin-panel"><h2>Earlier pending orders awaiting market data</h2><p>These orders reserve book capacity. Restore the missing data and replay the affected session to settle them; later sessions cannot fill these orders.</p><SimpleTable rows={outstandingOrders.filter((row) => row.market_data_state === "awaiting_data")} columns={["ticker", "session_date", "awaiting_session_date", "status", "market_data_reason", "limit_price", "stop"]} /></section> : null}
    <p className="martin-lifecycle-note">Selected, Pending, and Waiting capacity refer to the latest session; position totals include all saved sessions.</p>
    <section className="martin-panel"><div className="martin-panel-heading"><div><h2>{bookLabel} · {activeGroup === "open" || activeGroup === "closed" ? `${groupLabel} positions` : `${groupLabel} candidates`}</h2><p>{activeGroup === "selected" || activeGroup === "pending" ? "Candidates selected or awaiting a simulated limit fill in the latest session" : activeGroup === "all" ? "All candidates from the latest session" : "Position outcomes across all saved sessions"}</p></div>{activeGroup !== "all" ? <button type="button" onClick={() => { setActiveGroup("all"); setDetailTicker(null); }}>All candidates</button> : null}</div>
      {activeGroup === "open" || activeGroup === "closed" ? <TodayPositions rows={activeGroup === "open" ? openRows : closedRows} group={activeGroup} /> : rows.length ? <div className="martin-table-wrap martin-candidate-table"><table><thead><tr>{["Ticker", "Status", "Entry limit", "Stop", "Size", "Next expected event", "Details"].map((label) => <th key={label} scope="col">{label}</th>)}</tr></thead><tbody>{rows.map((row, index) => { const key = String(row.ticker ?? index); return <FragmentRow key={key} row={row} expanded={detailTicker === key} onToggle={() => setDetailTicker(detailTicker === key ? null : key)} />; })}</tbody></table></div> : <p className="martin-empty">{activeGroup === "all" ? "No candidates in the latest snapshot." : activeGroup === "selected" ? "No candidates are selected for this session." : "No candidate orders are pending in this book."}</p>}
    </section>
    <details className="martin-exclusions"><summary>Excluded candidates and exceptions <span>{excludedRows.length + attention.length} candidates</span></summary>
      <p>Routine strategy exclusions are separate from missing data and order-processing problems.</p>
      <h3>Routine exclusions</h3>{excludedRows.length ? <SimpleTable rows={excludedRows} columns={["ticker", "status", "reason_summary"]} /> : <p className="martin-empty">No routine exclusions in this snapshot.</p>}
      <h3>Needs attention</h3>{attention.length ? <SimpleTable rows={attention} columns={["ticker", "status", "reason_summary"]} /> : <p className="martin-empty">No missing data or order-processing errors in this snapshot.</p>}
    </details>
  </div>;
}

function FragmentRow({ row, expanded, onToggle }: { row: MartinForwardRow; expanded: boolean; onToggle: () => void }) {
  const details = ["selection_date", "order_date", "queue_rank", "source_snapshot_rank", "selection_reason", "entry_policy_version", "market_data_state", "market_data_reason", "entry", "max_entry", "sector", "sector_quadrant", "selection_tier", "adr14_pct", "adr_suitability", "stop_to_adr", "strike_score", "scanner_count", "risk_dollars", "allocation_dollars", "selection_metadata", "reasons"];
  const nextEvent = row.market_data_state === "awaiting_data" ? "Awaiting exact session bar; order remains reserved" : row.status === "pending" ? "Check next completed session for limit fill" : row.status === "filled" ? "Manage open position at stop" : candidateAction(row);
  return <>
    <tr><th scope="row">{String(row.ticker ?? "—")}</th><td>{statusLabel(String(row.status ?? "Unknown"))}</td><td>{formatPrice(row.limit_price)}</td><td>{formatPrice(row.stop)}</td><td>{format(row.shares ?? row.position_size)}</td><td>{nextEvent}</td><td><button type="button" aria-expanded={expanded} onClick={onToggle}>{expanded ? "Hide details" : "Details"}</button></td></tr>
    {expanded ? <tr><td colSpan={7}><dl className="martin-details">{details.filter((field) => row[field] != null).map((field) => <div key={field}><dt>{field === "reasons" ? "Reason details" : field.replace(/_/g, " ")}</dt><dd>{field === "reasons" ? humanReasons(row[field]) : format(row[field])}{field === "reasons" ? <small className="martin-raw-code">Stored value: {format(row[field])}</small> : null}</dd></div>)}</dl></td></tr> : null}
  </>;
}

function TodayPositions({ rows, group }: { rows: MartinForwardRow[]; group: "open" | "closed" }) {
  const [expanded, setExpanded] = useState<string | null>(null);
  return rows.length ? <div className="martin-position-list">{rows.map((raw, index) => { const row = unpack(raw), key = String(raw.id ?? raw.ticker ?? index); return <article className="martin-position" key={key}><div className="martin-position-compact"><strong>{String(raw.ticker ?? "—")}</strong><span>{statusLabel(String(raw.status ?? "Unknown"))}</span><span>Entry {formatPrice(row.entry_price)}</span><span>Remaining {format(row.remaining_shares)} shares</span><span>Stop {formatPrice(row.stop)}</span><span>Realized {format(row.realized_r)} R · {formatCurrency(row.realized_pnl)}</span><span>Scheduled exit {row.exit_next_open ? "Next open" : "—"}</span><button type="button" aria-expanded={expanded === key} onClick={() => setExpanded(expanded === key ? null : key)}>{expanded === key ? "Hide details" : "Details"}</button></div>{expanded === key ? <dl className="martin-details">{["selection_date", "order_date", "entry_policy_version", "opened_on", "initial_shares", "holding_sessions", "exit_price", "exit_type", "exit_next_open"].map((field) => <div key={field}><dt>{field.replace(/_/g, " ")}</dt><dd>{format(row[field])}</dd></div>)}</dl> : null}</article>; })}</div> : <p className="martin-empty">{group === "open" ? "No simulated entries have filled yet. Pending orders are evaluated against the next completed session." : "No simulated positions have closed yet."}</p>;
}

function Positions({ positions, filter, setFilter, expanded, setExpanded }: { positions: MartinForwardRow[]; filter: string; setFilter: (value: string) => void; expanded: string | null; setExpanded: (value: string | null) => void }) {
  const rows = positions.filter((row) => filter === "all" || (filter === "closed" ? row.status === "closed" : filter === "needs_action" ? row.status === "open" && Boolean(unpack(row).exit_next_open) : row.status === "open"));
  const openCount = positions.filter((row) => row.status === "open").length;
  const needsActionCount = positions.filter((row) => row.status === "open" && Boolean(unpack(row).exit_next_open)).length;
  const closedCount = positions.filter((row) => row.status === "closed").length;
  const realizedR = positions.filter((row) => row.status === "closed").reduce((sum, row) => sum + Number(unpack(row).realized_r || 0), 0);
  return <div className="martin-view"><section className="martin-lifecycle" aria-label="Position summary">{[["Open", openCount], ["Needs action", needsActionCount], ["Closed", closedCount], ["Closed R", realizedR]].map(([label, value]) => <div key={String(label)}><span>{label}</span><strong>{format(value)}</strong></div>)}</section>
    <section className="martin-panel"><div className="martin-panel-heading"><div><h2>Positions</h2><p>Realized results and remaining exposure by position</p></div>
    <label>Show <select value={filter} onChange={(event) => setFilter(event.target.value)}><option value="open">Open</option><option value="needs_action">Needs action</option><option value="closed">Closed</option><option value="all">All</option></select></label></div>
    {!rows.length ? <p className="martin-empty">No positions match this filter.</p> : <div className="martin-position-list">{rows.map((raw, index) => { const row = unpack(raw); const key = String(raw.id ?? index); return <article className="martin-position" key={key}>
      <button type="button" className="martin-position-summary" aria-expanded={expanded === key} onClick={() => setExpanded(expanded === key ? null : key)}><strong>{String(raw.ticker)}</strong><span>{statusLabel(String(raw.status))}</span><span>Entry {formatPrice(row.entry_price)}</span><span>Remaining {format(row.remaining_shares)}</span><span>Stop {formatPrice(row.stop)}</span><span>Realized {format(row.realized_r)} R · {formatCurrency(row.realized_pnl)}</span><span>Exit {row.exit_next_open ? "Next open" : "—"}</span></button>
      {expanded === key ? <dl className="martin-details">{["selection_date", "order_date", "entry_policy_version", "entry_price", "stop", "initial_shares", "remaining_shares", "holding_sessions", "exit_price", "exit_next_open", "exit_type"].map((field) => <div key={field}><dt>{field.replace(/_/g, " ")}</dt><dd>{format(row[field])}</dd></div>)}</dl> : null}
    </article>; })}</div>}
    </section></div>;
}

function Performance({ book, books }: { book: MartinForwardBook; books: MartinForwardResponse["books"] }) {
  const metrics = book.metrics;
  const primary = [["Cumulative R", metrics.cumulative_r == null ? null : `${format(metrics.cumulative_r)} R`], ["Realized P&L", formatCurrency(metrics.realized_pnl)], ["Expectancy", metrics.expectancy_r == null ? null : `${format(metrics.expectancy_r)} R / trade`], ["Max drawdown", metrics.max_r_drawdown == null ? null : `${format(metrics.max_r_drawdown)} R`]];
  return <div className="martin-view">
    {Number(metrics.closed_count ?? 0) < 20 ? <p className="martin-inline-state" role="note">Early sample: fewer than 20 closed trades. Performance metrics are preliminary.</p> : null}
    <section className="martin-metrics martin-metrics-primary">{primary.map(([label, value]) => <div className="martin-metric" key={String(label)}><span>{label}</span><strong>{format(value)}</strong></div>)}</section>
    <section className="martin-panel"><h2>Additional metrics</h2><div className="martin-metrics">{[["Realized R", metrics.realized_r], ["Unrealized R", metrics.unrealized_r], ["Realized P&L", formatCurrency(metrics.realized_pnl)], ["Closed trades", metrics.closed_count], ["Win rate", metrics.win_rate_pct == null ? null : `${format(metrics.win_rate_pct)}%`], ["Profit factor", metrics.profit_factor_r], ["Average holding sessions", metrics.average_holding_sessions], ["Gross wins", formatCurrency(metrics.gross_wins)], ["Gross losses", formatCurrency(metrics.gross_losses)], ["Account equity", book === books.account ? formatCurrency(metrics.account_equity) : null], ["Equity drawdown", book === books.account && metrics.equity_drawdown_pct != null ? `${format(metrics.equity_drawdown_pct)}%` : null]].map(([label, value]) => <div className="martin-metric" key={String(label)}><span>{label}</span><strong>{format(value)}</strong></div>)}</div></section>
    <section className="martin-panel"><h2>Book comparison</h2><SimpleTable rows={(["account", "sector_opportunity", "daily_top5"] as const).flatMap((key) => books[key] ? [{ name: key === "account" ? "Account" : key === "sector_opportunity" ? "Sector opportunity" : "Daily Top 5", ...books[key]!.metrics, cumulative_r: books[key]!.metrics.cumulative_r == null ? null : `${format(books[key]!.metrics.cumulative_r)} R`, expectancy_r: books[key]!.metrics.expectancy_r == null ? null : `${format(books[key]!.metrics.expectancy_r)} R`, realized_pnl: formatCurrency(books[key]!.metrics.realized_pnl), account_equity: key === "account" ? formatCurrency(books[key]!.metrics.account_equity) : null, equity_drawdown_pct: key === "account" && books[key]!.metrics.equity_drawdown_pct != null ? `${format(books[key]!.metrics.equity_drawdown_pct)}%` : null, max_r_drawdown: books[key]!.metrics.max_r_drawdown == null ? null : `${format(books[key]!.metrics.max_r_drawdown)} R` }] : [])} columns={["name", "cumulative_r", "realized_r", "unrealized_r", "expectancy_r", "realized_pnl", "win_rate_pct", "profit_factor_r", "account_equity", "equity_drawdown_pct", "max_r_drawdown"]} /></section>
    <section className="martin-panel"><h2>Cumulative R comparison</h2><ComparisonChart books={books} field="cumulative_r" /></section>
    {books.account ? <section className="martin-panel"><h2>Account book equity and drawdown</h2><p className="martin-panel-copy">Account sizing metrics use the account book’s fixed simulation capital.</p><HistoryChart points={books.account.history} field="account_equity" /><HistoryChart points={books.account.history} field="equity_drawdown_pct" /></section> : null}
    <section className="martin-panel"><h2>Trade attribution</h2>{Object.entries(book.attribution).map(([dimension, values]) => <div key={dimension}><h3>{dimension.replace(/_/g, " ")}</h3><SimpleTable rows={Object.entries(values).map(([name, result]) => ({ name, ...result }))} columns={["name", "count", "realized_r", "realized_pnl"]} /></div>)}</section>
  </div>;
}

function HistoryChart({ points, field }: { points: MartinForwardBook["history"]; field: "cumulative_r" | "account_equity" | "equity_drawdown_pct" }) {
  const usable = points.filter((point) => point[field] != null);
  if (!usable.length) return <p className="martin-empty">History will appear after positions close.</p>;
  const values = usable.map((point) => Number(point[field]));
  const low = Math.min(...values), high = Math.max(...values), span = high - low || 1;
  const path = values.map((value, index) => `${index ? "L" : "M"}${(index / Math.max(values.length - 1, 1)) * 100},${35 - ((value - low) / span) * 30}`).join(" ");
  const lastPoint = usable[usable.length - 1];
  return <div className="martin-chart-block"><p className="martin-chart-title">{chartLabel(field)}</p><div className="martin-chart" role="img" aria-label={`${chartLabel(field)} from ${usable[0].date} ${chartValue(field, values[0])} to ${lastPoint.date} ${chartValue(field, values[values.length - 1])}`}><div className="martin-chart-scale"><span>{chartValue(field, high)}</span><span>{chartValue(field, low)}</span></div><svg viewBox="0 0 100 40" preserveAspectRatio="none"><path d={path} /></svg><div><span>{usable[0].date}</span><span>{lastPoint.date}</span></div></div></div>;
}

function ComparisonChart({ books, field }: { books: MartinForwardResponse["books"]; field: "cumulative_r" }) {
  const series = [["Account", books.account?.history ?? []], ["Sector opportunity", books.sector_opportunity?.history ?? []], ["Daily Top 5", books.daily_top5?.history ?? []]] as const;
  const dates = Array.from(new Set(series.flatMap(([, points]) => points.map((point) => point.date)))).sort();
  const aligned = series.map(([name, points]) => {
    const byDate = new Map(points.map((point) => [point.date, Number(point[field])]));
    let previous: number | null = null;
    return { name, values: dates.map((date) => { previous = byDate.get(date) ?? previous; return previous; }) };
  });
  const values = aligned.flatMap((item) => item.values.filter((value): value is number => value != null));
  if (!values.length) return <p className="martin-empty">History will appear after positions close.</p>;
  const low = Math.min(...values), span = Math.max(...values) - low || 1;
  return <div className="martin-chart-block"><p className="martin-chart-title">Cumulative return in risk units (R)</p><div className="martin-chart martin-comparison-chart" role="img" aria-label={`Cumulative R comparison from ${dates[0]} to ${dates[dates.length - 1]}, values from ${format(Math.min(...values))} to ${format(Math.max(...values))} R`}><div className="martin-chart-scale"><span>{format(Math.max(...values))} R</span><span>{format(Math.min(...values))} R</span></div><svg viewBox="0 0 100 40" preserveAspectRatio="none">{aligned.map(({ name, values: points }, seriesIndex) => { const path = points.map((value, index) => value == null ? "" : `${index && points[index - 1] != null ? "L" : "M"}${(index / Math.max(points.length - 1, 1)) * 100},${35 - ((value - low) / span) * 30}`).filter(Boolean).join(" "); return <path key={name} className={["account-series", "sector-series", "daily-top5-series"][seriesIndex]} d={path} />; })}</svg><div><span>{dates[0]}–{dates[dates.length - 1]}</span>{aligned.map((item, index) => <span key={item.name} className={["account-legend", "sector-legend", "daily-top5-legend"][index]}>{item.name}</span>)}</div></div></div>;
}

function SimpleTable({ rows, columns }: { rows: MartinForwardRow[]; columns: string[] }) {
  return <div className={`martin-table-wrap ${columns.includes("cumulative_r") ? "martin-comparison-table" : ""}`}><table><thead><tr>{columns.map((column) => <th key={column} scope="col">{columnLabel(column)}</th>)}</tr></thead><tbody>{rows.map((raw, index) => { const row = unpack(raw); return <tr key={String(raw.id ?? index)}>{columns.map((column) => <td key={column}>{column === "reason_summary" ? <><span>{humanReasons(row.reasons)}</span><details className="martin-code-detail"><summary>Stored reason code</summary><code>{format(row.reasons)}</code></details></> : column === "status" ? statusLabel(String(row[column] ?? raw[column] ?? "Unknown")) : format(row[column] ?? raw[column])}</td>)}</tr>; })}</tbody></table></div>;
}

function StatePanel({ title, detail }: { title: string; detail: string }) { return <section className="martin-state" role="status"><h2>{title}</h2><p>{detail}</p></section>; }
function object(value: unknown): Record<string, unknown> { return value && typeof value === "object" ? value as Record<string, unknown> : {}; }
function unpack(row: MartinForwardRow): MartinForwardRow { return { ...object(row.payload_json), ...row, ...object(row.payload_json), reasons: row.reasons_json ?? row.reasons ?? object(row.payload_json).reasons, status: row.status ?? object(row.payload_json).status, rank: row.rank ?? object(row.payload_json).rank }; }
function isSelected(row: MartinForwardRow): boolean { return row.selected_for_book === true || Boolean(row.selection_tier); }
function formatCurrency(value: unknown): string { return typeof value === "number" ? `$${value.toLocaleString(undefined, { maximumFractionDigits: 2 })}` : "—"; }
function formatPrice(value: unknown): string { return typeof value === "number" ? `$${value.toLocaleString(undefined, { maximumFractionDigits: 2 })}` : "—"; }
function candidateAction(row: MartinForwardRow): string {
  const status = String(row.status ?? "");
  const reasons = Array.isArray(row.reasons) ? row.reasons.map(String).join(", ") : String(row.reasons ?? "");
  if (status === "pending") return `Wait for limit fill at ${formatPrice(row.limit_price)}`;
  if (status === "eligible") return "Selected; order awaits sizing";
  if (status === "filled") return `Manage open position; stop ${formatPrice(row.stop)}`;
  if (status.startsWith("blocked")) return `Resolve block${reasons ? `: ${reasons}` : ""}`;
  if (status === "expired" || status === "cancelled") return `Review ${status} order${reasons ? `: ${reasons}` : ""}`;
  if (status === "not_selected") return `Not selected${reasons ? `: ${reasons}` : ""}`;
  if (status === "watch" || status === "avoid") return `No order: ${status}`;
  return reasons || "Review snapshot levels";
}
function format(value: unknown): string { if (value == null) return "—"; if (typeof value === "number") return value.toLocaleString(undefined, { maximumFractionDigits: 2 }); if (typeof value === "object") return JSON.stringify(value); return String(value); }
function chartLabel(field: "cumulative_r" | "account_equity" | "equity_drawdown_pct"): string { return ({ cumulative_r: "Cumulative realized and open risk units (R)", account_equity: "Simulated account equity (USD)", equity_drawdown_pct: "Account equity drawdown (%)" } as const)[field]; }
function chartValue(field: "cumulative_r" | "account_equity" | "equity_drawdown_pct", value: number): string { return field === "account_equity" ? `$${format(value)}` : field === "equity_drawdown_pct" ? `${format(value)}%` : `${format(value)} R`; }
function columnLabel(value: string): string { return ({ source_run_id: "Source run", created_at: "Created", session_date: "Session date", event_date: "Event date", event_type: "Event", ticker: "Ticker", status: "Status", realized_r: "Realized R", realized_pnl: "Realized P&L (USD)", cumulative_r: "Cumulative R", unrealized_r: "Unrealized R", expectancy_r: "Expectancy R", profit_factor_r: "Profit factor", account_equity: "Account equity (USD)", equity_drawdown_pct: "Equity drawdown %", max_r_drawdown: "Max drawdown R", win_rate_pct: "Win rate %", name: "Book", count: "Trades", reason_summary: "Reason summary" } as Record<string, string>)[value] ?? value.replace(/_/g, " "); }
function statusLabel(status: string): string { return ({ not_selected: "Not selected", blocked: "Excluded · failed strategy gates", blocked_data: "Blocked · missing data", blocked_capacity: "Blocked · capacity", waiting_capacity: "Waiting for capacity", blocked_open_position: "Blocked · existing position", blocked_existing_order: "Blocked · existing order", pending: "Pending", filled: "Filled", closed: "Closed", open: "Open", watch: "Watch", avoid: "Avoid", error: "Processing error", cancelled: "Cancelled", expired: "Expired" } as Record<string, string>)[status] ?? status.replace(/_/g, " "); }
function hasMissingReason(value: unknown): boolean { const reasons = Array.isArray(value) ? value.map(String) : value == null ? [] : [String(value)]; return reasons.some((reason) => ["missing_ticker", "missing_sector", "missing_adr_or_stop", "invalid_entry_stop_levels"].includes(reason)); }
function humanReasons(value: unknown): string {
  const labels: Record<string, string> = { missing_ticker: "Ticker is missing", market_cap_below_1b_or_missing: "Market cap is below $1B or unavailable", close_below_5_or_missing: "Closing price is below $5 or unavailable", rs_outside_90_99_or_missing: "Relative strength is outside 90–99 or unavailable", no_martin_focus_scanner: "No Martin Focus scanner match", marked_avoid: "Marked avoid by trade readiness", trade_readiness_not_defined_risk: "Trade risk is not defined", missing_adr_or_stop: "ADR or stop level is missing", invalid_entry_stop_levels: "Entry and stop levels are invalid", missing_sector: "Sector data is missing", sector_quota_or_session_limit: "Outside sector quota or session limit", open_position_exists: "Position already open", pending_order_exists: "An order is already pending", position_or_allocation_capacity: "Position or allocation capacity reached", not_in_complete_snapshot: "Ticker was not in the complete snapshot", open_position_already_exists: "Position already open", same_day_stop: "Stop was reached on entry day" };
  const reasons = Array.isArray(value) ? value.map(String) : value == null ? [] : [String(value)];
  return reasons.map((reason) => labels[reason] ?? reason.replace(/_/g, " ")).join("; ") || "No reason recorded";
}
