import { useEffect, useMemo, useState } from "react";
import { useSearchParams } from "react-router-dom";
import type { MartinForwardBook, MartinForwardResponse, MartinForwardRow } from "../lib/types";

type View = "today" | "positions" | "performance";
const views: View[] = ["today", "positions", "performance"];

export default function MartinForwardTestPage() {
  const [params, setParams] = useSearchParams();
  const view = views.includes(params.get("view") as View) ? params.get("view") as View : "today";
  const bookKey = params.get("book") === "sector_opportunity" ? "sector_opportunity" : "account";
  const [data, setData] = useState<MartinForwardResponse | null>(null);
  const [error, setError] = useState("");
  const [filter, setFilter] = useState("open");
  const [expanded, setExpanded] = useState<string | null>(null);
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
  const pending = candidateRows.filter((row) => row.status === "pending").length;
  const attention = candidateRows.filter((row) => String(row.status).startsWith("blocked") || ["error", "cancelled", "expired"].includes(String(row.status)));
  const summary = object(latest?.summary_json);
  const sessionDate = String(latest?.session_date ?? "");
  const stale = sessionDate ? (Date.now() - new Date(`${sessionDate}T00:00:00`).getTime()) > 3 * 86400000 : false;
  const setQuery = (key: string, value: string) => setParams((previous) => { const next = new URLSearchParams(previous); next.set(key, value); return next; });

  return <main className="page-shell martin-page">
    <header className="martin-header">
      <div><span className="martin-eyebrow">PAPER STRATEGY WORKSPACE</span><h1>Martin Focus Forward Test</h1>
        <p>Historical snapshot candidates, simulated orders and position outcomes.</p></div>
      <span className="martin-simulation-badge">SIMULATED · NO BROKER ORDERS</span>
    </header>
    {error ? <StatePanel title="Database error" detail={error} /> : !data ? <StatePanel title="Loading forward test" detail="Loading saved sessions and positions…" /> : !data.configured ? <StatePanel title="Database unavailable" detail="Forward test persistence is not configured." /> : <>
      <div className="martin-toolbar">
        <div className="martin-book-switch" aria-label="Strategy book">
          <button type="button" aria-pressed={bookKey === "account"} onClick={() => setQuery("book", "account")}>Account book</button>
          <button type="button" aria-pressed={bookKey === "sector_opportunity"} onClick={() => setQuery("book", "sector_opportunity")}>Sector opportunity</button>
        </div>
        <div className="martin-freshness"><span className={stale ? "martin-dot stale" : "martin-dot"} />
          {latest ? `${stale ? "Stale snapshot" : "Snapshot current"} · ${sessionDate}` : "No session processed"}
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
        {view === "today" ? <Today book={book!} candidates={candidateRows} attention={attention} pending={pending} /> : null}
        {view === "positions" ? <Positions positions={positionRows} filter={filter} setFilter={setFilter} expanded={expanded} setExpanded={setExpanded} /> : null}
        {view === "performance" ? <Performance book={book!} books={data.books} /> : null}
        <details className="martin-audit"><summary>Data &amp; audit</summary>
          <p>Session {sessionDate} · snapshot fingerprint {String(summary.source_fingerprint ?? latest.source_fingerprint ?? "—")}</p>
          <h3>Stored runs</h3><SimpleTable rows={book!.runs} columns={["session_date", "source_run_id", "created_at"]} />
          <h3>Recent events</h3><SimpleTable rows={book!.events} columns={["event_date", "ticker", "event_type", "payload_json"]} />
          <h3>Raw session data</h3><pre>{JSON.stringify({ candidates: book!.candidates, positions: book!.positions }, null, 2)}</pre>
        </details>
      </>}
    </>}
  </main>;
}

function Today({ book, candidates, attention, pending }: { book: MartinForwardBook; candidates: MartinForwardRow[]; attention: MartinForwardRow[]; pending: number }) {
  const [detailTicker, setDetailTicker] = useState<string | null>(null);
  const open = book.positions.filter((row) => row.status === "open").length;
  return <div className="martin-view">
    <section className="martin-lifecycle" aria-label="Session lifecycle">
      {[{ label: "Selected", value: candidates.filter(isSelected).length }, { label: "Pending", value: pending }, { label: "Open", value: open }, { label: "Closed", value: book.metrics.closed_count ?? 0 }].map((item) => <div key={item.label}><span>{item.label}</span><strong>{String(item.value)}</strong></div>)}
    </section>
    <section className="martin-panel"><div className="martin-panel-heading"><div><h2>Needs attention</h2><p>Blocked candidates and order failures that need review</p></div><strong>{attention.length}</strong></div>
      {attention.length ? <SimpleTable rows={attention} columns={["ticker", "status", "reasons", "sector", "sector_quadrant"]} /> : <p className="martin-empty">No candidate exceptions in this snapshot.</p>}
    </section>
    <section className="martin-panel"><div className="martin-panel-heading"><div><h2>Candidate funnel</h2><p>Snapshot rank and persisted sector evidence</p></div></div>
      {candidates.length ? <div className="martin-table-wrap"><table><thead><tr>{["ticker", "tier", "sector", "quadrant", "status", "trigger", "max entry", "stop", "risk / ADR", "size", "next action", "details"].map((label) => <th key={label} scope="col">{label}</th>)}</tr></thead><tbody>{candidates.map((row, index) => { const key = String(row.ticker ?? index); return <><tr key={key}><th scope="row">{String(row.ticker ?? "—")}</th><td>{format(row.selection_tier ?? (isSelected(row) ? "Selected" : null))}</td><td>{format(row.sector)}</td><td>{format(row.sector_quadrant)}</td><td>{format(row.status)}</td><td>{formatPrice(row.entry)}</td><td>{formatPrice(row.max_entry)}</td><td>{formatPrice(row.stop)}</td><td>{riskAdr(row)}</td><td>{format(row.shares)}</td><td>{candidateAction(row)}</td><td><button type="button" aria-expanded={detailTicker === key} onClick={() => setDetailTicker(detailTicker === key ? null : key)}>{detailTicker === key ? "Hide" : "View"}</button></td></tr>{detailTicker === key ? <tr key={`${key}-details`}><td colSpan={12}><dl className="martin-details">{["reasons", "limit_price", "risk_dollars", "allocation_dollars", "adr14_pct", "adr_suitability", "stop_to_adr", "strike_score", "scanner_count", "selection_metadata"].map((field) => <div key={field}><dt>{field.replace(/_/g, " ")}</dt><dd>{format(row[field])}</dd></div>)}</dl></td></tr> : null}</>; })}</tbody></table></div> : <p className="martin-empty">No candidates in the latest snapshot.</p>}
    </section>
  </div>;
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
      <button type="button" className="martin-position-summary" aria-expanded={expanded === key} onClick={() => setExpanded(expanded === key ? null : key)}><strong>{String(raw.ticker)}</strong><span>{String(raw.status)}</span><span>Opened {String(raw.opened_on ?? "—")}</span><span>{format(row.realized_r)} R</span><span>{format(row.realized_pnl)}</span></button>
      {expanded === key ? <dl className="martin-details">{["entry_price", "stop", "initial_shares", "remaining_shares", "holding_sessions", "exit_price", "exit_next_open", "exit_type"].map((field) => <div key={field}><dt>{field.replace(/_/g, " ")}</dt><dd>{format(row[field])}</dd></div>)}</dl> : null}
    </article>; })}</div>}
    </section></div>;
}

function Performance({ book, books }: { book: MartinForwardBook; books: MartinForwardResponse["books"] }) {
  const metrics = book.metrics;
  return <div className="martin-view">
    {Number(metrics.closed_count ?? 0) < 20 ? <p className="martin-inline-state" role="note">Early sample: fewer than 20 closed trades. Performance metrics are preliminary.</p> : null}
    <section className="martin-metrics">{[["Cumulative R", metrics.cumulative_r], ["Realized R", metrics.realized_r], ["Unrealized R", metrics.unrealized_r], ["Realized P&L", metrics.realized_pnl], ["Closed trades", metrics.closed_count], ["Expectancy R", metrics.expectancy_r], ["Win rate", metrics.win_rate_pct == null ? null : `${format(metrics.win_rate_pct)}%`], ["Profit factor R", metrics.profit_factor_r], ["Average holding sessions", metrics.average_holding_sessions], ["Gross wins", metrics.gross_wins], ["Gross losses", metrics.gross_losses], ["Max R drawdown", metrics.max_r_drawdown], ["Account equity", metrics.account_equity], ["Equity drawdown", metrics.equity_drawdown_pct == null ? null : `${format(metrics.equity_drawdown_pct)}%`]].map(([label, value]) => <div className="martin-metric" key={String(label)}><span>{label}</span><strong>{format(value)}</strong></div>)}</section>
    <section className="martin-panel"><h2>Book comparison</h2><SimpleTable rows={(["account", "sector_opportunity"] as const).flatMap((key) => books[key] ? [{ name: key === "account" ? "Account" : "Sector opportunity", ...books[key]!.metrics }] : [])} columns={["name", "cumulative_r", "realized_r", "unrealized_r", "expectancy_r", "win_rate_pct", "profit_factor_r", "account_equity", "equity_drawdown_pct", "max_r_drawdown"]} /></section>
    <section className="martin-panel"><h2>Cumulative R comparison</h2><ComparisonChart books={books} field="cumulative_r" /></section>
    <section className="martin-panel"><h2>Account equity and drawdown</h2><HistoryChart points={(books.account ?? book).history} field="account_equity" /><HistoryChart points={(books.account ?? book).history} field="equity_drawdown_pct" /></section>
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
  return <div className="martin-chart" role="img" aria-label={`${field.replace(/_/g, " ")} over time`}><svg viewBox="0 0 100 40" preserveAspectRatio="none"><path d={path} /></svg><div><span>{usable[0].date} · {format(values[0])}</span><span>{lastPoint.date} · {format(values[values.length - 1])}</span></div></div>;
}

function ComparisonChart({ books, field }: { books: MartinForwardResponse["books"]; field: "cumulative_r" }) {
  const series = [["Account", books.account?.history ?? []], ["Sector opportunity", books.sector_opportunity?.history ?? []]] as const;
  const dates = Array.from(new Set(series.flatMap(([, points]) => points.map((point) => point.date)))).sort();
  const aligned = series.map(([name, points]) => {
    const byDate = new Map(points.map((point) => [point.date, Number(point[field])]));
    let previous: number | null = null;
    return { name, values: dates.map((date) => { previous = byDate.get(date) ?? previous; return previous; }) };
  });
  const values = aligned.flatMap((item) => item.values.filter((value): value is number => value != null));
  if (!values.length) return <p className="martin-empty">History will appear after positions close.</p>;
  const low = Math.min(...values), span = Math.max(...values) - low || 1;
  return <div className="martin-chart martin-comparison-chart" role="img" aria-label="Cumulative R comparison by book"><svg viewBox="0 0 100 40" preserveAspectRatio="none">{aligned.map(({ name, values: points }, seriesIndex) => { const path = points.map((value, index) => value == null ? "" : `${index && points[index - 1] != null ? "L" : "M"}${(index / Math.max(points.length - 1, 1)) * 100},${35 - ((value - low) / span) * 30}`).filter(Boolean).join(" "); return <path key={name} className={seriesIndex ? "sector-series" : "account-series"} d={path} />; })}</svg><div><span>{dates[0]}–{dates[dates.length - 1]}</span>{aligned.map((item, index) => <span key={item.name} className={index ? "sector-legend" : "account-legend"}>{item.name}</span>)}</div></div>;
}

function SimpleTable({ rows, columns }: { rows: MartinForwardRow[]; columns: string[] }) {
  return <div className="martin-table-wrap"><table><thead><tr>{columns.map((column) => <th key={column} scope="col">{column.replace(/_/g, " ")}</th>)}</tr></thead><tbody>{rows.map((raw, index) => { const row = unpack(raw); return <tr key={String(raw.id ?? index)}>{columns.map((column) => <td key={column}>{format(row[column] ?? raw[column])}</td>)}</tr>; })}</tbody></table></div>;
}

function StatePanel({ title, detail }: { title: string; detail: string }) { return <section className="martin-state" role="status"><h2>{title}</h2><p>{detail}</p></section>; }
function object(value: unknown): Record<string, unknown> { return value && typeof value === "object" ? value as Record<string, unknown> : {}; }
function unpack(row: MartinForwardRow): MartinForwardRow { return { ...object(row.payload_json), ...row, ...object(row.payload_json), reasons: row.reasons_json ?? object(row.payload_json).reasons, status: row.status ?? object(row.payload_json).status, rank: row.rank ?? object(row.payload_json).rank }; }
function isSelected(row: MartinForwardRow): boolean { return row.selected_for_book === true || Boolean(row.selection_tier) || ["eligible", "pending", "filled"].includes(String(row.status)); }
function formatPrice(value: unknown): string { return typeof value === "number" ? `$${value.toLocaleString(undefined, { maximumFractionDigits: 2 })}` : "—"; }
function riskAdr(row: MartinForwardRow): string {
  const ratio = typeof row.stop_to_adr === "number" ? `Stop/ADR ${format(row.stop_to_adr)}×` : null;
  const adr = typeof row.adr14_pct === "number" ? `ADR ${format(row.adr14_pct)}%` : null;
  const risk = typeof row.risk_dollars === "number" ? `Risk ${format(row.risk_dollars)}` : null;
  return [ratio, adr, risk].filter(Boolean).join(" · ") || "—";
}
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
