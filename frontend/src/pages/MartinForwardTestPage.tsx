import { Fragment, useEffect, useMemo, useState } from "react";
import { useSearchParams } from "react-router-dom";
import type { MartinForwardBook, MartinForwardResponse, MartinForwardRow } from "../lib/types";

type View = "today" | "positions" | "performance";
const views: View[] = ["today", "positions", "performance"];
const bookLabels = { account: "Account book", sector_opportunity: "Sector opportunity", daily_top5: "Daily Top 5", rs_expanded: "RS Expanded · experimental" } as const;

export default function MartinForwardTestPage() {
  const [params, setParams] = useSearchParams();
  const view = views.includes(params.get("view") as View) ? params.get("view") as View : "today";
  const requestedBook = params.get("book");
  const bookKey = requestedBook === "sector_opportunity" || requestedBook === "daily_top5" || requestedBook === "rs_expanded" ? requestedBook : "account";
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
  const attention = candidateRows.filter((row) => row.status === "blocked_data" || ["error", "cancelled", "expired"].includes(String(row.status)) || hasMissingReason(row.reasons_json ?? row.reasons ?? object(row.payload_json).reasons, row));
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
          {Object.entries(bookLabels).map(([key, label]) => <button key={key} type="button" aria-pressed={bookKey === key} onClick={() => setQuery("book", key)}>{label}</button>)}
        </div>
        <p className="martin-book-description">{bookKey === "account" ? "Account book: fixed $100,000 sizing base, 0.5% risk per trade, 20% allocation cap, and up to five open or pending positions." : bookKey === "sector_opportunity" ? "Sector opportunity: up to ten sector-ranked candidates, simulated at 100 shares each without an account-capital limit." : bookKey === "rs_expanded" ? "Experimental account: RS 90–99 preferred, with RS 80–89 eligible only after additional trend, confirmation and liquidity checks. Baseline Account book and Top Hits defaults are unchanged." : "Daily Top 5: up to five new 100-share orders each session, with no concurrent holding or capital limit."}</p>
        {bookKey === "rs_expanded" ? <details className="martin-code-detail"><summary>Experiment rules and comparison limits</summary><p>Same fixed $100,000 sizing base, 0.5% risk per trade, 20% allocation cap and five-position capacity as the Account book. Entry-to-stop risk stays capped at 5%. Entries use next-session limits; management trims 10% at 3R and exits after a daily close below EMA9.</p><p>Structural v1 applies to both RS tiers: MA retests use the completed daily reclaim candle low with no extra buffer; EMA21/SMA200 retain their saved support-test low. Stops wider than half ADR are cautions, not automatic rejections. Missing structural evidence blocks entry; no ATR fallback is used. This daily adaptation is Martin-inspired, not an exact replication of his discretionary rules. Top Hits and the other books retain their existing stop policy.</p><p>RS 80–89 additionally needs rising SMA50/SMA200, price above both, EMA21 above SMA50, improving 20-session strength versus SPY, constructive or neutral pullback quality, a fresh supported daily confirmation, average daily dollar volume of at least $20M, and an order no larger than 0.1% of that volume. Missing evidence blocks eligibility. RS below 80 stays research-only.</p><p>This experiment changes both RS eligibility and stop selection; performance differences cannot be attributed to RS alone. Earlier Account book trades are not copied. Compare matching periods and the RS cohorts separately before drawing conclusions.</p></details> : null}
        <div className="martin-freshness"><span className={stale ? "martin-dot stale" : "martin-dot"} />
          {latest ? `${stale ? "Latest session may be stale" : "Latest session"} · ${sessionDate}` : "No session processed"}
          {latest && summary.source_run_id ? ` · source run ${String(summary.source_run_id)}` : ""}</div>
      </div>
      {!latest ? <StatePanel title="No session yet" detail={bookKey === "rs_expanded" ? "No RS Expanded session has been saved yet. The next Martin forward test job can start this experiment; baseline trades are not copied into it." : "Run the existing Martin forward test job after a persisted snapshot is available."} /> : <>
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
        {view === "today" ? <Today key={bookKey} book={book!} bookLabel={bookLabels[bookKey]} candidates={candidateRows} outstandingOrders={(book?.outstanding_orders ?? []).map(unpack)} attention={attention} /> : null}
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
  const details = ["selection_date", "order_date", "queue_rank", "source_snapshot_rank", "selection_reason", "entry_policy_version", "source_strategy_id", "signal_date", "plan_state", "risk_status", "reference_risk_pct", "market_data_state", "market_data_reason", "entry", "max_entry", "sector", "sector_quadrant", "selection_tier", "adr14_pct", "adr_suitability", "stop_to_adr", "strike_score", "scanner_count", "risk_dollars", "allocation_dollars", "selection_metadata", "reasons"];
  const nextEvent = row.market_data_state === "awaiting_data" ? "Awaiting exact session bar; order remains reserved" : row.status === "pending" ? "Check next completed session for limit fill" : row.status === "filled" ? "Manage open position at stop" : candidateAction(row);
  return <>
    <tr><th scope="row">{String(row.ticker ?? "—")}</th><td>{statusLabel(String(row.status ?? "Unknown"))}</td><td>{formatPrice(row.limit_price)}</td><td>{formatPrice(row.stop)}</td><td>{format(row.shares ?? row.position_size)}</td><td>{nextEvent}</td><td><button type="button" aria-expanded={expanded} onClick={onToggle}>{expanded ? "Hide details" : "Details"}</button></td></tr>
    {expanded ? <tr><td colSpan={7}><dl className="martin-details">{details.filter((field) => row[field] != null).map((field) => <div key={field}><dt>{field === "reasons" ? "Reason details" : field.replace(/_/g, " ")}</dt><dd>{field === "reasons" ? humanReasons(row[field], row) : format(row[field])}{field === "reasons" ? <small className="martin-raw-code">Stored value: {format(row[field])}</small> : null}</dd></div>)}</dl>{row.status === "watch" ? <TradePlanPreview row={row} /> : <><RsEvidence row={row} /><ReadinessExplanation row={row} /></>}</td></tr> : null}
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
  const accountLike = book === books.account || book === books.rs_expanded;
  const primary = [["Cumulative R", metrics.cumulative_r == null ? null : `${format(metrics.cumulative_r)} R`], ["Realized P&L", formatCurrency(metrics.realized_pnl)], ["Expectancy", metrics.expectancy_r == null ? null : `${format(metrics.expectancy_r)} R / trade`], ["Max drawdown", metrics.max_r_drawdown == null ? null : `${format(metrics.max_r_drawdown)} R`]];
  return <div className="martin-view">
    {Number(metrics.closed_count ?? 0) < 20 ? <p className="martin-inline-state" role="note">Early sample: fewer than 20 closed trades. Performance metrics are preliminary.</p> : null}
    <section className="martin-metrics martin-metrics-primary">{primary.map(([label, value]) => <div className="martin-metric" key={String(label)}><span>{label}</span><strong>{format(value)}</strong></div>)}</section>
    {book === books.account ? <BenchmarkComparison comparison={book.benchmark_comparison} /> : null}
    {book === books.rs_expanded ? <section className="martin-panel"><h2>RS cohort outcomes</h2><p className="martin-panel-copy">Closed-trade results for preferred RS 90–99 and conditional emerging RS 80–89 entries. Drawdown is closed-trade R, not account NAV. Small cohorts are preliminary.</p><p className="martin-panel-copy">Cost model: {book.cost_model || "Unavailable in saved results."}</p>{book.rs_tier_metrics ? <><p className="martin-panel-copy">Scroll the table horizontally to inspect all cohort metrics.</p><SimpleTable rows={Object.entries(book.rs_tier_metrics).map(([tier, result]) => ({ ...result, name: rsTierLabel(tier), realized_pnl: formatCurrency(result.realized_pnl) }))} columns={["name", "closed_count", "realized_r", "realized_pnl", "win_rate_pct", "expectancy_r", "stop_out_rate_pct", "average_holding_sessions", "max_closed_drawdown_r"]} /></> : <p className="martin-empty">Saved RS cohort outcomes are unavailable. No baseline trades are substituted.</p>}</section> : null}
    <section className="martin-panel"><h2>Additional metrics</h2><div className="martin-metrics">{[["Realized R", metrics.realized_r], ["Unrealized R", metrics.unrealized_r], ["Realized P&L", formatCurrency(metrics.realized_pnl)], ["Closed trades", metrics.closed_count], ["Win rate", metrics.win_rate_pct == null ? null : `${format(metrics.win_rate_pct)}%`], ["Profit factor", metrics.profit_factor_r], ["Average holding sessions", metrics.average_holding_sessions], ["Gross wins", formatCurrency(metrics.gross_wins)], ["Gross losses", formatCurrency(metrics.gross_losses)], ["Account equity", accountLike ? formatCurrency(metrics.account_equity) : null], ["Equity drawdown", accountLike && metrics.equity_drawdown_pct != null ? `${format(metrics.equity_drawdown_pct)}%` : null]].map(([label, value]) => <div className="martin-metric" key={String(label)}><span>{label}</span><strong>{format(value)}</strong></div>)}</div></section>
    <section className="martin-panel"><h2>Book comparison</h2><p className="martin-panel-copy">Books can start on different dates; these are whole-book results, not matched-period outperformance. Compare the same sessions before drawing conclusions. Only Account and RS Expanded use the fixed $100,000 account sizing base.</p><SimpleTable rows={(Object.keys(bookLabels) as Array<keyof typeof bookLabels>).flatMap((key) => books[key] ? [{ name: bookLabels[key], ...books[key]!.metrics, cumulative_r: books[key]!.metrics.cumulative_r == null ? null : `${format(books[key]!.metrics.cumulative_r)} R`, expectancy_r: books[key]!.metrics.expectancy_r == null ? null : `${format(books[key]!.metrics.expectancy_r)} R`, realized_pnl: formatCurrency(books[key]!.metrics.realized_pnl), account_equity: key === "account" || key === "rs_expanded" ? formatCurrency(books[key]!.metrics.account_equity) : null, equity_drawdown_pct: (key === "account" || key === "rs_expanded") && books[key]!.metrics.equity_drawdown_pct != null ? `${format(books[key]!.metrics.equity_drawdown_pct)}%` : null, max_r_drawdown: books[key]!.metrics.max_r_drawdown == null ? null : `${format(books[key]!.metrics.max_r_drawdown)} R` }] : [])} columns={["name", "cumulative_r", "realized_r", "unrealized_r", "expectancy_r", "realized_pnl", "win_rate_pct", "profit_factor_r", "account_equity", "equity_drawdown_pct", "max_r_drawdown"]} /></section>
    <section className="martin-panel"><h2>Cumulative R comparison</h2><ComparisonChart books={books} field="cumulative_r" /></section>
    {accountLike ? <section className="martin-panel"><h2>{book === books.rs_expanded ? "RS Expanded equity and drawdown" : "Account book equity and drawdown"}</h2><p className="martin-panel-copy">Account sizing metrics use this book’s fixed simulation capital and saved sessions.</p><HistoryChart points={book.history} field="account_equity" /><HistoryChart points={book.history} field="equity_drawdown_pct" /></section> : books.account ? <section className="martin-panel"><h2>Account book equity and drawdown</h2><p className="martin-panel-copy">Account sizing metrics use the account book’s fixed simulation capital.</p><HistoryChart points={books.account.history} field="account_equity" /><HistoryChart points={books.account.history} field="equity_drawdown_pct" /></section> : null}
    <section className="martin-panel"><h2>Trade attribution</h2>{Object.entries(book.attribution).map(([dimension, values]) => <div key={dimension}><h3>{dimension.replace(/_/g, " ")}</h3><SimpleTable rows={Object.entries(values).map(([name, result]) => ({ name, ...result }))} columns={["name", "count", "realized_r", "realized_pnl"]} /></div>)}</section>
  </div>;
}

function BenchmarkComparison({ comparison }: { comparison: MartinForwardBook["benchmark_comparison"] }) {
  if (!comparison || comparison.status !== "available" || !comparison.series) {
    return <section className="martin-panel"><h2>Account vs SPY and QQQ</h2><p className="martin-empty">{comparison?.reason || "Stored account and benchmark history is unavailable."}</p></section>;
  }
  const names = ["Account", "SPY", "QQQ"] as const;
  const dates = Array.from(new Set(names.flatMap((name) => comparison.series![name].map((point) => point.date)))).sort();
  const values = names.flatMap((name) => comparison.series![name].flatMap((point) => point.value == null ? [] : [point.value]));
  const low = Math.min(100, ...values), high = Math.max(100, ...values), span = high - low || 1;
  const pathFor = (name: typeof names[number]) => comparison.series![name].map((point, index, points) => {
    if (point.value == null) return "";
    const previous = index > 0 ? points[index - 1] : null;
    return `${index && previous?.value != null ? "L" : "M"}${(dates.indexOf(point.date) / Math.max(dates.length - 1, 1)) * 100},${35 - ((point.value - low) / span) * 30}`;
  }).filter(Boolean).join(" ");
  const date = comparison.comparison_date || dates[dates.length - 1];
  const atDate = (name: typeof names[number]) => comparison.series![name].find((point) => point.date === date);
  const maxDrawdown = (name: typeof names[number]) => comparison.series![name].filter((point) => point.date <= date && point.drawdown_pct != null).reduce((minimum, point) => Math.min(minimum, point.drawdown_pct!), 0);
  const accountReturn = atDate("Account")?.return_pct;
  const delta = (name: "SPY" | "QQQ") => accountReturn == null || atDate(name)?.return_pct == null ? "—" : `${accountReturn - atDate(name)!.return_pct! >= 0 ? "+" : ""}${format(accountReturn - atDate(name)!.return_pct!)} pp`;
  const rangeEnd = dates[dates.length - 1];
  return <section className="martin-panel">
    <h2>Account vs SPY and QQQ</h2>
    <p className="martin-panel-copy">Rebased to 100 on {comparison.baseline_date}. Account return uses persisted simulated NAV (cash plus realized and open P&amp;L); SPY and QQQ use stored Close price returns, excluding distributions. Max drawdown starts at this common baseline.</p>
    <p className="martin-panel-copy">Return, max drawdown, and account deltas are measured through the latest common session, {date}. The chart covers saved history through {rangeEnd}.</p>
    <div className="martin-table-wrap martin-benchmark-table"><table><thead><tr><th scope="col">Series</th><th scope="col">Return</th><th scope="col">Max drawdown</th><th scope="col">Account delta</th></tr></thead><tbody>{names.map((name) => { const point = atDate(name); return <tr key={name}><th scope="row">{name}</th><td>{point?.return_pct == null ? "—" : `${format(point.return_pct)}%`}</td><td>{format(maxDrawdown(name))}%</td><td>{name === "Account" ? "—" : delta(name)}</td></tr>; })}</tbody></table></div>
    <div className="martin-chart-block"><p className="martin-chart-title">Rebased value (common account and ETF sessions)</p><div className="martin-chart martin-comparison-chart" role="img" aria-label={`Account NAV and ETF price returns rebased to 100 from ${comparison.baseline_date} to ${rangeEnd}; missing sessions are shown as gaps`}>
      <div className="martin-chart-scale"><span>{format(high)}</span><span>{format(low)}</span></div>
      <svg viewBox="0 0 100 40" preserveAspectRatio="none">{names.map((name, index) => <path key={name} className={["account-series", "sector-series", "daily-top5-series"][index]} d={pathFor(name)} />)}</svg>
      <div><span>{comparison.baseline_date}</span>{names.map((name, index) => <span key={name} className={["account-legend", "sector-legend", "daily-top5-legend"][index]}>{name}</span>)}<span>{rangeEnd}</span></div>
    </div></div>
    <p className="martin-panel-copy">Coverage: {comparison.included_sessions} of {comparison.expected_sessions} sampled saved sessions share valid account, SPY, and QQQ values. Gaps can hide deeper drawdowns. Positive account delta means the account outperformed that ETF, in percentage points.</p>
    {comparison.missing_dates?.length ? <details className="martin-code-detail"><summary>Missing sessions ({comparison.missing_dates.length})</summary><p>{comparison.missing_dates.join(", ")}</p></details> : null}
  </section>;
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
  const series: Array<[string, MartinForwardBook["history"]]> = [["Account", books.account?.history ?? []], ["Sector opportunity", books.sector_opportunity?.history ?? []], ["Daily Top 5", books.daily_top5?.history ?? []]];
  if (books.rs_expanded?.history.length) series.push(["RS Expanded", books.rs_expanded.history]);
  const dates = Array.from(new Set(series.flatMap(([, points]) => points.map((point) => point.date)))).sort();
  const aligned = series.map(([name, points]) => {
    const byDate = new Map(points.map((point) => [point.date, Number(point[field])]));
    let previous: number | null = null;
    return { name, values: dates.map((date) => { previous = byDate.get(date) ?? previous; return previous; }) };
  });
  const values = aligned.flatMap((item) => item.values.filter((value): value is number => value != null));
  if (!values.length) return <p className="martin-empty">History will appear after positions close.</p>;
  const low = Math.min(...values), span = Math.max(...values) - low || 1;
  return <div className="martin-chart-block"><p className="martin-chart-title">Cumulative return in risk units (R)</p><div className="martin-chart martin-comparison-chart" role="img" aria-label={`Cumulative R comparison from ${dates[0]} to ${dates[dates.length - 1]}, values from ${format(Math.min(...values))} to ${format(Math.max(...values))} R`}><div className="martin-chart-scale"><span>{format(Math.max(...values))} R</span><span>{format(Math.min(...values))} R</span></div><svg viewBox="0 0 100 40" preserveAspectRatio="none">{aligned.map(({ name, values: points }, seriesIndex) => { const path = points.map((value, index) => value == null ? "" : `${index && points[index - 1] != null ? "L" : "M"}${(index / Math.max(points.length - 1, 1)) * 100},${35 - ((value - low) / span) * 30}`).filter(Boolean).join(" "); return <path key={name} className={["account-series", "sector-series", "daily-top5-series", "rs-expanded-series"][seriesIndex]} d={path} />; })}</svg><div><span>{dates[0]}–{dates[dates.length - 1]}</span>{aligned.map((item, index) => <span key={item.name} className={["account-legend", "sector-legend", "daily-top5-legend", "rs-expanded-legend"][index]}>{item.name}</span>)}</div></div></div>;
}

function SimpleTable({ rows, columns }: { rows: MartinForwardRow[]; columns: string[] }) {
  const [previewRow, setPreviewRow] = useState<string | null>(null);
  return <div className={`martin-table-wrap ${columns.includes("cumulative_r") ? "martin-comparison-table" : ""}`}><table><thead><tr>{columns.map((column) => <th key={column} scope="col">{columnLabel(column)}</th>)}</tr></thead><tbody>{rows.map((raw, index) => {
    const row = unpack(raw), key = String(raw.id ?? row.ticker ?? index), showPreview = previewRow === key;
    return <Fragment key={key}><tr className={columns.includes("reason_summary") ? "martin-exclusion-row" : undefined}>{columns.map((column) => <td key={column}>{column === "reason_summary" ? <>
      <span>{row.status === "avoid" && typeof row.readiness_explanation === "string" ? row.readiness_explanation : humanReasons(row.reasons, row)}</span>
      <ReadinessExplanation row={row} />
      <details className="martin-code-detail"><summary>Stored reason code</summary><code>{format(row.reasons)}</code></details>
      {row.status === "watch" ? <button type="button" aria-expanded={showPreview} onClick={() => setPreviewRow(showPreview ? null : key)}>{showPreview ? "Hide trade plan" : "View trade plan"}</button> : null}
    </> : column === "status" ? statusLabel(String(row[column] ?? raw[column] ?? "Unknown")) : format(row[column] ?? raw[column])}</td>)}</tr>{showPreview ? <tr className="martin-exclusion-preview-row"><td colSpan={columns.length}><TradePlanPreview row={row} /></td></tr> : null}</Fragment>;
  })}</tbody></table></div>;
}

function ReadinessExplanation({ row }: { row: MartinForwardRow }) {
  const awaiting = reasonCodes(row.reasons, row).includes("awaiting_confirmation");
  const avoided = row.status === "avoid" || row.trade_readiness === "avoid";
  if (!awaiting && !avoided) return null;
  const details = Array.isArray(row.readiness_details) ? row.readiness_details.filter((value): value is string => typeof value === "string" && Boolean(value.trim())) : [];
  return <details className="martin-code-detail"><summary>{awaiting ? "Confirmation rule" : "Why this was avoided"}</summary>
    {awaiting ? <p>{typeof row.confirmation_rules === "string" && row.confirmation_rules.trim() ? row.confirmation_rules : "Scanner confirmation details are unavailable in this saved snapshot."}</p> : details.length ? <ul>{details.map((detail) => <li key={detail}>{detail}</li>)}</ul> : <p>Saved explanation details are unavailable for this historical row.</p>}
  </details>;
}

function TradePlanPreview({ row }: { row: MartinForwardRow }) {
  const entry = positiveNumber(row.entry), stop = positiveNumber(row.stop), storedRiskValue = finiteNumber(row.reference_risk_pct);
  const storedRisk = storedRiskValue != null && storedRiskValue > 0 ? storedRiskValue : null;
  const canCalculateRisk = entry != null && stop != null && entry > stop;
  const risk = storedRisk ?? (canCalculateRisk ? ((entry - stop) / entry) * 100 : null);
  const overLimit = row.risk_status === "over_limit" || (risk != null && risk > 5);
  const fields: [string, unknown, "price" | "percent" | "text"][] = [
    ["Trigger", row.entry, "price"], ["Stop", row.stop, "price"], ["Max entry (5% risk)", row.max_entry, "price"],
    ["Reference risk at trigger", risk, "percent"], ["Plan state", row.plan_state, "text"],
    ["Source scanner", typeof row.source_strategy_id === "string" ? ({ ma_pullback_retest: "MA Pullback & Retest", ema21_pullback_buy: "EMA21 Pullback Buy", sma200_pullback_buy: "SMA200 Pullback Buy" } as Record<string, string>)[row.source_strategy_id] ?? row.source_strategy_id.replace(/_/g, " ") : row.source_strategy_id, "text"],
    ["Signal date", row.signal_date, "text"], ["Selection session", row.selection_date, "text"],
  ];
  return <section className="martin-trade-plan" aria-label="Trade plan preview">
    <h4>Trade plan preview</h4><p>Plan preview · not an active order</p>
    <dl>{fields.map(([label, value, kind]) => { const unavailable = value == null || (typeof value === "string" && !value.trim()) || (kind === "price" && positiveNumber(value) == null); return <div key={label}><dt>{label}</dt><dd>{unavailable ? "Unavailable" : kind === "price" ? formatPrice(Number(value)) : kind === "percent" ? `${formatRisk(Number(value))}%${storedRisk == null ? " · Calculated from saved levels" : ""}` : format(value)}</dd></div>; })}</dl>
    {overLimit ? <p className="martin-trade-plan-warning" role="note">Saved plan risk exceeds the 5% limit.</p> : null}
    <p className="martin-trade-plan-why"><strong>Why Watch:</strong> {humanReasons(row.reasons, row)}</p>
    <ReadinessExplanation row={row} />
    <RsEvidence row={row} />
  </section>;
}

function rsTierLabel(value: unknown): string { return ({ preferred: "Preferred · RS 90–99", emerging: "Emerging · RS 80–89", research: "Research · RS below 80", missing: "Missing RS · data issue", invalid: "Invalid RS · data issue" } as Record<string, string>)[String(value)] ?? String(value ?? "Unavailable"); }

function StopEvidence({ row }: { row: MartinForwardRow }) {
  if (row.stop_policy_version == null) return null;
  const evidence = object(row.stop_policy);
  const valueLabels: Record<string, string> = { daily_reclaim_low: "Daily reclaim candle low", saved_support_test_low: "Saved support-test candle low", within_half_adr: "Within half ADR", wide_stop_caution: "Wider than half ADR · caution", unavailable: "Unavailable" };
  const labels: Record<string, string> = { version: "Policy version", stop_basis: "Stop basis", source_strategy_id: "Source scanner", profile_id: "Selected support profile", signal_date: "Signal session", invalidation_date: "Invalidation candle date", invalidation_low: "Invalidation low (USD)", buffer: "Extra buffer (USD)", stop: "Structural stop (USD)", legacy_stop: "Legacy stop (USD)", reference_entry: "Reference entry (USD)", reference_risk_pct: "Reference stop distance (%)", stop_to_adr: "Reference stop / ADR", adr_fit: "ADR assessment", weekly_reference: "Weekly reference" };
  return <details className="martin-code-detail"><summary>Structural stop evidence</summary>{evidence.provisional === true ? <p>Provisional setup only: a completed daily confirmation is still required before an order.</p> : null}<p>Saved at selection and retained on fill. ADR assessment uses the reference entry; actual fill risk may differ. An unavailable test-candle date is not inferred from the signal date.</p><dl className="martin-details">{Object.entries(labels).filter(([key]) => key in evidence).map(([key, label]) => <div key={key}><dt>{label}</dt><dd>{valueLabels[String(evidence[key])] ?? format(key === "profile_id" ? evidence.support_label ?? String(evidence[key]).replace(/_/g, " ") : evidence[key])}</dd></div>)}</dl></details>;
}

function RsEvidence({ row }: { row: MartinForwardRow }) {
  if (row.rs_tier == null) return null;
  const checks = object(row.rs_exception_checks), metrics = object(row.rs_exception_metrics);
  const labels: Record<string, string> = { fresh_completed_session: "Fresh completed session", history_complete: "Complete price history", trend: "Rising SMA50/SMA200 trend", relative_strength_improving: "20-session strength vs SPY improving", liquidity: "Average dollar volume ≥ $20M", supported_plan: "Supported trade plan", confirmed: "Daily confirmation", pullback_quality: "Constructive or neutral pullback", valid_plan_levels: "Valid entry and stop", risk_within_5pct: "Entry-to-stop risk ≤ 5%", order_notional_within_liquidity: "Order ≤ 0.1% of average dollar volume" };
  labels.defined_risk_readiness = "Plan Ready with defined risk";
  const metricLabels: Record<string, string> = { as_of_date: "Evidence session", minimum_adv20_dollars: "Minimum average dollar volume (USD)", maximum_order_adv_fraction: "Maximum order / average dollar volume", close: "Close (USD)", sma50: "SMA50", sma200: "SMA200", sma50_5_sessions_ago: "SMA50 five sessions earlier", sma200_5_sessions_ago: "SMA200 five sessions earlier", ema21: "EMA21", stock_spy_ratio: "Stock / SPY ratio", stock_spy_ratio_20_sessions_ago: "Stock / SPY ratio 20 sessions earlier", adv20_dollars: "20-session average dollar volume (USD)", reference_risk_pct: "Reference risk (%)", actual_order_notional: "Actual order value (USD)" };
  return <section className="martin-rs-evidence" aria-label="Saved RS eligibility evidence"><h4>RS tier: {rsTierLabel(row.rs_tier)}</h4><p className="martin-panel-copy">Saved eligibility evidence, not a new signal or an order.</p><StopEvidence row={row} />{Object.keys(checks).length ? <dl className="martin-details">{Object.entries(checks).map(([key, passed]) => <div key={key}><dt>{labels[key] ?? key.replace(/_/g, " ")}</dt><dd>{passed === true ? "Passed" : passed === false ? "Not passed" : "Unavailable"}</dd></div>)}</dl> : <p className="martin-panel-copy">No additional RS 80–89 checks were saved for this row.</p>}{Object.keys(metrics).length ? <details className="martin-code-detail"><summary>Saved RS measurements</summary><dl className="martin-details">{Object.entries(metrics).map(([key, value]) => <div key={key}><dt>{metricLabels[key] ?? key.replace(/_/g, " ")}</dt><dd>{value == null ? "Unavailable" : typeof value !== "number" ? format(value) : key === "maximum_order_adv_fraction" ? `${format(value * 100)}%` : ["adv20_dollars", "minimum_adv20_dollars", "actual_order_notional", "close", "sma50", "sma200", "sma50_5_sessions_ago", "sma200_5_sessions_ago", "ema21"].includes(key) ? formatCurrency(value) : key === "reference_risk_pct" ? `${formatRisk(value)}%` : value.toLocaleString(undefined, { maximumFractionDigits: 6 })}</dd></div>)}</dl></details> : null}</section>;
}

function finiteNumber(value: unknown): number | null {
  if (typeof value !== "number" || !Number.isFinite(value)) return null;
  return value;
}
function positiveNumber(value: unknown): number | null {
  const number = finiteNumber(value);
  return number != null && number > 0 ? number : null;
}
function formatRisk(value: number): string { return value.toLocaleString(undefined, { maximumFractionDigits: 4 }); }

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
  if (status === "watch" || status === "avoid") return `No order: ${humanReasons(row.reasons, row)}`;
  return reasons || "Review snapshot levels";
}
function format(value: unknown): string { if (value == null) return "—"; if (typeof value === "number") return value.toLocaleString(undefined, { maximumFractionDigits: 2 }); if (typeof value === "object") return JSON.stringify(value); return String(value); }
function chartLabel(field: "cumulative_r" | "account_equity" | "equity_drawdown_pct"): string { return ({ cumulative_r: "Cumulative realized and open risk units (R)", account_equity: "Simulated account equity (USD)", equity_drawdown_pct: "Account equity drawdown (%)" } as const)[field]; }
function chartValue(field: "cumulative_r" | "account_equity" | "equity_drawdown_pct", value: number): string { return field === "account_equity" ? `$${format(value)}` : field === "equity_drawdown_pct" ? `${format(value)}%` : `${format(value)} R`; }
function columnLabel(value: string): string { return ({ source_run_id: "Source run", created_at: "Created", session_date: "Session date", event_date: "Event date", event_type: "Event", ticker: "Ticker", status: "Status", realized_r: "Realized R", realized_pnl: "Realized P&L (USD)", cumulative_r: "Cumulative R", unrealized_r: "Unrealized R", expectancy_r: "Expectancy R", profit_factor_r: "Profit factor", account_equity: "Account equity (USD)", equity_drawdown_pct: "Equity drawdown %", max_r_drawdown: "Max drawdown R", win_rate_pct: "Win rate %", stop_out_rate_pct: "Stop-out rate %", max_closed_drawdown_r: "Max closed-trade drawdown R", average_holding_sessions: "Average holding sessions", closed_count: "Closed trades", name: "Book / cohort", count: "Trades", reason_summary: "Reason summary" } as Record<string, string>)[value] ?? value.replace(/_/g, " "); }
function statusLabel(status: string): string { return ({ not_selected: "Not selected", blocked: "Excluded · failed strategy gates", blocked_data: "Blocked · missing data", blocked_capacity: "Blocked · capacity", waiting_capacity: "Waiting for capacity", blocked_open_position: "Blocked · existing position", blocked_existing_order: "Blocked · existing order", pending: "Pending", filled: "Filled", closed: "Closed", open: "Open", watch: "Watch", avoid: "Avoid", error: "Processing error", cancelled: "Cancelled", expired: "Expired" } as Record<string, string>)[status] ?? status.replace(/_/g, " "); }
function reasonCodes(value: unknown, row?: MartinForwardRow): string[] {
  const reasons = Array.isArray(value) ? value.map(String) : value == null ? [] : [String(value)];
  return [...new Set(reasons.flatMap((reason) => {
    if (reason !== "trade_readiness_not_defined_risk" || !row) return [reason];
    // Explain older saved candidates without changing their stored reason or eligibility.
    if (row.trade_readiness === "leader") return ["no_supported_plan"];
    if (!["pullback_zone", "reclaim_trigger"].includes(String(row.trade_readiness))) return [reason];
    const entry = Number(row.entry), stop = Number(row.stop);
    if (!Number.isFinite(entry) || !Number.isFinite(stop) || stop <= 0 || entry <= stop) return ["plan_levels_missing"];
    const result = row.trade_readiness === "pullback_zone" ? ["awaiting_confirmation"] : [];
    if (row.risk_status === "over_limit" || ((entry - stop) / entry) * 100 > 5) result.push("risk_exceeds_5pct");
    return result.length ? result : [reason];
  }))];
}
function hasMissingReason(value: unknown, row?: MartinForwardRow): boolean { return reasonCodes(value, row).some((reason) => ["missing_ticker", "missing_sector", "missing_adr_or_stop", "invalid_entry_stop_levels", "plan_levels_missing", "rs_rating_missing", "rs_rating_invalid", "rs_history_missing_or_stale", "rs_exception_data_missing", "rs_pullback_quality_missing"].includes(reason)); }
function humanReasons(value: unknown, row?: MartinForwardRow): string {
  const labels: Record<string, string> = { missing_ticker: "Ticker is missing", market_cap_below_1b_or_missing: "Market cap is below $1B or unavailable", close_below_5_or_missing: "Closing price is below $5 or unavailable", rs_outside_90_99_or_missing: "Relative strength is outside 90–99 or unavailable", no_martin_focus_scanner: "No Martin Focus scanner match", marked_avoid: "Marked avoid by trade readiness", trade_readiness_not_defined_risk: "Readiness not yet Plan Ready", awaiting_confirmation: "Awaiting confirmation", risk_exceeds_5pct: "Risk exceeds 5%", no_supported_plan: "No supported plan", plan_levels_missing: "Plan levels missing", missing_adr_or_stop: "ADR or stop level is missing", invalid_entry_stop_levels: "Entry and stop levels are invalid", missing_sector: "Sector data is missing", sector_quota_or_session_limit: "Outside sector quota or session limit", open_position_exists: "Position already open", pending_order_exists: "An order is already pending", position_or_allocation_capacity: "Position or allocation capacity reached", not_in_complete_snapshot: "Ticker was not in the complete snapshot", open_position_already_exists: "Position already open", same_day_stop: "Stop was reached on entry day" };
  Object.assign(labels, { rs_rating_missing: "RS rating is missing; data is required", rs_rating_invalid: "RS rating is invalid; check the data", rs_below_80_research: "RS below 80 · research only, no entry", rs_history_missing_or_stale: "RS qualification history is missing or stale", rs_exception_data_missing: "RS 80–89 qualification evidence is unavailable", rs_trend_not_qualified: "RS 80–89 trend requirements are not met", rs_relative_strength_not_improving: "20-session strength versus SPY is not improving", rs_liquidity_below_minimum: "Average daily dollar volume is below $20M", rs_supported_plan_required: "RS 80–89 requires a supported trade plan", rs_confirmation_required: "RS 80–89 is awaiting a fresh daily confirmation", rs_pullback_quality_required: "RS 80–89 requires constructive or neutral pullback quality", rs_order_notional_exceeds_liquidity: "Order exceeds 0.1% of average daily dollar volume" });
  Object.assign(labels, { rs_defined_risk_required: "RS 80–89 requires Plan Ready with defined risk", rs_pullback_quality_missing: "Pullback quality data is missing for RS 80–89" });
  Object.assign(labels, { structural_confirmation_stale: "Structural v1 needs a fresh source confirmation", structural_evidence_missing: "Structural stop evidence is missing or stale; no ATR fallback" });
  return reasonCodes(value, row).map((reason) => labels[reason] ?? reason.replace(/_/g, " ")).join("; ") || "No reason recorded";
}
