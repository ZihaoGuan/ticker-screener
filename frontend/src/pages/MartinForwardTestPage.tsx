import { Fragment, useEffect, useMemo, useState } from "react";
import { Link, Navigate, useSearchParams } from "react-router-dom";
import type { MartinForwardBook, MartinForwardResponse, MartinForwardRow } from "../lib/types";

type View = "today" | "positions" | "performance";
const views: View[] = ["today", "positions", "performance"];
const bookLabels = { account: "Account book", sector_opportunity: "Sector opportunity", daily_top5: "Daily Top 5", rs_expanded: "RS Expanded · experimental", rs_expanded_rotation: "RS Expanded · 8 + Rotation", gap_retest_replay_v4: "Daily Gap Retest · experimental" } as const;
const isExecutionVersion = (version?: string) => ["martin_bar_retest_execution_v1", "martin_bar_retest_execution_v2", "martin_bar_retest_execution_v3", "martin_bar_retest_execution_v4"].includes(version ?? "");
const isPivotVersion = (version?: string) => version === "rs_leader_30m_pivot_execution_v1";
const isExecutionBookKey = (key: string) => key.startsWith("martin_range_bar_retest_") || key.startsWith("rs_leader_30m_pivot_replay_");
const isKnownExecutionBook = (book?: MartinForwardBook) => isExecutionVersion(book?.execution_book_version) || isPivotVersion(book?.execution_book_version);

function MartinForwardPage({ executionOnly = false }: { executionOnly?: boolean }) {
  const [params, setParams] = useSearchParams();
  const view = views.includes(params.get("view") as View) ? params.get("view") as View : "today";
  const requestedBook = params.get("book");
  const bookKey = executionOnly ? (requestedBook && isExecutionBookKey(requestedBook) ? requestedBook : "bar_retest_execution") : requestedBook === "bar_retest_execution" ? requestedBook : requestedBook === "sector_opportunity" || requestedBook === "daily_top5" || requestedBook === "rs_expanded" || requestedBook === "rs_expanded_rotation" || requestedBook === "gap_retest_replay_v4" ? requestedBook
    : requestedBook === "gap_retest" || requestedBook === "gap_retest_replay" || requestedBook === "gap_retest_replay_v2" || requestedBook === "gap_retest_replay_v3" ? "gap_retest_replay_v4" : "account";
  const replayBook = bookKey === "gap_retest_replay_v4";
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

  const requestedExecutionBook = isExecutionBookKey(bookKey) ? data?.books?.[bookKey] : undefined;
  const discoveredExecutionBook = Object.keys(data?.books ?? {}).find((key) => key.startsWith("rs_leader_30m_pivot_replay_") && isPivotVersion(data?.books[key]?.execution_book_version))
    ?? Object.keys(data?.books ?? {}).find((key) => key.startsWith("martin_range_bar_retest_") && isExecutionVersion(data?.books[key]?.execution_book_version));
  const activeBookKey = executionOnly
    ? bookKey === "bar_retest_execution" ? discoveredExecutionBook ?? bookKey
      : isKnownExecutionBook(requestedExecutionBook) ? bookKey : bookKey
    : isKnownExecutionBook(requestedExecutionBook) ? bookKey
      : isExecutionBookKey(bookKey) ? "account" : bookKey;
  const book = data?.books?.[activeBookKey];
  const latest = book?.runs?.[0];
  const candidateRows = useMemo(() => (book?.candidates ?? []).map(unpack), [book]);
  const positionRows = useMemo(() => (book?.positions ?? []).map(unpack), [book]);
  const attention = candidateRows.filter((row) => ["blocked_data", "blocked_source", "blocked_capacity", "ambiguous", "error", "cancelled", "expired"].includes(String(row.status)) || hasMissingReason(row.reasons_json ?? row.reasons ?? object(row.payload_json).reasons, row));
  const summary = object(latest?.summary_json);
  const executionActions = object(summary.execution_actions ?? summary.action_counts);
  const pivotBook = activeBookKey.startsWith("rs_leader_30m_pivot_replay_");
  const executionBook = activeBookKey === "bar_retest_execution" || isExecutionBookKey(activeBookKey);
  const backdatedSimulation = executionBook && Boolean(book?.runs?.some((run) => object(run.summary_json).simulate_backdated_buys === true));
  const retrospectiveHistory = executionBook && Boolean(book?.runs?.some((run) => object(run.summary_json).retrospective_source_scan === true || object(run.summary_json).source_reconstructed_later === true || Number(object(run.summary_json).original_source_retrospective_count ?? 0) > 0));
  const replay = book?.runs?.map((run) => object(object(run.summary_json).replay_provenance)).find((item) => item.origin === "historical_replay");
  const executionLabel = (key: string) => key === "bar_retest_execution" ? "Bar Retest execution" : key.startsWith("rs_leader_30m_pivot_replay_") ? `RS Leader 30m Pivot · ${key.slice("rs_leader_30m_pivot_replay_".length)}` : key.startsWith("martin_range_bar_retest_") ? `Bar Retest execution · ${key.slice("martin_range_bar_retest_".length)}` : bookLabels[key as keyof typeof bookLabels] ?? key;
  const sessionDate = String(latest?.session_date ?? "");
  const stale = sessionDate ? (Date.now() - new Date(`${sessionDate}T00:00:00`).getTime()) > 3 * 86400000 : false;
  const setQuery = (key: string, value: string) => setParams((previous) => { const next = new URLSearchParams(previous); next.set(key, value); return next; });

  if (!executionOnly && (requestedBook === "bar_retest_execution" || requestedBook?.startsWith("martin_range_bar_retest_") || requestedBook?.startsWith("rs_leader_30m_pivot_replay_"))) return <Navigate to={`/backtests/bar-retest${window.location.search}`} replace />;
  if (executionOnly && data?.configured && isExecutionBookKey(bookKey) && !isKnownExecutionBook(requestedExecutionBook)) {
    return <main className="page-shell martin-page"><header className="martin-header"><div><h1>Paper Execution Books</h1><p>Select a saved execution book.</p></div></header><StatePanel title="Execution book not found" detail={`No supported execution book is available for ${bookKey}. Choose a saved book from the list.`} /><Link to="/backtests/martin-forward">Return to Martin forward tests</Link></main>;
  }

  return <main className="page-shell martin-page">
    <header className="martin-header">
      <div><h1>{executionOnly ? (pivotBook ? "RS Leader 30-Minute Pivot" : "Paper Execution Books") : "Martin Focus Forward Test"}</h1>
        <p>{pivotBook ? "Independent paper book from persisted Martin Bar Watchlist runs. No broker orders are placed." : "Paper-trade results from saved Martin Focus snapshots. No broker orders are placed."}</p></div>
      <span className="martin-simulation-badge">SIMULATED · NO BROKER ORDERS</span>
    </header>
    {error ? <StatePanel title="Database error" detail={error} /> : !data ? <StatePanel title="Loading forward test" detail="Loading saved sessions and positions…" /> : !data.configured ? <StatePanel title="Database unavailable" detail="Forward test persistence is not configured." /> : <>
      <div className="martin-toolbar">
        <div className="martin-book-switch" aria-label="Strategy book">
          {executionOnly ? null : <><Link className="button-link" to="/backtests/bar-retest">Paper execution books</Link>{Object.entries(bookLabels).map(([key, label]) => <button key={key} type="button" aria-pressed={activeBookKey === key} onClick={() => setQuery("book", key)}>{label}</button>)}</>}
          {executionOnly ? <>{Object.keys(data.books).filter((key) => isExecutionBookKey(key) && isKnownExecutionBook(data.books[key])).map((key) => <button key={key} type="button" aria-pressed={activeBookKey === key} onClick={() => setQuery("book", key)}>{executionLabel(key)}</button>)}{!Object.keys(data.books).some((key) => isExecutionBookKey(key) && isKnownExecutionBook(data.books[key])) ? <button type="button" aria-pressed={activeBookKey === "bar_retest_execution"} onClick={() => setQuery("book", "bar_retest_execution")}>Bar Retest execution</button> : null}</> : null}
        </div>
        <p className="martin-book-description">{pivotBook ? "Separate RS Leader replay: candidate discovery uses saved Martin Bar Watchlist runs, daily EMA21 or weekly EMA8 support (or a saved gap/base level), and inferred 3–12% pullback / within 2% support thresholds. Entry requires a later 30-minute break of the first green pivot after a red bar; stop is 0.1% under pivot low. Missing or invalid 30-minute evidence blocks entry. $100,000 base equity, $500 planned risk, $20,000 per-name cap, $2,500 committed-risk cap, eight positions, one-third trim at 3R, existing runner logic." : executionBook ? "Isolated Martin Bar Retest execution replay. Classification-only Martin Bar Retest results are a separate book and are not treated as fills. Sizing uses $100,000 base equity, $500 planned risk per trade, $20,000 per-name cap, $2,500 committed-risk cap and up to eight positions." : activeBookKey === "account" ? "Account book: fixed $100,000 sizing base, 0.5% risk per trade, 20% allocation cap, and up to five open or pending positions." : activeBookKey === "sector_opportunity" ? "Sector opportunity: up to ten sector-ranked candidates, simulated at 100 shares each without an account-capital limit." : activeBookKey === "rs_expanded" ? "Experimental account: RS 90–99 preferred, with RS 80–89 eligible only after additional trend, confirmation and liquidity checks. Baseline Account book and Top Hits defaults are unchanged." : activeBookKey === "rs_expanded_rotation" ? "Independent paper experiment: RS Expanded entries with up to eight open or reserved positions and automatic Rotation v1. Existing books and their history are unchanged; no broker orders are placed." : replayBook ? "Daily Gap Retest: replay-seeded paper book that continues after each completed session. Daily gap-event watches use fresh reclaim plans and next-session paper entries. Eight slots, fixed $100,000 sizing base, $500 planned risk per trade, no rotation, and the saved 10%-at-3R / EMA9 exit policy. Mixed-source price basis is provisional." : "Daily Top 5: up to five new 100-share orders each session, with no concurrent holding or capital limit."}</p>
        {executionBook && book?.execution_book_version ? <p className="martin-inline-state" role="status">Execution book version: {book.execution_book_version}{pivotBook ? " · separate isolated RS Leader book" : " · separate from classification-only martin_bar_retest_v1"}.</p> : null}
        {backdatedSimulation ? <section className="martin-backdated-banner" role="status" aria-label="Hypothetical backdated backtest"><strong>Hypothetical backdated backtest</strong><p>Some saved scans were reconstructed after they would have been available live. Simulated fills are hypothetical backdated results, not forward-paper fills.</p></section> : null}
        {retrospectiveHistory && !backdatedSimulation ? <section className="martin-backdated-banner" role="status" aria-label="Retrospective source provenance"><strong>Retrospective source history</strong><p>Some source scans were saved after their following-session open. Those origins remain marked as retrospective and cannot create new forward buys.</p></section> : null}
        {replay && !executionBook ? <p className="martin-inline-state" role="status">Seeded by historical replay · {String(replay.from_date)} to {String(replay.to_date)}. Those sessions reconstruct past signals; later sessions are scheduled paper evaluations. Stored historical prices may contain later corrections.</p> : null}
        {executionBook ? <details className="martin-code-detail"><summary>Execution replay provenance and limits</summary><p>{pivotBook ? "Persisted Martin Bar Watchlist run IDs, source fingerprints, daily-bar provenance, intraday candle manifest fingerprints, and per-session outcome counters are stored with this independent account book. Candidate discovery is on D and execution can start no earlier than D+1. Same-bar entry/stop ordering remains ambiguous and is conservatively not counted as a fill. The 3–12% pullback and 2% support distance are inferred thresholds." : "Saved Bar Watchlist classifications feed this independent paper execution book. Source dates, publication timing, scan run IDs, category cohorts, order fills, exits and replay actions are available in the stored runs and events below."} Orders are simulated and no broker orders are placed.</p>{!pivotBook ? <p>{book?.execution_book_version === "martin_bar_retest_execution_v4" ? "Entry and same-day stop evaluation uses frozen regular-session 15-minute bars. A complete BATS alternate feed inside the stored daily high/low envelope may decide limit touch and stop order even when opening prices conflict; the event records that feed conflict. Same-bar ambiguity is refined with frozen 1-minute bars when present; unresolved ambiguity is settled conservatively. Valid intraday expiry and stop-before-entry cancellation are authoritative, so daily bars cannot create a contradictory fill. V4 trims one-third of remaining whole shares at 3R using floor rounding; remaining shares follow the existing stop and EMA9 management." : book?.execution_book_version === "martin_bar_retest_execution_v3" ? "Entry and same-day stop evaluation uses frozen regular-session 15-minute bars. A complete BATS alternate feed inside the stored daily high/low envelope may decide limit touch and stop order even when opening prices conflict; the event records that feed conflict. Same-bar ambiguity is refined with frozen 1-minute bars when present; unresolved ambiguity is settled conservatively. V3 trims one-third of remaining whole shares at 3R using floor rounding; remaining shares follow the existing stop and EMA9 management." : book?.execution_book_version === "martin_bar_retest_execution_v2" ? "Entry and same-day stop evaluation uses frozen regular-session 15-minute bars. BATS alternate feed requires daily range and opening-fill agreement. Same-bar ambiguity is refined with frozen 1-minute bars when present; unresolved ambiguity is settled conservatively. V2 trims one-third of remaining whole shares at 3R using floor rounding; remaining shares follow the existing stop and EMA9 management." : "Daily bars cannot establish intraday stop ordering; stop wins when both occur in one bar."} Compare event category cohorts as exploratory results, not evidence of causal performance.</p> : null}</details> : null}
        {bookKey === "rs_expanded_rotation" ? <details className="martin-code-detail"><summary>Eight-slot and automatic rotation rules</summary><p>Fixed $100,000 sizing base. Up to eight holdings or reserved entry slots, including runners; maximum $500 planned risk and $20,000 initial allocation per name, with a $2,500 aggregate committed stop-risk budget. Available cash, liquidity and risk capacity can reduce order size. No leverage and no requirement to fill all slots.</p><p>Ordinary entries inherit RS Expanded eligibility, structural daily v1 stops and ADR cautions. Management still trims 10% at 3R and exits after a daily close below EMA9. RS 80–89 can enter with spare capacity; only RS 90–99 candidates can displace a holding.</p><p>Rotation requires constrained capacity, SPY and QQQ above rising EMA21s, and no rotation exit during the previous five exchange sessions. An untrimmed holding must have five full sessions after entry, less than +0.5R currently, never reached +1R, and trail SPY by at least two percentage points over five sessions.</p><p>A fresh confirmed replacement must pass the entry gates, close above EMA9 above a rising EMA21, outperform the outgoing holding by at least two percentage points over five sessions, and trade at least $20M average daily dollar volume. Earnings must be more than seven calendar days away; missing evidence blocks rotation. At most one pair is selected per evaluation.</p><p>The exit must fill before its paired entry can use the proceeds. A missed entry leaves cash; there is no chasing or automatic rebuy. Committed stop risk excludes unrealized profit giveback, which is reported separately; stops cannot guarantee the planned loss during gaps.</p><p>This book starts flat without copied positions or history. Capacity, risk controls and rotation all differ from the parent: performance differences cannot be attributed to rotation alone. Thresholds are experimental adaptations, not verified guru rules.</p></details> : null}
        <div className="martin-freshness"><span className={stale ? "martin-dot stale" : "martin-dot"} />
          {latest ? `${stale ? "Latest session may be stale" : "Latest session"} · ${sessionDate}` : "No session processed"}
          {latest && summary.source_run_id ? ` · source run ${String(summary.source_run_id)}` : ""}</div>
      </div>
      {!latest ? <StatePanel title={pivotBook ? "No RS Leader 30-Minute Pivot replay yet" : executionBook ? "No execution book yet" : "No session yet"} detail={pivotBook ? "Run the isolated replay with persisted Martin Bar Watchlist source runs and a frozen 30-minute bundle. It starts flat and does not import Bar Retest positions or classifications." : replayBook ? "Run Daily Gap Retest replay v4 through the previous market session before enabling its daily schedule. It starts flat and does not copy earlier books." : executionBook ? "Execution replays appear here after they are applied to an isolated book. The classification-only Martin Bar Retest results are separate and do not count as orders or fills." : activeBookKey === "rs_expanded_rotation" ? "No 8 + Rotation session has been saved yet. The scheduled Martin forward test job starts this independent book flat; no existing positions or historical sessions are copied." : activeBookKey === "rs_expanded" ? "No RS Expanded session has been saved yet. The next Martin forward test job can start this experiment; baseline trades are not copied into it." : "Run the existing Martin forward test job after a persisted snapshot is available."} /> : <>
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
        {summary.partial ? <section className="martin-inline-state" role="status">{Number(executionActions.intraday_fallbacks ?? 0) > 0 ? <><p>Daily conservative settlement was used for {format(executionActions.intraday_fallbacks)} order(s) because frozen intraday evidence could not be used. {formatIntradayFallbackReasons(object(executionActions.intraday_fallback_reasons ?? summary.intraday_fallback_reasons))}</p><IntradayFallbackDetails rows={Array.isArray(executionActions.intraday_fallback_details) ? executionActions.intraday_fallback_details as Record<string, unknown>[] : []} /></> : <p>Partial session: market bars were unavailable for {format(executionActions.missing_bars)} pending orders or open positions.</p>}</section> : null}
        {view === "today" ? <Today key={activeBookKey} book={book!} bookLabel={executionLabel(activeBookKey)} candidates={candidateRows} outstandingOrders={(book?.outstanding_orders ?? []).map(unpack)} attention={attention} executionBook={executionBook} /> : null}
        {view === "positions" ? <Positions positions={positionRows} filter={filter} setFilter={setFilter} expanded={expanded} setExpanded={setExpanded} /> : null}
        {view === "performance" ? <Performance book={book!} books={data.books} /> : null}
        <details className="martin-audit"><summary>Data &amp; audit</summary>
          <button type="button" onClick={async () => { try { await navigator.clipboard.writeText(JSON.stringify({ session: latest, candidates: book!.candidates, positions: book!.positions, events: book!.events, metrics: book!.metrics, history: book!.history, attribution: book!.attribution }, null, 2)); setCopyMessage("Session data copied."); } catch { setCopyMessage("Copy failed. Select the raw JSON below to copy it."); } }}>{copyMessage ? "Copy again" : "Copy session JSON"}</button>{copyMessage ? <span role="status"> {copyMessage}</span> : null}
          <p>Session {sessionDate} · snapshot fingerprint {String(summary.source_fingerprint ?? latest.source_fingerprint ?? "—")}</p>
          <h3>Stored runs</h3><SimpleTable rows={book!.runs} columns={executionBook ? ["session_date", "source_run_id", "created_at", "summary_json"] : ["session_date", "source_run_id", "created_at"]} />
          <h3>Recent events</h3><SimpleTable rows={book!.events} columns={["event_date", "ticker", "event_type", "payload_json"]} />
          <h3>Raw session data</h3><pre>{JSON.stringify({ candidates: book!.candidates, positions: book!.positions }, null, 2)}</pre>
        </details>
      </>}
    </>}
  </main>;
}

export default function MartinForwardTestPage() { return <MartinForwardPage />; }
export function BarRetestExecutionPage() { return <MartinForwardPage executionOnly />; }

function Today({ book, bookLabel, candidates, outstandingOrders, attention, executionBook = false }: { book: MartinForwardBook; bookLabel: string; candidates: MartinForwardRow[]; outstandingOrders: MartinForwardRow[]; attention: MartinForwardRow[]; executionBook?: boolean }) {
  type LifecycleGroup = "selected" | "watch" | "pending" | "waiting" | "blocked" | "open" | "closed" | "all";
  const gapBook = book.strategy_id.startsWith("martin_daily_gap_retest");
  const selected = candidates.filter(isSelected);
  const gapWatchRows = gapBook ? candidates.filter((row) => ["watch", "support_zone", "confirmed"].includes(gapState(row))) : [];
  const pendingRows = candidates.filter((row) => row.status === "pending");
  const waitingRows = candidates.filter((row) => ["waiting_capacity", "waiting_risk"].includes(String(row.status)) || (gapBook || executionBook) && ["waiting_risk", "waiting_capacity"].includes(gapState(row)));
  const blockedRows = gapBook || executionBook ? candidates.filter((row) => row.status === "blocked_data" || gapState(row) === "blocked_data") : [];
  const openRows = book.positions.filter((row) => row.status === "open");
  const closedRows = book.positions.filter((row) => row.status === "closed");
  const defaultGroup: LifecycleGroup = pendingRows.length ? "pending" : (executionBook ? candidates.some((row) => ["watch", "support_zone"].includes(gapState(row))) : gapWatchRows.length > 0) ? "watch" : selected.length ? "selected" : openRows.length ? "open" : "closed";
  const [activeGroup, setActiveGroup] = useState<LifecycleGroup>(defaultGroup);
  const [detailTicker, setDetailTicker] = useState<string | null>(null);
  const retestWatchRows = executionBook ? candidates.filter((row) => ["watch", "support_zone"].includes(gapState(row))) : gapWatchRows;
  const retestConfirmedRows = executionBook ? candidates.filter((row) => gapState(row) === "confirmed" || row.status === "pending") : pendingRows;
  const rows = activeGroup === "all" ? candidates : activeGroup === "selected" ? selected : activeGroup === "watch" ? retestWatchRows : activeGroup === "pending" ? retestConfirmedRows : activeGroup === "waiting" ? waitingRows : activeGroup === "blocked" ? blockedRows : [];
  const excludedRows = candidates.filter((row) => !isSelected(row) && row.status !== "pending" && !attention.includes(row));
  const groupLabel = activeGroup === "all" ? "All candidates" : activeGroup[0].toUpperCase() + activeGroup.slice(1);
  return <div className="martin-view">
    {book.strategy_id === "martin_rs_expanded_rotation_v1" ? <RotationOverview book={book} /> : null}
    <section className="martin-lifecycle" aria-label="Session lifecycle">
    {(executionBook ? [{ group: "watch" as const, label: "Watch / support zone", value: candidates.filter((row) => ["watch", "support_zone"].includes(gapState(row))).length }, { group: "pending" as const, label: "Confirmed / pending", value: pendingRows.length + candidates.filter((row) => gapState(row) === "confirmed" && row.status !== "pending").length }, { group: "waiting" as const, label: "Waiting risk / capacity", value: waitingRows.length }, { group: "blocked" as const, label: "Data blocked", value: blockedRows.length }, { group: "open" as const, label: "Open positions", value: openRows.length }, { group: "closed" as const, label: "Closed positions", value: closedRows.length }] : gapBook ? [{ group: "watch" as const, label: "Watch / support zone", value: gapWatchRows.length }, { group: "waiting" as const, label: "Risk / capacity wait", value: waitingRows.length }, { group: "blocked" as const, label: "Data blocked", value: blockedRows.length }, { group: "pending" as const, label: "Next-open orders", value: pendingRows.length }, { group: "open" as const, label: "Open positions", value: openRows.length }, { group: "closed" as const, label: "Closed positions", value: closedRows.length }] : [{ group: "selected" as const, label: "Selected · latest session", value: selected.length }, { group: "pending" as const, label: "Pending · latest session", value: pendingRows.length }, { group: "waiting" as const, label: "Waiting capacity", value: waitingRows.length }, { group: "open" as const, label: "Open · all sessions", value: openRows.length }, { group: "closed" as const, label: "Closed · all sessions", value: closedRows.length }]).map((item) => <button type="button" className="martin-lifecycle-card" key={item.group} aria-pressed={activeGroup === item.group} onClick={() => { setActiveGroup(item.group); setDetailTicker(null); }}><span>{item.label}</span><strong>{String(item.value)}</strong></button>)}
    </section>
    {outstandingOrders.some((row) => row.market_data_state === "awaiting_data") ? <section className="martin-panel"><h2>Earlier pending orders awaiting market data</h2><p>These orders reserve book capacity. Restore the missing data and replay the affected session to settle them; later sessions cannot fill these orders.</p><SimpleTable rows={outstandingOrders.filter((row) => row.market_data_state === "awaiting_data")} columns={["ticker", "session_date", "awaiting_session_date", "status", "market_data_reason", "limit_price", "stop"]} /></section> : null}
    <p className="martin-lifecycle-note">{executionBook || gapBook ? "Watch, risk/capacity, data, and pending-order queues refer to the latest session; positions include all saved sessions." : "Selected, Pending, and Waiting capacity refer to the latest session; position totals include all saved sessions."}</p>
    <section className="martin-panel"><div className="martin-panel-heading"><div><h2>{bookLabel} · {activeGroup === "open" || activeGroup === "closed" ? `${groupLabel} positions` : `${groupLabel} candidates`}</h2><p>{gapBook ? activeGroup === "pending" ? "Executable paper entries are valid for the next regular-session open only." : activeGroup === "all" ? "All saved watch and order decisions from the latest session." : "Saved daily watch, confirmation, risk, and execution decisions." : activeGroup === "selected" || activeGroup === "pending" ? "Candidates selected or awaiting a simulated limit fill in the latest session" : activeGroup === "all" ? "All candidates from the latest session" : "Position outcomes across all saved sessions"}</p></div>{activeGroup !== "all" ? <button type="button" onClick={() => { setActiveGroup("all"); setDetailTicker(null); }}>All candidates</button> : null}</div>
      {activeGroup === "open" || activeGroup === "closed" ? <TodayPositions rows={activeGroup === "open" ? openRows : closedRows} group={activeGroup} /> : rows.length ? <div className="martin-table-wrap martin-candidate-table"><table><thead><tr>{executionBook ? ["Ticker", "Category", "Status", "Entry limit", "Stop", "Shares", "Details"].map((label) => <th key={label} scope="col">{label}</th>) : (gapBook ? ["Ticker", "Gap state", "Reference entry", "Stop", "Max entry", "Next expected event", "Details"] : ["Ticker", "Status", "Entry limit", "Stop", "Size", "Next expected event", "Details"]).map((label) => <th key={label} scope="col">{label}</th>)}</tr></thead><tbody>{rows.map((row, index) => { const key = String(row.ticker ?? index); return <FragmentRow key={key} row={row} expanded={detailTicker === key} onToggle={() => setDetailTicker(detailTicker === key ? null : key)} gapBook={gapBook || executionBook} executionBook={executionBook} />; })}</tbody></table></div> : <p className="martin-empty">{activeGroup === "all" ? "No candidates in the latest snapshot." : activeGroup === "watch" ? "No watch or support-zone candidates." : activeGroup === "blocked" ? "No candidates are blocked on data." : activeGroup === "selected" ? "No candidates are selected for this session." : "No candidate orders are pending in this book."}</p>}
    </section>
    <details className="martin-exclusions"><summary>Excluded candidates and exceptions <span>{excludedRows.length + attention.length} candidates</span></summary>
      <p>Routine strategy exclusions are separate from missing data and order-processing problems.</p>
      <h3>Routine exclusions</h3>{excludedRows.length ? <SimpleTable rows={excludedRows} columns={["ticker", "status", "reason_summary"]} /> : <p className="martin-empty">No routine exclusions in this snapshot.</p>}
      <h3>Needs attention</h3>{attention.length ? <SimpleTable rows={attention} columns={["ticker", "status", "reason_summary"]} /> : <p className="martin-empty">No missing data or order-processing errors in this snapshot.</p>}
    </details>
  </div>;
}

function FragmentRow({ row, expanded, onToggle, gapBook = false, executionBook = false }: { row: MartinForwardRow; expanded: boolean; onToggle: () => void; gapBook?: boolean; executionBook?: boolean }) {
  const details = ["source_run_id", "source_session_date", "original_gap_date", "first_observed_date", "gap_age_sessions", "episode_start_date", "episode_last_date", "selected_ema_period", "selected_ema_value", "touch_tolerance", "confirmation_date", "confirmation_ohlcv", "trend_gate", "liquidity_gate", "price_gate", "history_gate", "ranking_inputs", "allocation_decision", "source_config_version", "decision_id", "selection_date", "order_date", "queue_rank", "source_snapshot_rank", "selection_reason", "entry_policy_version", "stop_policy_version", "management_policy_version", "source_strategy_id", "signal_date", "plan_state", "gap_state", "event_category", "category", "risk_status", "risk_dollars", "reserved_value", "reference_risk_pct", "execution_status", "market_data_state", "market_data_reason", "reference_entry", "initial_stop", "max_entry", "entry", "max_entry_price", "stop", "sector", "sector_quadrant", "selection_tier", "adr14_pct", "adr_suitability", "stop_to_adr", "strike_score", "scanner_count", "allocation_dollars", "selection_metadata", "reasons"];
  const nextEvent = executionBook ? row.status === "pending" ? "Next-session limit fill check" : gapState(row) === "confirmed" ? "Eligible for simulated order" : "Classification only; no order" : gapBook && row.status === "pending" ? "Execute once at next regular-session open only; expire after that session" : row.market_data_state === "awaiting_data" ? "Awaiting exact session bar; order remains reserved" : row.status === "pending" && row.rotation_intent ? "Await paired exit, then check the entry limit" : row.status === "pending" ? "Check next completed session for limit fill" : row.status === "filled" ? "Manage open position at stop" : candidateAction(row);
  return <>
    <tr><th scope="row">{String(row.ticker ?? "—")}</th>{executionBook ? <><td>{String(row.event_category ?? row.category ?? "—")}</td><td>{statusLabel(gapState(row) || String(row.status ?? "Unknown"))}</td><td>{formatPrice(row.limit_price ?? row.max_entry)}</td><td>{formatPrice(row.stop)}</td><td>{format(row.shares)}</td><td><button type="button" aria-expanded={expanded} onClick={onToggle}>{expanded ? "Hide details" : "Details"}</button></td></> : <><td>{gapBook ? statusLabel(gapState(row) || String(row.status ?? "Unknown")) : statusLabel(String(row.status ?? "Unknown"))}</td><td>{formatPrice(gapBook ? row.reference_entry ?? row.entry : row.limit_price)}</td><td>{formatPrice(gapBook ? row.initial_stop ?? row.stop : row.stop)}</td><td>{gapBook ? formatPrice(row.max_entry ?? row.max_entry_price) : format(row.shares ?? row.position_size)}</td><td>{nextEvent}</td><td><button type="button" aria-expanded={expanded} onClick={onToggle}>{expanded ? "Hide details" : "Details"}</button></td></>}</tr>
    {expanded ? <tr><td colSpan={7}><dl className="martin-details">{details.filter((field) => row[field] != null).map((field) => <div key={field}><dt>{field === "reasons" ? "Reason details" : field.replace(/_/g, " ")}</dt><dd>{field === "reasons" ? humanReasons(row[field], row) : format(row[field])}{field === "reasons" ? <small className="martin-raw-code">Stored value: {format(row[field])}</small> : null}</dd></div>)}</dl>{executionBook ? <><p>{nextEvent}</p><ReadinessExplanation row={row} /></> : gapBook ? <GapRetestPlanPreview row={row} /> : row.status === "watch" ? <TradePlanPreview row={row} /> : <><RsEvidence row={row} /><ReadinessExplanation row={row} /></>}</td></tr> : null}
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
      {expanded === key ? <dl className="martin-details">{["selection_date", "order_date", "entry_policy_version", "stop_policy_version", "management_policy_version", "entry_price", "initial_stop", "stop", "initial_r_per_share", "initial_dollar_risk", "initial_shares", "remaining_shares", "trim_history", "realized_pnl", "unrealized_pnl", "marked_pnl", "holding_sessions", "exit_price", "exit_next_open", "scheduled_exit_reason", "exit_type"].map((field) => <div key={field}><dt>{field.replace(/_/g, " ")}</dt><dd>{format(row[field])}</dd></div>)}</dl> : null}
    </article>; })}</div>}
    </section></div>;
}

function Performance({ book, books }: { book: MartinForwardBook; books: MartinForwardResponse["books"] }) {
  const metrics = book.metrics;
  const rsBook = book === books.rs_expanded || book === books.rs_expanded_rotation;
  const pivotBook = isPivotVersion(book.execution_book_version);
  const executionBook = isExecutionVersion(book.execution_book_version) || pivotBook;
  const accountLike = executionBook || book === books.account || rsBook || book === books.gap_retest || book === books.gap_retest_replay || book === books.gap_retest_replay_v2 || book === books.gap_retest_replay_v3 || book === books.gap_retest_replay_v4;
  const primary = [["Cumulative R", metrics.cumulative_r == null ? null : `${format(metrics.cumulative_r)} R`], ["Realized P&L", formatCurrency(metrics.realized_pnl)], ["Expectancy", metrics.expectancy_r == null ? null : `${format(metrics.expectancy_r)} R / trade`], ["Max drawdown", metrics.max_r_drawdown == null ? null : `${format(metrics.max_r_drawdown)} R`]];
  return <div className="martin-view">
    {Number(metrics.closed_count ?? 0) < 20 ? <p className="martin-inline-state" role="note">Early sample: fewer than 20 closed trades. Performance metrics are preliminary.</p> : null}
    {executionBook && book.history_source !== "stored_session_marks" ? <p className="martin-inline-state" role="status">{book.history_source === "stored_session_marks_partial" ? "Daily marks exist, but at least one open position lacked a same-date Close; portfolio equity and cumulative R are unavailable for that session." : "Daily portfolio marks are unavailable for this legacy book. Equity, unrealized P&amp;L, drawdown, and benchmark coverage are not reconstructed from current positions."}</p> : null}
    <section className="martin-metrics martin-metrics-primary">{primary.map(([label, value]) => <div className="martin-metric" key={String(label)}><span>{label}</span><strong>{format(value)}</strong></div>)}</section>
    {executionBook ? <section className="martin-panel"><h2>Execution risk controls</h2><p className="martin-panel-copy">Fixed $100,000 starting capital; $500 planned risk per order; $20,000 per-name allocation cap; $2,500 committed stop-risk cap; eight open or reserved positions maximum. Equity and unrealized P&amp;L use saved daily close marks; legacy books without marks remain unavailable.</p></section> : null}
    {executionBook && metrics.mark_coverage ? <p className="martin-inline-state" role="status">Daily mark coverage: {metrics.mark_coverage.marked_positions}/{metrics.mark_coverage.open_positions} open positions · {metrics.mark_coverage.status}{metrics.mark_coverage.missing_tickers?.length ? ` · missing ${metrics.mark_coverage.missing_tickers.join(", ")}` : ""}</p> : null}
    {executionBook || book === books.account || book === books.rs_expanded_rotation || book === books.gap_retest || book === books.gap_retest_replay || book === books.gap_retest_replay_v2 || book === books.gap_retest_replay_v3 || book === books.gap_retest_replay_v4 ? <BenchmarkComparison comparison={book.benchmark_comparison} label={pivotBook ? "RS Leader 30m Pivot" : executionBook ? "Bar Retest execution" : book === books.gap_retest || book === books.gap_retest_replay || book === books.gap_retest_replay_v2 || book === books.gap_retest_replay_v3 || book === books.gap_retest_replay_v4 ? "Daily Gap Retest" : "Account"} /> : null}
    {rsBook ? <section className="martin-panel"><h2>RS cohort outcomes</h2><p className="martin-panel-copy">Closed-trade results for preferred RS 90–99 and conditional emerging RS 80–89 entries. Drawdown is closed-trade R, not account NAV. Small cohorts are preliminary.</p><p className="martin-panel-copy">Cost model: {book.cost_model || "Unavailable in saved results."}</p>{book.rs_tier_metrics ? <><p className="martin-panel-copy">Scroll the table horizontally to inspect all cohort metrics.</p><SimpleTable rows={Object.entries(book.rs_tier_metrics).map(([tier, result]) => ({ ...result, name: rsTierLabel(tier), realized_pnl: formatCurrency(result.realized_pnl) }))} columns={["name", "closed_count", "realized_r", "realized_pnl", "win_rate_pct", "expectancy_r", "stop_out_rate_pct", "average_holding_sessions", "max_closed_drawdown_r"]} /></> : <p className="martin-empty">Saved RS cohort outcomes are unavailable. No baseline trades are substituted.</p>}</section> : null}
    <section className="martin-panel"><h2>Additional metrics</h2><div className="martin-metrics">{[["Realized R", metrics.realized_r], ["Unrealized R", metrics.unrealized_r], ["Realized P&L", formatCurrency(metrics.realized_pnl)], ["Closed trades", metrics.closed_count], ["Win rate", metrics.win_rate_pct == null ? null : `${format(metrics.win_rate_pct)}%`], ["Profit factor", metrics.profit_factor_r], ["Average holding sessions", metrics.average_holding_sessions], ["Gross wins", formatCurrency(metrics.gross_wins)], ["Gross losses", formatCurrency(metrics.gross_losses)], ["Account equity", accountLike ? formatCurrency(metrics.account_equity) : null], ["Equity drawdown", accountLike && metrics.equity_drawdown_pct != null ? `${format(metrics.equity_drawdown_pct)}%` : null]].map(([label, value]) => <div className="martin-metric" key={String(label)}><span>{label}</span><strong>{format(value)}</strong></div>)}</div></section>
    <section className="martin-panel"><h2>Book comparison</h2><p className="martin-panel-copy">Books can start on different dates; these are whole-book results, not matched-period outperformance. Compare the same sessions before drawing conclusions. Account books use fixed simulation capital and saved sessions.</p><SimpleTable rows={(Object.keys(bookLabels) as Array<keyof typeof bookLabels>).flatMap((key) => books[key] ? [{ name: bookLabels[key], ...books[key]!.metrics, cumulative_r: books[key]!.metrics.cumulative_r == null ? null : `${format(books[key]!.metrics.cumulative_r)} R`, expectancy_r: books[key]!.metrics.expectancy_r == null ? null : `${format(books[key]!.metrics.expectancy_r)} R`, realized_pnl: formatCurrency(books[key]!.metrics.realized_pnl), account_equity: key === "account" || key === "rs_expanded" || key === "rs_expanded_rotation" || key === "gap_retest_replay_v4" ? formatCurrency(books[key]!.metrics.account_equity) : null, equity_drawdown_pct: (key === "account" || key === "rs_expanded" || key === "rs_expanded_rotation" || key === "gap_retest_replay_v4") && books[key]!.metrics.equity_drawdown_pct != null ? `${format(books[key]!.metrics.equity_drawdown_pct)}%` : null, max_r_drawdown: books[key]!.metrics.max_r_drawdown == null ? null : `${format(books[key]!.metrics.max_r_drawdown)} R` }] : [])} columns={["name", "cumulative_r", "realized_r", "unrealized_r", "expectancy_r", "realized_pnl", "win_rate_pct", "profit_factor_r", "account_equity", "equity_drawdown_pct", "max_r_drawdown"]} /></section>
    <section className="martin-panel"><h2>Cumulative R comparison</h2><ComparisonChart books={books} field="cumulative_r" /></section>
    {accountLike ? <section className="martin-panel"><h2>{pivotBook ? "RS Leader 30m Pivot equity and drawdown" : executionBook ? "Bar Retest execution equity and drawdown" : book === books.rs_expanded_rotation ? "8 + Rotation equity and drawdown" : book === books.rs_expanded ? "RS Expanded equity and drawdown" : book === books.gap_retest || book === books.gap_retest_replay || book === books.gap_retest_replay_v2 || book === books.gap_retest_replay_v3 || book === books.gap_retest_replay_v4 ? "Daily Gap Retest equity and drawdown" : "Account book equity and drawdown"}</h2><p className="martin-panel-copy">Account sizing metrics use this book’s fixed simulation capital and saved sessions.</p><HistoryChart points={book.history} field="account_equity" /><HistoryChart points={book.history} field="equity_drawdown_pct" /></section> : books.account ? <section className="martin-panel"><h2>Account book equity and drawdown</h2><p className="martin-panel-copy">Account sizing metrics use this book’s fixed simulation capital.</p><HistoryChart points={books.account.history} field="account_equity" /><HistoryChart points={books.account.history} field="equity_drawdown_pct" /></section> : null}
    <section className="martin-panel"><h2>Trade attribution</h2>{Object.entries(book.attribution).map(([dimension, values]) => <div key={dimension}><h3>{dimension.replace(/_/g, " ")}</h3><SimpleTable rows={Object.entries(values).map(([name, result]) => ({ name, ...result }))} columns={["name", "count", "realized_r", "realized_pnl"]} /></div>)}</section>
  </div>;
}

function RotationOverview({ book }: { book: MartinForwardBook }) {
  const summary = object(book.runs[0]?.summary_json);
  const rotation = object(summary.rotation);
  const outgoing = object(rotation.outgoing).ticker ?? rotation.outgoing_ticker;
  const incoming = object(rotation.incoming).ticker ?? rotation.incoming_ticker;
  const events = book.events.filter((event) => String(event.event_type ?? "").startsWith("rotation_") || event.event_type === "pending_replaced");
  const rows = events.map((event) => {
    const payload = object(event.payload_json), intent = object(payload.rotation_intent);
    const decision = Object.keys(intent).length ? intent : payload;
    return { event_date: event.event_date, outgoing_ticker: object(decision.outgoing).ticker ?? decision.outgoing_ticker ?? (event.event_type === "pending_replaced" ? event.ticker : undefined),
      incoming_ticker: object(decision.incoming).ticker ?? decision.incoming_ticker,
      rotation_event: rotationStatus(String(event.event_type)), rotation_reason: humanReasons(payload.reasons ?? payload.reason ?? decision.reason), decision_id: decision.decision_id };
  });
  return <section className="martin-panel" aria-label="Automatic rotation status">
    <h2>Automatic Rotation v1</h2>
    <p className="martin-panel-copy">Completed daily evidence selects at most one pair. A replacement cannot use the outgoing position’s capital until its exit fills.</p>
    <dl className="martin-details">
      <div><dt>Latest decision</dt><dd>{rotation.status == null ? "No saved decision" : rotationStatus(String(rotation.status))}</dd></div>
      <div><dt>Cooldown remaining</dt><dd>{rotation.cooldown_sessions_remaining == null ? "Unavailable" : `${format(rotation.cooldown_sessions_remaining)} exchange sessions`}</dd></div>
      <div><dt>Committed stop risk / $2,500 budget</dt><dd>{formatCurrency(summary.committed_stop_risk)}</dd></div>
      <div><dt>Current-price-to-stop exposure</dt><dd>{formatCurrency(summary.mark_to_stop_exposure)}</dd></div>
    </dl>
    {outgoing && incoming ? <p><strong>{rotation.status === "pending_replaced" ? "Reservation swap:" : "Planned pair:"}</strong> {rotation.status === "pending_replaced" ? `replace unfilled ${String(outgoing)} with ${String(incoming)}.` : `exit ${String(outgoing)} before funding ${String(incoming)}.`}</p> : null}
    {rotation.reasons != null || rotation.reason != null ? <p>{humanReasons(rotation.reasons ?? rotation.reason)}</p> : null}
    <p className="martin-panel-copy">Committed risk counts remaining entry-to-hard-stop risk and pending orders. Current-price-to-stop exposure includes unrealized profit giveback; neither is a guaranteed loss limit during gaps.</p>
    <h3>Recent rotation decisions</h3>
    {rows.length ? <><p className="martin-panel-copy">Scroll the table horizontally to inspect the paired tickers and reasons.</p><SimpleTable rows={rows} columns={["event_date", "outgoing_ticker", "incoming_ticker", "rotation_event", "rotation_reason"]} /></> : <p className="martin-empty">No rotation events have been saved. The book can still submit ordinary entries when capacity is available.</p>}
  </section>;
}

function BenchmarkComparison({ comparison, label = "Account" }: { comparison: MartinForwardBook["benchmark_comparison"]; label?: string }) {
  if (!comparison || comparison.status !== "available" || !comparison.series) {
    return <section className="martin-panel"><h2>{label} vs SPY and QQQ</h2><p className="martin-empty">{comparison?.reason || "Stored account and benchmark history is unavailable."}</p></section>;
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
    <h2>{label} vs SPY and QQQ</h2>
    <p className="martin-panel-copy">Rebased to 100 on {comparison.baseline_date}. {label} return uses persisted simulated NAV (cash plus realized and open P&amp;L); SPY and QQQ use stored Close price returns, excluding distributions. Max drawdown starts at this common baseline.</p>
    <p className="martin-panel-copy">Return, max drawdown, and account deltas are measured through the latest common session, {date}. The chart covers saved history through {rangeEnd}.</p>
    <div className="martin-table-wrap martin-benchmark-table"><table><thead><tr><th scope="col">Series</th><th scope="col">Return</th><th scope="col">Max drawdown</th><th scope="col">{label} delta</th></tr></thead><tbody>{names.map((name) => { const point = atDate(name); return <tr key={name}><th scope="row">{name === "Account" ? label : name}</th><td>{point?.return_pct == null ? "—" : `${format(point.return_pct)}%`}</td><td>{format(maxDrawdown(name))}%</td><td>{name === "Account" ? "—" : delta(name)}</td></tr>; })}</tbody></table></div>
    <div className="martin-chart-block"><p className="martin-chart-title">Rebased value (common account and ETF sessions)</p><div className="martin-chart martin-comparison-chart" role="img" aria-label={`Account NAV and ETF price returns rebased to 100 from ${comparison.baseline_date} to ${rangeEnd}; missing sessions are shown as gaps${comparison.provisional_dates?.length ? `; orange circles mark provisional daily-fallback account sessions: ${comparison.provisional_dates.join(", ")}` : ""}`}>
      <div className="martin-chart-scale"><span>{format(high)}</span><span>{format(low)}</span></div>
      <svg viewBox="0 0 100 40" preserveAspectRatio="none">{names.map((name, index) => <path key={name} className={["account-series", "sector-series", "daily-top5-series"][index]} d={pathFor(name)} />)}{comparison.provisional_dates?.map((day) => { const index = dates.indexOf(day); const value = comparison.series!.Account.find((item) => item.date === day)?.value; return value == null ? null : <circle key={day} className="provisional-account-point" cx={(index / Math.max(dates.length - 1, 1)) * 100} cy={35 - ((value - low) / span) * 30} r="1.2"><title>{day}: provisional daily-fallback account mark</title></circle>; })}</svg>
      <div><span>{comparison.baseline_date}</span>{names.map((name, index) => <span key={name} className={["account-legend", "sector-legend", "daily-top5-legend"][index]}>{name}</span>)}<span>{rangeEnd}</span></div>
    </div></div>
    <p className="martin-panel-copy">Coverage: {comparison.included_sessions} of {comparison.expected_sessions} sampled saved sessions share valid account, SPY, and QQQ values. Gaps can hide deeper drawdowns. Positive account delta means the account outperformed that ETF, in percentage points.</p>
    {comparison.provisional_dates?.length ? <p className="martin-panel-copy">Provisional daily-fallback dates included in the main comparison (orange circles on Account line): {comparison.provisional_dates.length} ({comparison.provisional_dates.join(", ")}). Strict samples omit those dates{comparison.strict_comparison?.baseline_date ? `, rebase on ${comparison.strict_comparison.baseline_date}, and end ${comparison.strict_comparison.comparison_date || "—"}: ` : ": "}{(["Account", "SPY", "QQQ"] as const).map((name) => { const strict = comparison.strict_comparison; const point = strict?.series?.[name]?.find((item) => item.date === strict.comparison_date); return `${name} ${point?.return_pct == null ? "—" : `${format(point.return_pct)}%`}`; }).join(" · ")}. Omitting dates can understate drawdown.</p> : null}
    {comparison.missing_dates?.length ? <details className="martin-code-detail"><summary>Dates missing valid account or ETF evidence ({comparison.missing_dates.length})</summary><p>{comparison.missing_dates.join(", ")}</p></details> : null}
  </section>;
}

function HistoryChart({ points, field }: { points: MartinForwardBook["history"]; field: "cumulative_r" | "account_equity" | "equity_drawdown_pct" }) {
  const usable = points.filter((point) => point[field] != null);
  if (!usable.length) return <p className="martin-empty">No complete daily portfolio marks are available.</p>;
  const values = usable.map((point) => Number(point[field]));
  const low = Math.min(...values), high = Math.max(...values), span = high - low || 1;
  const path = values.map((value, index) => `${index ? "L" : "M"}${(index / Math.max(values.length - 1, 1)) * 100},${35 - ((value - low) / span) * 30}`).join(" ");
  const lastPoint = usable[usable.length - 1];
  return <div className="martin-chart-block"><p className="martin-chart-title">{chartLabel(field)}</p><div className="martin-chart" role="img" aria-label={`${chartLabel(field)} from ${usable[0].date} ${chartValue(field, values[0])} to ${lastPoint.date} ${chartValue(field, values[values.length - 1])}`}><div className="martin-chart-scale"><span>{chartValue(field, high)}</span><span>{chartValue(field, low)}</span></div><svg viewBox="0 0 100 40" preserveAspectRatio="none"><path d={path} /></svg><div><span>{usable[0].date}</span><span>{lastPoint.date}</span></div></div></div>;
}

function ComparisonChart({ books, field }: { books: MartinForwardResponse["books"]; field: "cumulative_r" }) {
  const series: Array<[string, MartinForwardBook["history"]]> = [["Account", books.account?.history ?? []], ["Sector opportunity", books.sector_opportunity?.history ?? []], ["Daily Top 5", books.daily_top5?.history ?? []]];
  if (books.rs_expanded?.history.length) series.push(["RS Expanded", books.rs_expanded.history]);
  if (books.rs_expanded_rotation?.history.length) series.push(["8 + Rotation", books.rs_expanded_rotation.history]);
  if (books.gap_retest_replay_v4?.history.length) series.push(["Daily Gap Retest · replay", books.gap_retest_replay_v4.history]);
  for (const [key, book] of Object.entries(books)) {
    if (key.startsWith("martin_range_bar_retest_") && book?.history.length) series.push([`Bar Retest · ${key.slice("martin_range_bar_retest_".length)}`, book.history]);
    if (key.startsWith("rs_leader_30m_pivot_replay_") && book?.history.length) series.push([`RS Leader 30m Pivot · ${key.slice("rs_leader_30m_pivot_replay_".length)}`, book.history]);
  }
  const dates = Array.from(new Set(series.flatMap(([, points]) => points.map((point) => point.date)))).sort();
  const aligned = series.map(([name, points]) => {
    const byDate = new Map(points.map((point) => [point.date, point[field] == null ? null : Number(point[field])]));
    let previous: number | null = null;
    return { name, values: dates.map((date) => { if (byDate.has(date)) previous = byDate.get(date) ?? null; return previous; }) };
  });
  const values = aligned.flatMap((item) => item.values.filter((value): value is number => value != null));
  if (!values.length) return <p className="martin-empty">No complete daily R marks are available.</p>;
  const low = Math.min(...values), span = Math.max(...values) - low || 1;
  const seriesClass: Record<string, string> = { Account: "account", "Sector opportunity": "sector", "Daily Top 5": "daily-top5", "RS Expanded": "rs-expanded", "8 + Rotation": "rs-rotation", "Daily Gap Retest · replay": "gap-retest" };
  const classFor = (name: string) => seriesClass[name] ?? "account";
  return <div className="martin-chart-block"><p className="martin-chart-title">Cumulative return in risk units (R)</p><div className="martin-chart martin-comparison-chart" role="img" aria-label={`Cumulative R comparison from ${dates[0]} to ${dates[dates.length - 1]}, values from ${format(Math.min(...values))} to ${format(Math.max(...values))} R`}><div className="martin-chart-scale"><span>{format(Math.max(...values))} R</span><span>{format(Math.min(...values))} R</span></div><svg viewBox="0 0 100 40" preserveAspectRatio="none">{aligned.map(({ name, values: points }) => { const path = points.map((value, index) => value == null ? "" : `${index && points[index - 1] != null ? "L" : "M"}${(index / Math.max(points.length - 1, 1)) * 100},${35 - ((value - low) / span) * 30}`).filter(Boolean).join(" "); return <path key={name} className={`${classFor(name)}-series`} d={path} />; })}</svg><div><span>{dates[0]}–{dates[dates.length - 1]}</span>{aligned.map((item) => <span key={item.name} className={`${classFor(item.name)}-legend`}>{item.name}</span>)}</div></div></div>;
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

function GapRetestPlanPreview({ row }: { row: MartinForwardRow }) {
  const payload = object(row.payload_json), evidence = object(row.evidence), plan = object(row.plan);
  const read = (key: string) => row[key] ?? plan[key] ?? evidence[key] ?? payload[key];
  const fields: [string, unknown][] = [
    ["Gap date / original event", read("original_gap_date") ?? read("gap_date")], ["Gap age (XNYS sessions)", read("gap_age_sessions") ?? read("gap_age")],
    ["First observed", read("first_observed_date")], ["Source run / session", read("source_run_id") ?? read("source_session_date")],
    ["Pullback episode start / last", read("episode_start_date") ?? read("pullback_episode_start"),], ["Selected EMA / value", read("selected_ema_period") ?? read("selected_ema")],
    ["Confirmation date", read("confirmation_date") ?? read("decision_date")], ["Reference entry", read("reference_entry") ?? read("entry")],
    ["Initial reclaim-candle stop", read("initial_stop") ?? read("stop")], ["Maximum entry", read("max_entry") ?? read("max_entry_price")],
    ["Reference risk %", read("reference_risk_pct")], ["ADR14 % / stop-to-ADR", read("adr14_pct")],
    ["Ranking inputs", read("ranking_inputs")], ["Allocation decision", read("allocation_decision")],
    ["Source / policy versions", read("source_config_version") ?? read("config_version")], ["Decision ID", read("decision_id")],
  ];
  const gateFields = ["trend_gate", "liquidity_gate", "price_gate", "history_gate", "confirmation_test", "touch_gate", "adjustment_basis"];
  return <section className="martin-trade-plan" aria-label="Daily Gap Retest saved decision"><h4>Daily Gap Retest · saved daily decision</h4><p>Next-session open entries only; no standing limit order. The preview is the saved evaluator decision for this session.</p>
    <dl>{fields.filter(([, value]) => value != null).map(([label, value]) => <div key={label}><dt>{label}</dt><dd>{format(value)}</dd></div>)}</dl>
    <h4>Gate results</h4><dl>{gateFields.filter((key) => read(key) != null).map((key) => <div key={key}><dt>{key.replace(/_/g, " ")}</dt><dd>{format(read(key))}</dd></div>)}</dl>
    <p className="martin-trade-plan-why"><strong>Saved reasons:</strong> {humanReasons(row.reasons ?? row.reason, row)}</p>
    <p>Management: retain the original hard stop; trim 10% once at 3R; exit the remainder at the next open after a completed close below EMA9.</p>
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
function formatIntradayFallbackReasons(reasons: Record<string, unknown>): string {
  const labels: Record<string, string> = { symbol_lookup_failed: "symbol lookup failure", SYMBOL_ERROR: "symbol lookup failure",
    feed_mismatch: "feed conflict", feed_conflict: "feed conflict", intraday_feed_mismatch: "feed conflict",
    missing_bars: "incomplete session", incomplete_session: "incomplete session",
    intraday_coverage_incomplete: "incomplete session", provider_unavailable: "provider unavailable",
    provider_fetch_failed: "provider unavailable", frozen_intraday_bundle_missing: "missing frozen bundle" };
  const items = Object.entries(reasons).filter(([, count]) => Number(count) > 0)
    .map(([reason, count]) => `${labels[reason] ?? reason.replace(/_/g, " ")}: ${format(count)}`);
  return items.length ? `Reasons: ${items.join(", ")}.` : "Stored per-order reason counts are unavailable.";
}
function IntradayFallbackDetails({ rows }: { rows: Record<string, unknown>[] }) {
  if (!rows.length) return <p>Per-order fallback detail is unavailable for this saved session.</p>;
  const groups: Record<string, Record<string, unknown>[]> = {};
  for (const row of rows) {
    const reason = String(row.evidence_status ?? "unknown").toLowerCase();
    const label = /feed|mismatch|conflict/.test(reason) ? "Feed conflict"
      : /incomplete|coverage|missing_bar/.test(reason) ? "Incomplete session"
      : /symbol|lookup|unresolved/.test(reason) ? "Symbol unresolved"
      : /provider|fetch|unavailable/.test(reason) ? "Provider failure"
      : /bundle/.test(reason) ? "Missing bundle" : "Other";
    (groups[label] ??= []).push(row);
  }
  return <details><summary>Intraday fallback details ({rows.length})</summary>{Object.entries(groups).map(([label, items]) => <section key={label}><h3>{label} ({items.length})</h3><div className="martin-table-wrap"><table><thead><tr>{["Ticker", "Date", "Requested symbol", "Resolved symbol", "Evidence status", "Reason"].map((name) => <th key={name} scope="col">{name}</th>)}</tr></thead><tbody>{items.map((row, index) => <tr key={`${String(row.ticker)}-${String(row.date)}-${index}`}><td>{format(row.ticker)}</td><td>{format(row.date)}</td><td>{format(row.requested_symbol)}</td><td>{format(row.resolved_symbol)}</td><td>{format(row.evidence_status)}</td><td>{format(row.reason)}</td></tr>)}</tbody></table></div></section>)}</details>;
}
function unpack(row: MartinForwardRow): MartinForwardRow { return { ...object(row.payload_json), ...row, ...object(row.payload_json), reasons: row.reasons_json ?? row.reasons ?? object(row.payload_json).reasons, status: row.status ?? object(row.payload_json).status, rank: row.rank ?? object(row.payload_json).rank }; }
function gapState(row: MartinForwardRow): string { return String(row.gap_state ?? object(row.payload_json).gap_state ?? row.status ?? ""); }
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
function statusLabel(status: string): string { return ({ not_selected: "Not selected", blocked: "Excluded · failed strategy gates", blocked_data: "Blocked · missing data", blocked_capacity: "Blocked · capacity", waiting_capacity: "Waiting for capacity", waiting_risk: "Waiting · risk plan", blocked_open_position: "Blocked · existing position", blocked_existing_order: "Blocked · existing order", pending: "Pending · next open", filled: "Filled", closed: "Closed", open: "Open", watch: "Watch", support_zone: "Support zone", confirmed: "Confirmed today", retired: "Retired", avoid: "Avoid", error: "Processing error", cancelled: "Cancelled", expired: "Expired" } as Record<string, string>)[status] ?? status.replace(/_/g, " "); }
function rotationStatus(status: string): string { return ({ scheduled: "Exit and contingent entry scheduled", not_scheduled: "No holding replacement scheduled", pending_replaced: "Unfilled reservation replaced", rotation_scheduled: "Pair scheduled", rotation_exit: "Outgoing position exited", rotation_entry_expired: "Replacement expired · cash retained", rotation_cancelled: "Rotation cancelled" } as Record<string, string>)[status] ?? statusLabel(status); }
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
  Object.assign(labels, { awaiting_first_retest: "Awaiting the first supported pullback", awaiting_daily_reclaim: "Awaiting a bullish daily reclaim", trend_not_qualified: "Trend gate is not qualified", liquidity_below_minimum: "Average dollar volume is below $20M", price_below_minimum: "Closing price is below $5", risk_exceeds_5pct: "Reference risk exceeds 5%", capacity_full: "All eight position slots are committed", cash_or_risk_budget: "Cash or aggregate stop-risk budget is committed", gap_event_expired: "Gap event is older than 60 sessions", gap_support_close_failed: "A completed close fell below the gap-day low", first_episode_expired: "The first pullback episode expired", first_episode_already_expired: "The first pullback episode had already expired when observed", event_already_traded: "This gap event already produced a filled trade", open_position_exists: "A position for this ticker is already open", source_missing_or_stale: "The Power Gap source is missing or stale", history_missing_sessions: "Required daily history is missing sessions", invalid_ohlcv: "Daily OHLCV is invalid", adjustment_basis_unknown: "Price adjustment basis is unknown", missing_adr: "ADR14 is unavailable", opened_at_or_below_stop: "Next open was at or below the stop", opened_above_max_entry: "Next open exceeded maximum entry after slippage", signal_not_fresh: "The confirmation was not fresh for this session" });
  Object.assign(labels, { rotation_cooldown: "Rotation cooldown is active", rotation_benchmark_data_missing: "SPY or QQQ evidence is missing; no rotation", rotation_market_regime: "SPY and QQQ do not both meet the rising EMA21 condition", rotation_no_stalled_holding: "No holding meets every stalled-position condition", rotation_no_qualified_replacement: "No capacity-blocked newcomer meets every replacement condition", rotation_projected_capacity_unavailable: "The projected exit would not free enough capital or risk capacity", stalled_relative_laggard_replaced_by_confirmed_leader: "A stalled relative laggard is paired with a confirmed leader", higher_priority_unfilled_order: "A higher-priority candidate replaced an unfilled reservation; no holding was sold", rotation_waiting_exit: "Replacement must wait for its paired position to exit", position_cash_or_risk_capacity: "Holding, cash or committed stop-risk capacity reached" });
  return reasonCodes(value, row).map((reason) => labels[reason] ?? reason.replace(/_/g, " ")).join("; ") || "No reason recorded";
}
