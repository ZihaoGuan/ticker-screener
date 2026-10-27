import { useEffect, useMemo, useState } from "react";
import { Link, useSearchParams } from "react-router-dom";
import { LoadingBlock } from "../components/LoadingBlock";
import { fetchJson } from "../lib/api";
import { formatLocalDate, formatLocalDateTime } from "../lib/format";

type SavedRun = { id: number; strategy_id: string; run_date: string; hit_count: number; created_at: string };
type Horizon = { status: string; return_pct?: number; downside_pct?: number | null; end_date?: string; source?: string };
type FollowRow = { ticker: string; rank: number | null; reasons: string[]; horizons: Record<string, Horizon>; benchmarks: Record<string, Record<string, number | null>>; retrospective_publication: boolean; publication_status: string };
type FollowThrough = { run: SavedRun; rows: FollowRow[]; offset: number; limit: number; has_more: boolean; baseline: string; performance_note: string };
const PAGE_SIZE = 100;

function pct(value: number | null | undefined) {
  return value == null || !Number.isFinite(value) ? "—" : `${value > 0 ? "+" : ""}${value.toFixed(1)}%`;
}
function statusText(status?: string) {
  return ({ available: "Available", pending_horizon: "Pending", missing_bars: "Missing bars", adjustment_basis_issue: "Basis issue", insufficient_benchmark_history: "Insufficient benchmark history" } as Record<string, string>)[status ?? ""] ?? "—";
}

export function MartinBarWatchlistPage() {
  const [params, setParams] = useSearchParams();
  const runId = params.get("runId") ?? "";
  const [runs, setRuns] = useState<SavedRun[]>([]);
  const [runsOffset, setRunsOffset] = useState(0);
  const [runsHaveMore, setRunsHaveMore] = useState(false);
  const [loadingOlderRuns, setLoadingOlderRuns] = useState(false);
  const [payload, setPayload] = useState<FollowThrough | null>(null);
  const [loadingRuns, setLoadingRuns] = useState(true);
  const [loadingPage, setLoadingPage] = useState(false);
  const [error, setError] = useState("");
  const [offset, setOffset] = useState(0);

  useEffect(() => {
    let ignore = false;
    fetchJson<{ configured: boolean; runs: SavedRun[] }>(`/api/screener-runs?strategyId=martin_bar_watchlist_v1&limit=200&offset=0`)
      .then((result) => { if (!ignore) { setRuns(result.runs); setRunsOffset(result.runs.length); setRunsHaveMore(result.runs.length === 200); } })
      .catch((reason) => { if (!ignore) setError(reason instanceof Error ? reason.message : "Could not load saved runs."); })
      .finally(() => { if (!ignore) setLoadingRuns(false); });
    return () => { ignore = true; };
  }, []);

  useEffect(() => {
    let ignore = false;
    setPayload(null);
    setOffset(0);
    if (!runId) { setLoadingPage(false); return () => { ignore = true; }; }
    setLoadingPage(true);
    setError("");
    const query = new URLSearchParams({ runId, limit: String(PAGE_SIZE), offset: "0" });
    fetchJson<FollowThrough>(`/api/martin-bar-watchlist-follow-through?${query}`)
      .then((result) => { if (!ignore) setPayload(result); })
      .catch((reason) => { if (!ignore) setError(reason instanceof Error ? reason.message : "Could not load follow-through."); })
      .finally(() => { if (!ignore) setLoadingPage(false); });
    return () => { ignore = true; };
  }, [runId]);

  const selectedRun = useMemo(() => runs.find((run) => String(run.id) === runId), [runs, runId]);
  const selectRun = (id: string) => setParams(id ? { runId: id } : {}, { replace: true });
  const loadOlderRuns = async () => {
    setLoadingOlderRuns(true);
    try {
      const result = await fetchJson<{ runs: SavedRun[] }>(`/api/screener-runs?strategyId=martin_bar_watchlist_v1&limit=200&offset=${runsOffset}`);
      setRuns((current) => [...current, ...result.runs]);
      setRunsOffset((current) => current + result.runs.length);
      setRunsHaveMore(result.runs.length === 200);
    } catch (reason) { setError(reason instanceof Error ? reason.message : "Could not load older runs."); }
    finally { setLoadingOlderRuns(false); }
  };
  const loadMore = async () => {
    if (!payload || loadingPage) return;
    const nextOffset = offset + PAGE_SIZE;
    setLoadingPage(true);
    setError("");
    try {
      const query = new URLSearchParams({ runId, limit: String(PAGE_SIZE), offset: String(nextOffset) });
      const next = await fetchJson<FollowThrough>(`/api/martin-bar-watchlist-follow-through?${query}`);
      setPayload((current) => current ? { ...current, rows: [...current.rows, ...next.rows], has_more: next.has_more } : next);
      setOffset(nextOffset);
    } catch (reason) { setError(reason instanceof Error ? reason.message : "Could not load more hits."); }
    finally { setLoadingPage(false); }
  };

  return <div className="page-grid martin-history-page">
    <header className="page-header"><div><h1>Martin Bar Watchlist V1</h1><p className="panel-copy">Review saved discoveries and market-session follow-through for each published run.</p></div><Link className="ghost-button" to="/scanner">Back to scanners</Link></header>
    <section className="panel martin-history-toolbar">
      <label><span>Saved screen run</span><select value={runId} disabled={loadingRuns} onChange={(event) => selectRun(event.target.value)}>
        <option value="">{loadingRuns ? "Loading saved runs…" : "Choose a run"}</option>
        {runs.map((run) => <option value={run.id} key={run.id}>{formatLocalDate(run.run_date)} · {run.hit_count} hits · run #{run.id}</option>)}
        {runId && !runs.some((run) => String(run.id) === runId) ? <option value={runId}>Linked run #{runId}</option> : null}
      </select></label>
      {runsHaveMore ? <button className="ghost-button" type="button" onClick={() => void loadOlderRuns()} disabled={loadingOlderRuns}>{loadingOlderRuns ? "Loading older runs…" : "Load older saved runs"}</button> : null}
      {payload?.run ? <p className="panel-copy">Run #{payload.run.id} · signal date {formatLocalDate(payload.run.run_date)} · published {formatLocalDateTime(payload.run.created_at)}</p> : null}
      <p className="panel-copy">{payload?.baseline ?? "Returns use the next trading session open as baseline; 5, 21, and 63 count market sessions."} Results are descriptive follow-through, not trade P&amp;L.</p>
    </section>
    {error ? <div className="scanner-state" role="alert"><h2>Run history unavailable</h2><p>{error}</p></div> : null}
    {loadingPage && !payload ? <LoadingBlock label="Loading saved run and price history…" /> : null}
    {!loadingRuns && !loadingPage && !runId && !error ? <div className="scanner-state"><h2>Select a saved screen run</h2><p>Each selection shows the hits and reasons stored with that run.</p></div> : null}
    {payload ? <>
      {payload.rows.some((row) => row.retrospective_publication) ? <p className="martin-history-note">This run was published after its signal date. Its follow-through is retrospective.</p> : null}
      {payload.rows.some((row) => row.publication_status === "same_date_timing_unknown") ? <p className="panel-copy">Publication time relative to the session close is unavailable for same-date runs.</p> : null}
      <div className="data-table-responsive">
        <table className="data-table martin-history-table"><thead><tr><th>Ticker</th><th>Saved discovery reason</th><th>5 sessions</th><th>21 sessions</th><th>63 sessions</th><th>Details</th></tr></thead>
          <tbody>{payload.rows.map((row) => <tr key={`${row.ticker}-${row.rank}`}>
            <th scope="row">{row.ticker}</th>
            <td>{row.reasons.length ? row.reasons.join(" · ") : "No saved reason"}</td>
            {["5", "21", "63"].map((horizon) => { const value = row.horizons[horizon]; return <td key={horizon} className="martin-horizon-cell"><strong>{pct(value?.return_pct)}</strong><span>{value?.return_pct == null ? statusText(value?.status) : statusText(value.status)}</span></td>; })}
            <td><details><summary>Benchmarks &amp; downside</summary><div className="martin-detail-grid"><span>Price basis: next session open</span>{["5", "21", "63"].map((horizon) => <span key={`down-${horizon}`}>{horizon} session downside: {pct(row.horizons[horizon]?.downside_pct)}</span>)}{["SPY", "QQQ"].map((benchmark) => <span key={benchmark}>{benchmark}: {pct(row.benchmarks[benchmark]?.["5"])} / {pct(row.benchmarks[benchmark]?.["21"])} / {pct(row.benchmarks[benchmark]?.["63"])}</span>)}{row.horizons["5"]?.source ? <span>Price source: {row.horizons["5"].source}</span> : null}</div></details></td>
          </tr>)}</tbody>
        </table>
      </div>
      {payload.rows.length === 0 ? <div className="scanner-state"><h2>No saved hits in this run</h2><p>This run contains no passing Martin Bar Watchlist entries.</p></div> : null}
      {payload.has_more ? <div className="scanner-board-actions"><button className="ghost-button" onClick={() => void loadMore()} disabled={loadingPage}>{loadingPage ? "Loading…" : "Load more hits"}</button></div> : null}
      {loadingPage && payload ? <LoadingBlock label="Loading more saved hits…" /> : null}
    </> : null}
    {selectedRun && !payload && !loadingPage && !error ? <p className="panel-copy">Loading run #{selectedRun.id}…</p> : null}
  </div>;
}
