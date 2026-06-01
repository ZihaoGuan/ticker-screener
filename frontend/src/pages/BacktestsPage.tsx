import { useEffect, useMemo, useState } from "react";
import { useAuth } from "../auth/AuthContext";
import { LoadingBlock } from "../components/LoadingBlock";
import { Panel } from "../components/Panel";
import { StatusPill } from "../components/StatusPill";
import { fetchJson } from "../lib/api";
import { formatLocalDateTime } from "../lib/format";
import type { BacktestsResponse, JobsResponse, ScreenerRunDetail, ScreenerRunsResponse, SignalCacheCalendarDay, SignalCacheCalendarResponse } from "../lib/types";

const today = new Date().toISOString().slice(0, 10);
const ninetyDaysAgo = new Date(Date.now() - 90 * 24 * 60 * 60 * 1000).toISOString().slice(0, 10);
const currentMonthStart = new Date(new Date().getFullYear(), new Date().getMonth(), 1).toISOString().slice(0, 10);

export function BacktestsPage() {
  const auth = useAuth();
  const [payload, setPayload] = useState<BacktestsResponse | null>(null);
  const [screenRuns, setScreenRuns] = useState<ScreenerRunsResponse | null>(null);
  const [jobs, setJobs] = useState<JobsResponse | null>(null);
  const [calendarPayload, setCalendarPayload] = useState<SignalCacheCalendarResponse | null>(null);
  const [isLoading, setIsLoading] = useState(true);
  const [isLoadingCalendar, setIsLoadingCalendar] = useState(false);
  const [isSubmittingCache, setIsSubmittingCache] = useState(false);
  const [isSubmittingBacktest, setIsSubmittingBacktest] = useState(false);
  const [includeDeleted, setIncludeDeleted] = useState(false);
  const [cacheStrategies, setCacheStrategies] = useState<string[]>([]);
  const [cacheStartDate, setCacheStartDate] = useState(ninetyDaysAgo);
  const [cacheEndDate, setCacheEndDate] = useState(today);
  const [calendarMonth, setCalendarMonth] = useState(currentMonthStart);
  const [selectedCacheDate, setSelectedCacheDate] = useState("");
  const [selectedRunDetail, setSelectedRunDetail] = useState<ScreenerRunDetail | null>(null);
  const [isLoadingRunDetail, setIsLoadingRunDetail] = useState(false);
  const [backtestStrategies, setBacktestStrategies] = useState<string[]>([]);
  const [backtestMinCount, setBacktestMinCount] = useState("2");
  const [backtestStartDate, setBacktestStartDate] = useState(ninetyDaysAgo);
  const [backtestEndDate, setBacktestEndDate] = useState(today);
  const [signalCachePolicy, setSignalCachePolicy] = useState<"reuse_then_fill" | "reuse_only">("reuse_then_fill");
  const [statusMessage, setStatusMessage] = useState("");

  const refresh = () => {
    return Promise.all([
      fetchJson<BacktestsResponse>("/api/backtests"),
      fetchJson<ScreenerRunsResponse>(`/api/screener-runs?includeDeleted=${includeDeleted ? "true" : "false"}&limit=20`),
      fetchJson<JobsResponse>("/api/jobs"),
    ]).then(([backtestsPayload, screenRunsPayload, jobsPayload]) => {
      setPayload(backtestsPayload);
      setScreenRuns(screenRunsPayload);
      setJobs(jobsPayload);
    });
  };

  const refreshCalendar = () => {
    const monthStart = new Date(`${calendarMonth}T00:00:00`);
    const from = new Date(monthStart.getFullYear(), monthStart.getMonth(), 1);
    const to = new Date(monthStart.getFullYear(), monthStart.getMonth() + 1, 0);
    const params = new URLSearchParams({
      from: from.toISOString().slice(0, 10),
      to: to.toISOString().slice(0, 10),
      includeDeleted: includeDeleted ? "true" : "false",
    });
    for (const strategyId of cacheStrategies) {
      params.append("strategyIds", strategyId);
    }
    setIsLoadingCalendar(true);
    return fetchJson<SignalCacheCalendarResponse>(`/api/screener-runs/cache-calendar?${params.toString()}`)
      .then((result) => {
        setCalendarPayload(result);
        if (!selectedCacheDate && result.days.length > 0) {
          const firstInteresting = result.days.find((item) => item.cached_strategy_count > 0) ?? result.days[0];
          setSelectedCacheDate(firstInteresting.date);
        }
      })
      .finally(() => setIsLoadingCalendar(false));
  };

  useEffect(() => {
    void refresh()
      .catch(() => {
        setPayload({
          backtest_templates: [],
          backtest_runs: [],
          signal_cache: [],
          available_strategies: [],
          default_exit_rules: [],
        });
        setScreenRuns({ configured: false, runs: [], coverage: [], available_strategies: [] });
        setJobs({ actions: [], jobs: [] });
        setCalendarPayload(null);
      })
      .finally(() => setIsLoading(false));
  }, [includeDeleted]);

  useEffect(() => {
    if (!payload?.available_strategies?.length) {
      return;
    }
    if (cacheStrategies.length === 0) {
      setCacheStrategies(payload.available_strategies.slice(0, 2).map((item) => item.id));
    }
    if (backtestStrategies.length === 0) {
      setBacktestStrategies(payload.available_strategies.slice(0, 2).map((item) => item.id));
    }
  }, [payload, cacheStrategies.length, backtestStrategies.length]);

  useEffect(() => {
    if (!payload?.available_strategies?.length) {
      return;
    }
    void refreshCalendar();
  }, [payload, calendarMonth, includeDeleted, cacheStrategies.join(",")]);

  const activeResearchJobs = useMemo(
    () =>
      (jobs?.jobs ?? []).filter(
        (item) => item.action_id === "screener_history_batch" || item.action_id === "backtest_v1" || item.screen_run_id || item.backtest_run_id,
      ),
    [jobs],
  );

  const toggleStrategy = (current: string[], strategyId: string) =>
    current.includes(strategyId) ? current.filter((item) => item !== strategyId) : [...current, strategyId];

  const selectedCalendarDay = useMemo(
    () => calendarPayload?.days.find((item) => item.date === selectedCacheDate) ?? null,
    [calendarPayload, selectedCacheDate],
  );

  const monthGrid = useMemo(() => {
    const monthStart = new Date(`${calendarMonth}T00:00:00`);
    const start = new Date(monthStart.getFullYear(), monthStart.getMonth(), 1);
    const end = new Date(monthStart.getFullYear(), monthStart.getMonth() + 1, 0);
    const leading = (start.getDay() + 6) % 7;
    const days: Array<{ date: string; inMonth: boolean; payload: SignalCacheCalendarDay | null }> = [];
    for (let index = 0; index < leading; index += 1) {
      days.push({ date: "", inMonth: false, payload: null });
    }
    for (let day = 1; day <= end.getDate(); day += 1) {
      const date = new Date(start.getFullYear(), start.getMonth(), day).toISOString().slice(0, 10);
      days.push({
        date,
        inMonth: true,
        payload: calendarPayload?.days.find((item) => item.date === date) ?? null,
      });
    }
    while (days.length % 7 !== 0) {
      days.push({ date: "", inMonth: false, payload: null });
    }
    return days;
  }, [calendarMonth, calendarPayload]);

  const loadRunDetail = async (runId: number) => {
    setIsLoadingRunDetail(true);
    try {
      const detail = await fetchJson<ScreenerRunDetail>(`/api/screener-runs/${runId}?includeHits=true&hitLimit=500`);
      setSelectedRunDetail(detail);
    } finally {
      setIsLoadingRunDetail(false);
    }
  };

  const shiftCalendarMonth = (delta: number) => {
    const base = new Date(`${calendarMonth}T00:00:00`);
    const next = new Date(base.getFullYear(), base.getMonth() + delta, 1);
    setCalendarMonth(next.toISOString().slice(0, 10));
  };

  const submitSignalCache = async () => {
    setIsSubmittingCache(true);
    setStatusMessage("");
    try {
      const response = await fetchJson<{ ok: boolean; job_id: string }>("/api/screener-runs/batch", {
        method: "POST",
        body: JSON.stringify({
          strategy_ids: cacheStrategies,
          start_date: cacheStartDate,
          end_date: cacheEndDate,
          market_data_mode: "database-first",
          overwrite_policy: "skip_existing",
          scope: {},
        }),
      });
      setStatusMessage(`Signal cache job queued: ${response.job_id}`);
      await refresh();
      await refreshCalendar();
    } catch (error) {
      setStatusMessage(error instanceof Error ? error.message : "Unable to queue signal cache job.");
    } finally {
      setIsSubmittingCache(false);
    }
  };

  const submitBacktest = async () => {
    setIsSubmittingBacktest(true);
    setStatusMessage("");
    try {
      const minCount = Math.max(1, Number.parseInt(backtestMinCount, 10) || backtestStrategies.length || 1);
      const response = await fetchJson<{ ok: boolean; job_id: string }>("/api/backtests", {
        method: "POST",
        body: JSON.stringify({
          entry_rule: {
            mode: "min_count_same_day",
            screener_ids: backtestStrategies,
            min_count: minCount,
          },
          date_range: {
            start_date: backtestStartDate,
            end_date: backtestEndDate,
          },
          exit_rules: payload?.default_exit_rules ?? [],
          position_rules: {},
          signal_cache_policy: signalCachePolicy,
          market_data_mode: "database_only",
        }),
      });
      setStatusMessage(`Backtest job queued: ${response.job_id}`);
      await refresh();
    } catch (error) {
      setStatusMessage(error instanceof Error ? error.message : "Unable to queue backtest job.");
    } finally {
      setIsSubmittingBacktest(false);
    }
  };

  return (
    <div className="page-grid">
      <Panel title="Signal Cache" aside={<span className="eyebrow">{screenRuns?.configured ? "Persisted" : "DB off"}</span>}>
        {isLoading && !payload ? <LoadingBlock label="Loading backtest controls…" compact /> : null}
        <p className="panel-copy">Batch-fill historical screener snapshots so backtests can reuse persisted same-day signals.</p>
        <div className="button-row" style={{ flexWrap: "wrap", gap: 12 }}>
          {(payload?.available_strategies ?? []).map((item) => (
            <label key={item.id} style={{ display: "inline-flex", alignItems: "center", gap: 8 }}>
              <input
                type="checkbox"
                checked={cacheStrategies.includes(item.id)}
                onChange={() => setCacheStrategies((current) => toggleStrategy(current, item.id))}
              />
              <span>{item.label}</span>
            </label>
          ))}
        </div>
        {auth.hasCapability("run_backtests") ? (
          <div className="button-row" style={{ flexWrap: "wrap", marginTop: 16 }}>
            <label>
              <span className="eyebrow">Start</span>
              <input type="date" value={cacheStartDate} onChange={(event) => setCacheStartDate(event.target.value)} />
            </label>
            <label>
              <span className="eyebrow">End</span>
              <input type="date" value={cacheEndDate} onChange={(event) => setCacheEndDate(event.target.value)} />
            </label>
            <button type="button" className="screener-run-button" disabled={isSubmittingCache || cacheStrategies.length === 0} onClick={() => void submitSignalCache()}>
              {isSubmittingCache ? "QUEUING..." : "QUEUE CACHE FILL"}
            </button>
          </div>
        ) : (
          <p className="panel-copy">Signal cache fills are admin-only. Visitors can still inspect cache coverage.</p>
        )}
        <div className="button-row" style={{ justifyContent: "space-between", marginTop: 16 }}>
          <label style={{ display: "inline-flex", alignItems: "center", gap: 8 }}>
            <input type="checkbox" checked={includeDeleted} onChange={() => setIncludeDeleted((current) => !current)} />
            <span>Show soft-deleted screener runs</span>
          </label>
          {statusMessage ? <span className="eyebrow">{statusMessage}</span> : null}
        </div>
        <div className="data-table-responsive" style={{ marginTop: 16 }}>
          <table className="data-table">
            <thead>
              <tr>
                <th>Strategy</th>
                <th>Cached Days</th>
                <th>Days With Hits</th>
                <th>First Day</th>
                <th>Last Day</th>
              </tr>
            </thead>
            <tbody>
              {(screenRuns?.coverage ?? []).map((item) => (
                <tr key={item.strategy_id}>
                  <td>{item.strategy_id}</td>
                  <td>{item.run_count}</td>
                  <td>{item.run_with_hits_count}</td>
                  <td>{item.first_run_date ?? "-"}</td>
                  <td>{item.last_run_date ?? "-"}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
        <div className="panel-head" style={{ marginTop: 24 }}>
          <h3>Cache Calendar</h3>
          <div className="button-row">
            <button type="button" className="ghost-button" onClick={() => shiftCalendarMonth(-1)}>
              Prev
            </button>
            <span className="eyebrow">{calendarMonth.slice(0, 7)}</span>
            <button type="button" className="ghost-button" onClick={() => shiftCalendarMonth(1)}>
              Next
            </button>
          </div>
        </div>
        {isLoadingCalendar ? <LoadingBlock label="Loading signal cache calendar…" compact /> : null}
        <div className="calendar-legend">
          <span className="file-meta"><span className="calendar-dot is-none" /> none</span>
          <span className="file-meta"><span className="calendar-dot is-partial" /> partial</span>
          <span className="file-meta"><span className="calendar-dot is-no-hits" /> cached no hits</span>
          <span className="file-meta"><span className="calendar-dot is-hits" /> cached with hits</span>
        </div>
        <div className="cache-calendar-grid">
          {["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"].map((label) => (
            <div key={label} className="cache-calendar-head">
              {label}
            </div>
          ))}
          {monthGrid.map((item, index) =>
            item.inMonth ? (
              <button
                key={`${item.date}-${index}`}
                type="button"
                className={`cache-calendar-cell is-${item.payload?.status ?? "none"}${selectedCacheDate === item.date ? " is-selected" : ""}`}
                onClick={() => {
                  setSelectedCacheDate(item.date);
                  setSelectedRunDetail(null);
                }}
              >
                <span className="cache-calendar-day">{Number(item.date.slice(-2))}</span>
                <span className="cache-calendar-meta">{item.payload?.cached_strategy_count ?? 0}/{item.payload?.strategy_count ?? cacheStrategies.length}</span>
                <span className="cache-calendar-meta">{item.payload?.total_hits ?? 0} hits</span>
              </button>
            ) : (
              <div key={`blank-${index}`} className="cache-calendar-cell is-empty" />
            ),
          )}
        </div>
        <div className="split-grid" style={{ marginTop: 20 }}>
          <div className="panel" style={{ padding: 0, background: "transparent", border: 0 }}>
            <div className="panel-head">
              <h3>{selectedCacheDate || "Select a date"}</h3>
              <span className="eyebrow">{selectedCalendarDay?.status ?? "none"}</span>
            </div>
            <div className="data-table-responsive">
              <table className="data-table">
                <thead>
                  <tr>
                    <th>Strategy</th>
                    <th>Hits</th>
                    <th>Failures</th>
                    <th>Mode</th>
                    <th>State</th>
                    <th>Action</th>
                  </tr>
                </thead>
                <tbody>
                  {(selectedCalendarDay?.strategies ?? []).length === 0 ? (
                    <tr>
                      <td colSpan={6}>No cached screener runs for this date and strategy filter.</td>
                    </tr>
                  ) : (
                    (selectedCalendarDay?.strategies ?? []).map((item) => (
                      <tr key={item.run_id}>
                        <td>{item.strategy_id}</td>
                        <td>{item.hit_count}</td>
                        <td>{item.failure_count}</td>
                        <td>{item.market_data_mode || "-"}</td>
                        <td>{item.deleted_at ? "soft-deleted" : "active"}</td>
                        <td>
                          <button type="button" className="table-action-button" onClick={() => void loadRunDetail(item.run_id)}>
                            View Cached Result
                          </button>
                        </td>
                      </tr>
                    ))
                  )}
                </tbody>
              </table>
            </div>
          </div>
          <div className="panel" style={{ padding: 0, background: "transparent", border: 0 }}>
            <div className="panel-head">
              <h3>Cached Result Detail</h3>
              {selectedRunDetail ? <span className="eyebrow">run {selectedRunDetail.id}</span> : null}
            </div>
            {isLoadingRunDetail ? <LoadingBlock label="Loading cached result detail…" compact /> : null}
            {!selectedRunDetail && !isLoadingRunDetail ? <p className="panel-copy">Select a cached screener run to inspect its saved result detail.</p> : null}
            {selectedRunDetail ? (
              <div className="page-grid">
                <div className="detail-card">
                  <div className="detail-card-head">
                    <div>
                      <div className="ticker-symbol">{selectedRunDetail.strategy_id}</div>
                      <div className="file-meta">
                        {selectedRunDetail.run_date} · hits {selectedRunDetail.hit_count} · failures {selectedRunDetail.failure_count}
                      </div>
                    </div>
                    <span className="eyebrow">{selectedRunDetail.deleted_at ? "soft-deleted" : "active"}</span>
                  </div>
                  <div className="detail-grid">
                    <div>
                      <div className="eyebrow">Config Hash</div>
                      <div className="panel-copy">{selectedRunDetail.config_hash}</div>
                    </div>
                    <div>
                      <div className="eyebrow">Scope Hash</div>
                      <div className="panel-copy">{selectedRunDetail.scope_hash}</div>
                    </div>
                  </div>
                  <div className="detail-grid">
                    <div>
                      <div className="eyebrow">Raw Artifact</div>
                      <div className="panel-copy">{selectedRunDetail.raw_artifact_path || "-"}</div>
                    </div>
                    <div>
                      <div className="eyebrow">Watchlist Artifact</div>
                      <div className="panel-copy">{selectedRunDetail.watchlist_artifact_path || "-"}</div>
                    </div>
                  </div>
                  <div className="detail-subsection">
                    <div className="eyebrow">Result Summary</div>
                    <pre className="panel-copy" style={{ whiteSpace: "pre-wrap" }}>
                      {JSON.stringify(selectedRunDetail.result_summary_json ?? {}, null, 2)}
                    </pre>
                  </div>
                  <div className="detail-subsection">
                    <div className="eyebrow">Config Snapshot</div>
                    <pre className="panel-copy" style={{ whiteSpace: "pre-wrap" }}>
                      {JSON.stringify(selectedRunDetail.config_json ?? {}, null, 2)}
                    </pre>
                  </div>
                  <div className="detail-subsection">
                    <div className="eyebrow">Scope Snapshot</div>
                    <pre className="panel-copy" style={{ whiteSpace: "pre-wrap" }}>
                      {JSON.stringify(selectedRunDetail.scope_json ?? {}, null, 2)}
                    </pre>
                  </div>
                </div>
                <div className="data-table-responsive">
                  <table className="data-table">
                    <thead>
                      <tr>
                        <th>Ticker</th>
                        <th>Passed</th>
                        <th>Rank</th>
                        <th>Reasons</th>
                        <th>Metrics</th>
                      </tr>
                    </thead>
                    <tbody>
                      {(selectedRunDetail.hits ?? []).length === 0 ? (
                        <tr>
                          <td colSpan={5}>No hit rows stored for this cached screener result.</td>
                        </tr>
                      ) : (
                        (selectedRunDetail.hits ?? []).map((hit) => (
                          <tr key={hit.id}>
                            <td>{hit.ticker}</td>
                            <td>{hit.passed ? "yes" : "no"}</td>
                            <td>{hit.rank ?? "-"}</td>
                            <td>{(hit.reasons_json ?? []).map((item) => String(item)).join(", ") || "-"}</td>
                            <td>
                              <pre className="panel-copy" style={{ whiteSpace: "pre-wrap", margin: 0 }}>
                                {JSON.stringify(hit.metrics_json ?? {}, null, 2)}
                              </pre>
                            </td>
                          </tr>
                        ))
                      )}
                    </tbody>
                  </table>
                </div>
              </div>
            ) : null}
          </div>
        </div>
      </Panel>

      <Panel title="Backtest Runs" aside={<span className="eyebrow">min_count_same_day</span>}>
        <p className="panel-copy">Run reusable combo backtests from cached screener history with deterministic database-only exits.</p>
        <div className="button-row" style={{ flexWrap: "wrap", gap: 12 }}>
          {(payload?.available_strategies ?? []).map((item) => (
            <label key={item.id} style={{ display: "inline-flex", alignItems: "center", gap: 8 }}>
              <input
                type="checkbox"
                checked={backtestStrategies.includes(item.id)}
                onChange={() => setBacktestStrategies((current) => toggleStrategy(current, item.id))}
              />
              <span>{item.label}</span>
            </label>
          ))}
        </div>
        {auth.hasCapability("run_backtests") ? (
          <div className="button-row" style={{ flexWrap: "wrap", marginTop: 16 }}>
            <label>
              <span className="eyebrow">Min Count</span>
              <input type="number" min={1} value={backtestMinCount} onChange={(event) => setBacktestMinCount(event.target.value)} />
            </label>
            <label>
              <span className="eyebrow">Start</span>
              <input type="date" value={backtestStartDate} onChange={(event) => setBacktestStartDate(event.target.value)} />
            </label>
            <label>
              <span className="eyebrow">End</span>
              <input type="date" value={backtestEndDate} onChange={(event) => setBacktestEndDate(event.target.value)} />
            </label>
            <label>
              <span className="eyebrow">Cache Policy</span>
              <select value={signalCachePolicy} onChange={(event) => setSignalCachePolicy(event.target.value as "reuse_then_fill" | "reuse_only")}>
                <option value="reuse_then_fill">reuse_then_fill</option>
                <option value="reuse_only">reuse_only</option>
              </select>
            </label>
            <button
              type="button"
              className="screener-run-button"
              disabled={isSubmittingBacktest || backtestStrategies.length === 0}
              onClick={() => void submitBacktest()}
            >
              {isSubmittingBacktest ? "QUEUING..." : "QUEUE BACKTEST"}
            </button>
          </div>
        ) : (
          <p className="panel-copy">Backtest launches are admin-only. Visitors can still review saved backtest results.</p>
        )}
        <div className="template-list" style={{ marginTop: 16 }}>
          {(payload?.backtest_runs ?? []).map((item) => (
            <article key={item.id} className="metric-card">
              <div className="panel-head">
                <h3>{item.strategy_id}</h3>
                <span className="eyebrow">{item.created_at ? formatLocalDateTime(item.created_at) : ""}</span>
              </div>
              <p className="panel-copy">
                {item.start_date} to {item.end_date} · entries {item.summary?.entry_count ?? 0} · signals {item.summary?.signal_count ?? 0}
              </p>
              <p className="panel-copy">JSON: {item.json_report_path}</p>
              <p className="panel-copy">HTML: {item.html_report_path}</p>
            </article>
          ))}
        </div>
      </Panel>

      <Panel title="Recent Research Jobs">
        <div className="data-table-responsive">
          <table className="data-table">
            <thead>
              <tr>
                <th>Job</th>
                <th>Status</th>
                <th>Started</th>
                <th>Finished</th>
                <th>Output</th>
              </tr>
            </thead>
            <tbody>
              {activeResearchJobs.map((job) => (
                <tr key={job.job_id}>
                  <td>{job.label}</td>
                  <td><StatusPill status={job.status} /></td>
                  <td>{formatLocalDateTime(job.started_at)}</td>
                  <td>{formatLocalDateTime(job.finished_at)}</td>
                  <td>{job.summary_file || job.watchlist_file || "-"}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </Panel>

      <Panel title="Recent Screener History">
        <div className="data-table-responsive">
          <table className="data-table">
            <thead>
              <tr>
                <th>Strategy</th>
                <th>Run Date</th>
                <th>Hits</th>
                <th>Failures</th>
                <th>Deleted</th>
              </tr>
            </thead>
            <tbody>
              {(screenRuns?.runs ?? []).map((item) => (
                <tr key={item.id}>
                  <td>{item.strategy_id}</td>
                  <td>{item.run_date}</td>
                  <td>{item.hit_count}</td>
                  <td>{item.failure_count}</td>
                  <td>{item.deleted_at ? "soft-deleted" : "active"}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </Panel>
    </div>
  );
}
