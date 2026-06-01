import { useEffect, useMemo, useState } from "react";
import { useAuth } from "../auth/AuthContext";
import { LoadingBlock } from "../components/LoadingBlock";
import { Panel } from "../components/Panel";
import { StatusPill } from "../components/StatusPill";
import { fetchJson } from "../lib/api";
import { formatLocalDateTime } from "../lib/format";
import type { BacktestsResponse, JobsResponse, ScreenerRunsResponse } from "../lib/types";

const today = new Date().toISOString().slice(0, 10);
const ninetyDaysAgo = new Date(Date.now() - 90 * 24 * 60 * 60 * 1000).toISOString().slice(0, 10);

export function BacktestsPage() {
  const auth = useAuth();
  const [payload, setPayload] = useState<BacktestsResponse | null>(null);
  const [screenRuns, setScreenRuns] = useState<ScreenerRunsResponse | null>(null);
  const [jobs, setJobs] = useState<JobsResponse | null>(null);
  const [isLoading, setIsLoading] = useState(true);
  const [isSubmittingCache, setIsSubmittingCache] = useState(false);
  const [isSubmittingBacktest, setIsSubmittingBacktest] = useState(false);
  const [includeDeleted, setIncludeDeleted] = useState(false);
  const [cacheStrategies, setCacheStrategies] = useState<string[]>([]);
  const [cacheStartDate, setCacheStartDate] = useState(ninetyDaysAgo);
  const [cacheEndDate, setCacheEndDate] = useState(today);
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

  const activeResearchJobs = useMemo(
    () =>
      (jobs?.jobs ?? []).filter(
        (item) => item.action_id === "screener_history_batch" || item.action_id === "backtest_v1" || item.screen_run_id || item.backtest_run_id,
      ),
    [jobs],
  );

  const toggleStrategy = (current: string[], strategyId: string) =>
    current.includes(strategyId) ? current.filter((item) => item !== strategyId) : [...current, strategyId];

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
