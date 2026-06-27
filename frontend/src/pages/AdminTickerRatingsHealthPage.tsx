import { useEffect, useState, type FormEvent } from "react";
import { AdminSubnav } from "../components/AdminSubnav";
import { LoadingBlock } from "../components/LoadingBlock";
import { Panel } from "../components/Panel";
import { ProgressBar } from "../components/ProgressBar";
import { fetchJson } from "../lib/api";
import { formatCount, formatLocalDate, formatLocalDateTime } from "../lib/format";
import type { JobsResponse, RatingsAdminStatusResponse } from "../lib/types";
import "./RunsPage.css";

const EMPTY_RATINGS_RESPONSE: RatingsAdminStatusResponse = {
  database_configured: false,
  target_universe_count: 0,
  latest_fundamentals_as_of_date: null,
  latest_fundamentals_updated_at: null,
  latest_baselines_as_of_date: null,
  latest_baselines_updated_at: null,
  latest_ratings_as_of_date: null,
  latest_ratings_updated_at: null,
  latest_fundamentals_snapshot_count: 0,
  latest_rating_snapshot_count: 0,
  latest_fundamentals_parse_status_counts: {},
  latest_rating_status_counts: {},
  tickers_with_any_fundamentals: 0,
  tickers_with_latest_ok_rating: 0,
  diagnostics_count: 0,
  diagnostic_category_counts: {},
  diagnostics: [],
  healthy_remote_worker_count: 0,
  remote_workers: [],
  notes: [],
};

const RATINGS_ACTION_IDS = new Set([
  "run_finviz_ratings_pipeline",
  "sync_finviz_fundamentals",
  "build_sector_rating_baselines",
  "build_ticker_ratings",
]);

type RatingsRunJob = JobsResponse["jobs"][number];

export function AdminTickerRatingsHealthPage() {
  const [ratingsStatus, setRatingsStatus] = useState<RatingsAdminStatusResponse>(EMPTY_RATINGS_RESPONSE);
  const [isLoadingRatings, setIsLoadingRatings] = useState(true);
  const [ratingsRunAsOfDate, setRatingsRunAsOfDate] = useState(() => new Date().toISOString().slice(0, 10));
  const [ratingsRunTickers, setRatingsRunTickers] = useState("");
  const [isLaunchingRatingsRun, setIsLaunchingRatingsRun] = useState(false);
  const [ratingsRunNotice, setRatingsRunNotice] = useState("");
  const [ratingsRunJob, setRatingsRunJob] = useState<RatingsRunJob | null>(null);
  const [isLoadingRatingsRunJob, setIsLoadingRatingsRunJob] = useState(false);

  const loadRatings = () => {
    setIsLoadingRatings(true);
    void fetchJson<RatingsAdminStatusResponse>("/api/admin/ratings-status")
      .then(setRatingsStatus)
      .catch(() => {
        setRatingsStatus({
          ...EMPTY_RATINGS_RESPONSE,
          notes: ["Failed to load ratings status."],
        });
      })
      .finally(() => setIsLoadingRatings(false));
  };

  const loadRatingsRunJob = (jobId: string) => {
    if (!jobId) {
      return;
    }
    setIsLoadingRatingsRunJob(true);
    void fetchJson<RatingsRunJob>(`/api/jobs/${jobId}`)
      .then(setRatingsRunJob)
      .catch(() => {})
      .finally(() => setIsLoadingRatingsRunJob(false));
  };

  const loadLatestRatingsRunJob = () => {
    setIsLoadingRatingsRunJob(true);
    void fetchJson<JobsResponse>("/api/jobs")
      .then((result) => {
        const ratingsJobs = result.jobs.filter((job) => RATINGS_ACTION_IDS.has(job.action_id));
        ratingsJobs.sort((left, right) => {
          if (left.status === "running" && right.status !== "running") {
            return -1;
          }
          if (left.status !== "running" && right.status === "running") {
            return 1;
          }
          return String(right.started_at || "").localeCompare(String(left.started_at || ""));
        });
        setRatingsRunJob(ratingsJobs[0] ?? null);
      })
      .catch(() => {
        setRatingsRunJob(null);
      })
      .finally(() => setIsLoadingRatingsRunJob(false));
  };

  useEffect(() => {
    loadRatings();
    loadLatestRatingsRunJob();
  }, []);

  const handleLaunchRatingsRun = async (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    setIsLaunchingRatingsRun(true);
    setRatingsRunNotice("");
    try {
      const tickers = ratingsRunTickers
        .split(/[\s,]+/)
        .map((item) => item.trim().toUpperCase())
        .filter(Boolean);
      const response = await fetchJson<{ ok: boolean; job_id: string }>("/api/runs/run_finviz_ratings_pipeline", {
        method: "POST",
        body: JSON.stringify({
          as_of_date: ratingsRunAsOfDate,
          tickers,
        }),
      });
      setRatingsRunNotice(`Ratings pipeline launched: ${response.job_id}`);
      loadRatingsRunJob(response.job_id);
    } catch (error) {
      setRatingsRunNotice(error instanceof Error ? error.message : "Failed to launch ratings pipeline.");
    } finally {
      setIsLaunchingRatingsRun(false);
    }
  };

  const ratingDiagnostics = ratingsStatus.diagnostics;

  return (
    <div className="page-grid">
      <AdminSubnav
        title="Ticker Ratings Health"
        description="Inspect ratings pipeline freshness, diagnostics, remote worker health, and launch maintenance runs from a dedicated admin route."
      />

      <Panel title="Ticker Ratings Health" aside={<span className="eyebrow">{formatCount(ratingsStatus.diagnostics_count)} diagnostics</span>}>
        {isLoadingRatings ? <LoadingBlock label="Loading ratings status…" /> : null}
        <div className="run-toolbar">
          <form className="run-toolbar" onSubmit={(event) => void handleLaunchRatingsRun(event)}>
            <div className="run-params-grid">
              <label className="field">
                <span>As Of Date</span>
                <input type="date" value={ratingsRunAsOfDate} onChange={(event) => setRatingsRunAsOfDate(event.target.value)} required />
              </label>
              <label className="field" style={{ gridColumn: "span 2" }}>
                <span>Tickers</span>
                <input
                  type="text"
                  value={ratingsRunTickers}
                  onChange={(event) => setRatingsRunTickers(event.target.value)}
                  placeholder="Leave blank for full universe. Example: NVDA MSFT AAPL"
                />
              </label>
            </div>
            <div className="run-action-footer">
              <button className="primary-button" type="submit" disabled={isLaunchingRatingsRun}>
                {isLaunchingRatingsRun ? "Launching..." : "Run Ratings Pipeline Now"}
              </button>
              <button className="ghost-button" type="button" onClick={loadLatestRatingsRunJob} disabled={isLoadingRatingsRunJob}>
                {isLoadingRatingsRunJob ? "Refreshing job..." : "Refresh Job Progress"}
              </button>
              {ratingsRunNotice ? <span className="panel-copy">{ratingsRunNotice}</span> : null}
            </div>
          </form>

          <div className="run-progress-panel">
            <ProgressBar
              status={ratingsRunJob?.status ?? "cancelled"}
              label={
                ratingsRunJob
                  ? `${ratingsRunJob.label} · ${ratingsRunJob.progress_label || ratingsRunJob.status} · ${formatJobDuration(ratingsRunJob.duration_seconds)}`
                  : "No recent ratings pipeline job found"
              }
              progress={ratingsRunJob?.progress_percent ?? null}
            />
            {ratingsRunJob ? (
              <div className="data-table-responsive">
                <table className="data-table">
                  <tbody>
                    <tr>
                      <td data-label="Metric">Job ID</td>
                      <td data-label="Value" className="mono">#{ratingsRunJob.job_id}</td>
                    </tr>
                    <tr>
                      <td data-label="Metric">Action</td>
                      <td data-label="Value">{ratingsRunJob.action_id}</td>
                    </tr>
                    <tr>
                      <td data-label="Metric">Started / Finished</td>
                      <td data-label="Value">{formatLocalDateTime(ratingsRunJob.started_at)} / {formatLocalDateTime(ratingsRunJob.finished_at)}</td>
                    </tr>
                    <tr>
                      <td data-label="Metric">Progress</td>
                      <td data-label="Value">
                        {ratingsRunJob.progress_current != null && ratingsRunJob.progress_total != null
                          ? `${formatCount(ratingsRunJob.progress_current)} / ${formatCount(ratingsRunJob.progress_total)}`
                          : ratingsRunJob.progress_label || "-"}
                      </td>
                    </tr>
                    <tr>
                      <td data-label="Metric">Return Code</td>
                      <td data-label="Value">{ratingsRunJob.return_code ?? "-"}</td>
                    </tr>
                    <tr>
                      <td data-label="Metric">Scan Target</td>
                      <td data-label="Value">{ratingsRunJob.scan_target || "-"}</td>
                    </tr>
                  </tbody>
                </table>
              </div>
            ) : null}
            {ratingsRunJob?.log_tail ? (
              <pre className="panel-copy"><code>{ratingsRunJob.log_tail}</code></pre>
            ) : null}
          </div>

          <div className="card-grid overlap-cards">
            <article className="metric-card">
              <h3>Latest Fundamentals</h3>
              <div className="metric-value">{ratingsStatus.latest_fundamentals_as_of_date || "-"}</div>
              <div className="panel-copy">{formatCount(ratingsStatus.latest_fundamentals_snapshot_count)} snapshots</div>
            </article>
            <article className="metric-card">
              <h3>Latest Ratings</h3>
              <div className="metric-value">{ratingsStatus.latest_ratings_as_of_date || "-"}</div>
              <div className="panel-copy">{formatCount(ratingsStatus.latest_rating_snapshot_count)} snapshots</div>
            </article>
            <article className="metric-card">
              <h3>OK Ratings</h3>
              <div className="metric-value">{formatCount(ratingsStatus.tickers_with_latest_ok_rating)}</div>
              <div className="panel-copy">of {formatCount(ratingsStatus.target_universe_count)} target tickers</div>
            </article>
            <article className="metric-card">
              <h3>Healthy Workers</h3>
              <div className="metric-value">{formatCount(ratingsStatus.healthy_remote_worker_count)}</div>
              <div className="panel-copy">{formatCount(ratingsStatus.remote_workers.length)} registered remote workers</div>
            </article>
          </div>

          <div className="data-table-responsive">
            <table className="data-table">
              <tbody>
                <tr>
                  <td data-label="Metric">Database Configured</td>
                  <td data-label="Value">{ratingsStatus.database_configured ? "Yes" : "No"}</td>
                </tr>
                <tr>
                  <td data-label="Metric">Latest Fundamentals Updated</td>
                  <td data-label="Value">{formatLocalDateTime(ratingsStatus.latest_fundamentals_updated_at)}</td>
                </tr>
                <tr>
                  <td data-label="Metric">Latest Baselines Date / Updated</td>
                  <td data-label="Value">{ratingsStatus.latest_baselines_as_of_date || "-"} / {formatLocalDateTime(ratingsStatus.latest_baselines_updated_at)}</td>
                </tr>
                <tr>
                  <td data-label="Metric">Latest Ratings Updated</td>
                  <td data-label="Value">{formatLocalDateTime(ratingsStatus.latest_ratings_updated_at)}</td>
                </tr>
                <tr>
                  <td data-label="Metric">Tickers With Any Fundamentals</td>
                  <td data-label="Value">{formatCount(ratingsStatus.tickers_with_any_fundamentals)}</td>
                </tr>
                <tr>
                  <td data-label="Metric">Diagnostics</td>
                  <td data-label="Value">{formatCount(ratingsStatus.diagnostics_count)}</td>
                </tr>
              </tbody>
            </table>
          </div>

          {ratingsStatus.notes.length > 0 ? <div className="panel-copy">{ratingsStatus.notes.join(" ")}</div> : null}

          <div className="admin-sample-grid">
            <div>
              <div className="eyebrow">Remote Workers</div>
              <div className="pill-list">
                {ratingsStatus.remote_workers.length === 0 ? (
                  <span className="panel-copy">No remote workers have heartbeated yet.</span>
                ) : (
                  ratingsStatus.remote_workers.map((worker) => (
                    <span key={worker.worker_name} className="symbol-pill">
                      {worker.worker_name}: {worker.is_healthy ? "healthy" : "stale"}{worker.current_job_run_id ? ` · job ${worker.current_job_run_id}` : ""}
                    </span>
                  ))
                )}
              </div>
            </div>

            <div>
              <div className="eyebrow">Latest Fundamentals Parse Status</div>
              <div className="pill-list">
                {Object.entries(ratingsStatus.latest_fundamentals_parse_status_counts).length === 0 ? (
                  <span className="panel-copy">No fundamentals batch summary yet.</span>
                ) : (
                  Object.entries(ratingsStatus.latest_fundamentals_parse_status_counts).map(([key, value]) => (
                    <span key={key} className="symbol-pill">{key}: {formatCount(value)}</span>
                  ))
                )}
              </div>
            </div>

            <div>
              <div className="eyebrow">Latest Rating Status</div>
              <div className="pill-list">
                {Object.entries(ratingsStatus.latest_rating_status_counts).length === 0 ? (
                  <span className="panel-copy">No rating batch summary yet.</span>
                ) : (
                  Object.entries(ratingsStatus.latest_rating_status_counts).map(([key, value]) => (
                    <span key={key} className="symbol-pill">{key}: {formatCount(value)}</span>
                  ))
                )}
              </div>
            </div>

            <div>
              <div className="eyebrow">Diagnostic Categories</div>
              <div className="pill-list">
                {Object.entries(ratingsStatus.diagnostic_category_counts).length === 0 ? (
                  <span className="panel-copy">No rating diagnostics.</span>
                ) : (
                  Object.entries(ratingsStatus.diagnostic_category_counts).map(([key, value]) => (
                    <span key={key} className="symbol-pill">{key}: {formatCount(value)}</span>
                  ))
                )}
              </div>
            </div>
          </div>

          <div className="run-action-footer">
            <button className="ghost-button" type="button" onClick={loadRatings} disabled={isLoadingRatings}>
              {isLoadingRatings ? "Refreshing..." : "Refresh Ratings Status"}
            </button>
            <span className="panel-copy">Use the Runs page scheduler to automate `run_finviz_ratings_pipeline`.</span>
          </div>

          <div className="data-table-responsive">
            <table className="data-table">
              <thead>
                <tr>
                  <th>Ticker</th>
                  <th>Category</th>
                  <th>Reason</th>
                  <th>Fundamentals Date</th>
                  <th>Rating Date</th>
                  <th>Status</th>
                  <th>Metric Gaps</th>
                </tr>
              </thead>
              <tbody>
                {ratingDiagnostics.length === 0 ? (
                  <tr>
                    <td colSpan={7}>{isLoadingRatings ? "Loading ratings diagnostics..." : "No missing-rating diagnostics right now."}</td>
                  </tr>
                ) : (
                  ratingDiagnostics.map((item) => (
                    <tr key={`${item.ticker}-${item.category}`}>
                      <td data-label="Ticker">
                        <div className="admin-job-cell">
                          <strong>{item.ticker}</strong>
                          <span className="file-meta">{item.sector || "-"}{item.industry ? ` · ${item.industry}` : ""}</span>
                        </div>
                      </td>
                      <td data-label="Category">{item.category}</td>
                      <td data-label="Reason">{item.reason}</td>
                      <td data-label="Fundamentals Date">{item.fundamentals_as_of_date ? formatLocalDate(item.fundamentals_as_of_date) : "-"}</td>
                      <td data-label="Rating Date">{item.rating_as_of_date ? formatLocalDate(item.rating_as_of_date) : "-"}</td>
                      <td data-label="Status">{item.parse_status || item.rating_status || "-"}</td>
                      <td data-label="Metric Gaps" className="file-meta">
                        {item.missing_metric_names.length > 0
                          ? `missing: ${item.missing_metric_names.join(", ")}`
                          : item.insufficient_baseline_metrics.length > 0
                            ? `baseline: ${item.insufficient_baseline_metrics.join(", ")}`
                            : item.overall_rating != null
                              ? `rating ${item.overall_rating.toFixed(1)}`
                              : "-"}
                      </td>
                    </tr>
                  ))
                )}
              </tbody>
            </table>
          </div>
        </div>
      </Panel>
    </div>
  );
}

function formatJobDuration(totalSeconds: number | null | undefined): string {
  const value = Math.max(0, Number(totalSeconds || 0));
  const hours = Math.floor(value / 3600);
  const minutes = Math.floor((value % 3600) / 60);
  const seconds = value % 60;
  if (hours > 0) {
    return `${hours}h ${minutes}m ${seconds}s`;
  }
  if (minutes > 0) {
    return `${minutes}m ${seconds}s`;
  }
  return `${seconds}s`;
}
