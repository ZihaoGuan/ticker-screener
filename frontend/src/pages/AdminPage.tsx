import { useEffect, useMemo, useState, type FormEvent } from "react";
import { NavLink } from "react-router-dom";
import { ExclusionDialog } from "../components/ExclusionDialog";
import { LoadingBlock } from "../components/LoadingBlock";
import { Panel } from "../components/Panel";
import { ProgressBar } from "../components/ProgressBar";
import { fetchJson } from "../lib/api";
import { formatCount, formatLocalDate, formatLocalDateTime } from "../lib/format";
import type {
  AccessRequestSummary,
  AdminResponse,
  AuditEventSummary,
  AuditEventsResponse,
  ExclusionEntry,
  JobsResponse,
  MissingSectorAdminResponse,
  PartialTickerDetailResponse,
  RatingsAdminStatusResponse,
  RoleName,
} from "../lib/types";
import "./RunsPage.css";

const EMPTY_ADMIN_RESPONSE: AdminResponse = {
  excluded_tickers: [],
  excluded_count: 0,
  database_status: {
    database_configured: false,
    coverage_start: "2020-01-01",
    coverage_end: "",
    target_universe_count: 0,
    db_ticker_count: 0,
    covered_ticker_count: 0,
    partial_ticker_count: 0,
    missing_ticker_count: 0,
    total_bar_rows: 0,
    overall_first_trade_date: null,
    overall_last_trade_date: null,
    latest_metadata_update_at: null,
    stale_ticker_count: 0,
    coverage_percent: 0,
    sample_missing_tickers: [],
    sample_partial_tickers: [],
    notes: [],
  },
};

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

const EMPTY_MISSING_SECTOR_RESPONSE: MissingSectorAdminResponse = {
  database_configured: false,
  missing_count: 0,
  tickers: [],
  available_sectors: [],
  notes: [],
};

const RATINGS_ACTION_IDS = new Set([
  "run_finviz_ratings_pipeline",
  "sync_finviz_fundamentals",
  "build_sector_rating_baselines",
  "build_ticker_ratings",
]);

type RatingsRunJob = JobsResponse["jobs"][number];

export function AdminPage() {
  const [payload, setPayload] = useState<AdminResponse>(EMPTY_ADMIN_RESPONSE);
  const [ratingsStatus, setRatingsStatus] = useState<RatingsAdminStatusResponse>(EMPTY_RATINGS_RESPONSE);
  const [missingSectorPayload, setMissingSectorPayload] = useState<MissingSectorAdminResponse>(EMPTY_MISSING_SECTOR_RESPONSE);
  const [coverageStart, setCoverageStart] = useState("2020-01-01");
  const [syncStartDate, setSyncStartDate] = useState("2020-01-01");
  const [syncEndDate, setSyncEndDate] = useState("");
  const [syncTickers, setSyncTickers] = useState("");
  const [chunkSize, setChunkSize] = useState("100");
  const [isLaunching, setIsLaunching] = useState(false);
  const [launchMessage, setLaunchMessage] = useState("");
  const [isLoading, setIsLoading] = useState(true);
  const [exclusionFilter, setExclusionFilter] = useState("");
  const [selectedGapTicker, setSelectedGapTicker] = useState("");
  const [partialDetail, setPartialDetail] = useState<PartialTickerDetailResponse | null>(null);
  const [isGapLoading, setIsGapLoading] = useState(false);
  const [gapError, setGapError] = useState("");
  const [selectedExclusion, setSelectedExclusion] = useState<ExclusionEntry | null>(null);
  const [isRemoving, setIsRemoving] = useState(false);
  const [notice, setNotice] = useState("");
  const [users, setUsers] = useState<
    Array<{ id: number; email: string; role: RoleName; is_active: boolean; created_at?: string | null; updated_at?: string | null; last_login_at?: string | null }>
  >([]);
  const [inviteEmail, setInviteEmail] = useState("");
  const [inviteRole, setInviteRole] = useState<RoleName>("visitor");
  const [isSavingUser, setIsSavingUser] = useState(false);
  const [accessRequests, setAccessRequests] = useState<AccessRequestSummary[]>([]);
  const [auditEvents, setAuditEvents] = useState<AuditEventSummary[]>([]);
  const [auditActorEmail, setAuditActorEmail] = useState("");
  const [auditAction, setAuditAction] = useState("");
  const [auditResourceType, setAuditResourceType] = useState("");
  const [auditFromDate, setAuditFromDate] = useState("");
  const [auditToDate, setAuditToDate] = useState("");
  const [isLoadingAudit, setIsLoadingAudit] = useState(false);
  const [isLoadingRatings, setIsLoadingRatings] = useState(true);
  const [ratingsRunAsOfDate, setRatingsRunAsOfDate] = useState(() => new Date().toISOString().slice(0, 10));
  const [ratingsRunTickers, setRatingsRunTickers] = useState("");
  const [isLaunchingRatingsRun, setIsLaunchingRatingsRun] = useState(false);
  const [ratingsRunNotice, setRatingsRunNotice] = useState("");
  const [ratingsRunJob, setRatingsRunJob] = useState<RatingsRunJob | null>(null);
  const [isLoadingRatingsRunJob, setIsLoadingRatingsRunJob] = useState(false);
  const [missingSectorFilter, setMissingSectorFilter] = useState("");
  const [sectorSelections, setSectorSelections] = useState<Record<string, string>>({});
  const [isLoadingMissingSectors, setIsLoadingMissingSectors] = useState(true);
  const [isSavingSector, setIsSavingSector] = useState(false);
  const [sectorNotice, setSectorNotice] = useState("");

  const loadAdmin = (start: string) => {
    setIsLoading(true);
    const query = new URLSearchParams({ coverageStart: start });
    void fetchJson<AdminResponse>(`/api/admin/exclusions?${query.toString()}`)
      .then(setPayload)
      .catch(() => {
        setPayload({
          ...EMPTY_ADMIN_RESPONSE,
          database_status: {
            ...EMPTY_ADMIN_RESPONSE.database_status,
            notes: ["Failed to load admin data."],
          },
        });
      })
      .finally(() => setIsLoading(false));
  };

  const loadAudit = () => {
    setIsLoadingAudit(true);
    const query = new URLSearchParams();
    if (auditActorEmail.trim()) {
      query.set("actorEmail", auditActorEmail.trim());
    }
    if (auditAction.trim()) {
      query.set("action", auditAction.trim());
    }
    if (auditResourceType.trim()) {
      query.set("resourceType", auditResourceType.trim());
    }
    if (auditFromDate.trim()) {
      query.set("from", auditFromDate.trim());
    }
    if (auditToDate.trim()) {
      query.set("to", auditToDate.trim());
    }
    query.set("limit", "50");
    void fetchJson<AuditEventsResponse>(`/api/admin/audit-events?${query.toString()}`)
      .then((result) => setAuditEvents(result.events))
      .catch(() => setAuditEvents([]))
      .finally(() => setIsLoadingAudit(false));
  };

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

  const loadMissingSectors = () => {
    setIsLoadingMissingSectors(true);
    void fetchJson<MissingSectorAdminResponse>("/api/admin/missing-sectors")
      .then((result) => {
        setMissingSectorPayload(result);
        setSectorSelections((current) => {
          const next: Record<string, string> = {};
          result.tickers.forEach((item) => {
            next[item.ticker] = current[item.ticker] ?? item.suggested_sector ?? "";
          });
          return next;
        });
      })
      .catch(() => {
        setMissingSectorPayload({
          ...EMPTY_MISSING_SECTOR_RESPONSE,
          notes: ["Failed to load missing-sector tickers."],
        });
      })
      .finally(() => setIsLoadingMissingSectors(false));
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
    loadAdmin(coverageStart);
    loadRatings();
    loadMissingSectors();
    loadLatestRatingsRunJob();
    void fetchJson<{ users: Array<{ id: number; email: string; role: RoleName; is_active: boolean; created_at?: string | null; updated_at?: string | null; last_login_at?: string | null }>; access_requests?: AccessRequestSummary[] }>("/api/admin/users")
      .then((result) => {
        setUsers(result.users);
        setAccessRequests(result.access_requests ?? []);
      })
      .catch(() => {
        setUsers([]);
        setAccessRequests([]);
      });
  }, [coverageStart]);

  useEffect(() => {
    loadAudit();
  }, []);

  useEffect(() => {
    if (!ratingsRunJob?.job_id) {
      return;
    }
    if (ratingsRunJob.status === "running" || ratingsRunJob.status === "queued") {
      const timer = window.setTimeout(() => {
        loadRatingsRunJob(ratingsRunJob.job_id);
      }, 5000);
      return () => window.clearTimeout(timer);
    }
    if (ratingsRunJob.status === "success" || ratingsRunJob.status === "failed" || ratingsRunJob.status === "cancelled") {
      loadRatings();
    }
    return undefined;
  }, [ratingsRunJob?.job_id, ratingsRunJob?.status]);

  const handleLaunchSync = async (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    setIsLaunching(true);
    setLaunchMessage("");
    try {
      const tickers = syncTickers
        .split(/[\s,]+/)
        .map((item) => item.trim().toUpperCase())
        .filter(Boolean);
      const payloadBody: Record<string, string | string[] | number> = {
        start_date: syncStartDate,
      };
      if (syncEndDate.trim()) {
        payloadBody.end_date = syncEndDate.trim();
      }
      if (tickers.length > 0) {
        payloadBody.tickers = tickers;
      }
      if (chunkSize.trim()) {
        payloadBody.chunk_size = Number(chunkSize);
      }
      const response = await fetchJson<{ ok: boolean; job_id: string }>("/api/admin/history-sync", {
        method: "POST",
        body: JSON.stringify(payloadBody),
      });
      setLaunchMessage(`Sync job launched: ${response.job_id}`);
      loadAudit();
    } catch (error) {
      setLaunchMessage(error instanceof Error ? error.message : "Failed to launch sync job.");
    } finally {
      setIsLaunching(false);
    }
  };

  const handleInspectGap = async (ticker: string) => {
    setSelectedGapTicker(ticker);
    setGapError("");
    setIsGapLoading(true);
    try {
      const query = new URLSearchParams({ coverageStart });
      const detail = await fetchJson<PartialTickerDetailResponse>(`/api/admin/partial-tickers/${ticker}?${query.toString()}`);
      setPartialDetail(detail);
    } catch (error) {
      setPartialDetail(null);
      setGapError(error instanceof Error ? error.message : "Failed to load missing-date detail.");
    } finally {
      setIsGapLoading(false);
    }
  };

  const handleRemoveExclusion = async (reason: string) => {
    if (!selectedExclusion) {
      return;
    }
    setIsRemoving(true);
    try {
      await fetchJson<{ ok: boolean }>(`/api/admin/exclusions/${selectedExclusion.ticker}/remove`, {
        method: "POST",
        body: JSON.stringify({ reason }),
      });
      setNotice(`${selectedExclusion.ticker} removed from removable exclusions.`);
      setSelectedExclusion(null);
      loadAdmin(coverageStart);
      loadAudit();
    } catch (error) {
      setNotice(error instanceof Error ? error.message : "Failed to remove exclusion.");
    } finally {
      setIsRemoving(false);
    }
  };

  const filteredExclusions = useMemo(() => {
    const query = exclusionFilter.trim().toLowerCase();
    if (!query) {
      return payload.excluded_tickers;
    }
    return payload.excluded_tickers.filter((entry) =>
      [entry.ticker, entry.reason, entry.reasons.join(" "), entry.sources.join(" ")].join(" ").toLowerCase().includes(query),
    );
  }, [exclusionFilter, payload.excluded_tickers]);

  const filteredMissingSectorTickers = useMemo(() => {
    const query = missingSectorFilter.trim().toLowerCase();
    if (!query) {
      return missingSectorPayload.tickers;
    }
    return missingSectorPayload.tickers.filter((entry) =>
      [entry.ticker, entry.exchange, entry.industry, entry.source, entry.suggested_sector, entry.suggested_industry]
        .join(" ")
        .toLowerCase()
        .includes(query),
    );
  }, [missingSectorFilter, missingSectorPayload.tickers]);

  const db = payload.database_status;
  const ratingDiagnostics = ratingsStatus.diagnostics;

  const refreshUsers = () => {
    void fetchJson<{ users: Array<{ id: number; email: string; role: RoleName; is_active: boolean; created_at?: string | null; updated_at?: string | null; last_login_at?: string | null }>; access_requests?: AccessRequestSummary[] }>("/api/admin/users")
      .then((result) => {
        setUsers(result.users);
        setAccessRequests(result.access_requests ?? []);
      })
      .catch(() => {
        setUsers([]);
        setAccessRequests([]);
      });
  };

  const handleInviteUser = async (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    setIsSavingUser(true);
    setNotice("");
    try {
      await fetchJson<{ ok: boolean }>("/api/admin/users/invite", {
        method: "POST",
        body: JSON.stringify({ email: inviteEmail, role: inviteRole }),
      });
      setInviteEmail("");
      setInviteRole("visitor");
      setNotice("User saved. They can now sign in with Google using that email.");
      refreshUsers();
      loadAudit();
    } catch (error) {
      setNotice(error instanceof Error ? error.message : "Failed to save user.");
    } finally {
      setIsSavingUser(false);
    }
  };

  const handleRoleChange = async (userId: number, role: RoleName) => {
    setIsSavingUser(true);
    setNotice("");
    try {
      await fetchJson<{ ok: boolean }>(`/api/admin/users/${userId}/role`, {
        method: "POST",
        body: JSON.stringify({ role }),
      });
      setNotice("Role updated.");
      refreshUsers();
      loadAudit();
    } catch (error) {
      setNotice(error instanceof Error ? error.message : "Failed to update role.");
    } finally {
      setIsSavingUser(false);
    }
  };

  const handleToggleUser = async (userId: number, isActive: boolean) => {
    setIsSavingUser(true);
    setNotice("");
    try {
      await fetchJson<{ ok: boolean }>(`/api/admin/users/${userId}/${isActive ? "deactivate" : "reactivate"}`, {
        method: "POST",
      });
      setNotice(isActive ? "User deactivated." : "User reactivated.");
      refreshUsers();
      loadAudit();
    } catch (error) {
      setNotice(error instanceof Error ? error.message : "Failed to update user.");
    } finally {
      setIsSavingUser(false);
    }
  };

  const handleApproveAccessRequest = async (requestId: number) => {
    setIsSavingUser(true);
    setNotice("");
    try {
      await fetchJson<{ ok: boolean }>(`/api/admin/access-requests/${requestId}/approve`, {
        method: "POST",
      });
      setNotice("Access request approved. User can now sign in with Google using that email.");
      refreshUsers();
      loadAudit();
    } catch (error) {
      setNotice(error instanceof Error ? error.message : "Failed to approve access request.");
    } finally {
      setIsSavingUser(false);
    }
  };

  const handleDenyAccessRequest = async (requestId: number) => {
    setIsSavingUser(true);
    setNotice("");
    try {
      await fetchJson<{ ok: boolean }>(`/api/admin/access-requests/${requestId}/deny`, {
        method: "POST",
        body: JSON.stringify({ deny_reason: "" }),
      });
      setNotice("Access request denied.");
      refreshUsers();
      loadAudit();
    } catch (error) {
      setNotice(error instanceof Error ? error.message : "Failed to deny access request.");
    } finally {
      setIsSavingUser(false);
    }
  };

  const handleAuditFilterSubmit = (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    loadAudit();
  };

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
      loadAudit();
    } catch (error) {
      setRatingsRunNotice(error instanceof Error ? error.message : "Failed to launch ratings pipeline.");
    } finally {
      setIsLaunchingRatingsRun(false);
    }
  };

  const handleAssignSector = async (ticker: string) => {
    const sector = (sectorSelections[ticker] || "").trim();
    if (!sector) {
      setSectorNotice("Select a sector before saving.");
      return;
    }
    setIsSavingSector(true);
    setSectorNotice("");
    try {
      await fetchJson<{ ok: boolean; entry: { ticker: string; sector: string } }>(`/api/admin/ticker-sectors/${ticker}`, {
        method: "POST",
        body: JSON.stringify({ sector }),
      });
      setSectorNotice(`${ticker} sector set to ${sector}.`);
      loadMissingSectors();
      loadRatings();
      loadAudit();
    } catch (error) {
      setSectorNotice(error instanceof Error ? error.message : "Failed to update sector.");
    } finally {
      setIsSavingSector(false);
    }
  };

  return (
    <div className="page-grid">
      <section className="panel screeners-subnav-panel">
        <div className="screeners-subnav-copy">
          <span className="eyebrow">Admin</span>
          <h1>Operations Console</h1>
          <p className="panel-copy">Manage exclusions, ratings maintenance, users, and notification plumbing from one admin area.</p>
        </div>
        <div className="screeners-subnav-links" role="tablist" aria-label="Admin sections">
          <NavLink to="/admin" end className={({ isActive }) => `screeners-subnav-link${isActive ? " is-active" : ""}`}>
            Admin Overview
          </NavLink>
          <NavLink to="/admin/discord-notifications" className={({ isActive }) => `screeners-subnav-link${isActive ? " is-active" : ""}`}>
            Discord Alerts
          </NavLink>
        </div>
      </section>
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

      <Panel title="Missing Sector Assignments" aside={<span className="eyebrow">{formatCount(missingSectorPayload.missing_count)} tickers</span>}>
        {isLoadingMissingSectors ? <LoadingBlock label="Loading missing-sector tickers…" /> : null}
        <div className="run-toolbar">
          <div className="run-action-footer">
            <label className="field" style={{ flex: "1 1 20rem" }}>
              <span>Filter tickers</span>
              <input
                type="text"
                value={missingSectorFilter}
                onChange={(event) => setMissingSectorFilter(event.target.value)}
                placeholder="Ticker, exchange, industry, source"
              />
            </label>
            <button className="ghost-button" type="button" onClick={loadMissingSectors} disabled={isLoadingMissingSectors || isSavingSector}>
              {isLoadingMissingSectors ? "Refreshing..." : "Refresh Missing Sectors"}
            </button>
          </div>

          {missingSectorPayload.notes.length > 0 ? <div className="panel-copy">{missingSectorPayload.notes.join(" ")}</div> : null}
          {sectorNotice ? <div className="panel-copy">{sectorNotice}</div> : null}

          <div className="pill-list">
            {missingSectorPayload.available_sectors.map((sector) => (
              <span key={sector} className="symbol-pill">{sector}</span>
            ))}
          </div>

          <div className="data-table-responsive">
            <table className="data-table">
              <thead>
                <tr>
                  <th>Ticker</th>
                  <th>Industry</th>
                  <th>Suggested</th>
                  <th>Source</th>
                  <th>Updated</th>
                  <th>Assign Sector</th>
                </tr>
              </thead>
              <tbody>
                {filteredMissingSectorTickers.length === 0 ? (
                  <tr>
                    <td colSpan={6}>
                      {isLoadingMissingSectors ? "Loading missing-sector tickers..." : "No tickers missing sector."}
                    </td>
                  </tr>
                ) : (
                  filteredMissingSectorTickers.map((item) => (
                    <tr key={item.ticker}>
                      <td data-label="Ticker">
                        <div className="admin-job-cell">
                          <strong>{item.ticker}</strong>
                          <span className="file-meta">{item.exchange || "-"}</span>
                        </div>
                      </td>
                      <td data-label="Industry" className="file-meta">
                        {item.industry || item.suggested_industry || "-"}
                      </td>
                      <td data-label="Suggested">{item.suggested_sector || "-"}</td>
                      <td data-label="Source">{item.source || "-"}</td>
                      <td data-label="Updated">{formatLocalDateTime(item.updated_at)}</td>
                      <td data-label="Assign Sector">
                        <div className="button-row">
                          <select
                            value={sectorSelections[item.ticker] ?? ""}
                            onChange={(event) =>
                              setSectorSelections((current) => ({
                                ...current,
                                [item.ticker]: event.target.value,
                              }))
                            }
                            disabled={isSavingSector}
                          >
                            <option value="">Select sector</option>
                            {missingSectorPayload.available_sectors.map((sector) => (
                              <option key={`${item.ticker}-${sector}`} value={sector}>
                                {sector}
                              </option>
                            ))}
                          </select>
                          <button
                            className="table-action-button"
                            type="button"
                            onClick={() => void handleAssignSector(item.ticker)}
                            disabled={isSavingSector || !(sectorSelections[item.ticker] || "").trim()}
                          >
                            {isSavingSector ? "Saving..." : "Save"}
                          </button>
                        </div>
                      </td>
                    </tr>
                  ))
                )}
              </tbody>
            </table>
          </div>
        </div>
      </Panel>

      <Panel title="Postgres History Coverage" aside={<span className="eyebrow">{db.coverage_percent}% covered</span>}>
        {isLoading ? <LoadingBlock label="Loading admin coverage…" /> : null}
        <div className="run-toolbar">
          <div className="run-params-grid">
            <label className="field">
              <span>Coverage Start</span>
              <input type="date" value={coverageStart} onChange={(event) => setCoverageStart(event.target.value)} />
            </label>
          </div>

          <div className="card-grid overlap-cards">
            <article className="metric-card">
              <h3>Target Universe</h3>
              <div className="metric-value">{formatCount(db.target_universe_count)}</div>
            </article>
            <article className="metric-card">
              <h3>Fully Covered</h3>
              <div className="metric-value">{formatCount(db.covered_ticker_count)}</div>
            </article>
            <article className="metric-card">
              <h3>Missing / Partial</h3>
              <div className="metric-value">{formatCount(db.missing_ticker_count + db.partial_ticker_count)}</div>
            </article>
          </div>

          <div className="data-table-responsive">
            <table className="data-table">
              <tbody>
                <tr>
                  <td data-label="Metric">Database Configured</td>
                  <td data-label="Value">{db.database_configured ? "Yes" : "No"}</td>
                </tr>
                <tr>
                  <td data-label="Metric">Coverage Window</td>
                  <td data-label="Value">{db.coverage_start} to {db.coverage_end || "-"}</td>
                </tr>
                <tr>
                  <td data-label="Metric">DB Tickers</td>
                  <td data-label="Value">{formatCount(db.db_ticker_count)}</td>
                </tr>
                <tr>
                  <td data-label="Metric">Total Bar Rows</td>
                  <td data-label="Value">{formatCount(db.total_bar_rows)}</td>
                </tr>
                <tr>
                  <td data-label="Metric">Overall First / Last Trade Date</td>
                  <td data-label="Value">{db.overall_first_trade_date || "-"} / {db.overall_last_trade_date || "-"}</td>
                </tr>
                <tr>
                  <td data-label="Metric">Latest Metadata Update</td>
                  <td data-label="Value">{formatLocalDateTime(db.latest_metadata_update_at)}</td>
                </tr>
                <tr>
                  <td data-label="Metric">Partial Tickers</td>
                  <td data-label="Value">{formatCount(db.partial_ticker_count)}</td>
                </tr>
                <tr>
                  <td data-label="Metric">Missing Tickers</td>
                  <td data-label="Value">{formatCount(db.missing_ticker_count)}</td>
                </tr>
                <tr>
                  <td data-label="Metric">Stale Tickers</td>
                  <td data-label="Value">{formatCount(db.stale_ticker_count)}</td>
                </tr>
              </tbody>
            </table>
          </div>

          {db.notes.length > 0 ? <div className="panel-copy">{db.notes.join(" ")}</div> : null}

          <div className="admin-sample-grid">
            {db.sample_missing_tickers.length > 0 ? (
              <div>
                <div className="eyebrow">Sample Missing Tickers</div>
                <div className="pill-list">
                  {db.sample_missing_tickers.map((item) => (
                    <button key={item.ticker} className="symbol-pill symbol-pill-button" type="button" onClick={() => void handleInspectGap(item.ticker)}>
                      {item.ticker}
                    </button>
                  ))}
                </div>
              </div>
            ) : null}

            {db.sample_partial_tickers.length > 0 ? (
              <div>
                <div className="eyebrow">Sample Partial Tickers</div>
                <div className="pill-list">
                  {db.sample_partial_tickers.map((item) => (
                    <button key={item.ticker} className="symbol-pill symbol-pill-button" type="button" onClick={() => void handleInspectGap(item.ticker)}>
                      {item.ticker}
                    </button>
                  ))}
                </div>
              </div>
            ) : null}
          </div>

          {isGapLoading ? <LoadingBlock label={`Inspecting coverage gaps for ${selectedGapTicker}…`} compact /> : null}
          {gapError ? <p className="panel-copy">{gapError}</p> : null}
          {partialDetail ? (
            <div className="detail-card">
              <div className="detail-card-head">
                <div>
                  <div className="eyebrow">Missing Coverage Detail</div>
                  <div className="ticker-symbol">{partialDetail.ticker}</div>
                </div>
                <div className="detail-card-metrics">
                  <span className="file-meta">Bars {formatCount(partialDetail.bar_count)}</span>
                  <span className="file-meta">Missing dates {formatCount(partialDetail.missing_date_count)}</span>
                </div>
              </div>
              <div className="detail-grid">
                <div>
                  <div className="eyebrow">Coverage Window</div>
                  <div className="panel-copy">
                    {formatLocalDate(partialDetail.coverage_start)} to {formatLocalDate(partialDetail.coverage_end)}
                  </div>
                </div>
                <div>
                  <div className="eyebrow">First / Last Trade Date</div>
                  <div className="panel-copy">
                    {partialDetail.first_trade_date ? formatLocalDate(partialDetail.first_trade_date) : "-"} /{" "}
                    {partialDetail.last_trade_date ? formatLocalDate(partialDetail.last_trade_date) : "-"}
                  </div>
                </div>
              </div>
              <div className="detail-subsection">
                <div className="eyebrow">Missing Ranges</div>
                <div className="range-list">
                  {partialDetail.missing_ranges.map((range) => (
                    <article key={`${range.start}-${range.end}`} className="range-item">
                      <strong>{formatLocalDate(range.start)}</strong>
                      <span className="panel-copy">
                        to {formatLocalDate(range.end)} · {formatCount(range.days)} days
                      </span>
                    </article>
                  ))}
                </div>
              </div>
              {partialDetail.sample_missing_dates.length > 0 ? (
                <div className="detail-subsection">
                  <div className="eyebrow">Sample Missing Dates</div>
                  <div className="pill-list">
                    {partialDetail.sample_missing_dates.map((item) => (
                      <span key={item} className="symbol-pill">
                        {item}
                      </span>
                    ))}
                  </div>
                </div>
              ) : null}
            </div>
          ) : null}
        </div>
      </Panel>

      <Panel title="Fetch Market History" aside={<span className="eyebrow">Launches background sync job</span>}>
        <form className="run-toolbar" onSubmit={(event) => void handleLaunchSync(event)}>
          <div className="run-params-grid">
            <label className="field">
              <span>Start Date</span>
              <input type="date" value={syncStartDate} onChange={(event) => setSyncStartDate(event.target.value)} required />
            </label>
            <label className="field">
              <span>End Date</span>
              <input type="date" value={syncEndDate} onChange={(event) => setSyncEndDate(event.target.value)} />
            </label>
            <label className="field">
              <span>Chunk Size</span>
              <input type="number" min="1" max="500" value={chunkSize} onChange={(event) => setChunkSize(event.target.value)} />
            </label>
          </div>
          <label className="field">
            <span>Selected Tickers</span>
            <textarea
              value={syncTickers}
              onChange={(event) => setSyncTickers(event.target.value)}
              rows={3}
              placeholder="Leave blank for all tickers. Example: AAPL NVDA CRWD"
            />
          </label>
          <div className="run-action-footer">
            <button className="primary-button" type="submit" disabled={isLaunching}>
              {isLaunching ? "Launching..." : "Fetch History"}
            </button>
            <span className="panel-copy">
              Leave end date blank for up-to-today fetch. Leave tickers blank for whole configured universe.
            </span>
          </div>
          {launchMessage ? <div className="panel-copy">{launchMessage}</div> : null}
        </form>
      </Panel>

      <Panel title="Users and Roles" aside={<span className="eyebrow">{users.length} accounts</span>}>
        <form className="run-toolbar" onSubmit={(event) => void handleInviteUser(event)}>
          <div className="run-params-grid">
            <label className="field">
              <span>Email</span>
              <input type="email" value={inviteEmail} onChange={(event) => setInviteEmail(event.target.value)} placeholder="user@example.com" />
            </label>
            <label className="field">
              <span>Role</span>
              <select value={inviteRole} onChange={(event) => setInviteRole(event.target.value as RoleName)}>
                <option value="visitor">visitor</option>
                <option value="premium">premium</option>
                <option value="admin">admin</option>
              </select>
            </label>
          </div>
          <div className="run-action-footer">
            <button className="primary-button" type="submit" disabled={isSavingUser}>
              {isSavingUser ? "Saving..." : "Add or Update User"}
            </button>
            <span className="panel-copy">Use Google-account email here. Added users can sign in immediately with same Google email.</span>
          </div>
        </form>

        <div className="data-table-responsive">
          <table className="data-table">
            <thead>
              <tr>
                <th>Email</th>
                <th>Role</th>
                <th>Status</th>
                <th>Last Login</th>
                <th>Actions</th>
              </tr>
            </thead>
            <tbody>
              {users.map((user) => (
                <tr key={user.id}>
                  <td>{user.email}</td>
                  <td>
                    <select value={user.role} onChange={(event) => void handleRoleChange(user.id, event.target.value as RoleName)} disabled={isSavingUser}>
                      <option value="visitor">visitor</option>
                      <option value="premium">premium</option>
                      <option value="admin">admin</option>
                    </select>
                  </td>
                  <td>{user.is_active ? "active" : "inactive"}</td>
                  <td>{formatLocalDateTime(user.last_login_at)}</td>
                  <td>
                    <button className="table-action-button" type="button" disabled={isSavingUser} onClick={() => void handleToggleUser(user.id, user.is_active)}>
                      {user.is_active ? "Deactivate" : "Reactivate"}
                    </button>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </Panel>

      <Panel title="Premium Access Requests" aside={<span className="eyebrow">{accessRequests.filter((item) => item.status === "pending").length} pending</span>}>
        <div className="data-table-responsive">
          <table className="data-table">
            <thead>
              <tr>
                <th>Email</th>
                <th>Requested Role</th>
                <th>Status</th>
                <th>Requested</th>
                <th>Reviewed</th>
                <th>Actions</th>
              </tr>
            </thead>
            <tbody>
              {accessRequests.length === 0 ? (
                <tr>
                  <td colSpan={6}>No access requests yet.</td>
                </tr>
              ) : (
                accessRequests.map((item) => (
                  <tr key={item.id}>
                    <td>{item.email}</td>
                    <td>{item.requested_role}</td>
                    <td>{item.status}</td>
                    <td>{formatLocalDateTime(item.requested_at)}</td>
                    <td>{item.reviewed_at ? `${formatLocalDateTime(item.reviewed_at)}${item.reviewed_by_email ? ` · ${item.reviewed_by_email}` : ""}` : "-"}</td>
                    <td>
                      {item.status === "pending" ? (
                        <div className="button-row">
                          <button className="table-action-button" type="button" disabled={isSavingUser} onClick={() => void handleApproveAccessRequest(item.id)}>
                            Approve
                          </button>
                          <button className="table-action-button" type="button" disabled={isSavingUser} onClick={() => void handleDenyAccessRequest(item.id)}>
                            Deny
                          </button>
                        </div>
                      ) : (
                        <span className="file-meta">{item.status === "approved" ? `Granted${item.invited_user_email ? ` · ${item.invited_user_email}` : ""}` : item.deny_reason || "Closed"}</span>
                      )}
                    </td>
                  </tr>
                ))
              )}
            </tbody>
          </table>
        </div>
      </Panel>

      <Panel title="Audit Trail" aside={<span className="eyebrow">{auditEvents.length} events</span>}>
        <form className="run-toolbar" onSubmit={handleAuditFilterSubmit}>
          <div className="run-params-grid">
            <label className="field">
              <span>Actor Email</span>
              <input type="text" value={auditActorEmail} onChange={(event) => setAuditActorEmail(event.target.value)} placeholder="admin@example.com" />
            </label>
            <label className="field">
              <span>Action</span>
              <input type="text" value={auditAction} onChange={(event) => setAuditAction(event.target.value)} placeholder="admin.user.invite" />
            </label>
            <label className="field">
              <span>Resource Type</span>
              <input type="text" value={auditResourceType} onChange={(event) => setAuditResourceType(event.target.value)} placeholder="user" />
            </label>
            <label className="field">
              <span>From</span>
              <input type="date" value={auditFromDate} onChange={(event) => setAuditFromDate(event.target.value)} />
            </label>
            <label className="field">
              <span>To</span>
              <input type="date" value={auditToDate} onChange={(event) => setAuditToDate(event.target.value)} />
            </label>
          </div>
          <div className="run-action-footer">
            <button className="primary-button" type="submit" disabled={isLoadingAudit}>
              {isLoadingAudit ? "Loading..." : "Refresh Audit"}
            </button>
          </div>
        </form>
        <div className="data-table-responsive">
          <table className="data-table">
            <thead>
              <tr>
                <th>When</th>
                <th>Actor</th>
                <th>Action</th>
                <th>Target</th>
                <th>Message</th>
              </tr>
            </thead>
            <tbody>
              {auditEvents.length === 0 ? (
                <tr>
                  <td colSpan={5}>{isLoadingAudit ? "Loading audit events..." : "No audit events found."}</td>
                </tr>
              ) : (
                auditEvents.map((item) => (
                  <tr key={item.id}>
                    <td>{formatLocalDateTime(item.event_at)}</td>
                    <td>{item.actor_email || item.actor_role || "-"}</td>
                    <td>{item.action}</td>
                    <td>{item.resource_label || item.resource_id || item.resource_type}</td>
                    <td>{item.message}</td>
                  </tr>
                ))
              )}
            </tbody>
          </table>
        </div>
      </Panel>

      <Panel title="Exclusions" aside={<span className="eyebrow">{payload.excluded_count} symbols</span>}>
        <div className="run-toolbar">
          <label className="field">
            <span>Filter exclusions</span>
            <input
              type="text"
              value={exclusionFilter}
              onChange={(event) => setExclusionFilter(event.target.value)}
              placeholder="Ticker, reason, source"
            />
          </label>
          {notice ? <p className="panel-copy">{notice}</p> : null}
          <div className="exclusion-grid">
            {filteredExclusions.map((entry) => (
              <article key={entry.ticker} className="exclusion-card">
                <div className="detail-card-head">
                  <div>
                    <div className="ticker-symbol">{entry.ticker}</div>
                    <div className="file-meta">{entry.sources.join(" · ")}</div>
                  </div>
                  {entry.removable ? (
                    <button className="ghost-button" type="button" onClick={() => setSelectedExclusion(entry)}>
                      Remove
                    </button>
                  ) : (
                    <span className="eyebrow">Read only</span>
                  )}
                </div>
                <p className="panel-copy">{entry.reason || "No reason recorded."}</p>
                {entry.reasons.length > 1 ? (
                  <div className="pill-list">
                    {entry.reasons.map((reason) => (
                      <span key={reason} className="symbol-pill">
                        {reason}
                      </span>
                    ))}
                  </div>
                ) : null}
              </article>
            ))}
          </div>
        </div>
      </Panel>

      <ExclusionDialog
        isOpen={selectedExclusion != null}
        mode="remove"
        ticker={selectedExclusion?.ticker ?? ""}
        title={selectedExclusion ? `Remove ${selectedExclusion.ticker} from exclusions` : "Remove exclusion"}
        confirmLabel="Remove Exclusion"
        helperText="This removes the ticker from user-editable exclusion files and records your removal reason in the audit log."
        submitting={isRemoving}
        onClose={() => setSelectedExclusion(null)}
        onSubmit={handleRemoveExclusion}
      />
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
