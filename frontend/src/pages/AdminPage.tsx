import { useEffect, useMemo, useState, type FormEvent } from "react";
import { ExclusionDialog } from "../components/ExclusionDialog";
import { LoadingBlock } from "../components/LoadingBlock";
import { Panel } from "../components/Panel";
import { StatusPill } from "../components/StatusPill";
import { fetchJson } from "../lib/api";
import { formatCount, formatLocalDate, formatLocalDateTime } from "../lib/format";
import type { AccessRequestSummary, AdminResponse, AuditEventSummary, AuditEventsResponse, ExclusionEntry, PartialTickerDetailResponse, RoleName, ScheduledJobConfig, ScheduledJobConfigResponse, ScheduledJobSummary } from "../lib/types";

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

export function AdminPage() {
  const [payload, setPayload] = useState<AdminResponse>(EMPTY_ADMIN_RESPONSE);
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
  const [scheduledJobs, setScheduledJobs] = useState<ScheduledJobSummary[]>([]);
  const [isLoadingScheduledJobs, setIsLoadingScheduledJobs] = useState(true);
  const [scheduledConfigs, setScheduledConfigs] = useState<ScheduledJobConfig[]>([]);
  const [availableScheduledActions, setAvailableScheduledActions] = useState<
    Array<{
      id: string;
      label: string;
      fields: Array<{
        id: string;
        label: string;
        type: "text" | "number" | "date" | "select" | "multiselect";
        placeholder?: string | null;
        help_text?: string | null;
        options: Array<{ value: string; label: string }>;
      }>;
    }>
  >([]);
  const [commonTimezones, setCommonTimezones] = useState<string[]>([]);
  const [schedulerCommand, setSchedulerCommand] = useState("");
  const [isLoadingScheduleConfig, setIsLoadingScheduleConfig] = useState(true);
  const [scheduleJobId, setScheduleJobId] = useState("");
  const [scheduleJobLabel, setScheduleJobLabel] = useState("");
  const [scheduleActionId, setScheduleActionId] = useState("weekly_rs");
  const [scheduleCronExpr, setScheduleCronExpr] = useState("30 16 * * 1-5");
  const [scheduleCronTz, setScheduleCronTz] = useState("America/New_York");
  const [scheduleEnabled, setScheduleEnabled] = useState(true);
  const [scheduleOptionsJson, setScheduleOptionsJson] = useState("{}");
  const [lastSuggestedOptionsJson, setLastSuggestedOptionsJson] = useState("{}");
  const [isSavingSchedule, setIsSavingSchedule] = useState(false);

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

  const loadScheduledJobs = () => {
    setIsLoadingScheduledJobs(true);
    void fetchJson<{ jobs: ScheduledJobSummary[] }>("/api/admin/scheduled-jobs")
      .then((result) => setScheduledJobs(result.jobs))
      .catch(() => setScheduledJobs([]))
      .finally(() => setIsLoadingScheduledJobs(false));
  };

  const loadScheduleConfig = () => {
    setIsLoadingScheduleConfig(true);
    void fetchJson<ScheduledJobConfigResponse>("/api/admin/schedules")
      .then((result) => {
        setScheduledConfigs(result.jobs);
        setAvailableScheduledActions(result.available_actions);
        setCommonTimezones(result.common_timezones);
        setSchedulerCommand(result.scheduler_command);
        if (!result.available_actions.find((item) => item.id === scheduleActionId) && result.available_actions[0]) {
          setScheduleActionId(result.available_actions[0].id);
        }
      })
      .catch(() => {
        setScheduledConfigs([]);
        setAvailableScheduledActions([]);
        setCommonTimezones([]);
        setSchedulerCommand("");
      })
      .finally(() => setIsLoadingScheduleConfig(false));
  };

  useEffect(() => {
    loadAdmin(coverageStart);
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
    loadScheduledJobs();
  }, []);

  useEffect(() => {
    loadScheduleConfig();
  }, []);

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

  const db = payload.database_status;

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
      setNotice("User saved and sign-in email sent.");
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
      setNotice("Access request approved and sign-in email sent.");
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

  const resetScheduleForm = () => {
    const nextActionId = availableScheduledActions[0]?.id ?? "weekly_rs";
    setScheduleJobId("");
    setScheduleJobLabel("");
    setScheduleActionId(nextActionId);
    setScheduleCronExpr("30 16 * * 1-5");
    setScheduleCronTz(commonTimezones[0] ?? "America/New_York");
    setScheduleEnabled(true);
    const nextSuggested = buildScheduleOptionsTemplate(nextActionId);
    setScheduleOptionsJson(nextSuggested);
    setLastSuggestedOptionsJson(nextSuggested);
  };

  const handleSaveSchedule = async (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    setIsSavingSchedule(true);
    setNotice("");
    try {
      const parsedOptions = JSON.parse(scheduleOptionsJson || "{}") as Record<string, unknown>;
      await fetchJson<{ ok: boolean }>("/api/admin/schedules", {
        method: "POST",
        body: JSON.stringify({
          job_id: scheduleJobId,
          job_label: scheduleJobLabel,
          action_id: scheduleActionId,
          cron_expr: scheduleCronExpr,
          cron_tz: scheduleCronTz,
          enabled: scheduleEnabled,
          options: parsedOptions,
        }),
      });
      setNotice("Scheduled job saved.");
      loadScheduleConfig();
      resetScheduleForm();
    } catch (error) {
      setNotice(error instanceof Error ? error.message : "Failed to save scheduled job.");
    } finally {
      setIsSavingSchedule(false);
    }
  };

  const handleEditSchedule = (job: ScheduledJobConfig) => {
    setScheduleJobId(job.job_id);
    setScheduleJobLabel(job.job_label);
    setScheduleActionId(job.action_id);
    setScheduleCronExpr(job.cron_expr);
    setScheduleCronTz(job.cron_tz);
    setScheduleEnabled(job.enabled);
    const serialized = JSON.stringify(job.options ?? {}, null, 2);
    setScheduleOptionsJson(serialized);
    setLastSuggestedOptionsJson(serialized);
  };

  const handleDeleteSchedule = async (jobId: string) => {
    setIsSavingSchedule(true);
    setNotice("");
    try {
      await fetchJson<{ ok: boolean }>(`/api/admin/schedules/${jobId}/delete`, { method: "POST" });
      setNotice("Scheduled job deleted.");
      loadScheduleConfig();
      if (scheduleJobId === jobId) {
        resetScheduleForm();
      }
    } catch (error) {
      setNotice(error instanceof Error ? error.message : "Failed to delete scheduled job.");
    } finally {
      setIsSavingSchedule(false);
    }
  };

  const renderScheduledJobStatus = (status: string) => {
    if (status === "running" || status === "success" || status === "failed") {
      return <StatusPill status={status} />;
    }
    return <span className="status-pill status-unknown">{status || "unknown"}</span>;
  };

  const selectedScheduledAction = useMemo(
    () => availableScheduledActions.find((item) => item.id === scheduleActionId) ?? null,
    [availableScheduledActions, scheduleActionId],
  );

  const suggestedScheduleOptionsJson = useMemo(
    () => buildScheduleOptionsTemplate(scheduleActionId),
    [scheduleActionId],
  );

  useEffect(() => {
    const trimmed = scheduleOptionsJson.trim();
    if (trimmed === "" || trimmed === "{}" || trimmed === lastSuggestedOptionsJson.trim()) {
      setScheduleOptionsJson(suggestedScheduleOptionsJson);
    }
    setLastSuggestedOptionsJson(suggestedScheduleOptionsJson);
  }, [lastSuggestedOptionsJson, scheduleActionId, scheduleOptionsJson, suggestedScheduleOptionsJson]);

  return (
    <div className="page-grid">
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

      <Panel title="Scheduled Jobs" aside={<span className="eyebrow">{scheduledJobs.length} tracked</span>}>
        <div className="run-toolbar">
          <div className="run-action-footer">
            <button className="primary-button" type="button" onClick={loadScheduledJobs} disabled={isLoadingScheduledJobs}>
              {isLoadingScheduledJobs ? "Loading..." : "Refresh Scheduled Status"}
            </button>
            <span className="panel-copy">Reads JSON status files under the app artifacts directory. Host cron remains source of truth.</span>
          </div>
          {isLoadingScheduledJobs ? <LoadingBlock label="Loading scheduled job status…" compact /> : null}
          <div className="data-table-responsive">
            <table className="data-table">
              <thead>
                <tr>
                  <th>Job</th>
                  <th>Status</th>
                  <th>Last Start</th>
                  <th>Last Finish</th>
                  <th>Exit</th>
                  <th>Log</th>
                  <th>Artifact</th>
                </tr>
              </thead>
              <tbody>
                {scheduledJobs.length === 0 ? (
                  <tr>
                    <td colSpan={7}>{isLoadingScheduledJobs ? "Loading scheduled jobs..." : "No scheduled job status files found."}</td>
                  </tr>
                ) : (
                  scheduledJobs.map((job) => (
                    <tr key={job.job_id}>
                      <td data-label="Job">
                        <div className="admin-job-cell">
                          <strong>{job.job_label}</strong>
                          <span className="file-meta">{job.job_id}</span>
                          {job.message ? <span className="panel-copy">{job.message}</span> : null}
                        </div>
                      </td>
                      <td data-label="Status">{renderScheduledJobStatus(job.status)}</td>
                      <td data-label="Last Start">{formatLocalDateTime(job.last_started_at)}</td>
                      <td data-label="Last Finish">{formatLocalDateTime(job.last_finished_at)}</td>
                      <td data-label="Exit">{job.exit_code ?? "-"}</td>
                      <td data-label="Log">{job.log_file || "-"}</td>
                      <td data-label="Artifact">{job.artifact_file || "-"}</td>
                    </tr>
                  ))
                )}
              </tbody>
            </table>
          </div>
        </div>
      </Panel>

      <Panel title="Scheduler Config" aside={<span className="eyebrow">{scheduledConfigs.length} schedules</span>}>
        <div className="run-toolbar">
          <form className="run-toolbar" onSubmit={(event) => void handleSaveSchedule(event)}>
            <div className="run-params-grid">
              <label className="field">
                <span>Job ID</span>
                <input type="text" value={scheduleJobId} onChange={(event) => setScheduleJobId(event.target.value)} placeholder="weekly_rs_close" required />
              </label>
              <label className="field">
                <span>Job Label</span>
                <input type="text" value={scheduleJobLabel} onChange={(event) => setScheduleJobLabel(event.target.value)} placeholder="Weekly RS After Close" required />
              </label>
              <label className="field">
                <span>Screener</span>
                <select value={scheduleActionId} onChange={(event) => setScheduleActionId(event.target.value)}>
                  {availableScheduledActions.map((item) => (
                    <option key={item.id} value={item.id}>
                      {item.label}
                    </option>
                  ))}
                </select>
              </label>
              <label className="field">
                <span>Cron Expr</span>
                <input type="text" value={scheduleCronExpr} onChange={(event) => setScheduleCronExpr(event.target.value)} placeholder="30 16 * * 1-5" required />
              </label>
              <label className="field">
                <span>Timezone</span>
                <select value={scheduleCronTz} onChange={(event) => setScheduleCronTz(event.target.value)}>
                  {commonTimezones.map((item) => (
                    <option key={item} value={item}>
                      {item}
                    </option>
                  ))}
                </select>
              </label>
              <label className="field">
                <span>Enabled</span>
                <select value={scheduleEnabled ? "true" : "false"} onChange={(event) => setScheduleEnabled(event.target.value === "true")}>
                  <option value="true">enabled</option>
                  <option value="false">disabled</option>
                </select>
              </label>
              <label className="field" style={{ gridColumn: "1 / -1" }}>
                <span>Action Options JSON</span>
                <textarea
                  value={scheduleOptionsJson}
                  onChange={(event) => setScheduleOptionsJson(event.target.value)}
                  rows={8}
                  placeholder='{"reference_date":"{{local_date}}"}'
                />
              </label>
            </div>
            <div className="run-action-footer">
              <button className="primary-button" type="submit" disabled={isSavingSchedule || isLoadingScheduleConfig}>
                {isSavingSchedule ? "Saving..." : "Save Schedule"}
              </button>
              <button className="ghost-button" type="button" onClick={resetScheduleForm} disabled={isSavingSchedule}>
                Clear
              </button>
              <span className="panel-copy">Host cron should run scheduler command every 5 minutes: {schedulerCommand || "-"}</span>
            </div>
            <p className="panel-copy">
              Supported schedule date templates: <code>{'{{local_date}}'}</code>, <code>{'{{local_date_plus_7}}'}</code>, <code>{'{{local_date_plus_14}}'}</code>.
            </p>
            <p className="panel-copy">
              Suggested options for this action:
            </p>
            <pre className="panel-copy"><code>{suggestedScheduleOptionsJson}</code></pre>
            {selectedScheduledAction?.fields?.length ? (
              <p className="panel-copy">
                Action fields: {selectedScheduledAction.fields.map((field) => field.id).join(", ")}
              </p>
            ) : null}
          </form>
          {isLoadingScheduleConfig ? <LoadingBlock label="Loading scheduler config…" compact /> : null}
          <div className="data-table-responsive">
            <table className="data-table">
              <thead>
                <tr>
                  <th>Job</th>
                  <th>Screener</th>
                  <th>Cron</th>
                  <th>TZ</th>
                  <th>Options</th>
                  <th>Enabled</th>
                  <th>Actions</th>
                </tr>
              </thead>
              <tbody>
                {scheduledConfigs.length === 0 ? (
                  <tr>
                    <td colSpan={7}>{isLoadingScheduleConfig ? "Loading schedules..." : "No scheduled jobs configured yet."}</td>
                  </tr>
                ) : (
                  scheduledConfigs.map((job) => (
                    <tr key={job.job_id}>
                      <td data-label="Job">
                        <div className="admin-job-cell">
                          <strong>{job.job_label}</strong>
                          <span className="file-meta">{job.job_id}</span>
                        </div>
                      </td>
                      <td data-label="Screener">{job.action_id}</td>
                      <td data-label="Cron">{job.cron_expr}</td>
                      <td data-label="TZ">{job.cron_tz}</td>
                      <td data-label="Options">
                        <code>{JSON.stringify(job.options ?? {})}</code>
                      </td>
                      <td data-label="Enabled">{job.enabled ? "Yes" : "No"}</td>
                      <td data-label="Actions">
                        <div className="button-row">
                          <button className="table-action-button" type="button" disabled={isSavingSchedule} onClick={() => handleEditSchedule(job)}>
                            Edit
                          </button>
                          <button className="table-action-button" type="button" disabled={isSavingSchedule} onClick={() => void handleDeleteSchedule(job.job_id)}>
                            Delete
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
              {isSavingUser ? "Saving..." : "Invite or Create User"}
            </button>
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

function buildScheduleOptionsTemplate(actionId: string): string {
  const sharedUniverseTemplate = {
    market_data_source: "database-first",
    filter_precedence: "exclude",
  };

  if (actionId === "earnings_weekly_criteria") {
    return JSON.stringify({ reference_date: "{{local_date}}" }, null, 2);
  }
  if (actionId === "legacy_peg" || actionId === "sean_peg") {
    return JSON.stringify(
      {
        source: "earnings-watchlist",
        reference_date: "{{local_date}}",
        market_data_source: "database-first",
      },
      null,
      2,
    );
  }
  if (actionId === "screener_history_batch") {
    return JSON.stringify(
      {
        strategy_ids: ["rs", "vcp"],
        start_date: "{{local_date_plus_14}}",
        end_date: "{{local_date}}",
        market_data_source: "database-first",
        overwrite_policy: "skip_existing",
        scope: {},
      },
      null,
      2,
    );
  }
  if (actionId === "backtest_v1") {
    return JSON.stringify(
      {
        entry_rule: {
          mode: "min_count_same_day",
          screener_ids: ["rs", "vcp"],
          min_count: 2,
        },
        date_range: {
          start_date: "2026-01-01",
          end_date: "{{local_date}}",
        },
        exit_rules: [],
        position_rules: {},
        signal_cache_policy: "reuse_then_fill",
        market_data_mode: "database_only",
      },
      null,
      2,
    );
  }
  return JSON.stringify(sharedUniverseTemplate, null, 2);
}
