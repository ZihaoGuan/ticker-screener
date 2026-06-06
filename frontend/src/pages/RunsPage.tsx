import { useEffect, useMemo, useState, type FormEvent } from "react";
import { Link } from "react-router-dom";
import { useAuth } from "../auth/AuthContext";
import { LoadingBlock } from "../components/LoadingBlock";
import { Panel } from "../components/Panel";
import { ProgressBar } from "../components/ProgressBar";
import { ScreenerConfigModal } from "../components/ScreenerConfigModal";
import { StatusPill } from "../components/StatusPill";
import { fetchJson } from "../lib/api";
import { formatLocalDateTime } from "../lib/format";
import type {
  BacktestRunDetailV1,
  BacktestRunsResponseV1,
  JobsResponse,
  OverlapWarmCoverageResponse,
  ScheduledJobConfig,
  ScheduledJobConfigResponse,
  ScheduledJobSummary,
} from "../lib/types";
import "./RunsPage.css";

export function RunsPage() {
  const auth = useAuth();
  const canManageSchedules = auth.hasCapability("manage_exclusions");
  const [payload, setPayload] = useState<JobsResponse | null>(null);
  const [selectedActionId, setSelectedActionId] = useState("");
  const [selectedJobId, setSelectedJobId] = useState("");
  const [isModalOpen, setIsModalOpen] = useState(false);
  const [isRunning, setIsRunning] = useState(false);
  const [isCancellingJobId, setIsCancellingJobId] = useState("");
  const [isLoading, setIsLoading] = useState(true);
  const [hasError, setHasError] = useState(false);
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
  const [maxParallelJobs, setMaxParallelJobs] = useState("5");
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
  const [isSavingScheduleSettings, setIsSavingScheduleSettings] = useState(false);
  const [scheduleNotice, setScheduleNotice] = useState("");
  const [warmStrategyIds, setWarmStrategyIds] = useState("");
  const [warmFrom, setWarmFrom] = useState(isoDateDaysAgo(20));
  const [warmTo, setWarmTo] = useState(isoDateDaysAgo(1));
  const [warmThreshold, setWarmThreshold] = useState("4");
  const [coveragePayload, setCoveragePayload] = useState<OverlapWarmCoverageResponse | null>(null);
  const [isLoadingCoverage, setIsLoadingCoverage] = useState(false);
  const [coverageNotice, setCoverageNotice] = useState("");
  const [backtestsPayload, setBacktestsPayload] = useState<BacktestRunsResponseV1 | null>(null);
  const [isLoadingBacktests, setIsLoadingBacktests] = useState(false);
  const [selectedBacktestId, setSelectedBacktestId] = useState<number | null>(null);
  const [selectedBacktest, setSelectedBacktest] = useState<BacktestRunDetailV1 | null>(null);
  const [isLoadingBacktestDetail, setIsLoadingBacktestDetail] = useState(false);

  const refresh = () => {
    void fetchJson<JobsResponse>("/api/jobs")
      .then((nextPayload) => {
        setPayload(nextPayload);
        setHasError(false);
      })
      .catch(() => {
        setPayload({ actions: [], jobs: [] });
        setHasError(true);
      })
      .finally(() => setIsLoading(false));
  };

  const loadScheduledJobs = () => {
    if (!canManageSchedules) {
      setScheduledJobs([]);
      setIsLoadingScheduledJobs(false);
      return;
    }
    setIsLoadingScheduledJobs(true);
    void fetchJson<{ jobs: ScheduledJobSummary[] }>("/api/admin/scheduled-jobs")
      .then((result) => setScheduledJobs(result.jobs))
      .catch(() => setScheduledJobs([]))
      .finally(() => setIsLoadingScheduledJobs(false));
  };

  const loadScheduleConfig = () => {
    if (!canManageSchedules) {
      setScheduledConfigs([]);
      setAvailableScheduledActions([]);
      setCommonTimezones([]);
      setSchedulerCommand("");
      setMaxParallelJobs("5");
      setIsLoadingScheduleConfig(false);
      return;
    }
    setIsLoadingScheduleConfig(true);
    void fetchJson<ScheduledJobConfigResponse>("/api/admin/schedules")
      .then((result) => {
        setScheduledConfigs(result.jobs);
        setAvailableScheduledActions(result.available_actions);
        setCommonTimezones(result.common_timezones);
        setSchedulerCommand(result.scheduler_command);
        setMaxParallelJobs(String(result.max_parallel_jobs ?? 5));
        if (!result.available_actions.find((item) => item.id === scheduleActionId) && result.available_actions[0]) {
          setScheduleActionId(result.available_actions[0].id);
        }
      })
      .catch(() => {
        setScheduledConfigs([]);
        setAvailableScheduledActions([]);
        setCommonTimezones([]);
        setSchedulerCommand("");
        setMaxParallelJobs("5");
      })
      .finally(() => setIsLoadingScheduleConfig(false));
  };

  useEffect(() => {
    refresh();
    const timer = window.setInterval(refresh, 4000);
    return () => window.clearInterval(timer);
  }, []);

  useEffect(() => {
    loadScheduledJobs();
    loadScheduleConfig();
  }, [canManageSchedules]);

  useEffect(() => {
    if (!payload?.actions?.length) {
      return;
    }
    setSelectedActionId((current) => current || payload.actions[0].id);
    setWarmStrategyIds((current) => {
      if (current.trim()) {
        return current;
      }
      return payload.actions
        .filter((item) => !["signal_warm_batch", "overlap_backtest_v1"].includes(item.id))
        .slice(0, 4)
        .map((item) => item.id)
        .join(",");
    });
  }, [payload]);

  useEffect(() => {
    if (!payload?.jobs?.length) {
      setSelectedJobId("");
      return;
    }
    setSelectedJobId((current) => {
      if (current && payload.jobs.some((job) => job.job_id === current)) {
        return current;
      }
      return payload.jobs[0].job_id;
    });
  }, [payload]);

  const activeJob = useMemo(() => payload?.jobs.find((job) => job.status === "running") ?? null, [payload]);
  const selectedJob = useMemo(
    () => payload?.jobs.find((job) => job.job_id === selectedJobId) ?? payload?.jobs[0] ?? null,
    [payload, selectedJobId],
  );
  const selectedAction = useMemo(
    () => payload?.actions.find((action) => action.id === selectedActionId) ?? payload?.actions[0] ?? null,
    [payload, selectedActionId],
  );
  const selectedJobLog = useMemo(() => selectedJob?.log_tail ?? "No job log yet.", [selectedJob]);
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

  useEffect(() => {
    if (!warmStrategyIds.trim()) {
      setCoveragePayload(null);
      return;
    }
    const timer = window.setTimeout(() => {
      setIsLoadingCoverage(true);
      setCoverageNotice("");
      const query = new URLSearchParams({
        from: warmFrom,
        to: warmTo,
        strategyIds: warmStrategyIds,
        candidateThreshold: warmThreshold || "4",
      });
      void fetchJson<OverlapWarmCoverageResponse>(`/api/overlap-warm/coverage?${query.toString()}`)
        .then((result) => setCoveragePayload(result))
        .catch((error) => {
          setCoveragePayload(null);
          setCoverageNotice(error instanceof Error ? error.message : "Failed to load warm coverage.");
        })
        .finally(() => setIsLoadingCoverage(false));
    }, 200);
    return () => window.clearTimeout(timer);
  }, [warmFrom, warmStrategyIds, warmThreshold, warmTo]);

  useEffect(() => {
    setIsLoadingBacktests(true);
    void fetchJson<BacktestRunsResponseV1>("/api/backtests-v1")
      .then((result) => setBacktestsPayload(result))
      .catch(() => setBacktestsPayload({ configured: false, runs: [] }))
      .finally(() => setIsLoadingBacktests(false));
  }, [payload]);

  useEffect(() => {
    if (!backtestsPayload?.runs?.length) {
      setSelectedBacktestId(null);
      setSelectedBacktest(null);
      return;
    }
    setSelectedBacktestId((current) => {
      if (current != null && backtestsPayload.runs.some((item) => item.id === current)) {
        return current;
      }
      return backtestsPayload.runs[0].id;
    });
  }, [backtestsPayload]);

  useEffect(() => {
    if (selectedBacktestId == null) {
      setSelectedBacktest(null);
      return;
    }
    setIsLoadingBacktestDetail(true);
    void fetchJson<BacktestRunDetailV1>(`/api/backtests-v1/${selectedBacktestId}`)
      .then((result) => setSelectedBacktest(result))
      .catch(() => setSelectedBacktest(null))
      .finally(() => setIsLoadingBacktestDetail(false));
  }, [selectedBacktestId]);

  const handleRunAction = async (params: Record<string, string | string[]>, actionId = selectedActionId) => {
    setIsRunning(true);
    try {
      const body: Record<string, string | string[]> = {};
      for (const [key, value] of Object.entries(params)) {
        if (Array.isArray(value)) {
          if (value.length > 0) {
            body[key] = value;
          }
        } else if (value.trim()) {
          body[key] = value.trim();
        }
      }
      await fetchJson<{ ok: boolean; job_id: string }>(`/api/runs/${actionId}`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(body),
      });
      refresh();
    } finally {
      setIsRunning(false);
    }
  };

  const handleConfigureClick = (actionId: string) => {
    setSelectedActionId(actionId);
    setIsModalOpen(true);
  };

  const handleQuickRun = async (actionId: string) => {
    setSelectedActionId(actionId);
    await handleRunAction({}, actionId);
  };

  const handleCancelJob = async (jobId: string) => {
    setIsCancellingJobId(jobId);
    try {
      await fetchJson<{ ok: boolean; job: JobsResponse["jobs"][number] }>(`/api/jobs/${jobId}/cancel`, {
        method: "POST",
      });
      refresh();
    } finally {
      setIsCancellingJobId("");
    }
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
    setScheduleNotice("");
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
      setScheduleNotice("Scheduled job saved.");
      loadScheduleConfig();
      resetScheduleForm();
    } catch (error) {
      setScheduleNotice(error instanceof Error ? error.message : "Failed to save scheduled job.");
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
    setScheduleNotice("");
    try {
      await fetchJson<{ ok: boolean }>(`/api/admin/schedules/${jobId}/delete`, { method: "POST" });
      setScheduleNotice("Scheduled job deleted.");
      loadScheduleConfig();
      if (scheduleJobId === jobId) {
        resetScheduleForm();
      }
    } catch (error) {
      setScheduleNotice(error instanceof Error ? error.message : "Failed to delete scheduled job.");
    } finally {
      setIsSavingSchedule(false);
    }
  };

  const handleSaveScheduleSettings = async () => {
    setIsSavingScheduleSettings(true);
    setScheduleNotice("");
    try {
      const parsed = Number(maxParallelJobs);
      await fetchJson<{ ok: boolean; max_parallel_jobs: number }>("/api/admin/schedules/settings", {
        method: "POST",
        body: JSON.stringify({ max_parallel_jobs: parsed }),
      });
      setScheduleNotice("Scheduler settings saved.");
      loadScheduleConfig();
    } catch (error) {
      setScheduleNotice(error instanceof Error ? error.message : "Failed to save scheduler settings.");
    } finally {
      setIsSavingScheduleSettings(false);
    }
  };

  const renderScheduledJobStatus = (status: string) => {
    if (status === "queued" || status === "running" || status === "success" || status === "failed") {
      return <StatusPill status={status} />;
    }
    return <span className="status-pill status-unknown">{status || "unknown"}</span>;
  };

  const formatDuration = (seconds: number) => {
    if (!Number.isFinite(seconds) || seconds <= 0) {
      return "-";
    }
    const hours = Math.floor(seconds / 3600);
    const minutes = Math.floor((seconds % 3600) / 60);
    const remainingSeconds = seconds % 60;
    if (hours > 0) {
      return `${hours}h ${minutes}m ${remainingSeconds}s`;
    }
    if (minutes > 0) {
      return `${minutes}m ${remainingSeconds}s`;
    }
    return `${remainingSeconds}s`;
  };

  return (
    <>
      <div className="page-grid">
        <Panel title="Available Screeners">
          {isLoading && !payload ? <LoadingBlock label="Loading available screeners…" /> : null}
          <div className="screeners-grid">
            {(payload?.actions ?? []).map((action) => (
              <div key={action.id} className="screener-card">
                {(() => {
                  const configureOnly = ["signal_warm_batch", "overlap_backtest_v1"].includes(action.id);
                  return (
                    <>
                <div className="screener-card-header">
                  <h3>{action.label}</h3>
                </div>
                <p className="screener-description">
                  {configureOnly
                    ? "Open config to choose date range and screener set."
                    : action.fields.length > 0
                    ? "Run with defaults now, or open config for custom parameters."
                    : "Run immediately with the default screener settings."}
                </p>
                <div className="screener-card-actions">
                  {!configureOnly ? (
                    <button className="screener-run-button" onClick={() => void handleQuickRun(action.id)} type="button" disabled={isRunning}>
                      RUN DEFAULT
                    </button>
                  ) : null}
                  {action.fields.length > 0 ? (
                    <button className="screener-config-button" onClick={() => handleConfigureClick(action.id)} type="button" disabled={isRunning}>
                      {configureOnly ? "OPEN CONFIG" : "CONFIGURE"}
                    </button>
                  ) : null}
                </div>
                    </>
                  );
                })()}
              </div>
            ))}
          </div>
        </Panel>

        {canManageSchedules ? (
          <>
            <Panel title="Scheduled Screeners" aside={<span className="eyebrow">{scheduledJobs.length} tracked</span>}>
              {isLoadingScheduledJobs ? <LoadingBlock label="Loading scheduled job status…" compact /> : null}
              <div className="data-table-responsive">
                <table className="data-table">
                  <thead>
                    <tr>
                      <th>Job</th>
                      <th>Status</th>
                      <th>Last Start</th>
                      <th>Last Finish</th>
                      <th>Exit Code</th>
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
                            </div>
                          </td>
                          <td data-label="Status">{renderScheduledJobStatus(job.status)}</td>
                          <td data-label="Last Start">{formatLocalDateTime(job.last_started_at)}</td>
                          <td data-label="Last Finish">{formatLocalDateTime(job.last_finished_at)}</td>
                          <td data-label="Exit Code" className="mono">{job.exit_code ?? "-"}</td>
                          <td data-label="Log" className="file-meta">{job.log_file || "-"}</td>
                          <td data-label="Artifact" className="file-meta">{job.artifact_file || "-"}</td>
                        </tr>
                      ))
                    )}
                  </tbody>
                </table>
              </div>
            </Panel>

            <Panel title="Scheduler Config" aside={<span className="eyebrow">{scheduledConfigs.length} schedules</span>}>
              <div className="run-toolbar">
                <div className="run-params-grid">
                  <label className="field">
                    <span>Max Parallel Jobs</span>
                    <input type="number" min={1} max={20} value={maxParallelJobs} onChange={(event) => setMaxParallelJobs(event.target.value)} />
                  </label>
                </div>
                <div className="run-action-footer">
                  <button className="primary-button" type="button" disabled={isSavingScheduleSettings || isLoadingScheduleConfig} onClick={() => void handleSaveScheduleSettings()}>
                    {isSavingScheduleSettings ? "Saving..." : "Save Scheduler Settings"}
                  </button>
                  <span className="panel-copy">Host cron should run scheduler command every 5 minutes: {schedulerCommand || "-"}</span>
                </div>
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
                    {scheduleNotice ? <span className="panel-copy">{scheduleNotice}</span> : null}
                  </div>
                  <p className="panel-copy">
                    Supported schedule date templates: <code>{'{{local_date}}'}</code>, <code>{'{{local_date_plus_7}}'}</code>, <code>{'{{local_date_plus_14}}'}</code>.
                  </p>
                  <p className="panel-copy">Suggested options for this action:</p>
                  <pre className="panel-copy"><code>{suggestedScheduleOptionsJson}</code></pre>
                  {selectedScheduledAction?.fields?.length ? (
                    <p className="panel-copy">Action fields: {selectedScheduledAction.fields.map((field) => field.id).join(", ")}</p>
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
          </>
        ) : null}

        <Panel
          title="Current Progress"
          aside={activeJob ? <span className="eyebrow">{activeJob.label}</span> : <span className="eyebrow">Idle</span>}
        >
          <div className="run-progress-panel">
            <ProgressBar
              status={activeJob?.status ?? "cancelled"}
              label={
                activeJob
                  ? `${activeJob.label} · ${activeJob.progress_label || `started ${activeJob.started_at || "just now"}`} · ${activeJob.success_count} hits · ${formatDuration(activeJob.duration_seconds)}`
                  : "No screener currently running"
              }
              progress={activeJob?.progress_percent ?? null}
            />
            {activeJob ? (
              <div className="button-row">
                <button
                  className="secondary-button"
                  type="button"
                  onClick={() => void handleCancelJob(activeJob.job_id)}
                  disabled={isCancellingJobId === activeJob.job_id}
                >
                  {isCancellingJobId === activeJob.job_id ? "Stopping..." : "Stop Current Job"}
                </button>
              </div>
            ) : null}
          </div>
        </Panel>

        <Panel title="Warm Coverage" aside={<span className="eyebrow">{coveragePayload?.days.length ?? 0} dates</span>}>
          <div className="run-params-grid">
            <label className="field">
              <span>Screeners</span>
              <input type="text" value={warmStrategyIds} onChange={(event) => setWarmStrategyIds(event.target.value)} placeholder="rs,vcp,gap_fill,fearzone" />
            </label>
            <label className="field">
              <span>From</span>
              <input type="date" value={warmFrom} onChange={(event) => setWarmFrom(event.target.value)} />
            </label>
            <label className="field">
              <span>To</span>
              <input type="date" value={warmTo} onChange={(event) => setWarmTo(event.target.value)} />
            </label>
            <label className="field">
              <span>Threshold</span>
              <input type="number" min={2} max={20} value={warmThreshold} onChange={(event) => setWarmThreshold(event.target.value)} />
            </label>
          </div>
          {isLoadingCoverage ? <LoadingBlock label="Loading warm coverage…" compact /> : null}
          {coverageNotice ? <p className="panel-copy">{coverageNotice}</p> : null}
          <div className="data-table-responsive">
            <table className="data-table">
              <thead>
                <tr>
                  <th>Date</th>
                  <th>Screened</th>
                  <th>Overlap</th>
                  <th>4+</th>
                  <th>Missing</th>
                </tr>
              </thead>
              <tbody>
                {(coveragePayload?.days ?? []).map((day) => (
                  <tr key={day.date}>
                    <td data-label="Date">
                      <Link className="table-action-button table-link-button" to={`/report?date=${encodeURIComponent(day.date)}`}>
                        {day.date}
                      </Link>
                    </td>
                    <td data-label="Screened">
                      {day.screened_strategy_count}/{day.expected_strategy_count} · {day.screen_status}
                    </td>
                    <td data-label="Overlap">{day.overlap_ready ? "ready" : "pending"}</td>
                    <td data-label="4+">{day.overlap_four_plus_count}</td>
                    <td data-label="Missing">{day.missing_strategy_ids.join(", ") || "-"}</td>
                  </tr>
                ))}
                {!isLoadingCoverage && (coveragePayload?.days.length ?? 0) === 0 ? (
                  <tr>
                    <td colSpan={5}>No warm coverage rows yet.</td>
                  </tr>
                ) : null}
              </tbody>
            </table>
          </div>
        </Panel>

        <Panel title="Backtest V1" aside={<span className="eyebrow">{backtestsPayload?.runs.length ?? 0} runs</span>}>
          {isLoadingBacktests ? <LoadingBlock label="Loading backtests…" compact /> : null}
          <p className="panel-copy">Entry uses same-day close when signal count is at least four.</p>
          <div className="data-table-responsive">
            <table className="data-table">
              <thead>
                <tr>
                  <th>Created</th>
                  <th>Range</th>
                  <th>Screeners</th>
                  <th>Trades</th>
                  <th>5D Avg</th>
                  <th>10D Avg</th>
                </tr>
              </thead>
              <tbody>
                {(backtestsPayload?.runs ?? []).map((run) => {
                  const holds = run.summary?.holds ?? {};
                  const avg5d = holds["5"]?.avg_return_pct;
                  const avg10d = holds["10"]?.avg_return_pct;
                  return (
                    <tr key={run.id} className={run.id === selectedBacktestId ? "is-selected-row" : ""} onClick={() => setSelectedBacktestId(run.id)}>
                      <td data-label="Created">{formatLocalDateTime(run.created_at)}</td>
                      <td data-label="Range">{run.start_date} to {run.end_date}</td>
                      <td data-label="Screeners">{run.strategy_ids_json.join(", ")}</td>
                      <td data-label="Trades">{run.summary?.trade_count ?? 0}</td>
                      <td data-label="5D Avg">{avg5d == null ? "-" : `${avg5d.toFixed(2)}%`}</td>
                      <td data-label="10D Avg">{avg10d == null ? "-" : `${avg10d.toFixed(2)}%`}</td>
                    </tr>
                  );
                })}
                {!isLoadingBacktests && (backtestsPayload?.runs.length ?? 0) === 0 ? (
                  <tr>
                    <td colSpan={6}>No backtest runs yet.</td>
                  </tr>
                ) : null}
              </tbody>
            </table>
          </div>
          <div className="backtest-detail-grid">
            <article className="metric-card">
              <h3>Selected Run</h3>
              <div className="metric-value">{selectedBacktest?.id ?? "-"}</div>
              <p className="card-meta">{selectedBacktest?.strategy_ids_json?.join(", ") || "No run selected"}</p>
            </article>
            <article className="metric-card">
              <h3>Trades</h3>
              <div className="metric-value">{selectedBacktest?.summary?.trade_count ?? 0}</div>
              <p className="card-meta">Threshold {selectedBacktest?.entry_signal_threshold ?? 4}</p>
            </article>
            <article className="metric-card">
              <h3>5D Excess</h3>
              <div className="metric-value">
                {selectedBacktest?.summary?.holds?.["5"]?.avg_excess_return_pct == null ? "-" : `${selectedBacktest.summary.holds["5"].avg_excess_return_pct?.toFixed(2)}%`}
              </div>
            </article>
            <article className="metric-card">
              <h3>10D Excess</h3>
              <div className="metric-value">
                {selectedBacktest?.summary?.holds?.["10"]?.avg_excess_return_pct == null ? "-" : `${selectedBacktest.summary.holds["10"].avg_excess_return_pct?.toFixed(2)}%`}
              </div>
            </article>
          </div>
          {isLoadingBacktestDetail ? <LoadingBlock label="Loading backtest detail…" compact /> : null}
          <div className="data-table-responsive">
            <table className="data-table">
              <thead>
                <tr>
                  <th>Date</th>
                  <th>Ticker</th>
                  <th>Signals</th>
                  <th>5D</th>
                  <th>10D</th>
                </tr>
              </thead>
              <tbody>
                {(selectedBacktest?.trades ?? []).slice(0, 20).map((trade) => {
                  const hold5 = trade.hold_results_json["5"] as { return_pct?: number } | undefined;
                  const hold10 = trade.hold_results_json["10"] as { return_pct?: number } | undefined;
                  return (
                    <tr key={`${trade.signal_date}-${trade.ticker}`}>
                      <td data-label="Date">{trade.signal_date}</td>
                      <td data-label="Ticker">
                        <Link className="table-action-button table-link-button" to={`/charts?ticker=${encodeURIComponent(trade.ticker)}&date=${encodeURIComponent(trade.signal_date)}`}>
                          {trade.ticker}
                        </Link>
                      </td>
                      <td data-label="Signals">{trade.signal_count}</td>
                      <td data-label="5D">{hold5?.return_pct == null ? "-" : `${hold5.return_pct.toFixed(2)}%`}</td>
                      <td data-label="10D">{hold10?.return_pct == null ? "-" : `${hold10.return_pct.toFixed(2)}%`}</td>
                    </tr>
                  );
                })}
                {!isLoadingBacktestDetail && (selectedBacktest?.trades.length ?? 0) === 0 ? (
                  <tr>
                    <td colSpan={5}>No trade rows for selected run.</td>
                  </tr>
                ) : null}
              </tbody>
            </table>
          </div>
        </Panel>

        <Panel title="Recent Screener Jobs">
          {isLoading && !payload ? <LoadingBlock label="Loading recent jobs…" /> : null}
          <div className="data-table-responsive">
            <table className="data-table">
              <thead>
                <tr>
                  <th>Job ID</th>
                  <th>Screener</th>
                  <th>Status</th>
                  <th>Start Time</th>
                  <th>Finish Time</th>
                  <th>Hits</th>
                  <th>Duration</th>
                  <th>Progress</th>
                  <th>RC</th>
                  <th>Action</th>
                </tr>
              </thead>
              <tbody>
                {(payload?.jobs ?? []).map((job) => (
                  <tr
                    key={job.job_id}
                    className={job.job_id === selectedJob?.job_id ? "is-selected-row" : ""}
                    onClick={() => setSelectedJobId(job.job_id)}
                  >
                    <td data-label="Job ID" className="mono">#{job.job_id}</td>
                    <td data-label="Screener">{job.label}</td>
                    <td data-label="Status">
                      <StatusPill status={job.status} />
                    </td>
                    <td data-label="Start Time">{formatLocalDateTime(job.started_at)}</td>
                    <td data-label="Finish Time">{formatLocalDateTime(job.finished_at)}</td>
                    <td data-label="Hits">{job.success_count}</td>
                    <td data-label="Duration">{formatDuration(job.duration_seconds)}</td>
                    <td data-label="Progress">
                      <ProgressBar status={job.status} progress={job.progress_percent} label={job.progress_label ?? undefined} compact />
                    </td>
                    <td data-label="RC" className="mono">{job.return_code ?? "-"}</td>
                    <td data-label="Action">
                      {job.status === "running" ? (
                        <button
                          className="table-action-button"
                          type="button"
                          onClick={(event) => {
                            event.stopPropagation();
                            void handleCancelJob(job.job_id);
                          }}
                          disabled={isCancellingJobId === job.job_id}
                        >
                          {isCancellingJobId === job.job_id ? "Stopping..." : "Stop"}
                        </button>
                      ) : job.watchlist_url ? (
                        <Link className="table-action-button table-link-button" to={job.watchlist_url} onClick={(event) => event.stopPropagation()}>
                          Open Result
                        </Link>
                      ) : (
                        <span className="eyebrow">Done</span>
                      )}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
          {hasError ? <p className="panel-copy">Latest job snapshot failed to refresh. Showing empty fallback.</p> : null}
        </Panel>

        <Panel
          title="Console Tail"
          aside={
            <div className="runs-panel-aside">
              <span className="eyebrow">{selectedJob ? `${selectedJob.label} · ${selectedJob.success_count} hits · ${formatDuration(selectedJob.duration_seconds)}` : "Auto-refresh: 4s"}</span>
              {selectedJob ? <StatusPill status={selectedJob.status} /> : null}
            </div>
          }
        >
          <pre className="console-surface">{selectedJobLog}</pre>
        </Panel>
      </div>

      <ScreenerConfigModal
        action={selectedAction}
        isOpen={isModalOpen}
        onClose={() => setIsModalOpen(false)}
        onSubmit={handleRunAction}
        isLoading={isRunning}
      />
    </>
  );
}

function isoDateDaysAgo(daysAgo: number): string {
  const value = new Date();
  value.setDate(value.getDate() - daysAgo);
  return value.toISOString().slice(0, 10);
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
  if (actionId === "signal_warm_batch") {
    return JSON.stringify(
      {
        strategy_ids: ["rs", "vcp", "gap_fill", "fearzone"],
        start_date: "{{local_date_plus_14}}",
        end_date: "{{local_date}}",
        market_data_source: "database-first",
        overwrite_policy: "skip_existing",
        candidate_threshold: 4,
      },
      null,
      2,
    );
  }
  if (actionId === "overlap_backtest_v1") {
    return JSON.stringify(
      {
        strategy_ids: ["rs", "vcp", "gap_fill", "fearzone"],
        start_date: "{{local_date_plus_14}}",
        end_date: "{{local_date}}",
        entry_signal_threshold: 4,
        hold_periods_json: "[5, 10]",
      },
      null,
      2,
    );
  }
  return JSON.stringify(sharedUniverseTemplate, null, 2);
}
