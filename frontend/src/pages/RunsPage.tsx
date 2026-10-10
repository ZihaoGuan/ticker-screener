import { useEffect, useMemo, useRef, useState, type CSSProperties, type FormEvent, type KeyboardEvent as ReactKeyboardEvent, type PointerEvent as ReactPointerEvent } from "react";
import { Link, NavLink, useSearchParams } from "react-router-dom";
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
  ScreenerActionActivity,
  ScreenerActivityEntry,
  RtsValidationReportResponse,
} from "../lib/types";
import "./RunsPage.css";

type RunsPageMode = "screeners" | "schedules" | "warmup" | "backtests";

type RunsPageProps = {
  mode?: RunsPageMode;
};

type ScheduledActionOption = {
  id: string;
  label: string;
  bias_group?: "bullish" | "bearish" | "data" | "other";
  bullish_subgroup?: "leaders" | "pullbacks" | "bottoming" | "";
  estimated_duration_seconds: number | null;
  estimate_sample_count: number;
  fields: Array<{
    id: string;
    label: string;
    type: "text" | "number" | "date" | "select" | "multiselect" | "boolean";
    placeholder?: string | null;
    help_text?: string | null;
    options: Array<{ value: string; label: string }>;
  }>;
};

type ScheduleCadence = "weekdays" | "weekly_saturday" | "weekly_sunday";
type ScheduleCronDraft = {
  cadence: ScheduleCadence;
  time: string;
  cronExpr: string;
  hasUnsupportedCron: boolean;
  cadenceTouched: boolean;
};

type ScheduledJobLogResponse = {
  available: boolean;
  text: string;
  next_cursor: number;
  truncated: boolean;
  reset: boolean;
  status: string;
};

export function RunsPage({ mode = "screeners" }: RunsPageProps) {
  const screenersMode = mode === "screeners";
  const schedulesMode = mode === "schedules";
  const screenersSectionMode = screenersMode || schedulesMode;
  const auth = useAuth();
  const canManageSchedules = auth.hasCapability("manage_exclusions");
  const [payload, setPayload] = useState<JobsResponse | null>(null);
  const [selectedActionId, setSelectedActionId] = useState("");
  const [selectedJobId, setSelectedJobId] = useState("");
  const [selectedChildJobId, setSelectedChildJobId] = useState<number | null>(null);
  const [expandedBatchDateByJobId, setExpandedBatchDateByJobId] = useState<Record<string, string | null>>({});
  const [isModalOpen, setIsModalOpen] = useState(false);
  const [isRunning, setIsRunning] = useState(false);
  const [isCancellingJobId, setIsCancellingJobId] = useState("");
  const [isLoading, setIsLoading] = useState(true);
  const [hasError, setHasError] = useState(false);
  const [scheduledJobs, setScheduledJobs] = useState<ScheduledJobSummary[]>([]);
  const [isLoadingScheduledJobs, setIsLoadingScheduledJobs] = useState(true);
  const [scheduledConfigs, setScheduledConfigs] = useState<ScheduledJobConfig[]>([]);
  const [availableScheduledActions, setAvailableScheduledActions] = useState<ScheduledActionOption[]>([]);
  const [commonTimezones, setCommonTimezones] = useState<string[]>([]);
  const [schedulerCommand, setSchedulerCommand] = useState("");
  const [maxParallelJobs, setMaxParallelJobs] = useState("5");
  const [isLoadingScheduleConfig, setIsLoadingScheduleConfig] = useState(true);
  const [scheduleJobId, setScheduleJobId] = useState("");
  const [scheduleJobLabel, setScheduleJobLabel] = useState("");
  const [scheduleActionId, setScheduleActionId] = useState("weekly_rs");
  const [scheduleCronExpr, setScheduleCronExpr] = useState("30 16 * * 1-5");
  const [scheduleCadence, setScheduleCadence] = useState<ScheduleCadence>("weekdays");
  const [scheduleTime, setScheduleTime] = useState("16:30");
  const [scheduleHasUnsupportedCron, setScheduleHasUnsupportedCron] = useState(false);
  const [scheduleCadenceTouched, setScheduleCadenceTouched] = useState(false);
  const [scheduleCronTz, setScheduleCronTz] = useState("America/New_York");
  const [scheduleEnabled, setScheduleEnabled] = useState(true);
  const [scheduleOptionsJson, setScheduleOptionsJson] = useState("{}");
  const [lastSuggestedOptionsJson, setLastSuggestedOptionsJson] = useState("{}");
  const [scheduleActionSearch, setScheduleActionSearch] = useState("");
  const [isSavingSchedule, setIsSavingSchedule] = useState(false);
  const [isSavingScheduleSettings, setIsSavingScheduleSettings] = useState(false);
  const [scheduleNotice, setScheduleNotice] = useState("");
  const [isScheduleEditorOpen, setIsScheduleEditorOpen] = useState(false);
  const [isEditingSchedule, setIsEditingSchedule] = useState(false);
  const [isSchedulerSettingsOpen, setIsSchedulerSettingsOpen] = useState(false);
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
  const [rtsValidation, setRtsValidation] = useState<RtsValidationReportResponse | null>(null);
  const [batchRunNotice, setBatchRunNotice] = useState("");
  const consoleRef = useRef<HTMLPreElement | null>(null);
  const shouldAutoScrollConsoleRef = useRef(true);
  const lastAutoScheduleIdentityRef = useRef<{ jobId: string; jobLabel: string }>({ jobId: "", jobLabel: "" });
  const scheduleCronDraftRef = useRef<ScheduleCronDraft>({
    cadence: "weekdays",
    time: "16:30",
    cronExpr: "30 16 * * 1-5",
    hasUnsupportedCron: false,
    cadenceTouched: false,
  });

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
    if (schedulesMode) {
      setPayload(null);
      setIsLoading(false);
      setHasError(false);
      return;
    }
    setIsLoading(true);
    setHasError(false);
    const source = new EventSource("/api/jobs/stream", { withCredentials: true });

    source.addEventListener("snapshot", (event) => {
      const nextPayload = JSON.parse((event as MessageEvent).data) as JobsResponse;
      setPayload(nextPayload);
      setHasError(false);
      setIsLoading(false);
    });

    source.addEventListener("jobs", (event) => {
      const nextPayload = JSON.parse((event as MessageEvent).data) as JobsResponse;
      setPayload(nextPayload);
      setHasError(false);
      setIsLoading(false);
    });

    source.onerror = () => {
      setHasError(true);
      setIsLoading(false);
    };

    return () => source.close();
  }, [schedulesMode]);

  useEffect(() => {
    loadScheduledJobs();
    loadScheduleConfig();
  }, [canManageSchedules]);

  useEffect(() => {
    if (!schedulesMode || !canManageSchedules) {
      return;
    }
    const timer = window.setInterval(() => {
      void fetchJson<{ jobs: ScheduledJobSummary[] }>("/api/admin/scheduled-jobs")
        .then((result) => setScheduledJobs(result.jobs))
        .catch(() => undefined);
    }, 15_000);
    return () => window.clearInterval(timer);
  }, [canManageSchedules, schedulesMode]);

  useEffect(() => {
    if (!isScheduleEditorOpen && !isSchedulerSettingsOpen) return;
    const handleEscape = (event: KeyboardEvent) => {
      if (event.key !== "Escape") return;
      setIsScheduleEditorOpen(false);
      setIsSchedulerSettingsOpen(false);
    };
    window.addEventListener("keydown", handleEscape);
    return () => window.removeEventListener("keydown", handleEscape);
  }, [isScheduleEditorOpen, isSchedulerSettingsOpen]);

  const visibleActions = useMemo(() => {
    const actions = payload?.actions ?? [];
    if (mode === "warmup") {
      return actions.filter((action) => action.id === "signal_warm_batch");
    }
    if (mode === "backtests") {
      return actions.filter((action) => ["overlap_backtest_v1", "rts_validation"].includes(action.id));
    }
    if (mode === "schedules") {
      return [];
    }
    return actions.filter((action) => !["signal_warm_batch", "overlap_backtest_v1", "rts_validation"].includes(action.id));
  }, [mode, payload]);

  useEffect(() => {
    if (mode !== "backtests") {
      setRtsValidation(null);
      return;
    }
    void fetchJson<RtsValidationReportResponse>("/api/rts-validation/latest")
      .then(setRtsValidation)
      .catch(() => setRtsValidation({ available: false, report: null }));
  }, [mode, payload]);
  const actionActivityById = useMemo(
    () => new Map((payload?.action_activity ?? []).map((activity) => [activity.action_id, activity])),
    [payload?.action_activity],
  );

  const visibleJobs = useMemo(() => {
    const jobs = payload?.jobs ?? [];
    if (mode === "warmup") {
      return jobs.filter((job) => ["signal_warm_batch", "screener_history_batch"].includes(job.action_id));
    }
    if (mode === "backtests") {
      return jobs.filter((job) => ["overlap_backtest_v1", "rts_validation"].includes(job.action_id));
    }
    if (mode === "schedules") {
      return [];
    }
    return jobs.filter((job) => !["signal_warm_batch", "screener_history_batch", "overlap_backtest_v1", "rts_validation"].includes(job.action_id));
  }, [mode, payload]);

  const visibleActiveJob = useMemo(() => visibleJobs.find((job) => job.status === "running") ?? null, [visibleJobs]);

  useEffect(() => {
    if (!visibleActions.length) {
      return;
    }
    setSelectedActionId((current) => (current && visibleActions.some((action) => action.id === current) ? current : visibleActions[0].id));
    setWarmStrategyIds((current) => {
      if (current.trim()) {
        return current;
      }
      return (payload?.actions ?? [])
        .filter((item) => !["signal_warm_batch", "overlap_backtest_v1", "rts_validation"].includes(item.id))
        .slice(0, 4)
        .map((item) => item.id)
        .join(",");
    });
  }, [payload, visibleActions]);

  useEffect(() => {
    if (!visibleJobs.length) {
      setSelectedJobId("");
      setSelectedChildJobId(null);
      return;
    }
    setSelectedJobId((current) => {
      if (current && visibleJobs.some((job) => job.job_id === current)) {
        return current;
      }
      return visibleJobs[0].job_id;
    });
  }, [visibleJobs]);

  const selectedJob = useMemo(
    () => visibleJobs.find((job) => job.job_id === selectedJobId) ?? visibleJobs[0] ?? null,
    [selectedJobId, visibleJobs],
  );
  const selectedAction = useMemo(
    () => visibleActions.find((action) => action.id === selectedActionId) ?? visibleActions[0] ?? null,
    [selectedActionId, visibleActions],
  );
  const selectedChildJob = useMemo(
    () => selectedJob?.child_jobs.find((job) => job.job_run_id === selectedChildJobId) ?? null,
    [selectedChildJobId, selectedJob],
  );
  const liveStreamPath = useMemo(() => {
    if (selectedChildJob?.job_run_id != null) {
      return `/api/child-jobs/${selectedChildJob.job_run_id}/stream`;
    }
    if (selectedJob) {
      return `/api/jobs/${selectedJob.job_id}/stream`;
    }
    return null;
  }, [selectedChildJob, selectedJob]);
  const liveJobStream = useJobStream(liveStreamPath, Boolean(liveStreamPath));
  const childJobGroups = useMemo(() => groupChildJobsByDate(selectedJob?.child_jobs ?? []), [selectedJob]);
  const expandedBatchDate = useMemo(
    () => (selectedJob ? expandedBatchDateByJobId[selectedJob.job_id] ?? null : null),
    [expandedBatchDateByJobId, selectedJob],
  );
  const selectedJobLog = useMemo(() => {
    const liveStreamJob = liveJobStream.job;
    const liveStreamLog = liveJobStream.lines.length > 0 ? liveJobStream.lines.join("\n") : "";
    const liveChildSnapshotLog =
      selectedChildJob && liveStreamJob && "job_run_id" in liveStreamJob && liveStreamJob.job_run_id === selectedChildJob.job_run_id
        ? liveStreamJob.log_tail
        : "";
    const liveParentSnapshotLog =
      selectedJob && liveStreamJob && "job_id" in liveStreamJob && liveStreamJob.job_id === selectedJob.job_id
        ? liveStreamJob.log_tail
        : "";
    if (liveStreamLog) {
      return liveStreamLog;
    }
    if (liveJobStream.logTail) {
      return liveJobStream.logTail;
    }
    if (selectedChildJob) {
      return liveChildSnapshotLog || selectedChildJob.log_tail || "No screener log yet.";
    }
    if (selectedJob && isHierarchicalBatchJob(selectedJob.action_id)) {
      return childJobGroups.length > 0
        ? "Select date row, then click screener row to view separate log."
        : "Waiting for screener subtasks to attach to this batch run.";
    }
    return liveParentSnapshotLog || selectedJob?.log_tail || "No job log yet.";
  }, [childJobGroups.length, liveJobStream.job, liveJobStream.lines, liveJobStream.logTail, selectedChildJob, selectedJob]);
  const displayedSelectedJob = useMemo(() => {
    if (selectedJob && liveJobStream.job && "job_id" in liveJobStream.job && liveJobStream.job.job_id === selectedJob.job_id) {
      return liveJobStream.job;
    }
    return selectedJob;
  }, [liveJobStream.job, selectedJob]);
  const displayedSelectedChildJob = useMemo(() => {
    if (selectedChildJob && liveJobStream.job && "job_run_id" in liveJobStream.job && liveJobStream.job.job_run_id === selectedChildJob.job_run_id) {
      return liveJobStream.job;
    }
    return selectedChildJob;
  }, [liveJobStream.job, selectedChildJob]);
  const selectedScheduledAction = useMemo(
    () => availableScheduledActions.find((item) => item.id === scheduleActionId) ?? null,
    [availableScheduledActions, scheduleActionId],
  );
  const sortedScheduledActions = useMemo(
    () => [...availableScheduledActions].sort((left, right) => left.label.localeCompare(right.label)),
    [availableScheduledActions],
  );
  const filteredScheduledActions = useMemo(() => {
    const needle = scheduleActionSearch.trim().toLowerCase();
    if (!needle) {
      return sortedScheduledActions;
    }
    return sortedScheduledActions.filter((item) => {
      const haystack = `${item.label} ${item.id}`.toLowerCase();
      return haystack.includes(needle);
    });
  }, [scheduleActionSearch, sortedScheduledActions]);
  const groupedScheduledActions = useMemo(() => groupScheduledActions(filteredScheduledActions), [filteredScheduledActions]);
  const scheduleCronPreview = useMemo(() => {
    if (scheduleHasUnsupportedCron && !scheduleCadenceTouched) {
      return scheduleCronExpr;
    }
    return buildScheduleCronExpr(scheduleCadence, scheduleTime);
  }, [scheduleCadence, scheduleCadenceTouched, scheduleCronExpr, scheduleHasUnsupportedCron, scheduleTime]);

  useEffect(() => {
    const element = consoleRef.current;
    if (!element) {
      return;
    }
    if (!shouldAutoScrollConsoleRef.current) {
      return;
    }
    element.scrollTop = element.scrollHeight;
  }, [selectedJobLog]);

  useEffect(() => {
    shouldAutoScrollConsoleRef.current = true;
    const element = consoleRef.current;
    if (!element) {
      return;
    }
    element.scrollTop = element.scrollHeight;
  }, [selectedChildJobId, selectedJobId]);

  const handleConsoleScroll = () => {
    const element = consoleRef.current;
    if (!element) {
      return;
    }
    const distanceFromBottom = element.scrollHeight - element.scrollTop - element.clientHeight;
    shouldAutoScrollConsoleRef.current = distanceFromBottom <= 48;
  };

  useEffect(() => {
    if (!selectedJob?.child_jobs.length) {
      setSelectedChildJobId(null);
      return;
    }
    setSelectedChildJobId((current) => {
      if (current != null && selectedJob.child_jobs.some((job) => job.job_run_id === current)) {
        return current;
      }
      return null;
    });
  }, [selectedJob]);

  useEffect(() => {
    if (!selectedJob || !isHierarchicalBatchJob(selectedJob.action_id)) {
      return;
    }
    const availableDates = new Set(selectedJob.child_jobs.map((job) => job.run_date || "Unscheduled"));
    setExpandedBatchDateByJobId((current) => {
      const activeDate = current[selectedJob.job_id] ?? null;
      if (activeDate && availableDates.has(activeDate)) {
        return current;
      }
      const next = { ...current };
      next[selectedJob.job_id] = null;
      return next;
    });
  }, [selectedJob]);
  const suggestedScheduleOptionsJson = useMemo(
    () => buildScheduleOptionsTemplate(selectedScheduledAction),
    [selectedScheduledAction],
  );

  useEffect(() => {
    const trimmed = scheduleOptionsJson.trim();
    if (trimmed === "{}" || trimmed === lastSuggestedOptionsJson.trim()) {
      setScheduleOptionsJson(suggestedScheduleOptionsJson);
    }
    setLastSuggestedOptionsJson(suggestedScheduleOptionsJson);
  }, [lastSuggestedOptionsJson, scheduleActionId, scheduleOptionsJson, suggestedScheduleOptionsJson]);

  useEffect(() => {
    if (!isScheduleEditorOpen) {
      return;
    }
    const nextIdentity = buildDefaultScheduleIdentity(selectedScheduledAction);
    const lastIdentity = lastAutoScheduleIdentityRef.current;
    setScheduleJobId((current) => {
      if (!current || current === lastIdentity.jobId) {
        return nextIdentity.jobId;
      }
      return current;
    });
    setScheduleJobLabel((current) => {
      if (!current || current === lastIdentity.jobLabel) {
        return nextIdentity.jobLabel;
      }
      return current;
    });
    lastAutoScheduleIdentityRef.current = nextIdentity;
  }, [isScheduleEditorOpen, selectedScheduledAction]);

  useEffect(() => {
    if (mode !== "warmup") {
      setCoveragePayload(null);
      return;
    }
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
  }, [mode, warmFrom, warmStrategyIds, warmThreshold, warmTo]);

  useEffect(() => {
    if (mode !== "backtests") {
      setBacktestsPayload(null);
      setSelectedBacktest(null);
      setSelectedBacktestId(null);
      return;
    }
    setIsLoadingBacktests(true);
    void fetchJson<BacktestRunsResponseV1>("/api/backtests-v1")
      .then((result) => setBacktestsPayload(result))
      .catch(() => setBacktestsPayload({ configured: false, runs: [] }))
      .finally(() => setIsLoadingBacktests(false));
  }, [mode, payload]);

  useEffect(() => {
    if (mode !== "backtests") {
      return;
    }
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
  }, [backtestsPayload, mode]);

  useEffect(() => {
    if (mode !== "backtests") {
      return;
    }
    if (selectedBacktestId == null) {
      setSelectedBacktest(null);
      return;
    }
    setIsLoadingBacktestDetail(true);
    void fetchJson<BacktestRunDetailV1>(`/api/backtests-v1/${selectedBacktestId}`)
      .then((result) => setSelectedBacktest(result))
      .catch(() => setSelectedBacktest(null))
      .finally(() => setIsLoadingBacktestDetail(false));
  }, [mode, selectedBacktestId]);

  const launchRunAction = async (actionId: string, params: Record<string, string | string[]>) => {
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
  };

  const handleRunAction = async (params: Record<string, string | string[]>, actionId = selectedActionId) => {
    setIsRunning(true);
    setBatchRunNotice("");
    try {
      await launchRunAction(actionId, params);
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

  const handleBatchRun = async (
    sectionLabel: string,
    actions: Array<{
      id: string;
      label: string;
      bias_group?: "bullish" | "bearish" | "data" | "other";
      bullish_subgroup?: "leaders" | "pullbacks" | "bottoming" | "";
      command: string;
      supports_limit: boolean;
      fields: Array<{
        id: string;
        label: string;
        type: "text" | "number" | "date" | "select" | "multiselect" | "boolean";
        placeholder?: string | null;
        help_text?: string | null;
        options: Array<{ value: string; label: string }>;
      }>;
    }>,
  ) => {
    if (actions.length === 0) {
      return;
    }
    setIsRunning(true);
    setBatchRunNotice("");
    setSelectedActionId(actions[0]?.id ?? "");
    try {
      for (const action of actions) {
        await launchRunAction(action.id, {});
      }
      setBatchRunNotice(`Queued ${actions.length} ${sectionLabel.toLowerCase()} screeners.`);
    } catch (error) {
      setBatchRunNotice(error instanceof Error ? error.message : `Failed to queue ${sectionLabel.toLowerCase()} screeners.`);
    } finally {
      setIsRunning(false);
    }
  };

  const handleCancelJob = async (jobId: string) => {
    setIsCancellingJobId(jobId);
    try {
      await fetchJson<{ ok: boolean; job: JobsResponse["jobs"][number] }>(`/api/jobs/${jobId}/cancel`, {
        method: "POST",
      });
    } finally {
      setIsCancellingJobId("");
    }
  };

  const resetScheduleForm = () => {
    setIsEditingSchedule(false);
    const nextActionId = sortedScheduledActions[0]?.id ?? "weekly_rs";
    const nextAction = sortedScheduledActions.find((item) => item.id === nextActionId) ?? null;
    const nextIdentity = buildDefaultScheduleIdentity(nextAction);
    scheduleCronDraftRef.current = {
      cadence: "weekdays",
      time: "16:30",
      cronExpr: "30 16 * * 1-5",
      hasUnsupportedCron: false,
      cadenceTouched: false,
    };
    setScheduleJobId(nextIdentity.jobId);
    setScheduleJobLabel(nextIdentity.jobLabel);
    setScheduleActionId(nextActionId);
    setScheduleCronExpr("30 16 * * 1-5");
    setScheduleCadence("weekdays");
    setScheduleTime("16:30");
    setScheduleHasUnsupportedCron(false);
    setScheduleCadenceTouched(false);
    setScheduleCronTz("America/New_York");
    setScheduleEnabled(true);
    setScheduleActionSearch("");
    const nextSuggested = buildScheduleOptionsTemplate(nextAction);
    setScheduleOptionsJson(nextSuggested);
    setLastSuggestedOptionsJson(nextSuggested);
    lastAutoScheduleIdentityRef.current = nextIdentity;
  };

  const handleSaveSchedule = async (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    setIsSavingSchedule(true);
    setScheduleNotice("");
    try {
      const parsedOptions = parseScheduleOptionsJson(scheduleOptionsJson);
      const scheduleCronPayload = resolveScheduleCronDraft(scheduleCronDraftRef.current);
      await fetchJson<{ ok: boolean }>("/api/admin/schedules", {
        method: "POST",
        body: JSON.stringify({
          job_id: scheduleJobId,
          job_label: scheduleJobLabel,
          action_id: scheduleActionId,
          cron_expr: scheduleCronPayload,
          cron_tz: scheduleCronTz,
          enabled: scheduleEnabled,
          options: parsedOptions,
        }),
      });
      setScheduleNotice("Scheduled job saved.");
      loadScheduleConfig();
      resetScheduleForm();
      setIsScheduleEditorOpen(false);
    } catch (error) {
      setScheduleNotice(error instanceof Error ? error.message : "Failed to save scheduled job.");
    } finally {
      setIsSavingSchedule(false);
    }
  };

  const handleEditSchedule = (job: ScheduledJobConfig) => {
    setIsEditingSchedule(true);
    const parsedCron = parseSimpleScheduleCron(job.cron_expr);
    scheduleCronDraftRef.current = {
      cadence: parsedCron.cadence,
      time: parsedCron.time,
      cronExpr: job.cron_expr,
      hasUnsupportedCron: !parsedCron.supported,
      cadenceTouched: false,
    };
    setScheduleJobId(job.job_id);
    setScheduleJobLabel(job.job_label);
    setScheduleActionId(job.action_id);
    setScheduleCronExpr(job.cron_expr);
    setScheduleCadence(parsedCron.cadence);
    setScheduleTime(parsedCron.time);
    setScheduleHasUnsupportedCron(!parsedCron.supported);
    setScheduleCadenceTouched(false);
    setScheduleCronTz(job.cron_tz);
    setScheduleEnabled(job.enabled);
    setScheduleActionSearch("");
    const serialized = JSON.stringify(job.options ?? {}, null, 2);
    setScheduleOptionsJson(serialized);
    setLastSuggestedOptionsJson(serialized);
    lastAutoScheduleIdentityRef.current = { jobId: job.job_id, jobLabel: job.job_label };
    setIsScheduleEditorOpen(true);
  };

  const handleMoveSchedule = async (job: ScheduledJobConfig, minute: number) => {
    const cronExpr = moveSimpleScheduleCron(job.cron_expr, minute);
    if (!cronExpr) {
      setScheduleNotice("Only weekday, Saturday, and Sunday schedules can be moved from the timeline.");
      return;
    }
    setIsSavingSchedule(true);
    setScheduleNotice("");
    try {
      await fetchJson<{ ok: boolean }>("/api/admin/schedules", {
        method: "POST",
        body: JSON.stringify({
          job_id: job.job_id,
          job_label: job.job_label,
          action_id: job.action_id,
          cron_expr: cronExpr,
          cron_tz: job.cron_tz,
          enabled: job.enabled,
          options: job.options ?? {},
        }),
      });
      setScheduleNotice(`${job.job_label} moved to ${formatMinuteOfDay(minute)}.`);
      loadScheduleConfig();
    } catch (error) {
      setScheduleNotice(error instanceof Error ? error.message : "Failed to move scheduled job.");
    } finally {
      setIsSavingSchedule(false);
    }
  };

  const handleOpenNewSchedule = () => {
    resetScheduleForm();
    setIsEditingSchedule(false);
    setScheduleNotice("");
    setIsScheduleEditorOpen(true);
  };

  const handleDeleteSchedule = async (jobId: string) => {
    const target = scheduledConfigs.find((job) => job.job_id === jobId);
    if (!window.confirm(`Remove ${target?.job_label || jobId} from the scheduler? This does not delete its past runs or artifacts.`)) {
      return;
    }
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
      setIsSchedulerSettingsOpen(false);
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
    if (status === "waiting") {
      return <span className="schedule-chip schedule-chip-soft">Waiting for prerequisites</span>;
    }
    return <span className="status-pill status-unknown">{status || "unknown"}</span>;
  };

  const renderScheduledPersistenceStatus = (job: ScheduledJobSummary) => {
    if (job.persisted_to_db === true) {
      return (
        <div className="schedule-cell-stack">
          <span className="schedule-state-pill schedule-state-pill-enabled">DB Persisted</span>
          <span className="file-meta">
            {job.screen_run_id != null ? `screen_run_id ${job.screen_run_id}` : job.persistence_message || "Persisted successfully."}
          </span>
        </div>
      );
    }
    if (job.persisted_to_db === false) {
      return (
        <div className="schedule-cell-stack">
          <span className="schedule-state-pill schedule-state-pill-disabled">DB Not Persisted</span>
          <span className="file-meta">{job.persistence_message || "Persistence was not confirmed."}</span>
        </div>
      );
    }
    return (
      <div className="schedule-cell-stack">
        <span className="schedule-chip schedule-chip-soft">DB Pending</span>
        <span className="file-meta">{job.persistence_message || "Persistence status not available yet."}</span>
      </div>
    );
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

  const pageTitle =
    mode === "warmup"
      ? "Warmup Batch"
      : mode === "backtests"
        ? "Backtest Runner"
        : schedulesMode
          ? "Screener Automation"
          : "Available Screeners";
  const jobsPanelTitle =
    mode === "warmup" ? "Recent Warmup Jobs" : mode === "backtests" ? "Recent Backtest Jobs" : "Recent Screener Jobs";
  const groupedVisibleActions = useMemo(() => {
    if (mode !== "screeners") {
      return [{ key: "all", label: "", sections: [{ key: "all", label: "", actions: visibleActions }] }];
    }
    const order: Array<{
      key: "data" | "bullish" | "bearish" | "other";
      label: string;
      sections: Array<{ key: string; label: string; match: (action: (typeof visibleActions)[number]) => boolean }>;
    }> = [
      {
        key: "data",
        label: "Data Refresh",
        sections: [{ key: "data", label: "", match: (action) => (action.bias_group ?? "other") === "data" }],
      },
      {
        key: "bullish",
        label: "Bullish",
        sections: [
          {
            key: "leaders",
            label: "Leader Signals",
            match: (action) => (action.bias_group ?? "other") === "bullish" && (action.bullish_subgroup ?? "leaders") === "leaders",
          },
          {
            key: "pullbacks",
            label: "Pullback Signals",
            match: (action) => (action.bias_group ?? "other") === "bullish" && (action.bullish_subgroup ?? "") === "pullbacks",
          },
          {
            key: "bottoming",
            label: "Breakout From Bottoming Signals",
            match: (action) => (action.bias_group ?? "other") === "bullish" && (action.bullish_subgroup ?? "") === "bottoming",
          },
        ],
      },
      {
        key: "bearish",
        label: "Bearish",
        sections: [{ key: "bearish", label: "", match: (action) => (action.bias_group ?? "other") === "bearish" }],
      },
      {
        key: "other",
        label: "Other",
        sections: [{ key: "other", label: "", match: (action) => (action.bias_group ?? "other") === "other" }],
      },
    ];
    return order
      .map((group) => ({
        key: group.key,
        label: group.label,
        sections: group.sections
          .map((section) => ({
            key: `${group.key}-${section.key}`,
            label: section.label,
            actions: visibleActions.filter(section.match),
          }))
          .filter((section) => section.actions.length > 0),
      }))
      .filter((group) => group.sections.length > 0);
  }, [mode, visibleActions]);

  const consoleMeta = selectedChildJob ? displayedSelectedChildJob : displayedSelectedJob;

  return (
    <>
      <div className="page-grid">
        {screenersSectionMode ? <ScreenersSubnav activeMode={mode} /> : null}
        {!schedulesMode ? (
        <Panel
          title={screenersMode ? "Screener Launcher v2" : pageTitle}
          aside={screenersMode ? <span className="screeners-operator-badge">Operator Mode</span> : undefined}
          className={screenersMode ? "screeners-launcher-panel" : ""}
        >
          {isLoading && !payload ? <LoadingBlock label="Loading available screeners…" /> : null}
          {batchRunNotice ? <p className="panel-copy">{batchRunNotice}</p> : null}
          {groupedVisibleActions.map((group) => (
            <div key={group.key} className="screener-group-section">
              {group.label ? <h2 className="screener-group-title">{group.label}</h2> : null}
              {group.sections.map((section) => (
                <div key={section.key} className="screener-subgroup-section">
                  <div className="screener-subgroup-head">
                    {section.label ? <h3 className="screener-subgroup-title">{section.label}</h3> : null}
                    {screenersMode ? (
                      <button
                        className="screener-batch-button"
                        onClick={() => void handleBatchRun(section.label || group.label || "selected", section.actions)}
                        type="button"
                        disabled={isRunning}
                      >
                        RUN ALL
                      </button>
                    ) : null}
                  </div>
                  <div className="screeners-grid">
                    {section.actions.map((action) => (
                      <div key={action.id} className="screener-card">
                        {(() => {
                          const configureOnly = ["signal_warm_batch", "overlap_backtest_v1", "rts_validation"].includes(action.id);
                          const timing = availableScheduledActions.find((item) => item.id === action.id);
                          const activity = actionActivityById.get(action.id);
                          const actionIsActive = Boolean(activity?.adhoc_current || activity?.scheduled_current);
                          const estimatedDuration = timing?.estimated_duration_seconds ?? activity?.scheduled_current?.estimated_duration_seconds ?? activity?.scheduled_last?.estimated_duration_seconds;
                          return (
                            <>
                              <div className="screener-card-header">
                                <div className="screener-card-title-block">
                                  <h3>{action.label}</h3>
                                  {screenersMode ? <span className="screener-card-id">ID: {action.id}</span> : null}
                                </div>
                              </div>
                              <p className="screener-description">
                                {configureOnly
                                  ? "Open config to choose date range and screener set."
                                  : describeScreenerAction(action.id, action.fields.length > 0)}
                              </p>
                              <p className="file-meta">
                                Estimated run time: {formatEstimatedDuration(estimatedDuration)}
                                {timing?.estimate_sample_count ? ` (${describeEstimateSample(timing.estimate_sample_count)})` : ""}
                              </p>
                              <div className="screener-card-activity" aria-live="polite">
                                {renderScreenerActivity("One-time", activity?.adhoc_current, activity?.adhoc_last)}
                                {renderScreenerActivity("Scheduled", activity?.scheduled_current, activity?.scheduled_last, activity?.scheduled_count)}
                              </div>
                              <div className="screener-card-actions">
                                {!configureOnly ? (
                                  <button className="screener-run-button" onClick={() => void handleQuickRun(action.id)} type="button" disabled={isRunning || actionIsActive}>
                                    RUN DEFAULT
                                  </button>
                                ) : null}
                                {action.fields.length > 0 ? (
                                  <button className="screener-config-button" onClick={() => handleConfigureClick(action.id)} type="button" disabled={isRunning}>
                                    {screenersMode && !configureOnly ? "▣" : configureOnly ? "OPEN CONFIG" : "CONFIGURE"}
                                  </button>
                                ) : null}
                              </div>
                            </>
                          );
                        })()}
                      </div>
                    ))}
                  </div>
                </div>
              ))}
            </div>
          ))}
        </Panel>
        ) : null}

        {!schedulesMode ? (
        <Panel
          title={screenersMode ? "Active Job Monitor" : "Current Progress"}
          aside={visibleActiveJob ? <span className="eyebrow">{visibleActiveJob.label}</span> : <span className="eyebrow">Idle</span>}
          className={screenersMode ? "screeners-progress-panel" : ""}
        >
          <div className={`run-progress-panel${screenersMode ? " screeners-progress-shell" : ""}`}>
            <ProgressBar
              status={visibleActiveJob?.status ?? "cancelled"}
              label={
                visibleActiveJob
                  ? `${visibleActiveJob.label} · ${visibleActiveJob.progress_label || `started ${visibleActiveJob.started_at || "just now"}`} · ${visibleActiveJob.success_count} hits · ${formatDuration(visibleActiveJob.duration_seconds)}`
                  : mode === "warmup"
                    ? "No warmup batch currently running"
                    : mode === "backtests"
                      ? "No backtest currently running"
                      : "No screener currently running"
              }
              progress={visibleActiveJob?.progress_percent ?? null}
            />
            {visibleActiveJob ? (
              <div className="button-row">
                <button
                  className="secondary-button"
                  type="button"
                  onClick={() => void handleCancelJob(visibleActiveJob.job_id)}
                  disabled={isCancellingJobId === visibleActiveJob.job_id}
                >
                  {isCancellingJobId === visibleActiveJob.job_id ? "Stopping..." : "Stop Current Job"}
                </button>
              </div>
            ) : null}
          </div>
        </Panel>
        ) : null}

        {mode === "warmup" ? (
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
        ) : null}

        {mode === "backtests" ? (
        <Panel title="RTS Validation" aside={<span className="eyebrow">{rtsValidation?.report?.observation_count ?? 0} observations</span>}>
          {!rtsValidation?.available || !rtsValidation.report ? (
            <p className="panel-copy">Run “Validate Relative Trend Strength” after enough daily RTS snapshots have accumulated.</p>
          ) : (
            <>
              <p className="panel-copy">Descriptive forward-return evidence for persisted RTS v1 snapshots. This report does not modify live trading scores.</p>
              <div className="backtest-detail-grid">
                {(["5", "10", "20", "63"] as const).map((horizon) => {
                  const metrics = rtsValidation.report?.overall.horizons[horizon];
                  return (
                    <article className="metric-card" key={horizon}>
                      <h3>{horizon}D Excess</h3>
                      <div className="metric-value">{metrics?.average_excess_return_pct == null ? "-" : `${metrics.average_excess_return_pct.toFixed(2)}%`}</div>
                      <p className="card-meta">Win {metrics?.excess_win_rate_pct == null ? "-" : `${metrics.excess_win_rate_pct.toFixed(1)}%`} · n={metrics?.count ?? 0}</p>
                    </article>
                  );
                })}
              </div>
              <div className="data-table-responsive">
                <table className="data-table">
                  <thead><tr><th>RTS Cohort</th><th>Count</th><th>20D Avg</th><th>20D Excess</th><th>20D Drawdown</th></tr></thead>
                  <tbody>
                    {Object.entries(rtsValidation.report.by_score_bucket).map(([bucket, summary]) => {
                      const metrics = summary.horizons["20"];
                      return <tr key={bucket}><td>{bucket}</td><td>{summary.count}</td><td>{formatMaybePercent(metrics?.average_return_pct)}</td><td>{formatMaybePercent(metrics?.average_excess_return_pct)}</td><td>{formatMaybePercent(metrics?.average_max_drawdown_pct)}</td></tr>;
                    })}
                  </tbody>
                </table>
              </div>
              <p className="panel-copy">Range {rtsValidation.report.start_date} to {rtsValidation.report.end_date}. {rtsValidation.report.limitations[0]}</p>
            </>
          )}
        </Panel>
        ) : null}

        {mode === "backtests" ? (
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
        ) : null}

        {!schedulesMode ? (
        <div className={screenersMode ? "screeners-history-console-grid" : ""}>
        <Panel title={screenersMode ? "Batch Tracking & History" : jobsPanelTitle} className={screenersMode ? "screeners-history-panel" : ""}>
          {isLoading && !payload ? <LoadingBlock label="Loading recent jobs…" /> : null}
          <div className="data-table-responsive">
            <table className="data-table">
              <thead>
                <tr>
                  <th>Job / Label</th>
                  <th>Screener</th>
                  <th>Scan Date</th>
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
                {visibleJobs.length === 0 ? (
                  <tr>
                    <td colSpan={11}>
                      {mode === "warmup" ? "No warmup jobs yet." : mode === "backtests" ? "No backtest jobs yet." : "No screener jobs yet."}
                    </td>
                  </tr>
                ) : visibleJobs.flatMap((job) => {
                  const isSelected = job.job_id === selectedJob?.job_id;
                  const hasChildren = job.child_jobs.length > 0;
                  const isHierarchicalBatch = isHierarchicalBatchJob(job.action_id);
                  const groupedChildren = groupChildJobsByDate(job.child_jobs);
                  const activeDate = expandedBatchDateByJobId[job.job_id] ?? null;
                  const rows = [
                    <tr
                      key={job.job_id}
                      className={`${screenersMode ? "screeners-history-row" : ""} ${isSelected ? "is-selected-row" : ""}`.trim()}
                      onClick={() => setSelectedJobId(job.job_id)}
                    >
                      <td data-label="Job / Label">
                        <div className="screeners-history-job">
                          <strong>{job.label}</strong>
                          <span className="file-meta mono">#{job.job_id}</span>
                        </div>
                      </td>
                      <td data-label="Screener">
                        <div className="screeners-history-cell">
                          <span className="schedule-chip mono">{job.action_id}</span>
                          {hasChildren ? <span className="file-meta">{job.child_jobs.length} subtasks</span> : <span className="file-meta">Single run</span>}
                        </div>
                      </td>
                      <td data-label="Scan Date">
                        <span className="schedule-chip schedule-chip-soft mono">{job.scan_target || "-"}</span>
                      </td>
                      <td data-label="Status">
                        <StatusPill status={job.status} />
                      </td>
                      <td data-label="Start Time">{formatLocalDateTime(job.started_at)}</td>
                      <td data-label="Finish Time">{formatLocalDateTime(job.finished_at)}</td>
                      <td data-label="Hits">{job.success_count}</td>
                      <td data-label="Duration">{formatDuration(job.duration_seconds)}</td>
                      <td data-label="Progress">
                        {isHierarchicalBatch ? (
                          <span className="file-meta">-</span>
                        ) : (
                          <ProgressBar status={job.status} progress={job.progress_percent} label={job.progress_label ?? undefined} compact />
                        )}
                      </td>
                      <td data-label="RC" className="mono">{job.return_code ?? "-"}</td>
                      <td data-label="Action">
                        {job.status === "running" ? (
                          <button
                            className="table-action-button screeners-history-action screeners-history-action-danger"
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
                          <Link className="table-action-button table-link-button screeners-history-action" to={job.watchlist_url} onClick={(event) => event.stopPropagation()}>
                            Open Result
                          </Link>
                        ) : (
                          <span className="eyebrow">Done</span>
                        )}
                      </td>
                    </tr>,
                  ];
                  if (isSelected && isHierarchicalBatch) {
                    rows.push(
                      <tr key={`${job.job_id}-children`} className="child-jobs-expanded-row">
                        <td colSpan={11}>
                          <div className="child-jobs-expanded">
                            <div className="child-jobs-expanded-header">
                              <strong>Dates to Screen</strong>
                              <span className="file-meta">Click date, then click screener to inspect its log.</span>
                            </div>
                            {groupedChildren.length === 0 ? (
                              <p className="panel-copy">Waiting for screener subtasks to attach to this batch run.</p>
                            ) : (
                              <div className="data-table-responsive">
                                <table className="data-table">
                                  <thead>
                                    <tr>
                                      <th>Date</th>
                                      <th>Status</th>
                                      <th>Screeners</th>
                                      <th>Hits</th>
                                      <th>Duration</th>
                                      <th>Note</th>
                                    </tr>
                                  </thead>
                                  <tbody>
                                    {groupedChildren.flatMap((group) => {
                                      const dateRows = [
                                        <tr
                                          key={`${job.job_id}-${group.runDate}`}
                                          className={group.runDate === activeDate ? "is-selected-row child-date-row" : "child-date-row"}
                                          onClick={(event) => {
                                            event.stopPropagation();
                                            setExpandedBatchDateByJobId((current) => {
                                              const next = { ...current };
                                              next[job.job_id] = current[job.job_id] === group.runDate ? null : group.runDate;
                                              return next;
                                            });
                                            if (activeDate === group.runDate && selectedChildJob?.run_date === group.runDate) {
                                              setSelectedChildJobId(null);
                                            }
                                          }}
                                        >
                                          <td data-label="Date">{group.runDate}</td>
                                          <td data-label="Status">
                                            <StatusPill status={group.status} />
                                          </td>
                                          <td data-label="Screeners">{group.children.length}</td>
                                          <td data-label="Hits">{group.successCount}</td>
                                          <td data-label="Duration">{formatDuration(group.durationSeconds)}</td>
                                          <td data-label="Note">{group.note}</td>
                                        </tr>,
                                      ];
                                      if (group.runDate === activeDate) {
                                        dateRows.push(
                                          <tr key={`${job.job_id}-${group.runDate}-screeners`} className="child-date-expanded-row">
                                            <td colSpan={6}>
                                              <div className="child-date-expanded">
                                                <table className="data-table child-screener-table">
                                                  <thead>
                                                    <tr>
                                                      <th>Screener</th>
                                                      <th>Status</th>
                                                      <th>Hits</th>
                                                      <th>Duration</th>
                                                      <th>Progress</th>
                                                      <th>Note</th>
                                                    </tr>
                                                  </thead>
                                                  <tbody>
                                                    {group.children.map((child) => (
                                                      <tr
                                                        key={child.job_run_id}
                                                        className={child.job_run_id === selectedChildJobId ? "is-selected-row" : ""}
                                                        onClick={(event) => {
                                                          event.stopPropagation();
                                                          setExpandedBatchDateByJobId((current) => ({ ...current, [job.job_id]: group.runDate }));
                                                          setSelectedChildJobId(child.job_run_id);
                                                        }}
                                                      >
                                                        <td data-label="Screener">{child.strategy_id || child.label}</td>
                                                        <td data-label="Status">
                                                          <StatusPill status={child.status} />
                                                        </td>
                                                        <td data-label="Hits">{child.success_count}</td>
                                                        <td data-label="Duration">{formatDuration(child.duration_seconds)}</td>
                                                        <td data-label="Progress">
                                                          <ProgressBar status={child.status} progress={child.progress_percent} label={child.progress_label ?? undefined} compact />
                                                        </td>
                                                        <td data-label="Note">{child.message || (child.skipped ? "Skipped" : "-")}</td>
                                                      </tr>
                                                    ))}
                                                  </tbody>
                                                </table>
                                              </div>
                                            </td>
                                          </tr>,
                                        );
                                      }
                                      return dateRows;
                                    })}
                                  </tbody>
                                </table>
                              </div>
                            )}
                          </div>
                        </td>
                      </tr>,
                    );
                  }
                  return rows;
                })}
              </tbody>
            </table>
          </div>
          {hasError ? <p className="panel-copy">Latest job snapshot failed to refresh. Showing empty fallback.</p> : null}
        </Panel>

        <Panel
          title={screenersMode ? "Live Console" : "Console Tail"}
          aside={
            <div className="runs-panel-aside">
              <span className="eyebrow">
                {selectedChildJob
                  ? `${liveJobDisplayLabel(displayedSelectedChildJob)} · ${displayedSelectedChildJob?.success_count ?? 0} hits · ${formatDuration(displayedSelectedChildJob?.duration_seconds ?? 0)}`
                  : displayedSelectedJob && isHierarchicalBatchJob(displayedSelectedJob.action_id) && expandedBatchDate
                    ? `${expandedBatchDate} · choose screener for separate log`
                  : displayedSelectedJob
                    ? `${displayedSelectedJob.label} · ${displayedSelectedJob.success_count} hits · ${formatDuration(displayedSelectedJob.duration_seconds)}`
                    : "Auto-refresh: 4s"}
              </span>
              {liveJobStream.connected ? <span className="status-pill status-running">LIVE</span> : null}
              {selectedChildJob ? <StatusPill status={displayedSelectedChildJob?.status ?? selectedChildJob.status} /> : displayedSelectedJob ? <StatusPill status={displayedSelectedJob.status} /> : null}
            </div>
          }
          className={screenersMode ? "screeners-console-panel" : ""}
        >
          {screenersMode ? (
            <div className="screeners-console-terminal">
              <div className="screeners-console-header">
                <div className="screeners-console-header-title">
                  <span className="schedule-chip mono">Console: {consoleMeta ? describeConsoleRunId(consoleMeta) : "idle"}</span>
                  <span className="file-meta">{consoleMeta ? liveJobDisplayLabel(consoleMeta) : "Select job to inspect live stream."}</span>
                </div>
                <div className="screeners-console-header-status">
                  {liveJobStream.connected ? <span className="status-pill status-running">LIVE</span> : <span className="schedule-state-pill schedule-state-pill-disabled">Snapshot</span>}
                </div>
              </div>
              <div className="screeners-console-shell">
                <pre ref={consoleRef} className="console-surface" onScroll={handleConsoleScroll}>{selectedJobLog}</pre>
                <div className="screeners-console-meta">
                  <div className="screeners-console-meta-block">
                    <span className="eyebrow">Label</span>
                    <strong>{consoleMeta ? liveJobDisplayLabel(consoleMeta) : "-"}</strong>
                  </div>
                  <div className="screeners-console-meta-block">
                    <span className="eyebrow">Hits</span>
                    <strong className="mono">{consoleMeta?.success_count ?? 0}</strong>
                  </div>
                  <div className="screeners-console-meta-block">
                    <span className="eyebrow">Status</span>
                    {consoleMeta ? <StatusPill status={consoleMeta.status} /> : <span className="eyebrow">Idle</span>}
                  </div>
                  <div className="screeners-console-meta-block">
                    <span className="eyebrow">Duration</span>
                    <strong className="mono">{formatDuration(consoleMeta?.duration_seconds ?? 0)}</strong>
                  </div>
                </div>
              </div>
            </div>
          ) : (
            <pre ref={consoleRef} className="console-surface" onScroll={handleConsoleScroll}>{selectedJobLog}</pre>
          )}
        </Panel>
        </div>
        ) : null}

        {canManageSchedules && schedulesMode ? (
          <>
            <ScheduleTimeline
              configs={scheduledConfigs}
              summaries={scheduledJobs}
              isLoading={isLoadingScheduleConfig || isLoadingScheduledJobs}
              maxParallelJobs={Number(maxParallelJobs) || 0}
              isSaving={isSavingSchedule}
              onEdit={handleEditSchedule}
              onMove={handleMoveSchedule}
              onNew={handleOpenNewSchedule}
              onOpenSettings={() => setIsSchedulerSettingsOpen(true)}
            />

            <Panel title="Recent Execution Details" aside={<span className="eyebrow">{scheduledJobs.length} tracked</span>} className="screeners-scheduled-panel screeners-secondary-panel">
              <p className="panel-copy screeners-section-intro">
                Latest scheduler status, persistence confirmation, logs, and result artifacts. Timeline state is derived from these records.
              </p>
              {isLoadingScheduledJobs ? <LoadingBlock label="Loading scheduled job status…" compact /> : null}
              <div className="data-table-responsive">
                <table className="data-table">
                  <thead>
                    <tr>
                      <th>Job</th>
                      <th>Status</th>
                      <th>DB Persist</th>
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
                        <td colSpan={8}>{isLoadingScheduledJobs ? "Loading scheduled jobs..." : "No scheduled job status files found."}</td>
                      </tr>
                    ) : (
                      scheduledJobs.map((job) => (
                        <tr key={job.job_id} className="screeners-scheduled-row">
                          <td data-label="Job">
                            <div className="admin-job-cell schedule-job-cell">
                              <strong>{job.job_label}</strong>
                              <span className="file-meta">{job.job_id}</span>
                            </div>
                          </td>
                          <td data-label="Status">
                            <div className="schedule-cell-stack">
                              {renderScheduledJobStatus(job.status)}
                              <span className="file-meta">{job.exit_code == null ? "Exit pending" : `exit ${job.exit_code}`}</span>
                            </div>
                          </td>
                          <td data-label="DB Persist">
                            {renderScheduledPersistenceStatus(job)}
                          </td>
                          <td data-label="Last Start">
                            <div className="schedule-cell-stack">
                              <span>{formatLocalDateTime(job.last_started_at)}</span>
                              <span className="file-meta">latest dispatch</span>
                            </div>
                          </td>
                          <td data-label="Last Finish">
                            <div className="schedule-cell-stack">
                              <span>{formatLocalDateTime(job.last_finished_at)}</span>
                              <span className="file-meta">latest completion</span>
                            </div>
                          </td>
                          <td data-label="Exit Code">
                            <span className="schedule-chip schedule-chip-soft mono">{job.exit_code ?? "-"}</span>
                          </td>
                          <td data-label="Log">
                            <div className="schedule-cell-stack">
                              <code className="schedule-options-code">{formatFilePathPreview(job.log_file)}</code>
                              <span className="file-meta">{job.log_file ? "status log" : "no log file"}</span>
                            </div>
                          </td>
                          <td data-label="Artifact">
                            <div className="schedule-cell-stack">
                              <code className="schedule-options-code">{formatFilePathPreview(job.artifact_file)}</code>
                              <span className="file-meta">{job.artifact_file ? "result artifact" : "no artifact yet"}</span>
                            </div>
                          </td>
                        </tr>
                      ))
                    )}
                  </tbody>
                </table>
              </div>
            </Panel>

            <Panel title="Schedule Definitions" aside={<span className="eyebrow">{scheduledConfigs.length} schedules</span>} className="screeners-scheduler-config-panel screeners-secondary-panel">
              <div className="run-toolbar">
                <div className="screeners-scheduler-toolbar">
                  <div>
                    <p className="panel-copy">Edit timing, screener choice, payload overrides, and enabled state.</p>
                    <p className="file-meta">Runner polls every 5 minutes · capacity {maxParallelJobs} parallel jobs</p>
                  </div>
                  <div className="button-row">
                    <button className="primary-button" type="button" onClick={handleOpenNewSchedule} disabled={isSavingSchedule || isLoadingScheduleConfig}>
                      New Schedule
                    </button>
                    <button className="ghost-button" type="button" onClick={() => setIsSchedulerSettingsOpen(true)} disabled={isSavingScheduleSettings || isLoadingScheduleConfig}>
                      Scheduler Settings
                    </button>
                  </div>
                </div>
                {scheduleNotice ? <p className="panel-copy" role="status" aria-live="polite">{scheduleNotice}</p> : null}
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
                        <th>Est. time</th>
                        <th>Enabled</th>
                        <th>Actions</th>
                      </tr>
                    </thead>
                    <tbody>
                      {scheduledConfigs.length === 0 ? (
                        <tr>
                          <td colSpan={8}>{isLoadingScheduleConfig ? "Loading schedules..." : "No scheduled jobs configured yet."}</td>
                        </tr>
                      ) : (
                        scheduledConfigs.map((job) => (
                          <tr key={job.job_id}>
                            <td data-label="Job">
                              <div className="admin-job-cell schedule-job-cell">
                                <strong>{job.job_label}</strong>
                                <span className="file-meta">{job.job_id}</span>
                              </div>
                            </td>
                            <td data-label="Screener">
                              <div className="schedule-cell-stack">
                                <span className="schedule-chip mono">{job.action_id}</span>
                                <span className="panel-copy">{describeScreenerAction(job.action_id, Boolean(job.options && Object.keys(job.options).length > 0))}</span>
                              </div>
                            </td>
                            <td data-label="Cron">
                              <div className="schedule-cell-stack">
                                <span className="schedule-chip schedule-chip-soft mono">{job.cron_expr}</span>
                                <span className="file-meta">{describeScheduleCadence(job.cron_expr)}</span>
                              </div>
                            </td>
                            <td data-label="TZ">
                              <span className="schedule-chip schedule-chip-soft">{job.cron_tz}</span>
                            </td>
                            <td data-label="Options">
                              <div className="schedule-cell-stack">
                                <code className="schedule-options-code">{formatScheduleOptionsPreview(job.options)}</code>
                                <span className="file-meta">{summarizeScheduleOptions(job.options)}</span>
                              </div>
                            </td>
                            <td data-label="Est. time">
                              <div className="schedule-cell-stack">
                                <strong>{formatEstimatedDuration(job.estimated_duration_seconds)}</strong>
                                <span className="file-meta">{describeEstimateSample(job.estimate_sample_count)}</span>
                              </div>
                            </td>
                            <td data-label="Enabled">
                              <span className={`schedule-state-pill ${job.enabled ? "schedule-state-pill-enabled" : "schedule-state-pill-disabled"}`}>
                                {job.enabled ? "Live" : "Paused"}
                              </span>
                            </td>
                            <td data-label="Actions">
                              <div className="schedule-actions">
                                <button className="table-action-button schedule-action-button" type="button" disabled={isSavingSchedule} onClick={() => handleEditSchedule(job)}>
                                  Open
                                </button>
                                <button className="table-action-button schedule-action-button schedule-action-button-danger" type="button" disabled={isSavingSchedule} onClick={() => void handleDeleteSchedule(job.job_id)}>
                                  Remove
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
      </div>

      {isScheduleEditorOpen ? (
        <div className="modal-backdrop" role="presentation" onClick={() => setIsScheduleEditorOpen(false)}>
          <div className="modal-shell screeners-schedule-modal-shell" role="dialog" aria-modal="true" aria-labelledby="schedule-editor-title" onClick={(event) => event.stopPropagation()}>
            <form className="modal-content screeners-schedule-modal" onSubmit={(event) => void handleSaveSchedule(event)}>
              <div className="modal-header">
                <div>
                  <div className="eyebrow">{isEditingSchedule ? "Edit schedule" : "New schedule"}</div>
                  <h2 id="schedule-editor-title">{scheduleJobLabel || "Configure Scheduled Screener"}</h2>
                </div>
                <button className="ghost-button" type="button" aria-label="Close schedule editor" onClick={() => setIsScheduleEditorOpen(false)}>
                  Close
                </button>
              </div>
              <div className="modal-body">
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
                    <input
                      type="search"
                      value={scheduleActionSearch}
                      onChange={(event) => setScheduleActionSearch(event.target.value)}
                      placeholder="Search screener by name or id"
                    />
                    <select
                      className="schedule-action-chooser"
                      value={scheduleActionId}
                      onChange={(event) => setScheduleActionId(event.target.value)}
                      size={Math.min(Math.max(filteredScheduledActions.length, 4), 6)}
                    >
                      {groupedScheduledActions.length === 0 ? <option value={scheduleActionId}>No matching screeners</option> : null}
                      {groupedScheduledActions.map((group) => (
                        <optgroup key={group.label} label={group.label}>
                          {group.actions.map((item) => (
                            <option key={item.id} value={item.id}>
                              {item.label} — {formatEstimatedDuration(item.estimated_duration_seconds)}
                            </option>
                          ))}
                        </optgroup>
                      ))}
                    </select>
                  </label>
                  <label className="field">
                    <span>Schedule</span>
                    <select
                      value={scheduleCadence}
                      onChange={(event) => {
                        const nextCadence = event.target.value as ScheduleCadence;
                        scheduleCronDraftRef.current = {
                          ...scheduleCronDraftRef.current,
                          cadence: nextCadence,
                          cadenceTouched: true,
                        };
                        setScheduleCadence(nextCadence);
                        setScheduleCadenceTouched(true);
                      }}
                    >
                      <option value="weekdays">Weekdays (Mon-Fri)</option>
                      <option value="weekly_saturday">Weekly (Saturday)</option>
                      <option value="weekly_sunday">Weekly (Sunday)</option>
                    </select>
                  </label>
                  <label className="field">
                    <span>Time</span>
                    <input
                      type="time"
                      value={scheduleTime}
                      onChange={(event) => {
                        scheduleCronDraftRef.current = {
                          ...scheduleCronDraftRef.current,
                          time: event.target.value,
                          cadenceTouched: true,
                        };
                        setScheduleTime(event.target.value);
                        setScheduleCadenceTouched(true);
                      }}
                      required
                    />
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
                  <label className="field schedule-options-field">
                    <span>Action Options JSON</span>
                    <textarea
                      value={scheduleOptionsJson}
                      onChange={(event) => setScheduleOptionsJson(event.target.value)}
                      rows={8}
                      placeholder='{"reference_date":"{{local_date}}"}'
                    />
                  </label>
                </div>
                <div className="detail-card">
                  <div className="eyebrow">Templates</div>
                  <p className="panel-copy">
                    Cron preview: <code>{scheduleCronPreview}</code>
                  </p>
                  {scheduleHasUnsupportedCron && !scheduleCadenceTouched ? (
                    <p className="file-meta">Existing cron does not match simplified daily/weekly rules yet. Save keeps raw cron unless you change schedule above.</p>
                  ) : null}
                  <p className="panel-copy">
                    Supported date templates: <code>{'{{local_date}}'}</code>, <code>{'{{local_date_minus_7}}'}</code>, <code>{'{{local_date_minus_14}}'}</code>, <code>{'{{local_date_minus_100}}'}</code>, <code>{'{{local_date_minus_465}}'}</code>, <code>{'{{local_date_plus_7}}'}</code>, <code>{'{{local_date_plus_14}}'}</code>.
                  </p>
                  <p className="panel-copy">Action Options JSON may be left blank or set to <code>null</code> when no options are needed.</p>
                  {selectedScheduledAction ? (
                    <p className="panel-copy">
                      Estimated run time: <strong>{formatEstimatedDuration(selectedScheduledAction.estimated_duration_seconds)}</strong> ({describeEstimateSample(selectedScheduledAction.estimate_sample_count)}).
                    </p>
                  ) : null}
                  <p className="panel-copy">Suggested options:</p>
                  <pre className="panel-copy"><code>{suggestedScheduleOptionsJson}</code></pre>
                  {selectedScheduledAction?.fields?.length ? (
                    <p className="panel-copy">Action fields: {selectedScheduledAction.fields.map((field) => field.id).join(", ")}</p>
                  ) : null}
                </div>
              </div>
              <div className="modal-footer">
                <button className="secondary-button" type="button" onClick={resetScheduleForm} disabled={isSavingSchedule}>
                  Clear
                </button>
                <div className="button-row">
                  <button className="ghost-button" type="button" onClick={() => setIsScheduleEditorOpen(false)} disabled={isSavingSchedule}>
                    Cancel
                  </button>
                  <button className="primary-button" type="submit" disabled={isSavingSchedule || isLoadingScheduleConfig}>
                    {isSavingSchedule ? "Saving..." : "Save Schedule"}
                  </button>
                </div>
              </div>
            </form>
          </div>
        </div>
      ) : null}

      {isSchedulerSettingsOpen ? (
        <div className="modal-backdrop" role="presentation" onClick={() => setIsSchedulerSettingsOpen(false)}>
          <div className="modal-shell" role="dialog" aria-modal="true" aria-labelledby="scheduler-settings-title" onClick={(event) => event.stopPropagation()}>
            <div className="modal-content screeners-settings-modal">
              <div className="modal-header">
                <div>
                  <div className="eyebrow">Scheduler settings</div>
                  <h2 id="scheduler-settings-title">Runner Throughput</h2>
                </div>
                <button className="ghost-button" type="button" aria-label="Close scheduler settings" onClick={() => setIsSchedulerSettingsOpen(false)}>
                  Close
                </button>
              </div>
              <div className="modal-body">
                <label className="field">
                  <span>Max Parallel Jobs</span>
                  <input type="number" min={1} max={20} value={maxParallelJobs} onChange={(event) => setMaxParallelJobs(event.target.value)} />
                </label>
                <div className="detail-card">
                  <div className="eyebrow">Host cron</div>
                  <p className="panel-copy">{schedulerCommand || "-"}</p>
                  <p className="file-meta">Host cron should run this command every 5 minutes.</p>
                </div>
              </div>
              <div className="modal-footer">
                <button className="ghost-button" type="button" onClick={() => setIsSchedulerSettingsOpen(false)} disabled={isSavingScheduleSettings}>
                  Cancel
                </button>
                <button className="primary-button" type="button" disabled={isSavingScheduleSettings || isLoadingScheduleConfig} onClick={() => void handleSaveScheduleSettings()}>
                  {isSavingScheduleSettings ? "Saving..." : "Save Settings"}
                </button>
              </div>
            </div>
          </div>
        </div>
      ) : null}

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

type TimelineRowState = "active" | "finished" | "upcoming" | "waiting" | "disabled";
type ScheduleTimelineView = "today" | "schedule";

const MARKET_OPEN_MINUTE_ET = 9 * 60 + 30;
const MARKET_CLOSE_MINUTE_ET = 16 * 60;
const VISIBLE_TIMELINE_MINUTES = 24 * 60 - (MARKET_CLOSE_MINUTE_ET - MARKET_OPEN_MINUTE_ET);
const TIMELINE_TRACK_WIDTH = 1400;
const TIMELINE_HOUR_MARKS = [...Array.from({ length: 10 }, (_, hour) => hour), ...Array.from({ length: 8 }, (_, index) => index + 16)];

type ScheduleTimelineRow = {
  config: ScheduledJobConfig;
  summary: ScheduledJobSummary | null;
  minute: number;
  durationMinutes: number;
  state: TimelineRowState;
  nextOccurrenceAt?: Date;
};

type ScheduleMapGroup = {
  key: ScheduleCadence;
  label: string;
  rows: ScheduleTimelineRow[];
};

function ScheduleTimeline({
  configs,
  summaries,
  isLoading,
  maxParallelJobs,
  isSaving,
  onEdit,
  onMove,
  onNew,
  onOpenSettings,
}: {
  configs: ScheduledJobConfig[];
  summaries: ScheduledJobSummary[];
  isLoading: boolean;
  maxParallelJobs: number;
  isSaving: boolean;
  onEdit: (job: ScheduledJobConfig) => void;
  onMove: (job: ScheduledJobConfig, minute: number) => void;
  onNew: () => void;
  onOpenSettings: () => void;
}) {
  const [searchParams, setSearchParams] = useSearchParams();
  const [query, setQuery] = useState("");
  const [enabledOnly, setEnabledOnly] = useState(false);
  const [now, setNow] = useState(() => new Date());
  const [selectedOccurrence, setSelectedOccurrence] = useState<{ jobId: string; dayKey: string } | null>(null);
  const [dragPreview, setDragPreview] = useState<{ jobId: string; minute: number } | null>(null);
  const timelineScrollRef = useRef<HTMLDivElement | null>(null);
  const hasAutoFocusedNowRef = useRef(false);
  const dragRef = useRef<{ jobId: string; pointerId: number; track: HTMLElement; row: ScheduleTimelineRow } | null>(null);

  useEffect(() => {
    const timer = window.setInterval(() => setNow(new Date()), 60_000);
    return () => window.clearInterval(timer);
  }, []);

  const primaryTimezone = useMemo(() => findPrimaryScheduleTimezone(configs), [configs]);
  const timeline = useMemo(
    () => buildScheduleTimeline(configs, summaries, now, primaryTimezone),
    [configs, summaries, now, primaryTimezone],
  );
  const scheduleView: ScheduleTimelineView = searchParams.get("view") === "schedule" || searchParams.get("view") === "week" ? "schedule" : "today";
  const scheduleMap = useMemo(
    () => buildScheduleMap(configs, summaries, now, primaryTimezone),
    [configs, summaries, now, primaryTimezone],
  );
  const normalizedQuery = query.trim().toLowerCase();
  const visibleRows = timeline.rows.filter((row) => {
    if (enabledOnly && !row.config.enabled) return false;
    if (!normalizedQuery) return true;
    return `${row.config.job_label} ${row.config.job_id} ${row.config.action_id}`.toLowerCase().includes(normalizedQuery);
  });
  const visibleScheduleMap = scheduleMap.map((group) => ({
    ...group,
    rows: group.rows.filter((row) => {
      if (enabledOnly && !row.config.enabled) return false;
      if (!normalizedQuery) return true;
      return `${row.config.job_label} ${row.config.job_id} ${row.config.action_id}`.toLowerCase().includes(normalizedQuery);
    }),
  }));
  const counts = timeline.rows.reduce(
    (result, row) => {
      result[row.state] += 1;
      return result;
    },
    { active: 0, finished: 0, upcoming: 0, waiting: 0, disabled: 0 } as Record<TimelineRowState, number>,
  );
  const selectedRow = selectedOccurrence
    ? (scheduleView === "schedule"
      ? scheduleMap.flatMap((group) => group.rows).find((row) => row.config.job_id === selectedOccurrence.jobId)
      : timeline.rows.find((row) => row.config.job_id === selectedOccurrence.jobId)) ?? null
    : null;
  const visibleNowMinute = toVisibleTimelineMinute(timeline.nowMinute);
  const scrollAnchorMinute = visibleNowMinute ?? MARKET_OPEN_MINUTE_ET;
  const nowPosition = visibleNowMinute == null ? null : `${(visibleNowMinute / VISIBLE_TIMELINE_MINUTES) * 100}%`;
  const nowLabel = formatMinuteOfDay(timeline.nowMinute);
  const scrollToNow = () => {
    const viewport = timelineScrollRef.current;
    if (!viewport) return;
    const labelColumnWidth = viewport.clientWidth <= 768 ? 236 : 300;
    const target = labelColumnWidth + (scrollAnchorMinute / VISIBLE_TIMELINE_MINUTES) * TIMELINE_TRACK_WIDTH - viewport.clientWidth / 2;
    viewport.scrollTo({ left: Math.max(0, target), behavior: "smooth" });
  };
  const selectView = (view: ScheduleTimelineView) => {
    const next = new URLSearchParams(searchParams);
    if (view === "schedule") next.set("view", "schedule");
    else next.delete("view");
    setSearchParams(next, { replace: true });
    setSelectedOccurrence(null);
  };
  const canMoveRow = (row: ScheduleTimelineRow) => (
    !isSaving
    && row.state === "upcoming"
    && row.config.enabled
    && row.config.cron_tz === primaryTimezone
    && parseSimpleScheduleCron(row.config.cron_expr).supported
  );
  const requestMove = (row: ScheduleTimelineRow, minute: number) => {
    const targetMinute = snapTimelineMinute(minute);
    if (targetMinute === row.minute) return;
    if (window.confirm(`Move ${row.config.job_label}'s ${describeScheduleCadence(row.config.cron_expr).toLowerCase()} run from ${formatMinuteOfDay(row.minute)} to ${formatMinuteOfDay(targetMinute)} ${primaryTimezone}?`)) {
      onMove(row.config, targetMinute);
    }
  };
  const dragMinuteForEvent = (event: ReactPointerEvent<HTMLButtonElement>, track: HTMLElement) => {
    const bounds = track.getBoundingClientRect();
    const position = Math.max(0, Math.min(bounds.width, event.clientX - bounds.left));
    return snapTimelineMinute(fromVisibleTimelineMinute((position / bounds.width) * VISIBLE_TIMELINE_MINUTES));
  };
  const handleDragStart = (event: ReactPointerEvent<HTMLButtonElement>, row: ScheduleTimelineRow) => {
    const track = event.currentTarget.closest(".schedule-timeline-track");
    if (!(track instanceof HTMLElement) || !canMoveRow(row)) return;
    event.preventDefault();
    event.stopPropagation();
    event.currentTarget.setPointerCapture(event.pointerId);
    dragRef.current = { jobId: row.config.job_id, pointerId: event.pointerId, track, row };
    setDragPreview({ jobId: row.config.job_id, minute: row.minute });
  };
  const handleDragMove = (event: ReactPointerEvent<HTMLButtonElement>) => {
    const drag = dragRef.current;
    if (!drag || drag.pointerId !== event.pointerId) return;
    setDragPreview({ jobId: drag.jobId, minute: dragMinuteForEvent(event, drag.track) });
  };
  const handleDragEnd = (event: ReactPointerEvent<HTMLButtonElement>) => {
    const drag = dragRef.current;
    if (!drag || drag.pointerId !== event.pointerId) return;
    const minute = dragMinuteForEvent(event, drag.track);
    dragRef.current = null;
    setDragPreview(null);
    requestMove(drag.row, minute);
  };
  const handleDragCancel = (event: ReactPointerEvent<HTMLButtonElement>) => {
    if (dragRef.current?.pointerId !== event.pointerId) return;
    dragRef.current = null;
    setDragPreview(null);
  };
  const handleMoveKeyDown = (event: ReactKeyboardEvent<HTMLButtonElement>, row: ScheduleTimelineRow) => {
    if (!canMoveRow(row) || (event.key !== "ArrowLeft" && event.key !== "ArrowRight")) return;
    event.preventDefault();
    requestMove(row, moveTimelineMinuteByQuarter(row.minute, event.key === "ArrowRight" ? 1 : -1));
  };

  useEffect(() => {
    if (isLoading || visibleRows.length === 0 || hasAutoFocusedNowRef.current) return;
    hasAutoFocusedNowRef.current = true;
    window.requestAnimationFrame(() => {
      const viewport = timelineScrollRef.current;
      if (!viewport) return;
      const labelColumnWidth = viewport.clientWidth <= 768 ? 236 : 300;
      const target = labelColumnWidth + (scrollAnchorMinute / VISIBLE_TIMELINE_MINUTES) * TIMELINE_TRACK_WIDTH - viewport.clientWidth / 2;
      viewport.scrollLeft = Math.max(0, target);
    });
  }, [isLoading, visibleRows.length]);

  useEffect(() => {
    if (!selectedRow) return;
    const handleEscape = (event: KeyboardEvent) => {
      if (event.key === "Escape") setSelectedOccurrence(null);
    };
    window.addEventListener("keydown", handleEscape);
    return () => window.removeEventListener("keydown", handleEscape);
  }, [selectedRow]);

  return (
    <section className="schedule-timeline-panel" aria-labelledby="schedule-timeline-title">
      <header className="schedule-timeline-header">
        <div>
          <h2 id="schedule-timeline-title">{scheduleView === "schedule" ? "Schedule Map" : "Today’s Automation Timeline"}</h2>
          <p>
            {scheduleView === "schedule"
              ? `Primary: ${primaryTimezone} · Reference: Pacific/Auckland · 09:30–16:00 ET hidden`
              : `${formatTimelineDate(now, primaryTimezone)} · ${primaryTimezone} · 09:30–16:00 market session hidden`}
          </p>
        </div>
        <div className="button-row">
          <button className="ghost-button" type="button" onClick={onOpenSettings}>Runner Settings</button>
          <button className="primary-button" type="button" onClick={onNew}>New Schedule</button>
        </div>
      </header>

      <div className="schedule-timeline-controls">
        <div className="schedule-timeline-stats" aria-label="Schedule status summary">
          <span className="timeline-stat timeline-stat-finished"><span aria-hidden="true" />{counts.finished} finished</span>
          <span className="timeline-stat timeline-stat-active"><span aria-hidden="true" />{counts.active} active</span>
          <span className="timeline-stat timeline-stat-upcoming"><span aria-hidden="true" />{counts.upcoming} upcoming</span>
          {counts.waiting ? <span className="timeline-stat timeline-stat-waiting"><span aria-hidden="true" />{counts.waiting} awaiting run</span> : null}
          {counts.disabled ? <span className="timeline-stat"><span aria-hidden="true" />{counts.disabled} paused</span> : null}
        </div>
        <div className="schedule-timeline-filters">
          <div className="schedule-view-toggle" role="group" aria-label="Schedule view">
            <button className={scheduleView === "today" ? "schedule-view-button schedule-view-button-active" : "schedule-view-button"} type="button" onClick={() => selectView("today")}>Today</button>
            <button className={scheduleView === "schedule" ? "schedule-view-button schedule-view-button-active" : "schedule-view-button"} type="button" onClick={() => selectView("schedule")}>Schedule map</button>
          </div>
          {scheduleView === "today" ? (
            <button className="ghost-button schedule-now-button" type="button" onClick={scrollToNow}>
              {visibleNowMinute == null ? "Jump to 16:00" : "Jump to now"}
            </button>
          ) : null}
          <label className="schedule-search-field">
            <span className="visually-hidden">Filter scheduled tasks</span>
            <input type="search" value={query} onChange={(event) => setQuery(event.target.value)} placeholder="Filter task or screener" />
          </label>
          <label className="schedule-enabled-filter">
            <input type="checkbox" checked={enabledOnly} onChange={(event) => setEnabledOnly(event.target.checked)} />
            Enabled only
          </label>
        </div>
      </div>

      {isLoading ? <LoadingBlock label={scheduleView === "schedule" ? "Building schedule map…" : "Building today’s timeline…"} compact /> : null}
      {!isLoading && scheduleView === "today" && timeline.rows.length === 0 ? (
        <div className="schedule-timeline-empty">
          <strong>No clock-based schedules run today.</strong>
          <span>Custom cron rules remain available in Schedule Definitions below.</span>
        </div>
      ) : !isLoading && scheduleView === "today" ? (
        <div ref={timelineScrollRef} className="schedule-timeline-scroll" tabIndex={0} aria-label="Scrollable schedule timeline with regular US market hours hidden">
          <div className="schedule-timeline-canvas">
            <div className="schedule-timeline-corner">
              <span>Scheduled automation</span>
              <span>{visibleRows.length} shown</span>
            </div>
            <div className="schedule-timeline-hours" aria-hidden="true">
              {TIMELINE_HOUR_MARKS.map((hour) => (
                <span className="schedule-timeline-hour" style={{ left: `${(toVisibleTimelineMinute(hour * 60)! / VISIBLE_TIMELINE_MINUTES) * 100}%` }} key={hour}>
                  {String(hour).padStart(2, "0")}:00
                </span>
              ))}
              <span className="schedule-market-hours-break" style={{ left: `${(MARKET_OPEN_MINUTE_ET / VISIBLE_TIMELINE_MINUTES) * 100}%` }}>
                <span>09:30–16:00 hidden</span>
              </span>
              {nowPosition ? (
                <span className="schedule-now-marker schedule-now-marker-header" style={{ left: nowPosition }}>
                  <span>{nowLabel} now</span>
                </span>
              ) : null}
            </div>

            {visibleRows.map((row) => {
              const visibleMinute = toVisibleTimelineMinute(row.minute)!;
              const widthMinutes = getVisibleTimelineDuration(row.minute, row.durationMinutes);
              const barTone = row.state === "finished" && row.summary?.status === "failed" ? "failed" : row.state;
              const barStyle = {
                "--timeline-start": `${(visibleMinute / VISIBLE_TIMELINE_MINUTES) * 100}%`,
                "--timeline-width": `${(widthMinutes / VISIBLE_TIMELINE_MINUTES) * 100}%`,
              } as CSSProperties;
              return (
                <div className="schedule-timeline-row" key={row.config.job_id}>
                  <button className="schedule-timeline-task" type="button" onClick={() => setSelectedOccurrence({ jobId: row.config.job_id, dayKey: "today" })}>
                    <span className="schedule-task-title">{row.config.job_label}</span>
                    <span className="schedule-task-meta">{row.config.action_id} · {formatMinuteOfDay(row.minute)}</span>
                  </button>
                  <div className="schedule-timeline-track">
                    {nowPosition ? <span className="schedule-now-marker" style={{ left: nowPosition }} aria-hidden="true" /> : null}
                    {dragPreview?.jobId === row.config.job_id ? (
                      <span
                        className="schedule-timeline-drag-preview"
                        style={{ "--timeline-start": `${(toVisibleTimelineMinute(dragPreview.minute)! / VISIBLE_TIMELINE_MINUTES) * 100}%` } as CSSProperties}
                        aria-hidden="true"
                      >
                        {formatMinuteOfDay(dragPreview.minute)}
                      </span>
                    ) : null}
                    <div className={`schedule-timeline-bar schedule-timeline-bar-${barTone}`} style={barStyle}>
                      <button
                        className="schedule-timeline-bar-content"
                        type="button"
                        onClick={() => setSelectedOccurrence({ jobId: row.config.job_id, dayKey: "today" })}
                        aria-label={`Inspect ${row.config.job_label}, ${timelineStateLabel(row.state)} at ${formatMinuteOfDay(row.minute)}`}
                      >
                        <span>{formatMinuteOfDay(row.minute)}</span>
                        <span>{timelineBarDetail(row)}</span>
                      </button>
                      {canMoveRow(row) ? (
                        <button
                          className="schedule-timeline-drag-handle"
                          type="button"
                          aria-label={`Move ${row.config.job_label}; drag or use left and right arrow keys in 15-minute steps`}
                          title="Move task in 15-minute steps"
                          onPointerDown={(event) => handleDragStart(event, row)}
                          onPointerMove={handleDragMove}
                          onPointerUp={handleDragEnd}
                          onPointerCancel={handleDragCancel}
                          onKeyDown={(event) => handleMoveKeyDown(event, row)}
                        >
                          ↔
                        </button>
                      ) : null}
                    </div>
                  </div>
                </div>
              );
            })}
          </div>
        </div>
      ) : !isLoading ? (
        <ScheduleMap
          groups={visibleScheduleMap}
          primaryTimezone={primaryTimezone}
          onInspect={(jobId) => setSelectedOccurrence({ jobId, dayKey: "schedule" })}
          canMoveRow={canMoveRow}
          dragPreview={dragPreview}
          onDragStart={handleDragStart}
          onDragMove={handleDragMove}
          onDragEnd={handleDragEnd}
          onDragCancel={handleDragCancel}
          onMoveKeyDown={handleMoveKeyDown}
        />
      ) : null}

      {selectedRow ? (
        <ScheduleTaskInspector
          row={selectedRow}
          timezone={primaryTimezone}
          now={now}
          onClose={() => setSelectedOccurrence(null)}
          onEdit={() => {
            setSelectedOccurrence(null);
            onEdit(selectedRow.config);
          }}
        />
      ) : null}

      <footer className="schedule-timeline-footer">
        <span><strong>{maxParallelJobs || "—"}</strong> max parallel jobs</span>
        <span>{scheduleView === "schedule" ? "Weekday, Saturday, and Sunday recurring rules" : `${timeline.customCount} custom or non-today rules shown below`}</span>
        {scheduleView === "schedule" ? <span>NZ time reflects each task’s next occurrence</span> : timeline.marketHoursHiddenCount ? <span>{timeline.marketHoursHiddenCount} market-hour schedules hidden</span> : null}
        <span>Status uses the latest scheduler record; duration uses recent-run median.</span>
      </footer>
    </section>
  );
}

function ScheduleMap({
  groups,
  primaryTimezone,
  onInspect,
  canMoveRow,
  dragPreview,
  onDragStart,
  onDragMove,
  onDragEnd,
  onDragCancel,
  onMoveKeyDown,
}: {
  groups: ScheduleMapGroup[];
  primaryTimezone: string;
  onInspect: (jobId: string) => void;
  canMoveRow: (row: ScheduleTimelineRow) => boolean;
  dragPreview: { jobId: string; minute: number } | null;
  onDragStart: (event: ReactPointerEvent<HTMLButtonElement>, row: ScheduleTimelineRow) => void;
  onDragMove: (event: ReactPointerEvent<HTMLButtonElement>) => void;
  onDragEnd: (event: ReactPointerEvent<HTMLButtonElement>) => void;
  onDragCancel: (event: ReactPointerEvent<HTMLButtonElement>) => void;
  onMoveKeyDown: (event: ReactKeyboardEvent<HTMLButtonElement>, row: ScheduleTimelineRow) => void;
}) {
  const scheduledCount = groups.reduce((count, group) => count + group.rows.length, 0);
  if (!scheduledCount) {
    return (
      <div className="schedule-timeline-empty">
        <strong>No simple weekday, Saturday, or Sunday schedules match this filter.</strong>
        <span>Custom cron rules remain available in Schedule Definitions below.</span>
      </div>
    );
  }
  return (
    <div className="schedule-map" aria-label="Recurring schedule map">
      {groups.map((group) => (
        <section className="schedule-map-group" key={group.key}>
          <header>
            <div>
              <h3>{group.label}</h3>
              <p>{group.key === "weekdays" ? "Runs Monday through Friday" : `Runs every ${group.label}`}</p>
            </div>
            <span>{group.rows.length} task{group.rows.length === 1 ? "" : "s"}</span>
          </header>
          {group.rows.length ? (
            <div className="schedule-map-scroll">
              <div className="schedule-map-canvas">
                <div className="schedule-map-corner">Task · ET / NZ time</div>
                <div className="schedule-timeline-hours" aria-hidden="true">
                  {TIMELINE_HOUR_MARKS.map((hour) => (
                    <span className="schedule-timeline-hour" style={{ left: `${(toVisibleTimelineMinute(hour * 60)! / VISIBLE_TIMELINE_MINUTES) * 100}%` }} key={hour}>
                      {String(hour).padStart(2, "0")}:00
                    </span>
                  ))}
                  <span className="schedule-market-hours-break" style={{ left: `${(MARKET_OPEN_MINUTE_ET / VISIBLE_TIMELINE_MINUTES) * 100}%` }}>
                    <span>09:30–16:00 hidden</span>
                  </span>
                </div>
                {group.rows.map((row) => {
                  const visibleMinute = toVisibleTimelineMinute(row.minute)!;
                  const widthMinutes = getVisibleTimelineDuration(row.minute, row.durationMinutes);
                  const barTone = row.state === "finished" && row.summary?.status === "failed" ? "failed" : row.state;
                  const barStyle = {
                    "--timeline-start": `${(visibleMinute / VISIBLE_TIMELINE_MINUTES) * 100}%`,
                    "--timeline-width": `${(widthMinutes / VISIBLE_TIMELINE_MINUTES) * 100}%`,
                  } as CSSProperties;
                  return (
                    <div className="schedule-timeline-row" key={row.config.job_id}>
                      <button className="schedule-timeline-task" type="button" onClick={() => onInspect(row.config.job_id)}>
                        <span className="schedule-task-title">{row.config.job_label}</span>
                        <span className="schedule-task-meta">{row.config.action_id} · {formatScheduleMapTime(row, primaryTimezone)}</span>
                      </button>
                      <div className="schedule-timeline-track">
                        {dragPreview?.jobId === row.config.job_id ? (
                          <span
                            className="schedule-timeline-drag-preview"
                            style={{ "--timeline-start": `${(toVisibleTimelineMinute(dragPreview.minute)! / VISIBLE_TIMELINE_MINUTES) * 100}%` } as CSSProperties}
                            aria-hidden="true"
                          >
                            {formatMinuteOfDay(dragPreview.minute)}
                          </span>
                        ) : null}
                        <div className={`schedule-timeline-bar schedule-timeline-bar-${barTone}`} style={barStyle}>
                          <button className="schedule-timeline-bar-content" type="button" onClick={() => onInspect(row.config.job_id)} aria-label={`Inspect ${row.config.job_label} at ${formatScheduleMapTime(row, primaryTimezone)}`}>
                            <span>{formatMinuteOfDay(row.minute)} ET</span>
                            <span>{timelineExpectedFinish(row) ?? "Scheduled"}</span>
                          </button>
                          {canMoveRow(row) ? (
                            <button
                              className="schedule-timeline-drag-handle"
                              type="button"
                              aria-label={`Move ${row.config.job_label}; drag or use left and right arrow keys in 15-minute steps`}
                              title="Move task in 15-minute steps"
                              onPointerDown={(event) => onDragStart(event, row)}
                              onPointerMove={onDragMove}
                              onPointerUp={onDragEnd}
                              onPointerCancel={onDragCancel}
                              onKeyDown={(event) => onMoveKeyDown(event, row)}
                            >
                              ↔
                            </button>
                          ) : null}
                        </div>
                      </div>
                    </div>
                  );
                })}
              </div>
            </div>
          ) : <p className="schedule-map-empty">No matching tasks</p>}
        </section>
      ))}
    </div>
  );
}

function ScheduleTaskInspector({
  row,
  timezone,
  now,
  onClose,
  onEdit,
}: {
  row: ScheduleTimelineRow;
  timezone: string;
  now: Date;
  onClose: () => void;
  onEdit: () => void;
}) {
  const summary = row.summary;
  const isFailed = row.state === "finished" && summary?.status === "failed";
  const isSuccessful = row.state === "finished" && summary?.status === "success";
  const shouldShowLog = row.state === "active" || isFailed;
  const [logText, setLogText] = useState("");
  const [isLogLoading, setIsLogLoading] = useState(false);
  const [logNotice, setLogNotice] = useState("");
  const logCursorRef = useRef<number | null>(null);
  const resultHref = isSuccessful && summary?.persisted_to_db === true && summary.screen_run_id != null
    ? `/scanner/${encodeURIComponent(row.config.action_id)}`
    : null;
  const startedAt = summary?.last_started_at ? new Date(summary.last_started_at) : null;
  const finishedAt = summary?.last_finished_at ? new Date(summary.last_finished_at) : null;
  const actualSeconds = startedAt && finishedAt ? Math.max(0, Math.round((finishedAt.getTime() - startedAt.getTime()) / 1000)) : null;
  const expectedFinish = timelineExpectedFinish(row, timezone);
  const failureReason = summary?.message || summary?.persistence_message || (summary?.exit_code != null ? `Exited with code ${summary.exit_code}.` : "The scheduler did not provide a failure reason.");

  useEffect(() => {
    logCursorRef.current = null;
    setLogText("");
    setLogNotice("");
    if (!shouldShowLog) return;
    let ignore = false;
    const loadLog = () => {
      setIsLogLoading(true);
      const cursor = logCursorRef.current;
      const query = cursor == null ? "" : `?cursor=${cursor}`;
      void fetchJson<ScheduledJobLogResponse>(`/api/admin/scheduled-jobs/${encodeURIComponent(row.config.job_id)}/log${query}`)
        .then((payload) => {
          if (ignore) return;
          const replaceText = cursor == null || payload.reset;
          setLogText((current) => replaceText ? payload.text : [current, payload.text].filter(Boolean).join("\n"));
          logCursorRef.current = payload.next_cursor;
          setLogNotice(payload.available ? (payload.truncated && replaceText ? "Showing the latest log output." : "") : "No scheduler log is available yet.");
        })
        .catch(() => {
          if (!ignore) setLogNotice("Unable to load the scheduler log.");
        })
        .finally(() => {
          if (!ignore) setIsLogLoading(false);
        });
    };
    loadLog();
    if (!isFailed) {
      const timer = window.setInterval(loadLog, 2_000);
      return () => {
        ignore = true;
        window.clearInterval(timer);
      };
    }
    return () => {
      ignore = true;
    };
  }, [isFailed, row.config.job_id, row.state, summary?.last_started_at, shouldShowLog]);

  return (
    <aside className="schedule-task-inspector" aria-labelledby="schedule-task-inspector-title">
      <div className="schedule-task-inspector-header">
        <div>
          <span className="eyebrow">Task inspector</span>
          <h3 id="schedule-task-inspector-title">{row.config.job_label}</h3>
          <p>{row.config.action_id} · {formatMinuteOfDay(row.minute)} · {timezone}</p>
        </div>
        <button className="ghost-button" type="button" onClick={onClose} aria-label="Close task inspector">Close</button>
      </div>

      <div className="schedule-task-inspector-details">
        <div>
          <span>Status</span>
          <strong className={`schedule-task-status schedule-task-status-${isFailed ? "failed" : row.state}`}>{timelineStateLabel(row.state)}</strong>
        </div>
        <div>
          <span>{isSuccessful ? "Finished" : row.state === "active" ? "Expected finish" : "Typical duration"}</span>
          <strong>{isSuccessful && finishedAt ? formatTimelineTimestamp(finishedAt, timezone) : row.state === "active" ? expectedFinish ?? "ETA unavailable" : formatEstimatedDuration(row.config.estimated_duration_seconds)}</strong>
        </div>
        <div>
          <span>{isSuccessful ? "Actual duration" : row.state === "active" ? "Elapsed" : "Next trigger"}</span>
          <strong>{isSuccessful ? (actualSeconds ? formatEstimatedDuration(actualSeconds).replace("~", "") : "Unavailable") : row.state === "active" ? (startedAt ? formatElapsedDuration(now.getTime() - startedAt.getTime()) : "Not reported") : formatMinuteOfDay(row.minute)}</strong>
        </div>
      </div>

      {isFailed ? <p className="schedule-task-inspector-message schedule-task-inspector-message-failed">{failureReason}</p> : null}
      {row.state === "active" ? <p className="schedule-task-inspector-message">Schedule status refreshes every 15 seconds. Log output refreshes every 2 seconds.</p> : null}
      {isSuccessful && !resultHref ? <p className="schedule-task-inspector-message">The task finished, but no persisted scanner result is available yet.</p> : null}

      {shouldShowLog ? (
        <section className="schedule-task-log" aria-label="Scheduler log output">
          <div className="schedule-task-log-header">
            <span>Scheduler log</span>
            {isLogLoading ? <span>Updating…</span> : null}
          </div>
          <pre>{logText || logNotice || "Waiting for log output…"}</pre>
          {logText && logNotice ? <p>{logNotice}</p> : null}
        </section>
      ) : null}

      <div className="button-row schedule-task-inspector-actions">
        {resultHref ? <Link className="primary-button" to={resultHref}>Open latest scanner result</Link> : null}
        <button className="ghost-button" type="button" onClick={onEdit}>Edit schedule</button>
      </div>
    </aside>
  );
}

function ScreenersSubnav({ activeMode }: { activeMode: RunsPageMode }) {
  const isSchedules = activeMode === "schedules";

  return (
    <section className="panel screeners-subnav-panel">
      <div className="screeners-subnav-copy">
        {!isSchedules ? <span className="eyebrow">Screeners Workspace</span> : null}
        <h1>{isSchedules ? "Automation" : "Run Center"}</h1>
        <p className="panel-copy">
          {isSchedules
            ? "Scheduled screeners, cron config, and scheduler controls live here."
            : "Launch scans, watch active jobs, inspect history, and open fresh results."}
        </p>
      </div>
      <div className="screeners-subnav-links" role="tablist" aria-label="Screeners sections">
        <NavLink
          to="/screeners"
          end
          className={({ isActive }) => `screeners-subnav-link${isActive ? " is-active" : ""}`}
        >
          Run Center
        </NavLink>
        <NavLink
          to="/screeners/schedules"
          className={({ isActive }) => `screeners-subnav-link${isActive ? " is-active" : ""}`}
        >
          Automation
        </NavLink>
      </div>
    </section>
  );
}

function isoDateDaysAgo(daysAgo: number): string {
  const value = new Date();
  value.setDate(value.getDate() - daysAgo);
  return value.toISOString().slice(0, 10);
}

type LiveJob = JobsResponse["jobs"][number] | JobsResponse["jobs"][number]["child_jobs"][number];

type JobStreamSnapshotEvent = {
  job: LiveJob;
  cursor: number;
  recent_lines: string[];
};

type JobStreamLogEvent = {
  job_id: string;
  cursor: number;
  line: string;
};

type JobStreamStatusEvent = {
  job: LiveJob;
  cursor: number;
};

const MAX_CACHED_LOG_LINES = 5000;
const jobConsoleLineCache = new Map<string, string[]>();

function useJobStream(streamPath: string | null, enabled: boolean) {
  const [job, setJob] = useState<LiveJob | null>(null);
  const [lines, setLines] = useState<string[]>(() => (streamPath ? jobConsoleLineCache.get(streamPath) ?? [] : []));
  const [logTail, setLogTail] = useState("");
  const [connected, setConnected] = useState(false);

  useEffect(() => {
    setJob(null);
    const cachedLines = streamPath ? jobConsoleLineCache.get(streamPath) ?? [] : [];
    setLines(cachedLines);
    setLogTail(cachedLines.length > 0 ? cachedLines.join("\n") : "");
    setConnected(false);

    if (!streamPath || !enabled) {
      return;
    }

    const url = new URL(streamPath, window.location.origin);
    const source = new EventSource(url.toString(), { withCredentials: true });

    source.addEventListener("snapshot", (event) => {
      const payload = JSON.parse((event as MessageEvent).data) as JobStreamSnapshotEvent;
      setJob(payload.job);
      const snapshotLines = payload.recent_lines ?? [];
      setLines((current) => {
        const next = mergeConsoleLines(current, snapshotLines);
        jobConsoleLineCache.set(streamPath, next);
        setLogTail(next.length > 0 ? next.join("\n") : payload.job?.log_tail || "");
        return next;
      });
      setConnected(true);
    });

    source.addEventListener("log", (event) => {
      const payload = JSON.parse((event as MessageEvent).data) as JobStreamLogEvent;
      setLines((current) => {
        const next = mergeConsoleLines(current, [payload.line]);
        jobConsoleLineCache.set(streamPath, next);
        setLogTail(next.join("\n"));
        return next;
      });
      setJob((current) => {
        if (!current) {
          return current;
        }
        const currentLog = typeof current.log_tail === "string" ? current.log_tail : "";
        const nextLog = currentLog ? `${currentLog}\n${payload.line}` : payload.line;
        return {
          ...current,
          log_tail: nextLog.split("\n").slice(-200).join("\n"),
        };
      });
      setConnected(true);
    });

    source.addEventListener("status", (event) => {
      const payload = JSON.parse((event as MessageEvent).data) as JobStreamStatusEvent;
      setJob(payload.job);
      const statusLines = payload.job?.log_tail ? payload.job.log_tail.split("\n").filter(Boolean) : [];
      if (statusLines.length > 0) {
        setLines((current) => {
          const next = mergeConsoleLines(current, statusLines);
          jobConsoleLineCache.set(streamPath, next);
          setLogTail(next.join("\n"));
          return next;
        });
      }
      setConnected(true);
    });

    source.addEventListener("eof", (event) => {
      const payload = JSON.parse((event as MessageEvent).data) as JobStreamStatusEvent;
      setJob(payload.job);
      const finalLines = payload.job?.log_tail ? payload.job.log_tail.split("\n").filter(Boolean) : [];
      if (finalLines.length > 0) {
        setLines((current) => {
          const next = mergeConsoleLines(current, finalLines);
          jobConsoleLineCache.set(streamPath, next);
          setLogTail(next.join("\n"));
          return next;
        });
      }
      setConnected(false);
      source.close();
    });

    source.onerror = () => {
      setConnected(false);
    };

    return () => {
      source.close();
      setConnected(false);
    };
  }, [enabled, streamPath]);

  return { job, lines, logTail, connected };
}

function mergeConsoleLines(current: string[], incoming: string[]): string[] {
  const normalizedIncoming = incoming.map((line) => line.replace(/\r$/, "")).filter((line) => line.length > 0);
  if (normalizedIncoming.length === 0) {
    return current;
  }
  if (current.length === 0) {
    return normalizedIncoming.slice(-MAX_CACHED_LOG_LINES);
  }

  const maxOverlap = Math.min(current.length, normalizedIncoming.length, 200);
  let overlap = 0;
  for (let size = maxOverlap; size > 0; size -= 1) {
    let matches = true;
    for (let index = 0; index < size; index += 1) {
      if (current[current.length - size + index] !== normalizedIncoming[index]) {
        matches = false;
        break;
      }
    }
    if (matches) {
      overlap = size;
      break;
    }
  }

  const next = current.concat(normalizedIncoming.slice(overlap));
  return next.slice(-MAX_CACHED_LOG_LINES);
}

function liveJobDisplayLabel(job: LiveJob | null): string {
  if (!job) {
    return "";
  }
  if ("strategy_id" in job) {
    return job.strategy_id || job.label;
  }
  return job.label;
}

function describeConsoleRunId(job: LiveJob): string {
  if ("job_run_id" in job && job.job_run_id != null) {
    return `#run-${job.job_run_id}`;
  }
  if ("job_id" in job) {
    return `#job-${job.job_id}`;
  }
  return "#job";
}

function describeScreenerAction(actionId: string, hasConfig: boolean): string {
  const catalog: Record<string, string> = {
    rs: "High relative-strength leaders still holding momentum before price fully extends.",
    daily_rs_new_high: "Daily relative-strength leaders making fresh RS highs whether or not price is already at a matching high.",
    rs_phase: "Daily relative-strength phase leaders with the RS line above its 21 EMA for sustained sponsorship.",
    weekly_rs_new_high: "Weekly relative-strength leaders making fresh RS highs whether or not price has already matched the move.",
    weekly_rs_before_price: "Weekly RS leaders with room before price catches up to relative strength.",
    vcp: "Volatility contraction setups tightening into potential breakout pivots.",
    vcp_v3: "Swing-based VCP scanner separating pre-breakout coils from fresh breakouts with contraction quality, dry-up, RS, and risk-reward context.",
    sepa_vcp: "SEPA names passing the Minervini trend template with pressure, risk, and relative-performance context.",
    trend_template:
      'This screen is based on the Trend Template (TTP) by 2 times US Investing Champion Mark Minervini. He uses the Trend Template as the first step for his stock selection. The criteria are described in his book "Think and trade like a stock market wizard" : The current stock price is above both the 150-day (30-week) and the 200-day (40-week) moving average price lines. The 150-day moving average is above the 200-day moving average. The 200-day moving average line is trending up for at least 1 month (preferably 4–5 months minimum in most cases). The 50-day (10-week) moving average is above both the 150-day and 200-day moving averages. The current stock price is trading above the 50-day moving average. The current stock price is at least 30% above its 52-week low. The current stock price is within at least 25% of its 52-week high (the closer to a new high the better). The Relative Strength ranking (RS ranking) is no less than 70.',
    weekly_candidate_pool:
      "Weekly candidate universe only, not an entry signal: price above $10 and 50 EMA, ADR20 above 4%, Daily RS above 90, 10 EMA above 20 EMA, and at least 70% above the 52-week low. Review charts, mark bases, then wait for a setup.",
    best_winners:
      "Liquid momentum winners: price above $1, ADR20 above 4.5%, at least 70% above the 52-week low, positive 3M/6M/1Y performance, 30D average dollar volume above $50M, current dollar volume above $20M, 8 EMA above 21 EMA, and price above 60 EMA.",
    kai_s1:
      "Weekly trend leaders: price above weekly 30/40 SMA and daily 50 SMA, weekly 30 SMA above weekly 40 SMA, price above $5, market cap above $10B, session dollar volume above $100M, and ADR20 above 2%.",
    kai_s2:
      "Liquid high-beta trend leaders with price above 20/50/200 SMA, 150D SMA above 200D SMA, market cap above $1B, avg volume above 2M, and close x 30D avg volume above $100M.",
    one_year_winners:
      "Rolling 1-year winners above 30% with market cap above $10B, positive TTM revenue growth, beta above 1, monthly dollar liquidity above $900M, price above $20 and 100 EMA, and 21 EMA above 50 SMA.",
    qullamaggie:
      "Momentum candidates up at least 30% over 3 or 6 months, near recent and 52-week highs, above 10/20/50/200 SMA, with 1M+ average volume, $100M+ market cap, ADR20 of 4%+, strong Daily RS, and a tight flag or pullback near the 10/20 SMA.",
    gap_fill: "Post-earnings gap reversal candidates with reclaim or fill behavior.",
    macd_golden_cross: "Fresh bullish MACD crossovers where the MACD line has recently moved above the signal line.",
    inside_dryup_v2: "Latest inside day plus extreme price-volume dry-up, without requiring breakout follow-through.",
    wyckoff_buy_signal: "Wyckoff accumulation-trigger scan for fresh BUY signals coming out of spring, LPS, or phase progression.",
    wyckoff_sell_signal: "Wyckoff distribution-trigger scan for fresh SELL signals coming out of UTAD, LPSY, or phase deterioration.",
    ftd_sweep:
      "Recent follow-through-day sweep reclaims where price undercut the FTD trigger, then quickly reclaimed and held back above it within the configured window.",
    high_tight_flag_setup: "Pre-breakout HTF setups with a completed pole, tight upper-range flag, volume dry-up, and constructive trend context.",
    leif_high_tight_flag: "Leif Soreide HTF breakout setup with scored pole, flag, volume, RS, and breakout rules.",
    weekly_tight_close: "Three weekly bars with ATR-scaled tight closes plus tight highs or lows, while first bar still passes wick and range filter.",
    weinstein_stage2_early: "Weekly regime names that just shifted from Stage 1 base into early Stage 2 advance above a rising 30-week EMA band.",
    weinstein_stage_analysis: "Full weekly Weinstein classification using the referenced 30-week EMA rules: Stage 1, Stage 2A/2B/2C, Stage 3, and Stage 4.",
    weekly_tight_close_breakout: "Names already breaking above the three-week tight-close box after the weekly tight-close detector formed.",
    ema21_pullback_buy: "Strict-uptrend leaders that tag the 21 EMA, hold the close, then fire first bullish break above the test-candle high.",
    sma200_pullback_buy: "Long-trend leaders that test the 200 SMA from above, hold the close, then reclaim through the test-candle high.",
    fearzone: "High-velocity panic reversals where snapback asymmetry may appear.",
    td9_bullish: "Bullish TD Sequential exhaustion names where downside pressure may be spent.",
    sean_breakout: "Daily leaders already clearing key EMA, volume, and ADR thresholds.",
    rsi_ma_bb_bullish: "RSI and MA/Bollinger bullish trigger set for early continuation entries.",
    bollinger_band_breakout: "Names already closing above the upper Bollinger Band, useful for tracking short-term extension pressure.",
    double_bottom_detection:
      "Active double-bottom bases still building below the middle-high breakout pivot, before a confirmed breakout invalidates the setup-stage scan.",
    rsi_ma_bb_bearish: "Bearish RSI and MA/Bollinger trigger set for breakdown continuation.",
    near_200ma: "Names testing the 200 day moving average with clear inflection context.",
    near_52wk_high: "Names trading from flat to 20% below their rolling 52-week high, useful for hunting leadership that stays close to highs.",
    lost_21ema: "Recent loss of 21 EMA support while longer trend structure still matters.",
    earnings_growth: "Earnings-led growth names showing improving fundamental acceleration.",
    earnings_weekly_criteria: "Weekly earnings filter using repo criteria for cleaner candidate sets.",
    peg: "PEG-oriented value screen tuned around earnings gap context.",
    legacy_peg: "Legacy PEG variant kept for compatibility and comparison runs.",
    sean_gap_up: "Sean earnings gap-up setup tuned around post-earnings power and continuation context.",
  };
  if (catalog[actionId]) {
    return catalog[actionId];
  }
  return hasConfig ? "Run with defaults now, or open config for custom parameters." : "Run immediately with the default screener settings.";
}

function describeScheduleCadence(cronExpr: string): string {
  const normalized = cronExpr.trim().split(/\s+/);
  if (normalized.length !== 5) {
    return "Custom cadence";
  }
  const [, hour, dayOfMonth, month, dayOfWeek] = normalized;
  if (dayOfMonth === "*" && month === "*" && dayOfWeek === "1-5") {
    return "Weekdays";
  }
  if (dayOfMonth === "*" && month === "*" && dayOfWeek === "*") {
    return "Daily";
  }
  if (dayOfMonth === "*" && month === "*" && dayOfWeek.includes(",")) {
    return "Selected weekdays";
  }
  if (dayOfMonth !== "*" && month === "*") {
    return "Monthly";
  }
  if (dayOfMonth !== "*" && month !== "*") {
    return "Calendar date";
  }
  if (hour === "*" || hour.includes("/")) {
    return "Intraday cadence";
  }
  return "Custom cadence";
}

function findPrimaryScheduleTimezone(configs: ScheduledJobConfig[]): string {
  const counts = new Map<string, number>();
  configs.forEach((config) => counts.set(config.cron_tz, (counts.get(config.cron_tz) ?? 0) + 1));
  return Array.from(counts.entries()).sort((left, right) => right[1] - left[1])[0]?.[0] || "America/New_York";
}

function buildScheduleTimeline(
  configs: ScheduledJobConfig[],
  summaries: ScheduledJobSummary[],
  now: Date,
  primaryTimezone: string,
): { rows: ScheduleTimelineRow[]; customCount: number; marketHoursHiddenCount: number; nowMinute: number } {
  const summaryByJobId = new Map(summaries.map((summary) => [summary.job_id, summary]));
  const primaryParts = getZonedDateParts(now, primaryTimezone);
  const nowMinute = primaryParts.hour * 60 + primaryParts.minute;
  const rows: ScheduleTimelineRow[] = [];
  let customCount = 0;
  let marketHoursHiddenCount = 0;

  configs.forEach((config) => {
    const clock = parseTimelineCron(config.cron_expr, now, config.cron_tz);
    if (!clock) {
      customCount += 1;
      return;
    }
    const sourceOffset = getTimezoneOffsetMinutes(now, config.cron_tz);
    const primaryOffset = getTimezoneOffsetMinutes(now, primaryTimezone);
    const minute = normalizeMinuteOfDay(clock.hour * 60 + clock.minute - sourceOffset + primaryOffset);
    if (isRegularMarketMinute(minute)) {
      marketHoursHiddenCount += 1;
      return;
    }
    const summary = summaryByJobId.get(config.job_id) ?? null;
    rows.push({
      config,
      summary,
      minute,
      durationMinutes: Math.max((config.estimated_duration_seconds ?? 0) / 60, 8),
      state: getTimelineRowState(config, summary, minute, nowMinute, now),
    });
  });

  rows.sort((left, right) => left.minute - right.minute || left.config.job_label.localeCompare(right.config.job_label));
  return { rows, customCount, marketHoursHiddenCount, nowMinute };
}

function buildScheduleMap(
  configs: ScheduledJobConfig[],
  summaries: ScheduledJobSummary[],
  now: Date,
  primaryTimezone: string,
): ScheduleMapGroup[] {
  const summaryByJobId = new Map(summaries.map((summary) => [summary.job_id, summary]));
  const groups: ScheduleMapGroup[] = [
    { key: "weekdays", label: "Weekdays", rows: [] },
    { key: "weekly_saturday", label: "Saturday", rows: [] },
    { key: "weekly_sunday", label: "Sunday", rows: [] },
  ];
  const groupByCadence = new Map(groups.map((group) => [group.key, group]));

  configs.forEach((config) => {
    const cadence = parseSimpleScheduleCron(config.cron_expr);
    if (!cadence.supported) return;
    const occurrence = findNextScheduleOccurrence(config.cron_expr, config.cron_tz, now);
    if (!occurrence) return;
    const primaryParts = getZonedDateParts(occurrence, primaryTimezone);
    const minute = primaryParts.hour * 60 + primaryParts.minute;
    if (isRegularMarketMinute(minute)) return;
    const summary = summaryByJobId.get(config.job_id) ?? null;
    const state: TimelineRowState = !config.enabled ? "disabled" : summary?.status === "running" || summary?.status === "queued" ? "active" : "upcoming";
    groupByCadence.get(cadence.cadence)?.rows.push({
      config,
      summary,
      minute,
      durationMinutes: Math.max((config.estimated_duration_seconds ?? 0) / 60, 8),
      state,
      nextOccurrenceAt: occurrence,
    });
  });

  groups.forEach((group) => group.rows.sort((left, right) => left.minute - right.minute || left.config.job_label.localeCompare(right.config.job_label)));
  return groups;
}

function findNextScheduleOccurrence(cronExpr: string, timezone: string, now: Date): Date | null {
  const expression = parseTimelineCronExpression(cronExpr);
  if (!expression) return null;
  for (let dayOffset = 0; dayOffset <= 7; dayOffset += 1) {
    const day = new Date(now.getTime() + dayOffset * 24 * 60 * 60 * 1_000);
    if (!cronRunsOnZonedWeekday(expression.dayOfWeek, day, timezone)) continue;
    const localDay = getZonedDateParts(day, timezone);
    const scheduledAt = zonedDateTimeToDate(localDay, expression.hour, expression.minute, timezone);
    if (scheduledAt.getTime() >= now.getTime()) return scheduledAt;
  }
  return null;
}

function zonedDateTimeToDate(day: ReturnType<typeof getZonedDateParts>, hour: number, minute: number, timezone: string): Date {
  const expectedUtc = Date.UTC(day.year, day.month - 1, day.day, hour, minute);
  let candidate = new Date(expectedUtc);
  for (let attempt = 0; attempt < 2; attempt += 1) {
    candidate = new Date(expectedUtc - getTimezoneOffsetMinutes(candidate, timezone) * 60_000);
  }
  return candidate;
}

function getTimelineRowState(config: ScheduledJobConfig, summary: ScheduledJobSummary | null, minute: number, nowMinute: number, now: Date): TimelineRowState {
  const status = summary?.status ?? "unknown";
  const ranToday = Boolean(summary?.last_started_at && isSameZonedDate(new Date(summary.last_started_at), now, config.cron_tz));
  if (!config.enabled) return "disabled";
  if (status === "running" || status === "queued") return "active";
  if (ranToday && (status === "success" || status === "failed")) return "finished";
  return minute > nowMinute ? "upcoming" : "waiting";
}

function isRegularMarketMinute(minute: number): boolean {
  return minute >= MARKET_OPEN_MINUTE_ET && minute < MARKET_CLOSE_MINUTE_ET;
}

function toVisibleTimelineMinute(minute: number): number | null {
  if (isRegularMarketMinute(minute)) return null;
  if (minute >= MARKET_CLOSE_MINUTE_ET) {
    return minute - (MARKET_CLOSE_MINUTE_ET - MARKET_OPEN_MINUTE_ET);
  }
  return minute;
}

function fromVisibleTimelineMinute(minute: number): number {
  const clamped = Math.max(0, Math.min(VISIBLE_TIMELINE_MINUTES, minute));
  return clamped < MARKET_OPEN_MINUTE_ET ? clamped : clamped + (MARKET_CLOSE_MINUTE_ET - MARKET_OPEN_MINUTE_ET);
}

function snapTimelineMinute(minute: number): number {
  const snapped = Math.round(minute / 15) * 15;
  return Math.max(0, Math.min(23 * 60 + 45, snapped));
}

function moveTimelineMinuteByQuarter(minute: number, direction: 1 | -1): number {
  const visibleMinute = toVisibleTimelineMinute(minute);
  if (visibleMinute == null) return minute;
  return snapTimelineMinute(fromVisibleTimelineMinute(visibleMinute + direction * 15));
}

function getVisibleTimelineDuration(startMinute: number, durationMinutes: number): number {
  const visibleWindowEnd = startMinute < MARKET_OPEN_MINUTE_ET ? MARKET_OPEN_MINUTE_ET : 24 * 60;
  return Math.max(1, Math.min(Math.max(durationMinutes, 8), visibleWindowEnd - startMinute));
}

function parseTimelineCron(cronExpr: string, now: Date, timezone: string): { hour: number; minute: number } | null {
  const expression = parseTimelineCronExpression(cronExpr);
  if (!expression || !cronRunsOnZonedWeekday(expression.dayOfWeek, now, timezone)) return null;
  return { hour: expression.hour, minute: expression.minute };
}

function parseTimelineCronExpression(cronExpr: string): { hour: number; minute: number; dayOfWeek: string } | null {
  const [minutePart, hourPart, dayOfMonth, month, dayOfWeek, ...extra] = cronExpr.trim().split(/\s+/);
  if (extra.length || !/^\d{1,2}$/.test(minutePart) || !/^\d{1,2}$/.test(hourPart) || dayOfMonth !== "*" || month !== "*") {
    return null;
  }
  const minute = Number(minutePart);
  const hour = Number(hourPart);
  return minute > 59 || hour > 23 ? null : { hour, minute, dayOfWeek };
}

function cronRunsOnZonedWeekday(dayOfWeek: string, now: Date, timezone: string): boolean {
  if (dayOfWeek === "*") return true;
  const weekdayName = new Intl.DateTimeFormat("en-US", { timeZone: timezone, weekday: "short" }).format(now);
  const weekday = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"].indexOf(weekdayName);
  if (dayOfWeek === "1-5") return weekday >= 1 && weekday <= 5;
  return dayOfWeek.split(",").some((value) => Number(value) % 7 === weekday);
}

function getZonedDateParts(date: Date, timezone: string): { year: number; month: number; day: number; hour: number; minute: number; second: number } {
  const parts = new Intl.DateTimeFormat("en-US", {
    timeZone: timezone,
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    hourCycle: "h23",
  }).formatToParts(date);
  const value = (type: Intl.DateTimeFormatPartTypes) => Number(parts.find((part) => part.type === type)?.value ?? 0);
  return { year: value("year"), month: value("month"), day: value("day"), hour: value("hour"), minute: value("minute"), second: value("second") };
}

function getTimezoneOffsetMinutes(date: Date, timezone: string): number {
  const parts = getZonedDateParts(date, timezone);
  const zonedTimestamp = Date.UTC(parts.year, parts.month - 1, parts.day, parts.hour, parts.minute, parts.second);
  return Math.round((zonedTimestamp - date.getTime()) / 60_000);
}

function isSameZonedDate(left: Date, right: Date, timezone: string): boolean {
  const leftParts = getZonedDateParts(left, timezone);
  const rightParts = getZonedDateParts(right, timezone);
  return leftParts.year === rightParts.year && leftParts.month === rightParts.month && leftParts.day === rightParts.day;
}

function normalizeMinuteOfDay(value: number): number {
  return ((value % 1440) + 1440) % 1440;
}

function formatMinuteOfDay(value: number): string {
  const normalized = normalizeMinuteOfDay(Math.round(value));
  return `${String(Math.floor(normalized / 60)).padStart(2, "0")}:${String(normalized % 60).padStart(2, "0")}`;
}

function formatScheduleMapTime(row: ScheduleTimelineRow, primaryTimezone: string): string {
  const occurrence = row.nextOccurrenceAt;
  if (!occurrence) return `${formatMinuteOfDay(row.minute)} ${primaryTimezone}`;
  const primary = new Intl.DateTimeFormat("en-US", {
    timeZone: primaryTimezone,
    weekday: "short",
    hour: "2-digit",
    minute: "2-digit",
    hourCycle: "h23",
    timeZoneName: "short",
  }).format(occurrence);
  const newZealand = new Intl.DateTimeFormat("en-NZ", {
    timeZone: "Pacific/Auckland",
    weekday: "short",
    hour: "2-digit",
    minute: "2-digit",
    hourCycle: "h23",
    timeZoneName: "short",
  }).format(occurrence);
  return `${primary} · NZ ${newZealand}`;
}

function formatTimelineDate(date: Date, timezone: string): string {
  return new Intl.DateTimeFormat("en-US", { timeZone: timezone, weekday: "short", month: "short", day: "numeric" }).format(date);
}

function timelineStateLabel(state: TimelineRowState): string {
  if (state === "active") return "active";
  if (state === "finished") return "finished";
  if (state === "upcoming") return "upcoming";
  if (state === "waiting") return "awaiting scheduler status";
  return "paused";
}

function timelineBarDetail(row: ScheduleTimelineRow): string {
  if (row.state === "active") {
    if (row.summary?.status === "queued") return "Queued";
    return timelineExpectedFinish(row) ?? "Running · ETA unavailable";
  }
  if (row.state === "finished") {
    if (row.summary?.status === "failed") return "Failed";
    const startedAt = row.summary?.last_started_at ? new Date(row.summary.last_started_at).getTime() : 0;
    const finishedAt = row.summary?.last_finished_at ? new Date(row.summary.last_finished_at).getTime() : 0;
    const actualSeconds = startedAt && finishedAt ? Math.max(0, Math.round((finishedAt - startedAt) / 1000)) : 0;
    return actualSeconds ? formatEstimatedDuration(actualSeconds).replace("~", "") : "Finished";
  }
  if (row.state === "upcoming") return timelineExpectedFinish(row) ?? "ETA unavailable";
  if (row.state === "waiting") return "Awaiting status";
  return "Paused";
}

function timelineExpectedFinish(row: ScheduleTimelineRow, timezone?: string): string | null {
  const estimate = row.config.estimated_duration_seconds;
  if (!estimate || estimate < 1) return null;
  if (row.state === "active" && row.summary?.last_started_at) {
    const startedAt = new Date(row.summary.last_started_at).getTime();
    if (!Number.isNaN(startedAt)) {
      const finish = new Date(startedAt + estimate * 1_000);
      return `ETA ${timezone ? formatTimelineTimestamp(finish, timezone) : formatMinuteOfDay(finish.getHours() * 60 + finish.getMinutes())}`;
    }
  }
  return `ETA ${formatMinuteOfDay(row.minute + estimate / 60)}`;
}

function formatTimelineTimestamp(date: Date, timezone: string): string {
  return new Intl.DateTimeFormat("en-US", {
    timeZone: timezone,
    month: "short",
    day: "numeric",
    hour: "2-digit",
    minute: "2-digit",
    hourCycle: "h23",
  }).format(date);
}

function formatElapsedDuration(milliseconds: number): string {
  return formatEstimatedDuration(Math.max(0, Math.round(milliseconds / 1_000))).replace("~", "");
}

function formatEstimatedDuration(seconds: number | null | undefined): string {
  if (!seconds || seconds < 1) {
    return "Not enough history";
  }
  if (seconds < 60) {
    return `~${seconds}s`;
  }
  const minutes = Math.round(seconds / 60);
  if (minutes < 60) {
    return `~${minutes} min`;
  }
  const hours = Math.floor(minutes / 60);
  const remainingMinutes = minutes % 60;
  return remainingMinutes ? `~${hours}h ${remainingMinutes}m` : `~${hours}h`;
}

function describeEstimateSample(sampleCount: number | undefined): string {
  if (!sampleCount) {
    return "awaiting completed runs";
  }
  return `median of ${sampleCount} recent ${sampleCount === 1 ? "run" : "runs"}`;
}

function renderScreenerActivity(
  label: string,
  current: ScreenerActivityEntry | null | undefined,
  last: ScreenerActivityEntry | null | undefined,
  scheduledCount = 0,
) {
  const entry = current ?? last;
  if (!entry) {
    return <span className="screener-activity-line screener-activity-idle">{label}: {scheduledCount ? "Idle" : "Not scheduled"}</span>;
  }
  const isActive = entry.status === "queued" || entry.status === "running";
  const labelText = isActive ? (entry.status === "queued" ? "Queued" : "Running") : describeActivityStatus(entry.status);
  const timestamp = isActive ? entry.started_at : entry.finished_at || entry.started_at;
  const timeText = timestamp ? formatLocalDateTime(timestamp) : "time unavailable";
  const timingText = isActive && entry.estimated_duration_seconds ? ` · ${formatEstimatedDuration(entry.estimated_duration_seconds)} typical` : "";
  return (
    <span className={`screener-activity-line screener-activity-${entry.status}`}>
      {isActive ? <span className="screener-activity-spinner" aria-label="Running" /> : null}
      {label}: {labelText} · {timeText}{timingText}
      {!isActive && entry.success_count > 0 ? ` · ${entry.success_count} hits` : ""}
    </span>
  );
}

function describeActivityStatus(status: ScreenerActivityEntry["status"]): string {
  if (status === "success") return "Succeeded";
  if (status === "failed") return "Failed";
  if (status === "cancelled") return "Cancelled";
  return "Interrupted";
}

function formatScheduleOptionsPreview(options: ScheduledJobConfig["options"]): string {
  const serialized = JSON.stringify(options ?? {});
  return serialized.length > 84 ? `${serialized.slice(0, 81)}...` : serialized;
}

function summarizeScheduleOptions(options: ScheduledJobConfig["options"]): string {
  const entries = Object.entries(options ?? {});
  if (entries.length === 0) {
    return "Default payload";
  }
  return `${entries.length} override${entries.length === 1 ? "" : "s"}`;
}

function formatFilePathPreview(value: string | null | undefined): string {
  if (!value) {
    return "-";
  }
  return value.length > 72 ? `...${value.slice(-69)}` : value;
}

function isHierarchicalBatchJob(actionId: string): boolean {
  return actionId === "signal_warm_batch" || actionId === "screener_history_batch";
}

function groupChildJobsByDate(childJobs: JobsResponse["jobs"][number]["child_jobs"]) {
  const groups = new Map<string, JobsResponse["jobs"][number]["child_jobs"]>();
  childJobs.forEach((child) => {
    const runDate = child.run_date || "Unscheduled";
    const existing = groups.get(runDate) ?? [];
    existing.push(child);
    groups.set(runDate, existing);
  });
  return Array.from(groups.entries())
    .sort(([left], [right]) => left.localeCompare(right))
    .map(([runDate, children]) => {
      const summary = summarizeChildStatuses(children);
      return {
        runDate,
        children,
        status: summarizeGroupStatus(summary),
        successCount: children.reduce((total, child) => total + (child.success_count || 0), 0),
        durationSeconds: children.reduce((total, child) => total + (child.duration_seconds || 0), 0),
        note:
          summary.running > 0
            ? `${summary.running}/${children.length} running`
            : summary.failed > 0
              ? `${summary.failed} failed`
              : summary.success === children.length
                ? "All done"
                : `${summary.success}/${children.length} complete`,
      };
    });
}

function summarizeChildStatuses(childJobs: JobsResponse["jobs"][number]["child_jobs"]) {
  return childJobs.reduce(
    (summary, child) => {
      if (child.status === "running") {
        summary.running += 1;
      } else if (child.status === "failed") {
        summary.failed += 1;
      } else if (child.status === "cancelled") {
        summary.cancelled += 1;
      } else if (child.status === "success") {
        summary.success += 1;
      } else {
        summary.queued += 1;
      }
      return summary;
    },
    { queued: 0, running: 0, success: 0, failed: 0, cancelled: 0 },
  );
}

function summarizeGroupStatus(summary: { queued: number; running: number; success: number; failed: number; cancelled: number }): "queued" | "running" | "success" | "failed" | "cancelled" {
  if (summary.running > 0) {
    return "running";
  }
  if (summary.failed > 0) {
    return "failed";
  }
  if (summary.cancelled > 0 && summary.success === 0 && summary.running === 0 && summary.failed === 0) {
    return "cancelled";
  }
  if (summary.success > 0 && summary.queued === 0) {
    return "success";
  }
  return "queued";
}

function buildDefaultScheduleIdentity(action: ScheduledActionOption | null): { jobId: string; jobLabel: string } {
  if (!action) {
    return { jobId: "", jobLabel: "" };
  }
  const normalizedId = action.id.trim().replace(/[^a-z0-9_]+/g, "_");
  return {
    jobId: normalizedId,
    jobLabel: action.label.trim(),
  };
}

function buildScheduleCronExpr(cadence: ScheduleCadence, timeValue: string): string {
  const normalizedTime = /^\d{2}:\d{2}$/.test(timeValue) ? timeValue : "16:30";
  const [hours, minutes] = normalizedTime.split(":");
  const weekday = cadence === "weekly_saturday" ? "6" : cadence === "weekly_sunday" ? "0" : "1-5";
  return `${Number(minutes)} ${Number(hours)} * * ${weekday}`;
}

function resolveScheduleCronDraft(draft: ScheduleCronDraft): string {
  if (draft.hasUnsupportedCron && !draft.cadenceTouched) {
    return draft.cronExpr;
  }
  return buildScheduleCronExpr(draft.cadence, draft.time);
}

function parseSimpleScheduleCron(cronExpr: string): { supported: boolean; cadence: ScheduleCadence; time: string } {
  const parts = cronExpr.trim().split(/\s+/);
  if (parts.length !== 5) {
    return { supported: false, cadence: "weekdays", time: "16:30" };
  }
  const [minutePart, hourPart, dayPart, monthPart, weekdayPart] = parts;
  if (!/^\d{1,2}$/.test(minutePart) || !/^\d{1,2}$/.test(hourPart) || dayPart !== "*" || monthPart !== "*") {
    return { supported: false, cadence: "weekdays", time: "16:30" };
  }
  if (weekdayPart !== "1-5" && weekdayPart !== "6" && weekdayPart !== "0" && weekdayPart !== "7") {
    return { supported: false, cadence: "weekdays", time: "16:30" };
  }
  const hours = Number(hourPart);
  const minutes = Number(minutePart);
  if (hours < 0 || hours > 23 || minutes < 0 || minutes > 59) {
    return { supported: false, cadence: "weekdays", time: "16:30" };
  }
  return {
    supported: true,
    cadence: weekdayPart === "6" ? "weekly_saturday" : weekdayPart === "0" || weekdayPart === "7" ? "weekly_sunday" : "weekdays",
    time: `${String(hours).padStart(2, "0")}:${String(minutes).padStart(2, "0")}`,
  };
}

function moveSimpleScheduleCron(cronExpr: string, minute: number): string | null {
  const parsed = parseSimpleScheduleCron(cronExpr);
  const expression = parseTimelineCronExpression(cronExpr);
  if (!parsed.supported || !expression) return null;
  const normalizedMinute = snapTimelineMinute(minute);
  return `${normalizedMinute % 60} ${Math.floor(normalizedMinute / 60)} * * ${expression.dayOfWeek}`;
}

function parseScheduleOptionsJson(value: string): Record<string, unknown> {
  const trimmed = value.trim();
  if (!trimmed || trimmed === "null") {
    return {};
  }
  const parsed = JSON.parse(trimmed) as unknown;
  if (parsed === null) {
    return {};
  }
  if (typeof parsed !== "object" || Array.isArray(parsed)) {
    throw new Error("Action Options JSON must be an object, null, or blank.");
  }
  return parsed as Record<string, unknown>;
}

function groupScheduledActions(actions: ScheduledActionOption[]): Array<{ label: string; actions: ScheduledActionOption[] }> {
  const groups = new Map<string, ScheduledActionOption[]>();
  for (const action of actions) {
    const label = scheduledActionGroupLabel(action);
    const current = groups.get(label) ?? [];
    current.push(action);
    groups.set(label, current);
  }
  return Array.from(groups.entries())
    .map(([label, items]) => ({
      label,
      actions: [...items].sort((left, right) => left.label.localeCompare(right.label)),
    }))
    .sort((left, right) => scheduledGroupRank(left.label) - scheduledGroupRank(right.label) || left.label.localeCompare(right.label));
}

function scheduledActionGroupLabel(action: ScheduledActionOption): string {
  if ((action.bias_group ?? "other") === "data") {
    return "Data Refresh";
  }
  if ((action.bias_group ?? "other") === "bullish") {
    if ((action.bullish_subgroup ?? "") === "leaders") {
      return "Bullish / Leader Signals";
    }
    if ((action.bullish_subgroup ?? "") === "pullbacks") {
      return "Bullish / Pullback Signals";
    }
    if ((action.bullish_subgroup ?? "") === "bottoming") {
      return "Bullish / Bottoming Breakouts";
    }
    return "Bullish / Other";
  }
  if ((action.bias_group ?? "other") === "bearish") {
    return "Bearish";
  }
  return "Other";
}

function scheduledGroupRank(label: string): number {
  if (label === "Data Refresh") {
    return 0;
  }
  if (label === "Bullish / Leader Signals") {
    return 1;
  }
  if (label === "Bullish / Pullback Signals") {
    return 2;
  }
  if (label === "Bullish / Bottoming Breakouts") {
    return 3;
  }
  if (label === "Bullish / Other") {
    return 4;
  }
  if (label === "Bearish") {
    return 5;
  }
  return 6;
}

function buildScheduleOptionsTemplate(action: ScheduledActionOption | null): string {
  if (!action) {
    return "{}";
  }

  const template: Record<string, unknown> = {};
  const fieldIds = new Set(action.fields.map((field) => field.id));

  if (fieldIds.has("market_data_source")) {
    template.market_data_source = "database-first";
  }
  if (fieldIds.has("filter_precedence")) {
    template.filter_precedence = "exclude";
  }
  if (fieldIds.has("as_of_date")) {
    template.as_of_date = "{{local_date}}";
  }
  if (fieldIds.has("trade_date")) {
    template.trade_date = "{{local_date}}";
  }
  if (fieldIds.has("reference_date")) {
    template.reference_date = "{{local_date}}";
  }
  if (fieldIds.has("overwrite_policy")) {
    template.overwrite_policy = "skip-existing";
  }
  if (fieldIds.has("candidate_threshold")) {
    template.candidate_threshold = 4;
  }
  if (fieldIds.has("entry_signal_threshold")) {
    template.entry_signal_threshold = 4;
  }
  if (fieldIds.has("max_parallel")) {
    template.max_parallel = 5;
  }
  if (fieldIds.has("hold_periods_json")) {
    template.hold_periods_json = "[5, 10]";
  }

  for (const field of action.fields) {
    if (field.type !== "number") {
      continue;
    }
    if (field.id in template) {
      continue;
    }
    const parsed = parseScheduleNumericPlaceholder(field.placeholder);
    if (parsed != null) {
      template[field.id] = parsed;
    }
  }

  if (action.id === "legacy_peg" || action.id === "sean_peg" || action.id === "sean_gap_up") {
    template.source = "earnings-watchlist";
  }
  if (action.id === "rts_validation") {
    template.start_date = "{{local_date_minus_465}}";
    template.end_date = "{{local_date_minus_100}}";
  } else if (action.id === "signal_warm_batch" || action.id === "overlap_backtest_v1" || action.id === "screener_history_batch") {
    template.start_date = "{{local_date_minus_14}}";
    template.end_date = "{{local_date}}";
  } else if (fieldIds.has("start_date") || fieldIds.has("end_date")) {
    if (fieldIds.has("start_date")) {
      template.start_date = "{{local_date}}";
    }
    if (fieldIds.has("end_date")) {
      template.end_date = "{{local_date}}";
    }
  }
  if (fieldIds.has("strategy_ids")) {
    template.strategy_ids = ["rs", "vcp", "gap_fill", "fearzone"];
  }
  if (action.id === "screener_history_batch") {
    template.scope = {};
  }

  return JSON.stringify(template, null, 2);
}

function parseScheduleNumericPlaceholder(value?: string | null): number | null {
  if (!value) {
    return null;
  }
  const trimmed = value.trim();
  if (!trimmed || /^optional$/i.test(trimmed)) {
    return null;
  }
  const parsed = Number(trimmed);
  return Number.isFinite(parsed) ? parsed : null;
}

function formatMaybePercent(value: number | null | undefined): string {
  return value == null || !Number.isFinite(value) ? "-" : `${value.toFixed(2)}%`;
}
