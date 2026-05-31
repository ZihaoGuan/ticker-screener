import { useEffect, useMemo, useState } from "react";
import { Link } from "react-router-dom";
import { LoadingBlock } from "../components/LoadingBlock";
import { StatusPill } from "../components/StatusPill";
import { Panel } from "../components/Panel";
import { ProgressBar } from "../components/ProgressBar";
import { ScreenerConfigModal } from "../components/ScreenerConfigModal";
import { fetchJson } from "../lib/api";
import { formatLocalDateTime } from "../lib/format";
import type { JobsResponse } from "../lib/types";
import "./RunsPage.css";

export function RunsPage() {
  const [payload, setPayload] = useState<JobsResponse | null>(null);
  const [selectedActionId, setSelectedActionId] = useState("");
  const [selectedJobId, setSelectedJobId] = useState("");
  const [isModalOpen, setIsModalOpen] = useState(false);
  const [isRunning, setIsRunning] = useState(false);
  const [isCancellingJobId, setIsCancellingJobId] = useState("");
  const [isLoading, setIsLoading] = useState(true);
  const [hasError, setHasError] = useState(false);

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

  useEffect(() => {
    refresh();
    const timer = window.setInterval(refresh, 4000);
    return () => window.clearInterval(timer);
  }, []);

  useEffect(() => {
    if (!payload?.actions?.length) {
      return;
    }
    setSelectedActionId((current) => current || payload.actions[0].id);
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
                <div className="screener-card-header">
                  <h3>{action.label}</h3>
                </div>
                <p className="screener-description">
                  {action.fields.length > 0
                    ? "Run with defaults now, or open config for custom parameters."
                    : "Run immediately with the default screener settings."}
                </p>
                <div className="screener-card-actions">
                  <button
                    className="screener-run-button"
                    onClick={() => void handleQuickRun(action.id)}
                    type="button"
                    disabled={isRunning}
                  >
                    RUN DEFAULT
                  </button>
                  {action.fields.length > 0 ? (
                    <button
                      className="screener-config-button"
                      onClick={() => handleConfigureClick(action.id)}
                      type="button"
                      disabled={isRunning}
                    >
                      CONFIGURE
                    </button>
                  ) : null}
                </div>
              </div>
            ))}
          </div>
        </Panel>

        <Panel
          title="Current Progress"
          aside={
            activeJob ? (
              <span className="eyebrow">{activeJob.label}</span>
            ) : (
              <span className="eyebrow">Idle</span>
            )
          }
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

        <Panel title="Recent Screener Jobs">
          {isLoading && !payload ? <LoadingBlock label="Loading recent jobs…" /> : null}
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
                  <td className="mono">#{job.job_id}</td>
                  <td>{job.label}</td>
                  <td>
                    <StatusPill status={job.status} />
                  </td>
                  <td>{formatLocalDateTime(job.started_at)}</td>
                  <td>{formatLocalDateTime(job.finished_at)}</td>
                  <td>{job.success_count}</td>
                  <td>{formatDuration(job.duration_seconds)}</td>
                  <td>
                    <ProgressBar
                      status={job.status}
                      progress={job.progress_percent}
                      label={job.progress_label ?? undefined}
                      compact
                    />
                  </td>
                  <td className="mono">{job.return_code ?? "-"}</td>
                  <td>
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
                      <Link
                        className="table-action-button table-link-button"
                        to={job.watchlist_url}
                        onClick={(event) => event.stopPropagation()}
                      >
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
