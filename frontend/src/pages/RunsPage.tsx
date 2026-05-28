import { useEffect, useMemo, useState } from "react";
import { StatusPill } from "../components/StatusPill";
import { Panel } from "../components/Panel";
import { ProgressBar } from "../components/ProgressBar";
import { ScreenerConfigModal } from "../components/ScreenerConfigModal";
import { fetchJson } from "../lib/api";
import type { JobsResponse } from "../lib/types";
import "./RunsPage.css";

export function RunsPage() {
  const [payload, setPayload] = useState<JobsResponse | null>(null);
  const [selectedActionId, setSelectedActionId] = useState("");
  const [isModalOpen, setIsModalOpen] = useState(false);
  const [isRunning, setIsRunning] = useState(false);

  const refresh = () => {
    void fetchJson<JobsResponse>("/api/jobs").then(setPayload).catch(() => setPayload({ actions: [], jobs: [] }));
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

  const latestLog = useMemo(() => payload?.jobs[0]?.log_tail ?? "No job log yet.", [payload]);
  const activeJob = useMemo(() => payload?.jobs.find((job) => job.status === "running") ?? null, [payload]);
  const selectedAction = useMemo(
    () => payload?.actions.find((action) => action.id === selectedActionId) ?? payload?.actions[0] ?? null,
    [payload, selectedActionId],
  );

  const handleRunAction = async (params: Record<string, string | string[]>) => {
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
      await fetchJson<{ ok: boolean; job_id: string }>(`/api/runs/${selectedActionId}`, {
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

  return (
    <>
      <div className="page-grid">
        <Panel title="Available Screeners">
          <div className="screeners-grid">
            {(payload?.actions ?? []).map((action) => (
              <div key={action.id} className="screener-card">
                <div className="screener-card-header">
                  <h3>{action.label}</h3>
                </div>
                <p className="screener-description">
                  {action.fields.length > 0
                    ? "Click to configure parameters and run"
                    : "Click to run without parameters"}
                </p>
                <button
                  className="screener-run-button"
                  onClick={() => handleConfigureClick(action.id)}
                  type="button"
                  disabled={isRunning}
                >
                  CONFIGURE & RUN
                </button>
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
              status={activeJob?.status ?? "success"}
              label={
                activeJob
                  ? `${activeJob.label} · ${activeJob.progress_label || `started ${activeJob.started_at || "just now"}`}`
                  : "No screener currently running"
              }
              progress={activeJob?.progress_percent ?? null}
            />
          </div>
        </Panel>

        <Panel title="Recent Screener Jobs">
          <table className="data-table">
            <thead>
              <tr>
                <th>Job ID</th>
                <th>Screener</th>
                <th>Status</th>
                <th>Start Time</th>
                <th>Finish Time</th>
                <th>Progress</th>
                <th>RC</th>
              </tr>
            </thead>
            <tbody>
              {(payload?.jobs ?? []).map((job) => (
                <tr key={job.job_id}>
                  <td className="mono">#{job.job_id}</td>
                  <td>{job.label}</td>
                  <td>
                    <StatusPill status={job.status} />
                  </td>
                  <td>{job.started_at || "-"}</td>
                  <td>{job.finished_at || "-"}</td>
                  <td>
                    <ProgressBar
                      status={job.status}
                      progress={job.progress_percent}
                      label={job.progress_label ?? undefined}
                      compact
                    />
                  </td>
                  <td className="mono">{job.return_code ?? "-"}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </Panel>

        <Panel title="Console Tail" aside={<span className="eyebrow">Auto-refresh: 4s</span>}>
          <pre className="console-surface">{latestLog}</pre>
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
