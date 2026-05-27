import { useEffect, useMemo, useState } from "react";
import { StatusPill } from "../components/StatusPill";
import { Panel } from "../components/Panel";
import { ProgressBar } from "../components/ProgressBar";
import { fetchJson } from "../lib/api";
import type { JobsResponse } from "../lib/types";

export function RunsPage() {
  const [payload, setPayload] = useState<JobsResponse | null>(null);
  const [limitValue, setLimitValue] = useState("2500");

  const refresh = () => {
    void fetchJson<JobsResponse>("/api/jobs").then(setPayload).catch(() => setPayload({ actions: [], jobs: [] }));
  };

  useEffect(() => {
    refresh();
    const timer = window.setInterval(refresh, 4000);
    return () => window.clearInterval(timer);
  }, []);

  const latestLog = useMemo(() => payload?.jobs[0]?.log_tail ?? "No job log yet.", [payload]);
  const activeJob = useMemo(() => payload?.jobs.find((job) => job.status === "running") ?? null, [payload]);

  const runAction = async (actionId: string) => {
    const limit = Number(limitValue);
    const query = Number.isFinite(limit) && limit > 0 ? `?limit=${limit}` : "";
    await fetchJson<{ ok: boolean; job_id: string }>(`/api/runs/${actionId}${query}`, { method: "POST" });
    refresh();
  };

  return (
    <div className="page-grid">
      <Panel title="Trigger Screener">
        <div className="run-toolbar">
          <div>
            <div className="panel-copy">Execute algorithmic screeners against the current research universe.</div>
          </div>
          <label className="field">
            <span>Universe Limit</span>
            <input value={limitValue} onChange={(event) => setLimitValue(event.target.value)} />
          </label>
          <div className="button-row">
            {(payload?.actions ?? []).map((action, index) => (
              <button
                key={action.id}
                className={index === 0 ? "primary-button" : "secondary-button"}
                onClick={() => void runAction(action.id)}
                type="button"
              >
                {action.label}
              </button>
            ))}
          </div>
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
  );
}
