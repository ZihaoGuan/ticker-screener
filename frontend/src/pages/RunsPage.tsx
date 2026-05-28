import { useEffect, useMemo, useState } from "react";
import { StatusPill } from "../components/StatusPill";
import { Panel } from "../components/Panel";
import { ProgressBar } from "../components/ProgressBar";
import { fetchJson } from "../lib/api";
import type { JobsResponse } from "../lib/types";

export function RunsPage() {
  const [payload, setPayload] = useState<JobsResponse | null>(null);
  const [selectedActionId, setSelectedActionId] = useState("");
  const [fieldValues, setFieldValues] = useState<Record<string, string | string[]>>({
    limit: "",
    tickers: "",
    date_label: "",
    as_of_date: "",
    source: "universe",
    reference_date: "",
    market_data_source: "internet",
    filter_precedence: "exclude",
    include_sectors: [],
    exclude_sectors: [],
    include_industries: [],
    exclude_industries: [],
    include_themes: [],
    exclude_themes: [],
  });

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

  const runAction = async (actionId: string) => {
    const body: Record<string, string | string[]> = {};
    for (const [key, value] of Object.entries(fieldValues)) {
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
  };

  return (
    <div className="page-grid">
      <Panel title="Trigger Screener">
        <div className="run-toolbar">
          <div>
            <div className="panel-copy">Execute algorithmic screeners against the current research universe.</div>
          </div>
          <div className="button-row">
            {(payload?.actions ?? []).map((action) => (
              <button
                key={action.id}
                className={selectedAction?.id === action.id ? "primary-button" : "secondary-button"}
                onClick={() => setSelectedActionId(action.id)}
                type="button"
              >
                {action.label}
              </button>
            ))}
          </div>
          {selectedAction ? (
            <div className="run-params-grid">
              {selectedAction.fields.map((field) => (
                <label className="field" key={field.id}>
                  <span>{field.label}</span>
                  {field.type === "select" ? (
                    <select
                      value={typeof fieldValues[field.id] === "string" ? fieldValues[field.id] : ""}
                      onChange={(event) =>
                        setFieldValues((current) => ({
                          ...current,
                          [field.id]: event.target.value,
                        }))
                      }
                    >
                      {field.options.map((option) => (
                        <option key={option.value} value={option.value}>
                          {option.label}
                        </option>
                      ))}
                    </select>
                  ) : field.type === "multiselect" ? (
                    <select
                      multiple
                      value={Array.isArray(fieldValues[field.id]) ? fieldValues[field.id] : []}
                      onChange={(event) =>
                        setFieldValues((current) => ({
                          ...current,
                          [field.id]: Array.from(event.target.selectedOptions).map((option) => option.value),
                        }))
                      }
                      className="multi-select"
                    >
                      {field.options.map((option) => (
                        <option key={option.value} value={option.value}>
                          {option.label}
                        </option>
                      ))}
                    </select>
                  ) : (
                    <input
                      type={field.type}
                      placeholder={field.placeholder ?? undefined}
                      value={typeof fieldValues[field.id] === "string" ? fieldValues[field.id] : ""}
                      onChange={(event) =>
                        setFieldValues((current) => ({
                          ...current,
                          [field.id]: event.target.value,
                        }))
                      }
                    />
                  )}
                  {field.help_text ? <small className="field-help">{field.help_text}</small> : null}
                </label>
              ))}
              <div className="run-action-footer">
                <button className="primary-button" onClick={() => void runAction(selectedAction.id)} type="button">
                  {selectedAction.label}
                </button>
                <span className="eyebrow">Command: {selectedAction.command}</span>
              </div>
            </div>
          ) : null}
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
