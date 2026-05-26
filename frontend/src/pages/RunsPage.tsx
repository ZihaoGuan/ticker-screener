import { Panel } from "../components/Panel";
import { StatusPill } from "../components/StatusPill";
import { consoleTail, screenerJobs } from "../lib/mock-data";

export function RunsPage() {
  return (
    <div className="page-grid">
      <Panel title="Trigger Screener">
        <div className="run-toolbar">
          <div>
            <div className="panel-copy">Execute algorithmic screeners against the current research universe.</div>
          </div>
          <label className="field">
            <span>Universe Limit</span>
            <input defaultValue="2500" />
          </label>
          <div className="button-row">
            <button className="primary-button">Run RS</button>
            <button className="secondary-button">Run VCP</button>
            <button className="secondary-button">Run Cup Handle</button>
          </div>
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
              <th>RC</th>
            </tr>
          </thead>
          <tbody>
            {screenerJobs.map((job) => (
              <tr key={job.jobId}>
                <td className="mono">#{job.jobId}</td>
                <td>{job.label}</td>
                <td>
                  <StatusPill status={job.status} />
                </td>
                <td>{job.startedAt}</td>
                <td>{job.finishedAt}</td>
                <td className="mono">{job.returnCode}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </Panel>

      <Panel title="Console Tail: #RUN-8821" aside={<span className="eyebrow">Auto-scroll: on</span>}>
        <pre className="console-surface">{consoleTail}</pre>
      </Panel>
    </div>
  );
}
