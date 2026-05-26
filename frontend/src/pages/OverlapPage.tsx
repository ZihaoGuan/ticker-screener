import { Panel } from "../components/Panel";
import { overlapEntries } from "../lib/mock-data";

export function OverlapPage() {
  return (
    <div className="page-grid">
      <Panel title="Overlap Summary" aside={<span className="eyebrow">Latest daily snapshot</span>}>
        <div className="card-grid overlap-cards">
          <article className="metric-card">
            <h3>Unique Tickers</h3>
            <div className="metric-value">38</div>
          </article>
          <article className="metric-card">
            <h3>Overlap ≥ 2</h3>
            <div className="metric-value">12</div>
          </article>
          <article className="metric-card">
            <h3>Overlap ≥ 3</h3>
            <div className="metric-value">4</div>
          </article>
        </div>
      </Panel>

      <Panel title="Candidates">
        <table className="data-table">
          <thead>
            <tr>
              <th>Ticker</th>
              <th>Overlap</th>
              <th>Pipelines</th>
              <th>Sector</th>
            </tr>
          </thead>
          <tbody>
            {overlapEntries.map((entry) => (
              <tr key={entry.ticker}>
                <td>{entry.ticker}</td>
                <td>{entry.overlapCount}</td>
                <td>{entry.pipelines.join(", ")}</td>
                <td>{entry.sector}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </Panel>
    </div>
  );
}
