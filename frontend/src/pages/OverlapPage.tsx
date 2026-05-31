import { useEffect, useState } from "react";
import { LoadingBlock } from "../components/LoadingBlock";
import { Panel } from "../components/Panel";
import { fetchJson } from "../lib/api";
import type { OverlapResponse } from "../lib/types";

export function OverlapPage() {
  const [payload, setPayload] = useState<OverlapResponse | null>(null);
  const [isLoading, setIsLoading] = useState(true);

  useEffect(() => {
    void fetchJson<OverlapResponse>("/api/overlap/latest")
      .then(setPayload)
      .catch(() => setPayload(null))
      .finally(() => setIsLoading(false));
  }, []);

  return (
    <div className="page-grid">
      <Panel title="Overlap Summary" aside={<span className="eyebrow">Latest daily snapshot</span>}>
        {isLoading ? <LoadingBlock label="Loading overlap summary…" compact /> : null}
        <div className="card-grid overlap-cards">
          <article className="metric-card">
            <h3>Unique Tickers</h3>
            <div className="metric-value">{payload?.unique_ticker_count ?? 0}</div>
          </article>
          <article className="metric-card">
            <h3>Overlap ≥ 2</h3>
            <div className="metric-value">{payload?.overlap_two_plus_count ?? 0}</div>
          </article>
          <article className="metric-card">
            <h3>Overlap ≥ 3</h3>
            <div className="metric-value">{payload?.overlap_three_plus_count ?? 0}</div>
          </article>
        </div>
      </Panel>

      <Panel title="Candidates">
        {isLoading ? <LoadingBlock label="Loading overlap candidates…" compact /> : null}
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
            {(payload?.overlap_two_plus ?? []).map((entry) => (
              <tr key={entry.ticker}>
                <td>{entry.ticker}</td>
                <td>{entry.pipeline_count}</td>
                <td>{entry.pipeline_labels.join(", ")}</td>
                <td>{entry.sector ?? "-"}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </Panel>
    </div>
  );
}
