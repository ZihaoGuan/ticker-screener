import { useEffect, useMemo, useState } from "react";
import { Link, useSearchParams } from "react-router-dom";
import { LoadingBlock } from "../components/LoadingBlock";
import { Panel } from "../components/Panel";
import { fetchJson } from "../lib/api";
import type { OverlapResponse } from "../lib/types";

export function OverlapPage() {
  const [searchParams] = useSearchParams();
  const requestedDate = (searchParams.get("date") ?? "").trim();
  const [payload, setPayload] = useState<OverlapResponse | null>(null);
  const [isLoading, setIsLoading] = useState(true);
  const [notice, setNotice] = useState("");

  const requestPath = useMemo(() => {
    return requestedDate ? `/api/overlap/${encodeURIComponent(requestedDate)}` : "/api/overlap/latest";
  }, [requestedDate]);

  useEffect(() => {
    setIsLoading(true);
    setNotice("");
    void fetchJson<OverlapResponse>(requestPath)
      .then((response) => {
        setPayload(response);
        if (requestedDate && response.date_label && response.date_label !== requestedDate) {
          setNotice(`Requested ${requestedDate}. Loaded ${response.date_label} instead.`);
        }
      })
      .catch((error) => {
        setPayload(null);
        setNotice(error instanceof Error ? error.message : "Failed to load daily report.");
      })
      .finally(() => setIsLoading(false));
  }, [requestPath, requestedDate]);

  const reportDate = payload?.date_label || requestedDate || "latest";
  const chartDate = payload?.date_label || requestedDate;
  const candidates = payload?.overlap_two_plus ?? [];
  const pipelineStatus = payload?.pipeline_status ?? [];

  return (
    <div className="page-grid">
      <Panel
        title="Daily Report"
        aside={
          <div className="weekly-watchlist-actions">
            <span className="eyebrow">{payload?.date_label ? `Snapshot ${payload.date_label}` : "Latest daily snapshot"}</span>
            {requestedDate ? (
              <Link className="ghost-button" to="/report">
                Latest
              </Link>
            ) : null}
          </div>
        }
      >
        {isLoading ? <LoadingBlock label="Loading overlap summary…" compact /> : null}
        {notice ? <p className="panel-copy">{notice}</p> : null}
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
        <p className="panel-copy">Daily v1 overlap report. Click ticker to jump into chart detail.</p>
      </Panel>

      <Panel title="Pipeline Status" aside={<span className="eyebrow">Artifact inputs for {reportDate}</span>}>
        {isLoading ? <LoadingBlock label="Loading pipeline status…" compact /> : null}
        <div className="card-grid overlap-cards">
          {pipelineStatus.map((item) => (
            <article key={item.label} className="metric-card">
              <h3>{item.label}</h3>
              <div className="metric-value">{item.count}</div>
              <p className="card-meta">{item.file_present ? "Artifact present" : "Artifact missing"}</p>
            </article>
          ))}
        </div>
      </Panel>

      <Panel title="Candidates">
        {isLoading ? <LoadingBlock label="Loading overlap candidates…" compact /> : null}
        {!isLoading && candidates.length === 0 ? <p className="panel-copy">No overlap candidates in this daily report.</p> : null}
        <div className="data-table-responsive">
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
              {candidates.map((entry) => (
                <tr key={entry.ticker}>
                  <td data-label="Ticker">
                    <Link
                      className="table-action-button table-link-button"
                      to={chartDate ? `/charts?ticker=${encodeURIComponent(entry.ticker)}&date=${encodeURIComponent(chartDate)}` : `/charts?ticker=${encodeURIComponent(entry.ticker)}`}
                    >
                      {entry.ticker}
                    </Link>
                  </td>
                  <td data-label="Overlap">{entry.pipeline_count}</td>
                  <td data-label="Pipelines">{entry.pipeline_labels.join(", ")}</td>
                  <td data-label="Sector">{entry.sector ?? "-"}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </Panel>
    </div>
  );
}
