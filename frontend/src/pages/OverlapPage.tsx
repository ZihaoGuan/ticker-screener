import { useEffect, useMemo, useState } from "react";
import { Link, useSearchParams } from "react-router-dom";
import { LoadingBlock } from "../components/LoadingBlock";
import { Panel } from "../components/Panel";
import { fetchJson } from "../lib/api";
import type { OverlapEntry, OverlapPipelineStatus, OverlapResponse } from "../lib/types";

export function OverlapPage() {
  const [searchParams, setSearchParams] = useSearchParams();
  const requestedDate = (searchParams.get("date") ?? "").trim();
  const [payload, setPayload] = useState<OverlapResponse | null>(null);
  const [isLoading, setIsLoading] = useState(true);
  const [notice, setNotice] = useState("");
  const [selectedPipelineId, setSelectedPipelineId] = useState("");

  const requestPath = useMemo(() => {
    return requestedDate ? `/api/overlap/${encodeURIComponent(requestedDate)}` : "/api/overlap/latest";
  }, [requestedDate]);

  useEffect(() => {
    setIsLoading(true);
    setNotice("");
    void fetchJson<OverlapResponse>(requestPath)
      .then((response) => {
        setPayload(response);
        setSelectedPipelineId((current) => {
          if (current && response.pipeline_tickers[current]) {
            return current;
          }
          return response.pipeline_status[0]?.id ?? "";
        });
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
  const candidates = useMemo(
    () => [...(payload?.overlap_two_plus ?? [])].sort((left, right) => right.pipeline_count - left.pipeline_count || left.ticker.localeCompare(right.ticker)),
    [payload?.overlap_two_plus],
  );
  const pipelineStatus = payload?.pipeline_status ?? [];
  const pipelineTickers = payload?.pipeline_tickers ?? {};
  const selectedTickers = selectedPipelineId ? pipelineTickers[selectedPipelineId] ?? [] : [];
  const selectedPipeline = pipelineStatus.find((item) => item.id === selectedPipelineId) ?? null;
  const fearzoneTickers = payload?.fearzone_tickers ?? [];
  const groupedPipelineStatus = useMemo(() => buildPipelineGroups(pipelineStatus), [pipelineStatus]);
  const availableDates = payload?.available_dates ?? [];
  const groupedCandidates = useMemo(() => buildGroupedOverlapCandidates(candidates, pipelineStatus), [candidates, pipelineStatus]);

  const handleDateChange = (nextDate: string) => {
    const nextParams = new URLSearchParams(searchParams);
    if (nextDate) {
      nextParams.set("date", nextDate);
    } else {
      nextParams.delete("date");
    }
    setSearchParams(nextParams, { replace: true });
  };

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
        <div className="overlap-date-toolbar">
          <label className="field overlap-date-field">
            <span>Report Date</span>
            <select value={requestedDate} onChange={(event) => handleDateChange(event.target.value)}>
              <option value="">Latest</option>
              {availableDates.map((date) => (
                <option key={date} value={date}>
                  {date}
                </option>
              ))}
            </select>
          </label>
        </div>
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
        {groupedPipelineStatus.map((group) => (
          <section key={group.key} className="overlap-group-section">
            <h2 className="overlap-group-title">{group.label}</h2>
            {group.sections.map((section) => (
              <div key={section.key} className="overlap-subgroup-section">
                {section.label ? <h3 className="overlap-subgroup-title">{section.label}</h3> : null}
                <div className="card-grid overlap-cards">
                  {section.items.map((item) => (
                    <button
                      key={item.id}
                      type="button"
                      className={`metric-card overlap-status-tile${selectedPipelineId === item.id ? " is-active" : ""}`}
                      onClick={() => setSelectedPipelineId(item.id)}
                    >
                      <h3>{item.label}</h3>
                      <div className="metric-value">{item.count}</div>
                      <p className="card-meta">{item.file_present ? "Artifact present" : "Artifact missing"}</p>
                    </button>
                  ))}
                </div>
              </div>
            ))}
          </section>
        ))}
      </Panel>

      <Panel
        title={selectedPipeline ? `${selectedPipeline.label} Tickers` : "Screener Tickers"}
        aside={<span className="eyebrow">{selectedTickers.length} names</span>}
      >
        {selectedPipeline == null ? <p className="panel-copy">Select screener tile to inspect ticker list.</p> : null}
        {selectedPipeline != null && selectedTickers.length === 0 ? (
          <p className="panel-copy">{selectedPipeline.file_present ? "No tickers in this screener artifact." : "Screener artifact missing for this date."}</p>
        ) : null}
        {selectedTickers.length > 0 ? (
          <div className="ticker-chip-grid">
            {selectedTickers.map((ticker) => (
              <Link
                key={`${selectedPipelineId}-${ticker}`}
                className="table-action-button table-link-button"
                to={chartDate ? `/charts?ticker=${encodeURIComponent(ticker)}&date=${encodeURIComponent(chartDate)}` : `/charts?ticker=${encodeURIComponent(ticker)}`}
              >
                {ticker}
              </Link>
            ))}
          </div>
        ) : null}
      </Panel>

      <Panel title="Fearzone Signals" aside={<span className="eyebrow">{fearzoneTickers.length} names</span>}>
        {!isLoading && fearzoneTickers.length === 0 ? <p className="panel-copy">No fearzone signals for this date.</p> : null}
        {fearzoneTickers.length > 0 ? (
          <div className="ticker-chip-grid">
            {fearzoneTickers.map((ticker) => (
              <Link
                key={`fearzone-${ticker}`}
                className="table-action-button table-link-button"
                to={chartDate ? `/charts?ticker=${encodeURIComponent(ticker)}&date=${encodeURIComponent(chartDate)}` : `/charts?ticker=${encodeURIComponent(ticker)}`}
              >
                {ticker}
              </Link>
            ))}
          </div>
        ) : null}
      </Panel>

      <Panel title="Grouped Overlaps">
        {isLoading ? <LoadingBlock label="Loading overlap candidates…" compact /> : null}
        {!isLoading && candidates.length === 0 ? <p className="panel-copy">No overlap candidates in this daily report.</p> : null}
        {groupedCandidates.map((group) => (
          <section key={group.key} className="overlap-group-section">
            <h2 className="overlap-group-title">{group.label}</h2>
            <div className="card-grid overlap-cards overlap-summary-cards">
              <article className="metric-card overlap-summary-card">
                <h3>Tickers</h3>
                <div className="metric-value">{group.entries.length}</div>
              </article>
              <article className="metric-card overlap-summary-card">
                <h3>Top Group Overlap</h3>
                <div className="metric-value">{group.entries[0]?.group_overlap_count ?? 0}</div>
              </article>
            </div>
            {group.entries.length === 0 ? (
              <p className="panel-copy">No {group.label.toLowerCase()} overlap candidates for this date.</p>
            ) : (
              <div className="data-table-responsive">
                <table className="data-table">
                  <thead>
                    <tr>
                      <th>Ticker</th>
                      <th>{group.label} Overlap</th>
                      <th>Total Overlap</th>
                      <th>ADR14</th>
                      <th>Trim</th>
                      <th>{group.label} Pipelines</th>
                      <th>Sector</th>
                    </tr>
                  </thead>
                  <tbody>
                    {group.entries.map((entry) => (
                      <tr key={`${group.key}-${entry.ticker}`}>
                        <td data-label="Ticker">
                          <Link
                            className="table-action-button table-link-button"
                            to={chartDate ? `/charts?ticker=${encodeURIComponent(entry.ticker)}&date=${encodeURIComponent(chartDate)}` : `/charts?ticker=${encodeURIComponent(entry.ticker)}`}
                          >
                            {entry.ticker}
                          </Link>
                        </td>
                        <td data-label={`${group.label} Overlap`}>{entry.group_overlap_count}</td>
                        <td data-label="Total Overlap">{entry.pipeline_count}</td>
                        <td data-label="ADR14">
                          {entry.adr14_pct == null ? (
                            "-"
                          ) : (
                            <span className={entry.adr14_in_range == null ? undefined : `adr-badge ${entry.adr14_in_range ? "is-in-range" : "is-out-of-range"}`}>
                              {entry.adr14_pct.toFixed(2)}%
                            </span>
                          )}
                        </td>
                        <td data-label="Trim">
                          {entry.trim_warning
                            ? entry.atr_multiple_from_50ma != null
                              ? `Warn ${entry.atr_multiple_from_50ma.toFixed(2)}x`
                              : "Warn"
                            : "-"}
                        </td>
                        <td data-label={`${group.label} Pipelines`}>{entry.group_pipeline_labels.join(", ")}</td>
                        <td data-label="Sector">{entry.sector ?? "-"}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            )}
          </section>
        ))}
      </Panel>
    </div>
  );
}

function buildPipelineGroups(pipelineStatus: OverlapPipelineStatus[]) {
  const order: Array<{
    key: "bullish" | "bearish" | "other";
    label: string;
    sections: Array<{
      key: string;
      label: string;
      match: (item: OverlapPipelineStatus) => boolean;
    }>;
  }> = [
    {
      key: "bullish",
      label: "Bullish",
      sections: [
        {
          key: "leaders",
          label: "Leaders",
          match: (item) => (item.bias_group ?? "other") === "bullish" && (item.bullish_subgroup ?? "leaders") === "leaders",
        },
        {
          key: "bottoming",
          label: "Bottoming",
          match: (item) => (item.bias_group ?? "other") === "bullish" && (item.bullish_subgroup ?? "") === "bottoming",
        },
      ],
    },
    {
      key: "bearish",
      label: "Bearish",
      sections: [
        {
          key: "bearish",
          label: "",
          match: (item) => (item.bias_group ?? "other") === "bearish",
        },
      ],
    },
    {
      key: "other",
      label: "Other",
      sections: [
        {
          key: "other",
          label: "",
          match: (item) => (item.bias_group ?? "other") === "other",
        },
      ],
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
          items: pipelineStatus.filter(section.match),
        }))
        .filter((section) => section.items.length > 0),
    }))
    .filter((group) => group.sections.length > 0);
}

function buildGroupedOverlapCandidates(candidates: OverlapEntry[], pipelineStatus: OverlapPipelineStatus[]) {
  const pipelineById = new Map(pipelineStatus.map((item) => [item.id, item] as const));
  const groupDefs = [
    {
      key: "leaders",
      label: "Leaders",
      match: (pipeline: OverlapPipelineStatus | undefined) =>
        (pipeline?.bias_group ?? "other") === "bullish" && (pipeline?.bullish_subgroup ?? "leaders") === "leaders",
    },
    {
      key: "bottoming",
      label: "Bottoming",
      match: (pipeline: OverlapPipelineStatus | undefined) =>
        (pipeline?.bias_group ?? "other") === "bullish" && (pipeline?.bullish_subgroup ?? "") === "bottoming",
    },
    {
      key: "bearish",
      label: "Bearish",
      match: (pipeline: OverlapPipelineStatus | undefined) => (pipeline?.bias_group ?? "other") === "bearish",
    },
  ] as const;

  return groupDefs.map((group) => ({
    key: group.key,
    label: group.label,
    entries: candidates
      .map((entry) => {
        const matchingPipelines = (entry.pipelines ?? []).filter((pipelineId) => group.match(pipelineById.get(pipelineId)));
        if (matchingPipelines.length === 0) {
          return null;
        }
        return {
          ...entry,
          group_overlap_count: matchingPipelines.length,
          group_pipeline_labels: matchingPipelines.map((pipelineId) => pipelineById.get(pipelineId)?.label ?? pipelineId),
        };
      })
      .filter((entry): entry is OverlapEntry & { group_overlap_count: number; group_pipeline_labels: string[] } => entry !== null)
      .sort((left, right) => right.group_overlap_count - left.group_overlap_count || right.pipeline_count - left.pipeline_count || left.ticker.localeCompare(right.ticker)),
  }));
}
