import { useEffect, useMemo, useState, type FormEvent } from "react";
import { ExclusionDialog } from "../components/ExclusionDialog";
import { LoadingBlock } from "../components/LoadingBlock";
import { Panel } from "../components/Panel";
import { fetchJson } from "../lib/api";
import { formatCount, formatLocalDate, formatLocalDateTime } from "../lib/format";
import type { AdminResponse, ExclusionEntry, PartialTickerDetailResponse } from "../lib/types";

const EMPTY_ADMIN_RESPONSE: AdminResponse = {
  excluded_tickers: [],
  excluded_count: 0,
  database_status: {
    database_configured: false,
    coverage_start: "2020-01-01",
    coverage_end: "",
    target_universe_count: 0,
    db_ticker_count: 0,
    covered_ticker_count: 0,
    partial_ticker_count: 0,
    missing_ticker_count: 0,
    total_bar_rows: 0,
    overall_first_trade_date: null,
    overall_last_trade_date: null,
    latest_metadata_update_at: null,
    stale_ticker_count: 0,
    coverage_percent: 0,
    sample_missing_tickers: [],
    sample_partial_tickers: [],
    notes: [],
  },
};

export function AdminPage() {
  const [payload, setPayload] = useState<AdminResponse>(EMPTY_ADMIN_RESPONSE);
  const [coverageStart, setCoverageStart] = useState("2020-01-01");
  const [syncStartDate, setSyncStartDate] = useState("2020-01-01");
  const [syncEndDate, setSyncEndDate] = useState("");
  const [syncTickers, setSyncTickers] = useState("");
  const [chunkSize, setChunkSize] = useState("100");
  const [isLaunching, setIsLaunching] = useState(false);
  const [launchMessage, setLaunchMessage] = useState("");
  const [isLoading, setIsLoading] = useState(true);
  const [exclusionFilter, setExclusionFilter] = useState("");
  const [selectedGapTicker, setSelectedGapTicker] = useState("");
  const [partialDetail, setPartialDetail] = useState<PartialTickerDetailResponse | null>(null);
  const [isGapLoading, setIsGapLoading] = useState(false);
  const [gapError, setGapError] = useState("");
  const [selectedExclusion, setSelectedExclusion] = useState<ExclusionEntry | null>(null);
  const [isRemoving, setIsRemoving] = useState(false);
  const [notice, setNotice] = useState("");

  const loadAdmin = (start: string) => {
    setIsLoading(true);
    const query = new URLSearchParams({ coverageStart: start });
    void fetchJson<AdminResponse>(`/api/admin/exclusions?${query.toString()}`)
      .then(setPayload)
      .catch(() => {
        setPayload({
          ...EMPTY_ADMIN_RESPONSE,
          database_status: {
            ...EMPTY_ADMIN_RESPONSE.database_status,
            notes: ["Failed to load admin data."],
          },
        });
      })
      .finally(() => setIsLoading(false));
  };

  useEffect(() => {
    loadAdmin(coverageStart);
  }, [coverageStart]);

  const handleLaunchSync = async (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    setIsLaunching(true);
    setLaunchMessage("");
    try {
      const tickers = syncTickers
        .split(/[\s,]+/)
        .map((item) => item.trim().toUpperCase())
        .filter(Boolean);
      const payloadBody: Record<string, string | string[] | number> = {
        start_date: syncStartDate,
      };
      if (syncEndDate.trim()) {
        payloadBody.end_date = syncEndDate.trim();
      }
      if (tickers.length > 0) {
        payloadBody.tickers = tickers;
      }
      if (chunkSize.trim()) {
        payloadBody.chunk_size = Number(chunkSize);
      }
      const response = await fetchJson<{ ok: boolean; job_id: string }>("/api/admin/history-sync", {
        method: "POST",
        body: JSON.stringify(payloadBody),
      });
      setLaunchMessage(`Sync job launched: ${response.job_id}`);
    } catch (error) {
      setLaunchMessage(error instanceof Error ? error.message : "Failed to launch sync job.");
    } finally {
      setIsLaunching(false);
    }
  };

  const handleInspectGap = async (ticker: string) => {
    setSelectedGapTicker(ticker);
    setGapError("");
    setIsGapLoading(true);
    try {
      const query = new URLSearchParams({ coverageStart });
      const detail = await fetchJson<PartialTickerDetailResponse>(`/api/admin/partial-tickers/${ticker}?${query.toString()}`);
      setPartialDetail(detail);
    } catch (error) {
      setPartialDetail(null);
      setGapError(error instanceof Error ? error.message : "Failed to load missing-date detail.");
    } finally {
      setIsGapLoading(false);
    }
  };

  const handleRemoveExclusion = async (reason: string) => {
    if (!selectedExclusion) {
      return;
    }
    setIsRemoving(true);
    try {
      await fetchJson<{ ok: boolean }>(`/api/admin/exclusions/${selectedExclusion.ticker}/remove`, {
        method: "POST",
        body: JSON.stringify({ reason }),
      });
      setNotice(`${selectedExclusion.ticker} removed from removable exclusions.`);
      setSelectedExclusion(null);
      loadAdmin(coverageStart);
    } catch (error) {
      setNotice(error instanceof Error ? error.message : "Failed to remove exclusion.");
    } finally {
      setIsRemoving(false);
    }
  };

  const filteredExclusions = useMemo(() => {
    const query = exclusionFilter.trim().toLowerCase();
    if (!query) {
      return payload.excluded_tickers;
    }
    return payload.excluded_tickers.filter((entry) =>
      [entry.ticker, entry.reason, entry.reasons.join(" "), entry.sources.join(" ")].join(" ").toLowerCase().includes(query),
    );
  }, [exclusionFilter, payload.excluded_tickers]);

  const db = payload.database_status;

  return (
    <div className="page-grid">
      <Panel title="Postgres History Coverage" aside={<span className="eyebrow">{db.coverage_percent}% covered</span>}>
        {isLoading ? <LoadingBlock label="Loading admin coverage…" /> : null}
        <div className="run-toolbar">
          <div className="run-params-grid">
            <label className="field">
              <span>Coverage Start</span>
              <input type="date" value={coverageStart} onChange={(event) => setCoverageStart(event.target.value)} />
            </label>
          </div>

          <div className="card-grid overlap-cards">
            <article className="metric-card">
              <h3>Target Universe</h3>
              <div className="metric-value">{formatCount(db.target_universe_count)}</div>
            </article>
            <article className="metric-card">
              <h3>Fully Covered</h3>
              <div className="metric-value">{formatCount(db.covered_ticker_count)}</div>
            </article>
            <article className="metric-card">
              <h3>Missing / Partial</h3>
              <div className="metric-value">{formatCount(db.missing_ticker_count + db.partial_ticker_count)}</div>
            </article>
          </div>

          <div className="data-table-responsive">
            <table className="data-table">
              <tbody>
                <tr>
                  <td data-label="Metric">Database Configured</td>
                  <td data-label="Value">{db.database_configured ? "Yes" : "No"}</td>
                </tr>
                <tr>
                  <td data-label="Metric">Coverage Window</td>
                  <td data-label="Value">{db.coverage_start} to {db.coverage_end || "-"}</td>
                </tr>
                <tr>
                  <td data-label="Metric">DB Tickers</td>
                  <td data-label="Value">{formatCount(db.db_ticker_count)}</td>
                </tr>
                <tr>
                  <td data-label="Metric">Total Bar Rows</td>
                  <td data-label="Value">{formatCount(db.total_bar_rows)}</td>
                </tr>
                <tr>
                  <td data-label="Metric">Overall First / Last Trade Date</td>
                  <td data-label="Value">{db.overall_first_trade_date || "-"} / {db.overall_last_trade_date || "-"}</td>
                </tr>
                <tr>
                  <td data-label="Metric">Latest Metadata Update</td>
                  <td data-label="Value">{formatLocalDateTime(db.latest_metadata_update_at)}</td>
                </tr>
                <tr>
                  <td data-label="Metric">Partial Tickers</td>
                  <td data-label="Value">{formatCount(db.partial_ticker_count)}</td>
                </tr>
                <tr>
                  <td data-label="Metric">Missing Tickers</td>
                  <td data-label="Value">{formatCount(db.missing_ticker_count)}</td>
                </tr>
                <tr>
                  <td data-label="Metric">Stale Tickers</td>
                  <td data-label="Value">{formatCount(db.stale_ticker_count)}</td>
                </tr>
              </tbody>
            </table>
          </div>

          {db.notes.length > 0 ? <div className="panel-copy">{db.notes.join(" ")}</div> : null}

          <div className="admin-sample-grid">
            {db.sample_missing_tickers.length > 0 ? (
              <div>
                <div className="eyebrow">Sample Missing Tickers</div>
                <div className="pill-list">
                  {db.sample_missing_tickers.map((item) => (
                    <button key={item.ticker} className="symbol-pill symbol-pill-button" type="button" onClick={() => void handleInspectGap(item.ticker)}>
                      {item.ticker}
                    </button>
                  ))}
                </div>
              </div>
            ) : null}

            {db.sample_partial_tickers.length > 0 ? (
              <div>
                <div className="eyebrow">Sample Partial Tickers</div>
                <div className="pill-list">
                  {db.sample_partial_tickers.map((item) => (
                    <button key={item.ticker} className="symbol-pill symbol-pill-button" type="button" onClick={() => void handleInspectGap(item.ticker)}>
                      {item.ticker}
                    </button>
                  ))}
                </div>
              </div>
            ) : null}
          </div>

          {isGapLoading ? <LoadingBlock label={`Inspecting coverage gaps for ${selectedGapTicker}…`} compact /> : null}
          {gapError ? <p className="panel-copy">{gapError}</p> : null}
          {partialDetail ? (
            <div className="detail-card">
              <div className="detail-card-head">
                <div>
                  <div className="eyebrow">Missing Coverage Detail</div>
                  <div className="ticker-symbol">{partialDetail.ticker}</div>
                </div>
                <div className="detail-card-metrics">
                  <span className="file-meta">Bars {formatCount(partialDetail.bar_count)}</span>
                  <span className="file-meta">Missing dates {formatCount(partialDetail.missing_date_count)}</span>
                </div>
              </div>
              <div className="detail-grid">
                <div>
                  <div className="eyebrow">Coverage Window</div>
                  <div className="panel-copy">
                    {formatLocalDate(partialDetail.coverage_start)} to {formatLocalDate(partialDetail.coverage_end)}
                  </div>
                </div>
                <div>
                  <div className="eyebrow">First / Last Trade Date</div>
                  <div className="panel-copy">
                    {partialDetail.first_trade_date ? formatLocalDate(partialDetail.first_trade_date) : "-"} /{" "}
                    {partialDetail.last_trade_date ? formatLocalDate(partialDetail.last_trade_date) : "-"}
                  </div>
                </div>
              </div>
              <div className="detail-subsection">
                <div className="eyebrow">Missing Ranges</div>
                <div className="range-list">
                  {partialDetail.missing_ranges.map((range) => (
                    <article key={`${range.start}-${range.end}`} className="range-item">
                      <strong>{formatLocalDate(range.start)}</strong>
                      <span className="panel-copy">
                        to {formatLocalDate(range.end)} · {formatCount(range.days)} days
                      </span>
                    </article>
                  ))}
                </div>
              </div>
              {partialDetail.sample_missing_dates.length > 0 ? (
                <div className="detail-subsection">
                  <div className="eyebrow">Sample Missing Dates</div>
                  <div className="pill-list">
                    {partialDetail.sample_missing_dates.map((item) => (
                      <span key={item} className="symbol-pill">
                        {item}
                      </span>
                    ))}
                  </div>
                </div>
              ) : null}
            </div>
          ) : null}
        </div>
      </Panel>

      <Panel title="Fetch Market History" aside={<span className="eyebrow">Launches background sync job</span>}>
        <form className="run-toolbar" onSubmit={(event) => void handleLaunchSync(event)}>
          <div className="run-params-grid">
            <label className="field">
              <span>Start Date</span>
              <input type="date" value={syncStartDate} onChange={(event) => setSyncStartDate(event.target.value)} required />
            </label>
            <label className="field">
              <span>End Date</span>
              <input type="date" value={syncEndDate} onChange={(event) => setSyncEndDate(event.target.value)} />
            </label>
            <label className="field">
              <span>Chunk Size</span>
              <input type="number" min="1" max="500" value={chunkSize} onChange={(event) => setChunkSize(event.target.value)} />
            </label>
          </div>
          <label className="field">
            <span>Selected Tickers</span>
            <textarea
              value={syncTickers}
              onChange={(event) => setSyncTickers(event.target.value)}
              rows={3}
              placeholder="Leave blank for all tickers. Example: AAPL NVDA CRWD"
            />
          </label>
          <div className="run-action-footer">
            <button className="primary-button" type="submit" disabled={isLaunching}>
              {isLaunching ? "Launching..." : "Fetch History"}
            </button>
            <span className="panel-copy">
              Leave end date blank for up-to-today fetch. Leave tickers blank for whole configured universe.
            </span>
          </div>
          {launchMessage ? <div className="panel-copy">{launchMessage}</div> : null}
        </form>
      </Panel>

      <Panel title="Exclusions" aside={<span className="eyebrow">{payload.excluded_count} symbols</span>}>
        <div className="run-toolbar">
          <label className="field">
            <span>Filter exclusions</span>
            <input
              type="text"
              value={exclusionFilter}
              onChange={(event) => setExclusionFilter(event.target.value)}
              placeholder="Ticker, reason, source"
            />
          </label>
          {notice ? <p className="panel-copy">{notice}</p> : null}
          <div className="exclusion-grid">
            {filteredExclusions.map((entry) => (
              <article key={entry.ticker} className="exclusion-card">
                <div className="detail-card-head">
                  <div>
                    <div className="ticker-symbol">{entry.ticker}</div>
                    <div className="file-meta">{entry.sources.join(" · ")}</div>
                  </div>
                  {entry.removable ? (
                    <button className="ghost-button" type="button" onClick={() => setSelectedExclusion(entry)}>
                      Remove
                    </button>
                  ) : (
                    <span className="eyebrow">Read only</span>
                  )}
                </div>
                <p className="panel-copy">{entry.reason || "No reason recorded."}</p>
                {entry.reasons.length > 1 ? (
                  <div className="pill-list">
                    {entry.reasons.map((reason) => (
                      <span key={reason} className="symbol-pill">
                        {reason}
                      </span>
                    ))}
                  </div>
                ) : null}
              </article>
            ))}
          </div>
        </div>
      </Panel>

      <ExclusionDialog
        isOpen={selectedExclusion != null}
        mode="remove"
        ticker={selectedExclusion?.ticker ?? ""}
        title={selectedExclusion ? `Remove ${selectedExclusion.ticker} from exclusions` : "Remove exclusion"}
        confirmLabel="Remove Exclusion"
        helperText="This removes the ticker from user-editable exclusion files and records your removal reason in the audit log."
        submitting={isRemoving}
        onClose={() => setSelectedExclusion(null)}
        onSubmit={handleRemoveExclusion}
      />
    </div>
  );
}
