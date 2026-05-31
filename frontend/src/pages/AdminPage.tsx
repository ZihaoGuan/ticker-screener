import { useEffect, useState, type FormEvent } from "react";
import { Panel } from "../components/Panel";
import { fetchJson } from "../lib/api";
import type { AdminResponse } from "../lib/types";

export function AdminPage() {
  const [payload, setPayload] = useState<AdminResponse>({
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
  });
  const [coverageStart, setCoverageStart] = useState("2020-01-01");
  const [syncStartDate, setSyncStartDate] = useState("2020-01-01");
  const [syncEndDate, setSyncEndDate] = useState("");
  const [syncTickers, setSyncTickers] = useState("");
  const [chunkSize, setChunkSize] = useState("100");
  const [isLaunching, setIsLaunching] = useState(false);
  const [launchMessage, setLaunchMessage] = useState("");

  const loadAdmin = (start: string) => {
    const query = new URLSearchParams({ coverageStart: start });
    void fetchJson<AdminResponse>(`/api/admin/exclusions?${query.toString()}`)
      .then(setPayload)
      .catch(() =>
        setPayload((current) => ({
          ...current,
          excluded_tickers: [],
          excluded_count: 0,
          database_status: {
            ...current.database_status,
            notes: ["Failed to load admin data."],
          },
        })),
      );
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
    } catch {
      setLaunchMessage("Failed to launch sync job.");
    } finally {
      setIsLaunching(false);
    }
  };

  const db = payload.database_status;

  return (
    <div className="page-grid">
      <Panel title="Postgres History Coverage" aside={<span className="eyebrow">{db.coverage_percent}% covered</span>}>
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
              <div className="metric-value">{db.target_universe_count}</div>
            </article>
            <article className="metric-card">
              <h3>Fully Covered</h3>
              <div className="metric-value">{db.covered_ticker_count}</div>
            </article>
            <article className="metric-card">
              <h3>Missing / Partial</h3>
              <div className="metric-value">{db.missing_ticker_count + db.partial_ticker_count}</div>
            </article>
          </div>

          <table className="data-table">
            <tbody>
              <tr>
                <td>Database Configured</td>
                <td>{db.database_configured ? "Yes" : "No"}</td>
              </tr>
              <tr>
                <td>Coverage Window</td>
                <td>{db.coverage_start} to {db.coverage_end || "-"}</td>
              </tr>
              <tr>
                <td>DB Tickers</td>
                <td>{db.db_ticker_count}</td>
              </tr>
              <tr>
                <td>Total Bar Rows</td>
                <td>{db.total_bar_rows.toLocaleString()}</td>
              </tr>
              <tr>
                <td>Overall First / Last Trade Date</td>
                <td>{db.overall_first_trade_date || "-"} / {db.overall_last_trade_date || "-"}</td>
              </tr>
              <tr>
                <td>Latest Metadata Update</td>
                <td>{db.latest_metadata_update_at || "-"}</td>
              </tr>
              <tr>
                <td>Partial Tickers</td>
                <td>{db.partial_ticker_count}</td>
              </tr>
              <tr>
                <td>Missing Tickers</td>
                <td>{db.missing_ticker_count}</td>
              </tr>
              <tr>
                <td>Stale Tickers</td>
                <td>{db.stale_ticker_count}</td>
              </tr>
            </tbody>
          </table>

          {db.notes.length > 0 ? (
            <div className="panel-copy">{db.notes.join(" ")}</div>
          ) : null}

          {db.sample_missing_tickers.length > 0 ? (
            <div>
              <div className="eyebrow">Sample Missing Tickers</div>
              <div className="pill-list">
                {db.sample_missing_tickers.map((item) => (
                  <span key={item} className="symbol-pill">{item}</span>
                ))}
              </div>
            </div>
          ) : null}

          {db.sample_partial_tickers.length > 0 ? (
            <div>
              <div className="eyebrow">Sample Partial Tickers</div>
              <div className="pill-list">
                {db.sample_partial_tickers.map((item) => (
                  <span key={item} className="symbol-pill">{item}</span>
                ))}
              </div>
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
            <label className="field">
              <span>Selected Tickers</span>
              <input
                type="text"
                value={syncTickers}
                onChange={(event) => setSyncTickers(event.target.value)}
                placeholder="Leave blank for all tickers. Example: AAPL NVDA CRWD"
              />
            </label>
          </div>
          <div className="run-action-footer">
            <button className="primary-button" type="submit" disabled={isLaunching}>
              {isLaunching ? "Launching..." : "Fetch History"}
            </button>
            <span className="panel-copy">
              Blank ticker list = full configured universe. Non-blank = selected tickers only.
            </span>
          </div>
          {launchMessage ? <div className="panel-copy">{launchMessage}</div> : null}
        </form>
      </Panel>

      <Panel title="Exclusions" aside={<span className="eyebrow">{payload.excluded_count} symbols</span>}>
        <div className="pill-list">
          {payload.excluded_tickers.map((item) => (
            <span key={item} className="symbol-pill">
              {item}
            </span>
          ))}
        </div>
      </Panel>
    </div>
  );
}
