import { useEffect, useMemo, useState } from "react";
import { AdminSubnav } from "../components/AdminSubnav";
import { ExclusionDialog } from "../components/ExclusionDialog";
import { LoadingBlock } from "../components/LoadingBlock";
import { Panel } from "../components/Panel";
import { fetchJson } from "../lib/api";
import { formatCount, formatLocalDateTime } from "../lib/format";
import type { MissingFinvizTickersAdminResponse } from "../lib/types";

const EMPTY_MISSING_FINVIZ_RESPONSE: MissingFinvizTickersAdminResponse = {
  missing_count: 0,
  tickers: [],
  notes: [],
};

const EXCLUSION_REASON_OPTIONS = [
  "Bad data quality",
  "Not tradable / structured product",
  "Too illiquid",
  "Too small-cap / low quality",
  "No longer want in scans",
] as const;

export function AdminMissingFinvizTickersPage() {
  const [payload, setPayload] = useState<MissingFinvizTickersAdminResponse>(EMPTY_MISSING_FINVIZ_RESPONSE);
  const [filter, setFilter] = useState("");
  const [isLoading, setIsLoading] = useState(true);
  const [isRemoving, setIsRemoving] = useState(false);
  const [isSavingExclusion, setIsSavingExclusion] = useState(false);
  const [notice, setNotice] = useState("");
  const [selectedExcludeTicker, setSelectedExcludeTicker] = useState("");

  const loadMissingFinvizTickers = () => {
    setIsLoading(true);
    void fetchJson<MissingFinvizTickersAdminResponse>("/api/admin/finviz-missing-tickers")
      .then(setPayload)
      .catch(() => {
        setPayload({
          ...EMPTY_MISSING_FINVIZ_RESPONSE,
          notes: ["Failed to load Finviz missing-ticker registry."],
        });
      })
      .finally(() => setIsLoading(false));
  };

  useEffect(() => {
    loadMissingFinvizTickers();
  }, []);

  const filteredTickers = useMemo(() => {
    const query = filter.trim().toLowerCase();
    if (!query) {
      return payload.tickers;
    }
    return payload.tickers.filter((entry) =>
      [entry.ticker, entry.source, entry.reason, entry.first_seen_at, entry.last_seen_at].join(" ").toLowerCase().includes(query),
    );
  }, [filter, payload.tickers]);

  const handleRemoveMissingFinvizTicker = async (ticker: string) => {
    setIsRemoving(true);
    setNotice("");
    try {
      await fetchJson<{ ok: boolean; entry: { ticker: string } }>(`/api/admin/finviz-missing-tickers/${ticker}/remove`, {
        method: "POST",
      });
      setNotice(`${ticker} removed from Finviz 404 skip list.`);
      loadMissingFinvizTickers();
    } catch (error) {
      setNotice(error instanceof Error ? error.message : "Failed to remove Finviz missing ticker.");
    } finally {
      setIsRemoving(false);
    }
  };

  const handleAddExclusion = async (reason: string) => {
    if (!selectedExcludeTicker) {
      return;
    }
    setIsSavingExclusion(true);
    setNotice("");
    try {
      await fetchJson<{ ok: boolean }>("/api/admin/exclusions", {
        method: "POST",
        body: JSON.stringify({ ticker: selectedExcludeTicker, reason }),
      });
      setNotice(`${selectedExcludeTicker} added to exclusions.`);
      setSelectedExcludeTicker("");
      loadMissingFinvizTickers();
    } catch (error) {
      setNotice(error instanceof Error ? error.message : "Failed to add exclusion.");
    } finally {
      setIsSavingExclusion(false);
    }
  };

  return (
    <div className="page-grid">
      <AdminSubnav
        title="Finviz Missing Tickers"
        description="Review the Finviz skip registry here, allow retries when a symbol recovers, or exclude noisy symbols entirely."
      />

      <Panel title="Finviz Missing Tickers" aside={<span className="eyebrow">{formatCount(payload.missing_count)} skipped</span>}>
        {isLoading ? <LoadingBlock label="Loading Finviz missing-ticker registry…" /> : null}
        <div className="run-toolbar">
          <div className="run-action-footer">
            <label className="field" style={{ flex: "1 1 20rem" }}>
              <span>Filter tickers</span>
              <input
                type="text"
                value={filter}
                onChange={(event) => setFilter(event.target.value)}
                placeholder="Ticker, source, reason"
              />
            </label>
            <button className="ghost-button" type="button" onClick={loadMissingFinvizTickers} disabled={isLoading || isRemoving || isSavingExclusion}>
              {isLoading ? "Refreshing..." : "Refresh Registry"}
            </button>
          </div>

          {payload.notes.length > 0 ? <div className="panel-copy">{payload.notes.join(" ")}</div> : null}
          {notice ? <div className="panel-copy">{notice}</div> : null}

          <div className="data-table-responsive">
            <table className="data-table">
              <thead>
                <tr>
                  <th>Ticker</th>
                  <th>Source</th>
                  <th>Hit Count</th>
                  <th>First Seen</th>
                  <th>Last Seen</th>
                  <th>Reason</th>
                  <th>Actions</th>
                </tr>
              </thead>
              <tbody>
                {filteredTickers.length === 0 ? (
                  <tr>
                    <td colSpan={7}>
                      {isLoading ? "Loading Finviz missing tickers..." : "No Finviz missing tickers recorded."}
                    </td>
                  </tr>
                ) : (
                  filteredTickers.map((item) => (
                    <tr key={item.ticker}>
                      <td data-label="Ticker">
                        <strong>{item.ticker}</strong>
                      </td>
                      <td data-label="Source">{item.source || "-"}</td>
                      <td data-label="Hit Count">{formatCount(item.hit_count)}</td>
                      <td data-label="First Seen">{formatLocalDateTime(item.first_seen_at)}</td>
                      <td data-label="Last Seen">{formatLocalDateTime(item.last_seen_at)}</td>
                      <td data-label="Reason" className="file-meta">{item.reason || "-"}</td>
                      <td data-label="Actions">
                        <div className="button-row">
                          <button
                            className="table-action-button"
                            type="button"
                            disabled={isRemoving}
                            onClick={() => void handleRemoveMissingFinvizTicker(item.ticker)}
                          >
                            {isRemoving ? "Removing..." : "Allow Retry"}
                          </button>
                          <button
                            className="table-action-button"
                            type="button"
                            disabled={isSavingExclusion}
                            onClick={() => setSelectedExcludeTicker(item.ticker)}
                          >
                            Exclude
                          </button>
                        </div>
                      </td>
                    </tr>
                  ))
                )}
              </tbody>
            </table>
          </div>
        </div>
      </Panel>

      <ExclusionDialog
        isOpen={Boolean(selectedExcludeTicker)}
        mode="add"
        ticker={selectedExcludeTicker}
        title={`Exclude ${selectedExcludeTicker}`}
        confirmLabel="Add Exclusion"
        helperText="Use this when a Finviz-missing symbol should stay out of scanner and admin review flows entirely."
        reasonOptions={[...EXCLUSION_REASON_OPTIONS]}
        submitting={isSavingExclusion}
        onClose={() => {
          if (!isSavingExclusion) {
            setSelectedExcludeTicker("");
          }
        }}
        onSubmit={handleAddExclusion}
      />
    </div>
  );
}
