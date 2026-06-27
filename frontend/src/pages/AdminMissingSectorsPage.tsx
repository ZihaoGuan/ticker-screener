import { useEffect, useMemo, useState } from "react";
import { AdminSubnav } from "../components/AdminSubnav";
import { ExclusionDialog } from "../components/ExclusionDialog";
import { LoadingBlock } from "../components/LoadingBlock";
import { Panel } from "../components/Panel";
import { fetchJson } from "../lib/api";
import { formatCount, formatLocalDateTime } from "../lib/format";
import type { MissingSectorAdminResponse } from "../lib/types";

const EMPTY_MISSING_SECTOR_RESPONSE: MissingSectorAdminResponse = {
  database_configured: false,
  missing_count: 0,
  tickers: [],
  available_sectors: [],
  notes: [],
};

const EXCLUSION_REASON_OPTIONS = [
  "Bad data quality",
  "Not tradable / structured product",
  "Too illiquid",
  "Too small-cap / low quality",
  "No longer want in scans",
] as const;

export function AdminMissingSectorsPage() {
  const [payload, setPayload] = useState<MissingSectorAdminResponse>(EMPTY_MISSING_SECTOR_RESPONSE);
  const [filter, setFilter] = useState("");
  const [sectorSelections, setSectorSelections] = useState<Record<string, string>>({});
  const [isLoading, setIsLoading] = useState(true);
  const [isSavingSector, setIsSavingSector] = useState(false);
  const [isSavingExclusion, setIsSavingExclusion] = useState(false);
  const [notice, setNotice] = useState("");
  const [selectedExcludeTicker, setSelectedExcludeTicker] = useState("");

  const loadMissingSectors = () => {
    setIsLoading(true);
    void fetchJson<MissingSectorAdminResponse>("/api/admin/missing-sectors")
      .then((result) => {
        setPayload(result);
        setSectorSelections((current) => {
          const next: Record<string, string> = {};
          result.tickers.forEach((item) => {
            next[item.ticker] = current[item.ticker] ?? item.suggested_sector ?? "";
          });
          return next;
        });
      })
      .catch(() => {
        setPayload({
          ...EMPTY_MISSING_SECTOR_RESPONSE,
          notes: ["Failed to load missing-sector tickers."],
        });
      })
      .finally(() => setIsLoading(false));
  };

  useEffect(() => {
    loadMissingSectors();
  }, []);

  const filteredTickers = useMemo(() => {
    const query = filter.trim().toLowerCase();
    if (!query) {
      return payload.tickers;
    }
    return payload.tickers.filter((entry) =>
      [entry.ticker, entry.exchange, entry.industry, entry.source, entry.suggested_sector, entry.suggested_industry]
        .join(" ")
        .toLowerCase()
        .includes(query),
    );
  }, [filter, payload.tickers]);

  const handleAssignSector = async (ticker: string) => {
    const sector = (sectorSelections[ticker] || "").trim();
    if (!sector) {
      setNotice("Select a sector before saving.");
      return;
    }
    setIsSavingSector(true);
    setNotice("");
    try {
      await fetchJson<{ ok: boolean; entry: { ticker: string; sector: string } }>(`/api/admin/ticker-sectors/${ticker}`, {
        method: "POST",
        body: JSON.stringify({ sector }),
      });
      setNotice(`${ticker} sector set to ${sector}.`);
      loadMissingSectors();
    } catch (error) {
      setNotice(error instanceof Error ? error.message : "Failed to update sector.");
    } finally {
      setIsSavingSector(false);
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
      loadMissingSectors();
    } catch (error) {
      setNotice(error instanceof Error ? error.message : "Failed to add exclusion.");
    } finally {
      setIsSavingExclusion(false);
    }
  };

  return (
    <div className="page-grid">
      <AdminSubnav
        title="Missing Sector Assignments"
        description="Repair unresolved sector metadata here, or exclude low-value names directly from the queue."
      />

      <Panel title="Missing Sector Assignments" aside={<span className="eyebrow">{formatCount(payload.missing_count)} tickers</span>}>
        {isLoading ? <LoadingBlock label="Loading missing-sector tickers…" /> : null}
        <div className="run-toolbar">
          <div className="run-action-footer">
            <label className="field" style={{ flex: "1 1 20rem" }}>
              <span>Filter tickers</span>
              <input
                type="text"
                value={filter}
                onChange={(event) => setFilter(event.target.value)}
                placeholder="Ticker, exchange, industry, source"
              />
            </label>
            <button className="ghost-button" type="button" onClick={loadMissingSectors} disabled={isLoading || isSavingSector || isSavingExclusion}>
              {isLoading ? "Refreshing..." : "Refresh Missing Sectors"}
            </button>
          </div>

          {payload.notes.length > 0 ? <div className="panel-copy">{payload.notes.join(" ")}</div> : null}
          {notice ? <div className="panel-copy">{notice}</div> : null}

          <div className="pill-list">
            {payload.available_sectors.map((sector) => (
              <span key={sector} className="symbol-pill">{sector}</span>
            ))}
          </div>

          <div className="data-table-responsive">
            <table className="data-table">
              <thead>
                <tr>
                  <th>Ticker</th>
                  <th>Industry</th>
                  <th>Suggested</th>
                  <th>Source</th>
                  <th>Updated</th>
                  <th>Actions</th>
                </tr>
              </thead>
              <tbody>
                {filteredTickers.length === 0 ? (
                  <tr>
                    <td colSpan={6}>
                      {isLoading ? "Loading missing-sector tickers..." : "No tickers missing sector."}
                    </td>
                  </tr>
                ) : (
                  filteredTickers.map((item) => (
                    <tr key={item.ticker}>
                      <td data-label="Ticker">
                        <div className="admin-job-cell">
                          <strong>{item.ticker}</strong>
                          <span className="file-meta">{item.exchange || "-"}</span>
                        </div>
                      </td>
                      <td data-label="Industry" className="file-meta">
                        {item.industry || item.suggested_industry || "-"}
                      </td>
                      <td data-label="Suggested">{item.suggested_sector || "-"}</td>
                      <td data-label="Source">{item.source || "-"}</td>
                      <td data-label="Updated">{formatLocalDateTime(item.updated_at)}</td>
                      <td data-label="Actions">
                        <div className="button-row">
                          <select
                            value={sectorSelections[item.ticker] ?? ""}
                            onChange={(event) =>
                              setSectorSelections((current) => ({
                                ...current,
                                [item.ticker]: event.target.value,
                              }))
                            }
                            disabled={isSavingSector}
                          >
                            <option value="">Select sector</option>
                            {payload.available_sectors.map((sector) => (
                              <option key={`${item.ticker}-${sector}`} value={sector}>
                                {sector}
                              </option>
                            ))}
                          </select>
                          <button
                            className="table-action-button"
                            type="button"
                            onClick={() => void handleAssignSector(item.ticker)}
                            disabled={isSavingSector || !(sectorSelections[item.ticker] || "").trim()}
                          >
                            {isSavingSector ? "Saving..." : "Save"}
                          </button>
                          <button
                            className="table-action-button"
                            type="button"
                            onClick={() => setSelectedExcludeTicker(item.ticker)}
                            disabled={isSavingExclusion}
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
        helperText="This removes the ticker from scanner/admin result surfaces that respect the exclusion registry."
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
