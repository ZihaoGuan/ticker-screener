import { useEffect, useMemo, useState, type FormEvent } from "react";
import { AdminSubnav } from "../components/AdminSubnav";
import { ExclusionDialog } from "../components/ExclusionDialog";
import { LoadingBlock } from "../components/LoadingBlock";
import { Panel } from "../components/Panel";
import { fetchJson } from "../lib/api";
import { formatCount, formatLocalDate, formatLocalDateTime } from "../lib/format";
import type {
  AccessRequestSummary,
  AdminResponse,
  AuditEventSummary,
  AuditEventsResponse,
  ExclusionEntry,
  GammaExposurePlotAdminResponse,
  MissingFinvizTickersAdminResponse,
  MissingSectorAdminResponse,
  PartialTickerDetailResponse,
  RoleName,
} from "../lib/types";
import "./RunsPage.css";

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

const EMPTY_MISSING_SECTOR_RESPONSE: MissingSectorAdminResponse = {
  database_configured: false,
  missing_count: 0,
  tickers: [],
  available_sectors: [],
  notes: [],
};

const EMPTY_MISSING_FINVIZ_RESPONSE: MissingFinvizTickersAdminResponse = {
  missing_count: 0,
  tickers: [],
  notes: [],
};

export function AdminPage() {
  const [payload, setPayload] = useState<AdminResponse>(EMPTY_ADMIN_RESPONSE);
  const [missingSectorPayload, setMissingSectorPayload] = useState<MissingSectorAdminResponse>(EMPTY_MISSING_SECTOR_RESPONSE);
  const [missingFinvizPayload, setMissingFinvizPayload] = useState<MissingFinvizTickersAdminResponse>(EMPTY_MISSING_FINVIZ_RESPONSE);
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
  const [users, setUsers] = useState<
    Array<{ id: number; email: string; role: RoleName; is_active: boolean; created_at?: string | null; updated_at?: string | null; last_login_at?: string | null }>
  >([]);
  const [inviteEmail, setInviteEmail] = useState("");
  const [inviteRole, setInviteRole] = useState<RoleName>("visitor");
  const [isSavingUser, setIsSavingUser] = useState(false);
  const [accessRequests, setAccessRequests] = useState<AccessRequestSummary[]>([]);
  const [auditEvents, setAuditEvents] = useState<AuditEventSummary[]>([]);
  const [auditActorEmail, setAuditActorEmail] = useState("");
  const [auditAction, setAuditAction] = useState("");
  const [auditResourceType, setAuditResourceType] = useState("");
  const [auditFromDate, setAuditFromDate] = useState("");
  const [auditToDate, setAuditToDate] = useState("");
  const [isLoadingAudit, setIsLoadingAudit] = useState(false);
  const [isLaunchingGexSnapshot, setIsLaunchingGexSnapshot] = useState(false);
  const [gexSnapshotNotice, setGexSnapshotNotice] = useState("");
  const [isLaunchingMarketBreadth, setIsLaunchingMarketBreadth] = useState(false);
  const [marketBreadthNotice, setMarketBreadthNotice] = useState("");
  const [isLaunchingUptrendAnalysis, setIsLaunchingUptrendAnalysis] = useState(false);
  const [uptrendAnalysisNotice, setUptrendAnalysisNotice] = useState("");
  const [isLaunchingIbdMonitor, setIsLaunchingIbdMonitor] = useState(false);
  const [ibdMonitorNotice, setIbdMonitorNotice] = useState("");
  const [isLaunchingExposureCoach, setIsLaunchingExposureCoach] = useState(false);
  const [exposureCoachNotice, setExposureCoachNotice] = useState("");
  const [isRestartingWebApp, setIsRestartingWebApp] = useState(false);
  const [webRestartNotice, setWebRestartNotice] = useState("");
  const [gammaPlotPayload, setGammaPlotPayload] = useState<GammaExposurePlotAdminResponse | null>(null);
  const [isLoadingGammaPlot, setIsLoadingGammaPlot] = useState(true);
  const [gammaPlotNotice, setGammaPlotNotice] = useState("");
  const [missingSectorFilter, setMissingSectorFilter] = useState("");
  const [sectorSelections, setSectorSelections] = useState<Record<string, string>>({});
  const [isLoadingMissingSectors, setIsLoadingMissingSectors] = useState(true);
  const [isSavingSector, setIsSavingSector] = useState(false);
  const [sectorNotice, setSectorNotice] = useState("");
  const [missingFinvizFilter, setMissingFinvizFilter] = useState("");
  const [isLoadingMissingFinviz, setIsLoadingMissingFinviz] = useState(true);
  const [isRemovingMissingFinviz, setIsRemovingMissingFinviz] = useState(false);
  const [missingFinvizNotice, setMissingFinvizNotice] = useState("");

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

  const loadAudit = () => {
    setIsLoadingAudit(true);
    const query = new URLSearchParams();
    if (auditActorEmail.trim()) {
      query.set("actorEmail", auditActorEmail.trim());
    }
    if (auditAction.trim()) {
      query.set("action", auditAction.trim());
    }
    if (auditResourceType.trim()) {
      query.set("resourceType", auditResourceType.trim());
    }
    if (auditFromDate.trim()) {
      query.set("from", auditFromDate.trim());
    }
    if (auditToDate.trim()) {
      query.set("to", auditToDate.trim());
    }
    query.set("limit", "50");
    void fetchJson<AuditEventsResponse>(`/api/admin/audit-events?${query.toString()}`)
      .then((result) => setAuditEvents(result.events))
      .catch(() => setAuditEvents([]))
      .finally(() => setIsLoadingAudit(false));
  };

  const loadMissingSectors = () => {
    setIsLoadingMissingSectors(true);
    void fetchJson<MissingSectorAdminResponse>("/api/admin/missing-sectors")
      .then((result) => {
        setMissingSectorPayload(result);
        setSectorSelections((current) => {
          const next: Record<string, string> = {};
          result.tickers.forEach((item) => {
            next[item.ticker] = current[item.ticker] ?? item.suggested_sector ?? "";
          });
          return next;
        });
      })
      .catch(() => {
        setMissingSectorPayload({
          ...EMPTY_MISSING_SECTOR_RESPONSE,
          notes: ["Failed to load missing-sector tickers."],
        });
      })
      .finally(() => setIsLoadingMissingSectors(false));
  };

  const loadMissingFinvizTickers = () => {
    setIsLoadingMissingFinviz(true);
    void fetchJson<MissingFinvizTickersAdminResponse>("/api/admin/finviz-missing-tickers")
      .then(setMissingFinvizPayload)
      .catch(() => {
        setMissingFinvizPayload({
          ...EMPTY_MISSING_FINVIZ_RESPONSE,
          notes: ["Failed to load Finviz missing-ticker registry."],
        });
      })
      .finally(() => setIsLoadingMissingFinviz(false));
  };

  const loadGammaExposurePlot = () => {
    setIsLoadingGammaPlot(true);
    setGammaPlotNotice("");
    void fetchJson<GammaExposurePlotAdminResponse>("/api/admin/gamma-exposure-plot?symbol=SPX")
      .then(setGammaPlotPayload)
      .catch((error) => {
        setGammaPlotPayload(null);
        setGammaPlotNotice(error instanceof Error ? error.message : "Failed to load SPX gamma exposure plot.");
      })
      .finally(() => setIsLoadingGammaPlot(false));
  };

  useEffect(() => {
    loadAdmin(coverageStart);
    loadMissingSectors();
    loadMissingFinvizTickers();
    void fetchJson<{ users: Array<{ id: number; email: string; role: RoleName; is_active: boolean; created_at?: string | null; updated_at?: string | null; last_login_at?: string | null }>; access_requests?: AccessRequestSummary[] }>("/api/admin/users")
      .then((result) => {
        setUsers(result.users);
        setAccessRequests(result.access_requests ?? []);
      })
      .catch(() => {
        setUsers([]);
        setAccessRequests([]);
      });
  }, [coverageStart]);

  useEffect(() => {
    loadAudit();
  }, []);

  useEffect(() => {
    loadGammaExposurePlot();
  }, []);

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
      loadAudit();
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

  const handleFetchTodayGexSnapshot = async () => {
    setIsLaunchingGexSnapshot(true);
    setGexSnapshotNotice("");
    try {
      const today = new Date().toISOString().slice(0, 10);
      const response = await fetchJson<{ ok: boolean; job_id: string }>("/api/runs/flashalpha_gex_close", {
        method: "POST",
        body: JSON.stringify({ as_of_date: today }),
      });
      setGexSnapshotNotice(`FlashAlpha GEX snapshot launched for ${today}: ${response.job_id}`);
      loadAudit();
    } catch (error) {
      setGexSnapshotNotice(error instanceof Error ? error.message : "Failed to launch FlashAlpha GEX snapshot.");
    } finally {
      setIsLaunchingGexSnapshot(false);
    }
  };

  const handleRunMarketBreadth = async () => {
    setIsLaunchingMarketBreadth(true);
    setMarketBreadthNotice("");
    try {
      const today = new Date().toISOString().slice(0, 10);
      const response = await fetchJson<{ ok: boolean; job_id: string }>("/api/runs/market_breadth", {
        method: "POST",
        body: JSON.stringify({ date_label: today }),
      });
      setMarketBreadthNotice(`Market breadth analysis launched for ${today}: ${response.job_id}`);
      loadAudit();
    } catch (error) {
      setMarketBreadthNotice(error instanceof Error ? error.message : "Failed to launch market breadth analysis.");
    } finally {
      setIsLaunchingMarketBreadth(false);
    }
  };

  const handleRunUptrendAnalysis = async () => {
    setIsLaunchingUptrendAnalysis(true);
    setUptrendAnalysisNotice("");
    try {
      const today = new Date().toISOString().slice(0, 10);
      const response = await fetchJson<{ ok: boolean; job_id: string }>("/api/runs/uptrend_analysis", {
        method: "POST",
        body: JSON.stringify({ date_label: today }),
      });
      setUptrendAnalysisNotice(`Uptrend analysis launched for ${today}: ${response.job_id}`);
      loadAudit();
    } catch (error) {
      setUptrendAnalysisNotice(error instanceof Error ? error.message : "Failed to launch uptrend analysis.");
    } finally {
      setIsLaunchingUptrendAnalysis(false);
    }
  };

  const handleRunIbdMonitor = async () => {
    setIsLaunchingIbdMonitor(true);
    setIbdMonitorNotice("");
    try {
      const today = new Date().toISOString().slice(0, 10);
      const response = await fetchJson<{ ok: boolean; job_id: string }>("/api/runs/ibd_distribution_day_monitor", {
        method: "POST",
        body: JSON.stringify({ as_of_date: today }),
      });
      setIbdMonitorNotice(`IBD monitor launched for ${today}: ${response.job_id}`);
      loadAudit();
    } catch (error) {
      setIbdMonitorNotice(error instanceof Error ? error.message : "Failed to launch IBD monitor.");
    } finally {
      setIsLaunchingIbdMonitor(false);
    }
  };

  const handleRunExposureCoach = async () => {
    setIsLaunchingExposureCoach(true);
    setExposureCoachNotice("");
    try {
      const today = new Date().toISOString().slice(0, 10);
      const response = await fetchJson<{ ok: boolean; job_id: string }>("/api/runs/exposure_coach", {
        method: "POST",
        body: JSON.stringify({ date_label: today }),
      });
      setExposureCoachNotice(`Exposure Coach launched for ${today}: ${response.job_id}`);
      loadAudit();
    } catch (error) {
      setExposureCoachNotice(error instanceof Error ? error.message : "Failed to launch Exposure Coach.");
    } finally {
      setIsLaunchingExposureCoach(false);
    }
  };

  const handleRestartWebApp = async () => {
    setIsRestartingWebApp(true);
    setWebRestartNotice("");
    try {
      const response = await fetchJson<{ ok: boolean; message: string; delay_seconds: number; restart_mode: string }>("/api/admin/web-restart", {
        method: "POST",
        body: JSON.stringify({ delay_seconds: 1.0 }),
      });
      setWebRestartNotice(response.message);
      loadAudit();
    } catch (error) {
      setWebRestartNotice(error instanceof Error ? error.message : "Failed to request web app restart.");
    } finally {
      setIsRestartingWebApp(false);
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
      loadAudit();
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

  const filteredMissingSectorTickers = useMemo(() => {
    const query = missingSectorFilter.trim().toLowerCase();
    if (!query) {
      return missingSectorPayload.tickers;
    }
    return missingSectorPayload.tickers.filter((entry) =>
      [entry.ticker, entry.exchange, entry.industry, entry.source, entry.suggested_sector, entry.suggested_industry]
        .join(" ")
        .toLowerCase()
        .includes(query),
    );
  }, [missingSectorFilter, missingSectorPayload.tickers]);

  const filteredMissingFinvizTickers = useMemo(() => {
    const query = missingFinvizFilter.trim().toLowerCase();
    if (!query) {
      return missingFinvizPayload.tickers;
    }
    return missingFinvizPayload.tickers.filter((entry) =>
      [entry.ticker, entry.source, entry.reason, entry.first_seen_at, entry.last_seen_at].join(" ").toLowerCase().includes(query),
    );
  }, [missingFinvizFilter, missingFinvizPayload.tickers]);

  const db = payload.database_status;
  const refreshUsers = () => {
    void fetchJson<{ users: Array<{ id: number; email: string; role: RoleName; is_active: boolean; created_at?: string | null; updated_at?: string | null; last_login_at?: string | null }>; access_requests?: AccessRequestSummary[] }>("/api/admin/users")
      .then((result) => {
        setUsers(result.users);
        setAccessRequests(result.access_requests ?? []);
      })
      .catch(() => {
        setUsers([]);
        setAccessRequests([]);
      });
  };

  const handleInviteUser = async (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    setIsSavingUser(true);
    setNotice("");
    try {
      await fetchJson<{ ok: boolean }>("/api/admin/users/invite", {
        method: "POST",
        body: JSON.stringify({ email: inviteEmail, role: inviteRole }),
      });
      setInviteEmail("");
      setInviteRole("visitor");
      setNotice("User saved. They can now sign in with Google using that email.");
      refreshUsers();
      loadAudit();
    } catch (error) {
      setNotice(error instanceof Error ? error.message : "Failed to save user.");
    } finally {
      setIsSavingUser(false);
    }
  };

  const handleRoleChange = async (userId: number, role: RoleName) => {
    setIsSavingUser(true);
    setNotice("");
    try {
      await fetchJson<{ ok: boolean }>(`/api/admin/users/${userId}/role`, {
        method: "POST",
        body: JSON.stringify({ role }),
      });
      setNotice("Role updated.");
      refreshUsers();
      loadAudit();
    } catch (error) {
      setNotice(error instanceof Error ? error.message : "Failed to update role.");
    } finally {
      setIsSavingUser(false);
    }
  };

  const handleToggleUser = async (userId: number, isActive: boolean) => {
    setIsSavingUser(true);
    setNotice("");
    try {
      await fetchJson<{ ok: boolean }>(`/api/admin/users/${userId}/${isActive ? "deactivate" : "reactivate"}`, {
        method: "POST",
      });
      setNotice(isActive ? "User deactivated." : "User reactivated.");
      refreshUsers();
      loadAudit();
    } catch (error) {
      setNotice(error instanceof Error ? error.message : "Failed to update user.");
    } finally {
      setIsSavingUser(false);
    }
  };

  const handleApproveAccessRequest = async (requestId: number) => {
    setIsSavingUser(true);
    setNotice("");
    try {
      await fetchJson<{ ok: boolean }>(`/api/admin/access-requests/${requestId}/approve`, {
        method: "POST",
      });
      setNotice("Access request approved. User can now sign in with Google using that email.");
      refreshUsers();
      loadAudit();
    } catch (error) {
      setNotice(error instanceof Error ? error.message : "Failed to approve access request.");
    } finally {
      setIsSavingUser(false);
    }
  };

  const handleDenyAccessRequest = async (requestId: number) => {
    setIsSavingUser(true);
    setNotice("");
    try {
      await fetchJson<{ ok: boolean }>(`/api/admin/access-requests/${requestId}/deny`, {
        method: "POST",
        body: JSON.stringify({ deny_reason: "" }),
      });
      setNotice("Access request denied.");
      refreshUsers();
      loadAudit();
    } catch (error) {
      setNotice(error instanceof Error ? error.message : "Failed to deny access request.");
    } finally {
      setIsSavingUser(false);
    }
  };

  const handleAuditFilterSubmit = (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    loadAudit();
  };

  const handleAssignSector = async (ticker: string) => {
    const sector = (sectorSelections[ticker] || "").trim();
    if (!sector) {
      setSectorNotice("Select a sector before saving.");
      return;
    }
    setIsSavingSector(true);
    setSectorNotice("");
    try {
      await fetchJson<{ ok: boolean; entry: { ticker: string; sector: string } }>(`/api/admin/ticker-sectors/${ticker}`, {
        method: "POST",
        body: JSON.stringify({ sector }),
      });
      setSectorNotice(`${ticker} sector set to ${sector}.`);
      loadMissingSectors();
      loadAudit();
    } catch (error) {
      setSectorNotice(error instanceof Error ? error.message : "Failed to update sector.");
    } finally {
      setIsSavingSector(false);
    }
  };

  const handleRemoveMissingFinvizTicker = async (ticker: string) => {
    setIsRemovingMissingFinviz(true);
    setMissingFinvizNotice("");
    try {
      await fetchJson<{ ok: boolean; entry: { ticker: string } }>(`/api/admin/finviz-missing-tickers/${ticker}/remove`, {
        method: "POST",
      });
      setMissingFinvizNotice(`${ticker} removed from Finviz 404 skip list.`);
      loadMissingFinvizTickers();
      loadAudit();
    } catch (error) {
      setMissingFinvizNotice(error instanceof Error ? error.message : "Failed to remove Finviz missing ticker.");
    } finally {
      setIsRemovingMissingFinviz(false);
    }
  };

  return (
    <div className="page-grid">
      <AdminSubnav
        title="Operations Console"
        description="Manage exclusions, users, notification plumbing, and admin maintenance from one overview page."
      />

      <Panel title="Missing Sector Assignments" aside={<span className="eyebrow">{formatCount(missingSectorPayload.missing_count)} tickers</span>}>
        {isLoadingMissingSectors ? <LoadingBlock label="Loading missing-sector tickers…" /> : null}
        <div className="run-toolbar">
          <div className="run-action-footer">
            <label className="field" style={{ flex: "1 1 20rem" }}>
              <span>Filter tickers</span>
              <input
                type="text"
                value={missingSectorFilter}
                onChange={(event) => setMissingSectorFilter(event.target.value)}
                placeholder="Ticker, exchange, industry, source"
              />
            </label>
            <button className="ghost-button" type="button" onClick={loadMissingSectors} disabled={isLoadingMissingSectors || isSavingSector}>
              {isLoadingMissingSectors ? "Refreshing..." : "Refresh Missing Sectors"}
            </button>
          </div>

          {missingSectorPayload.notes.length > 0 ? <div className="panel-copy">{missingSectorPayload.notes.join(" ")}</div> : null}
          {sectorNotice ? <div className="panel-copy">{sectorNotice}</div> : null}

          <div className="pill-list">
            {missingSectorPayload.available_sectors.map((sector) => (
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
                  <th>Assign Sector</th>
                </tr>
              </thead>
              <tbody>
                {filteredMissingSectorTickers.length === 0 ? (
                  <tr>
                    <td colSpan={6}>
                      {isLoadingMissingSectors ? "Loading missing-sector tickers..." : "No tickers missing sector."}
                    </td>
                  </tr>
                ) : (
                  filteredMissingSectorTickers.map((item) => (
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
                      <td data-label="Assign Sector">
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
                            {missingSectorPayload.available_sectors.map((sector) => (
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

      <Panel title="Finviz Missing Tickers" aside={<span className="eyebrow">{formatCount(missingFinvizPayload.missing_count)} skipped</span>}>
        {isLoadingMissingFinviz ? <LoadingBlock label="Loading Finviz missing-ticker registry…" /> : null}
        <div className="run-toolbar">
          <div className="run-action-footer">
            <label className="field" style={{ flex: "1 1 20rem" }}>
              <span>Filter tickers</span>
              <input
                type="text"
                value={missingFinvizFilter}
                onChange={(event) => setMissingFinvizFilter(event.target.value)}
                placeholder="Ticker, source, reason"
              />
            </label>
            <button className="ghost-button" type="button" onClick={loadMissingFinvizTickers} disabled={isLoadingMissingFinviz || isRemovingMissingFinviz}>
              {isLoadingMissingFinviz ? "Refreshing..." : "Refresh Registry"}
            </button>
          </div>

          {missingFinvizPayload.notes.length > 0 ? <div className="panel-copy">{missingFinvizPayload.notes.join(" ")}</div> : null}
          {missingFinvizNotice ? <div className="panel-copy">{missingFinvizNotice}</div> : null}

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
                  <th>Action</th>
                </tr>
              </thead>
              <tbody>
                {filteredMissingFinvizTickers.length === 0 ? (
                  <tr>
                    <td colSpan={7}>
                      {isLoadingMissingFinviz ? "Loading Finviz missing tickers..." : "No Finviz missing tickers recorded."}
                    </td>
                  </tr>
                ) : (
                  filteredMissingFinvizTickers.map((item) => (
                    <tr key={item.ticker}>
                      <td data-label="Ticker">
                        <strong>{item.ticker}</strong>
                      </td>
                      <td data-label="Source">{item.source || "-"}</td>
                      <td data-label="Hit Count">{formatCount(item.hit_count)}</td>
                      <td data-label="First Seen">{formatLocalDateTime(item.first_seen_at)}</td>
                      <td data-label="Last Seen">{formatLocalDateTime(item.last_seen_at)}</td>
                      <td data-label="Reason" className="file-meta">{item.reason || "-"}</td>
                      <td data-label="Action">
                        <button
                          className="table-action-button"
                          type="button"
                          disabled={isRemovingMissingFinviz}
                          onClick={() => void handleRemoveMissingFinvizTicker(item.ticker)}
                        >
                          {isRemovingMissingFinviz ? "Removing..." : "Allow Retry"}
                        </button>
                      </td>
                    </tr>
                  ))
                )}
              </tbody>
            </table>
          </div>
        </div>
      </Panel>

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

      <Panel title="Fetch Today SPX GEX" aside={<span className="eyebrow">CBOE delayed close snapshot</span>}>
        <div className="run-toolbar">
          <p className="panel-copy">
            Launches `flashalpha_gex_close` for today and persists a CBOE-delayed snapshot for dashboard DB reads. Default expiry is 0DTE when present, otherwise the nearest expiry.
          </p>
          <div className="run-action-footer">
            <button className="primary-button" type="button" disabled={isLaunchingGexSnapshot} onClick={() => void handleFetchTodayGexSnapshot()}>
              {isLaunchingGexSnapshot ? "Launching..." : "Fetch Now"}
            </button>
            <span className="panel-copy">Uses default symbol `SPX` and today date label.</span>
          </div>
          {gexSnapshotNotice ? <div className="panel-copy">{gexSnapshotNotice}</div> : null}
        </div>
      </Panel>

      <Panel title="Run Market Breadth" aside={<span className="eyebrow">TraderMonty CSV cache refresh</span>}>
        <div className="run-toolbar">
          <p className="panel-copy">
            Launches `market_breadth` for today and persists the latest breadth JSON artifact used by the dashboard breadth card.
          </p>
          <div className="run-action-footer">
            <button className="primary-button" type="button" disabled={isLaunchingMarketBreadth} onClick={() => void handleRunMarketBreadth()}>
              {isLaunchingMarketBreadth ? "Launching..." : "Run Now"}
            </button>
            <span className="panel-copy">Also available under the Runs / Screeners page and schedulable from the scheduler.</span>
          </div>
          {marketBreadthNotice ? <div className="panel-copy">{marketBreadthNotice}</div> : null}
        </div>
      </Panel>

      <Panel title="Run Uptrend Analyzer" aside={<span className="eyebrow">Monty uptrend ratio cache refresh</span>}>
        <div className="run-toolbar">
          <p className="panel-copy">
            Launches `uptrend_analysis` for today and persists the latest uptrend JSON artifact used by the dashboard uptrend card.
          </p>
          <div className="run-action-footer">
            <button className="primary-button" type="button" disabled={isLaunchingUptrendAnalysis} onClick={() => void handleRunUptrendAnalysis()}>
              {isLaunchingUptrendAnalysis ? "Launching..." : "Run Now"}
            </button>
            <span className="panel-copy">Also available under Runs / Screeners and schedulable from scheduler.</span>
          </div>
          {uptrendAnalysisNotice ? <div className="panel-copy">{uptrendAnalysisNotice}</div> : null}
        </div>
      </Panel>

      <Panel title="Run IBD Distribution Day Monitor" aside={<span className="eyebrow">QQQ / SPY deterioration check</span>}>
        <div className="run-toolbar">
          <p className="panel-copy">
            Launches `ibd_distribution_day_monitor` for today and persists the latest risk/action artifact used by the dashboard IBD card.
          </p>
          <div className="run-action-footer">
            <button className="primary-button" type="button" disabled={isLaunchingIbdMonitor} onClick={() => void handleRunIbdMonitor()}>
              {isLaunchingIbdMonitor ? "Launching..." : "Run Now"}
            </button>
            <span className="panel-copy">Uses default QQQ/SPY config and current TQQQ policy settings unless CLI overrides are added later.</span>
          </div>
          {ibdMonitorNotice ? <div className="panel-copy">{ibdMonitorNotice}</div> : null}
        </div>
      </Panel>

      <Panel title="Run Exposure Coach" aside={<span className="eyebrow">Market posture summary</span>}>
        <div className="run-toolbar">
          <p className="panel-copy">
            Launches `exposure_coach` using the latest cached breadth, uptrend, and IBD-derived top-risk proxy to persist a market posture artifact for the dashboard.
          </p>
          <div className="run-action-footer">
            <button className="primary-button" type="button" disabled={isLaunchingExposureCoach} onClick={() => void handleRunExposureCoach()}>
              {isLaunchingExposureCoach ? "Launching..." : "Run Now"}
            </button>
            <span className="panel-copy">Partial inputs are allowed; confidence drops when upstream artifacts are missing.</span>
          </div>
          {exposureCoachNotice ? <div className="panel-copy">{exposureCoachNotice}</div> : null}
        </div>
      </Panel>

      <Panel title="Restart Web App" aside={<span className="eyebrow">Admin-only repair action</span>}>
        <div className="run-toolbar">
          <p className="panel-copy">
            Requests the current web process to exit so Docker can restart the `web` container. Use this after config or dependency changes that need a fresh app process.
          </p>
          <div className="run-action-footer">
            <button className="primary-button" type="button" disabled={isRestartingWebApp} onClick={() => void handleRestartWebApp()}>
              {isRestartingWebApp ? "Requesting..." : "Restart Web App"}
            </button>
            <span className="panel-copy">Expected behavior on deploy host: brief downtime while Docker restarts the `web` service.</span>
          </div>
          {webRestartNotice ? <div className="panel-copy">{webRestartNotice}</div> : null}
        </div>
      </Panel>

      <Panel title="SPX Gamma Exposure Plot" aside={<span className="eyebrow">All-expiry CBOE profile</span>}>
        <div className="run-toolbar">
          <div className="run-action-footer">
            <button className="primary-button" type="button" disabled={isLoadingGammaPlot} onClick={() => loadGammaExposurePlot()}>
              {isLoadingGammaPlot ? "Loading..." : "Refresh Plot"}
            </button>
            <span className="panel-copy">Loads live CBOE-delayed SPX chain using the `_SPX` symbol alias and renders the SpotGamma-style profile.</span>
          </div>
          {gammaPlotNotice ? <div className="panel-copy">{gammaPlotNotice}</div> : null}
          {isLoadingGammaPlot ? <LoadingBlock label="Building SPX gamma profile..." /> : null}
          {!isLoadingGammaPlot && gammaPlotPayload ? (
            <>
              <p className="panel-copy">{gammaPlotPayload.summary}</p>
              <div className="gex-admin-metrics">
                <div className="gex-admin-metric">
                  <span className="gex-admin-label">As Of</span>
                  <strong className="gex-admin-value">{formatLocalDateTime(gammaPlotPayload.as_of)}</strong>
                </div>
                <div className="gex-admin-metric">
                  <span className="gex-admin-label">Spot</span>
                  <strong className="gex-admin-value">{formatDecimal(gammaPlotPayload.underlying_price)}</strong>
                </div>
                <div className="gex-admin-metric">
                  <span className="gex-admin-label">Net GEX</span>
                  <strong className="gex-admin-value">{formatBillions(gammaPlotPayload.net_gex)}</strong>
                </div>
                <div className="gex-admin-metric">
                  <span className="gex-admin-label">Call GEX</span>
                  <strong className="gex-admin-value">{formatBillions(gammaPlotPayload.call_gex_total)}</strong>
                </div>
                <div className="gex-admin-metric">
                  <span className="gex-admin-label">Put GEX</span>
                  <strong className="gex-admin-value">{formatBillions(gammaPlotPayload.put_gex_total)}</strong>
                </div>
                <div className="gex-admin-metric">
                  <span className="gex-admin-label">Gamma Flip</span>
                  <strong className="gex-admin-value">{formatDecimal(gammaPlotPayload.gamma_flip)}</strong>
                </div>
                <div className="gex-admin-metric">
                  <span className="gex-admin-label">Call Wall</span>
                  <strong className="gex-admin-value">{formatDecimal(gammaPlotPayload.call_wall)}</strong>
                </div>
                <div className="gex-admin-metric">
                  <span className="gex-admin-label">Put Wall</span>
                  <strong className="gex-admin-value">{formatDecimal(gammaPlotPayload.put_wall)}</strong>
                </div>
                <div className="gex-admin-metric">
                  <span className="gex-admin-label">Next Expiry</span>
                  <strong className="gex-admin-value">{gammaPlotPayload.next_expiry || "--"}</strong>
                </div>
                <div className="gex-admin-metric">
                  <span className="gex-admin-label">Next Monthly</span>
                  <strong className="gex-admin-value">{gammaPlotPayload.next_monthly_expiry || "--"}</strong>
                </div>
              </div>
              <p className="panel-copy">{gammaPlotPayload.methodology}</p>
              <div className="gex-admin-plot-grid">
                <div className="gex-admin-plot" dangerouslySetInnerHTML={{ __html: gammaPlotPayload.plots.absolute }} />
                <div className="gex-admin-plot" dangerouslySetInnerHTML={{ __html: gammaPlotPayload.plots.by_option_type }} />
                <div className="gex-admin-plot gex-admin-plot-wide" dangerouslySetInnerHTML={{ __html: gammaPlotPayload.plots.profile }} />
              </div>
            </>
          ) : null}
        </div>
      </Panel>

      <Panel title="Users and Roles" aside={<span className="eyebrow">{users.length} accounts</span>}>
        <form className="run-toolbar" onSubmit={(event) => void handleInviteUser(event)}>
          <div className="run-params-grid">
            <label className="field">
              <span>Email</span>
              <input type="email" value={inviteEmail} onChange={(event) => setInviteEmail(event.target.value)} placeholder="user@example.com" />
            </label>
            <label className="field">
              <span>Role</span>
              <select value={inviteRole} onChange={(event) => setInviteRole(event.target.value as RoleName)}>
                <option value="visitor">visitor</option>
                <option value="premium">premium</option>
                <option value="admin">admin</option>
              </select>
            </label>
          </div>
          <div className="run-action-footer">
            <button className="primary-button" type="submit" disabled={isSavingUser}>
              {isSavingUser ? "Saving..." : "Add or Update User"}
            </button>
            <span className="panel-copy">Use Google-account email here. Added users can sign in immediately with same Google email.</span>
          </div>
        </form>

        <div className="data-table-responsive">
          <table className="data-table">
            <thead>
              <tr>
                <th>Email</th>
                <th>Role</th>
                <th>Status</th>
                <th>Last Login</th>
                <th>Actions</th>
              </tr>
            </thead>
            <tbody>
              {users.map((user) => (
                <tr key={user.id}>
                  <td>{user.email}</td>
                  <td>
                    <select value={user.role} onChange={(event) => void handleRoleChange(user.id, event.target.value as RoleName)} disabled={isSavingUser}>
                      <option value="visitor">visitor</option>
                      <option value="premium">premium</option>
                      <option value="admin">admin</option>
                    </select>
                  </td>
                  <td>{user.is_active ? "active" : "inactive"}</td>
                  <td>{formatLocalDateTime(user.last_login_at)}</td>
                  <td>
                    <button className="table-action-button" type="button" disabled={isSavingUser} onClick={() => void handleToggleUser(user.id, user.is_active)}>
                      {user.is_active ? "Deactivate" : "Reactivate"}
                    </button>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </Panel>

      <Panel title="Premium Access Requests" aside={<span className="eyebrow">{accessRequests.filter((item) => item.status === "pending").length} pending</span>}>
        <div className="data-table-responsive">
          <table className="data-table">
            <thead>
              <tr>
                <th>Email</th>
                <th>Requested Role</th>
                <th>Status</th>
                <th>Requested</th>
                <th>Reviewed</th>
                <th>Actions</th>
              </tr>
            </thead>
            <tbody>
              {accessRequests.length === 0 ? (
                <tr>
                  <td colSpan={6}>No access requests yet.</td>
                </tr>
              ) : (
                accessRequests.map((item) => (
                  <tr key={item.id}>
                    <td>{item.email}</td>
                    <td>{item.requested_role}</td>
                    <td>{item.status}</td>
                    <td>{formatLocalDateTime(item.requested_at)}</td>
                    <td>{item.reviewed_at ? `${formatLocalDateTime(item.reviewed_at)}${item.reviewed_by_email ? ` · ${item.reviewed_by_email}` : ""}` : "-"}</td>
                    <td>
                      {item.status === "pending" ? (
                        <div className="button-row">
                          <button className="table-action-button" type="button" disabled={isSavingUser} onClick={() => void handleApproveAccessRequest(item.id)}>
                            Approve
                          </button>
                          <button className="table-action-button" type="button" disabled={isSavingUser} onClick={() => void handleDenyAccessRequest(item.id)}>
                            Deny
                          </button>
                        </div>
                      ) : (
                        <span className="file-meta">{item.status === "approved" ? `Granted${item.invited_user_email ? ` · ${item.invited_user_email}` : ""}` : item.deny_reason || "Closed"}</span>
                      )}
                    </td>
                  </tr>
                ))
              )}
            </tbody>
          </table>
        </div>
      </Panel>

      <Panel title="Audit Trail" aside={<span className="eyebrow">{auditEvents.length} events</span>}>
        <form className="run-toolbar" onSubmit={handleAuditFilterSubmit}>
          <div className="run-params-grid">
            <label className="field">
              <span>Actor Email</span>
              <input type="text" value={auditActorEmail} onChange={(event) => setAuditActorEmail(event.target.value)} placeholder="admin@example.com" />
            </label>
            <label className="field">
              <span>Action</span>
              <input type="text" value={auditAction} onChange={(event) => setAuditAction(event.target.value)} placeholder="admin.user.invite" />
            </label>
            <label className="field">
              <span>Resource Type</span>
              <input type="text" value={auditResourceType} onChange={(event) => setAuditResourceType(event.target.value)} placeholder="user" />
            </label>
            <label className="field">
              <span>From</span>
              <input type="date" value={auditFromDate} onChange={(event) => setAuditFromDate(event.target.value)} />
            </label>
            <label className="field">
              <span>To</span>
              <input type="date" value={auditToDate} onChange={(event) => setAuditToDate(event.target.value)} />
            </label>
          </div>
          <div className="run-action-footer">
            <button className="primary-button" type="submit" disabled={isLoadingAudit}>
              {isLoadingAudit ? "Loading..." : "Refresh Audit"}
            </button>
          </div>
        </form>
        <div className="data-table-responsive">
          <table className="data-table">
            <thead>
              <tr>
                <th>When</th>
                <th>Actor</th>
                <th>Action</th>
                <th>Target</th>
                <th>Message</th>
              </tr>
            </thead>
            <tbody>
              {auditEvents.length === 0 ? (
                <tr>
                  <td colSpan={5}>{isLoadingAudit ? "Loading audit events..." : "No audit events found."}</td>
                </tr>
              ) : (
                auditEvents.map((item) => (
                  <tr key={item.id}>
                    <td>{formatLocalDateTime(item.event_at)}</td>
                    <td>{item.actor_email || item.actor_role || "-"}</td>
                    <td>{item.action}</td>
                    <td>{item.resource_label || item.resource_id || item.resource_type}</td>
                    <td>{item.message}</td>
                  </tr>
                ))
              )}
            </tbody>
          </table>
        </div>
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

function formatDecimal(value: number | null | undefined): string {
  if (value == null || Number.isNaN(value)) {
    return "--";
  }
  return value.toLocaleString(undefined, { maximumFractionDigits: 2, minimumFractionDigits: 0 });
}

function formatBillions(value: number | null | undefined): string {
  if (value == null || Number.isNaN(value)) {
    return "--";
  }
  return `${(value / 1_000_000_000).toLocaleString(undefined, { maximumFractionDigits: 2, minimumFractionDigits: 2 })}B`;
}
