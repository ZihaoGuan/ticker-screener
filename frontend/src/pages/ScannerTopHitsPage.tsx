import { useEffect, useMemo, useState } from "react";
import { Link } from "react-router-dom";
import { useAuth } from "../auth/AuthContext";
import { LoadingBlock } from "../components/LoadingBlock";
import { PaginationControls } from "../components/PaginationControls";
import { ScannerMiniChart } from "../components/ScannerMiniChart";
import { fetchJson } from "../lib/api";
import { formatCount, formatLocalDate, formatLocalDateTime } from "../lib/format";
import { resolveRsMomentumSignal } from "../lib/rsMomentum";
import type { CandlePoint, MyPicksContextResponse, ScannerTopHitRow, ScannerTopHitsResponse, TechnicalIndicatorRatingCell, WatchlistChartResponse } from "../lib/types";

type SortKey = "hits" | "ticker" | "sector" | "sectorTopHit" | "industryTopHit" | "close" | "change" | "from52wLow" | "bollinger" | "rsEvidence" | "rsDays" | "rsPhaseDays" | "upOnDownDays" | "rs" | "dailyRs" | "rs3m" | "rs6m" | "rsMomentum" | "ta" | "fa" | "decision" | "decisionScore";
type SortDirection = "asc" | "desc";
type ViewMode = "list" | "charts";
const LIST_PAGE_SIZE = 50;
const CHART_PAGE_SIZE = 9;
const LEADERSHIP_SCANNER_IDS = new Set(["trend_template", "sean_breakout", "venu_scanner"]);

export function ScannerTopHitsPage() {
  const auth = useAuth();
  const [payload, setPayload] = useState<ScannerTopHitsResponse | null>(null);
  const [isLoading, setIsLoading] = useState(true);
  const [notice, setNotice] = useState("");
  const [myPicksNotice, setMyPicksNotice] = useState("");
  const [search, setSearch] = useState("");
  const [sectorFilter, setSectorFilter] = useState("all");
  const [eliteOnly, setEliteOnly] = useState(false);
  const [hasLeadershipScannerOnly, setHasLeadershipScannerOnly] = useState(false);
  const [hasFundamentalQualityOnly, setHasFundamentalQualityOnly] = useState(false);
  const [leaderRsOnly, setLeaderRsOnly] = useState(false);
  const [leaderRsMin, setLeaderRsMin] = useState("90");
  const [leaderRsMax, setLeaderRsMax] = useState("");
  const [rsEvidenceOnly, setRsEvidenceOnly] = useState(false);
  const [rsEvidenceMin, setRsEvidenceMin] = useState("5");
  const [rsDaysMinPct, setRsDaysMinPct] = useState("60");
  const [upOnDownDaysMin, setUpOnDownDaysMin] = useState("3");
  const [selectedScannerIds, setSelectedScannerIds] = useState<string[]>([]);
  const [sortBy, setSortBy] = useState<SortKey>("hits");
  const [sortDirection, setSortDirection] = useState<SortDirection>("desc");
  const [viewMode, setViewMode] = useState<ViewMode>("list");
  const [currentPage, setCurrentPage] = useState(1);
  const [myPickTickers, setMyPickTickers] = useState<Set<string>>(new Set());
  const [savingMyPickTickers, setSavingMyPickTickers] = useState<Record<string, boolean>>({});
  const [chartPayloads, setChartPayloads] = useState<Record<string, WatchlistChartResponse | null | undefined>>({});
  const [chartErrors, setChartErrors] = useState<Record<string, string>>({});
  const [chartLoadingTickers, setChartLoadingTickers] = useState<Record<string, boolean>>({});
  const canManageMyPicks = auth.hasCapability("manage_exclusions");

  useEffect(() => {
    setIsLoading(true);
    setNotice("");
    void fetchJson<ScannerTopHitsResponse>("/api/scanner-board/top-hits")
      .then(setPayload)
      .catch((error) => {
        setPayload(null);
        setNotice(error instanceof Error ? error.message : "Failed to load scanner top hits.");
      })
      .finally(() => setIsLoading(false));
  }, []);

  useEffect(() => {
    if (!canManageMyPicks) {
      setMyPickTickers(new Set());
      return;
    }
    void fetchJson<MyPicksContextResponse>("/api/admin/my-picks")
      .then((response) => {
        setMyPickTickers(new Set(response.rows.map((row) => row.ticker.toUpperCase())));
      })
      .catch(() => {
        setMyPickTickers(new Set());
      });
  }, [canManageMyPicks]);

  const rows = payload?.rows ?? [];
  const sectors = useMemo(
    () => Array.from(new Set(rows.map((row) => row.sector).filter((sector) => sector && sector !== "Unknown sector"))).sort(),
    [rows],
  );
  const scannerOptions = useMemo(() => {
    const options = new Map<string, string>();
    for (const row of rows) {
      for (const scanner of row.scanners) {
        const normalizedId = normalizeScannerId(scanner.id);
        const label = String(scanner.label || "").trim();
        if (normalizedId && label && !options.has(normalizedId)) {
          options.set(normalizedId, label);
        }
      }
    }
    return Array.from(options.entries())
      .map(([id, label]) => ({ id, label }))
      .sort((left, right) => left.label.localeCompare(right.label));
  }, [rows]);
  const normalizedLeaderRsRange = useMemo(() => normalizeRsRatingRange(leaderRsMin, leaderRsMax), [leaderRsMin, leaderRsMax]);
  const normalizedRsEvidenceMin = useMemo(() => normalizeBoundedInteger(rsEvidenceMin, 0, 9, 5), [rsEvidenceMin]);
  const normalizedRsDaysMinPct = useMemo(() => normalizeBoundedInteger(rsDaysMinPct, 0, 100, 60), [rsDaysMinPct]);
  const normalizedUpOnDownDaysMin = useMemo(() => normalizeBoundedInteger(upOnDownDaysMin, 0, 21, 3), [upOnDownDaysMin]);

  const filteredRows = useMemo(() => {
    const query = search.trim().toLowerCase();
    let nextRows = rows;
    if (sectorFilter !== "all") {
      nextRows = nextRows.filter((row) => row.sector === sectorFilter);
    }
    if (query) {
      nextRows = nextRows.filter((row) =>
        [row.ticker, row.company, row.sector, row.industry, row.scanner_labels.join(" ")].join(" ").toLowerCase().includes(query),
      );
    }
    if (eliteOnly) {
      nextRows = nextRows.filter(isElitePick);
    }
    if (hasLeadershipScannerOnly) {
      nextRows = nextRows.filter(hasLeadershipScannerSignal);
    }
    if (hasFundamentalQualityOnly) {
      nextRows = nextRows.filter(hasFundamentalQualitySignal);
    }
    if (leaderRsOnly) {
      nextRows = nextRows.filter((row) => hasDailyRsRatingInRange(row, normalizedLeaderRsRange.min, normalizedLeaderRsRange.max));
    }
    if (rsEvidenceOnly) {
      nextRows = nextRows.filter((row) =>
        hasRsEvidenceProfile(row, {
          minScore: normalizedRsEvidenceMin,
          minRsDaysPct: normalizedRsDaysMinPct,
          minUpOnDownDays: normalizedUpOnDownDaysMin,
        }),
      );
    }
    if (selectedScannerIds.length > 0) {
      nextRows = nextRows.filter((row) => hasSelectedScannerSignals(row, selectedScannerIds));
    }
    return [...nextRows].sort((left, right) => compareRows(left, right, sortBy, sortDirection, {
      sectorLeaders: eliteOnly ? buildEliteLeaderMap(nextRows, (item) => normalizeSectorKey(item.sector)) : new Map<string, string>(),
      industryLeaders: eliteOnly ? buildEliteLeaderMap(nextRows, (item) => normalizeIndustryKey(item.industry)) : new Map<string, string>(),
    }));
  }, [eliteOnly, hasFundamentalQualityOnly, hasLeadershipScannerOnly, leaderRsOnly, normalizedLeaderRsRange, normalizedRsDaysMinPct, normalizedRsEvidenceMin, normalizedUpOnDownDaysMin, rows, rsEvidenceOnly, search, sectorFilter, selectedScannerIds, sortBy, sortDirection]);

  useEffect(() => {
    setCurrentPage(1);
  }, [eliteOnly, hasFundamentalQualityOnly, hasLeadershipScannerOnly, leaderRsOnly, leaderRsMax, leaderRsMin, rsDaysMinPct, rsEvidenceMin, rsEvidenceOnly, search, sectorFilter, selectedScannerIds, sortBy, sortDirection, upOnDownDaysMin, viewMode]);

  const pageSize = viewMode === "charts" ? CHART_PAGE_SIZE : LIST_PAGE_SIZE;
  const totalPages = Math.max(1, Math.ceil(filteredRows.length / pageSize));
  const normalizedPage = Math.min(currentPage, totalPages);
  const pagedRows = useMemo(() => {
    const startIndex = (normalizedPage - 1) * pageSize;
    return filteredRows.slice(startIndex, startIndex + pageSize);
  }, [filteredRows, normalizedPage, pageSize]);
  const pagedTickerKey = useMemo(() => pagedRows.map((row) => row.ticker).join("|"), [pagedRows]);

  const eliteIndustryLeaders = useMemo(() => {
    if (!eliteOnly) {
      return new Map<string, string>();
    }
    return buildEliteLeaderMap(filteredRows, (row) => normalizeIndustryKey(row.industry));
  }, [eliteOnly, filteredRows]);

  const eliteSectorLeaders = useMemo(() => {
    if (!eliteOnly) {
      return new Map<string, string>();
    }
    return buildEliteLeaderMap(filteredRows, (row) => normalizeSectorKey(row.sector));
  }, [eliteOnly, filteredRows]);

  useEffect(() => {
    if (currentPage !== normalizedPage) {
      setCurrentPage(normalizedPage);
    }
  }, [currentPage, normalizedPage]);

  useEffect(() => {
    if (viewMode !== "charts" || pagedRows.length === 0) {
      return;
    }
    const missingTickers = pagedRows
      .map((row) => row.ticker)
      .filter((ticker) => chartPayloads[ticker] === undefined && !chartLoadingTickers[ticker]);
    if (missingTickers.length === 0) {
      return;
    }
    let ignore = false;
    setChartLoadingTickers((current) => {
      const next = { ...current };
      for (const ticker of missingTickers) {
        next[ticker] = true;
      }
      return next;
    });
    for (const ticker of missingTickers) {
      void fetchJson<WatchlistChartResponse>(`/api/charts/${encodeURIComponent(ticker)}/preview?period=18mo`)
        .then((payload) => {
          if (ignore) {
            return;
          }
          setChartPayloads((current) => ({ ...current, [ticker]: payload }));
          setChartErrors((current) => {
            const next = { ...current };
            delete next[ticker];
            return next;
          });
        })
        .catch((error) => {
          if (ignore) {
            return;
          }
          setChartPayloads((current) => ({ ...current, [ticker]: null }));
          setChartErrors((current) => ({
            ...current,
            [ticker]: error instanceof Error ? error.message : "Failed to load chart.",
          }));
        })
        .finally(() => {
          if (ignore) {
            return;
          }
          setChartLoadingTickers((current) => {
            const next = { ...current };
            delete next[ticker];
            return next;
          });
        });
    }
    return () => {
      ignore = true;
    };
  }, [pagedRows, pagedTickerKey, viewMode]);

  const handleAddToMyPicks = async (ticker: string) => {
    const normalizedTicker = ticker.trim().toUpperCase();
    if (!normalizedTicker || myPickTickers.has(normalizedTicker) || savingMyPickTickers[normalizedTicker]) {
      return;
    }
    setSavingMyPickTickers((current) => ({ ...current, [normalizedTicker]: true }));
    setMyPicksNotice("");
    try {
      await fetchJson<{ ok: boolean; pick: { ticker: string } }>("/api/admin/my-picks", {
        method: "POST",
        body: JSON.stringify({
          ticker: normalizedTicker,
          notes: "Added from scanner top hits.",
        }),
      });
      setMyPickTickers((current) => new Set([...current, normalizedTicker]));
      setMyPicksNotice(`${normalizedTicker} added to My Picks.`);
    } catch (error) {
      setMyPicksNotice(error instanceof Error ? error.message : "Failed to add ticker to My Picks.");
    } finally {
      setSavingMyPickTickers((current) => {
        const next = { ...current };
        delete next[normalizedTicker];
        return next;
      });
    }
  };

  return (
    <div className="page-grid scanner-top-hits-page">
      <section className="scanner-result-hero panel">
        <div className="scanner-result-breadcrumbs">
          <Link to="/">Dashboard</Link>
          <span>›</span>
          <Link to="/scanner">Stock Scanner</Link>
          <span>›</span>
          <span>Top Hits</span>
        </div>
        <div className="scanner-result-title-row">
          <div>
            <span className="scanner-result-kicker">Overlap Radar</span>
            <h1>Scanner top hits</h1>
          </div>
          <span className={`scanner-result-status${rows.length > 0 ? " is-live" : ""}`}>{rows.length > 0 ? "Overlap Found" : "No Overlap"}</span>
        </div>
        <p className="scanner-result-copy">Top hit = same ticker flagged by multiple live daily scanner boards. Sector momentum uses weekly sector RRG snapshot.</p>
        <div className="scanner-result-metrics">
          <div className="scanner-result-metric">
            <span className="eyebrow">Unique Tickers</span>
            <strong>{formatCount(payload?.total_unique_tickers ?? 0)}</strong>
          </div>
          <div className="scanner-result-metric">
            <span className="eyebrow">Overlap Names</span>
            <strong>{formatCount(payload?.overlapping_ticker_count ?? 0)}</strong>
          </div>
          <div className="scanner-result-metric">
            <span className="eyebrow">Daily Scanners</span>
            <strong>{formatCount(payload?.total_live_scanners ?? 0)}</strong>
          </div>
          <div className="scanner-result-metric">
            <span className="eyebrow">Signal Date</span>
            <strong>{formatLocalDate(payload?.latest_signal_date)}</strong>
          </div>
        </div>
      </section>

      <section className="scanner-result-filter-grid">
        <label className="scanner-result-filter panel">
          <span>Search</span>
          <input value={search} onChange={(event) => setSearch(event.target.value)} placeholder="Ticker, sector, scanner…" />
        </label>
        <label className="scanner-result-filter panel">
          <span>Sector</span>
          <select value={sectorFilter} onChange={(event) => setSectorFilter(event.target.value)}>
            <option value="all">All sectors</option>
            {sectors.map((sector) => (
              <option key={sector} value={sector}>
                {sector}
              </option>
            ))}
          </select>
        </label>
        <label className="scanner-result-filter panel">
          <span>Elite Pick</span>
          <span className="scanner-result-check">
            <input type="checkbox" checked={eliteOnly} onChange={(event) => setEliteOnly(event.target.checked)} />
            <span>Only show elite candidates</span>
          </span>
          <span className="panel-copy">1D + 1W Strong Buy, and FA rank top 200 when present.</span>
        </label>
        <label className="scanner-result-filter panel">
          <span>Scanner Mix</span>
          <span className="scanner-result-check">
            <input type="checkbox" checked={hasLeadershipScannerOnly} onChange={(event) => setHasLeadershipScannerOnly(event.target.checked)} />
            <span>Has Trend Template, Sean BO, or Venu Scan</span>
          </span>
          <span className="panel-copy">Focus on names confirmed by your leadership-style scanners.</span>
        </label>
        <label className="scanner-result-filter panel">
          <span>Fundamental Quality</span>
          <span className="scanner-result-check">
            <input type="checkbox" checked={hasFundamentalQualityOnly} onChange={(event) => setHasFundamentalQualityOnly(event.target.checked)} />
            <span>Has Fundamental Quality</span>
          </span>
          <span className="panel-copy">Keep only names that also appear on the Fundamental Quality board.</span>
        </label>
        <label className="scanner-result-filter panel">
          <span>RS Leader</span>
          <span className="scanner-result-check">
            <input type="checkbox" checked={leaderRsOnly} onChange={(event) => setLeaderRsOnly(event.target.checked)} />
            <span>Daily RS within range</span>
          </span>
          <div className="scanner-result-range-row">
            <input
              type="number"
              min={1}
              max={99}
              value={leaderRsMin}
              onChange={(event) => setLeaderRsMin(event.target.value)}
              placeholder="Min"
            />
            <input
              type="number"
              min={1}
              max={99}
              value={leaderRsMax}
              onChange={(event) => setLeaderRsMax(event.target.value)}
              placeholder="Max"
            />
          </div>
          <span className="panel-copy">Uses `daily_rs_rating`; blank max means up to 99.</span>
        </label>
        <label className="scanner-result-filter panel">
          <span>RS Evidence</span>
          <span className="scanner-result-check">
            <input type="checkbox" checked={rsEvidenceOnly} onChange={(event) => setRsEvidenceOnly(event.target.checked)} />
            <span>Use evidence stack filters</span>
          </span>
          <div className="scanner-result-range-row scanner-result-range-row-three">
            <input
              type="number"
              min={0}
              max={9}
              value={rsEvidenceMin}
              onChange={(event) => setRsEvidenceMin(event.target.value)}
              placeholder="Score"
              aria-label="Minimum RS evidence score"
            />
            <input
              type="number"
              min={0}
              max={100}
              value={rsDaysMinPct}
              onChange={(event) => setRsDaysMinPct(event.target.value)}
              placeholder="RS Days %"
              aria-label="Minimum RS days percent"
            />
            <input
              type="number"
              min={0}
              max={21}
              value={upOnDownDaysMin}
              onChange={(event) => setUpOnDownDaysMin(event.target.value)}
              placeholder="Up/Down"
              aria-label="Minimum up on down days"
            />
          </div>
          <span className="panel-copy">Score combines RS Phase, RS highs, RS days, up-on-down days, HVE, and Daily RS.</span>
        </label>
        <label className="scanner-result-filter panel">
          <span>Scanners</span>
          <div className="scanner-top-hit-filter-list">
            {scannerOptions.map((scanner) => {
              const checked = selectedScannerIds.includes(scanner.id);
              return (
                <label key={scanner.id} className="scanner-result-check">
                  <input
                    type="checkbox"
                    checked={checked}
                    onChange={(event) => {
                      setSelectedScannerIds((current) => {
                        if (event.target.checked) {
                          return current.includes(scanner.id) ? current : [...current, scanner.id];
                        }
                        return current.filter((item) => item !== scanner.id);
                      });
                    }}
                  />
                  <span>{scanner.label}</span>
                </label>
              );
            })}
          </div>
          <span className="panel-copy">Checked scanners use AND logic: a ticker must appear in every selected scanner.</span>
        </label>
        <div className="scanner-result-filter panel scanner-result-filter-actions">
          <span>Board Snapshot</span>
          <div className="scanner-result-view-actions">
            <Link className="ghost-button" to="/scanner">
              Back to board
            </Link>
          </div>
          <span className="panel-copy">Updated {formatLocalDateTime(payload?.latest_update_at)}.</span>
        </div>
        <div className="scanner-result-filter panel scanner-result-filter-actions">
          <span className="eyebrow">View</span>
          <div className="scanner-result-view-actions" role="tablist" aria-label="Scanner top hits view">
            <button
              className={`scanner-result-view-chip${viewMode === "list" ? " is-active" : ""}`}
              type="button"
              onClick={() => setViewMode("list")}
            >
              List
            </button>
            <button
              className={`scanner-result-view-chip${viewMode === "charts" ? " is-active" : ""}`}
              type="button"
              onClick={() => setViewMode("charts")}
            >
              Charts
            </button>
          </div>
          <span className="panel-copy">Chart mode loads visible names only.</span>
        </div>
      </section>

      <section className="scanner-result-table-shell panel">
        {isLoading ? <LoadingBlock label="Loading scanner top hits…" /> : null}
        {notice ? <p className="panel-copy">{notice}</p> : null}
        {!notice && myPicksNotice ? <p className="panel-copy earnings-console-note">{myPicksNotice}</p> : null}
        {!isLoading && !notice && filteredRows.length === 0 ? <p className="panel-copy">No tickers match current filters.</p> : null}
        {!isLoading && !notice && filteredRows.length > 0 ? (
          <>
            <div className="scanner-top-hits-toolbar">
              <span>{formatCount(filteredRows.length)} names</span>
              <span>Latest board date {formatLocalDate(payload?.target_trading_date)}</span>
            </div>
            <PaginationControls
              currentPage={normalizedPage}
              totalItems={filteredRows.length}
              totalPages={totalPages}
              pageSize={pageSize}
              onPageChange={setCurrentPage}
            />
            {viewMode === "charts" ? (
              <div className="scanner-result-chart-grid is-3-col">
                {pagedRows.map((row) => (
                  <ScannerTopHitChartCard
                    key={row.ticker}
                    row={row}
                    boardSignalDate={payload?.latest_signal_date}
                    canManageMyPicks={canManageMyPicks}
                    alreadyMyPick={myPickTickers.has(row.ticker)}
                    savingMyPick={Boolean(savingMyPickTickers[row.ticker])}
                    chartPayload={chartPayloads[row.ticker]}
                    chartError={chartErrors[row.ticker]}
                    isChartLoading={Boolean(chartLoadingTickers[row.ticker])}
                    onAddToMyPicks={handleAddToMyPicks}
                  />
                ))}
              </div>
            ) : (
            <div className="data-table-responsive scanner-result-table-wrap">
              <table className="data-table scanner-result-table scanner-top-hits-table">
                <thead>
                  <tr>
                    {canManageMyPicks ? <th>My Pick</th> : null}
                    <th>{renderSortButton("Ticker", "ticker", sortBy, sortDirection, setSortBy, setSortDirection)}</th>
                    <th>{renderSortButton("Hits", "hits", sortBy, sortDirection, setSortBy, setSortDirection)}</th>
                    <th>Scanners</th>
                    <th>{renderSortButton("Sector", "sector", sortBy, sortDirection, setSortBy, setSortDirection)}</th>
                    <th>Sector Momentum</th>
                    <th>{renderSortButton("Close", "close", sortBy, sortDirection, setSortBy, setSortDirection)}</th>
                    <th>{renderSortButton("Change", "change", sortBy, sortDirection, setSortBy, setSortDirection)}</th>
                    <th>{renderSortButton("From 52W Low %", "from52wLow", sortBy, sortDirection, setSortBy, setSortDirection)}</th>
                    <th>{renderSortButton("Bollinger", "bollinger", sortBy, sortDirection, setSortBy, setSortDirection)}</th>
                    <th>{renderSortButton("RS Evidence", "rsEvidence", sortBy, sortDirection, setSortBy, setSortDirection)}</th>
                    <th>{renderSortButton("RS Days", "rsDays", sortBy, sortDirection, setSortBy, setSortDirection)}</th>
                    <th>{renderSortButton("RS Phase", "rsPhaseDays", sortBy, sortDirection, setSortBy, setSortDirection)}</th>
                    <th>{renderSortButton("Up/Down", "upOnDownDays", sortBy, sortDirection, setSortBy, setSortDirection)}</th>
                    <th>1Y %</th>
                    <th>YTD %</th>
                    <th>CAN V2</th>
                    <th>VCP</th>
                    <th>Accel</th>
                    <th>{renderSortButton("RS", "rs", sortBy, sortDirection, setSortBy, setSortDirection)}</th>
                    <th>{renderSortButton("Daily RS", "dailyRs", sortBy, sortDirection, setSortBy, setSortDirection)}</th>
                    <th>{renderSortButton("3M RS", "rs3m", sortBy, sortDirection, setSortBy, setSortDirection)}</th>
                    <th>{renderSortButton("6M RS", "rs6m", sortBy, sortDirection, setSortBy, setSortDirection)}</th>
                    <th>{renderSortButton("RS Momentum", "rsMomentum", sortBy, sortDirection, setSortBy, setSortDirection)}</th>
                    <th>{renderSortButton("TA", "ta", sortBy, sortDirection, setSortBy, setSortDirection)}</th>
                    <th>1D</th>
                    <th>1W</th>
                    <th>{renderSortButton("FA", "fa", sortBy, sortDirection, setSortBy, setSortDirection)}</th>
                    <th>FA Rank</th>
                    <th>{renderSortButton("Decision", "decision", sortBy, sortDirection, setSortBy, setSortDirection)}</th>
                    <th>{renderSortButton("Decision Score", "decisionScore", sortBy, sortDirection, setSortBy, setSortDirection)}</th>
                    {eliteOnly ? <th>{renderSortButton("Sector Top Hit", "sectorTopHit", sortBy, sortDirection, setSortBy, setSortDirection)}</th> : null}
                    {eliteOnly ? <th>{renderSortButton("Industry Top Hit", "industryTopHit", sortBy, sortDirection, setSortBy, setSortDirection)}</th> : null}
                  </tr>
                </thead>
                <tbody>
                  {pagedRows.map((row) => (
                    <tr key={row.ticker}>
                      {canManageMyPicks ? (
                        <td data-label="My Pick">
                          <input
                            type="checkbox"
                            checked={myPickTickers.has(row.ticker)}
                            disabled={myPickTickers.has(row.ticker) || Boolean(savingMyPickTickers[row.ticker])}
                            aria-label={myPickTickers.has(row.ticker) ? `${row.ticker} already in My Picks` : `Add ${row.ticker} to My Picks`}
                            onChange={(event) => {
                              if (event.target.checked) {
                                void handleAddToMyPicks(row.ticker);
                              }
                            }}
                          />
                        </td>
                      ) : null}
                      <td data-label="Ticker">
                        <div className="scanner-result-company">
                          <Link className="scanner-result-symbol" to={buildChartHref(row.ticker)}>
                            {row.ticker}
                          </Link>
                          <span>{row.company || row.industry || "-"}</span>
                        </div>
                      </td>
                      <td data-label="Hits">
                        <strong>{formatCount(row.scanner_count)}</strong>
                      </td>
                      <td data-label="Scanners">
                        <div className="scanner-top-hit-pills">
                          {row.scanners.map((scanner) => (
                            <Link key={`${row.ticker}-${scanner.id}`} className="scanner-card-pill" to={`/scanner/${encodeURIComponent(scanner.id)}`}>
                              {scanner.label}
                            </Link>
                          ))}
                        </div>
                      </td>
                      <td data-label="Sector">
                        <div className="scanner-result-sector">
                          <strong>{row.sector || "Unknown sector"}</strong>
                          <span>{row.industry || "-"}</span>
                        </div>
                      </td>
                      <td data-label="Sector Momentum">
                        <SectorMomentumCell row={row} />
                      </td>
                      <td data-label="Close">{formatPrice(row.day_close)}</td>
                      <td data-label="Change">{renderChange(row.change_pct)}</td>
                      <td data-label="From 52W Low %">{renderChange(row.change_from_52wk_low_pct)}</td>
                      <td data-label="Bollinger">{renderBollingerBandStatus(row.bollinger_band_status)}</td>
                      <td data-label="RS Evidence">{renderRsEvidenceCell(row)}</td>
                      <td data-label="RS Days">{formatCountWithPercent(row.rs_days_21d, row.rs_days_21d_pct)}</td>
                      <td data-label="RS Phase">{formatPhaseDays(resolveRsPhaseActiveDays(row))}</td>
                      <td data-label="Up/Down">{formatCountWithPercent(row.up_on_down_days_21d, row.up_on_down_days_21d_pct)}</td>
                      <td data-label="1Y %">{renderChange(row.perf_year_pct)}</td>
                      <td data-label="YTD %">{renderChange(row.perf_ytd_pct)}</td>
                      <td data-label="CAN V2">{formatCanslimScore(row.canslim_score, row.canslim_max_score)}</td>
                      <td data-label="VCP">{formatVcpScore(row.vcp_score, row.vcp_rating)}</td>
                      <td data-label="Accel">{formatAccelerationScore(row.growth_acceleration_score, row.growth_acceleration_label)}</td>
                      <td data-label="RS">{formatRating(row.rs_rating)}</td>
                      <td data-label="Daily RS">{formatRating(row.daily_rs_rating ?? null)}</td>
                      <td data-label="3M RS">{formatRating(row.rs_rating_3m ?? null)}</td>
                      <td data-label="6M RS">{formatRating(row.rs_rating_6m ?? null)}</td>
                      <td data-label="RS Momentum">{renderRsMomentumCell(row)}</td>
                      <td data-label="TA">{formatRating(row.ta_rating)}</td>
                      <td data-label="1D">{formatTechnicalIndicatorLabel(row.technical_indicator_ratings?.["1d"])}</td>
                      <td data-label="1W">{formatTechnicalIndicatorLabel(row.technical_indicator_ratings?.["1w"])}</td>
                      <td data-label="FA">{formatRating(row.fa_rating)}</td>
                      <td data-label="FA Rank">{row.fa_current_rank != null ? `#${formatCount(row.fa_current_rank)}` : "--"}</td>
                      <td data-label="Decision">{renderPositionActionCell(row.position_action)}</td>
                      <td data-label="Decision Score">{formatDecisionScore(row.position_action?.action_score)}</td>
                      {eliteOnly ? (
                        <td data-label="Sector Top Hit">
                          {eliteSectorLeaders.get(normalizeSectorKey(row.sector)) === row.ticker ? (
                            <span className="scanner-score-pill is-strong">Top Hit</span>
                          ) : (
                            "--"
                          )}
                        </td>
                      ) : null}
                      {eliteOnly ? (
                        <td data-label="Industry Top Hit">
                          {eliteIndustryLeaders.get(normalizeIndustryKey(row.industry)) === row.ticker ? (
                            <span className="scanner-score-pill is-strong">Top Hit</span>
                          ) : (
                            "--"
                          )}
                        </td>
                      ) : null}
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
            )}
            <PaginationControls
              currentPage={normalizedPage}
              totalItems={filteredRows.length}
              totalPages={totalPages}
              pageSize={pageSize}
              onPageChange={setCurrentPage}
            />
          </>
        ) : null}
      </section>
    </div>
  );
}

function ScannerTopHitChartCard({
  row,
  boardSignalDate,
  canManageMyPicks,
  alreadyMyPick,
  savingMyPick,
  chartPayload,
  chartError,
  isChartLoading,
  onAddToMyPicks,
}: {
  row: ScannerTopHitRow;
  boardSignalDate: string | null | undefined;
  canManageMyPicks: boolean;
  alreadyMyPick: boolean;
  savingMyPick: boolean;
  chartPayload: WatchlistChartResponse | null | undefined;
  chartError: string | undefined;
  isChartLoading: boolean;
  onAddToMyPicks: (ticker: string) => Promise<void>;
}) {
  const chartCandles = buildMiniChartCandles(chartPayload);
  const latestCandle = chartCandles[chartCandles.length - 1] ?? null;
  return (
    <article className="scanner-chart-card scanner-top-hit-chart-card">
      <div className="scanner-chart-card-header">
        <div className="scanner-chart-card-heading">
          <div className="scanner-chart-card-symbol-row">
            {canManageMyPicks ? (
              <input
                type="checkbox"
                checked={alreadyMyPick}
                disabled={alreadyMyPick || savingMyPick}
                aria-label={alreadyMyPick ? `${row.ticker} already in My Picks` : `Add ${row.ticker} to My Picks`}
                onChange={(event) => {
                  if (event.target.checked) {
                    void onAddToMyPicks(row.ticker);
                  }
                }}
              />
            ) : null}
            <Link className="scanner-result-symbol" to={buildChartHref(row.ticker)}>
              <span>{row.ticker}</span>
            </Link>
          </div>
          <strong>{row.company || row.industry || "Scanner hit"}</strong>
          <span>{[row.sector, row.industry].filter(Boolean).join(" / ") || "Unknown group"}</span>
        </div>
        <div className="scanner-chart-card-price">
          <strong>{latestCandle ? formatPrice(latestCandle.close) : formatPrice(row.day_close)}</strong>
          {renderChange(row.change_pct)}
        </div>
      </div>
      <div className="scanner-chart-card-score-row">
        <span className="scanner-score-pill is-strong">{formatCount(row.scanner_count)} hits</span>
        {row.rs_evidence_score != null ? (
          <span className={`scanner-score-pill ${toneForRating(row.rs_evidence_score, 5)}`}>Evidence {row.rs_evidence_score}/{row.rs_evidence_max_score ?? 9}</span>
        ) : null}
        {resolveRsPhaseActiveDays(row) != null ? <span className="scanner-score-pill">RS Phase {formatPhaseDays(resolveRsPhaseActiveDays(row))}</span> : null}
        <span className={`scanner-score-pill ${toneForRating(row.daily_rs_rating ?? row.rs_rating, 90)}`}>RS {formatRating(row.daily_rs_rating ?? row.rs_rating)}</span>
        <span className={`scanner-score-pill ${toneForRating(row.ta_rating, 80)}`}>TA {formatRating(row.ta_rating)}</span>
        <span className={`scanner-score-pill ${toneForRating(row.fa_rating, 80)}`}>FA {formatRating(row.fa_rating)}</span>
        <span className={`scanner-score-pill ${toneForPositionAction(row.position_action?.action)}`}>{humanizePositionAction(row.position_action?.action)}</span>
      </div>
      <div className="scanner-top-hit-pills">
        {row.scanners.slice(0, 4).map((scanner) => (
          <Link key={`${row.ticker}-chart-${scanner.id}`} className="scanner-card-pill" to={`/scanner/${encodeURIComponent(scanner.id)}`}>
            {scanner.label}
          </Link>
        ))}
        {row.scanners.length > 4 ? <span className="scanner-card-pill muted">+{row.scanners.length - 4}</span> : null}
      </div>
      <div className="scanner-chart-card-body">
        {isChartLoading ? <LoadingBlock label={`Loading ${row.ticker} chart...`} /> : null}
        {!isChartLoading && chartError ? <p className="panel-copy">{chartError}</p> : null}
        {!isChartLoading && !chartError && chartCandles.length === 0 ? <p className="panel-copy">No chart data.</p> : null}
        {!isChartLoading && !chartError && chartCandles.length > 0 ? (
          <ScannerMiniChart
            ticker={row.ticker}
            candles={chartCandles}
            ema9={buildExponentialMovingAverage(chartCandles, 9)}
            ema21={chartPayload?.ema21 ?? buildExponentialMovingAverage(chartCandles, 21)}
          />
        ) : null}
      </div>
      <div className="scanner-chart-card-footer">
        <span>{chartPayload?.resolved_as_of_date ? `As of ${chartPayload.resolved_as_of_date}` : `Signal ${formatLocalDate(boardSignalDate)}`}</span>
        <Link to={buildChartHref(row.ticker)}>Analyze Full Chart</Link>
      </div>
    </article>
  );
}

function SectorMomentumCell({ row }: { row: ScannerTopHitRow }) {
  const momentum = row.sector_momentum;
  if (!momentum) {
    return <span className="panel-copy">--</span>;
  }
  const tone = toneForQuadrant(momentum.quadrant);
  return (
    <div className="scanner-top-hits-momentum">
      <span className={`scanner-score-pill ${tone}`}>{momentum.quadrant || "--"}</span>
      <span>{momentum.etf_ticker || momentum.sector}</span>
      <span>
        {formatCompactNumber(momentum.rs_ratio)} / {formatCompactNumber(momentum.momentum)}
      </span>
    </div>
  );
}

function renderSortButton(
  label: string,
  column: SortKey,
  sortBy: SortKey,
  sortDirection: SortDirection,
  setSortBy: (value: SortKey) => void,
  setSortDirection: (value: SortDirection) => void,
) {
  const isActive = sortBy === column;
  const indicator = !isActive ? "" : sortDirection === "asc" ? " ↑" : " ↓";
  return (
    <button
      className={`ghost-button scanner-result-sort-button${isActive ? " is-active" : ""}`}
      type="button"
      onClick={() => {
        if (isActive) {
          setSortDirection(sortDirection === "asc" ? "desc" : "asc");
          return;
        }
        setSortBy(column);
        setSortDirection(column === "ticker" || column === "sector" ? "asc" : "desc");
      }}
    >
      {label}
      {indicator}
    </button>
  );
}

function compareRows(
  left: ScannerTopHitRow,
  right: ScannerTopHitRow,
  sortBy: SortKey,
  sortDirection: SortDirection,
  leaderMaps: { sectorLeaders: Map<string, string>; industryLeaders: Map<string, string> },
) {
  if (sortBy === "ticker") {
    return compareText(left.ticker, right.ticker, sortDirection);
  }
  if (sortBy === "sector") {
    return compareText(left.sector, right.sector, sortDirection) || left.ticker.localeCompare(right.ticker);
  }
  if (sortBy === "sectorTopHit") {
    return compareNullableNumber(
      leaderMaps.sectorLeaders.get(normalizeSectorKey(left.sector)) === left.ticker ? 1 : 0,
      leaderMaps.sectorLeaders.get(normalizeSectorKey(right.sector)) === right.ticker ? 1 : 0,
      sortDirection,
    ) || left.ticker.localeCompare(right.ticker);
  }
  if (sortBy === "industryTopHit") {
    return compareNullableNumber(
      leaderMaps.industryLeaders.get(normalizeIndustryKey(left.industry)) === left.ticker ? 1 : 0,
      leaderMaps.industryLeaders.get(normalizeIndustryKey(right.industry)) === right.ticker ? 1 : 0,
      sortDirection,
    ) || left.ticker.localeCompare(right.ticker);
  }
  if (sortBy === "hits") {
    return compareNullableNumber(left.scanner_count, right.scanner_count, sortDirection) || left.ticker.localeCompare(right.ticker);
  }
  if (sortBy === "close") {
    return compareNullableNumber(left.day_close, right.day_close, sortDirection) || left.ticker.localeCompare(right.ticker);
  }
  if (sortBy === "change") {
    return compareNullableNumber(left.change_pct, right.change_pct, sortDirection) || left.ticker.localeCompare(right.ticker);
  }
  if (sortBy === "from52wLow") {
    return compareNullableNumber(left.change_from_52wk_low_pct, right.change_from_52wk_low_pct, sortDirection) || left.ticker.localeCompare(right.ticker);
  }
  if (sortBy === "bollinger") {
    return compareText(left.bollinger_band_status ?? "", right.bollinger_band_status ?? "", sortDirection) || left.ticker.localeCompare(right.ticker);
  }
  if (sortBy === "rsEvidence") {
    return compareNullableNumber(left.rs_evidence_score ?? null, right.rs_evidence_score ?? null, sortDirection) || left.ticker.localeCompare(right.ticker);
  }
  if (sortBy === "rsDays") {
    return compareNullableNumber(left.rs_days_21d_pct ?? null, right.rs_days_21d_pct ?? null, sortDirection) || left.ticker.localeCompare(right.ticker);
  }
  if (sortBy === "rsPhaseDays") {
    return compareNullableNumber(resolveRsPhaseActiveDays(left), resolveRsPhaseActiveDays(right), sortDirection) || left.ticker.localeCompare(right.ticker);
  }
  if (sortBy === "upOnDownDays") {
    return compareNullableNumber(left.up_on_down_days_21d ?? null, right.up_on_down_days_21d ?? null, sortDirection) || left.ticker.localeCompare(right.ticker);
  }
  if (sortBy === "rs") {
    return compareNullableNumber(left.rs_rating, right.rs_rating, sortDirection) || left.ticker.localeCompare(right.ticker);
  }
  if (sortBy === "dailyRs") {
    return compareNullableNumber(left.daily_rs_rating ?? null, right.daily_rs_rating ?? null, sortDirection) || left.ticker.localeCompare(right.ticker);
  }
  if (sortBy === "rs3m") {
    return compareNullableNumber(left.rs_rating_3m ?? null, right.rs_rating_3m ?? null, sortDirection) || left.ticker.localeCompare(right.ticker);
  }
  if (sortBy === "rs6m") {
    return compareNullableNumber(left.rs_rating_6m ?? null, right.rs_rating_6m ?? null, sortDirection) || left.ticker.localeCompare(right.ticker);
  }
  if (sortBy === "rsMomentum") {
    return (
      compareNullableNumber(resolveRsMomentumRank(left), resolveRsMomentumRank(right), sortDirection) ||
      left.ticker.localeCompare(right.ticker)
    );
  }
  if (sortBy === "ta") {
    return compareNullableNumber(left.ta_rating, right.ta_rating, sortDirection) || left.ticker.localeCompare(right.ticker);
  }
  if (sortBy === "decision") {
    return compareText(left.position_action?.action ?? "", right.position_action?.action ?? "", sortDirection) || left.ticker.localeCompare(right.ticker);
  }
  if (sortBy === "decisionScore") {
    return compareNullableNumber(left.position_action?.action_score ?? null, right.position_action?.action_score ?? null, sortDirection) || left.ticker.localeCompare(right.ticker);
  }
  return compareNullableNumber(left.fa_rating, right.fa_rating, sortDirection) || left.ticker.localeCompare(right.ticker);
}

function compareNullableNumber(left: number | null, right: number | null, sortDirection: SortDirection) {
  const missingSentinel = sortDirection === "asc" ? Number.POSITIVE_INFINITY : Number.NEGATIVE_INFINITY;
  const normalizedLeft = typeof left === "number" ? left : missingSentinel;
  const normalizedRight = typeof right === "number" ? right : missingSentinel;
  return sortDirection === "asc" ? normalizedLeft - normalizedRight : normalizedRight - normalizedLeft;
}

function compareText(left: string, right: string, sortDirection: SortDirection) {
  return sortDirection === "asc" ? left.localeCompare(right) : right.localeCompare(left);
}

function buildChartHref(ticker: string) {
  const params = new URLSearchParams();
  params.set("ticker", ticker);
  return `/charts?${params.toString()}`;
}

function formatPrice(value: number | null) {
  return value == null ? "--" : `$${value.toFixed(2)}`;
}

function formatRating(value: number | null) {
  return value == null ? "--" : value.toFixed(1);
}

function formatPhaseDays(value: number | null | undefined) {
  return value == null ? "--" : `${Math.round(value)}D`;
}

function resolveRsPhaseActiveDays(row: ScannerTopHitRow): number | null {
  return row.rs_phase_active_days ?? row.relative_strength_evidence?.rs_phase_active_days ?? null;
}

function formatCountWithPercent(count: number | null | undefined, pct: number | null | undefined) {
  if (count == null && pct == null) {
    return "--";
  }
  if (count == null) {
    return `${pct?.toFixed(1)}%`;
  }
  if (pct == null) {
    return `${formatCount(count)}`;
  }
  return `${formatCount(count)} / ${pct.toFixed(1)}%`;
}

function renderRsEvidenceCell(row: ScannerTopHitRow) {
  const score = row.rs_evidence_score ?? row.relative_strength_evidence?.score ?? null;
  const maxScore = row.rs_evidence_max_score ?? row.relative_strength_evidence?.max_score ?? null;
  if (score == null) {
    return <span className="panel-copy">--</span>;
  }
  const reasons = row.relative_strength_evidence?.reasons ?? [];
  return (
    <span className={`scanner-score-pill ${toneForRating(score, 5)}`} title={reasons.length > 0 ? reasons.join(" | ") : undefined}>
      {score}/{maxScore ?? 9}
    </span>
  );
}

function renderRsMomentumCell(row: ScannerTopHitRow) {
  const signal = resolveRsMomentumSignal(row.rs_rating_3m, row.rs_rating_6m, row.daily_rs_rating);
  return (
    <span className={`scanner-score-pill ${signal.toneClass}`} title={signal.title}>
      {signal.label}
    </span>
  );
}

function resolveRsMomentumRank(row: ScannerTopHitRow) {
  return resolveRsMomentumSignal(row.rs_rating_3m, row.rs_rating_6m, row.daily_rs_rating).rank;
}

function formatCanslimScore(score: number | null | undefined, maxScore: number | null | undefined) {
  if (score == null || Number.isNaN(score)) {
    return "--";
  }
  if (maxScore == null || Number.isNaN(maxScore)) {
    return `${Math.round(score)}`;
  }
  return `${Math.round(score)}/${Math.round(maxScore)}`;
}

function formatVcpScore(score: number | null | undefined, rating: string | null | undefined) {
  if (score == null || Number.isNaN(score)) {
    return "--";
  }
  const base = score.toFixed(1);
  return rating ? `${base} ${rating}` : base;
}

function renderBollingerBandStatus(status: string | null | undefined) {
  if (!status) {
    return <span className="panel-copy">--</span>;
  }
  switch (status.trim().toLowerCase()) {
    case "above_upper_band":
      return <span className="scanner-score-pill is-caution">Above</span>;
    case "within_bands":
      return <span className="scanner-score-pill is-neutral">Within</span>;
    case "below_lower_band":
      return <span className="scanner-score-pill is-negative">Below</span>;
    default:
      return <span className="panel-copy">{status}</span>;
  }
}

function formatCompactNumber(value: number | null | undefined) {
  return value == null ? "--" : value.toFixed(1);
}

function formatAccelerationScore(score: number | null | undefined, label: string | null | undefined) {
  if (score == null || Number.isNaN(score)) {
    return "--";
  }
  const base = score.toFixed(0);
  return label ? `${base} ${label}` : base;
}

function formatTechnicalIndicatorLabel(value: TechnicalIndicatorRatingCell | undefined) {
  return value?.rating_label ?? "--";
}

function formatDecisionScore(value: number | null | undefined) {
  return value == null ? "--" : value.toFixed(1);
}

function renderPositionActionCell(positionAction: ScannerTopHitRow["position_action"]) {
  if (!positionAction?.action) {
    return <span className="panel-copy">--</span>;
  }
  return (
    <div title={positionAction.reason_summary ?? undefined}>
      <span className={`scanner-score-pill ${toneForPositionAction(positionAction.action)}`}>{humanizePositionAction(positionAction.action)}</span>
      <div className="ticker-company-inline">
        {humanizePositionTrend(positionAction.trend_state)} / {humanizePositionExtension(positionAction.extension_state)}
      </div>
    </div>
  );
}

function humanizePositionAction(value: string | null | undefined) {
  switch (String(value || "").trim().toLowerCase()) {
    case "add_position":
      return "Add";
    case "hold_position":
      return "Hold";
    case "trim_reduce":
      return "Trim";
    case "avoid_new":
      return "Avoid";
    default:
      return "--";
  }
}

function humanizePositionTrend(value: string | null | undefined) {
  switch (String(value || "").trim().toLowerCase()) {
    case "healthy":
      return "Healthy";
    case "weakening":
      return "Weakening";
    case "broken":
      return "Broken";
    default:
      return "--";
  }
}

function humanizePositionExtension(value: string | null | undefined) {
  switch (String(value || "").trim().toLowerCase()) {
    case "normal":
      return "Normal";
    case "stretched":
      return "Stretched";
    case "extreme":
      return "Extreme";
    default:
      return "--";
  }
}

function toneForPositionAction(value: string | null | undefined) {
  switch (String(value || "").trim().toLowerCase()) {
    case "add_position":
      return "is-strong";
    case "hold_position":
      return "is-neutral";
    case "trim_reduce":
      return "is-warning";
    case "avoid_new":
      return "is-weak";
    default:
      return "";
  }
}

function isElitePick(row: ScannerTopHitRow) {
  const dailyLabel = normalizeIndicatorLabel(row.technical_indicator_ratings?.["1d"]);
  const weeklyLabel = normalizeIndicatorLabel(row.technical_indicator_ratings?.["1w"]);
  const faRankOk = row.fa_current_rank == null || row.fa_current_rank <= 200;
  return dailyLabel === "strong buy" && weeklyLabel === "strong buy" && faRankOk;
}

function hasLeadershipScannerSignal(row: ScannerTopHitRow) {
  return row.scanners.some((scanner) => LEADERSHIP_SCANNER_IDS.has(normalizeScannerId(scanner.id)));
}

function hasFundamentalQualitySignal(row: ScannerTopHitRow) {
  return row.scanners.some((scanner) => normalizeScannerId(scanner.id) === "fundamental_quality");
}

function hasDailyRsRatingInRange(row: ScannerTopHitRow, minimumDailyRsRating: number, maximumDailyRsRating: number) {
  return row.daily_rs_rating != null && row.daily_rs_rating >= minimumDailyRsRating && row.daily_rs_rating <= maximumDailyRsRating;
}

function hasRsEvidenceProfile(
  row: ScannerTopHitRow,
  filters: { minScore: number; minRsDaysPct: number; minUpOnDownDays: number },
) {
  const score = row.rs_evidence_score ?? row.relative_strength_evidence?.score ?? null;
  const rsDaysPct = row.rs_days_21d_pct ?? row.relative_strength_evidence?.rs_days_21d_pct ?? null;
  const upOnDownDays = row.up_on_down_days_21d ?? row.relative_strength_evidence?.up_on_down_days_21d ?? null;
  return (
    score != null &&
    score >= filters.minScore &&
    rsDaysPct != null &&
    rsDaysPct >= filters.minRsDaysPct &&
    upOnDownDays != null &&
    upOnDownDays >= filters.minUpOnDownDays
  );
}

function normalizeRsRatingRange(minValue: string, maxValue: string) {
  const minParsed = Number.parseInt(minValue, 10);
  const maxParsed = Number.parseInt(maxValue, 10);
  const min = Number.isFinite(minParsed) ? Math.max(1, Math.min(99, minParsed)) : 1;
  const max = Number.isFinite(maxParsed) ? Math.max(1, Math.min(99, maxParsed)) : 99;
  if (min > max) {
    return { min: max, max: min };
  }
  return { min, max };
}

function normalizeBoundedInteger(value: string, minValue: number, maxValue: number, fallback: number) {
  const parsed = Number.parseInt(value, 10);
  if (!Number.isFinite(parsed)) {
    return fallback;
  }
  return Math.max(minValue, Math.min(maxValue, parsed));
}

function hasSelectedScannerSignals(row: ScannerTopHitRow, selectedScannerIds: string[]) {
  const rowScannerIds = new Set(row.scanners.map((scanner) => normalizeScannerId(scanner.id)).filter(Boolean));
  return selectedScannerIds.every((scannerId) => rowScannerIds.has(scannerId));
}

function normalizeIndicatorLabel(value: TechnicalIndicatorRatingCell | undefined) {
  return String(value?.rating_label || "").trim().toLowerCase();
}

function normalizeScannerId(value: string | null | undefined) {
  return String(value || "").trim().toLowerCase();
}

function normalizeIndustryKey(industry: string | null | undefined) {
  return String(industry || "").trim().toLowerCase();
}

function normalizeSectorKey(sector: string | null | undefined) {
  return String(sector || "").trim().toLowerCase();
}

function buildEliteLeaderMap(
  rows: ScannerTopHitRow[],
  resolveKey: (row: ScannerTopHitRow) => string,
) {
  const leaders = new Map<string, ScannerTopHitRow>();
  for (const row of rows) {
    const key = resolveKey(row);
    if (!key) {
      continue;
    }
    const currentLeader = leaders.get(key);
    if (!currentLeader || compareEliteIndustryLeader(row, currentLeader) < 0) {
      leaders.set(key, row);
    }
  }
  return new Map(Array.from(leaders.entries()).map(([key, row]) => [key, row.ticker]));
}

function compareEliteIndustryLeader(left: ScannerTopHitRow, right: ScannerTopHitRow) {
  if (left.scanner_count !== right.scanner_count) {
    return right.scanner_count - left.scanner_count;
  }
  const leftFaRank = left.fa_current_rank ?? Number.POSITIVE_INFINITY;
  const rightFaRank = right.fa_current_rank ?? Number.POSITIVE_INFINITY;
  if (leftFaRank !== rightFaRank) {
    return leftFaRank - rightFaRank;
  }
  if ((left.fa_rating ?? Number.NEGATIVE_INFINITY) !== (right.fa_rating ?? Number.NEGATIVE_INFINITY)) {
    return (right.fa_rating ?? Number.NEGATIVE_INFINITY) - (left.fa_rating ?? Number.NEGATIVE_INFINITY);
  }
  if ((left.ta_rating ?? Number.NEGATIVE_INFINITY) !== (right.ta_rating ?? Number.NEGATIVE_INFINITY)) {
    return (right.ta_rating ?? Number.NEGATIVE_INFINITY) - (left.ta_rating ?? Number.NEGATIVE_INFINITY);
  }
  if ((left.rs_rating ?? Number.NEGATIVE_INFINITY) !== (right.rs_rating ?? Number.NEGATIVE_INFINITY)) {
    return (right.rs_rating ?? Number.NEGATIVE_INFINITY) - (left.rs_rating ?? Number.NEGATIVE_INFINITY);
  }
  return left.ticker.localeCompare(right.ticker);
}

function renderChange(value: number | null) {
  if (value == null) {
    return <span className="ticker-change neutral">--</span>;
  }
  const tone = value >= 0 ? "up" : "down";
  return (
    <span className={`ticker-change ${tone}`}>
      {value >= 0 ? "+" : ""}
      {value.toFixed(2)}%
    </span>
  );
}

function toneForQuadrant(value: string) {
  if (value === "Leading") {
    return "is-strong";
  }
  if (value === "Improving") {
    return "is-warm";
  }
  return "is-neutral";
}

function toneForRating(value: number | null | undefined, strongThreshold: number) {
  if (value == null || Number.isNaN(value)) {
    return "is-neutral";
  }
  if (value >= strongThreshold) {
    return "is-strong";
  }
  if (value >= strongThreshold - 20) {
    return "is-caution";
  }
  return "is-weak";
}

function buildMiniChartCandles(payload: WatchlistChartResponse | null | undefined): CandlePoint[] {
  if (!payload) {
    return [];
  }
  const volumeByTime = new Map((payload.volume ?? []).map((item) => [item.time, item.value]));
  return (payload.candles ?? []).map((item) => ({
    ...item,
    volume: volumeByTime.get(item.time) ?? 0,
  }));
}

function buildExponentialMovingAverage(candles: CandlePoint[], length: number): Array<{ time: string; value: number }> {
  if (candles.length === 0 || length <= 0) {
    return [];
  }
  const alpha = 2 / (length + 1);
  let ema = candles[0].close;
  const points = [{ time: candles[0].time, value: Number(ema.toFixed(2)) }];
  for (let index = 1; index < candles.length; index += 1) {
    ema = candles[index].close * alpha + ema * (1 - alpha);
    points.push({ time: candles[index].time, value: Number(ema.toFixed(2)) });
  }
  return points;
}
