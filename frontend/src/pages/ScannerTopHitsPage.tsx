import { useEffect, useMemo, useState } from "react";
import { Link } from "react-router-dom";
import { preloadChartsPage } from "../App";
import { useAuth } from "../auth/AuthContext";
import { LoadingBlock } from "../components/LoadingBlock";
import { PaginationControls } from "../components/PaginationControls";
import { ScannerMiniChart } from "../components/ScannerMiniChart";
import { fetchJson } from "../lib/api";
import { buildChartCandles, buildExponentialMovingAverage } from "../lib/chartData";
import { formatCount, formatLocalDate, formatLocalDateTime, humanizePositionAction, humanizePositionExtension, humanizePositionTrend, toneForPositionAction } from "../lib/format";
import { resolveRsMomentumSignal } from "../lib/rsMomentum";
import type { MyPicksContextResponse, ScannerTopHitRow, ScannerTopHitsResponse, TechnicalIndicatorRatingCell, WatchlistChartResponse } from "../lib/types";

type SortKey = "hits" | "ticker" | "sector" | "sectorTopHit" | "industryTopHit" | "close" | "change" | "from52wLow" | "bollinger" | "rsEvidence" | "rsDays" | "rsPhaseDays" | "upOnDownDays" | "rs" | "dailyRs" | "rs3m" | "rs6m" | "rsMomentum" | "ta" | "fa" | "decision" | "decisionScore";
type SortDirection = "asc" | "desc";
type ViewMode = "list" | "charts" | "guru" | "position";
type TopHitsFilterPreset = {
  sectorFilter: string;
  eliteOnly: boolean;
  hasLeadershipScannerOnly: boolean;
  hasFundamentalQualityOnly: boolean;
  leaderRsOnly: boolean;
  leaderRsMin: string;
  leaderRsMax: string;
  rsEvidenceOnly: boolean;
  rsEvidenceMin: string;
  rsDaysMinPct: string;
  upOnDownDaysMin: string;
  scannerGroups: string[][];
  sortBy: SortKey;
  sortDirection: SortDirection;
  viewMode: ViewMode;
};
type TopHitsPresetStore = { presets: Record<string, TopHitsFilterPreset>; defaultPresetName: string };
const LIST_PAGE_SIZE = 50;
const CHART_PAGE_SIZE = 9;
const GURU_COLUMN_PAGE_SIZE = 30;
const LEADERSHIP_SCANNER_IDS = new Set(["trend_template", "weekly_candidate_pool", "qullamaggie", "sean_breakout", "venu_scanner"]);
const PINNED_SCANNER_OPTIONS = [
  { id: "weekly_candidate_pool", label: "Weekly Candidate Pool" },
  { id: "qullamaggie", label: "Qullamaggie" },
];
const FILTER_PRESETS_STORAGE_KEY = "top-hits-filter-presets";
const EMPTY_TOP_HITS_FILTERS: TopHitsFilterPreset = {
  sectorFilter: "all",
  eliteOnly: false,
  hasLeadershipScannerOnly: false,
  hasFundamentalQualityOnly: false,
  leaderRsOnly: false,
  leaderRsMin: "90",
  leaderRsMax: "",
  rsEvidenceOnly: false,
  rsEvidenceMin: "5",
  rsDaysMinPct: "60",
  upOnDownDaysMin: "3",
  scannerGroups: [[]],
  sortBy: "hits",
  sortDirection: "desc",
  viewMode: "list",
};
const DEFAULT_TOP_HITS_FILTERS: TopHitsFilterPreset = {
  ...EMPTY_TOP_HITS_FILTERS,
  scannerGroups: [
    ["qullamaggie", "weekly_candidate_pool", "kai_s2"],
    ["venu_scanner", "trend_template", "one_year_winners", "finviz_smallover_sales_growth_trend", "sean_breakout"],
  ],
};

export function ScannerTopHitsPage() {
  const auth = useAuth();
  const [payload, setPayload] = useState<ScannerTopHitsResponse | null>(null);
  const [isLoading, setIsLoading] = useState(true);
  const [reloadKey, setReloadKey] = useState(0);
  const [notice, setNotice] = useState("");
  const [myPicksNotice, setMyPicksNotice] = useState("");
  const [presetStore, setPresetStore] = useState<TopHitsPresetStore>(loadTopHitsPresetStore);
  const initialFilters = presetStore.presets[presetStore.defaultPresetName] ?? DEFAULT_TOP_HITS_FILTERS;
  const [selectedPresetName, setSelectedPresetName] = useState(presetStore.defaultPresetName);
  const [presetNotice, setPresetNotice] = useState("");
  const [search, setSearch] = useState("");
  const [scannerSearch, setScannerSearch] = useState("");
  const [scannerNames, setScannerNames] = useState<Record<string, string>>(() => {
    try {
      const saved: unknown = JSON.parse(localStorage.getItem("top-hits-scanner-names") || "{}");
      return saved && typeof saved === "object" && !Array.isArray(saved)
        ? Object.fromEntries(Object.entries(saved).filter((entry): entry is [string, string] => typeof entry[1] === "string" && entry[1].trim().length > 0)) : {};
    } catch { return {}; }
  });
  const [nameNotice, setNameNotice] = useState("");
  const [sectorFilter, setSectorFilter] = useState(initialFilters.sectorFilter);
  const [eliteOnly, setEliteOnly] = useState(initialFilters.eliteOnly);
  const [hasLeadershipScannerOnly, setHasLeadershipScannerOnly] = useState(initialFilters.hasLeadershipScannerOnly);
  const [hasFundamentalQualityOnly, setHasFundamentalQualityOnly] = useState(initialFilters.hasFundamentalQualityOnly);
  const [leaderRsOnly, setLeaderRsOnly] = useState(initialFilters.leaderRsOnly);
  const [leaderRsMin, setLeaderRsMin] = useState(initialFilters.leaderRsMin);
  const [leaderRsMax, setLeaderRsMax] = useState(initialFilters.leaderRsMax);
  const [rsEvidenceOnly, setRsEvidenceOnly] = useState(initialFilters.rsEvidenceOnly);
  const [rsEvidenceMin, setRsEvidenceMin] = useState(initialFilters.rsEvidenceMin);
  const [rsDaysMinPct, setRsDaysMinPct] = useState(initialFilters.rsDaysMinPct);
  const [upOnDownDaysMin, setUpOnDownDaysMin] = useState(initialFilters.upOnDownDaysMin);
  const [scannerGroups, setScannerGroups] = useState<string[][]>(() => initialFilters.scannerGroups.map((group) => [...group]));
  const [activeScannerGroupIndex, setActiveScannerGroupIndex] = useState(0);
  const [sortBy, setSortBy] = useState<SortKey>(initialFilters.sortBy);
  const [sortDirection, setSortDirection] = useState<SortDirection>(initialFilters.sortDirection);
  const [viewMode, setViewMode] = useState<ViewMode>(initialFilters.viewMode);
  const [currentPage, setCurrentPage] = useState(1);
  const [myPickTickers, setMyPickTickers] = useState<Set<string>>(new Set());
  const [savingMyPickTickers, setSavingMyPickTickers] = useState<Record<string, boolean>>({});
  const [chartPayloads, setChartPayloads] = useState<Record<string, WatchlistChartResponse | null | undefined>>({});
  const [chartErrors, setChartErrors] = useState<Record<string, string>>({});
  const [chartLoadingTickers, setChartLoadingTickers] = useState<Record<string, boolean>>({});
  const canManageMyPicks = auth.hasCapability("manage_exclusions");

  const applyFilterPreset = (preset: TopHitsFilterPreset) => {
    setSectorFilter(preset.sectorFilter);
    setEliteOnly(preset.eliteOnly);
    setHasLeadershipScannerOnly(preset.hasLeadershipScannerOnly);
    setHasFundamentalQualityOnly(preset.hasFundamentalQualityOnly);
    setLeaderRsOnly(preset.leaderRsOnly);
    setLeaderRsMin(preset.leaderRsMin);
    setLeaderRsMax(preset.leaderRsMax);
    setRsEvidenceOnly(preset.rsEvidenceOnly);
    setRsEvidenceMin(preset.rsEvidenceMin);
    setRsDaysMinPct(preset.rsDaysMinPct);
    setUpOnDownDaysMin(preset.upOnDownDaysMin);
    setScannerGroups(preset.scannerGroups.map((group) => [...group]));
    setActiveScannerGroupIndex(0);
    setSortBy(preset.sortBy);
    setSortDirection(preset.sortDirection);
    setViewMode(preset.viewMode);
    setSearch("");
    setScannerSearch("");
  };

  const currentFilterPreset = (): TopHitsFilterPreset => ({
    sectorFilter,
    eliteOnly,
    hasLeadershipScannerOnly,
    hasFundamentalQualityOnly,
    leaderRsOnly,
    leaderRsMin,
    leaderRsMax,
    rsEvidenceOnly,
    rsEvidenceMin,
    rsDaysMinPct,
    upOnDownDaysMin,
    scannerGroups: scannerGroups.map((group) => [...group]),
    sortBy,
    sortDirection,
    viewMode,
  });

  const updatePresetStore = (nextStore: TopHitsPresetStore, successMessage: string) => {
    setPresetStore(nextStore);
    try {
      localStorage.setItem(FILTER_PRESETS_STORAGE_KEY, JSON.stringify(nextStore));
      setPresetNotice(successMessage);
    } catch {
      setPresetNotice("Preset changed for this visit, but browser storage is unavailable.");
    }
  };

  useEffect(() => {
    const controller = new AbortController();
    setIsLoading(true);
    setNotice("");
    void fetchJson<ScannerTopHitsResponse>("/api/scanner-board/top-hits", { signal: controller.signal })
      .then(setPayload)
      .catch((error) => {
        if (controller.signal.aborted) return;
        setNotice(error instanceof Error ? error.message : "Failed to load scanner top hits.");
      })
      .finally(() => {
        if (!controller.signal.aborted) setIsLoading(false);
      });
    return () => controller.abort();
  }, [reloadKey]);

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
  const guruBoard = payload?.guru_board;
  const snapshot = payload?.snapshot;
  const sectors = useMemo(
    () => Array.from(new Set(rows.map((row) => row.sector).filter((sector) => sector && sector !== "Unknown sector"))).sort(),
    [rows],
  );
  const scannerOptions = useMemo(() => {
    const options = new Map(PINNED_SCANNER_OPTIONS.map((scanner) => [scanner.id, scanner.label]));
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
  const visibleScannerOptions = scannerOptions.filter((scanner) =>
    `${scanner.label} ${scannerNames[scanner.id] || ""}`.toLowerCase().includes(scannerSearch.trim().toLowerCase()));
  const selectedScannerIds = useMemo(() => Array.from(new Set(scannerGroups.flat())), [scannerGroups]);
  const activeScannerGroup = scannerGroups[activeScannerGroupIndex] ?? [];
  const nonEmptyScannerGroupCount = scannerGroups.filter((group) => group.length > 0).length;
  const normalizedLeaderRsRange = useMemo(() => normalizeRsRatingRange(leaderRsMin, leaderRsMax), [leaderRsMin, leaderRsMax]);
  const normalizedRsEvidenceMin = useMemo(() => normalizeBoundedInteger(rsEvidenceMin, 0, 9, 5), [rsEvidenceMin]);
  const normalizedRsDaysMinPct = useMemo(() => normalizeBoundedInteger(rsDaysMinPct, 0, 100, 60), [rsDaysMinPct]);
  const normalizedUpOnDownDaysMin = useMemo(() => normalizeBoundedInteger(upOnDownDaysMin, 0, 21, 3), [upOnDownDaysMin]);
  const guruRows = useMemo(() => {
    const query = search.trim().toLowerCase();
    return (guruBoard?.rows ?? []).filter((row) => {
      if (sectorFilter !== "all" && row.sector !== sectorFilter) return false;
      if (query && ![row.ticker, row.company, row.sector, row.industry, row.scanners.map((scanner) => scanner.label).join(" ")].join(" ").toLowerCase().includes(query)) return false;
      if (leaderRsOnly && !hasDailyRsRatingInRange(row, normalizedLeaderRsRange.min, normalizedLeaderRsRange.max)) return false;
      return true;
    });
  }, [guruBoard?.rows, leaderRsOnly, normalizedLeaderRsRange, search, sectorFilter]);

  const filteredRows = useMemo(() => {
    const query = search.trim().toLowerCase();
    let nextRows = rows;
    if (sectorFilter !== "all") {
      nextRows = nextRows.filter((row) => row.sector === sectorFilter);
    }
    if (query) {
      nextRows = nextRows.filter((row) =>
        [row.ticker, row.company, row.sector, row.industry, row.scanners.map((scanner) => `${scanner.label} ${scannerNames[normalizeScannerId(scanner.id)] || ""}`).join(" ")].join(" ").toLowerCase().includes(query),
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
    if (nonEmptyScannerGroupCount > 0) {
      nextRows = nextRows.filter((row) => hasScannerGroupSignals(row, scannerGroups));
    }
    return [...nextRows].sort((left, right) => compareRows(left, right, sortBy, sortDirection, {
      sectorLeaders: eliteOnly ? buildEliteLeaderMap(nextRows, (item) => normalizeSectorKey(item.sector)) : new Map<string, string>(),
      industryLeaders: eliteOnly ? buildEliteLeaderMap(nextRows, (item) => normalizeIndustryKey(item.industry)) : new Map<string, string>(),
    }));
  }, [eliteOnly, hasFundamentalQualityOnly, hasLeadershipScannerOnly, leaderRsOnly, nonEmptyScannerGroupCount, normalizedLeaderRsRange, normalizedRsDaysMinPct, normalizedRsEvidenceMin, normalizedUpOnDownDaysMin, rows, rsEvidenceOnly, scannerGroups, scannerNames, search, sectorFilter, sortBy, sortDirection]);

  useEffect(() => {
    setCurrentPage(1);
  }, [eliteOnly, hasFundamentalQualityOnly, hasLeadershipScannerOnly, leaderRsOnly, leaderRsMax, leaderRsMin, rsDaysMinPct, rsEvidenceMin, rsEvidenceOnly, scannerGroups, search, sectorFilter, sortBy, sortDirection, upOnDownDaysMin, viewMode]);

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

      <div className="top-hits-filter-toolbar">
        <span>{filteredRows.length} of {rows.length} tickers · {selectedScannerIds.length} scanner{selectedScannerIds.length === 1 ? "" : "s"} selected</span>
        <div className="top-hits-preset-controls">
          <select aria-label="Saved filter preset" value={selectedPresetName} onChange={(event) => {
            const name = event.target.value;
            setSelectedPresetName(name);
            applyFilterPreset(name ? presetStore.presets[name] : DEFAULT_TOP_HITS_FILTERS);
            setPresetNotice(name ? `${name} loaded.` : "Built-in default loaded.");
          }}>
            <option value="">No preset</option>
            {Object.keys(presetStore.presets).sort().map((name) => (
              <option key={name} value={name}>{name}{name === presetStore.defaultPresetName ? " · Default" : ""}</option>
            ))}
          </select>
          <button type="button" className="ghost-button" onClick={() => {
            const name = (window.prompt("Preset name", selectedPresetName) || "").trim().slice(0, 60);
            if (!name) return;
            const nextStore = { ...presetStore, presets: { ...presetStore.presets, [name]: currentFilterPreset() } };
            setSelectedPresetName(name);
            updatePresetStore(nextStore, `${name} saved.`);
          }}>Save preset</button>
          {selectedPresetName ? <button type="button" className="ghost-button" disabled={selectedPresetName === presetStore.defaultPresetName} onClick={() => {
            updatePresetStore({ ...presetStore, defaultPresetName: selectedPresetName }, `${selectedPresetName} will load by default.`);
          }}>{selectedPresetName === presetStore.defaultPresetName ? "Default" : "Set default"}</button> : null}
          {selectedPresetName ? <button type="button" className="ghost-button" onClick={() => {
            const remainingPresets = { ...presetStore.presets };
            delete remainingPresets[selectedPresetName];
            const nextStore = {
              presets: remainingPresets,
              defaultPresetName: presetStore.defaultPresetName === selectedPresetName ? "" : presetStore.defaultPresetName,
            };
            updatePresetStore(nextStore, `${selectedPresetName} deleted.`);
            setSelectedPresetName("");
            applyFilterPreset(EMPTY_TOP_HITS_FILTERS);
          }}>Delete</button> : null}
          <button type="button" className="ghost-button" onClick={() => {
            setSelectedPresetName("");
            applyFilterPreset(EMPTY_TOP_HITS_FILTERS);
            setPresetNotice("Filters cleared.");
          }}>Clear filters</button>
        </div>
      </div>
      {presetNotice ? <p className="panel-copy top-hits-preset-notice" role="status">{presetNotice}</p> : null}
      <section className="scanner-result-filter-grid top-hits-filters">
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
        <details className="panel top-hits-filter-group">
          <summary>Quality &amp; leadership · {[eliteOnly, hasLeadershipScannerOnly, hasFundamentalQualityOnly].filter(Boolean).length} active</summary>
          <div className="top-hits-filter-group-body">
            <label className="scanner-result-filter">
              <span>Elite Pick</span>
              <span className="scanner-result-check">
                <input type="checkbox" checked={eliteOnly} onChange={(event) => setEliteOnly(event.target.checked)} />
                <span>Only show elite candidates</span>
              </span>
              <span className="panel-copy">1D + 1W Strong Buy, and FA rank top 200 when present.</span>
            </label>
            <label className="scanner-result-filter">
              <span>Scanner Mix</span>
              <span className="scanner-result-check">
                <input type="checkbox" checked={hasLeadershipScannerOnly} onChange={(event) => setHasLeadershipScannerOnly(event.target.checked)} />
                <span>Has Trend Template, Weekly Candidate Pool, Sean BO, or Venu Scan</span>
              </span>
              <span className="panel-copy">Focus on names confirmed by your leadership-style scanners.</span>
            </label>
            <label className="scanner-result-filter">
              <span>Fundamental Quality</span>
              <span className="scanner-result-check">
                <input type="checkbox" checked={hasFundamentalQualityOnly} onChange={(event) => setHasFundamentalQualityOnly(event.target.checked)} />
                <span>Has Fundamental Quality</span>
              </span>
              <span className="panel-copy">Keep only names that also appear on the Fundamental Quality board.</span>
            </label>
          </div>
        </details>
        <details className="panel top-hits-filter-group">
          <summary>Relative strength · {[leaderRsOnly, rsEvidenceOnly].filter(Boolean).length} active</summary>
          <div className="top-hits-filter-group-body">
            <label className="scanner-result-filter">
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
                  aria-label="Minimum daily RS"
                />
                <input
                  type="number"
                  min={1}
                  max={99}
                  value={leaderRsMax}
                  onChange={(event) => setLeaderRsMax(event.target.value)}
                  placeholder="Max"
                  aria-label="Maximum daily RS"
                />
              </div>
              <span className="panel-copy">Daily RS ranges from 1 to 99; leave maximum blank for no upper limit.</span>
            </label>
            <label className="scanner-result-filter">
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
          </div>
        </details>
        <details className="panel top-hits-filter-group top-hits-scanner-group" open>
          <summary>Scanners · {selectedScannerIds.length} selected · {nonEmptyScannerGroupCount} group{nonEmptyScannerGroupCount === 1 ? "" : "s"}</summary>
          <div className="scanner-result-filter">
            <input aria-label="Find scanners" placeholder="Find a scanner…" value={scannerSearch} onChange={(event) => setScannerSearch(event.target.value)} />
            <span className="panel-copy">A ticker may match any scanner inside a group (OR), and must match every non-empty group (AND).</span>
            <div className="scanner-filter-group-tabs" role="tablist" aria-label="Scanner filter groups">
              {scannerGroups.map((group, index) => <div key={index} className="scanner-filter-group-tab">
                <button type="button" role="tab" aria-selected={index === activeScannerGroupIndex}
                  className={`scanner-result-view-chip${index === activeScannerGroupIndex ? " is-active" : ""}`}
                  onClick={() => setActiveScannerGroupIndex(index)}>
                  Group {index + 1} · {group.length}
                </button>
                {scannerGroups.length > 1 ? <button type="button" className="scanner-filter-group-remove"
                  aria-label={`Remove scanner group ${index + 1}`}
                  onClick={() => {
                    setScannerGroups((current) => current.filter((_, groupIndex) => groupIndex !== index));
                    setActiveScannerGroupIndex((current) => Math.max(0, current > index ? current - 1 : Math.min(current, scannerGroups.length - 2)));
                  }}>×</button> : null}
              </div>)}
              <button type="button" className="ghost-button" onClick={() => {
                setScannerGroups((current) => [...current, []]);
                setActiveScannerGroupIndex(scannerGroups.length);
              }}>+ AND group</button>
            </div>
            <span className="panel-copy">Choose scanners for Group {activeScannerGroupIndex + 1}. Multiple choices in this group use OR.</span>
            {nonEmptyScannerGroupCount > 0 ? <div className="scanner-filter-expression" aria-label="Scanner filter expression">
              {scannerGroups.map((group, groupIndex) => ({ group, groupIndex })).filter(({ group }) => group.length > 0).map(({ group, groupIndex }, clauseIndex) => <div key={groupIndex} className="scanner-filter-expression-row">
                <strong>{clauseIndex > 0 ? "AND " : ""}Group {groupIndex + 1}</strong>
                <div className="scanner-top-hit-pills">
                  {group.map((id) => <button key={id} type="button" className="scanner-card-pill is-selected"
                    onClick={() => setScannerGroups((current) => current.map((item, index) => index === groupIndex ? item.filter((scannerId) => scannerId !== id) : item))}
                    aria-label={`Remove ${scannerNames[id] || scannerOptions.find((scanner) => scanner.id === id)?.label || id} from group ${groupIndex + 1}`}>
                    {scannerNames[id] || scannerOptions.find((scanner) => scanner.id === id)?.label || id} ×
                  </button>)}
                </div>
              </div>)}
            </div> : null}
            <div className="scanner-top-hit-filter-list">
              {visibleScannerOptions.map((scanner) => (
                <label key={scanner.id} className={`scanner-result-check${activeScannerGroup.includes(scanner.id) ? " is-selected" : ""}`}>
                  <input type="checkbox" checked={activeScannerGroup.includes(scanner.id)} onChange={(event) => {
                    setScannerGroups((current) => current.map((group, index) => index === activeScannerGroupIndex
                      ? event.target.checked ? [...group, scanner.id] : group.filter((id) => id !== scanner.id)
                      : group.filter((id) => id !== scanner.id)));
                  }} />
                  <span title={scanner.label}>{scannerNames[scanner.id] || scanner.label}{selectedScannerIds.includes(scanner.id) && !activeScannerGroup.includes(scanner.id) ? " · another group" : ""}</span>
                </label>
              ))}
            </div>
            {visibleScannerOptions.length === 0 ? <span className="panel-copy">No scanners match your search.</span> : null}
            <details>
              <summary>Customize scanner names</summary>
              <p className="panel-copy">Display names are saved in this browser. Leave blank to restore the original name.</p>
              <div className="scanner-top-hit-filter-list">
                {visibleScannerOptions.map((scanner) => <label key={scanner.id}>
                  <span>{scanner.label}</span>
                  <input aria-label={`Display name for ${scanner.label}`} maxLength={60} placeholder={scanner.label}
                    defaultValue={scannerNames[scanner.id] || ""}
                    onBlur={(event) => {
                      const next = { ...scannerNames };
                      const name = event.target.value.trim();
                      if (name) next[scanner.id] = name; else delete next[scanner.id];
                      setScannerNames(next);
                      try { localStorage.setItem("top-hits-scanner-names", JSON.stringify(next)); setNameNotice(""); }
                      catch { setNameNotice("Names changed for this visit, but browser storage is unavailable."); }
                    }} />
                </label>)}
              </div>
              {nameNotice ? <p role="status">{nameNotice}</p> : null}
            </details>
          </div>
        </details>
        <div className="scanner-result-filter panel scanner-result-filter-actions">
          <span>Board Snapshot</span>
          <div className="scanner-result-view-actions">
            <Link className="ghost-button" to="/scanner">
              Back to board
            </Link>
          </div>
          <span className="panel-copy">Updated {formatLocalDateTime(payload?.latest_update_at)}.</span>
          {snapshot?.freshness === "stale" ? <span className="panel-copy earnings-console-note">Showing the latest completed snapshot while a newer market day is pending.</span> : null}
          {snapshot?.freshness === "missing" ? <span className="panel-copy earnings-console-note">No completed Top Hits snapshot is available yet. Run “Build Top Hits Snapshot” after the scanner batch.</span> : null}
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
            <button
              className={`scanner-result-view-chip${viewMode === "guru" ? " is-active" : ""}`}
              type="button"
              onClick={() => setViewMode("guru")}
            >
              Guru Board
            </button>
            <button
              className={`scanner-result-view-chip${viewMode === "position" ? " is-active" : ""}`}
              type="button"
              onClick={() => setViewMode("position")}
            >
              Position Map
            </button>
          </div>
          <span className="panel-copy">Guru Board keeps scanner overlap visible; unavailable strategies stay clearly marked.</span>
        </div>
      </section>

      <section className="scanner-result-table-shell panel">
        {isLoading && !payload ? <LoadingBlock label="Loading scanner top hits…" /> : null}
        {notice ? <p className="panel-copy">{notice} <button className="ghost-button" type="button" onClick={() => setReloadKey((value) => value + 1)}>Retry</button></p> : null}
        {!notice && myPicksNotice ? <p className="panel-copy earnings-console-note">{myPicksNotice}</p> : null}
        {!isLoading && !notice && ((viewMode === "guru" || viewMode === "position") ? guruRows.length === 0 : filteredRows.length === 0) ? <p className="panel-copy">No tickers match current filters.</p> : null}
        {((viewMode === "guru" || viewMode === "position") ? Boolean(guruBoard) : filteredRows.length > 0) ? (
          <>
            {viewMode === "guru" ? (
              <GuruBoard rows={guruRows} definitions={guruBoard?.definitions ?? []} totalScannerMatches={guruBoard?.total_scanner_matches ?? 0} confluenceTickerCount={guruBoard?.confluence_ticker_count ?? 0} />
            ) : viewMode === "position" ? (
              <PositionMap rows={guruRows} />
            ) : <>
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
                    selectedScannerIds={selectedScannerIds} scannerNames={scannerNames}
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
                    {canManageMyPicks ? <th className="pinned-pick">My Pick</th> : null}
                    <th className="pinned-ticker">{renderSortButton("Ticker", "ticker", sortBy, sortDirection, setSortBy, setSortDirection)}</th>
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
                        <td className="pinned-pick" data-label="My Pick">
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
                      <td className="pinned-ticker" data-label="Ticker">
                        <div className="scanner-result-company">
                          <Link className="scanner-result-symbol" to={buildChartHref(row.ticker)} onMouseEnter={preloadChartsPage} onFocus={preloadChartsPage}>
                            {row.ticker}
                          </Link>
                          <span>{row.company || row.industry || "-"}</span>
                        </div>
                      </td>
                      <td data-label="Hits">
                        <strong>{formatCount(row.scanner_count)}</strong>
                      </td>
                      <td data-label="Scanners">
                        <ScannerBadges scanners={row.scanners} selectedScannerIds={selectedScannerIds} scannerNames={scannerNames} />
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
            </>}
          </>
        ) : null}
      </section>
    </div>
  );
}

function ScannerBadges({ scanners, selectedScannerIds, scannerNames }: { scanners: ScannerTopHitRow["scanners"]; selectedScannerIds: string[]; scannerNames: Record<string, string> }) {
  const [expanded, setExpanded] = useState(false);
  const selected = new Set(selectedScannerIds);
  const ordered = [...scanners].sort((a, b) => Number(selected.has(normalizeScannerId(b.id))) - Number(selected.has(normalizeScannerId(a.id))));
  const hiddenSelected = ordered.slice(3).filter((scanner) => selected.has(normalizeScannerId(scanner.id))).length;
  return (
    <div className="top-hits-scanner-badges">
      <div className="scanner-top-hit-pills">
        {(expanded ? ordered : ordered.slice(0, 3)).map((scanner) => {
          const checked = selected.has(normalizeScannerId(scanner.id));
          return <Link key={scanner.id} className={`scanner-card-pill${checked ? " is-selected" : ""}`} title={scanner.label} to={`/scanner/${encodeURIComponent(scanner.id)}`}>
            {checked ? <span aria-label="Selected scanner">✓ </span> : null}{scannerNames[normalizeScannerId(scanner.id)] || scanner.label}
          </Link>;
        })}
      </div>
      {ordered.length > 3 ? <button type="button" className="top-hits-scanner-toggle" aria-expanded={expanded} onClick={() => setExpanded(!expanded)}>
        {expanded ? "Show fewer" : `+${ordered.length - 3} more${hiddenSelected ? ` (${hiddenSelected} selected)` : ""}`}
      </button> : null}
    </div>
  );
}

function GuruBoard({
  rows,
  definitions,
  totalScannerMatches,
  confluenceTickerCount,
}: {
  rows: ScannerTopHitRow[];
  definitions: NonNullable<ScannerTopHitsResponse["guru_board"]>["definitions"];
  totalScannerMatches: number;
  confluenceTickerCount: number;
}) {
  const [visibleCounts, setVisibleCounts] = useState<Record<string, number>>({});
  return (
    <section className="guru-board" aria-label="Guru scanner board">
      <div className="guru-board-summary">
        <span><strong>{formatCount(rows.length)}</strong> Guru names</span>
        <span><strong>{formatCount(totalScannerMatches)}</strong> scanner matches</span>
        <span><strong>{formatCount(confluenceTickerCount)}</strong> confluence names</span>
      </div>
      <div className="guru-board-scroll">
        {definitions.map((definition) => {
          const columnRows = rows.filter((row) => row.scanners.some((scanner) => normalizeScannerId(scanner.id) === definition.id));
          const visibleCount = visibleCounts[definition.id] ?? GURU_COLUMN_PAGE_SIZE;
          const remainingCount = Math.max(0, columnRows.length - visibleCount);
          return (
            <article className={`guru-column is-${definition.accent}`} key={definition.id}>
              <header>
                <strong>{formatCount(columnRows.length)}</strong>
                <span>{definition.label}</span>
              </header>
              {!definition.available ? <p className="guru-column-unavailable">Rules not configured yet</p> : null}
              <div className="guru-column-cards">
                {columnRows.slice(0, visibleCount).map((row) => <GuruTickerCard key={row.ticker} row={row} />)}
              </div>
              {remainingCount > 0 ? <button className="ghost-button guru-column-load-more" type="button" onClick={() => setVisibleCounts((current) => ({ ...current, [definition.id]: visibleCount + GURU_COLUMN_PAGE_SIZE }))}>
                Load 30 more ({formatCount(remainingCount)} remaining)
              </button> : null}
            </article>
          );
        })}
      </div>
      <p className="panel-copy guru-board-note">A ticker can appear in multiple columns. Strike Zone is chart-review guidance, not an automatic buy signal.</p>
    </section>
  );
}

function GuruTickerCard({ row }: { row: ScannerTopHitRow }) {
  const stage = row.stage_analysis?.alias || "--";
  const strike = row.strike_zone?.label || "Context";
  const strikeTone = row.strike_zone?.state || "context";
  const atr = row.atr_to_sma50 == null ? "--" : `${row.atr_to_sma50 >= 0 ? "+" : ""}${row.atr_to_sma50.toFixed(1)} ATR`;
  const earnings = row.earnings_days == null ? "Earnings TBD" : row.earnings_days === 0 ? "Earnings today" : `Earnings ${row.earnings_days}d`;
  const rmv = row.rmv ? `RMV ${row.rmv.value.toFixed(0)} · R${row.rmv.rank || "–"}` : null;
  return (
    <Link className="guru-ticker-card" to={buildChartHref(row.ticker)} title={`${row.ticker}: ${row.strike_zone?.reason || ""}`}>
      <div className="guru-ticker-main">
        <strong>{row.ticker}</strong>
        <span className={row.change_pct != null && row.change_pct < 0 ? "ticker-change down" : "ticker-change up"}>{row.change_pct == null ? "--" : `${row.change_pct >= 0 ? "+" : ""}${row.change_pct.toFixed(1)}%`}</span>
      </div>
      <div className="guru-ticker-badges">
        <span title="Guru scanner overlap">{row.scanner_count}×</span>
        <span title="Weinstein stage">{stage}</span>
        <span title="Daily RS">RS {row.daily_rs_rating == null ? "--" : Math.round(row.daily_rs_rating)}</span>
        {rmv ? <span title="Relative Measured Volatility tightness rank">{rmv}</span> : null}
      </div>
      <div className="guru-ticker-context">
        <span title="ATR distance from SMA50">📏 {atr}</span>
        <span title={earnings}>📅 {row.earnings_days == null ? "TBD" : `${row.earnings_days}d`}</span>
        <span className={`guru-strike is-${strikeTone}`}>⚾ {strike}</span>
      </div>
    </Link>
  );
}

const POSITION_BUCKETS = [
  ["extended", "Extended"],
  ["above_ema10", "Above EMA10"],
  ["ema10_ema21", "EMA10–EMA21"],
  ["ema21_sma50", "EMA21–SMA50"],
  ["below_sma50_above_sma200", "Below SMA50 · Above SMA200"],
  ["below_sma200", "Below SMA200"],
  ["no_data", "No Data"],
] as const;

function PositionMap({ rows }: { rows: ScannerTopHitRow[] }) {
  return (
    <section className="guru-board" aria-label="Market position map">
      <div className="guru-board-summary"><span>Each Guru ticker appears once, based on its latest moving-average position.</span></div>
      <div className="guru-board-scroll position-map-scroll">
        {POSITION_BUCKETS.map(([id, label]) => {
          const bucketRows = rows.filter((row) => (row.position_bucket || "no_data") === id);
          return <article className="guru-column position-map-column" key={id}>
            <header><strong>{formatCount(bucketRows.length)}</strong><span>{label}</span></header>
            <div className="guru-column-cards">{bucketRows.slice(0, 30).map((row) => <GuruTickerCard key={row.ticker} row={row} />)}</div>
          </article>;
        })}
      </div>
    </section>
  );
}

function ScannerTopHitChartCard({
  row,
  selectedScannerIds,
  scannerNames,
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
  selectedScannerIds: string[];
  scannerNames: Record<string, string>;
  boardSignalDate: string | null | undefined;
  canManageMyPicks: boolean;
  alreadyMyPick: boolean;
  savingMyPick: boolean;
  chartPayload: WatchlistChartResponse | null | undefined;
  chartError: string | undefined;
  isChartLoading: boolean;
  onAddToMyPicks: (ticker: string) => Promise<void>;
}) {
  const chartCandles = buildChartCandles(chartPayload);
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
            <Link className="scanner-result-symbol" to={buildChartHref(row.ticker)} onMouseEnter={preloadChartsPage} onFocus={preloadChartsPage}>
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
      <ScannerBadges scanners={row.scanners} selectedScannerIds={selectedScannerIds} scannerNames={scannerNames} />
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
        <Link to={buildChartHref(row.ticker)} onMouseEnter={preloadChartsPage} onFocus={preloadChartsPage}>Analyze Full Chart</Link>
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

function hasScannerGroupSignals(row: ScannerTopHitRow, scannerGroups: string[][]) {
  const rowScannerIds = new Set(row.scanners.map((scanner) => normalizeScannerId(scanner.id)).filter(Boolean));
  return scannerGroups.filter((group) => group.length > 0).every((group) => group.some((scannerId) => rowScannerIds.has(scannerId)));
}

function loadTopHitsPresetStore(): TopHitsPresetStore {
  try {
    const parsed: unknown = JSON.parse(localStorage.getItem(FILTER_PRESETS_STORAGE_KEY) || "{}");
    if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) return { presets: {}, defaultPresetName: "" };
    const rawStore = parsed as Record<string, unknown>;
    const rawPresets = rawStore.presets;
    if (!rawPresets || typeof rawPresets !== "object" || Array.isArray(rawPresets)) return { presets: {}, defaultPresetName: "" };
    const presets = Object.fromEntries(
      Object.entries(rawPresets).map(([name, value]) => [name, normalizeTopHitsFilterPreset(value)]),
    );
    const defaultPresetName = typeof rawStore.defaultPresetName === "string" && presets[rawStore.defaultPresetName]
      ? rawStore.defaultPresetName : "";
    return { presets, defaultPresetName };
  } catch {
    return { presets: {}, defaultPresetName: "" };
  }
}

function normalizeTopHitsFilterPreset(value: unknown): TopHitsFilterPreset {
  const preset = value && typeof value === "object" && !Array.isArray(value)
    ? value as Partial<TopHitsFilterPreset> : {};
  const scannerGroups = Array.isArray(preset.scannerGroups)
    ? preset.scannerGroups
      .filter(Array.isArray)
      .map((group) => Array.from(new Set(group.filter((id): id is string => typeof id === "string" && id.length > 0))))
    : [[]];
  return {
    sectorFilter: typeof preset.sectorFilter === "string" ? preset.sectorFilter : "all",
    eliteOnly: preset.eliteOnly === true,
    hasLeadershipScannerOnly: preset.hasLeadershipScannerOnly === true,
    hasFundamentalQualityOnly: preset.hasFundamentalQualityOnly === true,
    leaderRsOnly: preset.leaderRsOnly === true,
    leaderRsMin: typeof preset.leaderRsMin === "string" ? preset.leaderRsMin : "90",
    leaderRsMax: typeof preset.leaderRsMax === "string" ? preset.leaderRsMax : "",
    rsEvidenceOnly: preset.rsEvidenceOnly === true,
    rsEvidenceMin: typeof preset.rsEvidenceMin === "string" ? preset.rsEvidenceMin : "5",
    rsDaysMinPct: typeof preset.rsDaysMinPct === "string" ? preset.rsDaysMinPct : "60",
    upOnDownDaysMin: typeof preset.upOnDownDaysMin === "string" ? preset.upOnDownDaysMin : "3",
    scannerGroups: scannerGroups.length > 0 ? scannerGroups : [[]],
    sortBy: typeof preset.sortBy === "string" ? preset.sortBy as SortKey : "hits",
    sortDirection: preset.sortDirection === "asc" ? "asc" : "desc",
    viewMode: preset.viewMode === "charts" ? "charts" : "list",
  };
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
