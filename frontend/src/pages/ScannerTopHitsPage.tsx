import { useEffect, useMemo, useRef, useState, type DragEvent as ReactDragEvent, type MouseEvent as ReactMouseEvent, type PointerEvent as ReactPointerEvent, type ReactNode } from "react";
import { Link, useSearchParams } from "react-router-dom";
import { preloadChartsPage } from "../App";
import { useAuth } from "../auth/AuthContext";
import { LoadingBlock } from "../components/LoadingBlock";
import { PaginationControls } from "../components/PaginationControls";
import { ScannerMiniChart } from "../components/ScannerMiniChart";
import { fetchJson } from "../lib/api";
import { buildChartCandles, buildExponentialMovingAverage, buildSimpleMovingAverage } from "../lib/chartData";
import { formatCount, formatLocalDate, formatLocalDateTime, humanizePositionAction, humanizePositionExtension, humanizePositionTrend, toneForPositionAction } from "../lib/format";
import { resolveRsMomentumSignal } from "../lib/rsMomentum";
import type { MomentumEtfPortfolioRow, MomentumEtfPortfoliosResponse, MyPickRow, MyPicksContextResponse, ScannerTopHitRow, ScannerTopHitsResponse, TechnicalIndicatorRatingCell, WatchlistChartResponse } from "../lib/types";

type SortKey = "hits" | "ticker" | "sector" | "sectorTopHit" | "industryTopHit" | "close" | "change" | "from52wLow" | "bollinger" | "rsEvidence" | "rsDays" | "rsPhaseDays" | "upOnDownDays" | "rs" | "dailyRs" | "rs3m" | "rs6m" | "rsMomentum" | "ta" | "fa" | "decision" | "decisionScore";
type SortDirection = "asc" | "desc";
type ViewMode = "list" | "charts" | "guru" | "sectors" | "position" | "etf-portfolios";
type ChartGridColumns = 2 | 3 | 4;
type ChartRange = "3m" | "6m" | "1y";
type ChartType = "candles" | "bars" | "line";
type ChartWorkspace = {
  gridColumns: ChartGridColumns;
  range: ChartRange;
  chartType: ChartType;
  showVolume: boolean;
  showEma8: boolean;
  showEma21: boolean;
  showEma60: boolean;
  showSma50: boolean;
};
type GuruSortKey = "default" | "stage" | "rs" | "rsPhase" | "strike";
type BoardChartSelection = {
  ticker: string;
  company: string;
  sector: string;
  close: number | null;
  changePct: number | null;
  dailyRs: number | null;
  stage: string;
  strikeLabel: string;
  strikeState: string;
  strikeScore: number | null;
  scannerSummary: string;
  context?: string;
};
type TopHitsFilterPreset = {
  sectorFilter: string;
  sizePriceFloorOnly: boolean;
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
const CHART_WORKSPACE_STORAGE_KEY = "top-hits-chart-workspace";
const DEFAULT_CHART_WORKSPACE: ChartWorkspace = {
  gridColumns: 2,
  range: "6m",
  chartType: "candles",
  showVolume: true,
  showEma8: true,
  showEma21: true,
  showEma60: false,
  showSma50: false,
};
const GURU_COLUMN_PAGE_SIZE = 30;
const MIN_TOP_HITS_MARKET_CAP = 1_000_000_000;
const MIN_TOP_HITS_PRICE = 5;
const MOMENTUM_ETF_ACCENTS: Record<string, string> = { FMTM: "amber", SPMO: "teal", PTF: "blue", FFTY: "cyan" };
const LEADERSHIP_SCANNER_IDS = new Set(["trend_template", "weekly_candidate_pool", "best_winners", "qullamaggie", "sean_breakout", "venu_scanner"]);
const PINNED_SCANNER_OPTIONS = [
  { id: "weekly_candidate_pool", label: "Weekly Candidate Pool" },
  { id: "best_winners", label: "Best Winners" },
  { id: "qullamaggie", label: "Qullamaggie" },
];
const FILTER_PRESETS_STORAGE_KEY = "top-hits-filter-presets";
const EMPTY_TOP_HITS_FILTERS: TopHitsFilterPreset = {
  sectorFilter: "all",
  sizePriceFloorOnly: false,
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
  sizePriceFloorOnly: true,
  scannerGroups: [
    ["qullamaggie", "weekly_candidate_pool", "kai_s2"],
    ["venu_scanner", "trend_template", "one_year_winners", "finviz_smallover_sales_growth_trend", "sean_breakout"],
  ],
};
const BUILT_IN_LEADERSHIP_PRESET_ID = "built-in:leadership-trend";
const BUILT_IN_ENTRY_CONFLUENCE_PRESET_ID = "built-in:entry-confluence";
const ENTRY_CONFLUENCE_TOP_HITS_FILTERS: TopHitsFilterPreset = {
  ...DEFAULT_TOP_HITS_FILTERS,
  scannerGroups: [
    [
      "qullamaggie", "weekly_candidate_pool", "best_winners", "kai_s2", "kai_s1", "one_year_winners", "venu_scanner",
      "trend_template", "finviz_smallover_sales_growth_trend", "sean_gap_up", "sean_breakout",
      "fundamental_quality", "stockbee_momentum_burst", "daily_rs_new_high", "eight_week_100_runup",
      "stockbee_4pct_daily_movers", "stockbee_20pct_weekly_movers", "stockbee_9m_movers",
    ],
    ["ftd_sweep", "wyckoff_buy_signal", "cup_detection", "macd_golden_cross", "elite_rs_hv1", "elite_rs_recent_peg"],
    ["rti", "rmv_tightness", "vcs_critical_tightness"],
  ],
};
const BUILT_IN_TOP_HITS_PRESETS: Record<string, { label: string; filters: TopHitsFilterPreset }> = {
  [BUILT_IN_LEADERSHIP_PRESET_ID]: { label: "Leadership + Trend", filters: DEFAULT_TOP_HITS_FILTERS },
  [BUILT_IN_ENTRY_CONFLUENCE_PRESET_ID]: { label: "Momentum + Entry Confluence", filters: ENTRY_CONFLUENCE_TOP_HITS_FILTERS },
};

export function ScannerTopHitsPage() {
  const auth = useAuth();
  const [searchParams, setSearchParams] = useSearchParams();
  const [payload, setPayload] = useState<ScannerTopHitsResponse | null>(null);
  const [isLoading, setIsLoading] = useState(true);
  const [reloadKey, setReloadKey] = useState(0);
  const [notice, setNotice] = useState("");
  const [myPicksNotice, setMyPicksNotice] = useState("");
  const [presetStore, setPresetStore] = useState<TopHitsPresetStore>(loadTopHitsPresetStore);
  const initialPresetName = presetStore.defaultPresetName || BUILT_IN_LEADERSHIP_PRESET_ID;
  const initialFilters = resolveTopHitsPreset(initialPresetName, presetStore) ?? DEFAULT_TOP_HITS_FILTERS;
  const [selectedPresetName, setSelectedPresetName] = useState(initialPresetName);
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
  const [sizePriceFloorOnly, setSizePriceFloorOnly] = useState(initialFilters.sizePriceFloorOnly);
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
  const [guruScannerGroupsOnly, setGuruScannerGroupsOnly] = useState(false);
  const [sortBy, setSortBy] = useState<SortKey>(initialFilters.sortBy);
  const [sortDirection, setSortDirection] = useState<SortDirection>(initialFilters.sortDirection);
  const [viewMode, setViewMode] = useState<ViewMode>(initialFilters.viewMode);
  const [chartWorkspace, setChartWorkspace] = useState<ChartWorkspace>(loadChartWorkspace);
  const [currentPage, setCurrentPage] = useState(1);
  const [myPickTickers, setMyPickTickers] = useState<Set<string>>(new Set());
  const [myPickIdsByTicker, setMyPickIdsByTicker] = useState<Record<string, number>>({});
  const [savingMyPickTickers, setSavingMyPickTickers] = useState<Record<string, boolean>>({});
  const [chartPayloads, setChartPayloads] = useState<Record<string, WatchlistChartResponse | null | undefined>>({});
  const [chartErrors, setChartErrors] = useState<Record<string, string>>({});
  const [chartLoadingTickers, setChartLoadingTickers] = useState<Record<string, boolean>>({});
  const [etfPayload, setEtfPayload] = useState<MomentumEtfPortfoliosResponse | null>(null);
  const [etfLoading, setEtfLoading] = useState(false);
  const [etfNotice, setEtfNotice] = useState("");
  const [selectedEtf, setSelectedEtf] = useState("all");
  const [etfTopHitsOnly, setEtfTopHitsOnly] = useState(false);
  const [etfMinOverlap, setEtfMinOverlap] = useState(1);
  const [selectedBoardTicker, setSelectedBoardTicker] = useState(() => searchParams.get("ticker")?.trim().toUpperCase() || "");
  const canManageMyPicks = auth.hasCapability("manage_exclusions");

  const selectBoardTicker = (ticker: string) => {
    const nextTicker = ticker.trim().toUpperCase();
    if (!nextTicker) return;
    setSelectedBoardTicker(nextTicker);
    setSearchParams((current) => {
      const next = new URLSearchParams(current);
      next.set("ticker", nextTicker);
      return next;
    }, { replace: true });
  };

  useEffect(() => {
    const tickerFromUrl = searchParams.get("ticker")?.trim().toUpperCase() || "";
    if (tickerFromUrl && tickerFromUrl !== selectedBoardTicker) {
      setSelectedBoardTicker(tickerFromUrl);
    }
  }, [searchParams, selectedBoardTicker]);

  const applyFilterPreset = (preset: TopHitsFilterPreset) => {
    setSectorFilter(preset.sectorFilter);
    setSizePriceFloorOnly(preset.sizePriceFloorOnly);
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
    sizePriceFloorOnly,
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

  const updateChartWorkspace = (patch: Partial<ChartWorkspace>) => {
    setChartWorkspace((current) => {
      const next = { ...current, ...patch };
      try {
        localStorage.setItem(CHART_WORKSPACE_STORAGE_KEY, JSON.stringify(next));
      } catch {
        // Keep the selection for this visit when browser storage is unavailable.
      }
      return next;
    });
  };

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
    if (viewMode !== "etf-portfolios" || etfPayload || etfLoading) return;
    const controller = new AbortController();
    setEtfLoading(true);
    setEtfNotice("");
    void fetchJson<MomentumEtfPortfoliosResponse>("/api/scanner-board/momentum-etf-portfolios", { signal: controller.signal })
      .then(setEtfPayload)
      .catch((error) => {
        if (!controller.signal.aborted) setEtfNotice(error instanceof Error ? error.message : "Failed to load momentum ETF portfolios.");
      })
      .finally(() => {
        if (!controller.signal.aborted) setEtfLoading(false);
      });
    return () => controller.abort();
  }, [etfPayload, viewMode]);

  useEffect(() => {
    if (!canManageMyPicks) {
      setMyPickTickers(new Set());
      setMyPickIdsByTicker({});
      return;
    }
    void fetchJson<MyPicksContextResponse>("/api/admin/my-picks")
      .then((response) => {
        setMyPickTickers(new Set(response.rows.map((row) => row.ticker.toUpperCase())));
        setMyPickIdsByTicker(Object.fromEntries(response.rows.map((row) => [row.ticker.toUpperCase(), row.id])));
      })
      .catch(() => {
        setMyPickTickers(new Set());
        setMyPickIdsByTicker({});
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
  const scannerIdsByTicker = useMemo(
    () => new Map(rows.map((row) => [row.ticker, row.scanners.map((scanner) => normalizeScannerId(scanner.id))])),
    [rows],
  );
  const normalizedLeaderRsRange = useMemo(() => normalizeRsRatingRange(leaderRsMin, leaderRsMax), [leaderRsMin, leaderRsMax]);
  const normalizedRsEvidenceMin = useMemo(() => normalizeBoundedInteger(rsEvidenceMin, 0, 9, 5), [rsEvidenceMin]);
  const normalizedRsDaysMinPct = useMemo(() => normalizeBoundedInteger(rsDaysMinPct, 0, 100, 60), [rsDaysMinPct]);
  const normalizedUpOnDownDaysMin = useMemo(() => normalizeBoundedInteger(upOnDownDaysMin, 0, 21, 3), [upOnDownDaysMin]);
  const guruRows = useMemo(() => {
    const query = search.trim().toLowerCase();
    return (guruBoard?.rows ?? []).filter((row) => {
      if (sizePriceFloorOnly && !meetsSizePriceFloor(row)) return false;
      if (sectorFilter !== "all" && row.sector !== sectorFilter) return false;
      if (query && ![row.ticker, row.company, row.sector, row.industry, row.scanners.map((scanner) => scanner.label).join(" ")].join(" ").toLowerCase().includes(query)) return false;
      if (leaderRsOnly && !hasDailyRsRatingInRange(row, normalizedLeaderRsRange.min, normalizedLeaderRsRange.max)) return false;
      return true;
    });
  }, [guruBoard?.rows, leaderRsOnly, normalizedLeaderRsRange, search, sectorFilter, sizePriceFloorOnly]);
  const visibleGuruRows = useMemo(() => {
    if (!guruScannerGroupsOnly || nonEmptyScannerGroupCount === 0) return guruRows;
    return guruRows.filter((row) => hasScannerGroupSignals(row, scannerGroups, scannerIdsByTicker.get(row.ticker)));
  }, [guruRows, guruScannerGroupsOnly, nonEmptyScannerGroupCount, scannerGroups, scannerIdsByTicker]);
  const isColumnView = viewMode === "guru" || viewMode === "sectors" || viewMode === "position" || viewMode === "etf-portfolios";
  const selectedBoardSelection = useMemo(() => {
    const guruRow = visibleGuruRows.find((row) => row.ticker === selectedBoardTicker)
      ?? guruRows.find((row) => row.ticker === selectedBoardTicker)
      ?? rows.find((row) => row.ticker === selectedBoardTicker);
    if (guruRow) return toBoardChartSelection(guruRow);
    const etfRow = (etfPayload?.rows ?? []).find((row) => row.ticker === selectedBoardTicker);
    return etfRow ? toEtfBoardChartSelection(etfRow) : null;
  }, [etfPayload?.rows, guruRows, rows, selectedBoardTicker, visibleGuruRows]);

  useEffect(() => {
    if (viewMode === "etf-portfolios" || !isColumnView || visibleGuruRows.length === 0) return;
    if (!visibleGuruRows.some((row) => row.ticker === selectedBoardTicker)) {
      selectBoardTicker(visibleGuruRows[0].ticker);
    }
  }, [isColumnView, selectedBoardTicker, viewMode, visibleGuruRows]);

  useEffect(() => {
    if (!isColumnView || !selectedBoardTicker || chartPayloads[selectedBoardTicker] !== undefined || chartLoadingTickers[selectedBoardTicker]) return;
    let ignore = false;
    setChartLoadingTickers((current) => ({ ...current, [selectedBoardTicker]: true }));
    void fetchJson<WatchlistChartResponse>(`/api/charts/${encodeURIComponent(selectedBoardTicker)}/preview?period=18mo`)
      .then((chartPayload) => {
        if (ignore) return;
        setChartPayloads((current) => ({ ...current, [selectedBoardTicker]: chartPayload }));
        setChartErrors((current) => {
          const next = { ...current };
          delete next[selectedBoardTicker];
          return next;
        });
      })
      .catch((error) => {
        if (ignore) return;
        setChartPayloads((current) => ({ ...current, [selectedBoardTicker]: null }));
        setChartErrors((current) => ({ ...current, [selectedBoardTicker]: error instanceof Error ? error.message : "Failed to load chart." }));
      })
      .finally(() => {
        setChartLoadingTickers((current) => {
          const next = { ...current };
          delete next[selectedBoardTicker];
          return next;
        });
      });
    return () => { ignore = true; };
  }, [isColumnView, selectedBoardTicker]);

  const filteredRows = useMemo(() => {
    const query = search.trim().toLowerCase();
    let nextRows = rows;
    if (sizePriceFloorOnly) {
      nextRows = nextRows.filter(meetsSizePriceFloor);
    }
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
  }, [eliteOnly, hasFundamentalQualityOnly, hasLeadershipScannerOnly, leaderRsOnly, nonEmptyScannerGroupCount, normalizedLeaderRsRange, normalizedRsDaysMinPct, normalizedRsEvidenceMin, normalizedUpOnDownDaysMin, rows, rsEvidenceOnly, scannerGroups, scannerNames, search, sectorFilter, sizePriceFloorOnly, sortBy, sortDirection]);

  useEffect(() => {
    setCurrentPage(1);
  }, [chartWorkspace.gridColumns, eliteOnly, hasFundamentalQualityOnly, hasLeadershipScannerOnly, leaderRsOnly, leaderRsMax, leaderRsMin, rsDaysMinPct, rsEvidenceMin, rsEvidenceOnly, scannerGroups, search, sectorFilter, sizePriceFloorOnly, sortBy, sortDirection, upOnDownDaysMin, viewMode]);

  const pageSize = viewMode === "charts" ? chartPageSize(chartWorkspace.gridColumns) : LIST_PAGE_SIZE;
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
      const response = await fetchJson<{ ok: boolean; pick: MyPickRow }>("/api/admin/my-picks", {
        method: "POST",
        body: JSON.stringify({
          ticker: normalizedTicker,
          notes: "Added from scanner top hits.",
        }),
      });
      setMyPickTickers((current) => new Set([...current, normalizedTicker]));
      setMyPickIdsByTicker((current) => ({ ...current, [normalizedTicker]: response.pick.id }));
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

  const handleToggleMyPick = async (ticker: string) => {
    const normalizedTicker = ticker.trim().toUpperCase();
    if (!normalizedTicker || savingMyPickTickers[normalizedTicker]) return;
    if (!myPickTickers.has(normalizedTicker)) {
      await handleAddToMyPicks(normalizedTicker);
      return;
    }
    const pickId = myPickIdsByTicker[normalizedTicker];
    if (pickId == null) {
      setMyPicksNotice(`Could not resolve ${normalizedTicker} in My Picks. Refresh and try again.`);
      return;
    }
    setSavingMyPickTickers((current) => ({ ...current, [normalizedTicker]: true }));
    setMyPicksNotice("");
    try {
      await fetchJson<{ ok: boolean }>(`/api/admin/my-picks/${pickId}/delete`, { method: "POST" });
      setMyPickTickers((current) => {
        const next = new Set(current);
        next.delete(normalizedTicker);
        return next;
      });
      setMyPickIdsByTicker((current) => {
        const next = { ...current };
        delete next[normalizedTicker];
        return next;
      });
      setMyPicksNotice(`${normalizedTicker} removed from My Picks.`);
    } catch (error) {
      setMyPicksNotice(error instanceof Error ? error.message : "Failed to remove ticker from My Picks.");
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
            const builtIn = BUILT_IN_TOP_HITS_PRESETS[name];
            applyFilterPreset(name ? resolveTopHitsPreset(name, presetStore) ?? DEFAULT_TOP_HITS_FILTERS : EMPTY_TOP_HITS_FILTERS);
            setPresetNotice(name ? `${builtIn?.label ?? name} loaded.` : "Filters cleared.");
          }}>
            <option value="">Custom / cleared filters</option>
            {Object.entries(BUILT_IN_TOP_HITS_PRESETS).map(([id, preset]) => (
              <option key={id} value={id}>{preset.label} · Built-in{id === (presetStore.defaultPresetName || BUILT_IN_LEADERSHIP_PRESET_ID) ? " · Default" : ""}</option>
            ))}
            {Object.keys(presetStore.presets).sort().map((name) => (
              <option key={name} value={name}>{name}{name === presetStore.defaultPresetName ? " · Default" : ""}</option>
            ))}
          </select>
          <button type="button" className="ghost-button" onClick={() => {
            const suggestedName = BUILT_IN_TOP_HITS_PRESETS[selectedPresetName] ? "" : selectedPresetName;
            const name = (window.prompt("Preset name", suggestedName) || "").trim().slice(0, 60);
            if (!name) return;
            const nextStore = { ...presetStore, presets: { ...presetStore.presets, [name]: currentFilterPreset() } };
            setSelectedPresetName(name);
            updatePresetStore(nextStore, `${name} saved.`);
          }}>Save preset</button>
          {selectedPresetName ? <button type="button" className="ghost-button" disabled={selectedPresetName === (presetStore.defaultPresetName || BUILT_IN_LEADERSHIP_PRESET_ID)} onClick={() => {
            const label = BUILT_IN_TOP_HITS_PRESETS[selectedPresetName]?.label ?? selectedPresetName;
            updatePresetStore({ ...presetStore, defaultPresetName: selectedPresetName }, `${label} will load by default.`);
          }}>{selectedPresetName === (presetStore.defaultPresetName || BUILT_IN_LEADERSHIP_PRESET_ID) ? "Default" : "Set default"}</button> : null}
          {selectedPresetName && !BUILT_IN_TOP_HITS_PRESETS[selectedPresetName] ? <button type="button" className="ghost-button" onClick={() => {
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
          <summary>Quality &amp; leadership · {[sizePriceFloorOnly, eliteOnly, hasLeadershipScannerOnly, hasFundamentalQualityOnly].filter(Boolean).length} active</summary>
          <div className="top-hits-filter-group-body">
            <label className="scanner-result-filter">
              <span>Size &amp; Price Floor</span>
              <span className="scanner-result-check">
                <input type="checkbox" checked={sizePriceFloorOnly} onChange={(event) => setSizePriceFloorOnly(event.target.checked)} />
                <span>Market cap $1B+ and price $5+</span>
              </span>
              <span className="panel-copy">Enabled by default across every Top Hits view. Names with missing size or price data are excluded.</span>
            </label>
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
          {snapshot?.refresh_status && snapshot.refresh_status !== "idle" ? <span className="panel-copy earnings-console-note">Refresh {snapshot.refresh_status}: {snapshot.refresh_message || "checking scanner inputs"}</span> : null}
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
              className={`scanner-result-view-chip${viewMode === "sectors" ? " is-active" : ""}`}
              type="button"
              onClick={() => setViewMode("sectors")}
            >
              Sectors
            </button>
            <button
              className={`scanner-result-view-chip${viewMode === "position" ? " is-active" : ""}`}
              type="button"
              onClick={() => setViewMode("position")}
            >
              Position Map
            </button>
            <button
              className={`scanner-result-view-chip${viewMode === "etf-portfolios" ? " is-active" : ""}`}
              type="button"
              onClick={() => setViewMode("etf-portfolios")}
            >
              ETF Portfolios
            </button>
          </div>
          <span className="panel-copy">Guru Board keeps scanner overlap visible; unavailable strategies stay clearly marked.</span>
        </div>
      </section>

      <section className="scanner-result-table-shell panel">
        {isLoading && !payload ? <LoadingBlock label="Loading scanner top hits…" /> : null}
        {notice ? <p className="panel-copy">{notice} <button className="ghost-button" type="button" onClick={() => setReloadKey((value) => value + 1)}>Retry</button></p> : null}
        {!notice && myPicksNotice ? <p className="panel-copy earnings-console-note">{myPicksNotice}</p> : null}
        {!isLoading && !notice && !etfLoading && viewMode !== "etf-portfolios" && ((viewMode === "guru" || viewMode === "sectors" || viewMode === "position") ? guruRows.length === 0 : filteredRows.length === 0) ? <p className="panel-copy">No tickers match current filters.</p> : null}
        {viewMode === "etf-portfolios" ? (
          <BoardChartWorkspace selection={selectedBoardSelection} chartPayload={chartPayloads[selectedBoardTicker]} chartError={chartErrors[selectedBoardTicker]} isChartLoading={Boolean(chartLoadingTickers[selectedBoardTicker])} workspace={chartWorkspace} onWorkspaceChange={updateChartWorkspace}>
            <MomentumEtfPortfolioBoard
              payload={etfPayload}
              loading={etfLoading}
              notice={etfNotice}
              selectedEtf={selectedEtf}
              topHitsOnly={etfTopHitsOnly}
              minOverlap={etfMinOverlap}
              sizePriceFloorOnly={sizePriceFloorOnly}
              scannerGroups={scannerGroups}
              scannerGroupsOnly={guruScannerGroupsOnly}
              selectedScannerGroupCount={nonEmptyScannerGroupCount}
              scannerIdsByTicker={scannerIdsByTicker}
              myPickTickers={myPickTickers}
              selectedTicker={selectedBoardTicker}
              onSelectedEtfChange={setSelectedEtf}
              onTopHitsOnlyChange={setEtfTopHitsOnly}
              onMinOverlapChange={setEtfMinOverlap}
              onScannerGroupsOnlyChange={setGuruScannerGroupsOnly}
              onSelectTicker={selectBoardTicker}
              onRetry={() => setEtfPayload(null)}
            />
          </BoardChartWorkspace>
        ) : ((viewMode === "guru" || viewMode === "sectors" || viewMode === "position") ? Boolean(guruBoard) : filteredRows.length > 0) ? (
          <>
            {viewMode === "guru" ? (
              <BoardChartWorkspace selection={selectedBoardSelection} chartPayload={chartPayloads[selectedBoardTicker]} chartError={chartErrors[selectedBoardTicker]} isChartLoading={Boolean(chartLoadingTickers[selectedBoardTicker])} workspace={chartWorkspace} onWorkspaceChange={updateChartWorkspace}>
                <GuruBoard
                  rows={visibleGuruRows}
                  definitions={guruBoard?.definitions ?? []}
                  totalScannerMatches={guruBoard?.total_scanner_matches ?? 0}
                  confluenceTickerCount={guruBoard?.confluence_ticker_count ?? 0}
                  scannerGroupsOnly={guruScannerGroupsOnly}
                  selectedScannerGroupCount={nonEmptyScannerGroupCount}
                  myPickTickers={myPickTickers}
                  savingMyPickTickers={savingMyPickTickers}
                  canManageMyPicks={canManageMyPicks}
                  selectedTicker={selectedBoardTicker}
                  onScannerGroupsOnlyChange={setGuruScannerGroupsOnly}
                  onSelectTicker={selectBoardTicker}
                  onToggleMyPick={handleToggleMyPick}
                />
              </BoardChartWorkspace>
            ) : viewMode === "sectors" ? (
              <BoardChartWorkspace selection={selectedBoardSelection} chartPayload={chartPayloads[selectedBoardTicker]} chartError={chartErrors[selectedBoardTicker]} isChartLoading={Boolean(chartLoadingTickers[selectedBoardTicker])} workspace={chartWorkspace} onWorkspaceChange={updateChartWorkspace}>
                <SectorBoard
                  rows={visibleGuruRows}
                  scannerGroupsOnly={guruScannerGroupsOnly}
                  selectedScannerGroupCount={nonEmptyScannerGroupCount}
                  myPickTickers={myPickTickers}
                  savingMyPickTickers={savingMyPickTickers}
                  canManageMyPicks={canManageMyPicks}
                  selectedTicker={selectedBoardTicker}
                  onScannerGroupsOnlyChange={setGuruScannerGroupsOnly}
                  onSelectTicker={selectBoardTicker}
                  onToggleMyPick={handleToggleMyPick}
                />
              </BoardChartWorkspace>
            ) : viewMode === "position" ? (
              <BoardChartWorkspace selection={selectedBoardSelection} chartPayload={chartPayloads[selectedBoardTicker]} chartError={chartErrors[selectedBoardTicker]} isChartLoading={Boolean(chartLoadingTickers[selectedBoardTicker])} workspace={chartWorkspace} onWorkspaceChange={updateChartWorkspace}>
                <PositionMap
                  rows={visibleGuruRows}
                  scannerGroupsOnly={guruScannerGroupsOnly}
                  selectedScannerGroupCount={nonEmptyScannerGroupCount}
                  myPickTickers={myPickTickers}
                  savingMyPickTickers={savingMyPickTickers}
                  canManageMyPicks={canManageMyPicks}
                  selectedTicker={selectedBoardTicker}
                  onScannerGroupsOnlyChange={setGuruScannerGroupsOnly}
                  onSelectTicker={selectBoardTicker}
                  onToggleMyPick={handleToggleMyPick}
                />
              </BoardChartWorkspace>
            ) : <>
            {viewMode === "charts" ? (
              <ChartWorkspaceToolbar
                totalRows={filteredRows.length}
                workspace={chartWorkspace}
                onChange={updateChartWorkspace}
              />
            ) : (
              <div className="scanner-top-hits-toolbar">
                <span>{formatCount(filteredRows.length)} names</span>
                <span>Latest board date {formatLocalDate(payload?.target_trading_date)}</span>
              </div>
            )}
            <PaginationControls
              currentPage={normalizedPage}
              totalItems={filteredRows.length}
              totalPages={totalPages}
              pageSize={pageSize}
              onPageChange={setCurrentPage}
            />
            {viewMode === "charts" ? (
              <div className={`scanner-result-chart-grid scanner-top-hit-chart-grid is-${chartWorkspace.gridColumns}-col`}>
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
                    workspace={chartWorkspace}
                    onToggleMyPick={handleToggleMyPick}
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
                      <td data-label="RS Phase">{resolveRsPhaseBadge(row) ?? "--"}</td>
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

function ScannerGroupsOnlyFilter({ checked, groupCount, onChange }: { checked: boolean; groupCount: number; onChange: (enabled: boolean) => void }) {
  return (
    <label className={`scanner-result-check guru-scanner-group-filter${checked ? " is-selected" : ""}`} title={groupCount > 0 ? "Use the scanner groups selected above" : "Select at least one scanner group first"}>
      <input type="checkbox" checked={checked} disabled={groupCount === 0} onChange={(event) => onChange(event.target.checked)} />
      <span>Match selected scanner groups{groupCount > 0 ? ` · ${groupCount}` : ""}</span>
    </label>
  );
}

function useHorizontalDragScroll() {
  const scrollRef = useRef<HTMLDivElement>(null);
  const drag = useRef({ pointerId: -1, startX: 0, startScrollLeft: 0, moved: false });
  const suppressClick = useRef(false);

  const finishDrag = (event: ReactPointerEvent<HTMLDivElement>) => {
    if (drag.current.pointerId !== event.pointerId) return;
    suppressClick.current = drag.current.moved;
    drag.current.pointerId = -1;
    event.currentTarget.classList.remove("is-dragging");
    if (event.currentTarget.hasPointerCapture(event.pointerId)) {
      event.currentTarget.releasePointerCapture(event.pointerId);
    }
  };

  return {
    ref: scrollRef,
    onPointerDown: (event: ReactPointerEvent<HTMLDivElement>) => {
      const element = scrollRef.current;
      if (event.pointerType !== "mouse" || event.button !== 0 || !element || element.scrollWidth <= element.clientWidth) return;
      suppressClick.current = false;
      drag.current = { pointerId: event.pointerId, startX: event.clientX, startScrollLeft: element.scrollLeft, moved: false };
    },
    onPointerMove: (event: ReactPointerEvent<HTMLDivElement>) => {
      const element = scrollRef.current;
      if (!element || drag.current.pointerId !== event.pointerId) return;
      const distance = event.clientX - drag.current.startX;
      if (!drag.current.moved && Math.abs(distance) < 5) return;
      if (!drag.current.moved) element.setPointerCapture(event.pointerId);
      drag.current.moved = true;
      element.classList.add("is-dragging");
      element.scrollLeft = drag.current.startScrollLeft - distance;
      event.preventDefault();
    },
    onPointerUp: finishDrag,
    onPointerCancel: finishDrag,
    onClickCapture: (event: ReactMouseEvent<HTMLDivElement>) => {
      if (!suppressClick.current) return;
      suppressClick.current = false;
      event.preventDefault();
      event.stopPropagation();
    },
    onDragStart: (event: ReactDragEvent<HTMLDivElement>) => event.preventDefault(),
  };
}

function toBoardChartSelection(row: ScannerTopHitRow): BoardChartSelection {
  return {
    ticker: row.ticker,
    company: row.company || row.industry || "",
    sector: row.sector || "",
    close: row.day_close,
    changePct: row.change_pct,
    dailyRs: row.daily_rs_rating ?? row.rs_rating,
    stage: row.stage_analysis?.alias || "--",
    strikeLabel: row.strike_zone?.label || "Context",
    strikeState: row.strike_zone?.state || "context",
    strikeScore: row.strike_zone?.score ?? null,
    scannerSummary: buildScannerHitsTitle(row),
    context: row.position_bucket ? row.position_bucket.replace(/_/g, " ") : undefined,
  };
}

function toEtfBoardChartSelection(row: MomentumEtfPortfolioRow): BoardChartSelection {
  return {
    ticker: row.ticker,
    company: row.company || "",
    sector: row.sector || "",
    close: row.day_close ?? null,
    changePct: row.change_pct ?? null,
    dailyRs: row.daily_rs_rating ?? null,
    stage: row.stage_analysis?.alias || "--",
    strikeLabel: row.strike_zone?.label || "Context",
    strikeState: row.strike_zone?.state || "context",
    strikeScore: row.strike_zone?.score ?? null,
    scannerSummary: row.scanner_labels.length ? `Top Hits: ${row.scanner_labels.join(" · ")}` : "Not currently in Top Hits",
    context: `${row.etf_count} momentum ETF${row.etf_count === 1 ? "" : "s"}`,
  };
}

function BoardChartWorkspace({
  children,
  selection,
  chartPayload,
  chartError,
  isChartLoading,
  workspace,
  onWorkspaceChange,
}: {
  children: ReactNode;
  selection: BoardChartSelection | null;
  chartPayload: WatchlistChartResponse | null | undefined;
  chartError: string | undefined;
  isChartLoading: boolean;
  workspace: ChartWorkspace;
  onWorkspaceChange: (patch: Partial<ChartWorkspace>) => void;
}) {
  const allCandles = buildChartCandles(chartPayload);
  const chartCandles = sliceCandlesToRange(allCandles, workspace.range);
  const firstChartTime = chartCandles[0]?.time;
  const ema8 = sliceChartSeries(buildExponentialMovingAverage(allCandles, 8), firstChartTime);
  const ema21 = sliceChartSeries(buildExponentialMovingAverage(allCandles, 21), firstChartTime);
  return (
    <div className="board-chart-split">
      <div className="board-chart-split-board">{children}</div>
      <aside className="board-chart-panel" aria-label="Selected ticker chart">
        {!selection ? <p className="panel-copy">Select a ticker card to review its chart.</p> : <>
          <div className="board-chart-panel-head">
            <div>
              <Link to={buildChartHref(selection.ticker)} onMouseEnter={preloadChartsPage} onFocus={preloadChartsPage}>{selection.ticker}</Link>
              <strong title={selection.company}>{selection.company || selection.sector || "Selected ticker"}</strong>
            </div>
            <div>
              <strong>{formatPrice(selection.close)}</strong>
              {renderChange(selection.changePct)}
            </div>
          </div>
          <div className="scanner-chart-card-score-row board-chart-panel-scores">
            <span className="scanner-score-pill">Stage {selection.stage}</span>
            <span className={`scanner-score-pill ${toneForRating(selection.dailyRs, 90)}`}>RS {formatRating(selection.dailyRs)}</span>
            <span className={`scanner-score-pill ${toneForStrikeZone(selection.strikeState)}`}>⚾ {selection.strikeLabel}{selection.strikeScore == null ? "" : ` ${selection.strikeScore}`}</span>
          </div>
          <div className="board-chart-panel-controls" role="group" aria-label="Selected chart range">
            {(["3m", "6m", "1y"] as ChartRange[]).map((range) => <button key={range} type="button" className={`scanner-result-view-chip${workspace.range === range ? " is-active" : ""}`} onClick={() => onWorkspaceChange({ range })}>{range.toUpperCase()}</button>)}
          </div>
          <div className="board-chart-panel-chart">
            {isChartLoading ? <LoadingBlock label={`Loading ${selection.ticker} chart...`} /> : null}
            {!isChartLoading && chartError ? <p className="panel-copy">{chartError}</p> : null}
            {!isChartLoading && !chartError && chartCandles.length === 0 ? <p className="panel-copy">No chart data.</p> : null}
            {!isChartLoading && !chartError && chartCandles.length > 0 ? <ScannerMiniChart ticker={selection.ticker} candles={chartCandles} chartType={workspace.chartType} height={330} showVolume={workspace.showVolume} ema8={workspace.showEma8 ? ema8 : []} ema21={workspace.showEma21 ? ema21 : []} /> : null}
          </div>
          <div className="board-chart-panel-context">
            <span>{selection.sector || "Sector unavailable"}</span>
            {selection.context ? <span>{selection.context}</span> : null}
            <span title={selection.scannerSummary}>{selection.scannerSummary}</span>
          </div>
          <Link className="ghost-button board-chart-panel-link" to={buildChartHref(selection.ticker)} onMouseEnter={preloadChartsPage} onFocus={preloadChartsPage}>Open full chart</Link>
        </>}
      </aside>
    </div>
  );
}

function GuruBoard({
  rows,
  definitions,
  totalScannerMatches,
  confluenceTickerCount,
  scannerGroupsOnly,
  selectedScannerGroupCount,
  myPickTickers,
  savingMyPickTickers,
  canManageMyPicks,
  selectedTicker,
  onScannerGroupsOnlyChange,
  onSelectTicker,
  onToggleMyPick,
}: {
  rows: ScannerTopHitRow[];
  definitions: NonNullable<ScannerTopHitsResponse["guru_board"]>["definitions"];
  totalScannerMatches: number;
  confluenceTickerCount: number;
  scannerGroupsOnly: boolean;
  selectedScannerGroupCount: number;
  myPickTickers: Set<string>;
  savingMyPickTickers: Record<string, boolean>;
  canManageMyPicks: boolean;
  selectedTicker: string;
  onScannerGroupsOnlyChange: (enabled: boolean) => void;
  onSelectTicker: (ticker: string) => void;
  onToggleMyPick: (ticker: string) => Promise<void>;
}) {
  const [visibleCounts, setVisibleCounts] = useState<Record<string, number>>({});
  const [strikeFilter, setStrikeFilter] = useState<"all" | "active" | "ready" | "context" | "avoid">("all");
  const [guruSortBy, setGuruSortBy] = useState<GuruSortKey>("default");
  const [guruSortDirection, setGuruSortDirection] = useState<SortDirection>("desc");
  const horizontalDragScroll = useHorizontalDragScroll();
  const visibleRows = useMemo(() => {
    const filtered = strikeFilter === "all" ? rows : rows.filter((row) => row.strike_zone?.state === strikeFilter);
    if (guruSortBy === "default") return filtered;
    return [...filtered].sort((left, right) => compareGuruRows(left, right, guruSortBy, guruSortDirection));
  }, [guruSortBy, guruSortDirection, rows, strikeFilter]);
  return (
    <section className="guru-board" aria-label="Guru scanner board">
      <div className="guru-board-summary">
        <span><strong>{formatCount(visibleRows.length)}</strong> Guru names</span>
        <span><strong>{formatCount(totalScannerMatches)}</strong> scanner matches</span>
        <span><strong>{formatCount(confluenceTickerCount)}</strong> confluence names</span>
      </div>
      <div className="guru-board-filters">
        <div className="guru-strike-filters" role="group" aria-label="Strike Zone status">
          {(["all", "active", "ready", "context", "avoid"] as const).map((state) => (
            <button key={state} type="button" className={`scanner-result-view-chip${strikeFilter === state ? " is-active" : ""}`} onClick={() => setStrikeFilter(state)}>
              {state === "all" ? "All" : state[0].toUpperCase() + state.slice(1)}
            </button>
          ))}
        </div>
        <div className="guru-sort-controls">
          <label>
            <span>Sort</span>
            <select value={guruSortBy} onChange={(event) => {
              const nextSort = event.target.value as GuruSortKey;
              setGuruSortBy(nextSort);
              setGuruSortDirection(nextSort === "stage" || nextSort === "rsPhase" ? "asc" : "desc");
            }}>
              <option value="default">Board priority</option>
              <option value="stage">Stage</option>
              <option value="rs">Daily RS score</option>
              <option value="rsPhase">RS Phase</option>
              <option value="strike">Strike score</option>
            </select>
          </label>
          <button
            type="button"
            className="ghost-button guru-sort-direction"
            disabled={guruSortBy === "default"}
            aria-label={guruSortDirection === "asc" ? "Sort ascending" : "Sort descending"}
            title={guruSortDirection === "asc" ? "Ascending" : "Descending"}
            onClick={() => setGuruSortDirection((current) => current === "asc" ? "desc" : "asc")}
          >
            {guruSortDirection === "asc" ? "↑" : "↓"}
          </button>
        </div>
        <ScannerGroupsOnlyFilter checked={scannerGroupsOnly} groupCount={selectedScannerGroupCount} onChange={onScannerGroupsOnlyChange} />
      </div>
      {scannerGroupsOnly && selectedScannerGroupCount > 0 && visibleRows.length === 0 ? <p className="panel-copy">No Guru names match the selected scanner groups and Strike Zone filter.</p> : null}
      <div className="guru-board-scroll is-drag-scroll" {...horizontalDragScroll}>
        {definitions.map((definition) => {
          const columnRows = visibleRows.filter((row) => row.scanners.some((scanner) => normalizeScannerId(scanner.id) === definition.id));
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
                {columnRows.slice(0, visibleCount).map((row) => (
                  <GuruTickerCard
                    canManageMyPicks={canManageMyPicks}
                    isMyPick={myPickTickers.has(row.ticker)}
                    isSavingMyPick={Boolean(savingMyPickTickers[row.ticker])}
                    isSelected={row.ticker === selectedTicker}
                    key={row.ticker}
                    onSelect={onSelectTicker}
                    onToggleMyPick={onToggleMyPick}
                    row={row}
                  />
                ))}
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

const SECTOR_BOARD_ACCENTS = ["amber", "yellow", "teal", "blue", "sky", "cyan", "gold"];

function SectorBoard({
  rows,
  scannerGroupsOnly,
  selectedScannerGroupCount,
  myPickTickers,
  savingMyPickTickers,
  canManageMyPicks,
  selectedTicker,
  onScannerGroupsOnlyChange,
  onSelectTicker,
  onToggleMyPick,
}: {
  rows: ScannerTopHitRow[];
  scannerGroupsOnly: boolean;
  selectedScannerGroupCount: number;
  myPickTickers: Set<string>;
  savingMyPickTickers: Record<string, boolean>;
  canManageMyPicks: boolean;
  selectedTicker: string;
  onScannerGroupsOnlyChange: (enabled: boolean) => void;
  onSelectTicker: (ticker: string) => void;
  onToggleMyPick: (ticker: string) => Promise<void>;
}) {
  const [visibleCounts, setVisibleCounts] = useState<Record<string, number>>({});
  const [strikeFilter, setStrikeFilter] = useState<"all" | "active" | "ready" | "context" | "avoid">("all");
  const [sectorSortBy, setSectorSortBy] = useState<GuruSortKey>("default");
  const [sectorSortDirection, setSectorSortDirection] = useState<SortDirection>("desc");
  const horizontalDragScroll = useHorizontalDragScroll();
  const visibleRows = useMemo(() => {
    const filtered = strikeFilter === "all" ? rows : rows.filter((row) => row.strike_zone?.state === strikeFilter);
    if (sectorSortBy === "default") return filtered;
    return [...filtered].sort((left, right) => compareGuruRows(left, right, sectorSortBy, sectorSortDirection));
  }, [rows, sectorSortBy, sectorSortDirection, strikeFilter]);
  const sectors = useMemo(() => {
    const grouped = new Map<string, ScannerTopHitRow[]>();
    for (const row of visibleRows) {
      const sector = String(row.sector || "").trim() || "Unclassified";
      grouped.set(sector, [...(grouped.get(sector) ?? []), row]);
    }
    return Array.from(grouped.entries())
      .map(([sector, sectorRows]) => ({ sector, rows: sectorRows }))
      .sort((left, right) => right.rows.length - left.rows.length || left.sector.localeCompare(right.sector));
  }, [visibleRows]);
  return (
    <section className="guru-board sector-board" aria-label="Top Hits by sector">
      <div className="guru-board-summary">
        <span><strong>{formatCount(visibleRows.length)}</strong> Guru names across <strong>{formatCount(sectors.length)}</strong> sectors</span>
        <span>Each ticker appears once in its latest sector.</span>
      </div>
      <div className="guru-board-filters">
        <div className="guru-strike-filters" role="group" aria-label="Strike Zone status">
          {(["all", "active", "ready", "context", "avoid"] as const).map((state) => (
            <button key={state} type="button" className={`scanner-result-view-chip${strikeFilter === state ? " is-active" : ""}`} onClick={() => setStrikeFilter(state)}>
              {state === "all" ? "All" : state[0].toUpperCase() + state.slice(1)}
            </button>
          ))}
        </div>
        <div className="guru-sort-controls">
          <label>
            <span>Sort cards</span>
            <select value={sectorSortBy} onChange={(event) => {
              const nextSort = event.target.value as GuruSortKey;
              setSectorSortBy(nextSort);
              setSectorSortDirection(nextSort === "stage" || nextSort === "rsPhase" ? "asc" : "desc");
            }}>
              <option value="default">Board priority</option>
              <option value="stage">Stage</option>
              <option value="rs">Daily RS score</option>
              <option value="rsPhase">RS Phase</option>
              <option value="strike">Strike score</option>
            </select>
          </label>
          <button
            type="button"
            className="ghost-button guru-sort-direction"
            disabled={sectorSortBy === "default"}
            aria-label={sectorSortDirection === "asc" ? "Sort ascending" : "Sort descending"}
            title={sectorSortDirection === "asc" ? "Ascending" : "Descending"}
            onClick={() => setSectorSortDirection((current) => current === "asc" ? "desc" : "asc")}
          >
            {sectorSortDirection === "asc" ? "↑" : "↓"}
          </button>
        </div>
        <ScannerGroupsOnlyFilter checked={scannerGroupsOnly} groupCount={selectedScannerGroupCount} onChange={onScannerGroupsOnlyChange} />
      </div>
      {scannerGroupsOnly && selectedScannerGroupCount > 0 && visibleRows.length === 0 ? <p className="panel-copy">No sector names match the selected scanner groups and Strike Zone filter.</p> : null}
      <div className="guru-board-scroll is-drag-scroll" {...horizontalDragScroll}>
        {sectors.map(({ sector, rows: sectorRows }, index) => {
          const visibleCount = visibleCounts[sector] ?? GURU_COLUMN_PAGE_SIZE;
          const remainingCount = Math.max(0, sectorRows.length - visibleCount);
          return (
            <article className={`guru-column is-${SECTOR_BOARD_ACCENTS[index % SECTOR_BOARD_ACCENTS.length]}`} key={sector}>
              <header title={sector}>
                <strong>{formatCount(sectorRows.length)}</strong>
                <span>{sector}</span>
              </header>
              <div className="guru-column-cards">
                {sectorRows.slice(0, visibleCount).map((row) => (
                  <GuruTickerCard
                    canManageMyPicks={canManageMyPicks}
                    isMyPick={myPickTickers.has(row.ticker)}
                    isSavingMyPick={Boolean(savingMyPickTickers[row.ticker])}
                    isSelected={row.ticker === selectedTicker}
                    key={row.ticker}
                    onSelect={onSelectTicker}
                    onToggleMyPick={onToggleMyPick}
                    row={row}
                  />
                ))}
              </div>
              {remainingCount > 0 ? <button className="ghost-button guru-column-load-more" type="button" onClick={() => setVisibleCounts((current) => ({ ...current, [sector]: visibleCount + GURU_COLUMN_PAGE_SIZE }))}>
                Load 30 more ({formatCount(remainingCount)} remaining)
              </button> : null}
            </article>
          );
        })}
      </div>
      <p className="panel-copy guru-board-note">Sector columns help compare leadership breadth. Strike Zone remains chart-review guidance, not an automatic buy signal.</p>
    </section>
  );
}

function GuruTickerCard({
  row,
  isMyPick,
  isSavingMyPick,
  canManageMyPicks,
  isSelected = false,
  onSelect,
  onToggleMyPick,
}: {
  row: ScannerTopHitRow;
  isMyPick: boolean;
  isSavingMyPick: boolean;
  canManageMyPicks: boolean;
  isSelected?: boolean;
  onSelect?: (ticker: string) => void;
  onToggleMyPick: (ticker: string) => Promise<void>;
}) {
  const stage = row.stage_analysis?.alias || "--";
  const strike = row.strike_zone?.label || "Context";
  const strikeTone = row.strike_zone?.state || "context";
  const atr = row.atr_to_sma50 == null ? "--" : `${row.atr_to_sma50 >= 0 ? "+" : ""}${row.atr_to_sma50.toFixed(1)} ATR`;
  const earnings = row.earnings_days == null ? "Earnings TBD" : row.earnings_days === 0 ? "Earnings today" : `Earnings ${row.earnings_days}d`;
  const rmv = row.rmv ? `RMV ${row.rmv.value.toFixed(0)} · R${row.rmv.rank || "–"}` : null;
  const strikeScore = row.strike_zone?.score;
  const primarySignal = row.strike_zone?.primary_signal;
  const signalAge = row.strike_zone?.signal_age_days;
  const strikeTitle = buildStrikeZoneTitle(row.strike_zone);
  return (
    <article className={`guru-ticker-card${isSelected ? " is-selected" : ""}`}>
      <Link className="guru-ticker-card-link" to={buildChartHref(row.ticker)} title={onSelect ? `Select ${row.ticker}` : `${row.ticker} chart`} onClick={(event) => {
        if (!onSelect) return;
        event.preventDefault();
        onSelect(row.ticker);
      }}>
        <div className="guru-ticker-main">
          <strong>{row.ticker}</strong>
          <span className={row.change_pct != null && row.change_pct < 0 ? "ticker-change down" : "ticker-change up"}>{row.change_pct == null ? "--" : `${row.change_pct >= 0 ? "+" : ""}${row.change_pct.toFixed(1)}%`}</span>
        </div>
        <div className="guru-ticker-badges">
          <span title={buildScannerHitsTitle(row)}>{row.scanner_count}×</span>
          <SectorTag sector={row.sector} />
          <span title="Weinstein stage">{stage}</span>
          <span title="Daily RS">RS {row.daily_rs_rating == null ? "--" : Math.round(row.daily_rs_rating)}</span>
          {resolveRsPhaseBadge(row) ? <span className={rsPhaseBadgeClass(row)} title="RS Phase lifecycle">{resolveRsPhaseBadge(row)}</span> : null}
          {rmv ? <span title="Relative Measured Volatility tightness rank">{rmv}</span> : null}
        </div>
        <div className="guru-ticker-context">
          <span title="ATR distance from SMA50">📏 {atr}</span>
          <span title={earnings}>📅 {row.earnings_days == null ? "TBD" : `${row.earnings_days}d`}</span>
          <span className={`guru-strike is-${strikeTone}`} title={strikeTitle}>⚾ {strike}{strikeScore == null ? "" : ` ${strikeScore}`}</span>
          {primarySignal ? <span className="guru-primary-signal" title={`Primary trigger${signalAge == null ? "" : ` · ${signalAge}d ago`}`}>⚡ {primarySignal}</span> : null}
        </div>
      </Link>
      {canManageMyPicks ? (
        <button
          type="button"
          className={`guru-my-pick-toggle${isMyPick ? " is-selected" : ""}`}
          aria-label={`${isMyPick ? "Remove" : "Add"} ${row.ticker} ${isMyPick ? "from" : "to"} My Picks`}
          aria-pressed={isMyPick}
          disabled={isSavingMyPick}
          title={isMyPick ? "Remove from My Picks" : "Add to My Picks"}
          onClick={() => void onToggleMyPick(row.ticker)}
        >
          {isSavingMyPick ? "…" : isMyPick ? "★" : "☆"}
        </button>
      ) : null}
    </article>
  );
}

function buildStrikeZoneTitle(strikeZone: ScannerTopHitRow["strike_zone"]): string {
  if (!strikeZone) {
    return "Strike Zone context is unavailable.";
  }
  const lines = [
    `Strike Zone: ${strikeZone.label}${strikeZone.score == null ? "" : ` ${strikeZone.score}/100`}`,
    strikeZone.reason,
  ];
  const breakdown = strikeZone.score_breakdown;
  if (breakdown?.blocked) {
    lines.push("Score blocked by risk controls.");
  }
  for (const group of breakdown?.groups ?? []) {
    lines.push("", `${group.label}: ${group.awarded_points}/${group.max_points} awarded`);
    for (const signal of group.signals) {
      const age = signal.age_days == null ? "" : ` · ${signal.age_days}d ago`;
      const freshness = signal.fresh === false ? " · stale, not counted" : age;
      lines.push(`• ${signal.label}: +${signal.points}${freshness}`);
    }
    if (group.signals.length === 0) {
      lines.push("• None");
    }
    lines.push(`Rule: ${group.scoring_rule}`);
  }
  if ((strikeZone.warnings?.length ?? 0) > 0) {
    lines.push("", "Warnings:", ...(strikeZone.warnings ?? []).map((warning) => `• ${warning}`));
  }
  return lines.filter((line): line is string => typeof line === "string").join("\n");
}

function buildScannerHitsTitle(row: ScannerTopHitRow): string {
  const labels = Array.from(new Set(row.scanners.map((scanner) => scanner.label || scanner.id).filter(Boolean)));
  if (labels.length === 0) return `${row.scanner_count} scanner hits`;
  return [`${row.scanner_count} scanner hits`, ...labels.map((label) => `• ${label}`)].join("\n");
}

function compareGuruRows(left: ScannerTopHitRow, right: ScannerTopHitRow, sortBy: Exclude<GuruSortKey, "default">, direction: SortDirection) {
  let comparison = 0;
  if (sortBy === "stage") {
    comparison = compareNullableNumber(guruStageRank(left), guruStageRank(right), direction);
  } else if (sortBy === "rs") {
    comparison = compareNullableNumber(left.daily_rs_rating ?? left.rs_rating, right.daily_rs_rating ?? right.rs_rating, direction);
  } else if (sortBy === "rsPhase") {
    comparison = compareNullableNumber(guruRsPhaseRank(left), guruRsPhaseRank(right), direction)
      || compareNullableNumber(resolveRsPhaseActiveDays(left), resolveRsPhaseActiveDays(right), direction);
  } else if (sortBy === "strike") {
    comparison = compareNullableNumber(left.strike_zone?.score ?? null, right.strike_zone?.score ?? null, direction);
  }
  return comparison || left.ticker.localeCompare(right.ticker);
}

function guruStageRank(row: ScannerTopHitRow): number | null {
  const match = String(row.stage_analysis?.alias || "").toUpperCase().match(/([1-4])\s*([A-Z])?/);
  if (!match) return null;
  const stage = Number(match[1]);
  const stagePriority: Record<number, number> = { 2: 0, 1: 10, 3: 20, 4: 30 };
  const substage = match[2] ? match[2].charCodeAt(0) - 65 : 0;
  return (stagePriority[stage] ?? 40) + substage;
}

function guruRsPhaseRank(row: ScannerTopHitRow): number | null {
  const state = String(row.rs_phase_state ?? row.relative_strength_evidence?.rs_phase_state ?? "").toLowerCase();
  const ranks: Record<string, number> = { new: 0, quick_reclaim: 1, established: 2, mature: 3 };
  if (state in ranks) return ranks[state];
  return resolveRsPhaseActiveDays(row) == null ? null : 4;
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

function MomentumEtfPortfolioBoard({
  payload,
  loading,
  notice,
  selectedEtf,
  topHitsOnly,
  minOverlap,
  sizePriceFloorOnly,
  scannerGroups,
  scannerGroupsOnly,
  selectedScannerGroupCount,
  scannerIdsByTicker,
  myPickTickers,
  selectedTicker,
  onSelectedEtfChange,
  onTopHitsOnlyChange,
  onMinOverlapChange,
  onScannerGroupsOnlyChange,
  onSelectTicker,
  onRetry,
}: {
  payload: MomentumEtfPortfoliosResponse | null;
  loading: boolean;
  notice: string;
  selectedEtf: string;
  topHitsOnly: boolean;
  minOverlap: number;
  sizePriceFloorOnly: boolean;
  scannerGroups: string[][];
  scannerGroupsOnly: boolean;
  selectedScannerGroupCount: number;
  scannerIdsByTicker: Map<string, string[]>;
  myPickTickers: Set<string>;
  selectedTicker: string;
  onSelectedEtfChange: (value: string) => void;
  onTopHitsOnlyChange: (value: boolean) => void;
  onMinOverlapChange: (value: number) => void;
  onScannerGroupsOnlyChange: (enabled: boolean) => void;
  onSelectTicker: (ticker: string) => void;
  onRetry: () => void;
}) {
  const [visibleCounts, setVisibleCounts] = useState<Record<string, number>>({});
  const rows = useMemo(() => (payload?.rows ?? []).filter((row) => {
    if (selectedEtf !== "all" && !row.funds.some((fund) => fund.ticker === selectedEtf)) return false;
    if (topHitsOnly && !row.top_hit) return false;
    if (sizePriceFloorOnly && !meetsSizePriceFloor(row)) return false;
    if (scannerGroupsOnly && selectedScannerGroupCount > 0 && !scannerIdsMatchGroups(
      [...(row.scanner_ids ?? []), ...(scannerIdsByTicker.get(row.ticker) ?? [])],
      scannerGroups,
    )) return false;
    return row.etf_count >= minOverlap;
  }), [minOverlap, payload?.rows, scannerGroups, scannerGroupsOnly, scannerIdsByTicker, selectedEtf, selectedScannerGroupCount, sizePriceFloorOnly, topHitsOnly]);
  const funds = selectedEtf === "all" ? (payload?.funds ?? []) : (payload?.funds ?? []).filter((fund) => fund.ticker === selectedEtf);
  useEffect(() => {
    if (rows.length > 0 && !rows.some((row) => row.ticker === selectedTicker)) {
      onSelectTicker(rows[0].ticker);
    }
  }, [onSelectTicker, rows, selectedTicker]);
  if (loading && !payload) return <LoadingBlock label="Loading momentum ETF portfolios…" />;
  if (notice) return <p className="panel-copy">{notice} <button className="ghost-button" type="button" onClick={onRetry}>Retry</button></p>;
  if (!payload) return <p className="panel-copy">No momentum ETF holdings cache is available yet. Run “Refresh Momentum ETF Holdings”.</p>;
  return (
    <section className="guru-board momentum-etf-board" aria-label="Momentum ETF portfolios">
      <div className="scanner-top-hits-toolbar">
        <span>{formatCount(payload.summary.total_unique_holdings)} unique holdings</span>
        <span>{formatCount(payload.summary.overlap_holding_count)} held by multiple ETFs</span>
        <span>{formatCount(payload.summary.top_hits_count)} Top Hits matches</span>
        <span>Holdings refreshed {formatLocalDate(payload.holdings_generated_at)}</span>
      </div>
      <div className="scanner-result-view-actions" role="group" aria-label="Momentum ETF filters">
        <button className={`scanner-result-view-chip${selectedEtf === "all" ? " is-active" : ""}`} type="button" onClick={() => onSelectedEtfChange("all")}>All</button>
        {payload.funds.map((fund) => <button className={`scanner-result-view-chip${selectedEtf === fund.ticker ? " is-active" : ""}`} type="button" key={fund.ticker} onClick={() => onSelectedEtfChange(fund.ticker)}>{fund.ticker}</button>)}
        <label className="scanner-result-checkbox"><input type="checkbox" checked={topHitsOnly} onChange={(event) => onTopHitsOnlyChange(event.target.checked)} /> Top Hits only</label>
        <ScannerGroupsOnlyFilter checked={scannerGroupsOnly} groupCount={selectedScannerGroupCount} onChange={onScannerGroupsOnlyChange} />
        <label className="field"><span>ETF overlap</span><select value={minOverlap} onChange={(event) => onMinOverlapChange(Number(event.target.value))}><option value={1}>Any ETF</option><option value={2}>2+ ETFs</option><option value={3}>3+ ETFs</option><option value={4}>All 4 ETFs</option></select></label>
      </div>
      <div className="guru-board-summary">
        {payload.funds.map((fund) => <span key={fund.ticker}><a href={fund.source_url} target="_blank" rel="noreferrer"><strong>{fund.ticker}</strong></a> {fund.holding_count}{fund.is_complete ? "" : ` of ${fund.reported_holding_count}`} holdings · {fund.top_hits_count} Top Hits · {fund.as_of_date || "date TBD"}{fund.source_kind === "fallback" ? " · source fallback" : ""}</span>)}
      </div>
      {Object.keys(payload.errors).length > 0 ? <p className="panel-copy earnings-console-note">Using the last successful cache where available. Refresh issue: {Object.keys(payload.errors).join(", ")}.</p> : null}
      {rows.length === 0 ? <p className="panel-copy">No holdings match these ETF filters.</p> : <div className="guru-board-scroll">
        {funds.map((fund) => {
          const fundRows = rows.filter((row) => row.funds.some((holding) => holding.ticker === fund.ticker));
          const visibleCount = visibleCounts[fund.ticker] ?? GURU_COLUMN_PAGE_SIZE;
          const remainingCount = Math.max(0, fundRows.length - visibleCount);
          const accent = MOMENTUM_ETF_ACCENTS[fund.ticker] || "amber";
          return <article className={`guru-column is-${accent}`} key={fund.ticker}>
            <header title={fund.name}>
              <strong>{formatCount(fundRows.length)}</strong>
              <span>{fund.ticker}</span>
            </header>
            {!fund.available ? <p className="guru-column-unavailable">Holdings unavailable</p> : null}
            <div className="guru-column-cards">
              {fundRows.slice(0, visibleCount).map((row) => <MomentumEtfHoldingCard fundTicker={fund.ticker} isMyPick={myPickTickers.has(row.ticker)} isSelected={row.ticker === selectedTicker} key={row.ticker} onSelect={onSelectTicker} row={row} />)}
            </div>
            {remainingCount > 0 ? <button className="ghost-button guru-column-load-more" type="button" onClick={() => setVisibleCounts((current) => ({ ...current, [fund.ticker]: visibleCount + GURU_COLUMN_PAGE_SIZE }))}>
              Load 30 more ({formatCount(remainingCount)} remaining)
            </button> : null}
          </article>;
        })}
      </div>}
      <p className="panel-copy guru-board-note">A ticker can appear in multiple ETF columns. Published ETF weight is ownership context, not a model allocation or entry signal.</p>
    </section>
  );
}

function MomentumEtfHoldingCard({ row, fundTicker, isMyPick, isSelected, onSelect }: { row: MomentumEtfPortfolioRow; fundTicker: string; isMyPick: boolean; isSelected: boolean; onSelect: (ticker: string) => void }) {
  const holding = row.funds.find((fund) => fund.ticker === fundTicker);
  const stage = row.stage_analysis?.alias || "--";
  const atr = row.atr_to_sma50 == null ? "--" : `${row.atr_to_sma50 >= 0 ? "+" : ""}${row.atr_to_sma50.toFixed(1)} ATR`;
  const strike = row.strike_zone?.label || "Context";
  const strikeTone = row.strike_zone?.state || "context";
  const strikeScore = row.strike_zone?.score;
  const title = [row.company || row.ticker, row.scanner_labels.length ? `Top Hits: ${row.scanner_labels.join(" · ")}` : "Not currently in Top Hits"].join("\n");
  return <Link className={`guru-ticker-card momentum-etf-card${row.top_hit ? " is-top-hit" : ""}${isSelected ? " is-selected" : ""}`} to={buildChartHref(row.ticker)} title={`Select ${row.ticker}`} onClick={(event) => { event.preventDefault(); onSelect(row.ticker); }}>
    <div className="guru-ticker-main">
      <strong>{row.ticker}<MyPickIndicator ticker={row.ticker} visible={isMyPick} /></strong>
      <span className={row.change_pct != null && row.change_pct < 0 ? "ticker-change down" : "ticker-change up"}>{row.change_pct == null ? "--" : `${row.change_pct >= 0 ? "+" : ""}${row.change_pct.toFixed(1)}%`}</span>
    </div>
    <div className="guru-ticker-badges">
      <span title={`${fundTicker} published portfolio weight`}>{holding?.weight == null ? "Weight --" : `${holding.weight.toFixed(2)}%`}</span>
      <span title="Number of selected momentum ETFs holding this stock">{row.etf_count}× ETFs</span>
      <SectorTag sector={row.sector} />
      <span title="Weinstein stage">{stage}</span>
      <span title="Daily RS">RS {row.daily_rs_rating == null ? "--" : Math.round(row.daily_rs_rating)}</span>
    </div>
    <div className="guru-ticker-context">
      <span title="ATR distance from SMA50">📏 {atr}</span>
      {row.top_hit ? <span title={row.scanner_labels.join(" · ")}>🔥 {row.scanner_count} hits</span> : null}
      <span className={`guru-strike is-${strikeTone}`} title={buildStrikeZoneTitle(row.strike_zone)}>⚾ {strike}{strikeScore == null ? "" : ` ${strikeScore}`}</span>
    </div>
  </Link>;
}

function SectorTag({ sector }: { sector: string | null | undefined }) {
  const label = String(sector || "").trim();
  if (!label || label === "Unknown sector") return null;
  return <span className="guru-sector-tag" title={`Sector: ${label}`}>{label}</span>;
}

function MyPickIndicator({ ticker, visible }: { ticker: string; visible: boolean }) {
  if (!visible) return null;
  return <span aria-label={`${ticker} is in My Picks`} className="guru-my-pick-indicator" role="img" title="In My Picks">★</span>;
}

function PositionMap({
  rows,
  scannerGroupsOnly,
  selectedScannerGroupCount,
  myPickTickers,
  savingMyPickTickers,
  canManageMyPicks,
  selectedTicker,
  onScannerGroupsOnlyChange,
  onSelectTicker,
  onToggleMyPick,
}: {
  rows: ScannerTopHitRow[];
  scannerGroupsOnly: boolean;
  selectedScannerGroupCount: number;
  myPickTickers: Set<string>;
  savingMyPickTickers: Record<string, boolean>;
  canManageMyPicks: boolean;
  selectedTicker: string;
  onScannerGroupsOnlyChange: (enabled: boolean) => void;
  onSelectTicker: (ticker: string) => void;
  onToggleMyPick: (ticker: string) => Promise<void>;
}) {
  return (
    <section className="guru-board" aria-label="Market position map">
      <div className="guru-board-summary">
        <span>Each Guru ticker appears once, based on its latest moving-average position.</span>
        <span><strong>{formatCount(rows.length)}</strong> names shown</span>
      </div>
      <div className="guru-board-filters">
        <ScannerGroupsOnlyFilter checked={scannerGroupsOnly} groupCount={selectedScannerGroupCount} onChange={onScannerGroupsOnlyChange} />
      </div>
      {scannerGroupsOnly && selectedScannerGroupCount > 0 && rows.length === 0 ? <p className="panel-copy">No Position Map names match the selected scanner groups.</p> : null}
      <div className="guru-board-scroll position-map-scroll">
        {POSITION_BUCKETS.map(([id, label]) => {
          const bucketRows = rows.filter((row) => (row.position_bucket || "no_data") === id);
          return <article className="guru-column position-map-column" key={id}>
            <header><strong>{formatCount(bucketRows.length)}</strong><span>{label}</span></header>
            <div className="guru-column-cards">{bucketRows.slice(0, 30).map((row) => (
              <GuruTickerCard
                canManageMyPicks={canManageMyPicks}
                isMyPick={myPickTickers.has(row.ticker)}
                isSavingMyPick={Boolean(savingMyPickTickers[row.ticker])}
                isSelected={row.ticker === selectedTicker}
                key={row.ticker}
                onSelect={onSelectTicker}
                onToggleMyPick={onToggleMyPick}
                row={row}
              />
            ))}</div>
          </article>;
        })}
      </div>
    </section>
  );
}

function ChartWorkspaceToolbar({
  totalRows,
  workspace,
  onChange,
}: {
  totalRows: number;
  workspace: ChartWorkspace;
  onChange: (patch: Partial<ChartWorkspace>) => void;
}) {
  return (
    <section className="scanner-chart-workspace-toolbar" aria-label="Chart workspace controls">
      <div className="scanner-chart-workspace-summary">
        <strong>Chart workspace</strong>
        <span>{formatCount(totalRows)} names</span>
      </div>
      <div className="scanner-chart-workspace-controls">
        <div className="scanner-chart-control-group" role="group" aria-label="Chart grid density">
          <span>Grid</span>
          {([2, 3, 4] as ChartGridColumns[]).map((columns) => (
            <button
              key={columns}
              type="button"
              className={`scanner-result-view-chip${workspace.gridColumns === columns ? " is-active" : ""}`}
              aria-pressed={workspace.gridColumns === columns}
              onClick={() => onChange({ gridColumns: columns })}
            >
              {columns}
            </button>
          ))}
        </div>
        <div className="scanner-chart-control-group" role="group" aria-label="Chart time range">
          <span>Range</span>
          {(["3m", "6m", "1y"] as ChartRange[]).map((range) => (
            <button
              key={range}
              type="button"
              className={`scanner-result-view-chip${workspace.range === range ? " is-active" : ""}`}
              aria-pressed={workspace.range === range}
              onClick={() => onChange({ range })}
            >
              {range.toUpperCase()}
            </button>
          ))}
        </div>
        <label className="scanner-chart-select-control">
          <span>Style</span>
          <select value={workspace.chartType} onChange={(event) => onChange({ chartType: event.target.value as ChartType })}>
            <option value="candles">Candles</option>
            <option value="bars">Bars</option>
            <option value="line">Line</option>
          </select>
        </label>
        <label className="scanner-chart-toggle">
          <input type="checkbox" checked={workspace.showVolume} onChange={(event) => onChange({ showVolume: event.target.checked })} />
          <span>Volume</span>
        </label>
        <details className="scanner-chart-overlay-menu">
          <summary>Overlays</summary>
          <label><input type="checkbox" checked={workspace.showEma8} onChange={(event) => onChange({ showEma8: event.target.checked })} /> EMA 8</label>
          <label><input type="checkbox" checked={workspace.showEma21} onChange={(event) => onChange({ showEma21: event.target.checked })} /> EMA 21</label>
          <label><input type="checkbox" checked={workspace.showEma60} onChange={(event) => onChange({ showEma60: event.target.checked })} /> EMA 60</label>
          <label><input type="checkbox" checked={workspace.showSma50} onChange={(event) => onChange({ showSma50: event.target.checked })} /> SMA 50</label>
        </details>
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
  workspace,
  onToggleMyPick,
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
  workspace: ChartWorkspace;
  onToggleMyPick: (ticker: string) => Promise<void>;
}) {
  const allCandles = buildChartCandles(chartPayload);
  const chartCandles = sliceCandlesToRange(allCandles, workspace.range);
  const latestCandle = chartCandles[chartCandles.length - 1] ?? null;
  const firstChartTime = chartCandles[0]?.time;
  const ema8 = workspace.showEma8 ? sliceChartSeries(buildExponentialMovingAverage(allCandles, 8), firstChartTime) : [];
  const ema21 = workspace.showEma21 ? sliceChartSeries(buildExponentialMovingAverage(allCandles, 21), firstChartTime) : [];
  const ema60 = workspace.showEma60 ? sliceChartSeries(buildExponentialMovingAverage(allCandles, 60), firstChartTime) : [];
  const sma50 = workspace.showSma50 ? sliceChartSeries(buildSimpleMovingAverage(allCandles, 50), firstChartTime) : [];
  const strikeLabel = row.strike_zone?.label || "Context";
  const stage = row.stage_analysis?.alias || "--";
  return (
    <article className="scanner-chart-card scanner-top-hit-chart-card">
      <div className="scanner-chart-card-header">
        <div className="scanner-chart-card-heading">
          <div className="scanner-chart-card-symbol-row">
            {canManageMyPicks ? (
              <button
                type="button"
                className={`scanner-chart-favorite${alreadyMyPick ? " is-selected" : ""}`}
                aria-label={`${alreadyMyPick ? "Remove" : "Add"} ${row.ticker} ${alreadyMyPick ? "from" : "to"} My Picks`}
                aria-pressed={alreadyMyPick}
                disabled={savingMyPick}
                title={alreadyMyPick ? "Remove from My Picks" : "Add to My Picks"}
                onClick={() => void onToggleMyPick(row.ticker)}
              >
                {savingMyPick ? "…" : alreadyMyPick ? "★" : "☆"}
              </button>
            ) : null}
            <Link className="scanner-result-symbol" to={buildChartHref(row.ticker)} onMouseEnter={preloadChartsPage} onFocus={preloadChartsPage}>
              <span>{row.ticker}</span>
            </Link>
            <span className="scanner-chart-card-company" title={row.company || row.industry || "Scanner hit"}>{row.company || row.industry || "Scanner hit"}</span>
          </div>
        </div>
        <div className="scanner-chart-card-price">
          <strong>{latestCandle ? formatPrice(latestCandle.close) : formatPrice(row.day_close)}</strong>
          {renderChange(row.change_pct)}
        </div>
      </div>
      <div className="scanner-chart-card-score-row">
        <span className="scanner-score-pill is-strong">{formatCount(row.scanner_count)} hits</span>
        <span className={`scanner-score-pill ${toneForStrikeZone(row.strike_zone?.state)}`}>⚾ {strikeLabel}</span>
        <span className="scanner-score-pill">Stage {stage}</span>
        <span className={`scanner-score-pill ${toneForRating(row.daily_rs_rating ?? row.rs_rating, 90)}`}>RS {formatRating(row.daily_rs_rating ?? row.rs_rating)}</span>
      </div>
      <div className="scanner-chart-card-body">
        {isChartLoading ? <LoadingBlock label={`Loading ${row.ticker} chart...`} /> : null}
        {!isChartLoading && chartError ? <p className="panel-copy">{chartError}</p> : null}
        {!isChartLoading && !chartError && chartCandles.length === 0 ? <p className="panel-copy">No chart data.</p> : null}
        {!isChartLoading && !chartError && chartCandles.length > 0 ? (
          <ScannerMiniChart
            ticker={row.ticker}
            candles={chartCandles}
            chartType={workspace.chartType}
            height={workspace.gridColumns === 2 ? 360 : 300}
            showVolume={workspace.showVolume}
            ema8={ema8}
            ema21={ema21}
            ema60={ema60}
            sma50={sma50}
          />
        ) : null}
      </div>
      <div className="scanner-chart-card-footer">
        <span>{chartPayload?.resolved_as_of_date ? `As of ${chartPayload.resolved_as_of_date}` : `Signal ${formatLocalDate(boardSignalDate)}`}</span>
        <details className="scanner-chart-card-details">
          <summary>Signals</summary>
          <ScannerBadges scanners={row.scanners} selectedScannerIds={selectedScannerIds} scannerNames={scannerNames} />
        </details>
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

function resolveRsPhaseBadge(row: ScannerTopHitRow): string | null {
  const label = row.rs_phase_badge_label ?? row.relative_strength_evidence?.rs_phase_badge_label;
  if (label) {
    return label;
  }
  const activeDays = resolveRsPhaseActiveDays(row);
  return activeDays == null ? null : `RS Phase ${formatPhaseDays(activeDays)}`;
}

function rsPhaseBadgeClass(row: ScannerTopHitRow): string {
  const state = row.rs_phase_state ?? row.relative_strength_evidence?.rs_phase_state ?? "";
  return state ? `is-rs-phase-${state}` : "";
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

function hasScannerGroupSignals(row: ScannerTopHitRow, scannerGroups: string[][], additionalScannerIds: string[] = []) {
  return scannerIdsMatchGroups([
    ...row.scanners.map((scanner) => normalizeScannerId(scanner.id)).filter(Boolean),
    ...additionalScannerIds,
  ], scannerGroups);
}

function scannerIdsMatchGroups(scannerIds: string[], scannerGroups: string[][]) {
  const rowScannerIds = new Set(scannerIds.map(normalizeScannerId).filter(Boolean));
  return scannerGroups.filter((group) => group.length > 0).every((group) => group.some((scannerId) => rowScannerIds.has(scannerId)));
}

function meetsSizePriceFloor(row: { market_cap?: number | null; day_close?: number | null }) {
  return row.market_cap != null && row.market_cap >= MIN_TOP_HITS_MARKET_CAP
    && row.day_close != null && row.day_close >= MIN_TOP_HITS_PRICE;
}

function chartPageSize(columns: ChartGridColumns) {
  return columns === 2 ? 8 : columns === 4 ? 12 : 9;
}

function loadChartWorkspace(): ChartWorkspace {
  try {
    const parsed: unknown = JSON.parse(localStorage.getItem(CHART_WORKSPACE_STORAGE_KEY) || "{}");
    const raw = parsed && typeof parsed === "object" && !Array.isArray(parsed) ? parsed as Partial<ChartWorkspace> : {};
    return {
      gridColumns: raw.gridColumns === 3 || raw.gridColumns === 4 ? raw.gridColumns : 2,
      range: raw.range === "3m" || raw.range === "1y" ? raw.range : "6m",
      chartType: raw.chartType === "bars" || raw.chartType === "line" ? raw.chartType : "candles",
      showVolume: raw.showVolume !== false,
      showEma8: raw.showEma8 !== false,
      showEma21: raw.showEma21 !== false,
      showEma60: raw.showEma60 === true,
      showSma50: raw.showSma50 === true,
    };
  } catch {
    return DEFAULT_CHART_WORKSPACE;
  }
}

function sliceCandlesToRange<T extends { time: string }>(candles: T[], range: ChartRange): T[] {
  if (candles.length === 0) return candles;
  const latest = new Date(`${candles[candles.length - 1].time}T00:00:00Z`);
  const lookbackDays = range === "3m" ? 92 : range === "6m" ? 184 : 366;
  const cutoff = new Date(latest);
  cutoff.setUTCDate(cutoff.getUTCDate() - lookbackDays);
  return candles.filter((candle) => new Date(`${candle.time}T00:00:00Z`) >= cutoff);
}

function sliceChartSeries<T extends { time: string }>(points: T[], firstTime: string | undefined): T[] {
  return firstTime ? points.filter((point) => point.time >= firstTime) : [];
}

function resolveTopHitsPreset(name: string, store: TopHitsPresetStore): TopHitsFilterPreset | undefined {
  return BUILT_IN_TOP_HITS_PRESETS[name]?.filters ?? store.presets[name];
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
    const defaultPresetName = typeof rawStore.defaultPresetName === "string"
      && (presets[rawStore.defaultPresetName] || BUILT_IN_TOP_HITS_PRESETS[rawStore.defaultPresetName])
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
    sizePriceFloorOnly: preset.sizePriceFloorOnly !== false,
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

function toneForStrikeZone(value: string | null | undefined) {
  switch (String(value || "").trim().toLowerCase()) {
    case "ready": return "is-strong";
    case "active": return "is-warm";
    case "caution": return "is-caution";
    default: return "is-neutral";
  }
}
