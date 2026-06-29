import { useEffect, useMemo, useState, type FormEvent } from "react";
import { Link, useSearchParams } from "react-router-dom";
import { useAuth } from "../auth/AuthContext";
import { ExclusionDialog } from "../components/ExclusionDialog";
import { LoadingBlock } from "../components/LoadingBlock";
import { Panel } from "../components/Panel";
import { PriceChart, type ChartVisibility } from "../components/PriceChart";
import { fetchJson } from "../lib/api";
import { formatLocalDate } from "../lib/format";
import type { AdminTickerListStatusResponse, CandlePoint, ChartFundamentalsResponse, ChartGexResponse, ChartInsiderResponse, ChartOverlaysResponse, MissingSectorAdminResponse, WatchlistChartResponse } from "../lib/types";

const DEFAULT_CHART_VISIBILITY: ChartVisibility = {
  ema8: true,
  ema21: true,
  sma50: true,
  sma200: true,
  weeklyEma8: true,
  ipoVwap: true,
  marketExtension: true,
  fibOverlay: false,
  gapZones: true,
  htfBox: true,
  rsLine: true,
  rsSignals: true,
  sellSignals: true,
  wyckoffSignals: true,
  wyckoffHoldSignals: true,
  flexSr: false,
};
const CHART_CACHE_PREFIX = "chart-screen-cache-v6";
const EXCLUSION_REASON_OPTIONS = [
  "Bad data quality",
  "Not tradable / structured product",
  "Too illiquid",
  "Too small-cap / low quality",
  "No longer want in scans",
] as const;

function isAbortError(error: unknown): boolean {
  return error instanceof DOMException && error.name === "AbortError";
}

export function ChartsPage() {
  const auth = useAuth();
  const [searchParams, setSearchParams] = useSearchParams();
  const requestedTicker = (searchParams.get("ticker") ?? "").trim().toUpperCase();
  const requestedDate = (searchParams.get("date") ?? "").trim();
  const [tickerInput, setTickerInput] = useState(requestedTicker);
  const [dateInput, setDateInput] = useState(requestedDate);
  const [payload, setPayload] = useState<WatchlistChartResponse | null>(null);
  const [overlayPayload, setOverlayPayload] = useState<ChartOverlaysResponse | null>(null);
  const [gexPayload, setGexPayload] = useState<ChartGexResponse | null>(null);
  const [fundamentalsPayload, setFundamentalsPayload] = useState<ChartFundamentalsResponse | null>(null);
  const [insiderPayload, setInsiderPayload] = useState<ChartInsiderResponse | null>(null);
  const [isLoading, setIsLoading] = useState(false);
  const [isGexLoading, setIsGexLoading] = useState(false);
  const [isFundamentalsLoading, setIsFundamentalsLoading] = useState(false);
  const [isInsiderLoading, setIsInsiderLoading] = useState(false);
  const [notice, setNotice] = useState("");
  const [gexNotice, setGexNotice] = useState("");
  const [fundamentalsNotice, setFundamentalsNotice] = useState("");
  const [insiderNotice, setInsiderNotice] = useState("");
  const [chartVisibility, setChartVisibility] = useState<ChartVisibility>(DEFAULT_CHART_VISIBILITY);
  const [refreshNonce, setRefreshNonce] = useState(0);
  const [tickerListStatus, setTickerListStatus] = useState<AdminTickerListStatusResponse | null>(null);
  const [isTickerListLoading, setIsTickerListLoading] = useState(false);
  const [isListDialogOpen, setIsListDialogOpen] = useState(false);
  const [listDialogMode, setListDialogMode] = useState<"addExclusion" | "removeExclusion">("addExclusion");
  const [isSavingListAction, setIsSavingListAction] = useState(false);
  const [isSavingMyPick, setIsSavingMyPick] = useState(false);
  const [isLaunchingBackfill, setIsLaunchingBackfill] = useState(false);
  const [backfillNotice, setBackfillNotice] = useState("");
  const [myPickNotice, setMyPickNotice] = useState("");
  const [expandedHeroGroup, setExpandedHeroGroup] = useState<string | null>(null);
  const [availableSectorOptions, setAvailableSectorOptions] = useState<string[]>([]);
  const [selectedSectorOption, setSelectedSectorOption] = useState("");
  const [isSectorOptionsLoading, setIsSectorOptionsLoading] = useState(false);
  const [isSavingSector, setIsSavingSector] = useState(false);
  const [sectorNotice, setSectorNotice] = useState("");
  const [syncedHoverTime, setSyncedHoverTime] = useState<string | null>(null);

  useEffect(() => {
    clearAllChartCache();
  }, []);

  useEffect(() => {
    setTickerInput(requestedTicker);
    setDateInput(requestedDate);
  }, [requestedDate, requestedTicker]);

  useEffect(() => {
    setSyncedHoverTime(null);
  }, [payload?.resolved_as_of_date, requestedDate, requestedTicker]);

  useEffect(() => {
    if (!requestedTicker) {
      setPayload(null);
      setOverlayPayload(null);
      setGexPayload(null);
      setFundamentalsPayload(null);
      setInsiderPayload(null);
      setSyncedHoverTime(null);
      setNotice("");
      setGexNotice("");
      setFundamentalsNotice("");
      setInsiderNotice("");
      setBackfillNotice("");
      return;
    }
    setIsLoading(true);
    setNotice("");
    let ignore = false;
    const controller = new AbortController();
    const query = new URLSearchParams({ period: "18mo" });
    if (requestedDate) {
      query.set("asOfDate", requestedDate);
    }
    void fetchJson<WatchlistChartResponse>(`/api/charts/${requestedTicker}?${query.toString()}`, { signal: controller.signal })
      .then((response) => {
        if (ignore) {
          return;
        }
        setPayload(response);
        if (!requestedDate && response.resolved_as_of_date) {
          setDateInput(response.resolved_as_of_date);
        }
        if (requestedDate && response.resolved_as_of_date && response.resolved_as_of_date !== requestedDate) {
          setNotice(`Requested ${requestedDate}. Used last trading day ${response.resolved_as_of_date}.`);
        }
      })
      .catch((error) => {
        if (ignore || isAbortError(error)) {
          return;
        }
        setPayload(null);
        setOverlayPayload(null);
        setNotice(error instanceof Error ? error.message : "Failed to load chart.");
      })
      .finally(() => {
        if (!ignore) {
          setIsLoading(false);
        }
      });
    return () => {
      ignore = true;
      controller.abort();
    };
  }, [refreshNonce, requestedDate, requestedTicker]);

  useEffect(() => {
    if (!requestedTicker || !payload?.resolved_as_of_date) {
      setOverlayPayload(null);
      return;
    }
    let ignore = false;
    const controller = new AbortController();
    const query = new URLSearchParams({ period: "18mo", asOfDate: payload.resolved_as_of_date, includeSetupMarkers: "true" });
    void fetchJson<ChartOverlaysResponse>(`/api/chart-overlays/${requestedTicker}?${query.toString()}`, { signal: controller.signal })
      .then((response) => {
        if (!ignore) {
          setOverlayPayload(response);
        }
      })
      .catch((error) => {
        if (!ignore && !isAbortError(error)) {
          setOverlayPayload(null);
        }
      });
    return () => {
      ignore = true;
      controller.abort();
    };
  }, [payload?.resolved_as_of_date, refreshNonce, requestedTicker]);

  useEffect(() => {
    if (!requestedTicker) {
      setGexPayload(null);
      setGexNotice("");
      return;
    }
    setGexPayload(null);
    setIsGexLoading(true);
    setGexNotice("");
    let ignore = false;
    const controller = new AbortController();
    void fetchJson<ChartGexResponse>(`/api/chart-gex/${requestedTicker}`, { signal: controller.signal })
      .then((response) => {
        if (ignore) {
          return;
        }
        setGexPayload(response);
        if (!response.available && response.error) {
          setGexNotice(response.error);
        }
      })
      .catch((error) => {
        if (ignore || isAbortError(error)) {
          return;
        }
        setGexPayload(null);
        setGexNotice(error instanceof Error ? error.message : "Failed to load GEX.");
      })
      .finally(() => {
        if (!ignore) {
          setIsGexLoading(false);
        }
      });
    return () => {
      ignore = true;
      controller.abort();
    };
  }, [refreshNonce, requestedTicker]);

  useEffect(() => {
    if (!requestedTicker) {
      setInsiderPayload(null);
      setInsiderNotice("");
      return;
    }
    setIsInsiderLoading(true);
    setInsiderNotice("");
    let ignore = false;
    const controller = new AbortController();
    const query = new URLSearchParams({ lookbackDays: "14" });
    if (payload?.resolved_as_of_date) {
      query.set("asOfDate", payload.resolved_as_of_date);
    } else if (requestedDate) {
      query.set("asOfDate", requestedDate);
    }
    void fetchJson<ChartInsiderResponse>(`/api/chart-insider/${requestedTicker}?${query.toString()}`, { signal: controller.signal })
      .then((response) => {
        if (!ignore) {
          setInsiderPayload(response);
        }
      })
      .catch((error) => {
        if (ignore || isAbortError(error)) {
          return;
        }
        setInsiderPayload(null);
        setInsiderNotice(error instanceof Error ? error.message : "Failed to load insider trades.");
      })
      .finally(() => {
        if (!ignore) {
          setIsInsiderLoading(false);
        }
      });
    return () => {
      ignore = true;
      controller.abort();
    };
  }, [payload?.resolved_as_of_date, refreshNonce, requestedDate, requestedTicker]);

  useEffect(() => {
    if (!requestedTicker || !auth.hasCapability("manage_exclusions")) {
      setTickerListStatus(null);
      setIsTickerListLoading(false);
      return;
    }
    setIsTickerListLoading(true);
    let ignore = false;
    const controller = new AbortController();
    void fetchJson<AdminTickerListStatusResponse>(`/api/admin/ticker-lists/${requestedTicker}`, { signal: controller.signal })
      .then((response) => {
        if (!ignore) {
          setTickerListStatus(response);
        }
      })
      .catch((error) => {
        if (!ignore && !isAbortError(error)) {
          setTickerListStatus(null);
        }
      })
      .finally(() => {
        if (!ignore) {
          setIsTickerListLoading(false);
        }
      });
    return () => {
      ignore = true;
      controller.abort();
    };
  }, [auth, requestedTicker, refreshNonce]);

  useEffect(() => {
    if (!requestedTicker) {
      setFundamentalsPayload(null);
      setFundamentalsNotice("");
      return;
    }
    setIsFundamentalsLoading(true);
    setFundamentalsNotice("");
    let ignore = false;
    const controller = new AbortController();
    void fetchJson<ChartFundamentalsResponse>(`/api/chart-fundamentals/${requestedTicker}?earningsLimit=4`, { signal: controller.signal })
      .then((response) => {
        if (ignore) {
          return;
        }
        setFundamentalsPayload(response);
        const earningsStatus = response.diagnostics.earnings.status;
        const holdersStatus = response.diagnostics.holders.status;
        const statisticsStatus = response.diagnostics.statistics.status;
        const optionsStatus = response.diagnostics.options.status;
        if (earningsStatus !== "ok" || holdersStatus !== "ok" || statisticsStatus !== "ok" || optionsStatus !== "ok") {
          setFundamentalsNotice(`Diagnostics: earnings=${earningsStatus}, holders=${holdersStatus}, statistics=${statisticsStatus}, options=${optionsStatus}`);
        }
      })
      .catch((error) => {
        if (ignore || isAbortError(error)) {
          return;
        }
        setFundamentalsPayload(null);
        setFundamentalsNotice(error instanceof Error ? error.message : "Failed to load chart fundamentals.");
      })
      .finally(() => {
        if (!ignore) {
          setIsFundamentalsLoading(false);
        }
      });
    return () => {
      ignore = true;
      controller.abort();
    };
  }, [refreshNonce, requestedTicker]);

  const chartPayload = useMemo<WatchlistChartResponse | null>(() => {
    if (!payload) {
      return null;
    }
    return overlayPayload ? { ...payload, ...overlayPayload } : payload;
  }, [overlayPayload, payload]);

  const chartData = useMemo<CandlePoint[]>(
    () =>
      (chartPayload?.candles ?? []).map((item, index) => ({
        ...item,
        volume: chartPayload?.volume[index]?.value ?? 0,
      })),
    [chartPayload],
  );
  const lastCandle = chartData[chartData.length - 1] ?? null;
  const previousCandle = chartData.length > 1 ? chartData[chartData.length - 2] : null;
  const lastClose = lastCandle?.close ?? null;
  const changePct =
    lastCandle && previousCandle && previousCandle.close > 0
      ? ((lastCandle.close - previousCandle.close) / previousCandle.close) * 100
      : null;
  const latestRsMarker = chartPayload?.rs_markers?.[chartPayload.rs_markers.length - 1] ?? null;
  const dailyRsRatingSeries = chartPayload?.daily_rs_rating ?? [];
  const adr14Pct = useMemo(() => computeAdrPercent(chartData, 14), [chartData]);
  const adr14InRange = adr14Pct != null ? adr14Pct >= 3 && adr14Pct <= 10 : null;
  const atr14 = useMemo(() => computeAtr(chartData, 14), [chartData]);
  const latestMa50 = chartPayload?.ma50?.[chartPayload.ma50.length - 1]?.value ?? null;
  const latestMarketExtension = chartPayload?.market_extension?.latest ?? null;
  const marketExtensionLabel = chartPayload?.market_extension?.config?.label ?? "10W SMA";
  const trendTemplate = chartPayload?.trend_template ?? null;
  const atrMultipleFrom50Ma =
    atr14 != null && latestMa50 != null && Number.isFinite(lastClose ?? NaN)
      ? ((lastClose ?? 0) - latestMa50) / atr14
      : null;
  const hasTrimWarning = atrMultipleFrom50Ma != null ? atrMultipleFrom50Ma >= 3 : false;
  const vcs = chartPayload?.vcs ?? null;
  const sepaDashboard = chartPayload?.sepa_dashboard ?? null;
  const earningsRows = fundamentalsPayload?.earnings_eps_history ?? [];
  const latestFundamentalsSnapshot = fundamentalsPayload?.fundamentals_snapshot ?? null;
  const latestRatingSnapshot = fundamentalsPayload?.rating_snapshot ?? null;
  const latestFundamentalRank = fundamentalsPayload?.fundamental_rank ?? null;
  const latestRatingDiagnostics = fundamentalsPayload?.rating_diagnostics ?? null;
  const technicalIndicatorRatings = fundamentalsPayload?.technical_indicator_ratings ?? {};
  const canslimSnapshot = fundamentalsPayload?.canslim_snapshot ?? null;
  const canslimLetters = useMemo(
    () =>
      ["C", "A", "N", "S", "L", "I", "M"].map((letter) => ({
        letter,
        score: canslimSnapshot?.letter_scores?.[letter] ?? null,
        passed: canslimSnapshot?.letter_passes?.[letter] ?? false,
      })),
    [canslimSnapshot],
  );
  const orderedTechnicalIndicatorRatings = ["1d", "1w"]
    .map((timeframe) => technicalIndicatorRatings[timeframe] ?? null)
    .filter((item): item is NonNullable<typeof item> => item != null);
  const sectorLabel = latestFundamentalsSnapshot?.sector?.trim() || null;
  const industryLabel = latestFundamentalsSnapshot?.industry?.trim() || null;
  const atrExtensionMarkers = useMemo(() => buildAtrExtensionMarkers(chartData, chartPayload?.ma50 ?? [], 14), [chartData, chartPayload?.ma50]);
  const sellIntoStrengthMarkers = useMemo(
    () => buildSellIntoStrengthMarkers(chartData, chartPayload?.ma50 ?? []),
    [chartData, chartPayload?.ma50],
  );
  const dangerSignals = chartPayload?.danger_signals?.signals ?? [];
  const dangerSignalCount = chartPayload?.danger_signals?.active_count ?? 0;
  const highestDangerSeverity = chartPayload?.danger_signals?.highest_severity ?? null;
  const wyckoffMarkers = useMemo(
    () =>
      (overlayPayload?.setup_markers ?? [])
        .map((marker) => {
          if (marker.kind === "mark_daily_extend") {
            return { time: marker.time, label: marker.label, color: "#fb923c", shape: "square" as const, position: "aboveBar" as const };
          }
          if (marker.kind === "wyckoff_buying_climax") {
            return { time: marker.time, label: marker.label, color: "#fb923c", shape: "square" as const, position: "aboveBar" as const };
          }
          if (marker.kind === "wyckoff_buy_signal") {
            return { time: marker.time, label: marker.label, color: "#4ade80", shape: "circle" as const, position: "belowBar" as const };
          }
          if (marker.kind === "wyckoff_sell_signal") {
            return { time: marker.time, label: marker.label, color: "#fb7185", shape: "square" as const, position: "aboveBar" as const };
          }
          if (marker.kind === "wyckoff_hold_signal") {
            return { time: marker.time, label: marker.label, color: "#facc15", shape: "circle" as const, position: "belowBar" as const };
          }
          return null;
        })
        .filter((marker): marker is NonNullable<typeof marker> => marker !== null),
    [overlayPayload?.setup_markers],
  );
  const markDailyExtendMarkers = useMemo(() => wyckoffMarkers.filter((marker) => marker.label === "Mark Extend"), [wyckoffMarkers]);
  const nonMarkMarkers = useMemo(() => wyckoffMarkers.filter((marker) => marker.label !== "Mark Extend"), [wyckoffMarkers]);
  const wyckoffPrimaryMarkers = useMemo(() => nonMarkMarkers.filter((marker) => marker.label !== "HOLD"), [nonMarkMarkers]);
  const wyckoffHoldMarkers = useMemo(() => nonMarkMarkers.filter((marker) => marker.label === "HOLD"), [nonMarkMarkers]);
  const wyckoffClimaxCount = wyckoffPrimaryMarkers.filter((marker) => marker.label === "BC").length;
  const wyckoffBuyCount = wyckoffPrimaryMarkers.filter((marker) => marker.label === "BUY").length;
  const wyckoffSellCount = wyckoffPrimaryMarkers.filter((marker) => marker.label === "SELL").length;
  const wyckoffHoldCount = wyckoffHoldMarkers.length;
  const priceChartExtraMarkers = useMemo(
    () => [
      ...atrExtensionMarkers,
      ...markDailyExtendMarkers,
      ...(chartVisibility.wyckoffSignals ? wyckoffPrimaryMarkers : []),
      ...(chartVisibility.wyckoffHoldSignals ? wyckoffHoldMarkers : []),
      ...(chartVisibility.sellSignals ? sellIntoStrengthMarkers : []),
    ],
    [
      atrExtensionMarkers,
      chartVisibility.sellSignals,
      chartVisibility.wyckoffHoldSignals,
      chartVisibility.wyckoffSignals,
      markDailyExtendMarkers,
      sellIntoStrengthMarkers,
      wyckoffHoldMarkers,
      wyckoffPrimaryMarkers,
    ],
  );
  const chartToggles: Array<{ key: keyof ChartVisibility; label: string }> = [
    { key: "ema8", label: "EMA 8" },
    { key: "ema21", label: "EMA 21" },
    { key: "sma50", label: "SMA 50" },
    { key: "sma200", label: "SMA 200" },
    { key: "weeklyEma8", label: "Weekly 8 EMA" },
    { key: "ipoVwap", label: "IPO VWAP" },
    { key: "marketExtension", label: "10W extension" },
    { key: "fibOverlay", label: "Fib overlay (exp)" },
    { key: "gapZones", label: "Gap zones" },
    { key: "htfBox", label: "HTF box" },
    { key: "rsLine", label: "RS line" },
    { key: "rsSignals", label: "RS markers" },
    { key: "sellSignals", label: "Sell signals" },
    { key: "wyckoffSignals", label: "Wyckoff signals" },
    { key: "wyckoffHoldSignals", label: "Wyckoff hold" },
    { key: "flexSr", label: "Flex SR (exp)" },
  ];
  const canManageExclusions = auth.hasCapability("manage_exclusions");
  const canSyncHistory = auth.hasCapability("sync_history");
  const isAdmin = auth.role === "admin";
  const currentExclusion = tickerListStatus?.exclusion_entry ?? null;
  const showAddExclusionButton = canManageExclusions && !currentExclusion;
  const showRemoveExclusionButton = canManageExclusions && !!currentExclusion;
  const needsSectorAssignment = canManageExclusions && !!requestedTicker && !sectorLabel;
  const showBackfillSection = canSyncHistory && requestedTicker !== "" && chartPayload?.data_source === "internet";

  useEffect(() => {
    if (!needsSectorAssignment) {
      setAvailableSectorOptions([]);
      setSelectedSectorOption("");
      setIsSectorOptionsLoading(false);
      setSectorNotice("");
      return;
    }
    setIsSectorOptionsLoading(true);
    void fetchJson<MissingSectorAdminResponse>("/api/admin/missing-sectors")
      .then((response) => {
        setAvailableSectorOptions(response.available_sectors ?? []);
        const matchedTicker = (response.tickers ?? []).find((item) => item.ticker === requestedTicker);
        setSelectedSectorOption(matchedTicker?.suggested_sector ?? response.available_sectors?.[0] ?? "");
      })
      .catch((error) => {
        setAvailableSectorOptions([]);
        setSelectedSectorOption("");
        setSectorNotice(error instanceof Error ? error.message : "Failed to load sector options.");
      })
      .finally(() => setIsSectorOptionsLoading(false));
  }, [needsSectorAssignment, requestedTicker]);
  const signalGuideGroups = useMemo(
    () =>
      buildSignalGuideGroups({
        chartPayload,
        latestRsMarker,
        atrExtensionCount: atrExtensionMarkers.length,
        sellSignalCount: sellIntoStrengthMarkers.length,
        hasTrimWarning,
      }),
    [atrExtensionMarkers.length, chartPayload, hasTrimWarning, latestRsMarker, sellIntoStrengthMarkers.length],
  );
  const heroStatGroups = useMemo(
    () => [
      {
        id: "context",
        title: "Context",
        description: "Basic context for this chart request: which trading session you are looking at, how many bars loaded, and whether data came from database or internet fallback.",
        items: [
          { label: "Source", value: chartPayload?.data_source ?? "-" },
        ],
      },
      {
        id: "structure",
        title: "Structure",
        description: "Structure groups base quality and setup health. VCS measures contraction quality. SEPA fields like TPR, buy risk, pressure, RPR, and VCP tell whether the broader trend-template and breakout context still look constructive.",
        items: [
          {
            label: "Trend Template",
            value: trendTemplate ? `${trendTemplate.matched ? "PASS" : "FAIL"} ${trendTemplate.criteria_passed}/${trendTemplate.criteria_total}` : "-",
            className: trendTemplate ? `status-pill ${trendTemplate.matched ? "status-success" : "status-unknown"}` : undefined,
          },
          { label: "TT RS", value: trendTemplate ? trendTemplate.rs_rating.toFixed(1) : "-" },
          { label: "VCS", value: formatScore(vcs?.score) },
          {
            label: "CAN V2",
            value:
              fundamentalsPayload?.canslim_v2_score != null
                ? `${Math.round(fundamentalsPayload.canslim_v2_score)}/${Math.round(fundamentalsPayload.canslim_v2_max_score ?? 14)}`
                : "-",
            className:
              fundamentalsPayload?.canslim_v2_score != null
                ? `scanner-score-pill ${toneForCanslimScore(fundamentalsPayload.canslim_v2_score, fundamentalsPayload.canslim_v2_max_score ?? 14)}`
                : undefined,
          },
          {
            label: "VCP Score",
            value:
              fundamentalsPayload?.vcp_score != null
                ? `${fundamentalsPayload.vcp_score.toFixed(1)}${fundamentalsPayload.vcp_rating ? ` ${fundamentalsPayload.vcp_rating}` : ""}`
                : "-",
            className:
              fundamentalsPayload?.vcp_score != null
                ? `scanner-score-pill ${toneForCanslimScore(fundamentalsPayload.vcp_score, 100)}`
                : undefined,
          },
          {
            label: "VCS Stage",
            value: vcs?.stage_label ?? "-",
            className: vcs ? `status-pill ${vcsStageClass(vcs.stage)}` : undefined,
          },
          {
            label: "TPR",
            value: sepaDashboard?.tpr_status ?? "-",
            className: sepaDashboard ? `status-pill ${sepaDashboard.tpr_pass ? "status-success" : "status-unknown"}` : undefined,
          },
          { label: "Buy Risk", value: sepaDashboard?.buy_risk_status ?? "-" },
          { label: "Pressure", value: sepaDashboard?.pressure_status ?? "-" },
          { label: "RPR", value: formatScore(sepaDashboard?.rpr_score) },
          { label: "5D VCP", value: sepaDashboard?.vcp_status ?? "-" },
          { label: "Recent Squeeze", value: sepaDashboard?.recent_vcp_signal_date ?? "-" },
        ],
      },
      {
        id: "extension",
        title: "Extension / Risk",
        description: "Extension and risk fields tell how stretched price is. Use them to judge chase risk, trim pressure, and whether the stock is still in a healthy daily range versus getting too extended.",
        items: [
          { label: marketExtensionLabel, value: formatPercent(latestMarketExtension?.extension_pct) },
          {
            label: "10W State",
            value: formatMarketExtensionState(latestMarketExtension?.state),
            className: latestMarketExtension ? `status-pill ${marketExtensionStateClass(latestMarketExtension.state)}` : undefined,
          },
          {
            label: "ADR14",
            value: formatPercent(adr14Pct),
            className: adr14InRange == null ? undefined : `adr-badge ${adr14InRange ? "is-in-range" : "is-out-of-range"}`,
          },
          { label: "ATR14", value: formatPrice(atr14) },
          { label: "ATR x 50MA", value: formatAtrMultiple(atrMultipleFrom50Ma) },
          {
            label: "Danger",
            value: dangerSignalCount > 0 ? `${dangerSignalCount} active` : "0 active",
            className: dangerSignalCount > 0 ? `status-pill ${dangerSignalSeverityClass(highestDangerSeverity)}` : undefined,
          },
          {
            label: "Trim Warn",
            value: hasTrimWarning ? ">= 3x ATR" : "Normal",
            className: hasTrimWarning ? "atr-badge is-warning" : undefined,
          },
        ],
      },
      {
        id: "fundamentals",
        title: "Fundamentals",
        description: "Quick fundamental pressure-check. These numbers help frame sponsorship, growth quality, and near-term event risk around the technical setup.",
        items: [
          { label: "FA Rank", value: latestFundamentalRank?.current_rank != null ? `#${latestFundamentalRank.current_rank}` : "-" },
          { label: "Inst Float", value: formatPercent(fundamentalsPayload?.holders_float_held_by_institutions_pct) },
          { label: "Rev YoY", value: formatPercent(fundamentalsPayload?.revenue_yoy_pct) },
          { label: "Imp Move", value: formatPercent(fundamentalsPayload?.implied_move?.percent_move) },
        ],
      },
    ],
    [
      adr14InRange,
      adr14Pct,
      atr14,
      atrMultipleFrom50Ma,
      chartData.length,
      chartPayload?.data_source,
      chartPayload?.resolved_as_of_date,
      fundamentalsPayload?.holders_float_held_by_institutions_pct,
      fundamentalsPayload?.implied_move?.percent_move,
      fundamentalsPayload?.revenue_yoy_pct,
      fundamentalsPayload?.canslim_v2_max_score,
      fundamentalsPayload?.canslim_v2_score,
      fundamentalsPayload?.vcp_rating,
      fundamentalsPayload?.vcp_score,
      hasTrimWarning,
      dangerSignalCount,
      highestDangerSeverity,
      latestMarketExtension,
      marketExtensionLabel,
      latestFundamentalRank,
      sepaDashboard,
      trendTemplate,
      vcs,
    ],
  );

  const handleSubmit = (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    const nextTicker = tickerInput.trim().toUpperCase();
    if (!nextTicker) {
      setNotice("Ticker is required.");
      return;
    }
    const nextParams = new URLSearchParams();
    nextParams.set("ticker", nextTicker);
    if (dateInput.trim()) {
      nextParams.set("date", dateInput.trim());
    }
    setSearchParams(nextParams, { replace: true });
  };

  const handleUseLatestTradingDay = () => {
    setDateInput("");
    const nextTicker = tickerInput.trim().toUpperCase();
    if (!nextTicker) {
      return;
    }
    setSearchParams(new URLSearchParams({ ticker: nextTicker }), { replace: true });
  };

  const handleRefresh = () => {
    setRefreshNonce((current) => current + 1);
  };

  const handleTickerListAction = async (reason: string) => {
    if (!requestedTicker) {
      return;
    }
    setIsSavingListAction(true);
    try {
      if (listDialogMode === "addExclusion") {
        await fetchJson<{ ok: boolean }>("/api/admin/exclusions", {
          method: "POST",
          body: JSON.stringify({
            ticker: requestedTicker,
            reason,
          }),
        });
        setNotice(`${requestedTicker} added to exclusions.`);
      } else {
        await fetchJson<{ ok: boolean }>(`/api/admin/exclusions/${requestedTicker}/remove`, {
          method: "POST",
          body: JSON.stringify({ reason }),
        });
        setNotice(`${requestedTicker} removed from removable exclusions.`);
      }
      setIsListDialogOpen(false);
      setRefreshNonce((current) => current + 1);
    } catch (error) {
      setNotice(error instanceof Error ? error.message : "Failed to update ticker list.");
    } finally {
      setIsSavingListAction(false);
    }
  };

  const handleBackfillTicker = async () => {
    if (!requestedTicker) {
      return;
    }
    setIsLaunchingBackfill(true);
    setBackfillNotice("");
    try {
      const response = await fetchJson<{ ok: boolean; job_id: string }>("/api/admin/history-sync", {
        method: "POST",
        body: JSON.stringify({
          start_date: "2020-01-01",
          tickers: [requestedTicker],
          chunk_size: 1,
          include_excluded_tickers: true,
        }),
      });
      setBackfillNotice(`Backfill job launched: ${response.job_id}`);
    } catch (error) {
      setBackfillNotice(error instanceof Error ? error.message : "Failed to launch ticker backfill.");
    } finally {
      setIsLaunchingBackfill(false);
    }
  };

  const handleAssignSector = async () => {
    if (!requestedTicker) {
      return;
    }
    const sector = selectedSectorOption.trim();
    if (!sector) {
      setSectorNotice("Select a sector before saving.");
      return;
    }
    setIsSavingSector(true);
    setSectorNotice("");
    try {
      await fetchJson<{ ok: boolean; entry: { ticker: string; sector: string } }>(`/api/admin/ticker-sectors/${requestedTicker}`, {
        method: "POST",
        body: JSON.stringify({ sector }),
      });
      setSectorNotice(`${requestedTicker} sector set to ${sector}.`);
      setRefreshNonce((current) => current + 1);
    } catch (error) {
      setSectorNotice(error instanceof Error ? error.message : "Failed to update sector.");
    } finally {
      setIsSavingSector(false);
    }
  };

  const handleAddToMyPicks = async () => {
    if (!requestedTicker) {
      return;
    }
    setIsSavingMyPick(true);
    setMyPickNotice("");
    try {
      await fetchJson<{ ok: boolean; pick: { ticker: string } }>("/api/admin/my-picks", {
        method: "POST",
        body: JSON.stringify({
          ticker: requestedTicker,
          notes: requestedDate ? `Added from chart view (${requestedDate}).` : "Added from chart view.",
        }),
      });
      setMyPickNotice(`${requestedTicker} added to My Picks.`);
    } catch (error) {
      setMyPickNotice(error instanceof Error ? error.message : "Failed to add ticker to My Picks.");
    } finally {
      setIsSavingMyPick(false);
    }
  };

  return (
    <div className="page-grid charts-page">
      <section className="hero-strip">
        <div>
          <div className="hero-symbol-row">
            <h1>{requestedTicker || "Chart"}</h1>
            {chartPayload?.benchmark_ticker ? <span className="ticker-exchange">RS vs {chartPayload.benchmark_ticker}</span> : null}
          </div>
          <div className="hero-price-row">
            <span className="hero-price">{formatPrice(lastClose)}</span>
            {changePct != null ? (
              <span className={`hero-change ${changePct >= 0 ? "positive" : "negative"}`}>
                {changePct >= 0 ? "+" : ""}
                {changePct.toFixed(2)}%
              </span>
            ) : (
              <span className="hero-change neutral">Select ticker to load chart</span>
            )}
          </div>
          {requestedTicker ? (
            <div style={{ marginTop: 10 }}>
              <p className="panel-copy" style={{ marginBottom: needsSectorAssignment ? 10 : 0 }}>
                Sector: {sectorLabel ?? "Missing"}
                {" · "}
                Industry: {industryLabel ?? "Missing"}
              </p>
              {needsSectorAssignment ? (
                <div className="button-row" style={{ alignItems: "center", flexWrap: "wrap" }}>
                  <select
                    value={selectedSectorOption}
                    onChange={(event) => setSelectedSectorOption(event.target.value)}
                    disabled={isSectorOptionsLoading || isSavingSector || availableSectorOptions.length === 0}
                  >
                    <option value="">Select sector</option>
                    {availableSectorOptions.map((sector) => (
                      <option key={sector} value={sector}>
                        {sector}
                      </option>
                    ))}
                  </select>
                  <button
                    className="ghost-button"
                    type="button"
                    onClick={() => void handleAssignSector()}
                    disabled={isSectorOptionsLoading || isSavingSector || !selectedSectorOption.trim()}
                  >
                    {isSavingSector ? "Saving Sector..." : "Assign Sector"}
                  </button>
                  <span className="panel-copy">
                    {isSectorOptionsLoading ? "Loading sector options..." : "Admin only: sector missing for this ticker."}
                  </span>
                </div>
              ) : null}
              {sectorNotice ? <p className="panel-copy">{sectorNotice}</p> : null}
            </div>
          ) : null}
          {canManageExclusions ? (
            <div className="button-row" style={{ marginTop: 12 }}>
              <button
                className="ghost-button"
                type="button"
                disabled={!requestedTicker || isSavingMyPick}
                onClick={() => void handleAddToMyPicks()}
              >
                {isSavingMyPick ? "Adding To My Picks..." : "Add To My Picks"}
              </button>
              {showAddExclusionButton ? (
                <button
                  className="ghost-button"
                  type="button"
                  disabled={!requestedTicker || isTickerListLoading}
                  onClick={() => {
                    setListDialogMode("addExclusion");
                    setIsListDialogOpen(true);
                  }}
                >
                  Add To Exclusion
                </button>
              ) : null}
              {showRemoveExclusionButton ? (
                <button
                  className="ghost-button"
                  type="button"
                  disabled={!requestedTicker || !currentExclusion?.removable || isTickerListLoading}
                  onClick={() => {
                    setListDialogMode("removeExclusion");
                    setIsListDialogOpen(true);
                  }}
                >
                  {currentExclusion?.removable ? "Remove From Exclusion" : "Excluded (Not Removable Here)"}
                </button>
              ) : null}
            </div>
          ) : null}
          {myPickNotice ? <p className="panel-copy">{myPickNotice}</p> : null}
          {notice ? <p className="panel-copy">{notice}</p> : <p className="panel-copy">Standalone ticker chart with RS line, SMA overlays, 10W extension overlay, gap zones, HTF box, fearzone panel, and SEPA dashboard snapshot.</p>}
          {canManageExclusions && requestedTicker ? (
            <p className="panel-copy">
              {isTickerListLoading
                ? "Checking admin ticker-list status..."
                : currentExclusion
                  ? `${requestedTicker} is in exclusion list via ${currentExclusion.sources.join(", ")}.`
                  : `${requestedTicker} is not in exclusion list.`}
            </p>
          ) : null}
        </div>
        <div className="hero-as-of">
          <span className="eyebrow">As Of</span>
          <strong>{chartPayload?.resolved_as_of_date ?? "Latest trading day"}</strong>
        </div>
        <div className="hero-stats">
          {heroStatGroups.map((group) => {
            const isExpanded = expandedHeroGroup === group.id;
            return (
              <div key={group.id} className="hero-stat-group">
                <div className="hero-stat-group-head">
                  <span className="eyebrow">{group.title}</span>
                  <button
                    type="button"
                    className="hero-stat-help"
                    title={group.description}
                    aria-label={`${group.title} meaning`}
                    onClick={() => setExpandedHeroGroup((current) => (current === group.id ? null : group.id))}
                  >
                    ?
                  </button>
                </div>
                <div className="hero-stat-group-grid">
                  {group.items.map((item) => (
                    <div key={`${group.id}-${item.label}`} className="hero-stat-item">
                      <span className="eyebrow">{item.label}</span>
                      <strong className={item.className}>{item.value}</strong>
                    </div>
                  ))}
                </div>
                {isExpanded ? <p className="hero-stat-help-copy">{group.description}</p> : null}
              </div>
            );
          })}
        </div>
      </section>

      <Panel title="Load Chart" aside={<span className="eyebrow">Default date snaps to latest trading session</span>}>
        <form className="run-toolbar" onSubmit={handleSubmit}>
          <div className="run-params-grid">
            <label className="field">
              <span>Ticker</span>
              <input
                type="text"
                value={tickerInput}
                onChange={(event) => setTickerInput(event.target.value.toUpperCase())}
                placeholder="NVDA"
              />
            </label>
            <label className="field">
              <span>As Of Date</span>
              <input type="date" value={dateInput} onChange={(event) => setDateInput(event.target.value)} />
            </label>
          </div>
          <div className="button-row">
            <button className="primary-button" type="submit">
              Load Chart
            </button>
            <button className="ghost-button" type="button" onClick={handleUseLatestTradingDay}>
              Use Latest Trading Day
            </button>
            <button className="ghost-button" type="button" onClick={handleRefresh} disabled={!requestedTicker}>
              Refresh
            </button>
          </div>
        </form>
        {requestedTicker ? <p className="panel-copy">Refresh re-runs live chart, fundamentals, and insider requests for this ticker.</p> : null}
      </Panel>

      <Panel title="Options GEX" aside={<span className="eyebrow">CBOE delayed all-expiry profile</span>}>
        {!requestedTicker ? <p className="panel-copy">Load ticker to inspect latest GEX profile.</p> : null}
        {requestedTicker && isGexLoading ? <LoadingBlock label="Loading options GEX…" compact /> : null}
        {requestedTicker && !isGexLoading && !gexPayload ? <p className="panel-copy">No GEX response returned for this ticker.</p> : null}
        {requestedTicker && gexNotice ? <p className="panel-copy">{gexNotice}</p> : null}
        {gexPayload?.available ? (
          <>
            <p className="panel-copy">
              {gexPayload.gex_label ?? "Unavailable"}
              {" · "}
              Spot {formatPrice(gexPayload.spot ?? null)}
              {" · "}
              Net GEX {formatCompactCurrency(gexPayload.net_gex)}
              {" · "}
              Flip {formatPrice(gexPayload.gamma_flip ?? null)}
              {" · "}
              {formatFlipDistance(gexPayload.distance_to_flip_pct)}
            </p>
            <p className="panel-copy">
              Call wall {formatPrice(gexPayload.call_wall ?? null)}
              {" · "}
              Put wall {formatPrice(gexPayload.put_wall ?? null)}
              {" · "}
              ATM pin {formatPrice(gexPayload.atm_pin_strike ?? null)}
              {" · "}
              Put/Call OI {formatRatio(gexPayload.put_call_oi_ratio)}
            </p>
            <p className="panel-copy">
              Next expiry {gexPayload.next_expiry || "--"}
              {" · "}
              Next monthly {gexPayload.next_monthly_expiry || "--"}
              {" · "}
              Strikes {gexPayload.strike_count ?? "--"}
              {" · "}
              As of {gexPayload.as_of || "--"}
            </p>
            {gexPayload.summary ? <p className="panel-copy">{gexPayload.summary}</p> : null}
            {gexPayload.methodology ? <p className="panel-copy">{gexPayload.methodology}</p> : null}
            {gexPayload.plots?.v2 ? (
              <div className="gex-v2-section">
                <div className="chart-rs-header">GEX Plot Chart V2</div>
                <p className="panel-copy">Explainer-style net GEX by strike with gamma flip, call wall, and put wall.</p>
                <div className="gex-v2-card" dangerouslySetInnerHTML={{ __html: gexPayload.plots.v2 }} />
              </div>
            ) : null}
            {gexPayload.plots ? (
              <div className="list-grid">
                <div className="chart-card" dangerouslySetInnerHTML={{ __html: gexPayload.plots.absolute }} />
                <div className="chart-card" dangerouslySetInnerHTML={{ __html: gexPayload.plots.by_option_type }} />
                <div className="chart-card" style={{ gridColumn: "1 / -1" }} dangerouslySetInnerHTML={{ __html: gexPayload.plots.profile }} />
              </div>
            ) : null}
          </>
        ) : null}
      </Panel>

      {showBackfillSection ? (
        <Panel title="Admin Backfill" aside={<span className="eyebrow">Internet fallback detected</span>}>
          <p className="panel-copy">
            This chart loaded from internet fallback instead of full DB coverage. You can queue a targeted Postgres backfill for{" "}
            <strong>{requestedTicker}</strong>.
          </p>
          {currentExclusion ? (
            <p className="panel-copy">
              {requestedTicker} is currently excluded, so this repair path will explicitly bypass exclusion filtering for the one-off backfill job.
            </p>
          ) : null}
          <div className="button-row">
            <button className="primary-button" type="button" onClick={() => void handleBackfillTicker()} disabled={isLaunchingBackfill}>
              {isLaunchingBackfill ? "Launching Backfill..." : `Backfill ${requestedTicker}`}
            </button>
          </div>
          {backfillNotice ? <p className="panel-copy">{backfillNotice}</p> : null}
        </Panel>
      ) : null}

      <Panel title="Ticker Rating" aside={<span className="eyebrow">Latest DB-backed rating snapshot</span>}>
        {!requestedTicker ? <p className="panel-copy">Load ticker to inspect latest rating snapshot and diagnostics.</p> : null}
        {requestedTicker && isFundamentalsLoading ? <LoadingBlock label="Loading ticker rating…" compact /> : null}
        {requestedTicker && !isFundamentalsLoading && !latestRatingSnapshot ? (
          <p className="panel-copy">No latest rating snapshot returned for this ticker yet.</p>
        ) : null}
        {latestRatingSnapshot ? (
          <>
            <p className="panel-copy">
              Loaded from Postgres rating snapshots and latest fundamentals snapshot, not from the Yahoo scrape block below.
            </p>
            <p className="panel-copy">
              Status: {latestRatingSnapshot.rating_status || "-"}
              {" · "}
              Overall: {formatMetric(latestRatingSnapshot.overall_rating)}
              {latestFundamentalRank?.current_rank != null ? (
                <>
                  {" · "}
                  FA Rank: #{latestFundamentalRank.current_rank}/{latestFundamentalRank.list_limit}
                </>
              ) : null}
              {" · "}
              As Of: {formatLocalDate(latestRatingSnapshot.as_of_date)}
              {" · "}
              Source: {latestFundamentalsSnapshot?.source || "-"}
            </p>
            <div className="data-table-responsive">
              <table className="data-table">
                <thead>
                  <tr>
                    <th>Category</th>
                    <th>Grade</th>
                    <th>Score</th>
                  </tr>
                </thead>
                <tbody>
                  <tr>
                    <td>Valuation</td>
                    <td>{latestRatingSnapshot.valuation_grade || "-"}</td>
                    <td>{formatMetric(latestRatingSnapshot.valuation_score)}</td>
                  </tr>
                  <tr>
                    <td>Profitability</td>
                    <td>{latestRatingSnapshot.profitability_grade || "-"}</td>
                    <td>{formatMetric(latestRatingSnapshot.profitability_score)}</td>
                  </tr>
                  <tr>
                    <td>Growth</td>
                    <td>{latestRatingSnapshot.growth_grade || "-"}</td>
                    <td>{formatMetric(latestRatingSnapshot.growth_score)}</td>
                  </tr>
                  <tr>
                    <td>Performance</td>
                    <td>{latestRatingSnapshot.performance_grade || "-"}</td>
                    <td>{formatMetric(latestRatingSnapshot.performance_score)}</td>
                  </tr>
                </tbody>
              </table>
            </div>
            <p className="panel-copy">
              {latestFundamentalsSnapshot
                ? `${[latestFundamentalsSnapshot.sector, latestFundamentalsSnapshot.industry].filter(Boolean).join(" / ") || "No sector or industry"} · parse ${latestFundamentalsSnapshot.parse_status || "-"}`
                : "No fundamentals snapshot metadata."}
            </p>
            {latestRatingSnapshot.rating_status_reason ? <p className="panel-copy">{latestRatingSnapshot.rating_status_reason}</p> : null}
            {latestRatingDiagnostics && (latestRatingDiagnostics.missing_metric_names.length > 0 || latestRatingDiagnostics.insufficient_baseline_metrics.length > 0) ? (
              <details>
                <summary className="panel-copy">Rating diagnostics</summary>
                <pre className="panel-copy" style={{ whiteSpace: "pre-wrap" }}>
                  {JSON.stringify(latestRatingDiagnostics, null, 2)}
                </pre>
              </details>
            ) : null}
          </>
        ) : null}
      </Panel>

      <Panel title="CANSLIM Score" aside={<span className="eyebrow">Finviz + local technical scored proxy</span>}>
        {!requestedTicker ? <p className="panel-copy">Load ticker to inspect latest CANSLIM letter scores.</p> : null}
        {requestedTicker && isFundamentalsLoading ? <LoadingBlock label="Loading CANSLIM score…" compact /> : null}
        {requestedTicker && !isFundamentalsLoading && !canslimSnapshot ? (
          <p className="panel-copy">No CANSLIM snapshot available yet for this ticker.</p>
        ) : null}
        {canslimSnapshot ? (
          <>
            <p className="panel-copy">
              Total: {canslimSnapshot.score}/{canslimSnapshot.max_score}
              {" · "}
              As Of: {formatLocalDate(canslimSnapshot.as_of_date)}
              {" · "}
              Market: {canslimSnapshot.letter_passes.M ? "constructive" : "weak"}
            </p>
            <div className="button-row" style={{ alignItems: "center", flexWrap: "wrap" }}>
              <span className={`scanner-score-pill ${toneForCanslimScore(canslimSnapshot.score, canslimSnapshot.max_score)}`}>
                CANSLIM {canslimSnapshot.score}/{canslimSnapshot.max_score}
              </span>
              {canslimLetters.map((item) => (
                <span key={item.letter} className={`scanner-score-pill ${toneForCanslimLetter(item.score)}`}>
                  {item.letter}:{item.score ?? "--"} {item.passed ? "pass" : "miss"}
                </span>
              ))}
            </div>
            <p className="panel-copy">
              Leader flags: {canslimSnapshot.leader_flags.length > 0 ? canslimSnapshot.leader_flags.join(", ") : "none"}
            </p>
            <div className="data-table-responsive">
              <table className="data-table">
                <thead>
                  <tr>
                    <th>Metric</th>
                    <th>Value</th>
                  </tr>
                </thead>
                <tbody>
                  <tr>
                    <td>EPS Q/Q</td>
                    <td>{formatPercent(toNullableNumber(canslimSnapshot.metrics.eps_qq_pct))}</td>
                  </tr>
                  <tr>
                    <td>Sales Q/Q</td>
                    <td>{formatPercent(toNullableNumber(canslimSnapshot.metrics.sales_qq_pct))}</td>
                  </tr>
                  <tr>
                    <td>EPS This Y</td>
                    <td>{formatPercent(toNullableNumber(canslimSnapshot.metrics.eps_this_y_pct))}</td>
                  </tr>
                  <tr>
                    <td>EPS Next 5Y</td>
                    <td>{formatPercent(toNullableNumber(canslimSnapshot.metrics.eps_next_5y_pct))}</td>
                  </tr>
                  <tr>
                    <td>ROE</td>
                    <td>{formatPercent(toNullableNumber(canslimSnapshot.metrics.roe_pct))}</td>
                  </tr>
                  <tr>
                    <td>Inst Ownership</td>
                    <td>{formatPercent(toNullableNumber(canslimSnapshot.metrics.institutional_ownership_pct))}</td>
                  </tr>
                  <tr>
                    <td>Inst Trans</td>
                    <td>{formatPercent(toNullableNumber(canslimSnapshot.metrics.institutional_transactions_pct))}</td>
                  </tr>
                  <tr>
                    <td>Insider Ownership</td>
                    <td>{formatPercent(toNullableNumber(canslimSnapshot.metrics.insider_ownership_pct))}</td>
                  </tr>
                  <tr>
                    <td>Insider Trans</td>
                    <td>{formatPercent(toNullableNumber(canslimSnapshot.metrics.insider_transactions_pct))}</td>
                  </tr>
                  <tr>
                    <td>Leadership</td>
                    <td>{formatMetric(toNullableNumber(canslimSnapshot.metrics.leadership_score))}</td>
                  </tr>
                  <tr>
                    <td>Distance To 52W High</td>
                    <td>{formatPercent(toNullableNumber(canslimSnapshot.metrics.distance_from_52w_high_pct))}</td>
                  </tr>
                  <tr>
                    <td>20D Avg Volume</td>
                    <td>{formatInteger(toNullableNumber(canslimSnapshot.metrics.avg_volume_20d))}</td>
                  </tr>
                  <tr>
                    <td>Up / Down Volume</td>
                    <td>{formatMetric(toNullableNumber(canslimSnapshot.metrics.up_down_volume_ratio_20d))}</td>
                  </tr>
                </tbody>
              </table>
            </div>
            {canslimSnapshot.reasons.length > 0 ? (
              <p className="panel-copy">Reasons: {canslimSnapshot.reasons.slice(0, 6).join(" | ")}</p>
            ) : null}
          </>
        ) : null}
      </Panel>

      <Panel title="Technical Ratings" aside={<span className="eyebrow">Latest DB-backed daily / weekly composite labels</span>}>
        {!requestedTicker ? <p className="panel-copy">Load ticker to inspect multi-timeframe technical ratings.</p> : null}
        {requestedTicker && isFundamentalsLoading ? <LoadingBlock label="Loading technical ratings…" compact /> : null}
        {requestedTicker && !isFundamentalsLoading && orderedTechnicalIndicatorRatings.length === 0 ? (
          <p className="panel-copy">No multi-timeframe technical ratings returned for this ticker yet.</p>
        ) : null}
        {orderedTechnicalIndicatorRatings.length > 0 ? (
          <>
            <p className="panel-copy">TradingView-style composite score across 26 indicator conditions, stored per timeframe.</p>
            <div className="data-table-responsive">
              <table className="data-table">
                <thead>
                  <tr>
                    <th>Timeframe</th>
                    <th>Label</th>
                    <th>Overall</th>
                    <th>MAs</th>
                    <th>Oscillators</th>
                    <th>Status</th>
                  </tr>
                </thead>
                <tbody>
                  {orderedTechnicalIndicatorRatings.map((rating) => (
                    <tr key={`${rating.timeframe}-${rating.as_of_date}`}>
                      <td>{rating.timeframe.toUpperCase()}</td>
                      <td>{rating.rating_label ?? "-"}</td>
                      <td>{formatMetric(rating.overall_score)}</td>
                      <td>{formatMetric(rating.moving_average_score)}</td>
                      <td>{formatMetric(rating.oscillator_score)}</td>
                      <td>{rating.technical_status ?? "-"}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </>
        ) : null}
      </Panel>

      <Panel title="EPS History" aside={<span className="eyebrow">Yahoo internet scrape for estimate, reported, surprise</span>}>
        {!requestedTicker ? <p className="panel-copy">Load ticker to inspect recent earnings EPS rows.</p> : null}
        {requestedTicker && isFundamentalsLoading ? <LoadingBlock label="Loading chart fundamentals…" compact /> : null}
        {requestedTicker ? (
          <p className="panel-copy">
            Float held by institutions: {formatPercent(fundamentalsPayload?.holders_float_held_by_institutions_pct)}
          </p>
        ) : null}
        {requestedTicker ? (
          <p className="panel-copy">
            Revenue YoY: {formatPercent(fundamentalsPayload?.revenue_yoy_pct)}
            {" · "}
            Earnings YoY: {formatPercent(fundamentalsPayload?.earnings_yoy_pct)}
          </p>
        ) : null}
        {requestedTicker ? (
          <p className="panel-copy">
            ATM-ish implied move: {formatPercent(fundamentalsPayload?.implied_move?.percent_move)}
            {fundamentalsPayload?.implied_move?.dollar_move != null ? ` (${formatPrice(fundamentalsPayload.implied_move.dollar_move)})` : ""}
            {fundamentalsPayload?.implied_move?.strike != null ? ` at strike ${fundamentalsPayload.implied_move.strike.toFixed(2)}` : ""}
          </p>
        ) : null}
        {fundamentalsNotice ? <p className="panel-copy">{fundamentalsNotice}</p> : null}
        {requestedTicker && !isFundamentalsLoading && earningsRows.length === 0 ? (
          <p className="panel-copy">No EPS rows returned from Yahoo scrape for this ticker.</p>
        ) : null}
        {earningsRows.length > 0 ? (
          <div className="data-table-responsive">
            <table className="data-table">
              <thead>
                <tr>
                  <th>Date</th>
                  <th>EPS Estimate</th>
                  <th>Reported EPS</th>
                  <th>Surprise (%)</th>
                </tr>
              </thead>
              <tbody>
                {earningsRows.map((row) => (
                  <tr key={row.date}>
                    <td>{row.date}</td>
                    <td>{formatMetric(row.eps_estimate)}</td>
                    <td>{formatMetric(row.reported_eps)}</td>
                    <td>{formatPercent(row.surprise_pct)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        ) : null}
        {fundamentalsPayload ? (
          <details>
            <summary className="panel-copy">Scrape diagnostics</summary>
            <pre className="panel-copy" style={{ whiteSpace: "pre-wrap" }}>
              {JSON.stringify(fundamentalsPayload.diagnostics, null, 2)}
            </pre>
          </details>
        ) : null}
      </Panel>

      <Panel title="Recent Insider Trades" aside={<span className="eyebrow">SEC Form 4 cache, default last 14 days</span>}>
        {!requestedTicker ? <p className="panel-copy">Load ticker to inspect recent insider activity.</p> : null}
        {requestedTicker && isInsiderLoading ? <LoadingBlock label="Loading insider trades…" compact /> : null}
        {requestedTicker ? (
          <p className="panel-copy">
            Window: {insiderPayload?.window_start_date ?? "--"} to {insiderPayload?.window_end_date ?? "--"}
            {" · "}
            Net: {formatSignedCurrency(insiderPayload?.summary.net_amount)}
            {" · "}
            Rows: {insiderPayload?.summary.total_count ?? 0}
            {insiderPayload?.cache_status ? ` · Cache ${insiderPayload.cache_status}` : ""}
            {insiderPayload?.fetch_status ? ` · Fetch ${insiderPayload.fetch_status}` : ""}
          </p>
        ) : null}
        {requestedTicker && insiderPayload?.generated_at ? (
          <p className="panel-copy">Cache generated: {formatDateTime(insiderPayload.generated_at)}</p>
        ) : null}
        {insiderPayload?.notice ? <p className="panel-copy">{insiderPayload.notice}</p> : null}
        {insiderNotice ? <p className="panel-copy">{insiderNotice}</p> : null}
        {requestedTicker && !isInsiderLoading && (insiderPayload?.entries.length ?? 0) === 0 ? (
          <p className="panel-copy">No cached insider buys or sells in this window.</p>
        ) : null}
        {(insiderPayload?.entries.length ?? 0) > 0 ? (
          <div className="data-table-responsive">
            <table className="data-table">
              <thead>
                <tr>
                  <th>Trade Date</th>
                  <th>Owner</th>
                  <th>Role</th>
                  <th>Type</th>
                  <th>Gross</th>
                  <th>Shares</th>
                  <th>Price</th>
                  <th>Plan</th>
                  <th>Source</th>
                </tr>
              </thead>
              <tbody>
                {insiderPayload?.entries.map((row, index) => (
                  <tr key={`${row.owner_name}-${row.transaction_date}-${row.type}-${index}`}>
                    <td>{row.transaction_date ?? row.filing_date ?? "--"}</td>
                    <td>{row.owner_name || "--"}</td>
                    <td>{row.position || "--"}</td>
                    <td className={row.type === "BUY" ? "metric-positive" : row.type === "SELL" ? "metric-negative" : ""}>{row.type}</td>
                    <td>{formatCurrency(row.gross_amount)}</td>
                    <td>{formatInteger(row.shares)}</td>
                    <td>{formatPrice(row.price ?? null)}</td>
                    <td>{row.is_10b5_1 ? "10b5-1" : "Open"}</td>
                    <td>
                      {row.source_url ? (
                        <a href={row.source_url} target="_blank" rel="noreferrer">
                          SEC
                        </a>
                      ) : (
                        "--"
                      )}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        ) : null}
      </Panel>

      <Panel
        title="Candles"
        aside={
          <div className="watchlist-panel-aside">
            <div className="legend-row legend-row-compact">
              <span className="legend-marker legend-marker-gap" aria-hidden="true" />
              <span>Gap</span>
              <span className="legend-marker legend-marker-rs" aria-hidden="true" />
              <span>RS NH</span>
              <span className="legend-marker legend-marker-rs-before" aria-hidden="true" />
              <span>RS NH before price</span>
            </div>
            <Link className="ghost-button" to="/guide">
              Open Guide
            </Link>
          </div>
        }
      >
        <div className="chart-toolbar">
          {chartToggles.map((toggle) => (
            <label key={toggle.key} className="chart-toggle">
              <input
                type="checkbox"
                checked={chartVisibility[toggle.key]}
                onChange={() =>
                  setChartVisibility((current) => ({
                    ...current,
                    [toggle.key]: !current[toggle.key],
                  }))
                }
              />
              <span>{toggle.label}</span>
            </label>
          ))}
        </div>
        {isLoading ? <LoadingBlock label={`Loading chart for ${requestedTicker}…`} /> : null}
        {!isLoading && !requestedTicker ? <p className="panel-copy">Enter ticker, pick date if needed, load chart.</p> : null}
        {!isLoading && requestedTicker && chartData.length === 0 ? <p className="panel-copy">No chart data returned for this request.</p> : null}
        {chartData.length > 0 ? (
          <>
            <PriceChart
              ticker={requestedTicker}
              candles={chartData}
              overlays={chartPayload ?? undefined}
              extraMarkers={priceChartExtraMarkers}
              visibility={chartVisibility}
              forceFearzonePanel
              hoveredTime={syncedHoverTime}
              onHoverTimeChange={setSyncedHoverTime}
            />
            <div className="chart-annotation-strip">
              {chartPayload?.resolved_as_of_date ? <span className="chart-pill chart-pill-event">As Of {chartPayload.resolved_as_of_date}</span> : null}
              {chartPayload?.benchmark_ticker ? <span className="chart-pill chart-pill-rs">RS vs {chartPayload.benchmark_ticker}</span> : null}
              {latestRsMarker ? (
                <span className="chart-pill chart-pill-rs">
                  {latestRsMarker.kind === "daily_new_high_before_price" ? "RS new high before price" : "RS new high"}
                </span>
              ) : null}
              {chartPayload?.data_source ? <span className="chart-pill chart-pill-setup">Source {chartPayload.data_source}</span> : null}
              {latestMarketExtension ? (
                <span className={`chart-pill ${marketExtensionChartPillClass(latestMarketExtension.state)}`}>
                  {marketExtensionLabel} {formatPercent(latestMarketExtension.extension_pct)}
                </span>
              ) : null}
              {latestMarketExtension?.distance != null ? (
                <span className="chart-pill chart-pill-setup">Dist {formatPrice(latestMarketExtension.distance)}</span>
              ) : null}
              {markDailyExtendMarkers.length > 0 ? <span className="chart-pill chart-pill-event">{markDailyExtendMarkers.length} Mark extend</span> : null}
              {vcs ? <span className={`chart-pill ${vcsChartPillClass(vcs.stage)}`}>VCS {formatScore(vcs.score)} {vcs.stage_label}</span> : null}
              {atr14 != null ? <span className="chart-pill chart-pill-setup">ATR14 {formatPrice(atr14)}</span> : null}
              {atrMultipleFrom50Ma != null ? <span className="chart-pill chart-pill-setup">50MA {formatAtrMultiple(atrMultipleFrom50Ma)}</span> : null}
              {hasTrimWarning ? <span className="chart-pill chart-pill-event">Trim warning: 3x ATR above 50MA</span> : null}
              {atrExtensionMarkers.length > 0 ? <span className="chart-pill chart-pill-setup">{atrExtensionMarkers.length} ATR extension dot(s)</span> : null}
              {wyckoffClimaxCount > 0 ? <span className="chart-pill chart-pill-event">{wyckoffClimaxCount} Wyckoff BC</span> : null}
              {wyckoffBuyCount > 0 ? <span className="chart-pill chart-pill-setup">{wyckoffBuyCount} Wyckoff BUY</span> : null}
              {wyckoffSellCount > 0 ? <span className="chart-pill chart-pill-event">{wyckoffSellCount} Wyckoff SELL</span> : null}
              {wyckoffHoldCount > 0 ? <span className="chart-pill chart-pill-setup">{wyckoffHoldCount} Wyckoff HOLD</span> : null}
              {sellIntoStrengthMarkers.length > 0 ? <span className="chart-pill chart-pill-event">{sellIntoStrengthMarkers.length} sell signal(s)</span> : null}
            </div>
            <div className="rs-rating-grid">
              <RsRatingMiniChart
                title="RS Rating Daily"
                series={dailyRsRatingSeries}
                emptyLabel="Daily RS rating needs more history."
                hoveredTime={syncedHoverTime}
                onHoverTimeChange={setSyncedHoverTime}
              />
            </div>
          </>
        ) : null}
      </Panel>

      <Panel title="Danger Signals" aside={<span className="eyebrow">Latest bar snapshot, Mindwheel-inspired</span>}>
        {!requestedTicker ? <p className="panel-copy">Load ticker to inspect active danger signals on latest bar.</p> : null}
        {requestedTicker && isLoading ? <LoadingBlock label="Refreshing danger signals..." compact /> : null}
        {requestedTicker && !isLoading ? (
          <>
            <p className="panel-copy">
              Active on {chartPayload?.danger_signals?.as_of_date ?? chartPayload?.resolved_as_of_date ?? "latest bar"}: {dangerSignalCount}
              {dangerSignalCount > 0 && highestDangerSeverity ? ` | Highest severity ${dangerSignalSeverityLabel(highestDangerSeverity)}` : ""}
            </p>
            {dangerSignalCount === 0 ? (
              <p className="panel-copy">No active danger signals from current implemented rule set on latest bar.</p>
            ) : (
              <div className="list-grid">
                {dangerSignals.map((signal) => (
                  <div key={signal.key} className="chart-card danger-signal-card">
                    <div className="danger-signal-head">
                      <div>
                        <div className="chart-rs-header">{dangerSignalCategoryLabel(signal.category)}</div>
                        <strong>{signal.label}</strong>
                      </div>
                      <span className={`status-pill ${dangerSignalSeverityClass(signal.severity)}`}>{dangerSignalSeverityLabel(signal.severity)}</span>
                    </div>
                    <p className="panel-copy">{signal.summary}</p>
                    <p className="panel-copy">{signal.details}</p>
                    {signal.metrics.length > 0 ? (
                      <div className="danger-signal-metrics">
                        {signal.metrics.map((metric) => (
                          <span key={`${signal.key}-${metric.label}`} className="chart-pill chart-pill-setup">
                            {metric.label} {metric.value}
                          </span>
                        ))}
                      </div>
                    ) : null}
                  </div>
                ))}
              </div>
            )}
          </>
        ) : null}
      </Panel>

      {isAdmin && signalGuideGroups.length > 0 ? (
        <Panel title="Signal Logic" aside={<span className="eyebrow">Admin only</span>}>
          <div className="list-grid">
            {signalGuideGroups.map((group) => (
              <div key={group.title} className="chart-card">
                <div className="chart-rs-header">{group.title}</div>
                {group.items.map((item) => (
                  <div key={item.label} style={{ marginTop: 12 }}>
                    <strong>{item.label}</strong>
                    <p className="panel-copy">{item.meaning}</p>
                    <p className="panel-copy">{item.logic}</p>
                  </div>
                ))}
              </div>
            ))}
          </div>
        </Panel>
      ) : null}

      <ExclusionDialog
        isOpen={isListDialogOpen}
        mode={listDialogMode === "addExclusion" ? "add" : "remove"}
        ticker={requestedTicker || "--"}
        title={listDialogMode === "addExclusion" ? `Add ${requestedTicker} to exclusions` : `Remove ${requestedTicker} from exclusions`}
        confirmLabel={listDialogMode === "addExclusion" ? "Add To Exclusion" : "Remove Exclusion"}
        helperText={
          listDialogMode === "addExclusion"
            ? "This writes to the manual exclusions list so future scans can skip this ticker."
            : "This removes the ticker from removable exclusion files."
        }
        reasonOptions={listDialogMode === "addExclusion" ? [...EXCLUSION_REASON_OPTIONS] : []}
        submitting={isSavingListAction}
        onClose={() => setIsListDialogOpen(false)}
        onSubmit={handleTickerListAction}
      />
    </div>
  );
}

function formatPrice(value: number | null) {
  return value == null ? "--" : `$${value.toFixed(2)}`;
}

function formatScore(value: number | null | undefined) {
  return value == null ? "--" : value.toFixed(1);
}

function vcsStageClass(stage: "critical" | "setup" | "base"): string {
  if (stage === "critical") {
    return "status-success";
  }
  if (stage === "setup") {
    return "status-queued";
  }
  return "status-unknown";
}

function dangerSignalSeverityClass(severity: string | null | undefined): string {
  if (severity === "high") {
    return "status-critical";
  }
  if (severity === "risk") {
    return "status-risk";
  }
  return "status-caution";
}

function dangerSignalSeverityLabel(severity: string | null | undefined): string {
  if (severity === "high") {
    return "High";
  }
  if (severity === "risk") {
    return "Risk";
  }
  return "Warning";
}

function dangerSignalCategoryLabel(category: string | null | undefined): string {
  if (category === "late") {
    return "Late Trade";
  }
  if (category === "mid") {
    return "Mid Trade";
  }
  return "Early Warning";
}

function vcsChartPillClass(stage: "critical" | "setup" | "base"): string {
  if (stage === "critical") {
    return "chart-pill chart-pill-event";
  }
  if (stage === "setup") {
    return "chart-pill chart-pill-rs";
  }
  return "chart-pill chart-pill-setup";
}

function formatMarketExtensionState(state: "normal" | "warning" | "extreme" | null | undefined): string {
  if (state === "warning") {
    return "Overextended";
  }
  if (state === "extreme") {
    return "Extreme";
  }
  return "Normal";
}

function marketExtensionStateClass(state: "normal" | "warning" | "extreme"): string {
  if (state === "extreme") {
    return "status-failed";
  }
  if (state === "warning") {
    return "status-queued";
  }
  return "status-success";
}

function marketExtensionChartPillClass(state: "normal" | "warning" | "extreme"): string {
  if (state === "extreme") {
    return "chart-pill chart-pill-event";
  }
  if (state === "warning") {
    return "chart-pill chart-pill-trigger";
  }
  return "chart-pill chart-pill-setup";
}

function RsRatingMiniChart({
  title,
  series,
  emptyLabel,
  hoveredTime,
  onHoverTimeChange,
}: {
  title: string;
  series: Array<{ time: string; value: number }>;
  emptyLabel: string;
  hoveredTime: string | null;
  onHoverTimeChange: (time: string | null) => void;
}) {
  const path = useMemo(() => buildMiniChartPath(series), [series]);
  const hoveredIndex = hoveredTime ? series.findIndex((point) => point.time === hoveredTime) : -1;
  const activeIndex = hoveredIndex >= 0 && hoveredIndex < series.length ? hoveredIndex : series.length - 1;
  const activePoint = activeIndex >= 0 ? series[activeIndex] ?? null : null;
  const activePathPoint = path?.points[activeIndex] ?? null;
  const activeValue = activePoint?.value ?? null;
  const activeTime = activePoint?.time ?? "";
  const axisLabels = useMemo(() => buildMiniChartAxisLabels(series), [series]);

  return (
    <div className="chart-card rs-rating-card">
      <div className="rs-rating-card-head">
        <div>
          <div className="chart-rs-header">{title}</div>
          <div className="rs-rating-meta">{activeTime ? `${hoveredIndex >= 0 ? "Hover" : "Latest"} ${activeTime}` : emptyLabel}</div>
        </div>
        <div className="rs-rating-value">{activeValue == null ? "--" : activeValue.toFixed(1)}</div>
      </div>
      {series.length === 0 || path == null ? (
        <p className="panel-copy">{emptyLabel}</p>
      ) : (
        <svg
          className="rs-rating-svg"
          viewBox="0 0 560 180"
          preserveAspectRatio="xMidYMid meet"
          aria-label={title}
          onMouseMove={(event) => {
            const bounds = event.currentTarget.getBoundingClientRect();
            if (bounds.width <= 0 || series.length === 0) {
              return;
            }
            const rawX = ((event.clientX - bounds.left) / bounds.width) * 560;
            const nextIndex = clampMiniChartIndex(series.length, rawX);
            const nextTime = series[nextIndex]?.time ?? null;
            if (nextTime !== hoveredTime) {
              onHoverTimeChange(nextTime);
            }
          }}
          onMouseLeave={() => {
            if (hoveredTime != null) {
              onHoverTimeChange(null);
            }
          }}
        >
          <rect x="0" y="0" width="560" height="180" rx="10" fill="#111114" />
          {[30, 70, 90].map((level) => {
            const y = ratingToChartY(level);
            return (
              <g key={level}>
                <line x1="0" y1={y} x2="560" y2={y} stroke={level >= 90 ? "#14532d" : "#27272a"} strokeDasharray="4 4" strokeWidth="1" />
                <text x="8" y={y - 4} fill="#71717a" fontSize="11">
              {level}
                </text>
              </g>
            );
          })}
          {activePathPoint ? (
            <line
              x1={activePathPoint.x}
              y1="8"
              x2={activePathPoint.x}
              y2="174"
              stroke="#60a5fa"
              strokeWidth="1"
              strokeDasharray="4 4"
              opacity="0.95"
            />
          ) : null}
          {axisLabels.map((label) => (
            <text key={`${title}-${label.x}-${label.text}`} x={label.x} y="170" fill="#71717a" fontSize="11" textAnchor={label.anchor}>
              {label.text}
            </text>
          ))}
          <path d={path.areaPath} fill="rgba(96, 165, 250, 0.12)" />
          <path d={path.linePath} fill="none" stroke="#60a5fa" strokeWidth="2.5" strokeLinejoin="round" strokeLinecap="round" />
          {activePathPoint ? <circle cx={activePathPoint.x} cy={activePathPoint.y} r="4" fill="#93c5fd" stroke="#0f172a" strokeWidth="1.5" /> : null}
        </svg>
      )}
    </div>
  );
}

function buildMiniChartPath(series: Array<{ time: string; value: number }>) {
  if (series.length < 2) {
    return null;
  }
  const width = 560;
  const height = 180;
  const left = 10;
  const right = 10;
  const top = 14;
  const bottom = 32;
  const usableWidth = width - left - right;
  const baselineY = height - bottom;
  const points = series.map((point, index) => {
    const x = left + (usableWidth * index) / Math.max(1, series.length - 1);
    const y = ratingToChartY(point.value, { top, bottom, height });
    return { x, y };
  });
  const linePath = points.map((point, index) => `${index === 0 ? "M" : "L"} ${point.x.toFixed(2)} ${point.y.toFixed(2)}`).join(" ");
  const areaPath = `${linePath} L ${points[points.length - 1].x.toFixed(2)} ${baselineY} L ${points[0].x.toFixed(2)} ${baselineY} Z`;
  return {
    linePath,
    areaPath,
    points,
    lastPoint: points[points.length - 1] ?? null,
  };
}

function clampMiniChartIndex(length: number, svgX: number) {
  const width = 560;
  const left = 10;
  const right = 10;
  const usableWidth = width - left - right;
  const normalizedX = Math.max(left, Math.min(width - right, svgX)) - left;
  return Math.max(0, Math.min(length - 1, Math.round((normalizedX / Math.max(1, usableWidth)) * Math.max(1, length - 1))));
}

function buildMiniChartAxisLabels(series: Array<{ time: string; value: number }>) {
  if (series.length === 0) {
    return [];
  }
  const width = 560;
  const left = 10;
  const right = 10;
  const usableWidth = width - left - right;
  const indices = Array.from(new Set([0, Math.floor((series.length - 1) / 2), series.length - 1]));
  return indices.map((index) => {
    const point = series[index];
    const x = left + (usableWidth * index) / Math.max(1, series.length - 1);
    return {
      x,
      text: point?.time ?? "",
      anchor: index === 0 ? ("start" as const) : index === series.length - 1 ? ("end" as const) : ("middle" as const),
    };
  });
}

function ratingToChartY(
  value: number,
  dimensions: { top?: number; bottom?: number; height?: number } = {},
) {
  const top = dimensions.top ?? 14;
  const bottom = dimensions.bottom ?? 32;
  const height = dimensions.height ?? 180;
  const clamped = Math.max(0, Math.min(100, value));
  const usableHeight = height - top - bottom;
  return top + ((100 - clamped) / 100) * usableHeight;
}

function clearAllChartCache() {
  const prefix = `${CHART_CACHE_PREFIX}:`;
  for (let index = localStorage.length - 1; index >= 0; index -= 1) {
    const key = localStorage.key(index);
    if (!key || !key.startsWith(prefix)) {
      continue;
    }
    localStorage.removeItem(key);
  }
}

function formatCurrency(value: number | null | undefined) {
  return value == null ? "--" : `$${value.toLocaleString(undefined, { maximumFractionDigits: 2, minimumFractionDigits: 2 })}`;
}

function formatSignedCurrency(value: number | null | undefined) {
  if (value == null) {
    return "--";
  }
  const prefix = value > 0 ? "+" : "";
  return `${prefix}${formatCurrency(Math.abs(value)).replace("$", value < 0 ? "-$" : "$")}`;
}

function formatInteger(value: number | null | undefined) {
  return value == null ? "--" : Math.round(value).toLocaleString();
}

function formatDateTime(value: string | null | undefined) {
  if (!value) {
    return "--";
  }
  const parsed = new Date(value);
  return Number.isNaN(parsed.getTime()) ? value : parsed.toLocaleString();
}

function formatMetric(value: number | null | undefined) {
  return value == null ? "--" : value.toFixed(2);
}

function formatCompactCurrency(value: number | null | undefined) {
  if (value == null) {
    return "--";
  }
  const absValue = Math.abs(value);
  const sign = value < 0 ? "-" : "";
  if (absValue >= 1_000_000_000) {
    return `${sign}$${(absValue / 1_000_000_000).toFixed(2)}B`;
  }
  if (absValue >= 1_000_000) {
    return `${sign}$${(absValue / 1_000_000).toFixed(2)}M`;
  }
  if (absValue >= 1_000) {
    return `${sign}$${(absValue / 1_000).toFixed(2)}K`;
  }
  return `${sign}$${absValue.toFixed(2)}`;
}

function formatPercent(value: number | null | undefined) {
  return value == null ? "--" : `${value.toFixed(2)}%`;
}

function formatFlipDistance(value: number | null | undefined) {
  if (value == null) {
    return "Flip dist --";
  }
  return `${value >= 0 ? "Above flip" : "Below flip"} ${formatPercent(value)}`;
}

function formatRatio(value: number | null | undefined) {
  return value == null ? "--" : `${value.toFixed(2)}x`;
}

function formatAtrMultiple(value: number | null | undefined) {
  if (value == null) {
    return "--";
  }
  const prefix = value > 0 ? "+" : "";
  return `${prefix}${value.toFixed(2)}x`;
}

function toNullableNumber(value: string | number | boolean | null | undefined) {
  return typeof value === "number" && Number.isFinite(value) ? value : null;
}

function toneForCanslimScore(value: number | null | undefined, maxScore: number | null | undefined) {
  if (value == null || maxScore == null || maxScore <= 0) {
    return "is-neutral";
  }
  if (value / maxScore >= 0.7) {
    return "is-strong";
  }
  if (value / maxScore >= 0.45) {
    return "is-warm";
  }
  return "is-neutral";
}

function toneForCanslimLetter(value: number | null | undefined) {
  if (value == null) {
    return "is-neutral";
  }
  if (value >= 2) {
    return "is-strong";
  }
  if (value >= 1) {
    return "is-warm";
  }
  return "is-neutral";
}

function computeAdrPercent(candles: CandlePoint[], lookbackDays: number): number | null {
  const window = candles.slice(-lookbackDays);
  if (window.length < lookbackDays) {
    return null;
  }
  let totalRangePct = 0;
  for (const candle of window) {
    if (!Number.isFinite(candle.high) || !Number.isFinite(candle.low) || !Number.isFinite(candle.close) || candle.close <= 0) {
      return null;
    }
    totalRangePct += ((candle.high - candle.low) / candle.close) * 100;
  }
  return totalRangePct / window.length;
}

function computeAtr(candles: CandlePoint[], lookbackDays: number): number | null {
  const window = candles.slice(-(lookbackDays + 1));
  if (window.length < lookbackDays + 1) {
    return null;
  }
  let totalTrueRange = 0;
  for (let index = 1; index < window.length; index += 1) {
    const previousClose = window[index - 1]?.close;
    const candle = window[index];
    if (
      candle == null ||
      previousClose == null ||
      !Number.isFinite(candle.high) ||
      !Number.isFinite(candle.low) ||
      !Number.isFinite(previousClose)
    ) {
      return null;
    }
    const trueRange = Math.max(
      candle.high - candle.low,
      Math.abs(candle.high - previousClose),
      Math.abs(candle.low - previousClose),
    );
    totalTrueRange += trueRange;
  }
  return totalTrueRange / lookbackDays;
}

function buildAtrExtensionMarkers(
  candles: CandlePoint[],
  ma50Series: Array<{ time: string; value: number }>,
  atrLookbackDays: number,
): Array<{ time: string; label?: string; color: string; shape: "circle"; position: "aboveBar" }> {
  if (candles.length < atrLookbackDays + 1 || ma50Series.length === 0) {
    return [];
  }
  const ma50ByTime = new Map(ma50Series.map((point) => [point.time, point.value]));
  const markers: Array<{ time: string; label?: string; color: string; shape: "circle"; position: "aboveBar" }> = [];
  for (let index = atrLookbackDays; index < candles.length; index += 1) {
    const current = candles[index];
    const ma50 = ma50ByTime.get(current.time);
    if (ma50 == null || !Number.isFinite(ma50)) {
      continue;
    }
    const atr = computeAtr(candles.slice(0, index + 1), atrLookbackDays);
    if (atr == null || atr <= 0) {
      continue;
    }
    const multiple = (current.close - ma50) / atr;
    if (multiple >= 3) {
      markers.push({
        time: current.time,
        label: `ATR ext ${multiple.toFixed(1)}x`,
        color: "#22c55e",
        shape: "circle",
        position: "aboveBar",
      });
    }
  }
  return markers;
}

function buildSellIntoStrengthMarkers(
  candles: CandlePoint[],
  ma50Series: Array<{ time: string; value: number }>,
): Array<{ time: string; label?: string; color: string; shape: "square"; position: "aboveBar" }> {
  if (candles.length < 50 || ma50Series.length === 0) {
    return [];
  }
  const ema10 = buildExponentialMovingAverage(candles, 10);
  const ema10ByTime = new Map(ema10.map((point) => [point.time, point.value]));
  const ma50ByTime = new Map(ma50Series.map((point) => [point.time, point.value]));
  const markers: Array<{ time: string; label?: string; color: string; shape: "square"; position: "aboveBar" }> = [];

  for (let index = 20; index < candles.length; index += 1) {
    const current = candles[index];
    const ema10Value = ema10ByTime.get(current.time);
    const ma50Value = ma50ByTime.get(current.time);
    if (ema10Value == null || ma50Value == null || ema10Value <= 0 || ma50Value <= 0) {
      continue;
    }

    const volumeWindow = candles.slice(Math.max(0, index - 19), index + 1);
    const avgVolume20 = volumeWindow.reduce((sum, candle) => sum + candle.volume, 0) / volumeWindow.length;
    if (!Number.isFinite(avgVolume20) || avgVolume20 <= 0) {
      continue;
    }
    const volumeRatio = current.volume / avgVolume20;
    const distanceFromEma10Pct = ((current.close / ema10Value) - 1) * 100;
    const distanceFromMa50Pct = ((current.close / ma50Value) - 1) * 100;
    const threeDayAgo = candles[index - 3];
    const fiveDayAgo = candles[index - 5];
    const threeDayRunPct = threeDayAgo && threeDayAgo.close > 0 ? ((current.close / threeDayAgo.close) - 1) * 100 : null;
    const fiveDayRunPct = fiveDayAgo && fiveDayAgo.close > 0 ? ((current.close / fiveDayAgo.close) - 1) * 100 : null;
    const parabolicRun = (threeDayRunPct != null && threeDayRunPct >= 12) || (fiveDayRunPct != null && fiveDayRunPct >= 20);
    const explosiveVolume = volumeRatio >= 1.5;
    const emaSellSignal = distanceFromEma10Pct >= 20 && parabolicRun && explosiveVolume;
    const ma50SellSignal = distanceFromMa50Pct >= 50 && explosiveVolume;
    if (!emaSellSignal && !ma50SellSignal) {
      continue;
    }
    const reasons = [];
    if (emaSellSignal) {
      reasons.push(`10EMA +${distanceFromEma10Pct.toFixed(1)}%`);
    }
    if (ma50SellSignal) {
      reasons.push(`50SMA +${distanceFromMa50Pct.toFixed(1)}%`);
    }
    reasons.push(`Vol ${volumeRatio.toFixed(1)}x`);
    markers.push({
      time: current.time,
      label: `Sell ${reasons.join(" | ")}`,
      color: "#ef4444",
      shape: "square",
      position: "aboveBar",
    });
  }

  return markers;
}

function buildExponentialMovingAverage(
  candles: CandlePoint[],
  length: number,
): Array<{ time: string; value: number }> {
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

type SignalGuideItem = {
  label: string;
  meaning: string;
  logic: string;
};

type SignalGuideGroup = {
  title: string;
  items: SignalGuideItem[];
};

function buildSignalGuideGroups({
  chartPayload,
  latestRsMarker,
  atrExtensionCount,
  sellSignalCount,
  hasTrimWarning,
}: {
  chartPayload: WatchlistChartResponse | null;
  latestRsMarker: WatchlistChartResponse["rs_markers"][number] | null;
  atrExtensionCount: number;
  sellSignalCount: number;
  hasTrimWarning: boolean;
}): SignalGuideGroup[] {
  const groups: SignalGuideGroup[] = [];

  const coreItems: SignalGuideItem[] = [
    {
      label: "Gap Zones",
      meaning: "Highlights important gap areas on the price chart.",
      logic: "Chart overlay marks gap-up or gap-down zones so you can see likely support, resistance, or unfinished business fast.",
    },
    {
      label: "HTF Box",
      meaning: "Shows the higher-timeframe price box or compression range.",
      logic: "Overlay frames the larger structure so daily action can be judged inside a bigger weekly-style context.",
    },
    {
      label: "MA Stack",
      meaning: "Shows whether moving averages are aligned in a constructive trend order.",
      logic: "Uses the short and intermediate averages already plotted on chart. Clean bullish stacking means faster averages stay above slower ones.",
    },
  ];
  if (chartPayload?.market_extension?.latest) {
    coreItems.push({
      label: "10W Extension",
      meaning: "Shows how stretched price is versus the 10-week moving average.",
      logic: "Computed from weekly close versus 10W SMA. Warning at 11% or more. Extreme at 15% or more.",
    });
  }
  if (chartPayload?.vcs) {
    coreItems.push({
      label: "VCS",
      meaning: "Summarizes how constructive the current volatility-contraction structure is.",
      logic: "Uses the backend VCS score and stage labels to separate base, setup, and critical states.",
    });
  }
  if (chartPayload?.fearzone_panel?.rows?.length) {
    coreItems.push({
      label: "Fearzone Panel",
      meaning: "Highlights panic-reset context where downside emotion may be exhausting.",
      logic: "Driven by backend fearzone calculations and rendered as a dedicated chart sub-panel.",
    });
  }
  if (chartPayload?.sepa_dashboard) {
    coreItems.push({
      label: "SEPA Dashboard",
      meaning: "Quick health summary for trend quality, pressure, and buy-risk context.",
      logic: "Uses backend SEPA fields like TPR, RPR, VCP state, buy risk, and pressure to summarize setup quality.",
    });
  }
  if (coreItems.length > 0) {
    groups.push({ title: "Core Overlays", items: coreItems });
  }

  const rsItems: SignalGuideItem[] = [];
  if (latestRsMarker) {
    rsItems.push({
      label: latestRsMarker.kind === "daily_new_high_before_price" ? "RS New High Before Price" : "RS New High",
      meaning: "Relative strength line is leading price or making a fresh high with price strength confirmation.",
      logic: latestRsMarker.kind === "daily_new_high_before_price"
        ? "RS line breaks to a new high before price itself breaks to a new high. That is stronger leadership."
        : "RS line makes a fresh high while price is also advancing, confirming leadership versus the benchmark.",
    });
  }
  if ((chartPayload?.daily_rs_rating?.length ?? 0) > 0) {
    rsItems.push({
      label: "RS Rating Daily",
      meaning: "Daily relative-strength rating trend over time.",
      logic: "Backend supplies dated daily RS scores. Mini-chart now uses rating values vertically and actual dates on the horizontal axis.",
    });
  }
  if (chartPayload?.benchmark_ticker) {
    rsItems.push({
      label: "RS Line",
      meaning: `Compares ${chartPayload.ticker} versus ${chartPayload.benchmark_ticker} to show leadership or lagging behavior.`,
      logic: "The RS pane plots relative performance against the benchmark. Rising RS means the stock is outperforming even if price is not breaking out yet.",
    });
  }
  if (rsItems.length > 0) {
    groups.push({ title: "RS Signals", items: rsItems });
  }

  const riskItems: SignalGuideItem[] = [];
  if (hasTrimWarning) {
    riskItems.push({
      label: "Trim Warning",
      meaning: "Price is unusually far above the 50MA in ATR terms.",
      logic: "Triggered when close is at least 3 ATR above the current 50MA. This is extension risk, not an entry signal.",
    });
  }
  if (atrExtensionCount > 0) {
    riskItems.push({
      label: "ATR Extension Dots",
      meaning: "Marks bars where price became 3 ATR or more above the 50MA.",
      logic: "For each bar, ATR14 is recomputed and compared with distance above 50MA. Marker appears at 3.0x or higher.",
    });
  }
  if (sellSignalCount > 0) {
    riskItems.push({
      label: "Sell Signals",
      meaning: "Flags possible sell-into-strength conditions after fast, climactic upside runs.",
      logic: "Marks bars with explosive volume plus either 20% or more above 10EMA after a parabolic run, or 50% or more above 50SMA with explosive volume.",
    });
  }
  if (riskItems.length > 0) {
    groups.push({ title: "Trim / Sell", items: riskItems });
  }

  return groups;
}
