import { useEffect, useMemo, useState, type FormEvent } from "react";
import { Link, useSearchParams } from "react-router-dom";
import { useAuth } from "../auth/AuthContext";
import { ExclusionDialog } from "../components/ExclusionDialog";
import { LoadingBlock } from "../components/LoadingBlock";
import { Panel } from "../components/Panel";
import { PriceChart, type ChartVisibility } from "../components/PriceChart";
import { fetchJson } from "../lib/api";
import type { AdHocScreenResponse, AdminTickerListStatusResponse, CandlePoint, ChartAnnotations, ChartFundamentalsResponse, ChartInsiderResponse, WatchlistChartResponse } from "../lib/types";

const DEFAULT_CHART_VISIBILITY: ChartVisibility = {
  ema8: true,
  ema21: true,
  weeklyEma8: true,
  ipoVwap: true,
  maStack: true,
  gapZones: true,
  htfBox: true,
  rsLine: true,
  rsSignals: true,
  flexSr: false,
};
const CHART_CACHE_PREFIX = "chart-screen-cache-v3";
const CHART_CACHE_TTL_MS_BY_KIND: Record<"payload" | "fundamentals" | "insider", number> = {
  payload: 10 * 60 * 1000,
  fundamentals: 60 * 60 * 1000,
  insider: 4 * 60 * 60 * 1000,
};

export function ChartsPage() {
  const auth = useAuth();
  const setupOptions = [
    { id: "hve", label: "HVE" },
    { id: "inside_dryup", label: "Inside Dry-Up" },
    { id: "ftd_sweep", label: "FTD Sweep" },
    { id: "weekly_htf_pullback", label: "Weekly HTF Pullback" },
    { id: "htf_8w_runup", label: "HTF 8W Runup" },
    { id: "vcp", label: "VCP" },
  ] as const;
  const [searchParams, setSearchParams] = useSearchParams();
  const requestedTicker = (searchParams.get("ticker") ?? "").trim().toUpperCase();
  const requestedDate = (searchParams.get("date") ?? "").trim();
  const [tickerInput, setTickerInput] = useState(requestedTicker);
  const [dateInput, setDateInput] = useState(requestedDate);
  const [payload, setPayload] = useState<WatchlistChartResponse | null>(null);
  const [fundamentalsPayload, setFundamentalsPayload] = useState<ChartFundamentalsResponse | null>(null);
  const [insiderPayload, setInsiderPayload] = useState<ChartInsiderResponse | null>(null);
  const [setupPayload, setSetupPayload] = useState<AdHocScreenResponse | null>(null);
  const [isLoading, setIsLoading] = useState(false);
  const [isFundamentalsLoading, setIsFundamentalsLoading] = useState(false);
  const [isInsiderLoading, setIsInsiderLoading] = useState(false);
  const [isSetupLoading, setIsSetupLoading] = useState(false);
  const [notice, setNotice] = useState("");
  const [fundamentalsNotice, setFundamentalsNotice] = useState("");
  const [insiderNotice, setInsiderNotice] = useState("");
  const [setupNotice, setSetupNotice] = useState("");
  const [chartVisibility, setChartVisibility] = useState<ChartVisibility>(DEFAULT_CHART_VISIBILITY);
  const [refreshNonce, setRefreshNonce] = useState(0);
  const [selectedSetups, setSelectedSetups] = useState<Record<string, boolean>>({
    hve: false,
    inside_dryup: false,
    ftd_sweep: false,
    weekly_htf_pullback: false,
    htf_8w_runup: false,
    vcp: false,
  });
  const [tickerListStatus, setTickerListStatus] = useState<AdminTickerListStatusResponse | null>(null);
  const [isTickerListLoading, setIsTickerListLoading] = useState(false);
  const [isListDialogOpen, setIsListDialogOpen] = useState(false);
  const [listDialogMode, setListDialogMode] = useState<"addExclusion" | "removeExclusion">("addExclusion");
  const [isSavingListAction, setIsSavingListAction] = useState(false);
  const [isLaunchingBackfill, setIsLaunchingBackfill] = useState(false);
  const [backfillNotice, setBackfillNotice] = useState("");
  const [chartHoveredTime, setChartHoveredTime] = useState<string | null>(null);
  const [chartVisibleIndexRange, setChartVisibleIndexRange] = useState<{ from: number; to: number } | null>(null);

  useEffect(() => {
    setTickerInput(requestedTicker);
    setDateInput(requestedDate);
  }, [requestedDate, requestedTicker]);

  useEffect(() => {
    if (!requestedTicker) {
      setPayload(null);
      setFundamentalsPayload(null);
      setInsiderPayload(null);
      setSetupPayload(null);
      setNotice("");
      setFundamentalsNotice("");
      setInsiderNotice("");
      setBackfillNotice("");
      return;
    }
    setIsLoading(true);
    setNotice("");
    const query = new URLSearchParams({ period: "18mo" });
    if (requestedDate) {
      query.set("asOfDate", requestedDate);
    }
    const cacheKey = buildChartCacheKey("payload", requestedTicker, requestedDate || "latest");
    const cached = refreshNonce === 0 ? readChartCache<WatchlistChartResponse>(cacheKey) : null;
    if (cached && hasUsableChartData(cached)) {
      setPayload(cached);
      if (!requestedDate && cached.resolved_as_of_date) {
        setDateInput(cached.resolved_as_of_date);
      }
      if (requestedDate && cached.resolved_as_of_date && cached.resolved_as_of_date !== requestedDate) {
        setNotice(`Requested ${requestedDate}. Used last trading day ${cached.resolved_as_of_date}.`);
      }
      setIsLoading(false);
      return;
    }
    void fetchJson<WatchlistChartResponse>(`/api/charts/${requestedTicker}?${query.toString()}`)
      .then((response) => {
        if (hasUsableChartData(response)) {
          writeChartCache(cacheKey, response);
        } else {
          clearChartCacheKey(cacheKey);
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
        setPayload(null);
        setNotice(error instanceof Error ? error.message : "Failed to load chart.");
      })
      .finally(() => setIsLoading(false));
  }, [refreshNonce, requestedDate, requestedTicker]);

  useEffect(() => {
    if (!requestedTicker) {
      setInsiderPayload(null);
      setInsiderNotice("");
      return;
    }
    setIsInsiderLoading(true);
    setInsiderNotice("");
    const query = new URLSearchParams({ lookbackDays: "14" });
    if (payload?.resolved_as_of_date) {
      query.set("asOfDate", payload.resolved_as_of_date);
    } else if (requestedDate) {
      query.set("asOfDate", requestedDate);
    }
    const insiderAsOfDate = payload?.resolved_as_of_date || requestedDate || "latest";
    const cacheKey = buildChartCacheKey("insider", requestedTicker, insiderAsOfDate);
    const cached = refreshNonce === 0 ? readChartCache<ChartInsiderResponse>(cacheKey) : null;
    if (cached) {
      setInsiderPayload(cached);
      setIsInsiderLoading(false);
      return;
    }
    void fetchJson<ChartInsiderResponse>(`/api/chart-insider/${requestedTicker}?${query.toString()}`)
      .then((response) => {
        writeChartCache(cacheKey, response);
        setInsiderPayload(response);
      })
      .catch((error) => {
        setInsiderPayload(null);
        setInsiderNotice(error instanceof Error ? error.message : "Failed to load insider trades.");
      })
      .finally(() => setIsInsiderLoading(false));
  }, [payload?.resolved_as_of_date, refreshNonce, requestedDate, requestedTicker]);

  useEffect(() => {
    if (!requestedTicker || !auth.hasCapability("manage_exclusions")) {
      setTickerListStatus(null);
      setIsTickerListLoading(false);
      return;
    }
    setIsTickerListLoading(true);
    void fetchJson<AdminTickerListStatusResponse>(`/api/admin/ticker-lists/${requestedTicker}`)
      .then(setTickerListStatus)
      .catch(() => setTickerListStatus(null))
      .finally(() => setIsTickerListLoading(false));
  }, [auth, requestedTicker, refreshNonce]);

  useEffect(() => {
    if (!requestedTicker) {
      setFundamentalsPayload(null);
      setFundamentalsNotice("");
      return;
    }
    setIsFundamentalsLoading(true);
    setFundamentalsNotice("");
    const cacheKey = buildChartCacheKey("fundamentals", requestedTicker, "latest");
    const cached = refreshNonce === 0 ? readChartCache<ChartFundamentalsResponse>(cacheKey) : null;
    if (cached) {
      setFundamentalsPayload(cached);
      const earningsStatus = cached.diagnostics.earnings.status;
      const holdersStatus = cached.diagnostics.holders.status;
      const statisticsStatus = cached.diagnostics.statistics.status;
      const optionsStatus = cached.diagnostics.options.status;
      if (earningsStatus !== "ok" || holdersStatus !== "ok" || statisticsStatus !== "ok" || optionsStatus !== "ok") {
        setFundamentalsNotice(`Diagnostics: earnings=${earningsStatus}, holders=${holdersStatus}, statistics=${statisticsStatus}, options=${optionsStatus}`);
      }
      setIsFundamentalsLoading(false);
      return;
    }
    void fetchJson<ChartFundamentalsResponse>(`/api/chart-fundamentals/${requestedTicker}?earningsLimit=4`)
      .then((response) => {
        writeChartCache(cacheKey, response);
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
        setFundamentalsPayload(null);
        setFundamentalsNotice(error instanceof Error ? error.message : "Failed to load chart fundamentals.");
      })
      .finally(() => setIsFundamentalsLoading(false));
  }, [refreshNonce, requestedTicker]);

  const selectedSetupIds = useMemo(
    () => setupOptions.filter((option) => selectedSetups[option.id]).map((option) => option.id),
    [selectedSetups],
  );

  useEffect(() => {
    if (!requestedTicker || !payload?.resolved_as_of_date || selectedSetupIds.length === 0) {
      setSetupPayload(null);
      setSetupNotice("");
      return;
    }
    setIsSetupLoading(true);
    setSetupNotice("");
    void fetchJson<AdHocScreenResponse>("/api/ad-hoc-screen", {
      method: "POST",
      body: JSON.stringify({
        ticker: requestedTicker,
        as_of_date: payload.resolved_as_of_date,
        screeners: selectedSetupIds,
      }),
    })
      .then((response) => {
        setSetupPayload(response);
        const failed = response.screeners.filter((item) => !item.passed);
        if (failed.length > 0) {
          setSetupNotice(`Some setups not active: ${failed.map((item) => item.id).join(", ")}`);
        }
      })
      .catch((error) => {
        setSetupPayload(null);
        setSetupNotice(error instanceof Error ? error.message : "Failed to load setup overlays.");
      })
      .finally(() => setIsSetupLoading(false));
  }, [payload?.resolved_as_of_date, requestedTicker, selectedSetupIds]);

  const chartData = useMemo<CandlePoint[]>(
    () =>
      (payload?.candles ?? []).map((item, index) => ({
        ...item,
        volume: payload?.volume[index]?.value ?? 0,
      })),
    [payload],
  );
  const lastCandle = chartData[chartData.length - 1] ?? null;
  const previousCandle = chartData.length > 1 ? chartData[chartData.length - 2] : null;
  const lastClose = lastCandle?.close ?? null;
  const changePct =
    lastCandle && previousCandle && previousCandle.close > 0
      ? ((lastCandle.close - previousCandle.close) / previousCandle.close) * 100
      : null;
  const latestRsMarker = payload?.rs_markers?.[payload.rs_markers.length - 1] ?? null;
  const dailyRsRatingSeries = payload?.daily_rs_rating ?? [];
  const weeklyRsRatingSeries = payload?.weekly_rs_rating ?? [];
  const adr14Pct = useMemo(() => computeAdrPercent(chartData, 14), [chartData]);
  const adr14InRange = adr14Pct != null ? adr14Pct >= 3 && adr14Pct <= 10 : null;
  const atr14 = useMemo(() => computeAtr(chartData, 14), [chartData]);
  const latestMa50 = payload?.ma50?.[payload.ma50.length - 1]?.value ?? null;
  const atrMultipleFrom50Ma =
    atr14 != null && latestMa50 != null && Number.isFinite(lastClose ?? NaN)
      ? ((lastClose ?? 0) - latestMa50) / atr14
      : null;
  const hasTrimWarning = atrMultipleFrom50Ma != null ? atrMultipleFrom50Ma >= 3 : false;
  const earningsRows = fundamentalsPayload?.earnings_eps_history ?? [];
  const setupAnnotations = useMemo<ChartAnnotations[]>(() => {
    return (setupPayload?.screeners ?? [])
      .filter((item) => item.passed && item.hit)
      .map((item) => buildSetupAnnotation(item.id, item.hit!))
      .filter((item): item is ChartAnnotations => item !== null);
  }, [setupPayload]);
  const historicalSetupMarkers = useMemo(() => {
    if (!selectedSetups.ftd_sweep) {
      return [];
    }
    return (payload?.setup_markers ?? [])
      .filter((marker) => marker.kind === "ftd_sweep_breakout")
      .map((marker) => ({
        time: marker.time,
        label: marker.label ?? "FTD Sweep",
        color: "#f97316",
        shape: "square" as const,
        position: "belowBar" as const,
      }));
  }, [payload?.setup_markers, selectedSetups.ftd_sweep]);
  const atrExtensionMarkers = useMemo(() => buildAtrExtensionMarkers(chartData, payload?.ma50 ?? [], 14), [chartData, payload?.ma50]);
  const chartToggles: Array<{ key: keyof ChartVisibility; label: string }> = [
    { key: "ema8", label: "EMA 8" },
    { key: "ema21", label: "EMA 21" },
    { key: "weeklyEma8", label: "Weekly 8 EMA" },
    { key: "ipoVwap", label: "IPO VWAP" },
    { key: "maStack", label: "MA stack" },
    { key: "gapZones", label: "Gap zones" },
    { key: "htfBox", label: "HTF box" },
    { key: "rsLine", label: "RS line" },
    { key: "rsSignals", label: "RS markers" },
    { key: "flexSr", label: "Flex SR (exp)" },
  ];
  const canManageExclusions = auth.hasCapability("manage_exclusions");
  const canSyncHistory = auth.hasCapability("sync_history");
  const currentExclusion = tickerListStatus?.exclusion_entry ?? null;
  const showBackfillSection = canSyncHistory && requestedTicker !== "" && payload?.data_source === "internet";

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
    clearChartCacheForTicker(requestedTicker);
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

  return (
    <div className="page-grid charts-page">
      <section className="hero-strip">
        <div>
          <div className="hero-symbol-row">
            <h1>{requestedTicker || "Chart"}</h1>
            {payload?.benchmark_ticker ? <span className="ticker-exchange">RS vs {payload.benchmark_ticker}</span> : null}
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
          {canManageExclusions ? (
            <div className="button-row" style={{ marginTop: 12 }}>
              <button
                className="ghost-button"
                type="button"
                disabled={!requestedTicker || Boolean(currentExclusion) || isTickerListLoading}
                onClick={() => {
                  setListDialogMode("addExclusion");
                  setIsListDialogOpen(true);
                }}
              >
                Add To Exclusion
              </button>
              <button
                className="ghost-button"
                type="button"
                disabled={!requestedTicker || !currentExclusion?.removable || isTickerListLoading}
                onClick={() => {
                  setListDialogMode("removeExclusion");
                  setIsListDialogOpen(true);
                }}
              >
                Remove From Exclusion
              </button>
            </div>
          ) : null}
          {notice ? <p className="panel-copy">{notice}</p> : <p className="panel-copy">Standalone ticker chart with RS line, MA stack, gap zones, HTF box, and fearzone panel.</p>}
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
        <div className="hero-stats">
          <div>
            <span className="eyebrow">As Of</span>
            <strong>{payload?.resolved_as_of_date ?? "Latest trading day"}</strong>
          </div>
          <div>
            <span className="eyebrow">Requested</span>
            <strong>{requestedDate || "Latest"}</strong>
          </div>
          <div>
            <span className="eyebrow">Bars</span>
            <strong>{chartData.length || "-"}</strong>
          </div>
          <div>
            <span className="eyebrow">ADR14</span>
            <strong className={adr14InRange == null ? undefined : `adr-badge ${adr14InRange ? "is-in-range" : "is-out-of-range"}`}>
              {formatPercent(adr14Pct)}
            </strong>
          </div>
          <div>
            <span className="eyebrow">ATR14</span>
            <strong>{formatPrice(atr14)}</strong>
          </div>
          <div>
            <span className="eyebrow">ATR x 50MA</span>
            <strong>{formatAtrMultiple(atrMultipleFrom50Ma)}</strong>
          </div>
          <div>
            <span className="eyebrow">Trim Warn</span>
            <strong className={hasTrimWarning ? "atr-badge is-warning" : undefined}>{hasTrimWarning ? ">= 3x ATR" : "Normal"}</strong>
          </div>
          <div>
            <span className="eyebrow">Inst Float</span>
            <strong>{formatPercent(fundamentalsPayload?.holders_float_held_by_institutions_pct)}</strong>
          </div>
          <div>
            <span className="eyebrow">Rev YoY</span>
            <strong>{formatPercent(fundamentalsPayload?.revenue_yoy_pct)}</strong>
          </div>
          <div>
            <span className="eyebrow">Imp Move</span>
            <strong>{formatPercent(fundamentalsPayload?.implied_move?.percent_move)}</strong>
          </div>
          <div>
            <span className="eyebrow">Source</span>
            <strong>{payload?.data_source ?? "-"}</strong>
          </div>
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
        {requestedTicker ? <p className="panel-copy">Browser cache lasts 1 hour for chart, fundamentals, and insider data. Refresh to bypass cache.</p> : null}
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

      <Panel title="Setup Overlays" aside={<span className="eyebrow">Optional screener overlays for this ticker/date</span>}>
        <div className="chart-toolbar">
          {setupOptions.map((option) => (
            <label key={option.id} className="chart-toggle">
              <input
                type="checkbox"
                checked={selectedSetups[option.id]}
                onChange={() =>
                  setSelectedSetups((current) => ({
                    ...current,
                    [option.id]: !current[option.id],
                  }))
                }
              />
              <span>{option.label}</span>
            </label>
          ))}
        </div>
        {isSetupLoading ? <LoadingBlock label="Loading setup overlays…" compact /> : null}
        {setupNotice ? <p className="panel-copy">{setupNotice}</p> : null}
      </Panel>

      <Panel title="EPS History" aside={<span className="eyebrow">Yahoo scrape experiment for estimate, reported, surprise</span>}>
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
              overlays={payload ?? undefined}
              extraAnnotations={setupAnnotations}
              extraMarkers={[...historicalSetupMarkers, ...atrExtensionMarkers]}
              visibility={chartVisibility}
              forceFearzonePanel
              onHoverTimeChange={setChartHoveredTime}
              onVisibleIndexRangeChange={setChartVisibleIndexRange}
            />
            <div className="chart-annotation-strip">
              {payload?.resolved_as_of_date ? <span className="chart-pill chart-pill-event">As Of {payload.resolved_as_of_date}</span> : null}
              {payload?.benchmark_ticker ? <span className="chart-pill chart-pill-rs">RS vs {payload.benchmark_ticker}</span> : null}
              {latestRsMarker ? (
                <span className="chart-pill chart-pill-rs">
                  {latestRsMarker.kind === "daily_new_high_before_price" ? "RS new high before price" : "RS new high"}
                </span>
              ) : null}
              {payload?.data_source ? <span className="chart-pill chart-pill-setup">Source {payload.data_source}</span> : null}
              {historicalSetupMarkers.length > 0 ? <span className="chart-pill chart-pill-setup">{historicalSetupMarkers.length} old FTD sweep marker(s)</span> : null}
              {atr14 != null ? <span className="chart-pill chart-pill-setup">ATR14 {formatPrice(atr14)}</span> : null}
              {atrMultipleFrom50Ma != null ? <span className="chart-pill chart-pill-setup">50MA {formatAtrMultiple(atrMultipleFrom50Ma)}</span> : null}
              {hasTrimWarning ? <span className="chart-pill chart-pill-event">Trim warning: 3x ATR above 50MA</span> : null}
              {atrExtensionMarkers.length > 0 ? <span className="chart-pill chart-pill-setup">{atrExtensionMarkers.length} ATR extension dot(s)</span> : null}
              {setupAnnotations.map((item, index) =>
                item.setupLabel ? (
                  <span key={`${item.setupLabel}-${index}`} className="chart-pill chart-pill-setup">
                    {item.setupLabel}
                  </span>
                ) : null,
              )}
            </div>
            <div className="rs-rating-grid">
              <RsRatingTimelinePanel
                title="RS Rating Daily"
                timeline={chartData.map((item) => item.time)}
                series={dailyRsRatingSeries}
                emptyLabel="Daily RS rating needs more history."
                visibleIndexRange={chartVisibleIndexRange}
                hoveredTime={chartHoveredTime}
              />
              <RsRatingMiniChart
                title="RS Rating Weekly"
                series={weeklyRsRatingSeries}
                emptyLabel="Weekly RS rating unavailable yet."
              />
            </div>
          </>
        ) : null}
      </Panel>

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

function RsRatingTimelinePanel({
  title,
  timeline,
  series,
  emptyLabel,
  visibleIndexRange,
  hoveredTime,
}: {
  title: string;
  timeline: string[];
  series: Array<{ time: string; value: number }>;
  emptyLabel: string;
  visibleIndexRange: { from: number; to: number } | null;
  hoveredTime: string | null;
}) {
  const visibleTimeline = useMemo(() => {
    if (timeline.length === 0) {
      return [];
    }
    if (!visibleIndexRange) {
      return timeline;
    }
    return timeline.slice(visibleIndexRange.from, visibleIndexRange.to + 1);
  }, [timeline, visibleIndexRange]);
  const seriesByTime = useMemo(() => new Map(series.map((item) => [item.time, item.value] as const)), [series]);
  const visibleSeries = useMemo(
    () =>
      visibleTimeline.map((time) => ({
        time,
        value: seriesByTime.get(time) ?? null,
      })),
    [seriesByTime, visibleTimeline],
  );
  const chartData = useMemo(() => buildRsTimelinePaths(visibleSeries), [visibleSeries]);
  const latestPoint = [...visibleSeries].reverse().find((item) => item.value != null) ?? null;
  const hoverPointIndex = hoveredTime ? visibleTimeline.findIndex((time) => time === hoveredTime) : -1;
  const hoverX = chartData && hoverPointIndex >= 0 ? chartData.left + hoverPointIndex * chartData.step + Math.max(0.5, chartData.step / 2) : null;
  const dateMarkers = useMemo(
    () => (chartData ? buildRsTimelineMarkers(visibleTimeline, chartData.left, chartData.step) : []),
    [chartData, visibleTimeline],
  );

  return (
    <div className="chart-card rs-rating-card">
      <div className="rs-rating-card-head">
        <div>
          <div className="chart-rs-header">{title}</div>
          <div className="rs-rating-meta">{latestPoint?.time ? `Latest ${latestPoint.time}` : emptyLabel}</div>
        </div>
        <div className="rs-rating-value">{latestPoint?.value == null ? "--" : latestPoint.value.toFixed(1)}</div>
      </div>
      {!chartData ? (
        <p className="panel-copy">{emptyLabel}</p>
      ) : (
        <svg className="rs-rating-svg" viewBox="0 0 560 180" preserveAspectRatio="none" aria-label={title}>
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
          {chartData.areaSegments.map((segment, index) => (
            <path key={`area-${index}`} d={segment} fill="rgba(96, 165, 250, 0.12)" />
          ))}
          {chartData.lineSegments.map((segment, index) => (
            <path key={`line-${index}`} d={segment} fill="none" stroke="#60a5fa" strokeWidth="2.5" strokeLinejoin="round" strokeLinecap="round" />
          ))}
          {hoverX != null ? <line x1={hoverX} y1={8} x2={hoverX} y2={160} stroke="#60a5fa" strokeWidth="1" strokeDasharray="4 4" opacity="0.95" /> : null}
          {chartData.lastPoint ? <circle cx={chartData.lastPoint.x} cy={chartData.lastPoint.y} r="4" fill="#93c5fd" stroke="#0f172a" strokeWidth="1.5" /> : null}
          {dateMarkers.map((marker) => (
            <g key={`date-${marker.time}`}>
              <line x1={marker.x} y1={160} x2={marker.x} y2={165} stroke="#a1a1aa" strokeWidth="1" />
              <text x={marker.x} y={174} fill="#a1a1aa" fontSize="11" textAnchor="middle">
                {marker.label}
              </text>
            </g>
          ))}
        </svg>
      )}
    </div>
  );
}

function RsRatingMiniChart({
  title,
  series,
  emptyLabel,
}: {
  title: string;
  series: Array<{ time: string; value: number }>;
  emptyLabel: string;
}) {
  const path = useMemo(
    () => buildRsTimelinePaths(series.map((item) => ({ ...item, value: item.value }))),
    [series],
  );
  const latestValue = series.length > 0 ? series[series.length - 1]?.value ?? null : null;
  const latestTime = series.length > 0 ? series[series.length - 1]?.time ?? "" : "";

  return (
    <div className="chart-card rs-rating-card">
      <div className="rs-rating-card-head">
        <div>
          <div className="chart-rs-header">{title}</div>
          <div className="rs-rating-meta">{latestTime ? `Latest ${latestTime}` : emptyLabel}</div>
        </div>
        <div className="rs-rating-value">{latestValue == null ? "--" : latestValue.toFixed(1)}</div>
      </div>
      {series.length === 0 || path == null ? (
        <p className="panel-copy">{emptyLabel}</p>
      ) : (
        <svg className="rs-rating-svg" viewBox="0 0 560 180" preserveAspectRatio="none" aria-label={title}>
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
          {path.areaSegments.map((segment, index) => (
            <path key={`mini-area-${index}`} d={segment} fill="rgba(96, 165, 250, 0.12)" />
          ))}
          {path.lineSegments.map((segment, index) => (
            <path key={`mini-line-${index}`} d={segment} fill="none" stroke="#60a5fa" strokeWidth="2.5" strokeLinejoin="round" strokeLinecap="round" />
          ))}
          {path.lastPoint ? <circle cx={path.lastPoint.x} cy={path.lastPoint.y} r="4" fill="#93c5fd" stroke="#0f172a" strokeWidth="1.5" /> : null}
        </svg>
      )}
    </div>
  );
}

function buildRsTimelinePaths(series: Array<{ time: string; value: number | null }>) {
  if (series.length < 2) {
    return null;
  }
  const width = 560;
  const height = 180;
  const left = 10;
  const right = 10;
  const top = 14;
  const bottom = 20;
  const usableWidth = width - left - right;
  const step = usableWidth / Math.max(1, series.length - 1);
  const baselineY = height - bottom;
  const lineSegments: string[] = [];
  const areaSegments: string[] = [];
  let currentSegment: Array<{ x: number; y: number }> = [];
  let lastPoint: { x: number; y: number } | null = null;

  for (let index = 0; index < series.length; index += 1) {
    const point = series[index];
    const x = left + step * index;
    if (point.value == null) {
      if (currentSegment.length > 1) {
        lineSegments.push(currentSegment.map((item, itemIndex) => `${itemIndex === 0 ? "M" : "L"} ${item.x.toFixed(2)} ${item.y.toFixed(2)}`).join(" "));
        areaSegments.push(
          `${currentSegment.map((item, itemIndex) => `${itemIndex === 0 ? "M" : "L"} ${item.x.toFixed(2)} ${item.y.toFixed(2)}`).join(" ")} L ${currentSegment[currentSegment.length - 1].x.toFixed(2)} ${baselineY} L ${currentSegment[0].x.toFixed(2)} ${baselineY} Z`,
        );
      }
      currentSegment = [];
      continue;
    }
    const y = ratingToChartY(point.value, { top, bottom, height });
    const nextPoint = { x, y };
    currentSegment.push(nextPoint);
    lastPoint = nextPoint;
  }

  if (currentSegment.length > 1) {
    lineSegments.push(currentSegment.map((item, itemIndex) => `${itemIndex === 0 ? "M" : "L"} ${item.x.toFixed(2)} ${item.y.toFixed(2)}`).join(" "));
    areaSegments.push(
      `${currentSegment.map((item, itemIndex) => `${itemIndex === 0 ? "M" : "L"} ${item.x.toFixed(2)} ${item.y.toFixed(2)}`).join(" ")} L ${currentSegment[currentSegment.length - 1].x.toFixed(2)} ${baselineY} L ${currentSegment[0].x.toFixed(2)} ${baselineY} Z`,
    );
  }

  if (lineSegments.length === 0) {
    return null;
  }

  return {
    left,
    step,
    lineSegments,
    areaSegments,
    lastPoint,
  };
}

function buildRsTimelineMarkers(points: string[], left: number, step: number) {
  if (points.length === 0) {
    return [];
  }
  const targetCount = Math.min(6, points.length);
  const markers: Array<{ time: string; label: string; x: number }> = [];
  for (let markerIndex = 0; markerIndex < targetCount; markerIndex += 1) {
    const pointIndex = targetCount === 1 ? points.length - 1 : Math.round((markerIndex * (points.length - 1)) / (targetCount - 1));
    const point = points[pointIndex];
    if (!point) {
      continue;
    }
    markers.push({
      time: point,
      label: formatRsTimelineDate(point),
      x: left + pointIndex * step,
    });
  }
  return markers.filter((marker, index, source) => source.findIndex((item) => item.time === marker.time) === index);
}

function formatRsTimelineDate(value: string) {
  const [year, month, day] = value.split("-");
  if (!year || !month || !day) {
    return value;
  }
  return `${year.slice(2)}-${month}-${day}`;
}

function ratingToChartY(
  value: number,
  dimensions: { top?: number; bottom?: number; height?: number } = {},
) {
  const top = dimensions.top ?? 14;
  const bottom = dimensions.bottom ?? 20;
  const height = dimensions.height ?? 180;
  const clamped = Math.max(0, Math.min(100, value));
  const usableHeight = height - top - bottom;
  return top + ((100 - clamped) / 100) * usableHeight;
}

function buildChartCacheKey(kind: "payload" | "fundamentals" | "insider", ticker: string, scope: string) {
  return `${CHART_CACHE_PREFIX}:${kind}:${ticker}:${scope}`;
}

function readChartCache<T>(key: string): T | null {
  try {
    const raw = localStorage.getItem(key);
    if (!raw) {
      return null;
    }
    const parsed = JSON.parse(raw) as { value?: T; expiresAt?: number };
    if (!parsed || typeof parsed !== "object" || typeof parsed.expiresAt !== "number" || parsed.expiresAt <= Date.now()) {
      localStorage.removeItem(key);
      return null;
    }
    return (parsed.value ?? null) as T | null;
  } catch {
    localStorage.removeItem(key);
    return null;
  }
}

function writeChartCache<T>(key: string, value: T) {
  const kind = resolveChartCacheKind(key);
  const ttlMs = kind ? CHART_CACHE_TTL_MS_BY_KIND[kind] : 0;
  if (ttlMs <= 0) {
    return;
  }
  localStorage.setItem(
    key,
    JSON.stringify({
      value,
      expiresAt: Date.now() + ttlMs,
    }),
  );
}

function clearChartCacheKey(key: string) {
  try {
    localStorage.removeItem(key);
  } catch {
    // Ignore storage errors while clearing stale chart cache entries.
  }
}

function resolveChartCacheKind(key: string): "payload" | "fundamentals" | "insider" | null {
  const suffix = key.replace(`${CHART_CACHE_PREFIX}:`, "");
  if (suffix.startsWith("payload:")) {
    return "payload";
  }
  if (suffix.startsWith("fundamentals:")) {
    return "fundamentals";
  }
  if (suffix.startsWith("insider:")) {
    return "insider";
  }
  return null;
}

function hasUsableChartData(payload: WatchlistChartResponse | null | undefined): boolean {
  return Boolean(payload && Array.isArray(payload.candles) && payload.candles.length > 0);
}

function clearChartCacheForTicker(ticker: string) {
  if (!ticker) {
    return;
  }
  const prefix = `${CHART_CACHE_PREFIX}:`;
  const tickerFragment = `:${ticker}:`;
  for (let index = localStorage.length - 1; index >= 0; index -= 1) {
    const key = localStorage.key(index);
    if (!key || !key.startsWith(prefix) || !key.includes(tickerFragment)) {
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

function formatPercent(value: number | null | undefined) {
  return value == null ? "--" : `${value.toFixed(2)}%`;
}

function formatAtrMultiple(value: number | null | undefined) {
  if (value == null) {
    return "--";
  }
  const prefix = value > 0 ? "+" : "";
  return `${prefix}${value.toFixed(2)}x`;
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

function buildSetupAnnotation(id: string, hit: Record<string, unknown>): ChartAnnotations | null {
  switch (id) {
    case "hve":
      return {
        setupLabel: "HVE",
        eventDate: readString(hit.signal_date),
        eventLabel: "HVE signal",
        triggerPrice: readNumber(hit.high_price),
        triggerLabel: "Signal high",
        entryPrice: readNumber(hit.current_price),
        entryLabel: "Signal close",
        secondaryEntryPrice: readNumber(hit.ma50),
        secondaryEntryLabel: "50D MA",
        stopPrice: readNumber(hit.low_price),
        stopLabel: "Signal low",
      };
    case "inside_dryup":
      return {
        setupLabel: "Inside Day Dry-Up",
        eventDate: readString(hit.signal_date),
        eventLabel: "Inside day",
        triggerPrice: readNumber(hit.trigger_price),
        triggerLabel: "Inside-day high",
        entryPrice: readNumber(hit.trigger_price),
        entryLabel: "Trigger",
        secondaryEntryPrice: readNumber(hit.ema21),
        secondaryEntryLabel: "21 EMA",
        stopPrice: readNumber(hit.stop_price),
        stopLabel: "Inside-day low",
      };
    case "ftd_sweep":
      return {
        setupLabel: "FTD Sweep Breakout",
        eventDate: readString(hit.sweep_breakout_date),
        eventLabel: "Sweep breakout",
        triggerPrice: readNumber(hit.ftd_high),
        triggerLabel: "FTD High",
        entryPrice: readNumber(hit.breakout_level),
        entryLabel: "Breakout",
        secondaryEntryPrice: readNumber(hit.sweep_low),
        secondaryEntryLabel: "Sweep low",
        secondaryEntryLow: readNumber(hit.sweep_low),
        secondaryEntryHigh: readNumber(hit.ftd_high),
        stopPrice: readNumber(hit.ftd_pivot_low),
        stopLabel: "Pivot low",
      };
    case "weekly_htf_pullback":
      return {
        setupLabel: "Weekly HTF Pullback",
        eventDate: readString(hit.htf_runup_high_date),
        eventLabel: "Runup high",
        triggerPrice: readNumber(hit.weekly_ema8),
        triggerLabel: "8W EMA",
        secondaryEntryPrice: readNumber(hit.htf_runup_high),
        secondaryEntryLabel: "Runup high",
      };
    case "htf_8w_runup":
      return {
        setupLabel: "HTF 8W Runup",
        eventDate: readString(hit.runup_high_date),
        eventLabel: "Runup high",
        triggerPrice: readNumber(hit.runup_high),
        triggerLabel: "Runup high",
      };
    case "vcp":
      return {
        setupLabel: "VCP",
        triggerPrice: readNumber(hit.pivot_price),
        triggerLabel: "Pivot",
        entryPrice: readNumber(hit.pivot_price),
        entryLabel: "Pivot",
        stopPrice: readNumber(hit.support_price),
        stopLabel: "Support",
      };
    default:
      return null;
  }
}

function readString(value: unknown): string | null {
  return typeof value === "string" && value.trim() ? value : null;
}

function readNumber(value: unknown): number | null {
  return typeof value === "number" && Number.isFinite(value) ? value : null;
}
