import { useEffect, useMemo, useState, type FormEvent } from "react";
import { Link, useSearchParams } from "react-router-dom";
import { LoadingBlock } from "../components/LoadingBlock";
import { Panel } from "../components/Panel";
import { PriceChart, type ChartVisibility } from "../components/PriceChart";
import { fetchJson } from "../lib/api";
import type { AdHocScreenResponse, CandlePoint, ChartAnnotations, WatchlistChartResponse } from "../lib/types";

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
};

export function ChartsPage() {
  const setupOptions = [
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
  const [setupPayload, setSetupPayload] = useState<AdHocScreenResponse | null>(null);
  const [isLoading, setIsLoading] = useState(false);
  const [isSetupLoading, setIsSetupLoading] = useState(false);
  const [notice, setNotice] = useState("");
  const [setupNotice, setSetupNotice] = useState("");
  const [chartVisibility, setChartVisibility] = useState<ChartVisibility>(DEFAULT_CHART_VISIBILITY);
  const [selectedSetups, setSelectedSetups] = useState<Record<string, boolean>>({
    ftd_sweep: false,
    weekly_htf_pullback: false,
    htf_8w_runup: false,
    vcp: false,
  });

  useEffect(() => {
    setTickerInput(requestedTicker);
    setDateInput(requestedDate);
  }, [requestedDate, requestedTicker]);

  useEffect(() => {
    if (!requestedTicker) {
      setPayload(null);
      setSetupPayload(null);
      setNotice("");
      return;
    }
    setIsLoading(true);
    setNotice("");
    const query = new URLSearchParams({ period: "18mo" });
    if (requestedDate) {
      query.set("asOfDate", requestedDate);
    }
    void fetchJson<WatchlistChartResponse>(`/api/charts/${requestedTicker}?${query.toString()}`)
      .then((response) => {
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
  }, [requestedDate, requestedTicker]);

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
  const setupAnnotations = useMemo<ChartAnnotations[]>(() => {
    return (setupPayload?.screeners ?? [])
      .filter((item) => item.passed && item.hit)
      .map((item) => buildSetupAnnotation(item.id, item.hit!))
      .filter((item): item is ChartAnnotations => item !== null);
  }, [setupPayload]);
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
  ];

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
          {notice ? <p className="panel-copy">{notice}</p> : <p className="panel-copy">Standalone ticker chart with RS line, MA stack, gap zones, HTF box, and fearzone panel.</p>}
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
          </div>
        </form>
      </Panel>

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
              visibility={chartVisibility}
              forceFearzonePanel
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
              {setupAnnotations.map((item, index) =>
                item.setupLabel ? (
                  <span key={`${item.setupLabel}-${index}`} className="chart-pill chart-pill-setup">
                    {item.setupLabel}
                  </span>
                ) : null,
              )}
            </div>
          </>
        ) : null}
      </Panel>
    </div>
  );
}

function formatPrice(value: number | null) {
  return value == null ? "--" : `$${value.toFixed(2)}`;
}

function buildSetupAnnotation(id: string, hit: Record<string, unknown>): ChartAnnotations | null {
  switch (id) {
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
