import { useEffect, useMemo, useState, type FormEvent } from "react";
import { Link, useSearchParams } from "react-router-dom";
import { LoadingBlock } from "../components/LoadingBlock";
import { Panel } from "../components/Panel";
import { PriceChart, type ChartVisibility } from "../components/PriceChart";
import { fetchJson } from "../lib/api";
import type { AdHocScreenResponse, CandlePoint, ChartAnnotations, ChartFundamentalsResponse, ChartInsiderResponse, WatchlistChartResponse } from "../lib/types";

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
      setFundamentalsPayload(null);
      setInsiderPayload(null);
      setSetupPayload(null);
      setNotice("");
      setFundamentalsNotice("");
      setInsiderNotice("");
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
    void fetchJson<ChartInsiderResponse>(`/api/chart-insider/${requestedTicker}?${query.toString()}`)
      .then((response) => {
        setInsiderPayload(response);
      })
      .catch((error) => {
        setInsiderPayload(null);
        setInsiderNotice(error instanceof Error ? error.message : "Failed to load insider trades.");
      })
      .finally(() => setIsInsiderLoading(false));
  }, [payload?.resolved_as_of_date, requestedDate, requestedTicker]);

  useEffect(() => {
    if (!requestedTicker) {
      setFundamentalsPayload(null);
      setFundamentalsNotice("");
      return;
    }
    setIsFundamentalsLoading(true);
    setFundamentalsNotice("");
    void fetchJson<ChartFundamentalsResponse>(`/api/chart-fundamentals/${requestedTicker}?earningsLimit=4`)
      .then((response) => {
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
  }, [requestedTicker]);

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
              extraMarkers={historicalSetupMarkers}
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
              {historicalSetupMarkers.length > 0 ? <span className="chart-pill chart-pill-setup">{historicalSetupMarkers.length} old FTD sweep marker(s)</span> : null}
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
