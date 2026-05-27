import { useEffect, useMemo, useState } from "react";
import { Panel } from "../components/Panel";
import { PriceChart, type ChartVisibility } from "../components/PriceChart";
import { fetchJson } from "../lib/api";
import type { CandlePoint, ChartAnnotations, WatchlistChartResponse, WatchlistDetailResponse, WatchlistFile } from "../lib/types";

export function WatchlistsPage() {
  const [watchlists, setWatchlists] = useState<WatchlistFile[]>([]);
  const [selectedStem, setSelectedStem] = useState("");
  const [detail, setDetail] = useState<WatchlistDetailResponse | null>(null);
  const [selectedTicker, setSelectedTicker] = useState<Record<string, unknown> | null>(null);
  const [chartPayload, setChartPayload] = useState<WatchlistChartResponse | null>(null);
  const [chartVisibility, setChartVisibility] = useState<ChartVisibility>({
    ema8: true,
    ema21: true,
    weeklyEma8: true,
    ipoVwap: true,
    maStack: true,
    gapZones: true,
    rsLine: true,
    rsSignals: true,
  });

  useEffect(() => {
    void fetchJson<{ watchlists: WatchlistFile[] }>("/api/watchlists").then((payload) => {
      setWatchlists(payload.watchlists);
      if (payload.watchlists[0]) {
        setSelectedStem(payload.watchlists[0].stem);
      }
    });
  }, []);

  useEffect(() => {
    if (!selectedStem) {
      return;
    }
    void fetchJson<WatchlistDetailResponse>(`/api/watchlists/${selectedStem}`).then((payload) => {
      setDetail(payload);
      setSelectedTicker(payload.entries[0] ?? null);
    });
  }, [selectedStem]);

  useEffect(() => {
    const ticker = typeof selectedTicker?.ticker === "string" ? selectedTicker.ticker : "";
    if (!selectedStem || !ticker) {
      return;
    }
    void fetchJson<WatchlistChartResponse>(`/api/watchlists/${selectedStem}/chart/${ticker}`).then(setChartPayload);
  }, [selectedStem, selectedTicker]);

  const ticker = typeof selectedTicker?.ticker === "string" ? selectedTicker.ticker : "--";
  const company = typeof selectedTicker?.company_name === "string" ? selectedTicker.company_name : "";
  const industry = typeof selectedTicker?.industry === "string" ? selectedTicker.industry : "Unknown";
  const score = Number(selectedTicker?.rs_rank ?? selectedTicker?.score ?? 0);
  const summary = typeof selectedTicker?.summary === "string" ? selectedTicker.summary : "No summary available yet.";
  const smallChartData = useMemo<CandlePoint[]>(
    () =>
      (chartPayload?.candles ?? []).map((item, index) => ({
        ...item,
        volume: chartPayload?.volume[index]?.value ?? 0,
      })),
    [chartPayload],
  );
  const selectedPrice = resolveDisplayPrice(selectedTicker, chartPayload);
  const selectedChangePct = resolveDisplayChangePct(selectedTicker, chartPayload);
  const indicatorTone = selectedChangePct == null ? "neutral" : selectedChangePct >= 0 ? "positive" : "negative";
  const annotations = useMemo<ChartAnnotations>(
    () => ({
      setupLabel: typeof selectedTicker?.setup_label === "string" ? selectedTicker.setup_label : undefined,
      eventDate: typeof selectedTicker?.event_date === "string" ? selectedTicker.event_date : null,
      eventLabel: typeof selectedTicker?.event_label === "string" ? selectedTicker.event_label : null,
      triggerPrice: toNullableNumber(selectedTicker?.trigger_price),
      triggerLabel: typeof selectedTicker?.trigger_label === "string" ? selectedTicker.trigger_label : null,
      entryPrice: toNullableNumber(selectedTicker?.entry_price),
      entryLabel: typeof selectedTicker?.entry_label === "string" ? selectedTicker.entry_label : null,
      secondaryEntryPrice: toNullableNumber(selectedTicker?.secondary_entry_price),
      secondaryEntryLabel:
        typeof selectedTicker?.secondary_entry_label === "string" ? selectedTicker.secondary_entry_label : null,
      secondaryEntryLow: toNullableNumber(selectedTicker?.secondary_entry_low),
      secondaryEntryHigh: toNullableNumber(selectedTicker?.secondary_entry_high),
      stopPrice: toNullableNumber(selectedTicker?.stop_price),
      stopLabel: typeof selectedTicker?.stop_label === "string" ? selectedTicker.stop_label : null,
    }),
    [selectedTicker],
  );
  const latestRsMarker = chartPayload?.rs_markers?.[chartPayload.rs_markers.length - 1] ?? null;
  const chartToggles: Array<{ key: keyof ChartVisibility; label: string }> = [
    { key: "ema8", label: "EMA 8" },
    { key: "ema21", label: "EMA 21" },
    { key: "weeklyEma8", label: "Weekly 8 EMA" },
    { key: "ipoVwap", label: "IPO VWAP" },
    { key: "maStack", label: "MA stack" },
    { key: "gapZones", label: "Gap zones" },
    { key: "rsLine", label: "RS line" },
    { key: "rsSignals", label: "RS markers" },
  ];

  return (
    <div className="watchlists-layout">
      <aside className="panel files-pane">
        <div className="panel-head">
          <h2>Files (JSON)</h2>
        </div>
        <div className="file-list">
          {watchlists.map((file) => (
            <button
              key={file.stem}
              className={`file-row file-button${selectedStem === file.stem ? " is-selected" : ""}`}
              onClick={() => setSelectedStem(file.stem)}
              type="button"
            >
              <div className="file-name">{file.name}</div>
              <div className="file-meta">{file.stem}</div>
            </button>
          ))}
        </div>

        <div className="panel-head inline-head">
          <h2>Ticker List ({detail?.entry_count ?? 0})</h2>
        </div>
        <div className="ticker-list">
          {(detail?.entries ?? []).map((item, index) => (
            <button
              key={`${item.ticker ?? "ticker"}-${index}`}
              className={`ticker-row${ticker === item.ticker ? " is-selected" : ""}`}
              onClick={() => setSelectedTicker(item)}
              type="button"
            >
              <div>
                <div className="ticker-symbol">{String(item.ticker ?? "--")}</div>
                <div className="ticker-company">{String(item.company_name ?? "")}</div>
                <div className="ticker-tag">
                  RS Rank: {Number(item.rs_rank ?? item.score ?? 0)}
                </div>
              </div>
              <div className="ticker-side">
                <div className="ticker-price">{formatPrice(resolveDisplayPrice(item, null))}</div>
                {renderChange(resolveDisplayChangePct(item, null))}
              </div>
            </button>
          ))}
        </div>
      </aside>

      <div className="watchlists-main">
        <section className="hero-strip">
          <div>
            <div className="hero-symbol-row">
              <h1>{ticker}</h1>
              <span className="ticker-exchange">NASDAQGS</span>
              <span className="ticker-company-inline">{company}</span>
            </div>
            <div className="hero-price-row">
              <span className="hero-price">{formatPrice(selectedPrice)}</span>
              {selectedPrice != null && selectedChangePct != null ? (
                <span className={`hero-change ${indicatorTone}`}>
                  {selectedChangePct >= 0 ? "+" : ""}
                  {(selectedPrice * selectedChangePct / 100).toFixed(2)} ({selectedChangePct.toFixed(2)}%)
                </span>
              ) : (
                <span className="hero-change neutral">Change unavailable</span>
              )}
            </div>
          </div>
          <div className="hero-stats">
            <div>
              <span className="eyebrow">Industry</span>
              <strong>{industry}</strong>
            </div>
            <div>
              <span className="eyebrow">Score</span>
              <strong>{score}</strong>
            </div>
            <div>
              <span className="eyebrow">Mode</span>
              <strong>1D</strong>
            </div>
          </div>
        </section>

        <Panel
          title="Candles"
          aside={
            <div className="legend-row legend-row-compact">
              <span className="legend-marker legend-marker-event" aria-hidden="true" />
              <span>Event</span>
              <span className="legend-marker legend-marker-gap" aria-hidden="true" />
              <span>Gap</span>
              <span className="legend-marker legend-marker-rs" aria-hidden="true" />
              <span>RS NH</span>
              <span className="legend-marker legend-marker-rs-before" aria-hidden="true" />
              <span>RS NH before price</span>
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
          <PriceChart
            ticker={ticker}
            candles={smallChartData}
            overlays={chartPayload ?? undefined}
            annotations={annotations}
            visibility={chartVisibility}
          />
          <div className="chart-annotation-strip">
            {annotations.setupLabel ? <span className="chart-pill chart-pill-setup">{annotations.setupLabel}</span> : null}
            {annotations.eventDate ? <span className="chart-pill chart-pill-event">{annotations.eventLabel ?? "Event"} {annotations.eventDate}</span> : null}
            {annotations.triggerPrice != null ? <span className="chart-pill chart-pill-trigger">{annotations.triggerLabel ?? "Trigger"} {annotations.triggerPrice.toFixed(2)}</span> : null}
            {annotations.entryPrice != null ? <span className="chart-pill chart-pill-entry">{annotations.entryLabel ?? "Entry"} {annotations.entryPrice.toFixed(2)}</span> : null}
            {annotations.secondaryEntryPrice != null ? <span className="chart-pill chart-pill-secondary">{annotations.secondaryEntryLabel ?? "Secondary"} {annotations.secondaryEntryPrice.toFixed(2)}</span> : null}
            {annotations.stopPrice != null ? <span className="chart-pill chart-pill-stop">{annotations.stopLabel ?? "Stop"} {annotations.stopPrice.toFixed(2)}</span> : null}
            {chartPayload?.benchmark_ticker ? <span className="chart-pill chart-pill-rs">RS vs {chartPayload.benchmark_ticker}</span> : null}
            {latestRsMarker ? (
              <span className="chart-pill chart-pill-rs">
                {latestRsMarker.kind === "daily_new_high_before_price" ? "RS new high before price" : "RS new high"}
              </span>
            ) : null}
          </div>
        </Panel>

        <section className="tab-strip">
          <button className="tab-button is-active">Summary</button>
          <button className="tab-button">Financials</button>
          <button className="tab-button">News</button>
          <button className="tab-button">Analyst Estimates</button>
        </section>

        <div className="summary-grid">
          <Panel title="Growth Trend">
            <div className="summary-stat positive">Accelerating</div>
            <p className="panel-copy">{summary}</p>
          </Panel>
          <Panel title="RS Rank">
            <div className="big-number">{score}</div>
          </Panel>
          <Panel title="Group Rank">
            <div className="big-number">1 / 197</div>
          </Panel>
        </div>
      </div>
    </div>
  );
}

function toNullableNumber(value: unknown): number | null {
  if (value === null || value === undefined || value === "") {
    return null;
  }
  const numeric = Number(value);
  return Number.isFinite(numeric) ? numeric : null;
}

function resolveDisplayPrice(entry: Record<string, unknown> | null | undefined, chartPayload: WatchlistChartResponse | null): number | null {
  const candidates = [
    entry?.current_price,
    entry?.last_price,
    entry?.signal_close,
    entry?.close,
    entry?.entry_price,
    entry?.trigger_price,
    entry?.secondary_entry_price,
    latestCloseFromChart(chartPayload),
  ];
  for (const candidate of candidates) {
    const value = toNullableNumber(candidate);
    if (value != null) {
      return value;
    }
  }
  return null;
}

function resolveDisplayChangePct(entry: Record<string, unknown> | null | undefined, chartPayload: WatchlistChartResponse | null): number | null {
  const candidates = [
    entry?.price_change_pct,
    entry?.daily_change_pct,
    entry?.change_pct,
    entry?.pct_change,
    latestChangePctFromChart(chartPayload),
  ];
  for (const candidate of candidates) {
    const value = toNullableNumber(candidate);
    if (value != null) {
      return value;
    }
  }
  return null;
}

function latestCloseFromChart(chartPayload: WatchlistChartResponse | null): number | null {
  const candles = chartPayload?.candles ?? [];
  const latest = candles[candles.length - 1];
  return latest ? latest.close : null;
}

function latestChangePctFromChart(chartPayload: WatchlistChartResponse | null): number | null {
  const candles = chartPayload?.candles ?? [];
  if (candles.length < 2) {
    return null;
  }
  const previous = candles[candles.length - 2]?.close;
  const latest = candles[candles.length - 1]?.close;
  if (previous == null || latest == null || previous === 0) {
    return null;
  }
  return ((latest - previous) / previous) * 100;
}

function formatPrice(value: number | null): string {
  return value == null ? "—" : value.toFixed(2);
}

function renderChange(changePct: number | null) {
  if (changePct == null) {
    return <div className="ticker-change flat">—</div>;
  }
  return (
    <div className={`ticker-change ${changePct >= 0 ? "up" : "down"}`}>
      {changePct >= 0 ? "+" : ""}
      {changePct.toFixed(2)}%
    </div>
  );
}
