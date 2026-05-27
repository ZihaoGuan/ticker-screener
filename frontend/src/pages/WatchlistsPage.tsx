import { useEffect, useMemo, useState } from "react";
import { Panel } from "../components/Panel";
import { PriceChart } from "../components/PriceChart";
import { fetchJson } from "../lib/api";
import type { CandlePoint, ChartAnnotations, WatchlistChartResponse, WatchlistDetailResponse, WatchlistFile } from "../lib/types";

export function WatchlistsPage() {
  const [watchlists, setWatchlists] = useState<WatchlistFile[]>([]);
  const [selectedStem, setSelectedStem] = useState("");
  const [detail, setDetail] = useState<WatchlistDetailResponse | null>(null);
  const [selectedTicker, setSelectedTicker] = useState<Record<string, unknown> | null>(null);
  const [chartPayload, setChartPayload] = useState<WatchlistChartResponse | null>(null);

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
  const lastPrice = Number(selectedTicker?.current_price ?? selectedTicker?.last_price ?? 0);
  const dailyChangePct = Number(selectedTicker?.price_change_pct ?? selectedTicker?.daily_change_pct ?? 0);
  const summary = typeof selectedTicker?.summary === "string" ? selectedTicker.summary : "No summary available yet.";
  const positive = dailyChangePct >= 0;
  const indicatorTone = positive ? "positive" : "negative";
  const smallChartData = useMemo<CandlePoint[]>(
    () =>
      (chartPayload?.candles ?? []).map((item, index) => ({
        ...item,
        volume: chartPayload?.volume[index]?.value ?? 0,
      })),
    [chartPayload],
  );
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
                <div className="ticker-price">{Number(item.current_price ?? item.last_price ?? 0).toFixed(2)}</div>
                <div className={`ticker-change ${Number(item.price_change_pct ?? item.daily_change_pct ?? 0) >= 0 ? "up" : "down"}`}>
                  {Number(item.price_change_pct ?? item.daily_change_pct ?? 0) >= 0 ? "+" : ""}
                  {Number(item.price_change_pct ?? item.daily_change_pct ?? 0).toFixed(2)}%
                </div>
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
              <span className="hero-price">{lastPrice.toFixed(2)}</span>
              <span className={`hero-change ${indicatorTone}`}>
                {dailyChangePct >= 0 ? "+" : ""}
                {(lastPrice * dailyChangePct / 100).toFixed(2)} ({dailyChangePct.toFixed(2)}%)
              </span>
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

        <Panel title="Candles" aside={<div className="legend-row"><span>MA (20)</span><span>MA (50)</span><span>MA (200)</span><span>Gap Zones</span><span>Trigger / Entry / Stop</span></div>}>
          <PriceChart ticker={ticker} candles={smallChartData} overlays={chartPayload ?? undefined} annotations={annotations} />
          <div className="chart-annotation-strip">
            {annotations.setupLabel ? <span className="chart-pill chart-pill-setup">{annotations.setupLabel}</span> : null}
            {annotations.eventDate ? <span className="chart-pill chart-pill-event">{annotations.eventLabel ?? "Event"} {annotations.eventDate}</span> : null}
            {annotations.triggerPrice != null ? <span className="chart-pill chart-pill-trigger">{annotations.triggerLabel ?? "Trigger"} {annotations.triggerPrice.toFixed(2)}</span> : null}
            {annotations.entryPrice != null ? <span className="chart-pill chart-pill-entry">{annotations.entryLabel ?? "Entry"} {annotations.entryPrice.toFixed(2)}</span> : null}
            {annotations.secondaryEntryPrice != null ? <span className="chart-pill chart-pill-secondary">{annotations.secondaryEntryLabel ?? "Secondary"} {annotations.secondaryEntryPrice.toFixed(2)}</span> : null}
            {annotations.stopPrice != null ? <span className="chart-pill chart-pill-stop">{annotations.stopLabel ?? "Stop"} {annotations.stopPrice.toFixed(2)}</span> : null}
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
