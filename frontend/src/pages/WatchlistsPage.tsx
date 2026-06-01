import { useEffect, useMemo, useState } from "react";
import { Link, useSearchParams } from "react-router-dom";
import { useAuth } from "../auth/AuthContext";
import { ExclusionDialog } from "../components/ExclusionDialog";
import { LoadingBlock } from "../components/LoadingBlock";
import { Panel } from "../components/Panel";
import { PriceChart, type ChartVisibility } from "../components/PriceChart";
import { fetchJson } from "../lib/api";
import { formatLocalDate, formatLocalDateTime } from "../lib/format";
import type { CandlePoint, ChartAnnotations, WatchlistChartResponse, WatchlistDetailResponse, WatchlistFile } from "../lib/types";

const DEFAULT_VISIBLE_TICKERS = 80;

export function WatchlistsPage() {
  const auth = useAuth();
  const [searchParams, setSearchParams] = useSearchParams();
  const [watchlists, setWatchlists] = useState<WatchlistFile[]>([]);
  const [selectedStem, setSelectedStem] = useState("");
  const [detail, setDetail] = useState<WatchlistDetailResponse | null>(null);
  const [selectedTicker, setSelectedTicker] = useState<Record<string, unknown> | null>(null);
  const [chartPayload, setChartPayload] = useState<WatchlistChartResponse | null>(null);
  const [tickerSearch, setTickerSearch] = useState("");
  const [showAllTickers, setShowAllTickers] = useState(false);
  const [isFilesLoading, setIsFilesLoading] = useState(true);
  const [isDetailLoading, setIsDetailLoading] = useState(false);
  const [isChartLoading, setIsChartLoading] = useState(false);
  const [notice, setNotice] = useState("");
  const [isExclusionDialogOpen, setIsExclusionDialogOpen] = useState(false);
  const [isSavingExclusion, setIsSavingExclusion] = useState(false);
  const [chartVisibility, setChartVisibility] = useState<ChartVisibility>({
    ema8: true,
    ema21: true,
    weeklyEma8: true,
    ipoVwap: true,
    maStack: true,
    gapZones: true,
    htfBox: true,
    rsLine: true,
    rsSignals: true,
  });
  const requestedStem = searchParams.get("stem") ?? "";
  const requestedTicker = (searchParams.get("ticker") ?? "").toUpperCase();

  useEffect(() => {
    setIsFilesLoading(true);
    void fetchJson<{ watchlists: WatchlistFile[] }>("/api/watchlists")
      .then((payload) => {
        setWatchlists(payload.watchlists);
        setSelectedStem((current) => {
          return (
            payload.watchlists.find((item) => item.stem === current)?.stem ??
            payload.watchlists.find((item) => item.stem === requestedStem)?.stem ??
            payload.watchlists[0]?.stem ??
            ""
          );
        });
      })
      .finally(() => setIsFilesLoading(false));
  }, [requestedStem]);

  useEffect(() => {
    if (!selectedStem) {
      return;
    }
    setIsDetailLoading(true);
    setShowAllTickers(false);
    void fetchJson<WatchlistDetailResponse>(`/api/watchlists/${selectedStem}`)
      .then((payload) => {
        setDetail(payload);
        const nextTicker =
          payload.entries.find((item) => String(item.ticker ?? "").toUpperCase() === requestedTicker) ?? payload.entries[0] ?? null;
        setSelectedTicker(nextTicker);
      })
      .finally(() => setIsDetailLoading(false));
  }, [requestedTicker, selectedStem]);

  useEffect(() => {
    const ticker = typeof selectedTicker?.ticker === "string" ? selectedTicker.ticker : "";
    if (!selectedStem || !ticker) {
      return;
    }
    setChartPayload(null);
    setIsChartLoading(true);
    void fetchJson<WatchlistChartResponse>(`/api/watchlists/${selectedStem}/chart/${ticker}`)
      .then(setChartPayload)
      .finally(() => setIsChartLoading(false));
  }, [selectedStem, selectedTicker?.ticker]);

  useEffect(() => {
    if (!selectedStem) {
      return;
    }
    const nextParams = new URLSearchParams(searchParams);
    nextParams.set("stem", selectedStem);
    const ticker = typeof selectedTicker?.ticker === "string" ? selectedTicker.ticker : "";
    if (ticker) {
      nextParams.set("ticker", ticker);
    } else {
      nextParams.delete("ticker");
    }
    const nextSerialized = nextParams.toString();
    if (nextSerialized !== searchParams.toString()) {
      setSearchParams(nextParams, { replace: true });
    }
  }, [searchParams, selectedStem, selectedTicker, setSearchParams]);

  const ticker = typeof selectedTicker?.ticker === "string" ? selectedTicker.ticker : "--";
  const company = typeof selectedTicker?.company_name === "string" ? selectedTicker.company_name : "";
  const exchange = typeof selectedTicker?.exchange === "string" ? selectedTicker.exchange : "--";
  const industry = typeof selectedTicker?.industry === "string" ? selectedTicker.industry : "Unknown";
  const sector = typeof selectedTicker?.sector === "string" ? selectedTicker.sector : "Unknown";
  const themes = normalizeStringList(selectedTicker?.theme_tags);
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
    { key: "htfBox", label: "HTF box" },
    { key: "rsLine", label: "RS line" },
    { key: "rsSignals", label: "RS markers" },
  ];

  const groupedWatchlists = useMemo(() => {
    const groups = new Map<string, { label: string; items: WatchlistFile[] }>();
    for (const file of watchlists) {
      const group = groups.get(file.group_key) ?? { label: file.group_label, items: [] };
      group.items.push(file);
      groups.set(file.group_key, group);
    }
    return Array.from(groups.entries())
      .map(([groupKey, group]) => ({
        groupKey,
        label: group.label,
        items: group.items.sort((left, right) => right.captured_at.localeCompare(left.captured_at)),
      }))
      .sort((left, right) => left.label.localeCompare(right.label));
  }, [watchlists]);

  const filteredEntries = useMemo(() => {
    const query = tickerSearch.trim().toLowerCase();
    if (!query) {
      return detail?.entries ?? [];
    }
    return (detail?.entries ?? []).filter((entry) => {
      const parts = [
        String(entry.ticker ?? ""),
        String(entry.company_name ?? ""),
        String(entry.sector ?? ""),
        String(entry.industry ?? ""),
        ...normalizeStringList(entry.theme_tags),
      ];
      return parts.join(" ").toLowerCase().includes(query);
    });
  }, [detail?.entries, tickerSearch]);

  const visibleEntries = showAllTickers ? filteredEntries : filteredEntries.slice(0, DEFAULT_VISIBLE_TICKERS);

  const handleSelectStem = (stem: string) => {
    setNotice("");
    setSelectedStem(stem);
  };

  const handleAddExclusion = async (reason: string) => {
    setIsSavingExclusion(true);
    try {
      await fetchJson<{ ok: boolean }>("/api/admin/exclusions", {
        method: "POST",
        body: JSON.stringify({ ticker, reason }),
      });
      setNotice(`${ticker} added to exclusions.`);
      setIsExclusionDialogOpen(false);
    } catch (error) {
      setNotice(error instanceof Error ? error.message : "Failed to add exclusion.");
    } finally {
      setIsSavingExclusion(false);
    }
  };

  return (
    <div className="watchlists-layout">
      <aside className="panel files-pane">
        <div className="panel-head">
          <h2>Watchlists</h2>
          <span className="eyebrow">{watchlists.length} files</span>
        </div>
        {isFilesLoading ? <LoadingBlock label="Loading watchlist files…" compact /> : null}
        <div className="watchlist-group-list">
          {groupedWatchlists.map((group) => (
            <section key={group.groupKey} className="watchlist-group">
              <div className="watchlist-group-head">
                <span className="eyebrow">{group.label}</span>
                <span className="file-meta">{group.items.length}</span>
              </div>
              <div className="file-list">
                {group.items.map((file) => (
                  <button
                    key={file.stem}
                    className={`file-row file-button${selectedStem === file.stem ? " is-selected" : ""}`}
                    onClick={() => handleSelectStem(file.stem)}
                    type="button"
                  >
                    <div className="file-name">{file.name}</div>
                    <div className="file-meta">
                      {formatLocalDateTime(file.captured_at)}
                      {file.sort_date ? ` · ${formatLocalDate(file.sort_date)}` : ""}
                    </div>
                  </button>
                ))}
              </div>
            </section>
          ))}
        </div>

        <div className="panel-head inline-head">
          <h2>Ticker List ({detail?.entry_count ?? 0})</h2>
          <span className="eyebrow">{filteredEntries.length} matched</span>
        </div>
        <div className="ticker-list-toolbar">
          <label className="field">
            <span>Filter</span>
            <input
              type="text"
              value={tickerSearch}
              onChange={(event) => setTickerSearch(event.target.value)}
              placeholder="Ticker, company, sector, industry, theme"
            />
          </label>
        </div>
        {isDetailLoading ? <LoadingBlock label="Loading watchlist detail…" compact /> : null}
        <div className="ticker-list">
          {visibleEntries.map((item, index) => (
            <button
              key={`${item.ticker ?? "ticker"}-${index}`}
              className={`ticker-row${ticker === item.ticker ? " is-selected" : ""}`}
              onClick={() => setSelectedTicker(item)}
              type="button"
            >
              <div>
                <div className="ticker-symbol">{String(item.ticker ?? "--")}</div>
                <div className="ticker-company">{String(item.company_name ?? "")}</div>
                <div className="ticker-chip-row">
                  <div className="ticker-tag">RS Rank: {Number(item.rs_rank ?? item.score ?? 0)}</div>
                  {typeof item.industry === "string" && item.industry ? <div className="ticker-tag muted">{item.industry}</div> : null}
                </div>
              </div>
              <div className="ticker-side">
                <div className="ticker-price">{formatPrice(resolveDisplayPrice(item, null))}</div>
                {renderChange(resolveDisplayChangePct(item, null))}
              </div>
            </button>
          ))}
          {filteredEntries.length > DEFAULT_VISIBLE_TICKERS ? (
            <div className="list-footer">
              <span className="panel-copy">
                Showing {visibleEntries.length} of {filteredEntries.length} tickers.
              </span>
              <button className="ghost-button" type="button" onClick={() => setShowAllTickers((current) => !current)}>
                {showAllTickers ? "Show Less" : `Show All ${filteredEntries.length}`}
              </button>
            </div>
          ) : null}
        </div>
      </aside>

      <div className="watchlists-main">
        <section className="hero-strip">
          <div>
            <div className="hero-symbol-row">
              <h1>{ticker}</h1>
              <span className="ticker-exchange">{exchange}</span>
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
            {notice ? <p className="panel-copy">{notice}</p> : null}
          </div>
          <div className="hero-stats">
            <div>
              <span className="eyebrow">Sector</span>
              <strong>{sector}</strong>
            </div>
            <div>
              <span className="eyebrow">Industry</span>
              <strong>{industry}</strong>
            </div>
            <div>
              <span className="eyebrow">Themes</span>
              <strong>{themes.length ? themes.join(", ") : "None"}</strong>
            </div>
            <div>
              <span className="eyebrow">Score</span>
              <strong>{score}</strong>
            </div>
          </div>
          {auth.hasCapability("manage_exclusions") ? (
            <div className="hero-actions">
              <button className="primary-button" type="button" onClick={() => setIsExclusionDialogOpen(true)} disabled={ticker === "--"}>
                Exclude Ticker
              </button>
            </div>
          ) : null}
        </section>

        <Panel
          title="Candles"
          aside={
            <div className="watchlist-panel-aside">
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
          {isChartLoading ? <LoadingBlock label={`Loading chart for ${ticker}…`} /> : null}
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
          <Panel title="Theme / Group">
            <div className="summary-stat">{themes.length ? themes.join(", ") : industry}</div>
            <p className="panel-copy">Sector {sector}</p>
          </Panel>
        </div>
      </div>

      <ExclusionDialog
        isOpen={isExclusionDialogOpen}
        mode="add"
        ticker={ticker}
        title={`Add ${ticker} to exclusions`}
        confirmLabel="Add Exclusion"
        helperText="This writes to the manual exclusions list so future runs can skip this ticker."
        submitting={isSavingExclusion}
        onClose={() => setIsExclusionDialogOpen(false)}
        onSubmit={handleAddExclusion}
      />
    </div>
  );
}

function normalizeStringList(value: unknown): string[] {
  if (!Array.isArray(value)) {
    return [];
  }
  return value
    .map((item) => String(item ?? "").trim())
    .filter((item, index, array) => item && array.indexOf(item) === index);
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
  const latest = candles[candles.length - 1];
  const previous = candles[candles.length - 2];
  if (!latest || !previous || previous.close === 0) {
    return null;
  }
  return ((latest.close - previous.close) / previous.close) * 100;
}

function formatPrice(value: number | null): string {
  if (value == null) {
    return "--";
  }
  return `$${value.toFixed(2)}`;
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
