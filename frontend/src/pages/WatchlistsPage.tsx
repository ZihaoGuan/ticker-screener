import { useEffect, useMemo, useState } from "react";
import { Link, useSearchParams } from "react-router-dom";
import { LoadingBlock } from "../components/LoadingBlock";
import { Panel } from "../components/Panel";
import { fetchJson } from "../lib/api";
import { formatLocalDate, formatLocalDateTime } from "../lib/format";
import type { WatchlistDetailResponse, WatchlistFile } from "../lib/types";

const DEFAULT_VISIBLE_TICKERS = 80;

export function WatchlistsPage() {
  const [searchParams, setSearchParams] = useSearchParams();
  const [watchlists, setWatchlists] = useState<WatchlistFile[]>([]);
  const [selectedStem, setSelectedStem] = useState("");
  const [detail, setDetail] = useState<WatchlistDetailResponse | null>(null);
  const [tickerSearch, setTickerSearch] = useState("");
  const [showAllTickers, setShowAllTickers] = useState(false);
  const [isFilesLoading, setIsFilesLoading] = useState(true);
  const [isDetailLoading, setIsDetailLoading] = useState(false);
  const requestedStem = searchParams.get("stem") ?? "";

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
      })
      .finally(() => setIsDetailLoading(false));
  }, [selectedStem]);

  useEffect(() => {
    if (!selectedStem) {
      return;
    }
    const nextParams = new URLSearchParams(searchParams);
    nextParams.set("stem", selectedStem);
    nextParams.delete("ticker");
    const nextSerialized = nextParams.toString();
    if (nextSerialized !== searchParams.toString()) {
      setSearchParams(nextParams, { replace: true });
    }
  }, [searchParams, selectedStem, setSearchParams]);

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
  const selectedWatchlist = useMemo(
    () => watchlists.find((item) => item.stem === selectedStem) ?? null,
    [selectedStem, watchlists],
  );
  const selectedAsOfDate = useMemo(() => resolveWatchlistAsOfDate(selectedWatchlist), [selectedWatchlist]);

  const handleSelectStem = (stem: string) => {
    setSelectedStem(stem);
  };

  return (
    <div className="watchlists-layout">
      <aside className="panel files-pane">
        <div className="panel-head">
          <h2>Watchlists</h2>
          <span className="eyebrow">{watchlists.length} files</span>
        </div>
        <div className="watchlist-pane-actions">
          <Link className="ghost-button" to="/watchlists/weekly">
            Open Weekly Board
          </Link>
        </div>
        {isFilesLoading ? <LoadingBlock label="Loading watchlist files…" compact /> : null}
        <div className="watchlist-group-list">
          {groupedWatchlists.map((group) => (
            <section key={group.groupKey} className="watchlist-group">
              <div className="watchlist-group-head">
                <span className="eyebrow">
                  {group.label}
                  {group.items.some((item) => item.is_deprecated) ? " · Deprecated includes legacy report watchlists" : ""}
                </span>
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
                    <div className="file-name">
                      {file.name}
                      {file.is_deprecated ? " · DEPRECATED" : ""}
                    </div>
                    <div className="file-meta">
                      {formatLocalDateTime(file.captured_at)}
                      {file.sort_date ? ` · ${formatLocalDate(file.sort_date)}` : ""}
                      {file.is_deprecated && file.deprecation_reason ? ` · ${file.deprecation_reason}` : ""}
                    </div>
                  </button>
                ))}
              </div>
            </section>
          ))}
        </div>
      </aside>

      <div className="watchlists-main">
        <Panel
          title={selectedWatchlist?.name || "Ticker Cards"}
          aside={
            <div className="watchlist-panel-aside">
              <span className="eyebrow">{detail?.entry_count ?? 0} tickers</span>
              {selectedAsOfDate ? <span className="eyebrow">As of {selectedAsOfDate}</span> : null}
              <Link className="ghost-button" to="/guide">
                Open Guide
              </Link>
            </div>
          }
        >
          <div className="watchlist-board-copy">
            <p className="panel-copy">
              Read list here. Open full chart on click.
            </p>
            {selectedWatchlist?.is_deprecated ? (
              <p className="panel-copy">Deprecated legacy report watchlist. Prefer scanner boards, dated screener artifacts, or report page.</p>
            ) : null}
            {selectedWatchlist?.captured_at ? (
              <p className="panel-copy">Captured {formatLocalDateTime(selectedWatchlist.captured_at)}</p>
            ) : null}
          </div>
          <div className="ticker-list-toolbar watchlist-board-toolbar">
            <label className="field">
              <span>Filter</span>
              <input
                type="text"
                value={tickerSearch}
                onChange={(event) => setTickerSearch(event.target.value)}
                placeholder="Ticker, company, sector, industry, theme"
              />
            </label>
            <span className="eyebrow">{filteredEntries.length} matched</span>
          </div>
          {isDetailLoading ? <LoadingBlock label="Loading ticker cards…" compact /> : null}
          <div className="watchlist-card-grid">
            {visibleEntries.map((item, index) => (
              <Link
                key={`${item.ticker ?? "ticker"}-board-${index}`}
                className="watchlist-card"
                to={buildChartDetailHref(item, selectedAsOfDate)}
              >
                <div className="ticker-card-head">
                  <div>
                    <div className="ticker-symbol">{String(item.ticker ?? "--")}</div>
                    <div className="ticker-company">{String(item.company_name ?? "")}</div>
                  </div>
                  <div className="ticker-side">
                    <div className="ticker-price">{formatPrice(resolveDisplayPrice(item))}</div>
                    {renderChange(resolveDisplayChangePct(item))}
                  </div>
                </div>
                <div className="ticker-chip-row">
                  {resolveRankValue(item) != null ? <div className="ticker-tag">RS Rank: {resolveRankValue(item)}</div> : null}
                  {typeof item.setup_label === "string" && item.setup_label ? <div className="ticker-tag">{item.setup_label}</div> : null}
                  {normalizeStringList(item.signal_badges).slice(0, 3).map((badge) => (
                    <div key={`${String(item.ticker ?? "--")}-${badge}`} className="ticker-tag">
                      {badge}
                    </div>
                  ))}
                  {typeof item.industry === "string" && item.industry ? <div className="ticker-tag muted">{item.industry}</div> : null}
                </div>
                <div className="ticker-card-meta">
                  <span>{typeof item.sector === "string" && item.sector ? item.sector : "Unknown sector"}</span>
                  <span>{selectedAsOfDate ? `Chart date ${selectedAsOfDate}` : "Open latest chart"}</span>
                </div>
                {typeof item.summary === "string" && item.summary ? <p className="ticker-card-summary">{item.summary}</p> : null}
                <span className="ticker-card-cta">Open chart detail</span>
              </Link>
            ))}
          </div>
          {filteredEntries.length === 0 && !isDetailLoading ? <p className="panel-copy">No tickers match current filter.</p> : null}
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
        </Panel>
      </div>
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

function resolveDisplayPrice(entry: Record<string, unknown> | null | undefined): number | null {
  const candidates = [
    entry?.current_price,
    entry?.last_price,
    entry?.signal_close,
    entry?.close,
    entry?.entry_price,
    entry?.trigger_price,
    entry?.secondary_entry_price,
  ];
  for (const candidate of candidates) {
    const value = toNullableNumber(candidate);
    if (value != null) {
      return value;
    }
  }
  return null;
}

function resolveRankValue(entry: Record<string, unknown> | null | undefined): number | null {
  const rsRank = toNullableNumber(entry?.rs_rank);
  if (rsRank != null) {
    return rsRank;
  }
  const score = toNullableNumber(entry?.score);
  return score != null && score > 0 ? score : null;
}

function resolveDisplayChangePct(entry: Record<string, unknown> | null | undefined): number | null {
  const candidates = [
    entry?.price_change_pct,
    entry?.daily_change_pct,
    entry?.change_pct,
    entry?.pct_change,
  ];
  for (const candidate of candidates) {
    const value = toNullableNumber(candidate);
    if (value != null) {
      return value;
    }
  }
  return null;
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

function buildChartDetailHref(entry: Record<string, unknown>, asOfDate: string | null): string {
  const ticker = String(entry.ticker ?? "").trim().toUpperCase();
  const params = new URLSearchParams();
  params.set("ticker", ticker);
  if (asOfDate) {
    params.set("date", asOfDate);
  }
  return `/charts?${params.toString()}`;
}

function resolveWatchlistAsOfDate(file: WatchlistFile | null): string | null {
  if (!file) {
    return null;
  }
  const sortDate = String(file.sort_date ?? "").trim();
  if (/^\d{4}-\d{2}-\d{2}$/.test(sortDate)) {
    return sortDate;
  }
  const capturedAt = String(file.captured_at ?? "").trim();
  if (/^\d{4}-\d{2}-\d{2}/.test(capturedAt)) {
    return capturedAt.slice(0, 10);
  }
  return null;
}
