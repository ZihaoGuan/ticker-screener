import { useEffect, useMemo, useState } from "react";
import { Link, useSearchParams } from "react-router-dom";
import { LoadingBlock } from "../components/LoadingBlock";
import { fetchJson } from "../lib/api";
import { formatLocalDate, formatLocalDateTime } from "../lib/format";
import type { WeeklyWatchlistEntry, WeeklyWatchlistResponse } from "../lib/types";

function formatWeekLabel(value: string | null | undefined) {
  if (!value) {
    return "Weekly Watchlist";
  }
  const parsed = new Date(`${value}T12:00:00`);
  if (Number.isNaN(parsed.getTime())) {
    return `${value} Weekly Watchlist`;
  }
  return `${new Intl.DateTimeFormat("en-US", { month: "long", day: "numeric", year: "numeric" }).format(parsed)} Weekly Watchlist`;
}

function normalizeStringList(value: unknown): string[] {
  if (!Array.isArray(value)) {
    return [];
  }
  return value
    .map((item) => String(item ?? "").trim())
    .filter((item, index, array) => item.length > 0 && array.indexOf(item) === index);
}

function readNumber(value: unknown): number | null {
  if (value === null || value === undefined || value === "") {
    return null;
  }
  const numeric = Number(value);
  return Number.isFinite(numeric) ? numeric : null;
}

function formatPrice(value: number | null) {
  if (value == null) {
    return "--";
  }
  return `$${value.toFixed(2)}`;
}

function scoreForEntry(entry: WeeklyWatchlistEntry) {
  const rsRank = readNumber(entry.rs_rank);
  if (rsRank != null) {
    return rsRank;
  }
  return readNumber(entry.score);
}

function filterEntries(entries: WeeklyWatchlistEntry[], query: string) {
  const normalizedQuery = query.trim().toLowerCase();
  if (!normalizedQuery) {
    return entries;
  }
  return entries.filter((entry) => {
    const haystack = [
      entry.ticker,
      String(entry.sector ?? ""),
      String(entry.industry ?? ""),
      String(entry.summary ?? ""),
      String(entry.master_note ?? ""),
      ...normalizeStringList(entry.theme_tags),
      ...normalizeStringList(entry.signal_badges),
    ]
      .join(" ")
      .toLowerCase();
    return haystack.includes(normalizedQuery);
  });
}

export function WeeklyWatchlistPage() {
  const [searchParams, setSearchParams] = useSearchParams();
  const requestedStem = searchParams.get("stem") ?? "";
  const [payload, setPayload] = useState<WeeklyWatchlistResponse | null>(null);
  const [isLoading, setIsLoading] = useState(true);
  const [notice, setNotice] = useState("");
  const [query, setQuery] = useState("");

  useEffect(() => {
    setIsLoading(true);
    setNotice("");
    const path = requestedStem ? `/api/watchlists/weekly?stem=${encodeURIComponent(requestedStem)}` : "/api/watchlists/weekly";
    void fetchJson<WeeklyWatchlistResponse>(path)
      .then(setPayload)
      .catch((error) => {
        setPayload(null);
        setNotice(error instanceof Error ? error.message : "Failed to load weekly watchlist.");
      })
      .finally(() => setIsLoading(false));
  }, [requestedStem]);

  const entries = useMemo(() => filterEntries(payload?.entries ?? [], query), [payload?.entries, query]);
  const totalBadges = useMemo(
    () =>
      entries.reduce((total, entry) => total + normalizeStringList(entry.signal_badges).length, 0),
    [entries],
  );
  const highestScore = useMemo(() => {
    let value: number | null = null;
    for (const entry of entries) {
      const next = scoreForEntry(entry);
      if (next != null && (value == null || next > value)) {
        value = next;
      }
    }
    return value;
  }, [entries]);
  const weekLabel = formatWeekLabel(payload?.sort_date);

  return (
    <div className="page-grid earnings-board weekly-watchlist-board">
      <section className="earnings-board-hero">
        <div className="earnings-board-hero-copy">
          <span className="earnings-board-kicker">Weekly RS Board</span>
          <h1>{weekLabel}</h1>
          <p className="panel-copy">Card view for latest persisted weekly RS new-high watchlist. Open charts fast, skim thesis fast, keep raw artifact one click away.</p>
        </div>
        <div className="earnings-board-metrics">
          <div className="earnings-metric">
            <span className="eyebrow">Watchlist File</span>
            <strong>{payload?.source_stem || "--"}</strong>
          </div>
          <div className="earnings-metric">
            <span className="eyebrow">Ticker Count</span>
            <strong>{String(entries.length).padStart(2, "0")}</strong>
          </div>
          <div className="earnings-metric">
            <span className="eyebrow">Signal Badges</span>
            <strong>{String(totalBadges).padStart(2, "0")}</strong>
          </div>
          <div className="earnings-metric">
            <span className="eyebrow">Top Score</span>
            <strong>{highestScore != null ? highestScore : "--"}</strong>
          </div>
          <div className="earnings-metric earnings-metric-highlight">
            <span className="eyebrow">Captured</span>
            <strong>{payload?.captured_at ? formatLocalDateTime(payload.captured_at) : "--"}</strong>
          </div>
        </div>
      </section>

      <section className="panel earnings-filter-console">
        <div className="earnings-filter-console-row weekly-watchlist-console-row">
          <label className="field">
            <span>Weekly File</span>
            <select
              value={payload?.source_stem ?? requestedStem}
              onChange={(event) => {
                const nextParams = new URLSearchParams(searchParams);
                if (event.target.value) {
                  nextParams.set("stem", event.target.value);
                } else {
                  nextParams.delete("stem");
                }
                setSearchParams(nextParams, { replace: true });
              }}
            >
              {(payload?.available_files ?? []).map((file) => (
                <option key={file.stem} value={file.stem}>
                  {file.stem}
                </option>
              ))}
            </select>
          </label>
          <label className="field">
            <span>Filter</span>
            <input value={query} onChange={(event) => setQuery(event.target.value)} placeholder="Ticker, sector, setup, note, badge" />
          </label>
          <div className="weekly-watchlist-actions">
            <Link className="ghost-button" to={payload?.source_stem ? `/watchlists?stem=${encodeURIComponent(payload.source_stem)}` : "/watchlists"}>
              Open Raw Watchlist
            </Link>
          </div>
        </div>
        <p className="panel-copy earnings-console-note">
          {payload?.sort_date ? `Signal date ${formatLocalDate(payload.sort_date)}.` : "Using latest available weekly RS artifact."} Cards route to chart explorer for deeper review.
        </p>
        {notice ? <p className="panel-copy earnings-console-note">{notice}</p> : null}
      </section>

      <section className="panel earnings-calendar-panel">
        <div className="panel-head earnings-calendar-head">
          <div>
            <h2>Weekly RS Cards</h2>
            <span className="eyebrow">{entries.length} names</span>
          </div>
        </div>
        {isLoading ? <LoadingBlock label="Loading weekly watchlist…" /> : null}
        {!isLoading && entries.length === 0 ? <p className="panel-copy">No weekly RS watchlist entries found for this artifact.</p> : null}
        {entries.length > 0 ? (
          <div className="earnings-command-grid weekly-watchlist-grid">
            {entries.map((entry) => {
              const signalBadges = normalizeStringList(entry.signal_badges);
              const themeTags = normalizeStringList(entry.theme_tags);
              const triggerPrice = readNumber(entry.trigger_price);
              const score = scoreForEntry(entry);
              return (
                <article key={entry.ticker} className="earnings-entry-card weekly-watchlist-card">
                  <div className="earnings-entry-head">
                    <Link className="earnings-entry-link" to={`/charts?ticker=${encodeURIComponent(entry.ticker)}`}>
                      {entry.ticker}
                    </Link>
                    <span className="earnings-exchange-badge">{entry.exchange ?? "-"}</span>
                  </div>
                  <p className="earnings-entry-meta">
                    {[entry.sector, entry.industry].filter(Boolean).join(" / ") || "No sector or industry"}
                  </p>
                  {entry.setup_label ? <p className="weekly-watchlist-setup">{entry.setup_label}</p> : null}
                  {entry.summary ? <p className="earnings-entry-summary">"{entry.summary}"</p> : null}
                  {entry.master_note ? <p className="weekly-watchlist-note">{entry.master_note}</p> : null}
                  {signalBadges.length > 0 ? (
                    <div className="earnings-criteria-pill-row">
                      {signalBadges.map((badge) => (
                        <span key={`${entry.ticker}-${badge}`} className="earnings-criteria-pill is-match">
                          {badge}
                        </span>
                      ))}
                    </div>
                  ) : null}
                  {themeTags.length > 0 ? (
                    <div className="earnings-criteria-pill-row">
                      {themeTags.map((tag) => (
                        <span key={`${entry.ticker}-theme-${tag}`} className="earnings-criteria-pill weekly-watchlist-theme-pill">
                          {tag}
                        </span>
                      ))}
                    </div>
                  ) : null}
                  <div className="weekly-watchlist-footer">
                    <span className="weekly-watchlist-metric">Score {score != null ? score : "--"}</span>
                    <span className="weekly-watchlist-metric">Trigger {formatPrice(triggerPrice)}</span>
                  </div>
                </article>
              );
            })}
          </div>
        ) : null}
      </section>
    </div>
  );
}
