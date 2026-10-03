import { useCallback, useEffect, useMemo, useState } from "react";
import { Link, useSearchParams } from "react-router-dom";
import { useAuth } from "../auth/AuthContext";
import { LoadingBlock } from "../components/LoadingBlock";
import { fetchJson } from "../lib/api";
import { formatCount, formatLocalDate, formatLocalDateTime } from "../lib/format";
import type { ScannerBoardCard, ScannerBoardResponse } from "../lib/types";

const freshnessLabel: Record<string, string> = { current: "Current", stale: "Stale", not_run: "Not run" };

function hitCountPoints(history: ScannerBoardCard["hit_count_history"]) {
  if (history.length < 2) return "";
  const maximum = Math.max(1, ...history.map((item) => item.hit_count));
  return history.map((item, index) => `${(index / (history.length - 1)) * 100},${36 - (item.hit_count / maximum) * 28}`).join(" ");
}

export function ScannerBoardPage() {
  const auth = useAuth();
  const [searchParams, setSearchParams] = useSearchParams();
  const [payload, setPayload] = useState<ScannerBoardResponse | null>(null);
  const [isLoading, setIsLoading] = useState(true);
  const [error, setError] = useState("");
  const [notice, setNotice] = useState("");
  const [isRefreshingBoard, setIsRefreshingBoard] = useState(false);

  const loadBoard = useCallback(async () => {
    setIsLoading(true);
    setError("");
    try {
      setPayload(await fetchJson<ScannerBoardResponse>("/api/scanner-board"));
    } catch (loadError) {
      setError(loadError instanceof Error ? loadError.message : "Failed to load scanner board.");
    } finally {
      setIsLoading(false);
    }
  }, []);

  useEffect(() => {
    void loadBoard();
  }, [loadBoard]);

  const cards = payload?.cards ?? [];
  const query = searchParams.get("q") ?? "";
  const family = searchParams.get("family") ?? "recommended";
  const timeframe = searchParams.get("timeframe") ?? "all";
  const freshness = searchParams.get("freshness") ?? "current";
  const sort = searchParams.get("sort") ?? "recommended";
  const families = useMemo(() => [...new Set(cards.map((card) => card.family))].sort(), [cards]);
  const visibleCards = useMemo(() => {
    const lowerQuery = query.trim().toLowerCase();
    return cards.filter((card) => (family === "recommended" ? card.featured : family === "all" || card.family === family))
      .filter((card) => timeframe === "all" || card.timeframe.toLowerCase().includes(timeframe))
      .filter((card) => freshness === "all" || card.freshness === freshness)
      .filter((card) => !lowerQuery || `${card.label} ${card.description} ${card.preview_tickers.join(" ")}`.toLowerCase().includes(lowerQuery))
      .sort((left, right) => sort === "hits" ? right.entry_count - left.entry_count : sort === "fresh" ? right.sort_date.localeCompare(left.sort_date) : Number(right.featured) - Number(left.featured) || Number(right.has_results) - Number(left.has_results) || right.entry_count - left.entry_count);
  }, [cards, family, freshness, query, sort, timeframe]);
  const availableCards = useMemo(() => cards.filter((card) => card.has_results), [cards]);
  const isAdmin = auth.role === "admin";

  const updateFilter = (key: string, value: string) => {
    const next = new URLSearchParams(searchParams);
    if (!value || (key !== "family" && value === "all") || (key === "family" && value === "recommended") || (key === "freshness" && value === "current") || (key === "sort" && value === "recommended")) next.delete(key);
    else next.set(key, value);
    setSearchParams(next, { replace: true });
  };

  const handleManualRefresh = async () => {
    setIsRefreshingBoard(true);
    setNotice("");
    try {
      const refreshed = await fetchJson<ScannerBoardResponse>("/api/scanner-board/refresh", { method: "POST" });
      setPayload(refreshed);
      setNotice("Scanner board manually refreshed to latest visible New York trading day.");
    } catch (error) {
      setError(error instanceof Error ? error.message : "Failed to manually refresh scanner board.");
    } finally {
      setIsRefreshingBoard(false);
    }
  };

  return (
    <div className="page-grid scanner-board">
      <section className="earnings-board-hero scanner-board-hero">
        <div className="earnings-board-hero-copy">
          <span className="earnings-board-kicker">Scanner discovery</span>
          <h1>Find the strongest current setups</h1>
          <p className="panel-copy">
            Start focused, then widen by signal family. Each trend below reflects actual hit counts from persisted scanner runs.
          </p>
        </div>
        <div className="earnings-board-metrics scanner-board-metrics">
          <div className="earnings-metric">
            <span className="eyebrow">Target Trading Day</span>
            <strong>{formatLocalDate(payload?.target_trading_date)}</strong>
          </div>
          <div className="earnings-metric">
            <span className="eyebrow">Latest Update</span>
            <strong>{formatLocalDateTime(payload?.latest_update_at)}</strong>
          </div>
          <div className="earnings-metric">
            <span className="eyebrow">With Matches</span>
            <strong>{formatCount(availableCards.length)}</strong>
          </div>
        </div>
      </section>

      <section className="panel scanner-board-console">
        <div className="scanner-board-toolbar">
          <label className="scanner-search"><span>Search scanners</span><input type="search" value={query} onChange={(event) => updateFilter("q", event.target.value)} placeholder="Name, setup, or ticker" /></label>
          <label><span>Collection</span><select value={family} onChange={(event) => updateFilter("family", event.target.value)}><option value="recommended">Recommended</option><option value="all">All scanners</option>{families.map((item) => <option key={item} value={item}>{item}</option>)}</select></label>
          <label><span>Timeframe</span><select value={timeframe} onChange={(event) => updateFilter("timeframe", event.target.value)}><option value="all">All</option><option value="daily">Daily</option><option value="weekly">Weekly</option></select></label>
          <label><span>Freshness</span><select value={freshness} onChange={(event) => updateFilter("freshness", event.target.value)}><option value="current">Current</option><option value="all">Any state</option><option value="stale">Stale</option><option value="not_run">Not run</option></select></label>
          <label><span>Sort</span><select value={sort} onChange={(event) => updateFilter("sort", event.target.value)}><option value="recommended">Recommended</option><option value="hits">Most hits</option><option value="fresh">Most recent</option></select></label>
        </div>
        <div className="scanner-board-actions">
          <Link className="primary-button" to="/scanner/top-hits">
            Open Top Hits
          </Link>
          <Link className="ghost-button" to="/ratings">
            Open Fundamental Ratings
          </Link>
          <Link className="ghost-button" to="/ratings?mode=technical">
            Open Technical Top 100
          </Link>
          {isAdmin ? (
            <button className="ghost-button" type="button" onClick={() => void handleManualRefresh()} disabled={isRefreshingBoard}>
              {isRefreshingBoard ? "Refreshing…" : "Refresh board"}
            </button>
          ) : null}
        </div>
        <span className="scanner-result-count" aria-live="polite">{formatCount(visibleCards.length)} of {formatCount(cards.length)} scanners</span>
        {payload?.manual_override_active ? (
          <p className="panel-copy earnings-console-note">
            Admin override active for {formatLocalDate(payload.manual_override_target_date)}.
            {payload.manual_override_requested_at ? ` Requested ${formatLocalDateTime(payload.manual_override_requested_at)}.` : ""}
          </p>
        ) : null}
        {notice ? <p className="panel-copy earnings-console-note" role="status">{notice}</p> : null}
      </section>

      <section className="scanner-board-grid">
        {isLoading ? <LoadingBlock label="Loading scanner board…" /> : null}
        {!isLoading && error ? <div className="scanner-state"><h2>Scanner board is unavailable</h2><p>{error}</p><button className="primary-button" type="button" onClick={() => void loadBoard()}>Try again</button></div> : null}
        {!isLoading && !error && visibleCards.length === 0 ? <div className="scanner-state"><h2>No scanners match these filters</h2><p>Try another collection, include stale runs, or clear the search.</p><button className="ghost-button" type="button" onClick={() => setSearchParams({})}>Clear filters</button></div> : null}
        {!isLoading && !error && visibleCards.map((card) => {
          const content = (
            <>
              <div className="scanner-card-topline">
                <span className={`scanner-card-chip accent-${card.accent}`}>{card.timeframe}</span>
                <span className={`scanner-freshness is-${card.freshness}`}>{freshnessLabel[card.freshness] ?? card.freshness}</span>
              </div>
              <div className="scanner-card-body">
                <div className="scanner-card-heading"><h2>{card.label}</h2><strong>{formatCount(card.entry_count)} hits</strong></div>
                <p className="scanner-card-description">{card.description}</p>
                {hitCountPoints(card.hit_count_history) ? (
                  <div className="scanner-card-trend" aria-label={`Hit count over ${card.hit_count_history.length} persisted runs`}>
                    <svg viewBox="0 0 100 40" aria-hidden="true" preserveAspectRatio="none"><path className="scanner-trend-baseline" d="M0 36 H100" /><polyline className={`scanner-trend-line accent-${card.accent}`} points={hitCountPoints(card.hit_count_history)} /></svg>
                    <span>Last {card.hit_count_history.length} runs</span>
                  </div>
                ) : <span className="scanner-trend-empty">No run history yet</span>}
                <div className="scanner-card-preview">
                  {card.preview_tickers.length > 0 ? (
                    card.preview_tickers.map((ticker) => (
                      <span key={`${card.id}-${ticker}`} className="scanner-card-pill">
                        {ticker}
                      </span>
                    ))
                  ) : (
                    <span className="scanner-card-pill muted">No names yet</span>
                  )}
                </div>
              </div>
              <div className="scanner-card-footer">
                <div>
                  <span className="eyebrow">Signal</span>
                  <strong>{card.sort_date ? formatLocalDate(card.sort_date) : "No saved run"}</strong>
                </div>
                <div>
                  <span className="eyebrow">Family</span>
                  <strong>{card.family}</strong>
                </div>
                <span className="scanner-card-cta">{card.available ? "Open list" : card.has_run ? "No matches" : "Unavailable"}</span>
              </div>
            </>
          );

          if (!card.available || !card.stem) {
            return (
              <article key={card.id} className={`scanner-idea-card is-disabled accent-${card.accent}`}>
                {content}
              </article>
            );
          }

          return (
            <Link key={card.id} className={`scanner-idea-card accent-${card.accent}`} to={`/scanner/${encodeURIComponent(card.id)}`}>
              {content}
            </Link>
          );
        })}
      </section>
    </div>
  );
}
