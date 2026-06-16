import { useEffect, useMemo, useState } from "react";
import { Link } from "react-router-dom";
import { LoadingBlock } from "../components/LoadingBlock";
import { fetchJson } from "../lib/api";
import { formatCount, formatLocalDate, formatLocalDateTime } from "../lib/format";
import type { ScannerBoardCard, ScannerBoardResponse } from "../lib/types";

const CARD_ART: Record<string, string> = {
  weekly_rs: "M24 110 L78 82 L118 92 L166 58 L220 72 L276 36",
  sean_gap_up: "M24 122 L96 64 L142 88 L188 42 L188 84 L276 84",
  gap_fill: "M24 116 L88 114 L142 112 L182 76 L228 76 L276 48",
  sepa_vcp: "M24 120 L82 118 L126 116 L168 114 L210 98 L246 84 L276 52",
  trend_template: "M24 112 L76 100 L120 82 L164 70 L212 52 L276 34",
  fearzone: "M24 60 C72 20, 116 20, 156 62 S236 116, 276 92",
  td9_bullish: "M24 98 L82 122 L126 112 L178 78 L222 88 L276 50",
};

export function ScannerBoardPage() {
  const [payload, setPayload] = useState<ScannerBoardResponse | null>(null);
  const [isLoading, setIsLoading] = useState(true);
  const [notice, setNotice] = useState("");

  useEffect(() => {
    setIsLoading(true);
    setNotice("");
    void fetchJson<ScannerBoardResponse>("/api/scanner-board")
      .then(setPayload)
      .catch((error) => {
        setPayload(null);
        setNotice(error instanceof Error ? error.message : "Failed to load scanner board.");
      })
      .finally(() => setIsLoading(false));
  }, []);

  const cards = payload?.cards ?? [];
  const totalHits = useMemo(() => cards.reduce((sum, card) => sum + (card.entry_count || 0), 0), [cards]);
  const availableCards = useMemo(() => cards.filter((card) => card.available), [cards]);

  return (
    <div className="page-grid scanner-board">
      <section className="earnings-board-hero scanner-board-hero">
        <div className="earnings-board-hero-copy">
          <span className="earnings-board-kicker">Latest Scanner Board</span>
          <h1>Latest market-ready scanner hits</h1>
          <p className="panel-copy">
            End-user board for latest persisted weekday scan results. Target refresh is {payload?.cutoff_time_label ?? "18:00 America/New_York"}.
            If a newer run is not ready yet, board stays on prior trading day.
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
            <span className="eyebrow">Scanners Live</span>
            <strong>{formatCount(availableCards.length)}</strong>
          </div>
          <div className="earnings-metric earnings-metric-highlight">
            <span className="eyebrow">Total Hits</span>
            <strong>{formatCount(totalHits)}</strong>
          </div>
        </div>
      </section>

      <section className="panel scanner-board-console">
        <div className="scanner-board-console-row">
          <div>
            <span className="eyebrow">Board logic</span>
            <p className="panel-copy">Weekly RS New High, Sean Gap Up, Gap Fill, SEPA VCP, Trend Template, Fearzone, and TD9 Bullish.</p>
          </div>
          <div>
            <span className="eyebrow">Navigation</span>
            <p className="panel-copy">Open scanner cards for raw artifacts, or jump to the top-ratings leaderboard for strongest rated names.</p>
          </div>
        </div>
        <div className="weekly-watchlist-actions">
          <Link className="ghost-button" to="/ratings">
            Open Fundamental Ratings
          </Link>
          <Link className="ghost-button" to="/ratings?mode=technical">
            Open Technical Top 100
          </Link>
        </div>
        <p className="panel-copy earnings-console-note">
          Latest signal date on screen: {formatLocalDate(payload?.latest_signal_date)}.
          {payload?.reference_now_new_york ? ` Board generated ${formatLocalDateTime(payload.reference_now_new_york)} New York time.` : ""}
        </p>
        {notice ? <p className="panel-copy earnings-console-note">{notice}</p> : null}
      </section>

      <section className="scanner-board-grid">
        {isLoading ? <LoadingBlock label="Loading scanner board…" /> : null}
        {!isLoading && cards.length === 0 ? <p className="panel-copy">No scanner cards available.</p> : null}
        {cards.map((card) => {
          const content = (
            <>
              <div className="scanner-card-topline">
                <span className={`scanner-card-chip accent-${card.accent}`}>{card.timeframe}</span>
                <span className="scanner-card-count">{formatCount(card.entry_count)} results</span>
              </div>
              <div className="scanner-card-art" aria-hidden="true">
                <svg viewBox="0 0 300 150" role="presentation">
                  <path className="scanner-card-gridline" d="M24 96 H276" />
                  <path className={`scanner-card-trace accent-${card.accent}`} d={CARD_ART[card.id] ?? CARD_ART.weekly_rs} />
                </svg>
              </div>
              <div className="scanner-card-body">
                <h2>{card.label}</h2>
                <p className="scanner-card-description">{card.description}</p>
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
                  <span className="eyebrow">Signal Date</span>
                  <strong>{formatLocalDate(card.sort_date)}</strong>
                </div>
                <div>
                  <span className="eyebrow">Captured</span>
                  <strong>{formatLocalDateTime(card.captured_at)}</strong>
                </div>
                <span className="scanner-card-cta">{card.available ? "Open ticker list" : "Unavailable"}</span>
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
