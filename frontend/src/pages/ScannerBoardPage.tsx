import { useEffect, useMemo, useState } from "react";
import { Link } from "react-router-dom";
import { LoadingBlock } from "../components/LoadingBlock";
import { fetchJson } from "../lib/api";
import { formatCount, formatLocalDate, formatLocalDateTime } from "../lib/format";
import type { ScannerBoardCard, ScannerBoardResponse } from "../lib/types";

const CARD_ART: Record<string, string> = {
  weekly_rs: "M24 110 L78 82 L118 92 L166 58 L220 72 L276 36",
  rs: "M24 118 L72 102 L118 84 L158 88 L204 54 L248 58 L276 44",
  sean_gap_up: "M24 122 L96 64 L142 88 L188 42 L188 84 L276 84",
  gap_fill: "M24 116 L88 114 L142 112 L182 76 L228 76 L276 48",
  inside_dryup_v2: "M24 74 L92 74 L140 74 L188 76 L234 78 L276 80",
  wyckoff_buy_signal: "M24 118 L72 120 L118 108 L152 126 L190 94 L228 84 L276 52",
  wyckoff_sell_signal: "M24 48 L76 44 L118 58 L162 54 L208 82 L246 104 L276 118",
  ftd_sweep: "M24 118 L74 124 L126 92 L168 104 L214 68 L248 72 L276 42",
  sepa_vcp: "M24 120 L82 118 L126 116 L168 114 L210 98 L246 84 L276 52",
  cup_detection: "M24 112 L72 110 L118 84 L162 58 L206 64 L246 82 L276 70",
  eight_week_100_runup: "M24 124 L74 122 L118 108 L156 84 L198 58 L238 44 L276 26",
  three_weeks_tight: "M24 96 L86 94 L136 92 L186 90 L232 88 L276 84",
  double_bottom_detection: "M24 74 L76 108 L128 60 L176 106 L228 68 L276 46",
  ema21_pullback_buy: "M24 102 L74 72 L122 50 L164 74 L206 96 L248 70 L276 40",
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
        </div>
      </section>

      <section className="panel scanner-board-console">
        <div className="scanner-board-console-row">
          <div>
            <span className="eyebrow">Board logic</span>
            <p className="panel-copy">Weekly RS New High, RS New High Before Price, Sean Gap Up, Gap Fill, Inside Day + Extreme Dry-Up, Wyckoff Buy, Wyckoff Sell, FTD Successful Sweep, SEPA VCP, Cup Detection, 8 Week Run Up (Doubler), Three Weeks Tight, Range Tightness Index, Double Bottom, EMA21 Pullback Buy, Trend Template, Fearzone, and TD9 Bullish.</p>
          </div>
          <div>
            <span className="eyebrow">Navigation</span>
            <p className="panel-copy">Open scanner cards for raw artifacts, or jump to the top-ratings leaderboard for strongest rated names.</p>
          </div>
        </div>
        <div className="weekly-watchlist-actions">
          <Link className="primary-button" to="/scanner/top-hits">
            Open Top Hits
          </Link>
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
