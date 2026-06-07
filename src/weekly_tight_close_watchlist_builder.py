from __future__ import annotations

from .weekly_tight_close_screen import WeeklyTightCloseHit


def build_weekly_tight_close_watchlist(hits: list[WeeklyTightCloseHit]) -> list[dict[str, object]]:
    watchlist: list[dict[str, object]] = []
    for hit in hits:
        summary = (
            f"3 weekly tight closes active. "
            f"ATR threshold {hit.threshold_pct:.2f}%. "
            f"Breakout {hit.breakout_price:.2f}, box low {hit.lowest_price:.2f}."
        )
        watchlist.append(
            {
                "ticker": hit.ticker,
                "sector": hit.sector,
                "industry": hit.industry,
                "setup_label": "Weekly Tight Closes",
                "summary": summary,
                "master_note": ". ".join(hit.reasons),
                "event_date": hit.signal_date,
                "event_label": "Weekly tight closes",
                "trigger_label": "3-week high",
                "trigger_price": round(hit.breakout_price, 4),
                "entry_style": "weekly_tight_close_breakout_watch",
                "entry_price": round(hit.breakout_price, 4),
                "entry_label": "Tight range high",
                "entry_timeframe": "weekly",
                "lowest_price": round(hit.lowest_price, 4),
                "threshold_pct": round(hit.threshold_pct, 4),
                "close_spread_pct": round(hit.close_spread_pct, 4),
                "high_spread_pct": round(hit.high_spread_pct, 4),
                "low_spread_pct": round(hit.low_spread_pct, 4),
            }
        )
    return watchlist
