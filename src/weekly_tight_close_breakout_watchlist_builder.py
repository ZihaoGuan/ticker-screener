from __future__ import annotations

from .weekly_tight_close_screen import WeeklyTightCloseHit


def build_weekly_tight_close_breakout_watchlist(hits: list[WeeklyTightCloseHit]) -> list[dict[str, object]]:
    watchlist: list[dict[str, object]] = []
    for hit in hits:
        summary = (
            f"Weekly tight-close breakout. "
            f"Box high {hit.breakout_price:.2f}, box low {hit.lowest_price:.2f}. "
            f"Setup threshold {hit.threshold_pct:.2f}%."
        )
        watchlist.append(
            {
                "ticker": hit.ticker,
                "sector": hit.sector,
                "industry": hit.industry,
                "setup_label": "Weekly Tight Close Breakout",
                "summary": summary,
                "master_note": ". ".join(hit.reasons),
                "event_date": hit.signal_date,
                "event_label": "Weekly tight close breakout",
                "trigger_label": "Box high",
                "trigger_price": round(hit.breakout_price, 4),
                "entry_style": "weekly_tight_close_breakout",
                "entry_price": round(hit.current_price, 4),
                "entry_label": "Current close",
                "entry_timeframe": "weekly",
                "lowest_price": round(hit.lowest_price, 4),
                "threshold_pct": round(hit.threshold_pct, 4),
                "close_spread_pct": round(hit.close_spread_pct, 4),
                "high_spread_pct": round(hit.high_spread_pct, 4),
                "low_spread_pct": round(hit.low_spread_pct, 4),
            }
        )
    return watchlist
