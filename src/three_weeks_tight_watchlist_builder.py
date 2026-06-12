from __future__ import annotations

from .three_weeks_tight_screen import ThreeWeeksTightHit


def build_three_weeks_tight_watchlist(hits: list[ThreeWeeksTightHit]) -> list[dict[str, object]]:
    watchlist: list[dict[str, object]] = []
    for hit in hits:
        summary = (
            f"3 weeks tight active. "
            f"Threshold {hit.threshold_pct:.2f}%. "
            f"Range high {hit.range_high:.2f}, buy {hit.buy_price:.2f}."
        )
        watchlist.append(
            {
                "ticker": hit.ticker,
                "sector": hit.sector,
                "industry": hit.industry,
                "setup_label": "3 Weeks Tight",
                "summary": summary,
                "master_note": ". ".join(hit.reasons),
                "event_date": hit.signal_date,
                "event_label": "3 weeks tight",
                "trigger_label": "Range high + $0.10",
                "trigger_price": round(hit.buy_price, 4),
                "entry_style": "three_weeks_tight_breakout_watch",
                "entry_price": round(hit.buy_price, 4),
                "entry_label": "Buy trigger",
                "entry_timeframe": "weekly",
                "range_high": round(hit.range_high, 4),
                "threshold_pct": round(hit.threshold_pct, 4),
                "close_change_1_pct": round(hit.close_change_1_pct, 4),
                "close_change_2_pct": round(hit.close_change_2_pct, 4),
            }
        )
    return watchlist
