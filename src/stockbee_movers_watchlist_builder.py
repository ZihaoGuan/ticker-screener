from __future__ import annotations

from .stockbee_movers_screen import StockbeeMoverHit


def build_stockbee_movers_watchlist(hits: list[StockbeeMoverHit]) -> list[dict[str, object]]:
    return [
        {
            "ticker": hit.ticker,
            "sector": hit.sector,
            "industry": hit.industry,
            "setup_label": hit.profile_label,
            "summary": f"Daily {hit.daily_change_pct:.2f}%; five-session {hit.weekly_change_pct:.2f}%; volume {hit.volume:,}.",
            "master_note": ". ".join(hit.reasons),
            "event_date": hit.signal_date,
            "event_label": hit.profile_label,
            "trigger_label": "Closing price",
            "trigger_price": hit.current_price,
            "entry_style": hit.profile,
            "entry_price": hit.current_price,
            "entry_label": "Signal close",
            "entry_timeframe": "daily",
            "volume": hit.volume,
            "daily_change_pct": hit.daily_change_pct,
            "weekly_change_pct": hit.weekly_change_pct,
        }
        for hit in hits
    ]
