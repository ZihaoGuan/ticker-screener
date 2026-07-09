from __future__ import annotations

from .my_picks_sma50_reclaim_screen import MyPicksSma50ReclaimHit


def build_my_picks_sma50_reclaim_watchlist(hits: list[MyPicksSma50ReclaimHit]) -> list[dict[str, object]]:
    watchlist: list[dict[str, object]] = []
    for hit in hits:
        summary = (
            f"Latest bar reclaimed the 50 SMA intraday and closed above it. "
            f"Close {hit.current_price:.2f}, SMA50 {hit.sma50:.2f}, EMA9 {hit.ema9:.2f}, EMA21 {hit.ema21:.2f}."
        )
        watchlist.append(
            {
                "ticker": hit.ticker,
                "setup_label": "My Picks 50 SMA Reclaim",
                "summary": summary,
                "master_note": ". ".join(hit.reasons),
                "event_date": hit.signal_date,
                "event_label": "50 SMA Reclaim",
                "trigger_label": "SMA50 reclaim close",
                "trigger_price": round(hit.current_price, 4),
                "entry_style": "sma50_reclaim",
                "entry_price": round(hit.current_price, 4),
                "entry_label": "Signal close",
                "entry_timeframe": "daily",
                "secondary_entry_price": round(hit.sma50, 4),
                "secondary_entry_label": "SMA50 support",
                "secondary_entry_timeframe": "daily",
                "stop_price": round(min(hit.session_low, hit.sma50), 4),
                "stop_label": "Signal-day low",
                "stop_timeframe": "daily",
                "signal_badges": ["My Picks", "50 SMA Reclaim"],
                "notes": hit.notes,
            }
        )
    return watchlist
