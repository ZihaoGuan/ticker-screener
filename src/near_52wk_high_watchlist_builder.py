from __future__ import annotations

from .near_52wk_high_screen import Near52WeekHighHit


def build_near_52wk_high_watchlist(hits: list[Near52WeekHighHit]) -> list[dict[str, object]]:
    watchlist: list[dict[str, object]] = []
    for hit in hits:
        summary = (
            f"{hit.distance_from_52wk_high_pct:.1f}% below 52-week high "
            f"with 20D average dollar volume {hit.avg_dollar_volume_20:,.0f}."
        )
        watchlist.append(
            {
                "ticker": hit.ticker,
                "sector": hit.sector,
                "industry": hit.industry,
                "exchange": hit.exchange,
                "setup_label": "Near 52W High",
                "summary": summary,
                "master_note": ". ".join(hit.reasons),
                "event_date": hit.signal_date,
                "event_label": "Near high pass",
                "trigger_label": "52-week high",
                "trigger_price": round(hit.year_high, 4),
                "entry_style": "near_52wk_high",
                "entry_price": round(hit.current_price, 4),
                "entry_label": "Current close",
                "entry_timeframe": "daily",
                "current_price": round(hit.current_price, 4),
                "year_high": round(hit.year_high, 4),
                "distance_from_52wk_high_pct": round(hit.distance_from_52wk_high_pct, 2),
                "avg_volume_20": round(hit.avg_volume_20, 2),
                "avg_dollar_volume_20": round(hit.avg_dollar_volume_20, 2),
                "signal_badges": ["Near 52W High", "0-20% Below High"],
            }
        )
    return watchlist
