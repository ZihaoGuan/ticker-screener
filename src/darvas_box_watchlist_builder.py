from __future__ import annotations

from .darvas_box_screen import DarvasBoxBreakoutHit


def build_darvas_box_breakout_watchlist(hits: list[DarvasBoxBreakoutHit]) -> list[dict[str, object]]:
    return [
        {
            "ticker": hit.ticker,
            "sector": hit.sector,
            "industry": hit.industry,
            "exchange": hit.exchange,
            "setup_label": "Darvas Box Breakout",
            "summary": f"{hit.box_days}-session box cleared on {hit.volume_ratio_50:.1f}x 50-day volume near the 52-week high.",
            "master_note": ". ".join(hit.reasons),
            "event_date": hit.signal_date,
            "event_label": "Darvas box breakout",
            "trigger_label": "Box resistance",
            "trigger_price": round(hit.breakout_price, 4),
            "entry_style": "darvas_box_breakout",
            "entry_price": round(hit.current_price, 4),
            "entry_label": "Breakout close",
            "entry_timeframe": "daily",
            "stop_price": round(hit.box_low, 4),
            "stop_label": "Box low reference",
            "stop_timeframe": "daily",
            "current_price": round(hit.current_price, 4),
            "box_high": round(hit.box_high, 4),
            "box_low": round(hit.box_low, 4),
            "box_width_pct": round(hit.box_width_pct, 2),
            "resistance_touches": hit.resistance_touches,
            "distance_from_52w_high_pct": round(hit.distance_from_52w_high_pct, 2),
            "avg_volume_50": round(hit.avg_volume_50, 2),
            "volume_ratio_50": round(hit.volume_ratio_50, 2),
            "signal_badges": ["Darvas Box", "Breakout Close", "Volume >= 1.5x", "Near 52W High"],
        }
        for hit in hits
    ]
