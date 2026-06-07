from __future__ import annotations

from .double_bottom_detection_screen import DoubleBottomDetectionHit


def build_double_bottom_detection_watchlist(hits: list[DoubleBottomDetectionHit]) -> list[dict[str, object]]:
    watchlist: list[dict[str, object]] = []
    for hit in hits:
        summary = (
            f"Double bottom active. "
            f"{hit.pattern_weeks}w, {hit.depth_pct:.1f}% deep. "
            f"Top {hit.top_price:.2f}, middle high {hit.middle_high_price:.2f}, second low {hit.second_bottom_price:.2f}."
        )
        watchlist.append(
            {
                "ticker": hit.ticker,
                "sector": hit.sector,
                "industry": hit.industry,
                "setup_label": "Double Bottom Detection",
                "summary": summary,
                "master_note": ". ".join(hit.reasons),
                "event_date": hit.signal_date,
                "event_label": "Double bottom detected",
                "trigger_label": "Breakout price",
                "trigger_price": round(hit.breakout_price, 4),
                "entry_style": "double_bottom_breakout_watch",
                "entry_price": round(hit.breakout_price, 4),
                "entry_label": "Middle high",
                "entry_timeframe": "daily",
                "pattern_weeks": hit.pattern_weeks,
                "depth_pct": round(hit.depth_pct, 2),
                "top_price": round(hit.top_price, 4),
                "first_bottom_price": round(hit.first_bottom_price, 4),
                "second_bottom_price": round(hit.second_bottom_price, 4),
                "middle_high_price": round(hit.middle_high_price, 4),
                "signal_age_bars": hit.signal_age_bars,
            }
        )
    return watchlist
