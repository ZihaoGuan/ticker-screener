from __future__ import annotations

from .cup_detection_screen import CupDetectionHit


def build_cup_detection_watchlist(hits: list[CupDetectionHit]) -> list[dict[str, object]]:
    watchlist: list[dict[str, object]] = []
    for hit in hits:
        summary = (
            f"Cup pattern active. "
            f"{hit.cup_weeks}w, {hit.cup_depth_pct:.1f}% deep. "
            f"Cup high {hit.cup_high:.2f}, low {hit.cup_low:.2f}. "
            f"Shape {hit.shape_mode}."
        )
        watchlist.append(
            {
                "ticker": hit.ticker,
                "sector": hit.sector,
                "industry": hit.industry,
                "setup_label": "Cup Detection",
                "summary": summary,
                "master_note": ". ".join(hit.reasons),
                "event_date": hit.signal_date,
                "event_label": "Cup detected",
                "trigger_label": "Breakout price",
                "trigger_price": round(hit.breakout_price, 4),
                "entry_style": "cup_breakout_watch",
                "entry_price": round(hit.breakout_price, 4),
                "entry_label": "Cup high",
                "entry_timeframe": "daily",
                "cup_start_date": hit.base_start_date,
                "cup_weeks": hit.cup_weeks,
                "cup_depth_pct": round(hit.cup_depth_pct, 2),
                "cup_high": round(hit.cup_high, 4),
                "cup_low": round(hit.cup_low, 4),
                "cup_midpoint": round(hit.cup_midpoint, 4),
                "shape_mode": hit.shape_mode,
                "signal_age_bars": hit.signal_age_bars,
            }
        )
    return watchlist
