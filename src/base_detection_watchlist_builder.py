from __future__ import annotations

from .base_detection_screen import BaseDetectionHit


def build_base_detection_watchlist(hits: list[BaseDetectionHit]) -> list[dict[str, object]]:
    watchlist: list[dict[str, object]] = []
    for hit in hits:
        summary = (
            f"{hit.base_type} active. "
            f"{hit.base_weeks}w, {hit.base_depth_pct:.1f}% deep. "
            f"Base high {hit.base_high:.2f}, low {hit.base_low:.2f}. "
            f"Detected {hit.signal_date}."
        )
        watchlist.append(
            {
                "ticker": hit.ticker,
                "sector": hit.sector,
                "industry": hit.industry,
                "setup_label": "Base Detection",
                "summary": summary,
                "master_note": ". ".join(hit.reasons),
                "event_date": hit.signal_date,
                "event_label": "Base detected",
                "trigger_label": "Breakout price",
                "trigger_price": round(hit.breakout_price, 4),
                "entry_style": "base_breakout_watch",
                "entry_price": round(hit.breakout_price, 4),
                "entry_label": "Base high",
                "entry_timeframe": "daily",
                "base_type": hit.base_type,
                "base_start_date": hit.base_start_date,
                "base_weeks": hit.base_weeks,
                "base_depth_pct": round(hit.base_depth_pct, 2),
                "base_high": round(hit.base_high, 4),
                "base_low": round(hit.base_low, 4),
                "signal_age_bars": hit.signal_age_bars,
            }
        )
    return watchlist
