from __future__ import annotations

from .high_tight_flag_screen import HighTightFlagHit


def build_high_tight_flag_watchlist(hits: list[HighTightFlagHit]) -> list[dict[str, object]]:
    watchlist: list[dict[str, object]] = []
    for hit in hits:
        runup_40_pct = (hit.runup_40_ratio - 1.0) * 100.0
        runup_60_pct = (hit.runup_60_ratio - 1.0) * 100.0
        summary = (
            f"Leif Soreide high tight flag scan: 40-bar runup {runup_40_pct:.1f}%, "
            f"60-bar runup {runup_60_pct:.1f}%, ATR/close {hit.atr_ratio:.3f}."
        )
        watchlist.append(
            {
                "ticker": hit.ticker,
                "sector": hit.sector,
                "industry": hit.industry,
                "setup_label": "High Tight Flag",
                "summary": summary,
                "master_note": ". ".join(hit.reasons),
                "event_date": hit.signal_date,
                "event_label": "High Tight Flag",
                "trigger_label": "Current high",
                "trigger_price": round(hit.high_price, 4),
                "entry_style": "high_tight_flag",
                "entry_price": round(hit.current_price, 4),
                "entry_label": "Current close",
                "entry_timeframe": "daily",
                "stop_price": round(hit.low_price, 4),
                "stop_label": "Current low",
                "stop_timeframe": "daily",
                "signal_badges": ["HTF", "Leaders"],
                "atr_ratio": round(hit.atr_ratio, 4),
                "runup_40_pct": round(runup_40_pct, 2),
                "runup_60_pct": round(runup_60_pct, 2),
            }
        )
    return watchlist
