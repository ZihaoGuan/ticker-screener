from __future__ import annotations

from .ma_pullback_retest_screen import MaPullbackRetestHit


def build_ma_pullback_retest_watchlist(hits: list[MaPullbackRetestHit]) -> list[dict[str, object]]:
    return [{
        "ticker": hit.ticker, "sector": hit.sector, "industry": hit.industry, "exchange": hit.exchange,
        "setup_label": f"MA Pullback · {hit.signal_state.title()}", "summary": f"{', '.join(hit.matched_profiles)} retest; nearest support {hit.support_price:.2f} ({hit.distance_atr:+.2f} ATR).",
        "master_note": ". ".join(hit.reasons), "event_date": hit.signal_date, "event_label": f"MA pullback {hit.signal_state}",
        "trigger_label": "Support reclaim", "trigger_price": round(hit.support_price, 4), "entry_style": "ma_pullback_retest",
        "entry_price": round(hit.current_price, 4), "entry_label": "Current close", "entry_timeframe": "daily",
        "stop_price": round(hit.stop_price, 4), "stop_label": "Retest low / 1 ATR", "stop_timeframe": "daily",
        "current_price": round(hit.current_price, 4), "matched_profiles": hit.matched_profiles,
        "active_profiles": hit.active_profiles, "ready_profiles": hit.ready_profiles, "signal_state": hit.signal_state,
        "signal_badges": [hit.signal_state.title(), *hit.matched_profiles],
    } for hit in hits]
