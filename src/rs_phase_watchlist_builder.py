from __future__ import annotations

from .rs_phase_screen import RsPhaseHit


def build_rs_phase_watchlist(hits: list[RsPhaseHit]) -> list[dict[str, object]]:
    watchlist: list[dict[str, object]] = []
    for hit in hits:
        badges = [f"RS Phase {hit.rs_phase_active_days}D"]
        if hit.rs_phase_new_reclaim:
            badges.append("RS Reclaim")
        if hit.daily_rs_new_high_before_price:
            badges.append("RS NH Before Price")
        elif hit.daily_rs_new_high:
            badges.append("RS NH")
        summary = (
            f"Signal on {hit.signal_date}. RS line has stayed above its 21 EMA for "
            f"{hit.rs_phase_active_days} session(s). RS rating {hit.rs_rating:.1f}. "
            f"RS new high before price: {hit.daily_rs_new_high_before_price}."
        )
        watchlist.append(
            {
                "ticker": hit.ticker,
                "sector": hit.sector,
                "industry": hit.industry,
                "setup_label": "RS Phase",
                "summary": summary,
                "master_note": ". ".join(hit.reasons),
                "event_date": hit.signal_date,
                "event_label": "RS Phase",
                "trigger_label": "Signal close",
                "trigger_price": round(hit.current_price, 4),
                "entry_style": "rs_phase",
                "entry_price": round(hit.current_price, 4),
                "entry_label": "Signal close",
                "entry_timeframe": "daily",
                "signal_rs_rating": round(hit.rs_rating, 2),
                "signal_rs_score": round(hit.rs_score, 4),
                "rs_phase_active_days": hit.rs_phase_active_days,
                "signal_badges": badges,
            }
        )
    return watchlist
