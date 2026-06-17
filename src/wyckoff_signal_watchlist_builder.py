from __future__ import annotations

from .wyckoff_analysis import WyckoffSignalHit


def _format_master_note(hit: WyckoffSignalHit) -> str:
    flags = f" Events: {', '.join(hit.event_flags)}." if hit.event_flags else ""
    return (
        f"{' | '.join(hit.reasons)}. "
        f"Price position {hit.price_position:.2f}. "
        f"Accum {hit.accum_score} / Dist {hit.dist_score}. "
        f"Volume {hit.volume_state}.{flags}"
    )


def build_wyckoff_signal_watchlist(hits: list[WyckoffSignalHit]) -> list[dict[str, object]]:
    watchlist: list[dict[str, object]] = []
    for hit in hits:
        is_buy = hit.signal_type == "buy"
        setup_label = "Wyckoff Buy Signal" if is_buy else "Wyckoff Sell Signal"
        trigger_label = "Wyckoff BUY" if is_buy else "Wyckoff SELL"
        summary = (
            f"{setup_label} in {hit.phase.lower()} state. "
            f"{hit.sub_phase}. "
            f"Volume {hit.volume_state.lower().replace('_', ' ')} with "
            f"score balance A{hit.accum_score}/D{hit.dist_score}."
        )
        watchlist.append(
            {
                "ticker": hit.ticker,
                "sector": hit.sector,
                "industry": hit.industry,
                "setup_label": setup_label,
                "summary": summary,
                "master_note": _format_master_note(hit),
                "event_date": hit.signal_date,
                "event_label": trigger_label,
                "trigger_label": trigger_label,
                "entry_style": "wyckoff_signal",
                "entry_price": round(hit.current_price, 4),
                "entry_label": "Current close",
                "entry_timeframe": "daily",
                "price_position": round(hit.price_position, 4),
                "phase": hit.phase,
                "sub_phase": hit.sub_phase,
                "accum_score": hit.accum_score,
                "dist_score": hit.dist_score,
                "volume_state": hit.volume_state,
                "event_flags": hit.event_flags,
                "signal_type": hit.signal_type,
            }
        )
    return watchlist
