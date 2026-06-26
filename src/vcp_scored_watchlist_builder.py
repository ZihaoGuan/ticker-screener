from __future__ import annotations

from .vcp_scored_screen import VcpScoredHit


def build_vcp_scored_watchlist(hits: list[VcpScoredHit]) -> list[dict[str, object]]:
    watchlist: list[dict[str, object]] = []
    for hit in hits:
        summary = (
            f"{hit.rating} ({hit.composite_score:.1f}) {hit.pattern_type}. "
            f"State {hit.execution_state}. Pivot {hit.pivot_price:.2f}, support {hit.support_price:.2f}. "
            f"Demand dry: {'yes' if hit.is_demand_dry else 'no'}."
        )
        watchlist.append(
            {
                "ticker": hit.ticker,
                "sector": hit.sector,
                "setup_label": "VCP Scored",
                "summary": summary,
                "master_note": ". ".join(hit.reasons),
                "event_date": hit.signal_date,
                "event_label": hit.pattern_type,
                "trigger_label": "Pivot breakout",
                "trigger_price": round(hit.pivot_price, 4),
                "entry_style": "pivot_breakout",
                "entry_price": round(hit.pivot_price, 4),
                "entry_label": "Pivot",
                "entry_timeframe": "daily",
                "stop_price": round(hit.support_price, 4),
                "stop_label": "Support",
                "stop_timeframe": "daily",
                "score": round(hit.composite_score, 1),
                "score_label": hit.rating,
                "signal_kind": hit.execution_state,
                "signal_badges": [hit.rating, hit.execution_state, hit.pattern_type],
                "vcp_score": round(hit.composite_score, 1),
                "execution_state": hit.execution_state,
                "pattern_type": hit.pattern_type,
            }
        )
    return watchlist
