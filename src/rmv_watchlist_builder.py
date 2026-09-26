from __future__ import annotations

from .rmv_screen import RmvHit


def build_rmv_watchlist(hits: list[RmvHit]) -> list[dict[str, object]]:
    return [
        {
            "ticker": hit.ticker, "sector": hit.sector, "industry": hit.industry, "exchange": hit.exchange,
            "setup_label": f"RMV {hit.signal_kind.replace('_', ' ').title()}",
            "summary": f"RMV {hit.rmv:.1f}, smoothed {hit.rmv_smooth:.1f}; rank {hit.rank_tier or 'release'}.",
            "master_note": ". ".join(hit.reasons), "event_date": hit.signal_date,
            "event_label": "RMV tightness signal", "trigger_label": "Price-pane breakout confirmation",
            "entry_style": "rmv_tightness", "current_price": round(hit.current_price, 4),
            "rmv": round(hit.rmv, 2), "rmv_smooth": round(hit.rmv_smooth, 2), "rmv_rank": hit.rank_tier,
            "rmv_signal_kind": hit.signal_kind, "rmv_tight_streak": hit.tight_streak,
            "rmv_a_plus": hit.a_plus, "rmv_trough": hit.trough, "rmv_release": hit.bottom_breakout,
            "signal_badges": [f"RMV {hit.rmv:.0f}", f"R{hit.rank_tier}" if hit.rank_tier else "Release"] + (["A+"] if hit.a_plus else []) + (["VDU"] if hit.vdu_setup else []),
        }
        for hit in hits
    ]
