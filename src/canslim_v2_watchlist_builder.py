from __future__ import annotations

from .canslim_v2_screen import CanslimV2Hit


def build_canslim_v2_watchlist(hits: list[CanslimV2Hit]) -> list[dict[str, object]]:
    watchlist: list[dict[str, object]] = []
    for hit in hits:
        summary = (
            f"CANSLIM V2 {hit.rating} ({hit.composite_score:.1f}). "
            f"C {hit.component_scores['C']:.0f}, A {hit.component_scores['A']:.0f}, "
            f"N {hit.component_scores['N']:.0f}, L {hit.component_scores['L']:.0f}."
        )
        watchlist.append(
            {
                "ticker": hit.ticker,
                "sector": hit.sector,
                "industry": hit.industry,
                "setup_label": "CANSLIM V2",
                "summary": summary,
                "master_note": ". ".join(hit.reasons),
                "entry_style": "canslim",
                "entry_label": "Current close",
                "entry_timeframe": "daily",
                "score": round(hit.composite_score, 1),
                "score_label": hit.rating,
                "rank": hit.rank,
                "component_scores": dict(hit.component_scores),
                "component_passes": dict(hit.component_passes),
                "leader_flags": list(hit.leader_flags),
                "trigger_label": "Current close",
                "trigger_price": hit.metrics.get("close"),
                "entry_price": hit.metrics.get("close"),
            }
        )
    return watchlist
