from __future__ import annotations

from .canslim_screen import CANSLIM_MIN_SCORE, CANSLIM_MIN_WATCHLIST_COUNT, CANSLIM_WATCHLIST_FALLBACK_COUNT, CanslimHit


def _letters_text(hit: CanslimHit) -> str:
    return " ".join(f"{key}:{value}" for key, value in hit.letter_scores.items())


def build_canslim_watchlist(hits: list[CanslimHit]) -> list[dict[str, object]]:
    if not hits:
        return []
    qualified = [item for item in hits if item.score >= CANSLIM_MIN_SCORE]
    selected = qualified if len(qualified) >= CANSLIM_MIN_WATCHLIST_COUNT else hits[: min(len(hits), CANSLIM_WATCHLIST_FALLBACK_COUNT)]
    watchlist: list[dict[str, object]] = []
    for hit in selected:
        headline_reasons = ". ".join(hit.reasons[:3]).strip()
        summary = f"CANSLIM score {hit.score}/{hit.max_score}. {_letters_text(hit)}."
        if headline_reasons:
            summary = f"{summary} {headline_reasons}"
        watchlist.append(
            {
                "ticker": hit.ticker,
                "sector": hit.sector,
                "industry": hit.industry,
                "setup_label": "CANSLIM High Score",
                "summary": summary,
                "master_note": f"Rank {hit.rank}. Score {hit.score}/{hit.max_score}. {_letters_text(hit)}. {'; '.join(hit.reasons)}",
                "entry_style": "canslim",
                "entry_label": "Current close",
                "entry_timeframe": "daily",
                "score": hit.score,
                "max_score": hit.max_score,
                "rank": hit.rank,
                "letter_scores": dict(hit.letter_scores),
                "letter_passes": dict(hit.letter_passes),
                "leader_flags": list(hit.leader_flags),
                "trigger_label": "Current close",
                "trigger_price": hit.metrics.get("close"),
                "entry_price": hit.metrics.get("close"),
            }
        )
    return watchlist
