from __future__ import annotations

from .rs_rating_screen import RsRatingHit


def _format_note(hit: RsRatingHit) -> str:
    return (
        f"{' | '.join(hit.reasons)}. "
        f"Current close {hit.current_price:.2f}. "
        f"RS line {hit.current_rs_line:.4f}. "
        f"RS score {hit.rs_score:.2f}."
    )


def build_rs_rating_watchlist(hits: list[RsRatingHit]) -> list[dict[str, object]]:
    watchlist: list[dict[str, object]] = []
    for hit in hits:
        summary = (
            f"Latest RS rating is {hit.rs_rating:.1f} versus minimum {hit.min_rating:.1f}. "
            f"Weighted RS score is {hit.rs_score:.2f} versus {hit.benchmark_ticker}."
        )
        watchlist.append(
            {
                "ticker": hit.ticker,
                "sector": hit.sector,
                "industry": hit.industry,
                "setup_label": "RS Rating",
                "summary": summary,
                "master_note": _format_note(hit),
                "event_date": hit.signal_date,
                "event_label": "RS rating",
                "trigger_label": "Current close",
                "trigger_price": round(hit.current_price, 4),
                "entry_style": "rs_rating",
                "entry_price": round(hit.current_price, 4),
                "entry_label": "Current close",
                "entry_timeframe": "daily",
                "signal_rs_rating": round(hit.rs_rating, 2),
                "signal_rs_score": round(hit.rs_score, 4),
            }
        )
    return watchlist
