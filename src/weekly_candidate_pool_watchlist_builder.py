from __future__ import annotations

from .weekly_candidate_pool_screen import WeeklyCandidatePoolHit


def build_weekly_candidate_pool_watchlist(hits: list[WeeklyCandidatePoolHit]) -> list[dict[str, object]]:
    return [
        {
            "ticker": hit.ticker,
            "sector": hit.sector,
            "industry": hit.industry,
            "exchange": hit.exchange,
            "setup_label": "Weekly Candidate Pool",
            "summary": (
                "Candidate pool only — not a buy signal. "
                f"Daily RS {hit.daily_rs_rating:.1f}, ADR20 {hit.adr_pct_20:.1f}%, and {hit.distance_from_52wk_low_pct:.1f}% above its 52-week low."
            ),
            "master_note": ". ".join(hit.reasons),
            "event_date": hit.signal_date,
            "event_label": "Weekly candidate pass",
            "trigger_label": "Wait for chart setup",
            "trigger_price": None,
            "entry_style": "weekly_candidate_pool",
            "entry_price": round(hit.current_price, 4),
            "entry_label": "Current close",
            "entry_timeframe": "daily",
            "secondary_entry_price": round(hit.ema10, 4),
            "secondary_entry_label": "10 EMA",
            "secondary_entry_timeframe": "daily",
            "stop_price": round(hit.ema20, 4),
            "stop_label": "20 EMA reference",
            "stop_timeframe": "daily",
            "current_price": round(hit.current_price, 4),
            "ema10": round(hit.ema10, 4),
            "ema20": round(hit.ema20, 4),
            "ema50": round(hit.ema50, 4),
            "adr_pct_20": round(hit.adr_pct_20, 2),
            "daily_rs_rating": round(hit.daily_rs_rating, 2),
            "low_52wk": round(hit.low_52wk, 4),
            "distance_from_52wk_low_pct": round(hit.distance_from_52wk_low_pct, 2),
            "signal_badges": ["Candidate Pool", "Daily RS > 90", "ADR20 > 4%", "10 EMA > 20 EMA", "Above 50 EMA"],
        }
        for hit in hits
    ]
