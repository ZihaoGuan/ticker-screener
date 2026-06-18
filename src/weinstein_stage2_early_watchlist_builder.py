from __future__ import annotations

from .weinstein_stage2_early_screen import WeinsteinStage2EarlyHit


def build_weinstein_stage2_early_watchlist(hits: list[WeinsteinStage2EarlyHit]) -> list[dict[str, object]]:
    watchlist: list[dict[str, object]] = []
    for hit in hits:
        summary = (
            f"Weinstein Stage 2 early. "
            f"30W EMA {hit.weekly_ma30:.2f}, close {hit.weekly_close:.2f}, "
            f"run {hit.run_length_weeks} weeks."
        )
        watchlist.append(
            {
                "ticker": hit.ticker,
                "sector": hit.sector,
                "industry": hit.industry,
                "setup_label": "Weinstein Stage 2 Early",
                "summary": summary,
                "master_note": ". ".join(hit.reasons),
                "event_date": hit.signal_date,
                "event_label": "Weinstein Stage 2 early",
                "trigger_label": "30W EMA trend",
                "trigger_price": round(hit.weekly_ma30, 4),
                "entry_style": "weinstein_stage2_early",
                "entry_price": round(hit.weekly_close, 4),
                "entry_label": "Weekly close",
                "entry_timeframe": "weekly",
                "weekly_ma30": round(hit.weekly_ma30, 4),
                "slope_ratio_pct": round(hit.slope_ratio, 4),
                "extension_pct": round(hit.extension_pct, 4),
                "run_length_weeks": hit.run_length_weeks,
                "maturity": hit.maturity,
                "sentiment": hit.sentiment,
            }
        )
    return watchlist
