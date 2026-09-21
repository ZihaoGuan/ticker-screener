from __future__ import annotations

from .weinstein_stage2_early_screen import WeinsteinStageAnalysisHit


def build_weinstein_stage_analysis_watchlist(hits: list[WeinsteinStageAnalysisHit]) -> list[dict[str, object]]:
    watchlist: list[dict[str, object]] = []
    for hit in hits:
        stage_label = hit.current_stage.replace(" - ", " ")
        stage_alias = hit.stage_alias
        watchlist.append(
            {
                "ticker": hit.ticker,
                "sector": hit.sector,
                "industry": hit.industry,
                "setup_label": f"Weinstein {stage_alias}",
                "summary": (
                    f"{hit.current_stage} ({hit.maturity}); prior {hit.previous_stage}. "
                    f"30W EMA {hit.weekly_ma30:.2f}, close {hit.weekly_close:.2f}, run {hit.run_length_weeks} weeks."
                ),
                "master_note": ". ".join(hit.reasons),
                "event_date": hit.signal_date,
                "event_label": f"{stage_label} · {hit.maturity}",
                "trigger_label": "30W EMA trend",
                "trigger_price": round(hit.weekly_ma30, 4),
                "entry_style": "weinstein_stage_analysis",
                "entry_price": round(hit.weekly_close, 4),
                "entry_label": "Weekly close",
                "entry_timeframe": "weekly",
                "current_stage": hit.current_stage,
                "stage_alias": stage_alias,
                "previous_stage": hit.previous_stage,
                "weekly_ma30": round(hit.weekly_ma30, 4),
                "slope_ratio_pct": round(hit.slope_ratio, 4),
                "extension_pct": round(hit.extension_pct, 4),
                "run_length_weeks": hit.run_length_weeks,
                "maturity": hit.maturity,
                "sentiment": hit.sentiment,
            }
        )
    return watchlist
