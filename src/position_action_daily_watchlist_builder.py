from __future__ import annotations

from .position_action_daily_screen import PositionActionDailyHit


def build_position_action_daily_watchlist(hits: list[PositionActionDailyHit]) -> list[dict[str, object]]:
    watchlist: list[dict[str, object]] = []
    for hit in hits:
        action_label = "Add Position" if hit.action == "add_position" else "Trim / Reduce"
        summary = (
            f"{action_label}: trend {hit.trend_state}, extension {hit.extension_state}, "
            f"ATR distance vs 21EMA {hit.atr_dist_21 if hit.atr_dist_21 is not None else '-'}, "
            f"vs 10W {hit.atr_dist_10w if hit.atr_dist_10w is not None else '-'}."
        )
        watchlist.append(
            {
                "ticker": hit.ticker,
                "sector": hit.sector,
                "industry": hit.industry,
                "setup_label": action_label,
                "summary": summary,
                "master_note": hit.reason_summary,
                "event_date": hit.as_of_date,
                "event_label": action_label,
                "entry_style": "position_action_daily",
                "entry_price": round(hit.close_price, 4),
                "entry_label": "Latest close",
                "entry_timeframe": "daily",
                "secondary_entry_price": round(hit.ema21, 4) if hit.ema21 is not None else None,
                "secondary_entry_label": "EMA21",
                "secondary_entry_timeframe": "daily",
                "trigger_price": round(hit.sma10w, 4) if hit.sma10w is not None else None,
                "trigger_label": "10W SMA",
                "signal_badges": [action_label, hit.extension_state.replace("_", " ").title()],
                "action": hit.action,
                "action_score": round(hit.action_score, 2),
                "atr_dist_21": hit.atr_dist_21,
                "atr_dist_10w": hit.atr_dist_10w,
                "danger_signal_count": hit.danger_signal_count,
            }
        )
    return watchlist
