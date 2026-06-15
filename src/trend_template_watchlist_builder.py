from __future__ import annotations

from .trend_template_screen import TrendTemplateHit


def build_trend_template_watchlist(hits: list[TrendTemplateHit]) -> list[dict[str, object]]:
    watchlist: list[dict[str, object]] = []
    for hit in hits:
        summary = (
            f"Passed {hit.criteria_passed}/{hit.criteria_total} Minervini trend-template checks. "
            f"{hit.distance_from_52wk_high_pct:.1f}% below 52-week high and {hit.distance_from_52wk_low_pct:.1f}% above 52-week low."
        )
        watchlist.append(
            {
                "ticker": hit.ticker,
                "sector": hit.sector,
                "industry": hit.industry,
                "exchange": hit.exchange,
                "setup_label": "Trend Template",
                "summary": summary,
                "master_note": ". ".join(hit.reasons),
                "event_date": hit.signal_date,
                "event_label": "Trend pass",
                "trigger_label": "52-week high breakout",
                "trigger_price": round(hit.high_52wk, 4),
                "entry_style": "trend_template",
                "entry_price": round(hit.current_price, 4),
                "entry_label": "Current close",
                "entry_timeframe": "daily",
                "secondary_entry_price": round(hit.ma50, 4),
                "secondary_entry_label": "50D MA",
                "secondary_entry_timeframe": "daily",
                "stop_price": round(hit.ma50, 4),
                "stop_label": "50D MA support",
                "stop_timeframe": "daily",
                "current_price": round(hit.current_price, 4),
                "ma50": round(hit.ma50, 4),
                "ma150": round(hit.ma150, 4),
                "ma200": round(hit.ma200, 4),
                "high_52wk": round(hit.high_52wk, 4),
                "low_52wk": round(hit.low_52wk, 4),
                "distance_from_52wk_high_pct": round(hit.distance_from_52wk_high_pct, 2),
                "distance_from_52wk_low_pct": round(hit.distance_from_52wk_low_pct, 2),
                "signal_badges": [
                    "Trend Template",
                    "50>150>200",
                    "200D Up",
                    "Near 52W High",
                ],
            }
        )
    return watchlist
