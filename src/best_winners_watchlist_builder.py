from __future__ import annotations

from .best_winners_screen import BestWinnersHit


def build_best_winners_watchlist(hits: list[BestWinnersHit]) -> list[dict[str, object]]:
    return [
        {
            "ticker": hit.ticker,
            "sector": hit.sector,
            "industry": hit.industry,
            "exchange": hit.exchange,
            "setup_label": "Best Winners",
            "summary": (
                f"Positive 3M/6M/1Y momentum with ADR20 {hit.adr_pct_20:.1f}%, "
                f"{hit.distance_from_52wk_low_pct:.1f}% above the 52-week low, and "
                f"30D average dollar volume ${hit.avg_dollar_volume_30 / 1_000_000:.1f}M."
            ),
            "master_note": ". ".join(hit.reasons),
            "event_date": hit.signal_date,
            "event_label": "Best Winners pass",
            "trigger_label": "Review for entry setup",
            "trigger_price": None,
            "entry_style": "best_winners",
            "entry_price": round(hit.current_price, 4),
            "entry_label": "Current close",
            "entry_timeframe": "daily",
            "secondary_entry_price": round(hit.ema8, 4),
            "secondary_entry_label": "8 EMA",
            "secondary_entry_timeframe": "daily",
            "stop_price": round(hit.ema21, 4),
            "stop_label": "21 EMA reference",
            "stop_timeframe": "daily",
            "current_price": round(hit.current_price, 4),
            "adr_pct_20": round(hit.adr_pct_20, 2),
            "low_52wk": round(hit.low_52wk, 4),
            "distance_from_52wk_low_pct": round(hit.distance_from_52wk_low_pct, 2),
            "return_3m_pct": round(hit.return_3m_pct, 2),
            "return_6m_pct": round(hit.return_6m_pct, 2),
            "return_1y_pct": round(hit.return_1y_pct, 2),
            "avg_volume_30": round(hit.avg_volume_30, 2),
            "avg_dollar_volume_30": round(hit.avg_dollar_volume_30, 2),
            "session_volume": round(hit.session_volume, 2),
            "session_dollar_volume": round(hit.session_dollar_volume, 2),
            "ema8": round(hit.ema8, 4),
            "ema21": round(hit.ema21, 4),
            "ema60": round(hit.ema60, 4),
            "signal_badges": [
                "Best Winners",
                "ADR20 > 4.5%",
                "3M / 6M / 1Y > 0%",
                "8 EMA > 21 EMA",
                "Above 60 EMA",
            ],
        }
        for hit in hits
    ]
