from __future__ import annotations

from .one_year_winners_screen import OneYearWinnersHit


def build_one_year_winners_watchlist(hits: list[OneYearWinnersHit]) -> list[dict[str, object]]:
    return [
        {
            "ticker": hit.ticker,
            "sector": hit.sector,
            "industry": hit.industry,
            "exchange": hit.exchange,
            "setup_label": "1Y Winners > $10B",
            "summary": (
                f"Rolling 1Y return {hit.one_year_return_pct:.1f}%, market cap "
                f"${(hit.market_cap or 0.0) / 1_000_000_000:.1f}B, 1M return "
                f"{hit.one_month_return_pct:.1f}%, and 1Y beta {hit.beta_1y:.2f}."
            ),
            "master_note": ". ".join(hit.reasons),
            "event_date": hit.signal_date,
            "event_label": "1Y leadership pass",
            "trigger_label": "Current close",
            "trigger_price": round(hit.current_price, 4),
            "entry_style": "one_year_winners",
            "entry_price": round(hit.current_price, 4),
            "entry_label": "Current close",
            "entry_timeframe": "daily",
            "secondary_entry_price": round(hit.ema21, 4),
            "secondary_entry_label": "21 EMA",
            "secondary_entry_timeframe": "daily",
            "stop_price": round(hit.sma50, 4),
            "stop_label": "50 SMA reference",
            "stop_timeframe": "daily",
            "current_price": round(hit.current_price, 4),
            "market_cap": round(hit.market_cap, 2) if hit.market_cap is not None else None,
            "market_cap_b": round((hit.market_cap or 0.0) / 1_000_000_000, 4) if hit.market_cap is not None else None,
            "one_week_return_pct": round(hit.one_week_return_pct, 2),
            "one_month_return_pct": round(hit.one_month_return_pct, 2),
            "one_year_return_pct": round(hit.one_year_return_pct, 2),
            "revenue_growth_ttm_yoy_pct": round(hit.revenue_growth_ttm_yoy_pct, 2)
            if hit.revenue_growth_ttm_yoy_pct is not None
            else None,
            "beta_1y": round(hit.beta_1y, 4) if hit.beta_1y is not None else None,
            "avg_volume_1m": round(hit.avg_volume_1m, 2),
            "monthly_dollar_volume": round(hit.monthly_dollar_volume, 2),
            "ema21": round(hit.ema21, 4),
            "sma50": round(hit.sma50, 4),
            "ema100": round(hit.ema100, 4),
            "signal_badges": [
                "1Y Return > 30%",
                "Market Cap > $10B",
                "Revenue Growth > 0%",
                "Beta > 1",
                "$ Volume > $900M",
                "21 EMA > 50 SMA",
            ],
        }
        for hit in hits
    ]
