from __future__ import annotations

from .liquid_growth_screen import LiquidGrowthHit


def build_liquid_growth_watchlist(hits: list[LiquidGrowthHit]) -> list[dict[str, object]]:
    return [
        {
            "ticker": hit.ticker,
            "sector": hit.sector,
            "industry": hit.industry,
            "exchange": hit.exchange,
            "setup_label": "Liquid Growth (TML)",
            "summary": (
                f"Reported quarterly revenue/EPS YoY {hit.quarterly_revenue_yoy_pct:.1f}% / "
                f"{hit.quarterly_eps_yoy_pct:.1f}%, Daily RS {hit.daily_rs_rating:.0f}, "
                f"and 50D dollar volume ${hit.avg_dollar_volume_50 / 1_000_000:.1f}M."
            ),
            "master_note": ". ".join(hit.reasons),
            "event_date": hit.signal_date,
            "event_label": "Liquid Growth candidate",
            "trigger_label": "Current close",
            "trigger_price": round(hit.current_price, 4),
            "entry_style": "liquid_growth",
            "entry_price": round(hit.current_price, 4),
            "entry_label": "Current close",
            "entry_timeframe": "daily",
            "secondary_entry_price": round(hit.sma50, 4),
            "secondary_entry_label": "50 SMA reference",
            "secondary_entry_timeframe": "daily",
            "stop_price": round(hit.sma200, 4),
            "stop_label": "200 SMA reference",
            "stop_timeframe": "daily",
            "current_price": round(hit.current_price, 4),
            "market_cap": round(hit.market_cap, 2),
            "avg_volume_50": round(hit.avg_volume_50, 2),
            "avg_dollar_volume_50": round(hit.avg_dollar_volume_50, 2),
            "roe_pct": round(hit.roe_pct, 2),
            "gross_margin_pct": round(hit.gross_margin_pct, 2) if hit.gross_margin_pct is not None else None,
            "operating_margin_pct": round(hit.operating_margin_pct, 2) if hit.operating_margin_pct is not None else None,
            "daily_rs_rating": round(hit.daily_rs_rating, 2),
            "quarterly_revenue_yoy_pct": round(hit.quarterly_revenue_yoy_pct, 2),
            "quarterly_eps_yoy_pct": round(hit.quarterly_eps_yoy_pct, 2),
            "signal_badges": [
                "Price >= $20", "Market Cap >= $2B", "50D Dollar Volume >= $30M", "ROE >= 17%",
                "Reported Quarterly Revenue/EPS YoY >= 25%", "Above Rising 50/200 SMA", "Daily RS >= 85",
            ],
        }
        for hit in hits
    ]
