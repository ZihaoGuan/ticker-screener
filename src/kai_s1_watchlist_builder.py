from __future__ import annotations

from .kai_s1_screen import KaiS1Hit


def build_kai_s1_watchlist(hits: list[KaiS1Hit]) -> list[dict[str, object]]:
    return [
        {
            "ticker": hit.ticker, "sector": hit.sector, "industry": hit.industry, "exchange": hit.exchange,
            "setup_label": "Kai S1", "summary": "Passed Kai S1 weekly trend, liquidity, capitalization, and ADR filters.",
            "master_note": ". ".join(hit.reasons), "event_date": hit.signal_date, "event_label": "Kai S1 pass",
            "trigger_label": "Current close", "trigger_price": round(hit.current_price, 4), "entry_style": "kai_s1",
            "entry_price": round(hit.current_price, 4), "entry_label": "Current close", "entry_timeframe": "daily",
            "stop_price": round(hit.sma50, 4), "stop_label": "50D SMA support", "stop_timeframe": "daily",
            "current_price": round(hit.current_price, 4), "market_cap": round(hit.market_cap, 2) if hit.market_cap is not None else None,
            "market_cap_b": round(hit.market_cap / 1_000_000_000, 4) if hit.market_cap is not None else None,
            "sma50": round(hit.sma50, 4), "weekly_sma30": round(hit.weekly_sma30, 4), "weekly_sma40": round(hit.weekly_sma40, 4),
            "dollar_volume": round(hit.dollar_volume, 2), "adr_pct_20": round(hit.adr_pct_20, 4),
            "signal_badges": ["Kai S1", "Weekly SMA30 > SMA40", "Price > Weekly SMA30/40 + Daily SMA50", "Market cap > $10B", "ADR20 > 2%"],
        }
        for hit in hits
    ]
