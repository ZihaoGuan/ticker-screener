from __future__ import annotations

from .market_correction_resilience_screen import MarketCorrectionResilienceHit


def build_market_correction_resilience_watchlist(hits: list[MarketCorrectionResilienceHit]) -> list[dict[str, object]]:
    watchlist: list[dict[str, object]] = []
    for hit in hits:
        summary = (
            f"Held within {hit.distance_from_52wk_high_pct:.1f}% of the 52-week high while {hit.benchmark_ticker} stayed "
            f"{hit.benchmark_drawdown_pct:.1f}% below its prior high. RS {hit.rs_rating:.1f}."
        )
        watchlist.append(
            {
                "ticker": hit.ticker,
                "sector": hit.sector,
                "industry": hit.industry,
                "exchange": hit.exchange,
                "setup_label": "Market Correction Resilience",
                "summary": summary,
                "master_note": ". ".join(hit.reasons),
                "event_date": hit.signal_date,
                "event_label": "Correction resilience",
                "trigger_label": "Near 52-week high",
                "trigger_price": round(hit.high_52wk, 4),
                "entry_style": "market_correction_resilience",
                "entry_price": round(hit.current_price, 4),
                "entry_label": "Current close",
                "entry_timeframe": "daily",
                "secondary_entry_price": round(hit.ema21, 4),
                "secondary_entry_label": "EMA21 support",
                "secondary_entry_timeframe": "daily",
                "stop_price": round(hit.ema40, 4),
                "stop_label": "8W EMA support",
                "stop_timeframe": "daily",
                "current_price": round(hit.current_price, 4),
                "ema21": round(hit.ema21, 4),
                "ema40": round(hit.ema40, 4),
                "high_52wk": round(hit.high_52wk, 4),
                "distance_from_52wk_high_pct": round(hit.distance_from_52wk_high_pct, 2),
                "rs_rating": round(hit.rs_rating, 2),
                "benchmark_ticker": hit.benchmark_ticker,
                "benchmark_drawdown_pct": round(hit.benchmark_drawdown_pct, 2),
                "signal_badges": [
                    "Correction Resilience",
                    "Near 52W High",
                    "Above Rising EMA",
                    "RS Leader",
                ],
            }
        )
    return watchlist
