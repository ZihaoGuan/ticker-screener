from __future__ import annotations

from .kai_s2_screen import KaiS2Hit


def build_kai_s2_watchlist(hits: list[KaiS2Hit]) -> list[dict[str, object]]:
    watchlist: list[dict[str, object]] = []
    for hit in hits:
        market_cap_b = (hit.market_cap or 0.0) / 1_000_000_000.0
        summary = (
            f"Passed Kai S2 liquidity and trend filter with price {hit.current_price:.2f}, "
            f"market cap {market_cap_b:.2f}B, 30D avg volume {hit.avg_volume_30:,.0f}, "
            f"and 1Y beta {hit.beta_1y:.2f}."
        )
        watchlist.append(
            {
                "ticker": hit.ticker,
                "sector": hit.sector,
                "industry": hit.industry,
                "exchange": hit.exchange,
                "setup_label": "Kai S2",
                "summary": summary,
                "master_note": ". ".join(hit.reasons),
                "event_date": hit.signal_date,
                "event_label": "Kai S2 pass",
                "trigger_label": "Current close",
                "trigger_price": round(hit.current_price, 4),
                "entry_style": "kai_s2",
                "entry_price": round(hit.current_price, 4),
                "entry_label": "Current close",
                "entry_timeframe": "daily",
                "secondary_entry_price": round(hit.sma20, 4),
                "secondary_entry_label": "20D SMA",
                "secondary_entry_timeframe": "daily",
                "stop_price": round(hit.sma50, 4),
                "stop_label": "50D SMA support",
                "stop_timeframe": "daily",
                "current_price": round(hit.current_price, 4),
                "market_cap": round(hit.market_cap, 2) if hit.market_cap is not None else None,
                "market_cap_b": round(market_cap_b, 4) if hit.market_cap is not None else None,
                "sma20": round(hit.sma20, 4),
                "sma50": round(hit.sma50, 4),
                "sma150": round(hit.sma150, 4),
                "sma200": round(hit.sma200, 4),
                "avg_volume_30": round(hit.avg_volume_30, 2),
                "price_times_avg_volume_30": round(hit.price_times_avg_volume_30, 2),
                "beta_1y": round(hit.beta_1y, 4) if hit.beta_1y is not None else None,
                "signal_badges": [
                    "Kai S2",
                    "Price > 20/50/200 SMA",
                    "150 > 200 SMA",
                    "Avg Vol > 2M",
                    "Beta > 1",
                ],
            }
        )
    return watchlist
