from __future__ import annotations

from .sma200_pullback_buy_screen import Sma200PullbackBuyHit


def build_sma200_pullback_buy_watchlist(hits: list[Sma200PullbackBuyHit]) -> list[dict[str, object]]:
    watchlist: list[dict[str, object]] = []
    for hit in hits:
        summary = (
            f"200 SMA pullback test on {hit.test_date} held above the 200 SMA, then the latest bullish candle "
            f"took out the test high {hit.test_high:.2f}. SMA200 {hit.sma200:.2f}, SMA50 {hit.sma50:.2f}."
        )
        watchlist.append(
            {
                "ticker": hit.ticker,
                "sector": hit.sector,
                "industry": hit.industry,
                "setup_label": "200 SMA Pullback Buy",
                "summary": summary,
                "master_note": ". ".join(hit.reasons),
                "event_date": hit.signal_date,
                "event_label": "200 SMA Buy",
                "trigger_label": "Test candle high",
                "trigger_price": round(hit.test_high, 4),
                "entry_style": "sma200_pullback_buy",
                "entry_price": round(hit.breakout_close, 4),
                "entry_label": "Breakout close",
                "entry_timeframe": "daily",
                "secondary_entry_price": round(hit.sma200, 4),
                "secondary_entry_label": "200 SMA support",
                "secondary_entry_timeframe": "daily",
                "stop_price": round(hit.test_low, 4),
                "stop_label": "Test candle low",
                "stop_timeframe": "daily",
                "signal_badges": ["200 SMA Pullback", f"Test {hit.test_count}"],
            }
        )
    return watchlist
