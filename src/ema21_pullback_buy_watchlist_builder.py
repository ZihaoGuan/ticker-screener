from __future__ import annotations

from .ema21_pullback_buy_screen import Ema21PullbackBuyHit


def build_ema21_pullback_buy_watchlist(hits: list[Ema21PullbackBuyHit]) -> list[dict[str, object]]:
    watchlist: list[dict[str, object]] = []
    for hit in hits:
        summary = (
            f"EMA21 pullback test on {hit.test_date} held above the 21 EMA, then the latest bullish candle "
            f"took out the test high {hit.test_high:.2f}. EMA21 {hit.ema21:.2f}, SMA50 {hit.sma50:.2f}."
        )
        watchlist.append(
            {
                "ticker": hit.ticker,
                "sector": hit.sector,
                "industry": hit.industry,
                "setup_label": "EMA21 Pullback Buy",
                "summary": summary,
                "master_note": ". ".join(hit.reasons),
                "event_date": hit.signal_date,
                "event_label": "EMA21 Buy",
                "trigger_label": "Test candle high",
                "trigger_price": round(hit.test_high, 4),
                "entry_style": "ema21_pullback_buy",
                "entry_price": round(hit.breakout_close, 4),
                "entry_label": "Breakout close",
                "entry_timeframe": "daily",
                "secondary_entry_price": round(hit.ema21, 4),
                "secondary_entry_label": "EMA21 support",
                "secondary_entry_timeframe": "daily",
                "stop_price": round(hit.test_low, 4),
                "stop_label": "Test candle low",
                "stop_timeframe": "daily",
                "signal_badges": ["EMA21 Pullback", f"Test {hit.test_count}"],
            }
        )
    return watchlist
