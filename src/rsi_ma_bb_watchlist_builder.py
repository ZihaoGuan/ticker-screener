from __future__ import annotations

from .rsi_ma_bb_screen import RsiMaBbHit


def _setup_label(direction: str) -> str:
    return "RSI MA/BB Buy Signal" if direction == "bullish" else "RSI MA/BB Sell Signal"


def build_rsi_ma_bb_watchlist(hits: list[RsiMaBbHit]) -> list[dict[str, object]]:
    watchlist: list[dict[str, object]] = []
    for hit in hits:
        summary = (
            f"{_setup_label(hit.direction)} on {hit.signal_date}. "
            f"Sources {', '.join(hit.signal_sources)}. "
            f"RSI {hit.rsi_value:.2f}. Signal age {hit.signal_age_bars} bar(s)."
        )
        watchlist.append(
            {
                "ticker": hit.ticker,
                "sector": hit.sector,
                "industry": hit.industry,
                "setup_label": _setup_label(hit.direction),
                "summary": summary,
                "master_note": ". ".join(hit.reasons),
                "event_date": hit.signal_date,
                "event_label": _setup_label(hit.direction),
                "trigger_label": "Signal close",
                "trigger_price": round(hit.signal_close, 4),
                "entry_style": "rsi_ma_bb",
                "entry_price": round(hit.current_price, 4),
                "entry_label": "Current close" if hit.signal_age_bars > 0 else "Signal close",
                "entry_timeframe": "daily",
                "signal_direction": hit.direction,
                "signal_sources": hit.signal_sources,
                "rsi_value": round(hit.rsi_value, 2),
                "rsi_ma_value": round(hit.rsi_ma_value, 2),
                "rsi_bb_mid_value": round(hit.rsi_bb_mid_value, 2),
                "rsi_bb_upper_value": round(hit.rsi_bb_upper_value, 2),
                "rsi_bb_lower_value": round(hit.rsi_bb_lower_value, 2),
            }
        )
    return watchlist
