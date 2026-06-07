from __future__ import annotations

from .macd_screen import MacdHit


def _setup_label(direction: str) -> str:
    return "MACD Golden Cross" if direction == "golden_cross" else "MACD Dead Cross"


def build_macd_watchlist(hits: list[MacdHit]) -> list[dict[str, object]]:
    watchlist: list[dict[str, object]] = []
    for hit in hits:
        summary = (
            f"{_setup_label(hit.direction)} on {hit.signal_date}. "
            f"MACD {hit.macd_value:+.4f}, signal {hit.signal_value:+.4f}, histogram {hit.histogram_value:+.4f}. "
            f"Signal age {hit.signal_age_bars} bar(s)."
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
                "entry_style": "macd_cross",
                "entry_price": round(hit.current_price, 4),
                "entry_label": "Current close" if hit.signal_age_bars > 0 else "Signal close",
                "entry_timeframe": "daily",
                "signal_direction": hit.direction,
                "macd_value": round(hit.macd_value, 4),
                "signal_value": round(hit.signal_value, 4),
                "histogram_value": round(hit.histogram_value, 4),
            }
        )
    return watchlist
