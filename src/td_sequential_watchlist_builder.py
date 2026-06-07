from __future__ import annotations

from .td_sequential_screen import TdSequentialHit


def _setup_label(direction: str) -> str:
    return "Bullish TD9" if direction == "bullish" else "Bearish TD9"


def build_td_sequential_watchlist(hits: list[TdSequentialHit]) -> list[dict[str, object]]:
    watchlist: list[dict[str, object]] = []
    for hit in hits:
        direction_text = "bullish" if hit.direction == "bullish" else "bearish"
        summary = (
            f"{direction_text.title()} TD Sequential completed 9 on {hit.signal_date}. "
            f"Latest close {hit.signal_close:.2f} versus close[4] {hit.comparison_close:.2f}."
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
                "entry_style": "td9",
                "entry_price": round(hit.current_price, 4),
                "entry_label": "Signal close",
                "entry_timeframe": "daily",
                "signal_direction": hit.direction,
                "setup_count": hit.setup_count,
                "comparison_close": round(hit.comparison_close, 4),
            }
        )
    return watchlist
