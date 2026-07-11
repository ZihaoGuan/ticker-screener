from __future__ import annotations

from .bollinger_band_screen import BollingerBandBreakoutHit


def build_bollinger_band_breakout_watchlist(hits: list[BollingerBandBreakoutHit]) -> list[dict[str, object]]:
    watchlist: list[dict[str, object]] = []
    for hit in hits:
        summary = (
            f"Close above upper Bollinger Band by {hit.close_vs_upper_pct:.2f}%. "
            f"Upper {hit.upper_band:.2f}, 20SMA {hit.middle_band:.2f}."
        )
        watchlist.append(
            {
                "ticker": hit.ticker,
                "sector": hit.sector,
                "industry": hit.industry,
                "setup_label": "Above Upper Bollinger Band",
                "summary": summary,
                "master_note": ". ".join(hit.reasons),
                "event_date": hit.signal_date,
                "event_label": "Above Upper Bollinger Band",
                "trigger_label": "Upper band",
                "trigger_price": round(hit.upper_band, 4),
                "entry_style": "bollinger_band_breakout",
                "entry_price": round(hit.current_price, 4),
                "entry_label": "Latest close",
                "entry_timeframe": "daily",
                "middle_band": round(hit.middle_band, 4),
                "upper_band": round(hit.upper_band, 4),
                "lower_band": round(hit.lower_band, 4),
                "close_vs_upper_pct": round(hit.close_vs_upper_pct, 4),
                "signal_badges": ["Bollinger", "Above Upper"],
            }
        )
    return watchlist
