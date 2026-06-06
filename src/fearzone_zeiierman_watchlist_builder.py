from __future__ import annotations

from .fearzone_zeiierman_screen import FearzoneZeiiermanHit


def _format_note(hit: FearzoneZeiiermanHit) -> str:
    return (
        f"{' | '.join(hit.reasons)}. "
        f"Current close {hit.current_price:.2f}. "
        f"Signal close {hit.signal_close:.2f}, signal low {hit.signal_low:.2f}. "
        f"FZ1 {hit.fz1_value:.4f} vs limit {hit.fz1_limit:.4f}. "
        f"FZ2 {hit.fz2_value:.4f} vs limit {hit.fz2_limit:.4f}."
    )


def build_fearzone_zeiierman_watchlist(hits: list[FearzoneZeiiermanHit]) -> list[dict[str, object]]:
    watchlist: list[dict[str, object]] = []
    for hit in hits:
        summary = (
            f"Zeiierman Fearzone setup on {hit.signal_date}. "
            f"{hit.ma_type} source average with high period {hit.high_period} and stdev period {hit.stdev_period}. "
            f"Signal is {hit.signal_age_bars} bar(s) old."
        )
        watchlist.append(
            {
                "ticker": hit.ticker,
                "sector": hit.sector,
                "industry": hit.industry,
                "setup_label": "Fearzone Zeiierman",
                "summary": summary,
                "master_note": _format_note(hit),
                "event_date": hit.signal_date,
                "event_label": "Fearzone Zeiierman",
                "trigger_label": "Signal close",
                "trigger_price": round(hit.signal_close, 4),
                "entry_style": "fearzone_zeiierman",
                "entry_price": round(hit.current_price, 4),
                "entry_label": "Current close" if hit.signal_age_bars > 0 else "Signal close",
                "entry_timeframe": "daily",
                "stop_price": round(hit.signal_low, 4),
                "stop_label": "Signal low",
                "stop_timeframe": "daily",
                "signal_close": round(hit.signal_close, 4),
                "signal_age_bars": hit.signal_age_bars,
            }
        )
    return watchlist
