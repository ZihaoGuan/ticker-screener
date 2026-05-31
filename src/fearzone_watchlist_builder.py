from __future__ import annotations

from .fearzone_screen import FearzoneHit


def _trigger_summary(hit: FearzoneHit) -> str:
    triggers: list[str] = []
    if hit.trigger_negative_impulse:
        triggers.append("negative impulse")
    if hit.trigger_ricochet_zone:
        triggers.append("ricochet zone")
    if hit.trigger_magic_k1:
        triggers.append("Magic-K1")
    return ", ".join(triggers) if triggers else "fearzone trigger"


def _format_note(hit: FearzoneHit) -> str:
    return (
        f"{' | '.join(hit.reasons)}. "
        f"Current close {hit.current_price:.2f}. "
        f"Signal close {hit.signal_close:.2f}, signal low {hit.signal_low:.2f}, MA200 {hit.ma200:.2f}. "
        f"Avg vol20 {hit.avg_volume_20:,.0f}, avg $ vol20 {hit.avg_dollar_volume_20:,.0f}."
    )


def build_fearzone_watchlist(hits: list[FearzoneHit]) -> list[dict[str, object]]:
    watchlist: list[dict[str, object]] = []
    for hit in hits:
        trigger_summary = _trigger_summary(hit)
        summary = (
            f"Fearzone buy setup on {hit.signal_date} via {trigger_summary}. "
            f"FZ1/FZ2 panic filters aligned while price held above MA200 {hit.ma200:.2f}. "
            f"Signal is {hit.signal_age_bars} bar(s) old."
        )
        secondary_mid = (hit.signal_low + hit.signal_close) / 2.0
        watchlist.append(
            {
                "ticker": hit.ticker,
                "sector": hit.sector,
                "industry": hit.industry,
                "setup_label": "Fearzone Buy",
                "summary": summary,
                "master_note": _format_note(hit),
                "event_date": hit.signal_date,
                "event_label": "Fearzone",
                "trigger_label": "Signal close",
                "trigger_price": round(hit.signal_close, 4),
                "entry_style": "fearzone_buy",
                "entry_price": round(hit.current_price, 4),
                "entry_label": "Current close" if hit.signal_age_bars > 0 else "Signal close",
                "entry_timeframe": "daily",
                "secondary_entry_price": round(secondary_mid, 4),
                "secondary_entry_low": round(hit.signal_low, 4),
                "secondary_entry_high": round(hit.signal_close, 4),
                "secondary_entry_label": "Signal reclaim zone",
                "secondary_entry_timeframe": "daily",
                "stop_price": round(hit.signal_low, 4),
                "stop_label": "Signal low",
                "stop_timeframe": "daily",
                "signal_close": round(hit.signal_close, 4),
                "signal_age_bars": hit.signal_age_bars,
            }
        )
    return watchlist
