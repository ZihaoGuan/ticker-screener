from __future__ import annotations

from .hve_screen import HveHit


def _format_note(hit: HveHit) -> str:
    return (
        f"{' | '.join(hit.reasons)}. "
        f"Volume {hit.current_volume:,.0f} vs 50D avg {hit.volume_ma_50:,.0f}. "
        f"Signal bar O/H/L/C {hit.open_price:.2f}/{hit.high_price:.2f}/{hit.low_price:.2f}/{hit.current_price:.2f}. "
        f"50D MA {hit.ma50:.2f}, ATR14 {hit.atr14:.2f}."
    )


def build_hve_watchlist(hits: list[HveHit]) -> list[dict[str, object]]:
    watchlist: list[dict[str, object]] = []
    for hit in hits:
        summary = (
            f"Printed the highest volume in the last 52 weeks on {hit.signal_date}. "
            f"Volume buzz is {hit.volume_buzz_pct:+.1f}% versus the 50D average, "
            f"and the signal bar changed {hit.price_change_pct:+.1f}%. "
            f"Current close is {hit.distance_to_ma50_pct:+.1f}% vs 50D MA and {hit.atr_multiple_from_ma50:+.2f} ATR from the 50D MA."
        )
        watchlist.append(
            {
                "ticker": hit.ticker,
                "sector": hit.sector,
                "industry": hit.industry,
                "setup_label": "HVE 52W",
                "summary": summary,
                "master_note": _format_note(hit),
                "event_date": hit.signal_date,
                "event_label": "HVE signal",
                "trigger_label": "Signal high",
                "trigger_price": round(hit.high_price, 4),
                "entry_style": "highest_volume_52w",
                "entry_price": round(hit.current_price, 4),
                "entry_label": "Signal close",
                "entry_timeframe": "daily",
                "secondary_entry_price": round(hit.ma50, 4),
                "secondary_entry_label": "50D MA",
                "secondary_entry_timeframe": "daily",
                "stop_price": round(hit.low_price, 4),
                "stop_label": "Signal low",
                "stop_timeframe": "daily",
            }
        )
    return watchlist
