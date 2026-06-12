from __future__ import annotations

from .rti_screen import RtiHit


def build_rti_watchlist(hits: list[RtiHit]) -> list[dict[str, object]]:
    watchlist: list[dict[str, object]] = []
    for hit in hits:
        summary = (
            f"RTI {hit.signal_kind} at {hit.rti_value:.1f}. "
            f"Range {hit.current_volatility:.2f} inside {hit.min_volatility:.2f}-{hit.max_volatility:.2f} lookback band."
        )
        watchlist.append(
            {
                "ticker": hit.ticker,
                "sector": hit.sector,
                "industry": hit.industry,
                "setup_label": f"RTI {hit.signal_kind}",
                "summary": summary,
                "master_note": ". ".join(hit.reasons),
                "event_date": hit.signal_date,
                "event_label": f"RTI {hit.signal_kind}",
                "trigger_label": "Signal high",
                "trigger_price": round(hit.high_price, 4),
                "entry_style": "rti_signal",
                "entry_price": round(hit.current_price, 4),
                "entry_label": "Signal close",
                "entry_timeframe": "daily",
                "signal_kind": hit.signal_kind,
                "stop_price": round(hit.low_price, 4),
                "stop_label": "Signal low",
                "stop_timeframe": "daily",
                "rti_value": round(hit.rti_value, 4),
                "previous_rti_value": round(hit.previous_rti_value, 4),
                "consecutive_below_20_count": hit.consecutive_below_20_count,
            }
        )
    return watchlist
