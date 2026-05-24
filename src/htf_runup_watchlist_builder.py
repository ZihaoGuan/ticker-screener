from __future__ import annotations

from .htf_runup_screen import HtfRunupHit


def _format_note(hit: HtfRunupHit) -> str:
    return (
        f"{' | '.join(hit.reasons)}. "
        f"Current close {hit.current_price:.2f} vs 21 EMA {hit.ema_21:.2f}. "
        f"Runup low {hit.runup_low:.2f} on {hit.runup_low_date}. "
        f"Runup high {hit.runup_high:.2f} on {hit.runup_high_date}."
    )


def build_htf_runup_watchlist(hits: list[HtfRunupHit]) -> list[dict[str, object]]:
    watchlist: list[dict[str, object]] = []
    for hit in hits:
        summary = (
            f"{hit.runup_pct:.1f}% runup in the last {hit.runup_window_days} sessions. "
            f"Current close is above 21 EMA {hit.ema_21:.2f}. "
            f"Current close is {hit.pullback_from_high_pct:.1f}% below the runup high. "
            f"Monitor for a future HTF setup rather than treating this as a direct entry."
        )
        watchlist.append(
            {
                "ticker": hit.ticker,
                "sector": getattr(hit, "sector", None),
                "setup_label": "HTF 8W 100% Runup",
                "summary": summary,
                "master_note": _format_note(hit),
                "event_date": hit.runup_high_date,
                "event_label": "8W runup high",
                "trigger_label": "Runup high",
                "trigger_price": round(hit.runup_high, 4),
                "entry_style": "weekly_monitor_only",
                "entry_price": round(hit.current_price, 4),
                "entry_label": "Current close",
                "entry_timeframe": "weekly",
                "stop_price": round(hit.runup_low, 4),
                "stop_label": "8W runup low",
                "stop_timeframe": "weekly",
            }
        )
    return watchlist
