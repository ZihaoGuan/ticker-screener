from __future__ import annotations

from .rs_screen import ScreenHit


def _format_note(hit: ScreenHit) -> str:
    return (
        f"{' | '.join(hit.reasons)}. "
        f"RS line {hit.current_rs_line:.6f} vs {hit.benchmark_ticker}. "
        f"Daily lookback {hit.daily_lookback_days} sessions, "
        f"weekly lookback {hit.weekly_lookback_weeks} weeks."
    )


def build_watchlist(hits: list[ScreenHit]) -> list[dict[str, object]]:
    watchlist: list[dict[str, object]] = []
    for hit in hits:
        weekly_status = "True" if hit.weekly_rs_new_high_before_price else "False"
        watchlist.append(
            {
                "ticker": hit.ticker,
                "setup_label": "RS higher before price",
                "summary": (
                    f"Signal on {hit.signal_date}. "
                    f"Daily RS NH before price: {hit.daily_rs_new_high_before_price}. "
                    f"Weekly RS NH before price: {weekly_status}. "
                    f"Distance from year high: {hit.distance_from_year_high_pct:.1f}%."
                ),
                "master_note": _format_note(hit),
                "trigger_label": "Signal close",
                "trigger_price": round(hit.current_price, 4),
                "entry_style": "signal_day",
                "entry_price": round(hit.current_price, 4),
                "entry_label": "Signal close",
                "entry_timeframe": "daily",
            }
        )
    return watchlist
