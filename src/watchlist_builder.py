from __future__ import annotations

from .rs_screen import ScreenHit


def _format_note(hit: ScreenHit) -> str:
    return (
        f"{' | '.join(hit.reasons)}. "
        f"RS line {hit.current_rs_line:.6f} vs {hit.benchmark_ticker}. "
        f"Daily lookback {hit.daily_lookback_days} sessions, "
        f"weekly lookback {hit.weekly_lookback_weeks} weeks."
    )


def _signal_tags(hit: ScreenHit) -> list[str]:
    tags: list[str] = []
    if hit.recent_golden_cross:
        tags.append("Recent Golden Cross")
    if hit.recent_inside_day:
        tags.append("Recent Inside Day")
    return tags


def build_watchlist(hits: list[ScreenHit], *, signal_profile: str = "daily") -> list[dict[str, object]]:
    watchlist: list[dict[str, object]] = []
    for hit in hits:
        weekly_status = "True" if hit.weekly_rs_new_high_before_price else "False"
        signal_tags = _signal_tags(hit)
        if signal_profile == "weekly":
            recency_text = (
                f"{hit.weekly_signal_weeks_ago} week(s) ago"
                if hit.weekly_signal_weeks_ago is not None
                else f"within {hit.weekly_recent_signal_weeks} weeks"
            )
            setup_label = "Weekly RS new high"
            summary = (
                f"Signal on {hit.signal_date}. "
                f"Weekly RS NH: {hit.weekly_rs_new_high}. "
                f"Recent weekly RS NH: {hit.weekly_rs_new_high_recent}. "
                f"Last weekly signal: {recency_text}. "
                f"Weekly RS NH before price: {weekly_status}. "
                f"Recent golden cross ({hit.recent_golden_cross_days}D): {hit.recent_golden_cross}. "
                f"Recent inside day ({hit.recent_inside_day_days}D): {hit.recent_inside_day}. "
                f"Distance from year high: {hit.distance_from_year_high_pct:.1f}%."
            )
        else:
            setup_label = "RS higher before price"
            summary = (
                f"Signal on {hit.signal_date}. "
                f"Daily RS NH before price: {hit.daily_rs_new_high_before_price}. "
                f"Weekly RS NH before price: {weekly_status}. "
                f"Recent golden cross ({hit.recent_golden_cross_days}D): {hit.recent_golden_cross}. "
                f"Recent inside day ({hit.recent_inside_day_days}D): {hit.recent_inside_day}. "
                f"Distance from year high: {hit.distance_from_year_high_pct:.1f}%."
            )
        watchlist.append(
            {
                "ticker": hit.ticker,
                "sector": getattr(hit, "sector", None),
                "setup_label": setup_label,
                "summary": summary,
                "master_note": _format_note(hit),
                "signal_tags": signal_tags,
                "trigger_label": "Signal close",
                "trigger_price": round(hit.current_price, 4),
                "entry_style": "signal_day",
                "entry_price": round(hit.current_price, 4),
                "entry_label": "Signal close",
                "entry_timeframe": "daily",
            }
        )
    return watchlist
