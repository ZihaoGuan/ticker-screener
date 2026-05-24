from __future__ import annotations

from .weekly_htf_pullback_screen import WeeklyHtfPullbackHit


def _format_note(hit: WeeklyHtfPullbackHit) -> str:
    return (
        f"{' | '.join(hit.reasons)}. "
        f"HTF score {hit.htf_score:.1f} ({hit.htf_grade}). "
        f"Runup {hit.htf_runup_pct:.1f}% with pullback {hit.htf_pullback_from_high_pct:.1f}%. "
        f"RS line {hit.current_rs_line:.6f} vs {hit.benchmark_ticker}."
    )


def build_weekly_htf_pullback_watchlist(
    hits: list[WeeklyHtfPullbackHit],
    *,
    ema8_breach_tolerance_pct: float,
) -> list[dict[str, object]]:
    watchlist: list[dict[str, object]] = []
    for hit in hits:
        recency_text = (
            f"{hit.weekly_signal_weeks_ago} week(s) ago"
            if hit.weekly_signal_weeks_ago is not None
            else f"within {hit.weekly_recent_signal_weeks} weeks"
        )
        summary = (
            f"Recent weekly RS signal: {recency_text}. "
            f"HTF grade {hit.htf_grade} with {hit.htf_runup_pct:.1f}% runup and "
            f"{hit.htf_pullback_from_high_pct:.1f}% pullback. "
            f"Current close is {hit.weekly_ema8_distance_pct:+.1f}% vs 8-week EMA {hit.weekly_ema8:.2f}. "
            f"Distance from year high: {hit.distance_from_year_high_pct:.1f}%."
        )
        stop_price = hit.weekly_ema8 * (1.0 - max(ema8_breach_tolerance_pct, 0.005) * 1.5)
        watchlist.append(
            {
                "ticker": hit.ticker,
                "sector": getattr(hit, "sector", None),
                "setup_label": "Weekly RS + HTF 8W Pullback",
                "summary": summary,
                "master_note": _format_note(hit),
                "trigger_label": "8W EMA reclaim",
                "trigger_price": round(hit.weekly_ema8, 4),
                "entry_style": "weekly_pullback_reclaim",
                "entry_price": round(hit.current_price, 4),
                "entry_label": "Current close",
                "entry_timeframe": "weekly",
                "stop_price": round(stop_price, 4),
                "stop_label": "8W EMA fail",
                "stop_timeframe": "weekly",
            }
        )
    return watchlist
