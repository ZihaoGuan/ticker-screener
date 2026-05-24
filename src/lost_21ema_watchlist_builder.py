from __future__ import annotations

from .lost_21ema_screen import Lost21EmaHit


def _format_note(hit: Lost21EmaHit) -> str:
    return (
        f"{' | '.join(hit.reasons)}. "
        f"Current close {hit.current_price:.2f}. "
        f"EMA21 {hit.ema21:.2f}, 50D MA {hit.sma50:.2f}, 200D MA {hit.sma200:.2f}. "
        f"Avg vol20 {hit.avg_volume_20:,.0f}, avg $ vol20 {hit.avg_dollar_volume_20:,.0f}."
    )


def build_lost_21ema_watchlist(hits: list[Lost21EmaHit]) -> list[dict[str, object]]:
    watchlist: list[dict[str, object]] = []
    for hit in hits:
        if hit.support_state == "testing_50d_support":
            setup_label = "Lost 21 EMA -> 50D Test"
            summary = (
                f"Lost the 21 EMA {hit.days_since_lost_ema21} day(s) ago and is now only "
                f"{abs(hit.distance_to_sma50_pct):.1f}% from the 50D MA {hit.sma50:.2f}. "
                f"Still above the 50D, so this is a live support test that can turn into either a rebound or a deeper pullback."
            )
            trigger_label = "50D support test"
            stop_price = round(hit.sma50 * 0.985, 4)
            stop_label = "50D support fails"
        else:
            setup_label = "Lost 21 EMA -> 50D Breakdown Risk"
            summary = (
                f"Lost the 21 EMA {hit.days_since_lost_ema21} day(s) ago and has already slipped "
                f"{abs(hit.distance_to_sma50_pct):.1f}% below the 50D MA {hit.sma50:.2f}. "
                f"This is a deeper weakness check where a fast reclaim could stabilize the chart, but continued drift would confirm a broader pullback."
            )
            trigger_label = "50D reclaim watch"
            stop_price = round(hit.recent_low, 4)
            stop_label = "Recent swing low"

        watchlist.append(
            {
                "ticker": hit.ticker,
                "sector": hit.sector,
                "industry": hit.industry,
                "setup_label": setup_label,
                "summary": summary,
                "master_note": _format_note(hit),
                "event_date": hit.benchmark_ticker,
                "event_label": "EMA21",
                "trigger_label": trigger_label,
                "trigger_price": round(hit.sma50, 4),
                "entry_style": "ema21_to_50_watch",
                "entry_price": round(hit.current_price, 4),
                "entry_label": "Current close",
                "entry_timeframe": "daily",
                "secondary_entry_price": round(hit.ema21, 4),
                "secondary_entry_label": "Lost 21 EMA",
                "secondary_entry_timeframe": "daily",
                "stop_price": stop_price,
                "stop_label": stop_label,
                "stop_timeframe": "daily",
            }
        )
    return watchlist
