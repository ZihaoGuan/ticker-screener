from __future__ import annotations

from .inside_dryup_screen import InsideDryupHit


def _format_note(hit: InsideDryupHit) -> str:
    return (
        f"{' | '.join(hit.reasons)}. "
        f"Trigger {hit.trigger_price:.2f}, stop {hit.stop_price:.2f}. "
        f"EMA21 {hit.ema21:.2f}, EMA55 {hit.ema55:.2f}, EMA144 {hit.ema144:.2f}. "
        f"Quality {hit.quality_score}."
    )


def build_inside_dryup_watchlist(hits: list[InsideDryupHit]) -> list[dict[str, object]]:
    watchlist: list[dict[str, object]] = []
    for hit in hits:
        summary = (
            f"Inside-day dry-up setup after a {hit.pullback_bars}-bar pullback. "
            f"Average pullback volume is {hit.avg_pullback_volume_ratio:.2f}x of 50D average, "
            f"latest volume is {hit.latest_volume_ratio:.2f}x, and pullback depth is {hit.pullback_depth_pct * 100.0:.1f}%. "
            f"Watch the inside-day high {hit.trigger_price:.2f} as the continuation trigger."
        )
        watchlist.append(
            {
                "ticker": hit.ticker,
                "sector": hit.sector,
                "industry": hit.industry,
                "setup_label": "Inside Day Dry-Up",
                "summary": summary,
                "master_note": _format_note(hit),
                "event_date": hit.signal_date,
                "event_label": "Inside day",
                "trigger_label": "Inside-day high",
                "trigger_price": round(hit.trigger_price, 4),
                "entry_style": "inside_day_breakout",
                "entry_price": round(hit.trigger_price, 4),
                "entry_label": "Trigger",
                "entry_timeframe": "daily",
                "secondary_entry_price": round(hit.ema21, 4),
                "secondary_entry_label": "21 EMA",
                "secondary_entry_timeframe": "daily",
                "stop_price": round(hit.stop_price, 4),
                "stop_label": "Inside-day low",
                "stop_timeframe": "daily",
                "quality_score": hit.quality_score,
            }
        )
    return watchlist
