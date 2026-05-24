from __future__ import annotations

from .gap_fill_screen import GapFillHit


def _format_note(hit: GapFillHit) -> str:
    return (
        f"{' | '.join(hit.reasons)}. "
        f"Current close {hit.current_price:.2f}. "
        f"Gap entry {hit.gap_bottom:.2f}, gap fill target {hit.gap_top:.2f}. "
        f"Avg vol20 {hit.avg_volume_20:,.0f}, avg $ vol20 {hit.avg_dollar_volume_20:,.0f}."
    )


def build_gap_fill_watchlist(hits: list[GapFillHit]) -> list[dict[str, object]]:
    watchlist: list[dict[str, object]] = []
    for hit in hits:
        summary = (
            f"Open overhead gap from {hit.gap_date}. "
            f"Gap size {hit.gap_size_pct:.1f}%. "
            f"Current close is {hit.distance_to_gap_bottom_pct:+.1f}% vs gap entry and "
            f"{hit.distance_to_gap_top_pct:+.1f}% below the fill target. "
            f"Above 21 EMA {hit.ema_21:.2f} and 50 EMA {hit.ema_50:.2f}. "
            f"{'Inside day present. ' if hit.inside_day else ''}"
            f"{'Already trading inside the gap.' if hit.gap_reclaimed else 'Still approaching the gap entry.'}"
        )
        watchlist.append(
            {
                "ticker": hit.ticker,
                "sector": getattr(hit, "sector", None),
                "setup_label": "Potential Overhead Gap Fill",
                "summary": summary,
                "master_note": _format_note(hit),
                "event_date": hit.gap_date,
                "event_label": "Gap",
                "trigger_label": "Gap fill target",
                "trigger_price": round(hit.gap_top, 4),
                "entry_style": "gap_reclaim",
                "entry_price": round(hit.gap_bottom, 4),
                "entry_label": "Gap entry" if not hit.gap_reclaimed else "Trading inside gap",
                "entry_timeframe": "daily",
                "secondary_entry_price": round(hit.ema_21, 4),
                "secondary_entry_label": "21 EMA",
                "secondary_entry_timeframe": "daily",
                "stop_price": round(hit.recent_low, 4),
                "stop_label": "Recent range low",
                "stop_timeframe": "daily",
            }
        )
    return watchlist
