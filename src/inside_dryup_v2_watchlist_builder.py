from __future__ import annotations

from .inside_dryup_v2_screen import InsideDryupV2Hit


def _format_note(hit: InsideDryupV2Hit) -> str:
    return (
        f"{' | '.join(hit.reasons)}. "
        f"Inside-day high {hit.inside_day_high:.2f}, low {hit.inside_day_low:.2f}. "
        f"PxV {hit.price_volume_ratio:.2f}x of 20D average."
    )


def build_inside_dryup_v2_watchlist(hits: list[InsideDryupV2Hit]) -> list[dict[str, object]]:
    watchlist: list[dict[str, object]] = []
    for hit in hits:
        summary = (
            f"Inside day plus extreme price-volume dry-up. "
            f"Current PxV is {hit.price_volume_ratio:.2f}x of 20D average with a {hit.dry_count}-bar dry streak. "
            f"No breakout confirmation required for this v2 scan."
        )
        watchlist.append(
            {
                "ticker": hit.ticker,
                "sector": hit.sector,
                "industry": hit.industry,
                "setup_label": "Inside Day + Extreme Dry-Up",
                "summary": summary,
                "master_note": _format_note(hit),
                "event_date": hit.signal_date,
                "event_label": "Inside day",
                "trigger_label": "Inside-day high",
                "trigger_price": round(hit.inside_day_high, 4),
                "entry_style": "inside_day_extreme_dry_up",
                "entry_price": round(hit.current_price, 4),
                "entry_label": "Current close",
                "entry_timeframe": "daily",
                "stop_price": round(hit.inside_day_low, 4),
                "stop_label": "Inside-day low",
                "stop_timeframe": "daily",
                "price_volume_ratio": round(hit.price_volume_ratio, 4),
                "dry_count": hit.dry_count,
                "prior_runup_pct": round(hit.prior_runup_pct, 2),
            }
        )
    return watchlist
