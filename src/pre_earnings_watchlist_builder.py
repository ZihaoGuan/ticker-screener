from __future__ import annotations

from .pre_earnings_screen import PreEarningsHit


def _format_note(hit: PreEarningsHit) -> str:
    parts = []
    if hit.focus_reasons:
        parts.append(hit.focus_reasons)
    parts.append(
        f"Score {hit.focus_score:.1f} ({hit.focus_grade})"
    )
    parts.append(
        f"RS line {hit.current_rs_line:.4f} vs {hit.benchmark_ticker}"
    )
    if hit.recent_range_pct is not None:
        parts.append(f"Recent range {hit.recent_range_pct:.2f}%")
    if hit.distribution_warning:
        parts.append(f"Distribution warning ({hit.distribution_days_count} days)")
    return ". ".join(parts) + "."


def build_pre_earnings_watchlist(hits: list[PreEarningsHit]) -> list[dict[str, object]]:
    watchlist: list[dict[str, object]] = []
    for hit in hits:
        summary = (
            f"Pre-earnings focus score {hit.focus_score:.1f} ({hit.focus_grade}). "
            f"Trade plan {hit.trade_plan}. "
            f"Distance from year high {hit.distance_from_year_high_pct:.1f}%."
        )
        if hit.earnings_date:
            summary = f"Earnings {hit.earnings_date}. " + summary

        watchlist.append(
            {
                "ticker": hit.ticker,
                "setup_label": "Pre-earnings focus",
                "summary": summary,
                "master_note": _format_note(hit),
                "trigger_label": "Current close",
                "trigger_price": round(hit.current_price, 4),
                "entry_style": "pre_earnings_focus",
                "entry_price": round(hit.current_price, 4),
                "entry_label": "Current close",
                "entry_timeframe": "daily",
            }
        )
    return watchlist
