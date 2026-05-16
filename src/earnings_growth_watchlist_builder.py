from __future__ import annotations

from .earnings_growth_screen import EarningsGrowthHit


def _format_note(hit: EarningsGrowthHit) -> str:
    move_text = ", ".join(f"{value:.1f}%" for value in hit.historical_earnings_moves_pct[:4])
    parts = [
        f"Revenue YoY {hit.revenue_yoy_pct:.1f}%",
        f"Latest quarter revenue {hit.latest_quarter_revenue / 1_000_000:.1f}M",
        f"Latest EPS {hit.latest_eps_actual:.2f}",
        f"EPS trend {' > '.join(f'{value:.2f}' for value in hit.eps_series[:hit.eps_improving_quarters])}",
        f"Institutional ownership {hit.institutional_ownership_pct:.1f}%",
        f"Earnings reactions {move_text}",
    ]
    if hit.next_earnings_session == "before_market":
        parts.append("Next earnings session BMO")
    elif hit.next_earnings_session == "after_market":
        parts.append("Next earnings session AMC")
    return ". ".join(parts) + "."


def build_earnings_growth_watchlist(hits: list[EarningsGrowthHit]) -> list[dict[str, object]]:
    watchlist: list[dict[str, object]] = []
    for hit in hits:
        summary = (
            f"Next-week earnings growth setup. Revenue YoY {hit.revenue_yoy_pct:.1f}%. "
            f"Median post-earnings move {hit.median_post_earnings_move_pct:.1f}%. "
            f"Inst ownership {hit.institutional_ownership_pct:.1f}%."
        )
        if hit.earnings_date:
            summary = f"Earnings {hit.earnings_date}. " + summary
        watchlist.append(
            {
                "ticker": hit.ticker,
                "setup_label": "Next-week earnings growth",
                "summary": summary,
                "master_note": _format_note(hit),
                "trigger_label": "Current close",
                "trigger_price": round(hit.current_price, 4),
                "entry_style": "earnings_growth",
                "entry_price": round(hit.current_price, 4),
                "entry_label": "Current close",
                "entry_timeframe": "daily",
            }
        )
    return watchlist
