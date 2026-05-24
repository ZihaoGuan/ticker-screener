from __future__ import annotations

from .pre_earnings_ma_stack_screen import PreEarningsMaStackHit


def _format_note(hit: PreEarningsMaStackHit) -> str:
    return (
        f"{' | '.join(hit.reasons)}. "
        f"Close {hit.current_price:.2f}. "
        f"Avg vol20 {hit.avg_volume_20:,.0f}, avg $ vol20 {hit.avg_dollar_volume_20:,.0f}."
    )


def build_pre_earnings_ma_stack_watchlist(hits: list[PreEarningsMaStackHit]) -> list[dict[str, object]]:
    watchlist: list[dict[str, object]] = []
    for hit in hits:
        summary = (
            f"Earnings {hit.earnings_date or 'next week'}. "
            f"MA20 {hit.ma20:.2f} > MA50 {hit.ma50:.2f} > MA200 {hit.ma200:.2f}. "
            f"Close is {hit.distance_from_ma20_pct:+.1f}% vs MA20 and "
            f"{hit.distance_from_year_high_pct:+.1f}% below the 52-week high."
        )
        watchlist.append(
            {
                "ticker": hit.ticker,
                "sector": getattr(hit, "sector", None),
                "setup_label": "Pre-Earnings MA Stack",
                "summary": summary,
                "master_note": _format_note(hit),
                "event_date": hit.earnings_date,
                "event_label": "Earnings",
                "trigger_label": "Current close",
                "trigger_price": round(hit.current_price, 4),
                "entry_style": "pre_earnings_focus",
                "entry_price": round(hit.current_price, 4),
                "entry_label": "Current close",
                "entry_timeframe": "daily",
                "secondary_entry_price": round(hit.ma20, 4),
                "secondary_entry_label": "MA20",
                "secondary_entry_timeframe": "daily",
                "stop_price": round(hit.ma50, 4),
                "stop_label": "MA50",
                "stop_timeframe": "daily",
            }
        )
    return watchlist
