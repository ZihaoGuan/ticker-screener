from __future__ import annotations

from .near_200ma_screen import Near200MaHit


def _format_note(hit: Near200MaHit) -> str:
    return (
        f"{' | '.join(hit.reasons)}. "
        f"Current close {hit.current_price:.2f}. "
        f"MA20 {hit.ma20:.2f}, MA50 {hit.ma50:.2f}, MA200 {hit.ma200:.2f}. "
        f"Avg vol20 {hit.avg_volume_20:,.0f}, avg $ vol20 {hit.avg_dollar_volume_20:,.0f}."
    )


def build_near_200ma_watchlist(hits: list[Near200MaHit]) -> list[dict[str, object]]:
    watchlist: list[dict[str, object]] = []
    for hit in hits:
        if hit.case_group == "bull":
            setup_label = "Near 200D MA - Bull Case"
            summary = (
                f"Trading {abs(hit.distance_to_ma200_pct):.1f}% below 200D MA {hit.ma200:.2f}. "
                f"Price is still above 20D MA {hit.ma20:.2f} and 50D MA {hit.ma50:.2f}, "
                f"so the short and medium moving averages are supporting a possible reclaim of 200D resistance."
            )
            trigger_label = "200D reclaim"
            stop_label = "Recent support low"
        else:
            setup_label = "Near 200D MA - Bear Case"
            summary = (
                f"Trading {hit.distance_to_ma200_pct:.1f}% above 200D MA {hit.ma200:.2f}. "
                f"Price is below 20D MA {hit.ma20:.2f} and 50D MA {hit.ma50:.2f}, "
                f"so the short and medium moving averages are pressing price down toward a possible 200D break."
            )
            trigger_label = "200D breakdown"
            stop_label = "Recent squeeze high"
        watchlist.append(
            {
                "ticker": hit.ticker,
                "sector": getattr(hit, "sector", None),
                "setup_label": setup_label,
                "summary": summary,
                "master_note": _format_note(hit),
                "event_date": hit.benchmark_ticker,
                "event_label": "MA200",
                "trigger_label": trigger_label,
                "trigger_price": round(hit.ma200, 4),
                "entry_style": "ma200_inflection",
                "entry_price": round(hit.current_price, 4),
                "entry_label": "Current close",
                "entry_timeframe": "daily",
                "secondary_entry_price": round(hit.ma20, 4),
                "secondary_entry_label": "20D MA",
                "secondary_entry_timeframe": "daily",
                "stop_price": round(hit.recent_low if hit.case_group == "bull" else hit.recent_high, 4),
                "stop_label": stop_label,
                "stop_timeframe": "daily",
                "case_group": hit.case_group,
                "industry": hit.industry,
            }
        )
    return watchlist
