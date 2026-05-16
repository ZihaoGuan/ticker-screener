from __future__ import annotations

from .vcp_screen import VcpHit


def _format_note(hit: VcpHit) -> str:
    note_parts = [
        f"{hit.vcp_contractions_count} contractions",
        f"Pivot {hit.pivot_price:.2f}",
        f"Support {hit.support_price:.2f}",
        f"Breakout volume {hit.breakout_day_volume:.0f} vs {hit.breakout_avg_volume_50:.0f} avg",
    ]
    if hit.distance_from_year_high_pct is not None:
        note_parts.append(f"{hit.distance_from_year_high_pct * 100:.1f}% from year high")
    if hit.sector_etf:
        note_parts.append(f"Sector ETF {hit.sector_etf}")
    return ". ".join(note_parts) + "."


def build_vcp_watchlist(hits: list[VcpHit]) -> list[dict[str, object]]:
    watchlist: list[dict[str, object]] = []
    for hit in hits:
        summary = (
            f"VCP setup with {hit.vcp_contractions_count} contractions. "
            f"Pivot {hit.pivot_price:.2f}, support {hit.support_price:.2f}. "
            f"Demand dry: {'yes' if hit.is_demand_dry else 'no'}. "
            f"Breakout volume confirmed: {'yes' if hit.is_breakout_volume_confirmed else 'no'}."
        )
        if hit.distance_from_year_high_pct is not None:
            summary += f" Year-high distance {hit.distance_from_year_high_pct * 100:.1f}%."

        watchlist.append(
            {
                "ticker": hit.ticker,
                "setup_label": "VCP",
                "summary": summary,
                "master_note": _format_note(hit),
                "trigger_label": "Pivot breakout",
                "trigger_price": round(hit.pivot_price, 4),
                "entry_style": "pivot_breakout",
                "entry_price": round(hit.pivot_price, 4),
                "entry_label": "Pivot",
                "entry_timeframe": "daily",
                "stop_price": round(hit.support_price, 4),
                "stop_label": "Support",
                "stop_timeframe": "daily",
            }
        )
    return watchlist
