from __future__ import annotations

from .ftd_sweep_screen import FtdSweepHit


def _format_note(hit: FtdSweepHit) -> str:
    return (
        f"{' | '.join(hit.reasons)}. "
        f"Current close {hit.current_price:.2f}. "
        f"FTD high {hit.ftd_high:.2f}, sweep low {hit.sweep_low:.2f}, FTD pivot low {hit.ftd_pivot_low:.2f}. "
        f"Avg vol20 {hit.avg_volume_20:,.0f}, avg $ vol20 {hit.avg_dollar_volume_20:,.0f}."
    )


def build_ftd_sweep_watchlist(hits: list[FtdSweepHit]) -> list[dict[str, object]]:
    watchlist: list[dict[str, object]] = []
    for hit in hits:
        summary = (
            f"FTD on {hit.ftd_date} followed by a sweep reclaim on {hit.sweep_breakout_date}. "
            f"Breakout is {hit.breakout_distance_pct:+.1f}% vs the FTD high after a {hit.sweep_depth_pct:.1f}% sweep. "
            f"Signal is {hit.bars_since_breakout} bar(s) old."
        )
        watchlist.append(
            {
                "ticker": hit.ticker,
                "sector": getattr(hit, "sector", None),
                "setup_label": "FTD Sweep Breakout",
                "summary": summary,
                "master_note": _format_note(hit),
                "event_date": hit.ftd_date,
                "event_label": "FTD",
                "trigger_label": "FTD high reclaim",
                "trigger_price": round(hit.ftd_high, 4),
                "entry_style": "ftd_sweep_reclaim",
                "entry_price": round(hit.breakout_level, 4),
                "entry_label": "Sweep breakout",
                "entry_timeframe": "daily",
                "secondary_entry_price": round(hit.sweep_low, 4),
                "secondary_entry_low": round(hit.sweep_low, 4),
                "secondary_entry_high": round(hit.ftd_high, 4),
                "secondary_entry_label": "Sweep reclaim zone",
                "secondary_entry_timeframe": "daily",
                "stop_price": round(hit.ftd_pivot_low, 4),
                "stop_label": "FTD pivot low",
                "stop_timeframe": "daily",
            }
        )
    return watchlist
