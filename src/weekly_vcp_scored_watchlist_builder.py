from __future__ import annotations

from .vcp_scored_screen import VcpScoredHit
from .vcp_scored_watchlist_builder import build_vcp_scored_watchlist


def build_weekly_vcp_scored_watchlist(hits: list[VcpScoredHit]) -> list[dict[str, object]]:
    watchlist = build_vcp_scored_watchlist(hits)
    for entry in watchlist:
        entry["setup_label"] = "Weekly VCP Scored"
        entry["summary"] = f"Weekly timeframe. {entry['summary']}"
        entry["entry_timeframe"] = "weekly"
        entry["stop_timeframe"] = "weekly"
        entry["entry_style"] = "weekly_pivot_breakout"
    return watchlist
