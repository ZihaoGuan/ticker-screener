from __future__ import annotations

from .vcp_screen import VcpHit
from .vcp_watchlist_builder import build_vcp_watchlist


def build_weekly_vcp_watchlist(hits: list[VcpHit]) -> list[dict[str, object]]:
    watchlist = build_vcp_watchlist(hits)
    for entry in watchlist:
        entry["setup_label"] = "Weekly VCP"
        entry["summary"] = f"Weekly timeframe. {entry['summary']}"
        entry["entry_timeframe"] = "weekly"
        entry["stop_timeframe"] = "weekly"
        entry["entry_style"] = "weekly_pivot_breakout"
    return watchlist
