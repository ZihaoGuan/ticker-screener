from __future__ import annotations

from .vcp_v3_screen import VcpV3Hit
from .vcp_v3_watchlist_builder import build_vcp_v3_watchlist


def build_weekly_vcp_v3_watchlist(hits: list[VcpV3Hit]) -> list[dict[str, object]]:
    watchlist = build_vcp_v3_watchlist(hits)
    for entry in watchlist:
        entry["setup_label"] = "Weekly VCP v3"
        entry["summary"] = f"Weekly timeframe. {entry['summary']}"
        entry["entry_timeframe"] = "weekly"
        entry["stop_timeframe"] = "weekly"
        entry["entry_style"] = f"weekly_{entry.get('entry_style') or 'vcp_v3'}"
    return watchlist
