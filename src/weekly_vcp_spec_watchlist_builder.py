from __future__ import annotations

from .vcp_spec_screen import VcpSpecHit
from .vcp_spec_watchlist_builder import build_vcp_spec_watchlist


def build_weekly_vcp_spec_watchlist(hits: list[VcpSpecHit]) -> list[dict[str, object]]:
    watchlist = build_vcp_spec_watchlist(hits)
    for entry in watchlist:
        entry["setup_label"] = "Weekly VCP Spec"
        entry["summary"] = f"Weekly timeframe. {entry['summary']}"
        entry["entry_timeframe"] = "weekly"
        entry["stop_timeframe"] = "weekly"
        entry["entry_style"] = f"weekly_{entry.get('entry_style') or 'vcp_spec'}"
    return watchlist
