from __future__ import annotations

from .sepa_vcp_screen import SepaVcpHit
from .sepa_vcp_watchlist_builder import build_sepa_vcp_watchlist


def build_weekly_sepa_vcp_watchlist(hits: list[SepaVcpHit]) -> list[dict[str, object]]:
    watchlist = build_sepa_vcp_watchlist(hits)
    for entry in watchlist:
        entry["setup_label"] = "Weekly SEPA"
        entry["summary"] = f"Weekly timeframe. {entry['summary']}"
        entry["entry_timeframe"] = "weekly"
        entry["stop_timeframe"] = "weekly"
        entry["entry_style"] = "weekly_sepa_trend"
    return watchlist
