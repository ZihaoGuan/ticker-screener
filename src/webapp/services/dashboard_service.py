from __future__ import annotations

from pathlib import Path
from typing import Any

from ..repositories.dashboard_repository import DashboardRepository
from ..repositories.watchlist_repository import WatchlistRepository


class DashboardService:
    def __init__(self, database_url: str, artifacts_dir: Path) -> None:
        self.dashboard_repository = DashboardRepository(database_url=database_url, artifacts_dir=artifacts_dir)
        self.watchlist_repository = WatchlistRepository(artifacts_dir=artifacts_dir)

    def get_dashboard_context(self) -> dict[str, Any]:
        overview = self.dashboard_repository.get_overview()
        recent_watchlists = self.watchlist_repository.list_recent_watchlists(limit=8)
        return {
            "overview": overview,
            "recent_watchlists": recent_watchlists,
            "strategy_cards": [
                {"id": "rs", "label": "RS", "description": "Daily RS new high before price."},
                {"id": "vcp", "label": "VCP", "description": "Volatility contraction pattern scan."},
                {"id": "cup_handle", "label": "Cup and Handle", "description": "Breakout candidate scan."},
                {"id": "overlap", "label": "Overlap", "description": "Cross-strategy overlap summary."},
            ],
        }
