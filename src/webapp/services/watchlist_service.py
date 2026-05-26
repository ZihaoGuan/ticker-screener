from __future__ import annotations

from pathlib import Path
from typing import Any

from ..repositories.watchlist_repository import WatchlistRepository


class WatchlistService:
    def __init__(self, artifacts_dir: Path) -> None:
        self.repository = WatchlistRepository(artifacts_dir=artifacts_dir)

    def list_recent(self) -> list[dict[str, Any]]:
        return self.repository.list_recent_watchlists(limit=50)

    def get_watchlist_detail(self, stem: str) -> dict[str, Any]:
        entries = self.repository.load_watchlist(stem)
        return {
            "stem": stem,
            "entry_count": len(entries),
            "entries": entries[:200],
        }
