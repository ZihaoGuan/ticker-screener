from __future__ import annotations

import json
from pathlib import Path
from typing import Any


class WatchlistRepository:
    def __init__(self, artifacts_dir: Path) -> None:
        self.watchlist_dir = artifacts_dir / "watchlists"

    def list_recent_watchlists(self, limit: int = 20) -> list[dict[str, Any]]:
        if not self.watchlist_dir.exists():
            return []
        rows: list[dict[str, Any]] = []
        for path in sorted(self.watchlist_dir.glob("*.json"), reverse=True)[:limit]:
            rows.append(
                {
                    "name": path.name,
                    "stem": path.stem,
                    "path": str(path),
                }
            )
        return rows

    def load_watchlist(self, stem: str) -> list[dict[str, Any]]:
        path = self.watchlist_dir / f"{stem}.json"
        if not path.exists():
            return []
        payload = json.loads(path.read_text(encoding="utf-8"))
        if isinstance(payload, list):
            return [item for item in payload if isinstance(item, dict)]
        return []
