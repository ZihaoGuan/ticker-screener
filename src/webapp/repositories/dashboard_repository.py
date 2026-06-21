from __future__ import annotations

from pathlib import Path
from typing import Any

from .history_repository import HistoryRepository


class DashboardRepository:
    def __init__(self, database_url: str, artifacts_dir: Path) -> None:
        self.database_url = database_url.strip()
        self.artifacts_dir = artifacts_dir
        self.history_repository = HistoryRepository(database_url=self.database_url, artifacts_dir=artifacts_dir)

    def get_overview(self) -> dict[str, Any]:
        return {
            "database_configured": bool(self.database_url),
            "artifacts_dir": str(self.artifacts_dir),
            "latest_sync_at": None,
            "screen_run_count": None,
        }

    def get_latest_screen_run_summary(self, *, strategy_id: str) -> dict[str, Any] | None:
        rows = self.history_repository.list_screen_runs(strategy_id=strategy_id, limit=1)
        if not rows:
            return None
        payload = rows[0].get("result_summary_json")
        return payload if isinstance(payload, dict) else None
