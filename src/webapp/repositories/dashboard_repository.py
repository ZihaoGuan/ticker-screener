from __future__ import annotations

from pathlib import Path
from typing import Any


class DashboardRepository:
    def __init__(self, database_url: str, artifacts_dir: Path) -> None:
        self.database_url = database_url.strip()
        self.artifacts_dir = artifacts_dir

    def get_overview(self) -> dict[str, Any]:
        return {
            "database_configured": bool(self.database_url),
            "artifacts_dir": str(self.artifacts_dir),
            "latest_sync_at": None,
            "screen_run_count": None,
            "backtest_run_count": None,
        }
