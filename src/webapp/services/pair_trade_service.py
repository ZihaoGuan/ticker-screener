from __future__ import annotations

from pathlib import Path
from typing import Any

from ..repositories.pair_trade_repository import PairTradeRepository


class PairTradeService:
    def __init__(self, *, artifacts_dir: Path) -> None:
        self.repository = PairTradeRepository(artifacts_dir=artifacts_dir)

    def list_reports(self, *, limit: int = 100) -> list[dict[str, Any]]:
        return self.repository.list_reports(limit=limit)

    def get_report(self, stem: str) -> dict[str, Any]:
        payload = self.repository.get_report(stem)
        themes = payload.get("pairs") if isinstance(payload.get("pairs"), list) else []
        payload["pairs"] = [item for item in themes if isinstance(item, dict)]
        return payload
