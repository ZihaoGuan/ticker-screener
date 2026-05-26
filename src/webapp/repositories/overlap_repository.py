from __future__ import annotations

import json
from pathlib import Path
from typing import Any


class OverlapRepository:
    def __init__(self, artifacts_dir: Path) -> None:
        self.raw_dir = artifacts_dir / "raw"

    def list_available(self) -> list[Path]:
        if not self.raw_dir.exists():
            return []
        return sorted(self.raw_dir.glob("daily_overlap_summary_*.json"), reverse=True)

    def load_latest(self) -> dict[str, Any] | None:
        for path in self.list_available():
            payload = self._load_json(path)
            if payload:
                return payload
        return None

    def load_by_date_label(self, date_label: str) -> dict[str, Any] | None:
        path = self.raw_dir / f"daily_overlap_summary_{date_label}.json"
        return self._load_json(path)

    def _load_json(self, path: Path) -> dict[str, Any] | None:
        if not path.exists():
            return None
        payload = json.loads(path.read_text(encoding="utf-8"))
        if isinstance(payload, dict):
            return payload
        return None
