from __future__ import annotations

from pathlib import Path
from typing import Any

from ..repositories.overlap_repository import OverlapRepository


class OverlapService:
    def __init__(self, artifacts_dir: Path) -> None:
        self.repository = OverlapRepository(artifacts_dir=artifacts_dir)

    def get_latest_summary(self) -> dict[str, Any]:
        payload = self.repository.load_latest()
        if not payload:
            return {
                "date_label": "",
                "unique_ticker_count": 0,
                "overlap_two_plus_count": 0,
                "overlap_three_plus_count": 0,
                "overlap_two_plus": [],
                "pipeline_status": [],
            }
        return self._normalize(payload)

    def get_summary(self, date_label: str) -> dict[str, Any]:
        payload = self.repository.load_by_date_label(date_label)
        if not payload:
            return {
                "date_label": date_label,
                "unique_ticker_count": 0,
                "overlap_two_plus_count": 0,
                "overlap_three_plus_count": 0,
                "overlap_two_plus": [],
                "pipeline_status": [],
            }
        return self._normalize(payload)

    def _normalize(self, payload: dict[str, Any]) -> dict[str, Any]:
        overlap_two_plus = payload.get("overlap_two_plus", [])
        overlap_three_plus = payload.get("overlap_three_plus", [])
        pipeline_status = payload.get("pipeline_status", [])
        return {
            "date_label": str(payload.get("date_label", "")),
            "unique_ticker_count": int(payload.get("unique_ticker_count", 0) or 0),
            "overlap_two_plus_count": len(overlap_two_plus),
            "overlap_three_plus_count": len(overlap_three_plus),
            "overlap_two_plus": overlap_two_plus,
            "pipeline_status": pipeline_status,
        }
