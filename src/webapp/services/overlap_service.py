from __future__ import annotations

from pathlib import Path
from typing import Any

from src.overlap_summary import build_overlap_payload, discover_supported_dates

from ..repositories.overlap_repository import OverlapRepository


class OverlapService:
    def __init__(self, artifacts_dir: Path) -> None:
        self.artifacts_dir = artifacts_dir
        self.watchlist_dir = artifacts_dir / "watchlists"
        self.repository = OverlapRepository(artifacts_dir=artifacts_dir)

    def get_latest_summary(self) -> dict[str, Any]:
        payload = self.repository.load_latest()
        if payload:
            return self._normalize(payload)
        fallback_date = self._latest_watchlist_date()
        if fallback_date:
            return self._normalize(build_overlap_payload(fallback_date, self.watchlist_dir))
        return self._empty_summary("")

    def get_summary(self, date_label: str) -> dict[str, Any]:
        payload = self.repository.load_by_date_label(date_label)
        if payload:
            return self._normalize(payload)
        if date_label and date_label in discover_supported_dates(self.watchlist_dir):
            return self._normalize(build_overlap_payload(date_label, self.watchlist_dir))
        return self._empty_summary(date_label)

    def _normalize(self, payload: dict[str, Any]) -> dict[str, Any]:
        overlap_two_plus = list(payload.get("overlap_two_plus", []))
        overlap_two_plus.sort(key=lambda item: (-int(item.get("pipeline_count") or 0), str(item.get("ticker") or "")))
        overlap_three_plus = payload.get("overlap_three_plus", [])
        pipeline_status = payload.get("pipeline_status", [])
        return {
            "date_label": str(payload.get("date_label", "")),
            "unique_ticker_count": int(payload.get("unique_ticker_count", 0) or 0),
            "overlap_two_plus_count": len(overlap_two_plus),
            "overlap_three_plus_count": len(overlap_three_plus),
            "overlap_two_plus": overlap_two_plus,
            "pipeline_status": pipeline_status,
            "pipeline_tickers": payload.get("pipeline_tickers", {}),
            "fearzone_tickers": payload.get("fearzone_tickers", []),
        }

    def _latest_watchlist_date(self) -> str:
        dates = discover_supported_dates(self.watchlist_dir)
        return dates[0] if dates else ""

    def _empty_summary(self, date_label: str) -> dict[str, Any]:
        return {
            "date_label": date_label,
            "unique_ticker_count": 0,
            "overlap_two_plus_count": 0,
            "overlap_three_plus_count": 0,
            "overlap_two_plus": [],
            "pipeline_status": [],
            "pipeline_tickers": {},
            "fearzone_tickers": [],
        }
