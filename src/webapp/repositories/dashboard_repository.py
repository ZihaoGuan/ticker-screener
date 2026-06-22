from __future__ import annotations

import json
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

    def get_latest_screen_run_summary(self, *, strategy_id: str, preferred_ticker: str | None = None) -> dict[str, Any] | None:
        rows = self.history_repository.list_screen_runs(strategy_id=strategy_id, limit=10)
        if not rows:
            return None
        candidates: list[dict[str, Any]] = []
        for row in rows:
            payload = self._coerce_summary_payload(row.get("result_summary_json"))
            if isinstance(payload, dict):
                candidates.append(payload)
        if not candidates:
            return None
        normalized_preferred_ticker = str(preferred_ticker or "").strip().upper()
        if normalized_preferred_ticker:
            preferred_candidates = [
                payload
                for payload in candidates
                if str(payload.get("ticker") or "").strip().upper() == normalized_preferred_ticker
            ]
            if preferred_candidates:
                candidates = preferred_candidates
        return max(candidates, key=self._summary_completeness_score)

    def _coerce_summary_payload(self, payload: Any) -> dict[str, Any] | None:
        if isinstance(payload, dict):
            return payload
        if isinstance(payload, str):
            try:
                parsed = json.loads(payload)
            except json.JSONDecodeError:
                return None
            return parsed if isinstance(parsed, dict) else None
        return None

    def _summary_completeness_score(self, payload: dict[str, Any]) -> tuple[int, int]:
        populated_keys = 0
        for key in (
            "spot",
            "net_gex",
            "gamma_flip",
            "call_wall",
            "put_wall",
            "atm_pin_strike",
            "put_call_oi_ratio",
            "strike_count",
            "front_expiry",
            "summary",
            "methodology",
            "gex_label",
        ):
            if payload.get(key) not in (None, "", []):
                populated_keys += 1
        has_api_as_of = 1 if payload.get("api_as_of") not in (None, "") else 0
        return (populated_keys, has_api_as_of)
