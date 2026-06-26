from __future__ import annotations

import json
from pathlib import Path
from typing import Any


class PairTradeRepository:
    def __init__(self, *, artifacts_dir: Path) -> None:
        self.artifacts_dir = artifacts_dir

    def list_reports(self, *, limit: int = 100) -> list[dict[str, Any]]:
        candidates = sorted(
            [
                path
                for path in self.artifacts_dir.rglob("pair_trade_screener_*.json")
                if path.is_file()
            ],
            key=lambda path: (path.stat().st_mtime, str(path)),
            reverse=True,
        )
        results: list[dict[str, Any]] = []
        for path in candidates[: max(1, int(limit))]:
            payload = self._load_payload(path)
            if not isinstance(payload, dict):
                continue
            metadata = payload.get("metadata") if isinstance(payload.get("metadata"), dict) else {}
            summary = payload.get("summary") if isinstance(payload.get("summary"), dict) else {}
            results.append(
                {
                    "stem": path.stem,
                    "captured_at": str(payload.get("generated_at") or metadata.get("generated_at") or ""),
                    "date_label": str(metadata.get("date_label") or path.parent.name),
                    "as_of_date": str(metadata.get("as_of_date") or ""),
                    "group_mode": str(metadata.get("group_mode") or ""),
                    "included_groups": list(metadata.get("included_groups") or []) if isinstance(metadata.get("included_groups"), list) else [],
                    "universe_size": _coerce_int(summary.get("universe_size")),
                    "pairs_analyzed": _coerce_int(summary.get("pairs_analyzed")),
                    "cointegrated_pairs": _coerce_int(summary.get("cointegrated_pairs")),
                    "actionable_pairs": _coerce_int(summary.get("actionable_pairs")),
                    "top_pair": str(summary.get("top_pair") or ""),
                }
            )
        return results

    def get_report(self, stem: str) -> dict[str, Any]:
        normalized = str(stem or "").strip()
        if not normalized:
            raise ValueError("Report stem is required.")
        for path in self.artifacts_dir.rglob(f"{normalized}.json"):
            if not path.is_file():
                continue
            payload = self._load_payload(path)
            if isinstance(payload, dict):
                return payload
        raise ValueError(f"Unknown pair trade report: {normalized}")

    def _load_payload(self, path: Path) -> dict[str, Any] | None:
        try:
            parsed = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return None
        return parsed if isinstance(parsed, dict) else None


def _coerce_int(value: Any) -> int | None:
    if value in (None, ""):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None
