from __future__ import annotations

import json
from pathlib import Path
from typing import Any
import datetime as dt


class InsiderRepository:
    def __init__(self, artifacts_dir: Path) -> None:
        self.raw_dir = artifacts_dir / "raw"
        self.insider_dir = self.raw_dir / "insider"
        self.latest_path = self.insider_dir / "insider_trades_latest.json"

    def load_latest(self) -> dict[str, Any] | None:
        for path in self._candidate_paths():
            payload = self._load_json(path)
            if payload:
                return payload
        return None

    def load_cache_window(self, *, ticker: str, as_of_date: str, lookback_days: int) -> dict[str, Any] | None:
        payload = self.load_latest()
        if not payload:
            return None
        caches = payload.get("caches")
        if isinstance(caches, dict):
            window = caches.get(self._cache_key(ticker=ticker, as_of_date=as_of_date, lookback_days=lookback_days))
            return window if isinstance(window, dict) else None
        return None

    def save_cache_window(
        self,
        *,
        ticker: str,
        as_of_date: str,
        lookback_days: int,
        refreshed_at: str,
        entries: list[dict[str, Any]],
        requested_tickers: list[str] | None = None,
        source: str = "sec_form4_submissions",
    ) -> dict[str, Any]:
        payload = self.load_latest() or {}
        caches = payload.get("caches")
        normalized_caches = dict(caches) if isinstance(caches, dict) else {}
        normalized_caches[self._cache_key(ticker=ticker, as_of_date=as_of_date, lookback_days=lookback_days)] = {
            "ticker": ticker,
            "requested_tickers": requested_tickers or [ticker],
            "as_of_date": as_of_date,
            "lookback_days": int(lookback_days),
            "refreshed_at": refreshed_at,
            "entries": entries,
        }
        wrapped = {
            "generated_at": refreshed_at,
            "source": source,
            "caches": normalized_caches,
        }
        self.insider_dir.mkdir(parents=True, exist_ok=True)
        self.latest_path.write_text(json.dumps(wrapped, indent=2), encoding="utf-8")
        return wrapped

    def is_cache_window_fresh(self, window: dict[str, Any] | None, *, ttl_hours: int) -> bool:
        if not window:
            return False
        refreshed_at = str(window.get("refreshed_at") or "").strip()
        if not refreshed_at:
            return False
        try:
            refreshed = dt.datetime.fromisoformat(refreshed_at.replace("Z", "+00:00"))
        except ValueError:
            return False
        if refreshed.tzinfo is None:
            refreshed = refreshed.replace(tzinfo=dt.timezone.utc)
        age = dt.datetime.now(dt.timezone.utc) - refreshed.astimezone(dt.timezone.utc)
        return age <= dt.timedelta(hours=max(1, int(ttl_hours)))

    def _candidate_paths(self) -> list[Path]:
        candidates: list[Path] = []
        if self.latest_path.exists():
            candidates.append(self.latest_path)
        if self.insider_dir.exists():
            candidates.extend(sorted(self.insider_dir.glob("insider_trades_*.json"), reverse=True))
        return candidates

    def _load_json(self, path: Path) -> dict[str, Any] | None:
        if not path.exists():
            return None
        payload = json.loads(path.read_text(encoding="utf-8"))
        if isinstance(payload, dict):
            return payload
        return None

    def _cache_key(self, *, ticker: str, as_of_date: str, lookback_days: int) -> str:
        return f"{str(ticker).strip().upper()}|{str(as_of_date).strip()}|{int(lookback_days)}"
