"""Disk-backed, process-safe cache for validated raw intraday provider responses."""
from __future__ import annotations

from contextlib import contextmanager
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import tempfile
import threading
from typing import Any, Callable, Iterator


def _canonical_symbol(value: str) -> str:
    parts = str(value or "").strip().upper().split(":", 1)
    if len(parts) != 2 or not all(parts):
        raise ValueError("requested_symbol_must_be_exchange_qualified")
    return f"{parts[0]}:{parts[1]}"


def _digest(value: Any) -> str:
    encoded = json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _resolved_feed(data: dict[str, Any]) -> str:
    metadata = data.get("metadata") or {}
    return str(data.get("resolved_symbol") or metadata.get("full_name") or metadata.get("pro_name") or "").strip().upper()


class RawIntradayCache:
    """Share successful raw payloads; caller still owns book manifests and provenance.

    The API keeps at most one decoded payload live per call. The cache itself is
    disk-backed, so memory does not grow with the number of ticker/date pairs.
    """

    def __init__(self, root: str | Path):
        self.root = Path(root)
        self.stats = {"hits": 0, "misses": 0, "fetch_failures": 0, "published": 0}
        self._stats_lock = threading.Lock()

    @staticmethod
    def identity(*, provider: str, requested_symbol: str, date: str, timeframe: str,
                 session: str = "regular", adjustment: str = "splits",
                 symbol_resolution_mode: str = "exchange_fallback") -> dict[str, str]:
        day = str(date)[:10]
        if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", day):
            raise ValueError("invalid_intraday_date")
        interval = str(timeframe).strip().lower()
        if not interval:
            raise ValueError("intraday_timeframe_required")
        return {"provider": str(provider).strip().lower(),
                "requested_symbol": _canonical_symbol(requested_symbol), "date": day,
                "timeframe": interval, "session": str(session).strip().lower(),
                "adjustment": str(adjustment).strip().lower(),
                "symbol_resolution_mode": str(symbol_resolution_mode).strip().lower()}

    def get_or_fetch(self, identity: dict[str, str], fetcher: Callable[[], Any], *,
                     expected_resolved_symbol: str | None = None,
                     validator: Callable[[dict[str, Any]], bool] | None = None) -> dict[str, Any] | None:
        """Return validated cache data or fetch it; failed results are never cached.

        ``None`` from the fetcher is a retryable miss. Fetch exceptions propagate
        unchanged. A validator should reject provider error envelopes and malformed
        candles using the caller's existing domain-specific validation.
        """
        normalized = self.identity(**identity)
        key = _digest(normalized)
        path = self.root / key[:2] / f"{key}.json"
        path.parent.mkdir(parents=True, exist_ok=True)
        with self._key_lock(path.with_suffix(".lock")):
            cached = self._read(path, normalized, expected_resolved_symbol, validator)
            if cached is not None:
                with self._stats_lock:
                    self.stats["hits"] += 1
                return cached
            with self._stats_lock:
                self.stats["misses"] += 1
            try:
                data = fetcher()
            except Exception:
                with self._stats_lock:
                    self.stats["fetch_failures"] += 1
                raise
            if data is None or not self._valid(data, expected_resolved_symbol, validator):
                with self._stats_lock:
                    self.stats["fetch_failures"] += 1
                return data if isinstance(data, dict) else None
            envelope = {"identity": normalized, "resolved_symbol": _resolved_feed(data),
                        "content_fingerprint": _digest(data), "data": data}
            self._publish(path, envelope)
            with self._stats_lock:
                self.stats["published"] += 1
            return data

    @staticmethod
    def _valid(data: Any, expected_resolved_symbol: str | None,
               validator: Callable[[dict[str, Any]], bool] | None) -> bool:
        if not isinstance(data, dict) or data.get("error_code") or data.get("error"):
            return False
        resolved = _resolved_feed(data)
        if not resolved:
            return False
        if expected_resolved_symbol and resolved != str(expected_resolved_symbol).strip().upper():
            return False
        if validator is not None:
            try:
                if not validator(data):
                    return False
            except (ValueError, KeyError, TypeError, OverflowError):
                return False
        try:
            _digest(data)
        except (TypeError, ValueError, OverflowError):
            return False
        return True

    def _read(self, path: Path, identity: dict[str, str], expected_resolved_symbol: str | None,
              validator: Callable[[dict[str, Any]], bool] | None) -> dict[str, Any] | None:
        try:
            envelope = json.loads(path.read_text(encoding="utf-8"))
            data = envelope["data"]
            resolved = _resolved_feed(data)
            if (envelope.get("identity") != identity or not resolved or
                    envelope.get("resolved_symbol") != resolved or
                    envelope.get("content_fingerprint") != _digest(data) or
                    not self._valid(data, expected_resolved_symbol, validator)):
                return None
            return data
        except (OSError, UnicodeDecodeError, json.JSONDecodeError, KeyError, TypeError, ValueError):
            return None

    @staticmethod
    def _publish(path: Path, envelope: dict[str, Any]) -> None:
        raw = json.dumps(envelope, sort_keys=True, indent=2, ensure_ascii=False).encode("utf-8") + b"\n"
        fd, temporary = tempfile.mkstemp(prefix=f".{path.stem}.staging-", dir=path.parent)
        try:
            with os.fdopen(fd, "wb") as handle:
                handle.write(raw)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temporary, path)
        finally:
            try:
                os.unlink(temporary)
            except FileNotFoundError:
                pass

    @staticmethod
    @contextmanager
    def _key_lock(path: Path) -> Iterator[None]:
        with path.open("a+b") as handle:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
            try:
                yield
            finally:
                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
