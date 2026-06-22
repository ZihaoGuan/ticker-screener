from __future__ import annotations

import datetime as dt
import json
from pathlib import Path
from typing import Any

from .models import FinvizProbeResult


FINVIZ_MISSING_TICKERS_FILENAME = "finviz_missing_tickers.json"


def load_missing_finviz_tickers(artifacts_dir: Path) -> dict[str, dict[str, Any]]:
    payload = _load_payload(_registry_path(artifacts_dir))
    tickers = payload.get("tickers")
    if not isinstance(tickers, dict):
        return {}
    normalized: dict[str, dict[str, Any]] = {}
    for raw_ticker, raw_entry in tickers.items():
        ticker = str(raw_ticker or "").strip().upper()
        if not ticker or not isinstance(raw_entry, dict):
            continue
        normalized[ticker] = dict(raw_entry)
    return normalized


def is_known_missing_finviz_ticker(ticker: str, *, artifacts_dir: Path) -> bool:
    normalized = str(ticker or "").strip().upper()
    if not normalized:
        return False
    return normalized in load_missing_finviz_tickers(artifacts_dir)


def record_missing_finviz_ticker(
    ticker: str,
    *,
    artifacts_dir: Path,
    reason: str,
    source: str,
) -> None:
    normalized = str(ticker or "").strip().upper()
    if not normalized:
        return
    path = _registry_path(artifacts_dir)
    payload = _load_payload(path)
    entries = payload.get("tickers")
    normalized_entries = dict(entries) if isinstance(entries, dict) else {}
    current = normalized_entries.get(normalized)
    now_text = dt.datetime.now(dt.timezone.utc).replace(microsecond=0).isoformat()
    prior_hits = int(current.get("hit_count") or 0) if isinstance(current, dict) else 0
    first_seen = str(current.get("first_seen_at") or "").strip() if isinstance(current, dict) else ""
    normalized_entries[normalized] = {
        "ticker": normalized,
        "source": str(source or "").strip() or "unknown",
        "reason": str(reason or "").strip() or "Finviz returned 404.",
        "first_seen_at": first_seen or now_text,
        "last_seen_at": now_text,
        "hit_count": prior_hits + 1,
    }
    wrapped = {
        "generated_at": now_text,
        "tickers": normalized_entries,
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(wrapped, indent=2), encoding="utf-8")


def remove_missing_finviz_ticker(ticker: str, *, artifacts_dir: Path) -> dict[str, Any]:
    normalized = str(ticker or "").strip().upper()
    if not normalized:
        raise ValueError("Ticker is required.")
    path = _registry_path(artifacts_dir)
    payload = _load_payload(path)
    entries = payload.get("tickers")
    normalized_entries = dict(entries) if isinstance(entries, dict) else {}
    current = normalized_entries.get(normalized)
    if not isinstance(current, dict):
        raise ValueError(f"Ticker {normalized} is not in Finviz missing registry.")
    del normalized_entries[normalized]
    now_text = dt.datetime.now(dt.timezone.utc).replace(microsecond=0).isoformat()
    wrapped = {
        "generated_at": now_text,
        "tickers": normalized_entries,
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(wrapped, indent=2), encoding="utf-8")
    return dict(current)


def finviz_error_is_missing(value: object) -> bool:
    response = getattr(value, "response", None)
    status_code = getattr(response, "status_code", None)
    if status_code == 404:
        return True
    text = str(value or "").strip().lower()
    if "404 client error" in text:
        return True
    return "404" in text and "not found" in text


def finviz_probe_is_missing(probe: FinvizProbeResult) -> bool:
    if probe.status_code == 404:
        return True
    text = "\n".join((probe.title, probe.body_excerpt, probe.final_url)).lower()
    return "404" in text and "not found" in text


def _registry_path(artifacts_dir: Path) -> Path:
    return artifacts_dir / "raw" / FINVIZ_MISSING_TICKERS_FILENAME


def _load_payload(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}
