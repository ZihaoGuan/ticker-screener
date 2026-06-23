from __future__ import annotations

import json
from pathlib import Path
from typing import Any


def gamma_exposure_plot_cache_path(*, artifacts_dir: Path, symbol: str) -> Path:
    normalized_symbol = _normalize_symbol(symbol)
    return artifacts_dir / "cache" / f"gamma_exposure_plot_{normalized_symbol}.json"


def persist_gamma_exposure_plot_context(*, artifacts_dir: Path, payload: dict[str, Any]) -> Path:
    symbol = _normalize_symbol(str(payload.get("symbol") or "SPX"))
    path = gamma_exposure_plot_cache_path(artifacts_dir=artifacts_dir, symbol=symbol)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    return path


def load_gamma_exposure_plot_context(*, artifacts_dir: Path, symbol: str = "SPX") -> dict[str, Any] | None:
    path = gamma_exposure_plot_cache_path(artifacts_dir=artifacts_dir, symbol=symbol)
    if not path.exists():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    return payload if isinstance(payload, dict) else None


def _normalize_symbol(symbol: str) -> str:
    return "".join(char for char in str(symbol or "").strip().upper() if char.isalnum()) or "SPX"
