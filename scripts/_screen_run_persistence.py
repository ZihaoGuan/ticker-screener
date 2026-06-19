from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from src.webapp.config import load_webapp_config
from src.webapp.services.screener_history_service import ScreenerHistoryService


_ARG_OPTION_KEYS = (
    "limit",
    "tickers",
    "date_label",
    "as_of_date",
    "reference_date",
    "source",
    "filter_precedence",
    "include_sectors",
    "exclude_sectors",
    "include_industries",
    "exclude_industries",
    "include_themes",
    "exclude_themes",
    "pass_mode",
)


def persist_screen_run_artifacts_if_configured(
    *,
    args: argparse.Namespace,
    summary_path: Path,
    option_overrides: dict[str, Any] | None = None,
) -> int | None:
    if not summary_path.exists():
        return None
    try:
        summary_payload = json.loads(summary_path.read_text(encoding="utf-8"))
    except Exception:
        return None
    if not isinstance(summary_payload, dict):
        return None

    raw_results_file = str(summary_payload.get("raw_results_file") or "").strip()
    if not raw_results_file:
        return None
    raw_path = Path(raw_results_file)
    if not raw_path.exists():
        return None
    try:
        raw_payload = json.loads(raw_path.read_text(encoding="utf-8"))
    except Exception:
        return None
    if not isinstance(raw_payload, dict):
        return None

    strategy_id = str(summary_payload.get("strategy_id") or "").strip()
    if not strategy_id:
        return None

    webapp_config = load_webapp_config()
    history_service = ScreenerHistoryService(
        database_url=webapp_config.database_url,
        artifacts_dir=webapp_config.artifacts_dir,
    )
    if not history_service.is_configured():
        return None

    options = _build_options(args=args, summary_payload=summary_payload)
    if option_overrides:
        options.update(option_overrides)
    return history_service.persist_screen_run(
        strategy_id=strategy_id,
        options=options,
        summary_payload=summary_payload,
        raw_payload=raw_payload,
    )


def _build_options(*, args: argparse.Namespace, summary_payload: dict[str, Any]) -> dict[str, Any]:
    options: dict[str, Any] = {
        "market_data_source": "internet",
    }
    for key in _ARG_OPTION_KEYS:
        if hasattr(args, key):
            options[key] = getattr(args, key)
    for key in ("source", "reference_date", "pass_mode"):
        value = summary_payload.get(key)
        if value not in (None, ""):
            options[key] = value
    return options
