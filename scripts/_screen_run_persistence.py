from __future__ import annotations

import argparse
import json
import os
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
    "market_data_source",
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
    if os.environ.get("TICKER_SCREENER_SKIP_SCREEN_RUN_PERSISTENCE") == "1":
        return None
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

    # The raw scanner result is intentionally lean, while the watchlist
    # builder adds execution fields (for example trigger/stop levels).  The
    # history read model is sourced from ``hits``, so retain those derived
    # fields when persisting a run instead of silently dropping them.
    watchlist_file = str(summary_payload.get("watchlist_file") or "").strip()
    if watchlist_file:
        watchlist_path = Path(watchlist_file)
        if watchlist_path.exists():
            try:
                watchlist_payload = json.loads(watchlist_path.read_text(encoding="utf-8"))
            except Exception:
                watchlist_payload = None
            _merge_watchlist_fields(raw_payload, watchlist_payload)

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


def _merge_watchlist_fields(raw_payload: dict[str, Any], watchlist_payload: Any) -> None:
    """Fill missing hit fields from the enriched watchlist artifact.

    Do not replace raw values: the artifact is an enrichment layer and may
    round or rename fields.  Only absent/None values are filled.
    """
    if not isinstance(watchlist_payload, list):
        return
    by_ticker = {
        str(item.get("ticker") or item.get("symbol") or "").strip().upper(): item
        for item in watchlist_payload
        if isinstance(item, dict)
        and str(item.get("ticker") or item.get("symbol") or "").strip()
    }
    hits = raw_payload.get("hits")
    if not isinstance(hits, list):
        return
    for hit in hits:
        if not isinstance(hit, dict):
            continue
        ticker = str(hit.get("ticker") or hit.get("symbol") or "").strip().upper()
        enriched = by_ticker.get(ticker)
        if not enriched:
            continue
        for key, value in enriched.items():
            if hit.get(key) is None and value is not None:
                hit[key] = value


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
