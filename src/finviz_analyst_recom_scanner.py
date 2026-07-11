from __future__ import annotations

import datetime as dt
import time
from typing import Any, Sequence

try:
    from requests import exceptions as requests_exceptions
except ImportError:  # pragma: no cover - requests ships with finviz in production
    requests_exceptions = None


FINVIZ_ANALYST_RECOM_STRONGBUY_FILTERS: tuple[str, ...] = ("ind_stocksonly", "an_recom_strongbuy")
FINVIZ_ANALYST_RECOM_STRONGBUY_STRATEGY_ID = "finviz_analyst_recom_strongbuy"
FINVIZ_ANALYST_RECOM_STRONGBUY_LABEL = "Finviz Analyst Recom Strong Buy"
_RATE_LIMIT_RETRY_DELAYS: tuple[float, ...] = (1.0, 3.0, 6.0)


def _load_finviz_screener() -> type[Any]:
    try:
        from finviz.screener import Screener
    except ImportError as exc:
        raise RuntimeError(
            "finviz dependency missing. Install requirements-finviz.txt or requirements.txt before running analyst recommendation scanner."
        ) from exc
    return Screener


def _normalize_ticker_list(tickers: Sequence[str] | None) -> list[str]:
    normalized: list[str] = []
    seen: set[str] = set()
    for item in tickers or ():
        ticker = str(item or "").strip().upper()
        if not ticker or ticker in seen:
            continue
        seen.add(ticker)
        normalized.append(ticker)
    return normalized


def _normalize_hit(row: dict[str, Any]) -> dict[str, Any] | None:
    ticker = str(row.get("Ticker") or "").strip().upper()
    if not ticker:
        return None
    payload = dict(row)
    payload["ticker"] = ticker
    payload["company_name"] = str(row.get("Company") or "").strip()
    payload["strategy_id"] = FINVIZ_ANALYST_RECOM_STRONGBUY_STRATEGY_ID
    payload["source"] = "finviz"
    payload["analyst_recom_filter"] = "strongbuy"
    payload["analyst_recom_label"] = "Strong Buy (1)"
    return payload


def _is_rate_limit_error(error: Exception) -> bool:
    if requests_exceptions is not None and isinstance(error, requests_exceptions.HTTPError):
        response = getattr(error, "response", None)
        return getattr(response, "status_code", None) == 429
    return "429" in str(error)


def run_finviz_analyst_recom_strongbuy_scanner(
    *,
    limit: int | None = None,
    tickers: Sequence[str] | None = None,
) -> dict[str, Any]:
    screener_cls = _load_finviz_screener()
    last_error: Exception | None = None
    screener: Any | None = None
    for attempt_index in range(len(_RATE_LIMIT_RETRY_DELAYS) + 1):
        try:
            screener = screener_cls(filters=list(FINVIZ_ANALYST_RECOM_STRONGBUY_FILTERS), table="Overview", order="ticker")
            break
        except Exception as exc:
            last_error = exc
            if not _is_rate_limit_error(exc) or attempt_index >= len(_RATE_LIMIT_RETRY_DELAYS):
                raise
            time.sleep(_RATE_LIMIT_RETRY_DELAYS[attempt_index])
    if screener is None:
        if last_error is not None:
            raise last_error
        raise RuntimeError("Finviz analyst recommendation scanner failed before building screener.")

    requested_tickers = _normalize_ticker_list(tickers)
    requested_ticker_set = set(requested_tickers)
    hits: list[dict[str, Any]] = []

    for row in screener:
        normalized = _normalize_hit(dict(row))
        if normalized is None:
            continue
        if requested_ticker_set and str(normalized.get("ticker") or "") not in requested_ticker_set:
            continue
        hits.append(normalized)
        if limit is not None and len(hits) >= limit:
            break

    return {
        "strategy_id": FINVIZ_ANALYST_RECOM_STRONGBUY_STRATEGY_ID,
        "screen_name": FINVIZ_ANALYST_RECOM_STRONGBUY_LABEL,
        "source": "finviz.screener",
        "filters": list(FINVIZ_ANALYST_RECOM_STRONGBUY_FILTERS),
        "analyst_recom_filter": "strongbuy",
        "analyst_recom_label": "Strong Buy (1)",
        "fetched_at": dt.datetime.now(dt.UTC).isoformat(),
        "total_candidates": len(screener),
        "returned_candidates": len(hits),
        "requested_tickers": requested_tickers,
        "hits": hits,
    }
