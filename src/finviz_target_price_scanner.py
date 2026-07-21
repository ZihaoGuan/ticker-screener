from __future__ import annotations

import datetime as dt
import time
from typing import Any, Sequence

from .finviz_screener_rows import SafeFinvizScreener, normalize_finviz_ticker, sanitize_finviz_company_name

try:
    from requests import exceptions as requests_exceptions
except ImportError:  # pragma: no cover - requests ships with finviz in production
    requests_exceptions = None


FINVIZ_TARGET_PRICE_SCANNER_FILTERS: tuple[str, ...] = ("ind_stocksonly", "targetprice_a50")
FINVIZ_TARGET_PRICE_SCANNER_STRATEGY_ID = "finviz_target_price_50"
TARGET_PRICE_UPSIDE_RATIO = 1.5
_CUSTOM_TABLE_COLUMNS: tuple[str, ...] = ("1", "2", "65", "69")
_MULTIPLIERS = {"K": 1_000.0, "M": 1_000_000.0, "B": 1_000_000_000.0, "T": 1_000_000_000_000.0}
_RATE_LIMIT_RETRY_DELAYS: tuple[float, ...] = (1.0, 3.0, 6.0)


def _load_finviz_screener() -> type[Any]:
    try:
        import requests  # noqa: F401
        import lxml  # noqa: F401
    except ImportError as exc:
        raise RuntimeError(
            "Finviz parser dependencies missing. Install requirements-finviz.txt or requirements.txt before running target-price scanner."
        ) from exc
    return SafeFinvizScreener

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


def _parse_price(value: object) -> float | None:
    parsed = _coerce_number(str(value or ""))
    if parsed is None or parsed <= 0:
        return None
    return float(parsed)


def _coerce_number(value: str | None) -> float | None:
    text = str(value or "").strip().replace(",", "")
    if not text or text in {"-", "N/A"}:
        return None
    sign = -1.0 if text.startswith("-") else 1.0
    text = text.lstrip("+-")
    if text.endswith("%"):
        text = text[:-1]
    suffix = text[-1:] if text else ""
    multiplier = _MULTIPLIERS.get(suffix, 1.0)
    if multiplier != 1.0:
        text = text[:-1]
    try:
        return sign * float(text) * multiplier
    except ValueError:
        return None


def _normalize_hit(row: dict[str, Any], *, minimum_upside_ratio: float) -> dict[str, Any] | None:
    ticker = normalize_finviz_ticker(row)
    current_price = _parse_price(row.get("Price"))
    target_price = _parse_price(row.get("Target Price"))
    if not ticker or current_price is None or target_price is None:
        return None
    upside_ratio = target_price / current_price
    if upside_ratio < minimum_upside_ratio:
        return None

    payload = dict(row)
    payload["ticker"] = ticker
    payload["company_name"] = sanitize_finviz_company_name(row, ticker=ticker)
    payload["strategy_id"] = FINVIZ_TARGET_PRICE_SCANNER_STRATEGY_ID
    payload["source"] = "finviz"
    payload["current_price"] = round(current_price, 4)
    payload["target_price"] = round(target_price, 4)
    payload["target_price_upside_ratio"] = round(upside_ratio, 6)
    payload["target_price_upside_pct"] = round((upside_ratio - 1.0) * 100.0, 2)
    return payload


def _is_rate_limit_error(error: Exception) -> bool:
    if requests_exceptions is not None and isinstance(error, requests_exceptions.HTTPError):
        response = getattr(error, "response", None)
        return getattr(response, "status_code", None) == 429
    return "429" in str(error)


def _build_screener_rows(screener_cls: type[Any]) -> tuple[list[dict[str, Any]], str]:
    filters = list(FINVIZ_TARGET_PRICE_SCANNER_FILTERS)

    def _fetch_rows(*, table: str, request_method: str, custom: list[str] | None = None) -> list[dict[str, Any]]:
        last_error: Exception | None = None
        for attempt_index in range(len(_RATE_LIMIT_RETRY_DELAYS) + 1):
            try:
                screener = screener_cls(
                    filters=filters,
                    table=table,
                    custom=custom,
                    order="ticker",
                    request_method=request_method,
                )
                return [dict(row) for row in screener]
            except Exception as exc:
                last_error = exc
                if not _is_rate_limit_error(exc) or attempt_index >= len(_RATE_LIMIT_RETRY_DELAYS):
                    raise
                time.sleep(_RATE_LIMIT_RETRY_DELAYS[attempt_index])
        if last_error is not None:
            raise last_error
        return []

    last_error: Exception | None = None
    for request_method in ("async", "sync"):
        try:
            overview_rows = _fetch_rows(table="Overview", request_method=request_method)
        except Exception as exc:
            last_error = exc
            continue
        if overview_rows and "Target Price" in overview_rows[0]:
            return overview_rows, f"overview:{request_method}"
        try:
            custom_rows = _fetch_rows(table="Custom", request_method=request_method, custom=list(_CUSTOM_TABLE_COLUMNS))
        except Exception as exc:
            last_error = exc
            continue
        return custom_rows, f"custom:{request_method}"

    if last_error is not None:
        raise last_error
    return [], "overview:sync"


def run_finviz_target_price_scanner(
    *,
    limit: int | None = None,
    tickers: Sequence[str] | None = None,
    minimum_upside_ratio: float = TARGET_PRICE_UPSIDE_RATIO,
    as_of_date: dt.date | None = None,
    database_url: str | None = None,
) -> dict[str, Any]:
    _ = (as_of_date, database_url)
    screener_cls = _load_finviz_screener()
    requested_tickers = _normalize_ticker_list(tickers)
    requested_ticker_set = set(requested_tickers)
    hits: list[dict[str, Any]] = []
    scan_mode = "filters"

    rows, row_source = _build_screener_rows(screener_cls)
    total_candidates = len(rows)

    for row in rows:
        normalized = _normalize_hit(row, minimum_upside_ratio=minimum_upside_ratio)
        if normalized is None:
            continue
        if requested_ticker_set and str(normalized.get("ticker") or "") not in requested_ticker_set:
            continue
        hits.append(normalized)
        if limit is not None and len(hits) >= limit:
            break

    return {
        "strategy_id": FINVIZ_TARGET_PRICE_SCANNER_STRATEGY_ID,
        "screen_name": "Finviz Target Price +50%",
        "source": "finviz.screener",
        "filters": list(FINVIZ_TARGET_PRICE_SCANNER_FILTERS),
        "minimum_upside_ratio": minimum_upside_ratio,
        "minimum_upside_pct": round((minimum_upside_ratio - 1.0) * 100.0, 2),
        "fetched_at": dt.datetime.now(dt.UTC).isoformat(),
        "scan_mode": scan_mode,
        "row_source": row_source,
        "total_candidates": total_candidates,
        "returned_candidates": len(hits),
        "requested_tickers": requested_tickers,
        "hits": hits,
    }
