from __future__ import annotations

import datetime as dt
from typing import Any, Iterable, Sequence

from .market_data_access import load_active_universe_from_db


FINVIZ_TARGET_PRICE_SCANNER_FILTERS: tuple[str, ...] = ("ind_stocksonly",)
FINVIZ_TARGET_PRICE_SCANNER_STRATEGY_ID = "finviz_target_price_50"
TARGET_PRICE_UPSIDE_RATIO = 1.5
_TICKER_BATCH_SIZE = 150
_MULTIPLIERS = {"K": 1_000.0, "M": 1_000_000.0, "B": 1_000_000_000.0, "T": 1_000_000_000_000.0}


def _load_finviz_screener() -> type[Any]:
    try:
        from finviz.screener import Screener
    except ImportError as exc:
        raise RuntimeError(
            "finviz dependency missing. Install requirements-finviz.txt or requirements.txt before running target-price scanner."
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


def _batched(items: Sequence[str], batch_size: int) -> Iterable[list[str]]:
    for start in range(0, len(items), batch_size):
        yield list(items[start : start + batch_size])


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
    ticker = str(row.get("Ticker") or "").strip().upper()
    current_price = _parse_price(row.get("Price"))
    target_price = _parse_price(row.get("Target Price"))
    if not ticker or current_price is None or target_price is None:
        return None
    upside_ratio = target_price / current_price
    if upside_ratio < minimum_upside_ratio:
        return None

    payload = dict(row)
    payload["ticker"] = ticker
    payload["company_name"] = str(row.get("Company") or "").strip()
    payload["strategy_id"] = FINVIZ_TARGET_PRICE_SCANNER_STRATEGY_ID
    payload["source"] = "finviz"
    payload["current_price"] = round(current_price, 4)
    payload["target_price"] = round(target_price, 4)
    payload["target_price_upside_ratio"] = round(upside_ratio, 6)
    payload["target_price_upside_pct"] = round((upside_ratio - 1.0) * 100.0, 2)
    return payload


def _load_requested_universe_tickers(
    *,
    tickers: Sequence[str] | None,
    as_of_date: dt.date | None,
    database_url: str | None,
) -> list[str]:
    requested = _normalize_ticker_list(tickers)
    if requested:
        return requested
    db_universe = load_active_universe_from_db(as_of_date=as_of_date, database_url=database_url)
    if not db_universe:
        return []
    return [item.symbol for item in db_universe if getattr(item, "symbol", None)]


def _run_screener_batch(
    screener_cls: type[Any],
    *,
    batch_tickers: Sequence[str] | None,
) -> list[dict[str, Any]]:
    if batch_tickers:
        screener = screener_cls(tickers=list(batch_tickers), table="Overview", order="ticker")
    else:
        screener = screener_cls(filters=list(FINVIZ_TARGET_PRICE_SCANNER_FILTERS), table="Overview", order="ticker")

    detail_rows = screener.get_ticker_details()
    if isinstance(detail_rows, list):
        return [dict(row) for row in detail_rows]
    return [dict(row) for row in screener]


def run_finviz_target_price_scanner(
    *,
    limit: int | None = None,
    tickers: Sequence[str] | None = None,
    minimum_upside_ratio: float = TARGET_PRICE_UPSIDE_RATIO,
    as_of_date: dt.date | None = None,
    database_url: str | None = None,
) -> dict[str, Any]:
    screener_cls = _load_finviz_screener()
    requested_tickers = _load_requested_universe_tickers(
        tickers=tickers,
        as_of_date=as_of_date,
        database_url=database_url,
    )
    hits: list[dict[str, Any]] = []
    total_candidates = 0
    scan_mode = "filters"

    if requested_tickers:
        scan_mode = "tickers"
        for batch in _batched(requested_tickers, _TICKER_BATCH_SIZE):
            for row in _run_screener_batch(screener_cls, batch_tickers=batch):
                total_candidates += 1
                normalized = _normalize_hit(row, minimum_upside_ratio=minimum_upside_ratio)
                if normalized is None:
                    continue
                hits.append(normalized)
                if limit is not None and len(hits) >= limit:
                    break
            if limit is not None and len(hits) >= limit:
                break
    else:
        for row in _run_screener_batch(screener_cls, batch_tickers=None):
            total_candidates += 1
            normalized = _normalize_hit(row, minimum_upside_ratio=minimum_upside_ratio)
            if normalized is None:
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
        "total_candidates": total_candidates,
        "returned_candidates": len(hits),
        "requested_tickers": requested_tickers,
        "hits": hits,
    }
