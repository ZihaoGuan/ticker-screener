from __future__ import annotations

import datetime as dt
from typing import Any, Sequence

from .finviz_screener_rows import SafeFinvizScreener, normalize_finviz_ticker, sanitize_finviz_company_name


VENU_SCANNER_FILTERS: tuple[str, ...] = (
    "cap_midover",
    "sh_price_o5",
    "sh_avgvol_o500",
    "sh_relvol_o1",
    "sh_curvol_o1000",
    "ind_stocksonly",
    "ta_sma20_pa",
    "ta_sma50_pa",
    "ta_sma200_pa",
)


def _load_finviz_screener() -> type[Any]:
    try:
        import requests  # noqa: F401
        import lxml  # noqa: F401
    except ImportError as exc:
        raise RuntimeError(
            "Finviz parser dependencies missing. Install requirements-finviz.txt or requirements.txt before running venu scanner."
        ) from exc
    return SafeFinvizScreener

def _normalize_ticker_list(tickers: Sequence[str] | None) -> set[str]:
    normalized: set[str] = set()
    for item in tickers or ():
        ticker = str(item or "").strip().upper()
        if ticker:
            normalized.add(ticker)
    return normalized


def _normalize_hit(row: dict[str, Any]) -> dict[str, Any]:
    ticker = normalize_finviz_ticker(row)
    payload = dict(row)
    payload["ticker"] = ticker
    payload["company_name"] = sanitize_finviz_company_name(row, ticker=ticker)
    payload["strategy_id"] = "venu_scanner"
    payload["source"] = "finviz"
    return payload


def run_venu_scanner(*, limit: int | None = None, tickers: Sequence[str] | None = None) -> dict[str, Any]:
    screener_cls = _load_finviz_screener()
    screener = screener_cls(filters=list(VENU_SCANNER_FILTERS), table="Overview", order="price")
    requested_tickers = _normalize_ticker_list(tickers)
    hits: list[dict[str, Any]] = []

    for row in screener:
        normalized = _normalize_hit(dict(row))
        ticker = str(normalized.get("ticker") or "")
        if requested_tickers and ticker not in requested_tickers:
            continue
        hits.append(normalized)
        if limit is not None and len(hits) >= limit:
            break

    return {
        "strategy_id": "venu_scanner",
        "screen_name": "Venu Scanner",
        "source": "finviz.screener",
        "filters": list(VENU_SCANNER_FILTERS),
        "fetched_at": dt.datetime.now(dt.UTC).isoformat(),
        "total_candidates": len(screener),
        "returned_candidates": len(hits),
        "requested_tickers": sorted(requested_tickers),
        "hits": hits,
    }
