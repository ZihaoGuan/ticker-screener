from __future__ import annotations

import datetime as dt
import time
from typing import Any, Sequence

from .ratings.repository import RatingsRepository

try:
    from requests import exceptions as requests_exceptions
except ImportError:  # pragma: no cover - requests ships with finviz in production
    requests_exceptions = None


FINVIZ_SMALLOVER_SALES_GROWTH_TREND_FILTERS: tuple[str, ...] = (
    "cap_smallover",
    "fa_salesqoq_o5",
    "sh_curvol_o50",
    "sh_instown_o10",
    "sh_price_o20",
    "ta_highlow52w_b0to10h",
    "ta_sma200_sb50",
    "ta_sma50_pa",
)
FINVIZ_SMALLOVER_SALES_GROWTH_TREND_STRATEGY_ID = "finviz_smallover_sales_growth_trend"
FINVIZ_SMALLOVER_SALES_GROWTH_TREND_LABEL = "Finviz Small+ Sales Growth Trend"
MIN_DAILY_RS_RATING = 75.0
_RATE_LIMIT_RETRY_DELAYS: tuple[float, ...] = (1.0, 3.0, 6.0)


def _load_finviz_screener() -> type[Any]:
    try:
        from finviz.screener import Screener
    except ImportError as exc:
        raise RuntimeError(
            "finviz dependency missing. Install requirements-finviz.txt or requirements.txt before running Finviz small-plus sales-growth trend scanner."
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


def _normalize_hit(row: dict[str, Any], *, daily_rs_rating: float) -> dict[str, Any] | None:
    ticker = str(row.get("Ticker") or "").strip().upper()
    if not ticker:
        return None
    payload = dict(row)
    payload["ticker"] = ticker
    payload["company_name"] = str(row.get("Company") or "").strip()
    payload["strategy_id"] = FINVIZ_SMALLOVER_SALES_GROWTH_TREND_STRATEGY_ID
    payload["source"] = "finviz"
    payload["finviz_filter_set"] = "smallover_sales_growth_trend"
    payload["daily_rs_rating"] = float(daily_rs_rating)
    payload["min_daily_rs_rating"] = float(MIN_DAILY_RS_RATING)
    return payload


def _is_rate_limit_error(error: Exception) -> bool:
    if requests_exceptions is not None and isinstance(error, requests_exceptions.HTTPError):
        response = getattr(error, "response", None)
        return getattr(response, "status_code", None) == 429
    return "429" in str(error)


def run_finviz_smallover_sales_growth_trend_scanner(
    *,
    limit: int | None = None,
    tickers: Sequence[str] | None = None,
    database_url: str = "",
) -> dict[str, Any]:
    screener_cls = _load_finviz_screener()
    last_error: Exception | None = None
    screener: Any | None = None
    for attempt_index in range(len(_RATE_LIMIT_RETRY_DELAYS) + 1):
        try:
            screener = screener_cls(filters=list(FINVIZ_SMALLOVER_SALES_GROWTH_TREND_FILTERS), table="Overview", order="ticker")
            break
        except Exception as exc:
            last_error = exc
            if not _is_rate_limit_error(exc) or attempt_index >= len(_RATE_LIMIT_RETRY_DELAYS):
                raise
            time.sleep(_RATE_LIMIT_RETRY_DELAYS[attempt_index])
    if screener is None:
        if last_error is not None:
            raise last_error
        raise RuntimeError("Finviz small-plus sales-growth trend scanner failed before building screener.")

    requested_tickers = _normalize_ticker_list(tickers)
    requested_ticker_set = set(requested_tickers)
    candidate_tickers = [str(row.get("Ticker") or "").strip().upper() for row in screener if str(row.get("Ticker") or "").strip()]
    rs_snapshot_map = RatingsRepository(database_url).load_latest_technical_rating_snapshots_for_tickers(
        candidate_tickers,
        allow_older_as_of_date=True,
    )
    hits: list[dict[str, Any]] = []

    for row in screener:
        ticker = str(row.get("Ticker") or "").strip().upper()
        snapshot = rs_snapshot_map.get(ticker) or {}
        daily_rs_rating = snapshot.get("daily_rs_rating")
        if daily_rs_rating is None or float(daily_rs_rating) <= float(MIN_DAILY_RS_RATING):
            continue
        normalized = _normalize_hit(dict(row), daily_rs_rating=float(daily_rs_rating))
        if normalized is None:
            continue
        if requested_ticker_set and str(normalized.get("ticker") or "") not in requested_ticker_set:
            continue
        hits.append(normalized)
        if limit is not None and len(hits) >= limit:
            break

    return {
        "strategy_id": FINVIZ_SMALLOVER_SALES_GROWTH_TREND_STRATEGY_ID,
        "screen_name": FINVIZ_SMALLOVER_SALES_GROWTH_TREND_LABEL,
        "source": "finviz.screener",
        "filters": list(FINVIZ_SMALLOVER_SALES_GROWTH_TREND_FILTERS),
        "finviz_filter_set": "smallover_sales_growth_trend",
        "min_daily_rs_rating": float(MIN_DAILY_RS_RATING),
        "fetched_at": dt.datetime.now(dt.UTC).isoformat(),
        "total_candidates": len(screener),
        "returned_candidates": len(hits),
        "requested_tickers": requested_tickers,
        "hits": hits,
    }
