from __future__ import annotations

import datetime as dt
from functools import lru_cache
import importlib
import json
from pathlib import Path
import shutil
import subprocess
import sys
from typing import Any

from .constants import ALL_RATING_METRICS, METRIC_LABEL_TO_FIELD, RATING_STATUS_SCRAPE_FAILED
from .finviz_parser import _coerce_number, _parse_volatility
from .models import FundamentalsSnapshot

PROJECT_ROOT = Path(__file__).resolve().parents[2]
_FINVIZ_FETCH_SNIPPET = (
    "import json; "
    "from finviz import get_stock; "
    "print(json.dumps(get_stock({ticker!r})))"
)


class FinvizApiError(RuntimeError):
    pass


def _python_candidates() -> list[str]:
    candidates = [
        shutil.which("python3.12"),
        sys.executable,
        shutil.which("python3"),
        shutil.which("python3.11"),
        shutil.which("python3.10"),
    ]
    seen: set[str] = set()
    ordered: list[str] = []
    for candidate in candidates:
        if not candidate or candidate in seen:
            continue
        seen.add(candidate)
        ordered.append(candidate)
    return ordered


@lru_cache(maxsize=1)
def _load_finviz_get_stock() -> Any:
    try:
        module = importlib.import_module("finviz")
        get_stock = getattr(module, "get_stock", None)
        if not callable(get_stock):
            raise FinvizApiError("Installed finviz package does not expose get_stock.")
        return get_stock
    except Exception as exc:
        raise FinvizApiError(f"Unable to import installed finviz package in-process: {exc}") from exc


@lru_cache(maxsize=1)
def _select_finviz_python() -> str:
    for candidate in _python_candidates():
        result = subprocess.run(
            [candidate, "-c", "from finviz import get_stock; print('ok')"],
            cwd=str(PROJECT_ROOT),
            capture_output=True,
            text=True,
            check=False,
            timeout=20,
        )
        if result.returncode == 0 and "ok" in (result.stdout or ""):
            return candidate
    raise FinvizApiError("Unable to find a Python interpreter that can import the installed finviz package.")


def _has_complete_rating_inputs(snapshot: FundamentalsSnapshot) -> bool:
    return bool(snapshot.sector) and all(getattr(snapshot, metric_name) is not None for metric_name in ALL_RATING_METRICS)


def parse_finviz_stock_data(
    stock_data: dict[str, Any],
    *,
    ticker: str,
    as_of_date: dt.date,
    fallback_sector: str | None = None,
    fallback_industry: str | None = None,
) -> FundamentalsSnapshot:
    normalized_ticker = str(stock_data.get("Ticker") or ticker or "").strip().upper()
    snapshot = FundamentalsSnapshot(
        ticker=normalized_ticker,
        as_of_date=as_of_date,
        sector=str(stock_data.get("Sector") or fallback_sector or "").strip() or None,
        industry=str(stock_data.get("Industry") or fallback_industry or "").strip() or None,
        source="finviz-api",
        source_url=f"https://finviz.com/quote.ashx?t={normalized_ticker}&p=d",
        parse_status="ok",
    )

    for label, field_name in METRIC_LABEL_TO_FIELD.items():
        raw_value = stock_data.get(label)
        if raw_value is None:
            continue
        parsed_value = _coerce_number(str(raw_value))
        if parsed_value is not None:
            setattr(snapshot, field_name, parsed_value)

    growth_next_y = stock_data.get("EPS growth next Y")
    if growth_next_y is not None:
        snapshot.eps_next_y_pct = _coerce_number(str(growth_next_y))
    else:
        raw_eps_next_y = stock_data.get("EPS next Y")
        if isinstance(raw_eps_next_y, str) and raw_eps_next_y.strip().endswith("%"):
            snapshot.eps_next_y_pct = _coerce_number(raw_eps_next_y)

    volatility_month = stock_data.get("Volatility (Month)")
    volatility_week = stock_data.get("Volatility (Week)")
    if volatility_month is not None:
        snapshot.volatility_month_pct = _coerce_number(str(volatility_month))
    if volatility_week is not None:
        snapshot.volatility_week_pct = _coerce_number(str(volatility_week))
    if snapshot.volatility_month_pct is None and stock_data.get("Volatility") is not None:
        week_value, month_value = _parse_volatility(str(stock_data.get("Volatility")))
        snapshot.volatility_week_pct = snapshot.volatility_week_pct or week_value
        snapshot.volatility_month_pct = month_value

    return snapshot


def fetch_finviz_api_snapshot(
    ticker: str,
    *,
    as_of_date: dt.date,
    fallback_sector: str | None = None,
    fallback_industry: str | None = None,
) -> FundamentalsSnapshot:
    normalized = str(ticker or "").strip().upper()
    if not normalized:
        raise FinvizApiError("Missing ticker.")
    try:
        stock_data = _load_finviz_get_stock()(normalized)
    except Exception:
        stock_data = _fetch_finviz_api_snapshot_via_subprocess(normalized)
    if not isinstance(stock_data, dict) or not stock_data:
        raise FinvizApiError(f"finviz.get_stock returned no data for {normalized}.")
    snapshot = parse_finviz_stock_data(
        stock_data,
        ticker=normalized,
        as_of_date=as_of_date,
        fallback_sector=fallback_sector,
        fallback_industry=fallback_industry,
    )
    return snapshot


def _fetch_finviz_api_snapshot_via_subprocess(normalized: str) -> dict[str, Any]:
    try:
        result = subprocess.run(
            [_select_finviz_python(), "-c", _FINVIZ_FETCH_SNIPPET.format(ticker=normalized)],
            cwd=str(PROJECT_ROOT),
            capture_output=True,
            text=True,
            check=False,
            timeout=45,
        )
    except Exception as exc:
        raise FinvizApiError(f"finviz.get_stock failed for {normalized}: {exc}") from exc
    if result.returncode != 0:
        stderr = (result.stderr or "").strip()
        raise FinvizApiError(stderr or f"finviz.get_stock failed for {normalized} with code {result.returncode}")
    try:
        stock_data = json.loads((result.stdout or "").strip())
    except json.JSONDecodeError as exc:
        raise FinvizApiError(f"finviz.get_stock returned invalid JSON for {normalized}: {exc}") from exc
    if not isinstance(stock_data, dict):
        raise FinvizApiError(f"finviz.get_stock returned invalid payload for {normalized}.")
    return stock_data


def snapshot_needs_fallback(snapshot: FundamentalsSnapshot) -> bool:
    if snapshot.parse_status == RATING_STATUS_SCRAPE_FAILED:
        return True
    return not _has_complete_rating_inputs(snapshot)
