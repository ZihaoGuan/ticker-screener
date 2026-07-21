from __future__ import annotations

import re
from typing import Any

from .ticker_filters import normalize_ticker_symbol


_TICKER_LIKE_PATTERN = re.compile(r"[A-Z][A-Z0-9.-]{0,5}")


def normalize_finviz_ticker(row: dict[str, Any]) -> str:
    return normalize_ticker_symbol(str(row.get("Ticker") or ""))


def sanitize_finviz_company_name(row: dict[str, Any], *, ticker: str) -> str:
    company_name = str(row.get("Company") or "").strip()
    if is_ticker_like_finviz_company_name(company_name, ticker=ticker):
        return ""
    return company_name


def is_ticker_like_finviz_company_name(value: str, *, ticker: str) -> bool:
    candidate = normalize_ticker_symbol(value)
    normalized_ticker = normalize_ticker_symbol(ticker)
    if not candidate or candidate == normalized_ticker:
        return False
    return bool(_TICKER_LIKE_PATTERN.fullmatch(candidate))
