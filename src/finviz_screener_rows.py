from __future__ import annotations

import re
from typing import Any
from urllib.parse import parse_qs, urlparse

from lxml import html
import requests

from .ticker_filters import normalize_ticker_symbol

try:
    from finviz.config import USER_AGENT
except ImportError:  # pragma: no cover - fallback for partial finviz installs
    USER_AGENT = "Mozilla/5.0"


TABLE_TYPES = {
    "Overview": "111",
    "Valuation": "121",
    "Ownership": "131",
    "Performance": "141",
    "Custom": "152",
    "Financial": "161",
    "Technical": "171",
}
_TICKER_LIKE_PATTERN = re.compile(r"[A-Z][A-Z0-9.-]{0,5}")


class SafeFinvizScreener:
    """Small Finviz screener client that parses table cells instead of raw text nodes."""

    def __init__(
        self,
        tickers: list[str] | None = None,
        filters: list[str] | None = None,
        rows: int | None = None,
        order: str = "",
        signal: str = "",
        table: str | None = None,
        custom: list[str] | None = None,
        user_agent: str = USER_AGENT,
        request_method: str = "sequential",
    ) -> None:
        self._tickers = list(tickers or [])
        self._filters = list(filters or [])
        self._rows_requested = rows
        self._order = order
        self._signal = signal
        self._table = "152" if custom is not None else TABLE_TYPES.get(table or "Overview", "111")
        self._custom = list(custom or [])
        if custom is not None and "0" not in self._custom:
            self._custom = ["0", *self._custom]
        self._user_agent = user_agent
        self._request_method = request_method
        self.headers: list[str] = []
        self.data = self._search_screener()
        self.analysis: list[dict[str, Any]] = []

    def __iter__(self):
        return iter(self.data)

    def __len__(self) -> int:
        return len(self.data)

    def __getitem__(self, position: int) -> dict[str, Any]:
        return self.data[position]

    def get_ticker_details(self) -> list[dict[str, Any]]:
        return self.data

    def _search_screener(self) -> list[dict[str, Any]]:
        first_tree, first_url = self._fetch_page(1)
        self.headers = _extract_headers(first_tree)
        total_rows = _extract_total_rows(first_tree) or len(_extract_rows(first_tree, self.headers))
        row_limit = total_rows if self._rows_requested is None else min(self._rows_requested, total_rows)
        if row_limit <= 0:
            return []

        rows: list[dict[str, Any]] = []
        for row in _extract_rows(first_tree, self.headers):
            rows.append(row)
            if len(rows) >= row_limit:
                return rows

        for start in range(21, row_limit + 1, 20):
            page_tree, _ = self._fetch_page(start, base_url=first_url)
            for row in _extract_rows(page_tree, self.headers):
                rows.append(row)
                if len(rows) >= row_limit:
                    return rows
        return rows

    def _fetch_page(self, start: int, *, base_url: str | None = None):
        params = {
            "v": self._table,
            "t": ",".join(self._tickers),
            "f": ",".join(self._filters),
            "o": self._order,
            "s": self._signal,
            "c": ",".join(self._custom),
        }
        if start > 1:
            params["r"] = str(start)
        url = base_url or "https://finviz.com/screener.ashx"
        response = requests.get(url, params=params if base_url is None else {"r": str(start)}, headers={"User-Agent": self._user_agent}, timeout=30, verify=False)
        response.raise_for_status()
        return html.fromstring(response.text), response.url


def repair_shifted_finviz_row(row: dict[str, Any]) -> dict[str, Any]:
    repaired = dict(row)
    ticker = normalize_ticker_symbol(str(repaired.get("Ticker") or repaired.get("ticker") or ""))
    company = str(repaired.get("Company") or repaired.get("company_name") or "").strip()
    if not is_ticker_like_finviz_company_name(company, ticker=ticker):
        return repaired

    keys = list(repaired.keys())
    try:
        start = keys.index("Ticker")
    except ValueError:
        return repaired
    end = start + 1
    while end < len(keys) and _is_finviz_display_column(keys[end]):
        end += 1
    display_keys = keys[start:end]
    if len(display_keys) < 2:
        return repaired

    for index, key in enumerate(display_keys[:-1]):
        repaired[key] = repaired.get(display_keys[index + 1], "")
    repaired[display_keys[-1]] = ""

    repaired_ticker = normalize_finviz_ticker(repaired)
    repaired["ticker"] = repaired_ticker
    repaired["company_name"] = sanitize_finviz_company_name(repaired, ticker=repaired_ticker)
    return repaired


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


def _is_finviz_display_column(key: str) -> bool:
    return bool(key) and not key[:1].islower() and key not in {"strategy_id", "source"}


def _extract_headers(tree: html.HtmlElement) -> list[str]:
    header_rows = tree.cssselect('tr[valign="middle"]')
    if not header_rows:
        return []
    elements = header_rows[0].cssselect("th") or header_rows[0].xpath("td")
    return [item.text_content().strip() for item in elements if item.text_content().strip()]


def _extract_total_rows(tree: html.HtmlElement) -> int:
    text = tree.text_content()
    match = re.search(r"#1\s*/\s*([0-9,]+)\s+Total", text)
    if not match:
        return 0
    return int(match.group(1).replace(",", ""))


def _extract_rows(tree: html.HtmlElement, headers: list[str]) -> list[dict[str, Any]]:
    parsed_rows: list[dict[str, Any]] = []
    for row in tree.cssselect('tr[valign="top"]'):
        cells = row.xpath("td")
        if not cells:
            continue
        values = [_extract_cell_value(header, cell) for header, cell in zip(headers, cells)]
        parsed_rows.append(dict(zip(headers, values)))
    return parsed_rows


def _extract_cell_value(header: str, cell: html.HtmlElement) -> str:
    if header == "Ticker":
        ticker = _extract_ticker_from_cell(cell)
        if ticker:
            return ticker
    return " ".join(cell.text_content().split())


def _extract_ticker_from_cell(cell: html.HtmlElement) -> str:
    for link in cell.xpath('.//a[@href]'):
        href = str(link.get("href") or "")
        query = parse_qs(urlparse(href).query)
        ticker = normalize_ticker_symbol(str((query.get("t") or [""])[0]))
        if ticker:
            return ticker
    texts = [text.strip() for text in cell.xpath(".//text()") if text.strip()]
    return normalize_ticker_symbol(texts[-1]) if texts else ""
