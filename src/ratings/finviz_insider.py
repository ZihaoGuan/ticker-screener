from __future__ import annotations

import datetime as dt
from html.parser import HTMLParser
import json
import logging
from pathlib import Path
import time
from typing import Any
from urllib.parse import urljoin

import requests

from .finviz_missing_tickers import (
    finviz_error_is_missing,
    is_known_missing_finviz_ticker,
    record_missing_finviz_ticker,
)

logger = logging.getLogger(__name__)


FINVIZ_INSIDER_CACHE_TTL_HOURS = 12
FINVIZ_INSIDER_CACHE_FILENAME = "finviz_insider_trades_latest.json"
FINVIZ_QUOTE_URL = "https://finviz.com/quote.ashx"
FINVIZ_REQUEST_TIMEOUT = (10, 20)
FINVIZ_USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36"
)
_BLOCK_MARKERS = (
    "captcha",
    "verify you are human",
    "access denied",
    "too many requests",
    "unusual traffic",
)


class FinvizInsiderError(RuntimeError):
    pass


def _emit_info(message: str) -> None:
    logger.info(message)
    print(message, flush=True)


def _emit_warning(message: str) -> None:
    logger.warning(message)
    print(message, flush=True)


def load_finviz_insider_signal_map(
    tickers: list[str],
    *,
    as_of_date: dt.date,
    lookback_days: int,
    artifacts_dir: Path,
    ttl_hours: int = FINVIZ_INSIDER_CACHE_TTL_HOURS,
    session: requests.Session | None = None,
) -> dict[str, dict[str, float | int]]:
    normalized_tickers = [str(item or "").strip().upper() for item in tickers if str(item or "").strip()]
    if not normalized_tickers:
        return {}

    cache_path = artifacts_dir / "raw" / "insider" / FINVIZ_INSIDER_CACHE_FILENAME
    payload = _load_cache_payload(cache_path)
    caches = payload.get("caches")
    normalized_caches = dict(caches) if isinstance(caches, dict) else {}
    active_session = session or requests.Session()
    active_session.headers.update(
        {
            "User-Agent": FINVIZ_USER_AGENT,
            "Accept-Language": "en-US,en;q=0.9",
        }
    )

    refreshed_any = False
    now_text = dt.datetime.now(dt.timezone.utc).replace(microsecond=0).isoformat()
    for index, ticker in enumerate(normalized_tickers):
        cache_key = _cache_key(ticker=ticker, as_of_date=as_of_date.isoformat(), lookback_days=lookback_days)
        window = normalized_caches.get(cache_key)
        if _is_cache_window_fresh(window, ttl_hours=ttl_hours):
            _emit_info(f"finviz insider cache hit ticker={ticker} as_of={as_of_date.isoformat()} lookback_days={lookback_days}")
            continue
        if is_known_missing_finviz_ticker(ticker, artifacts_dir=artifacts_dir):
            _emit_info(f"finviz insider skip_known_missing ticker={ticker} reason=missing_registry")
            if isinstance(window, dict) and isinstance(window.get("entries"), list):
                _emit_info(f"finviz insider using stale cache ticker={ticker}")
            else:
                _emit_info(f"finviz insider skipping overlay ticker={ticker} reason=known_missing")
            continue
        try:
            entries = fetch_finviz_insider_trades(ticker, session=active_session)
        except (FinvizInsiderError, requests.RequestException) as exc:
            entries = None
            _emit_warning(f"finviz insider refresh failed ticker={ticker} error={exc}")
            if finviz_error_is_missing(exc):
                record_missing_finviz_ticker(
                    ticker,
                    artifacts_dir=artifacts_dir,
                    reason=str(exc),
                    source="insider",
                )
                _emit_info(f"finviz insider marked_known_missing ticker={ticker}")
            if isinstance(window, dict) and isinstance(window.get("entries"), list):
                _emit_info(f"finviz insider using stale cache ticker={ticker}")
            else:
                _emit_info(f"finviz insider skipping overlay ticker={ticker} reason=no_cache")
        if entries is not None:
            normalized_caches[cache_key] = {
                "ticker": ticker,
                "requested_tickers": [ticker],
                "as_of_date": as_of_date.isoformat(),
                "lookback_days": max(1, int(lookback_days)),
                "refreshed_at": now_text,
                "entries": entries,
            }
            refreshed_any = True
            _emit_info(f"finviz insider refreshed ticker={ticker} rows={len(entries)}")
        if index < len(normalized_tickers) - 1:
            time.sleep(0.2)

    if refreshed_any:
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        wrapped = {
            "generated_at": now_text,
            "source": "finviz_quote_insider",
            "caches": normalized_caches,
        }
        cache_path.write_text(json.dumps(wrapped, indent=2), encoding="utf-8")
        _emit_info(f"finviz insider cache wrote path={cache_path}")
    return _build_signal_map(
        caches=normalized_caches,
        tickers=normalized_tickers,
        as_of_date=as_of_date,
        lookback_days=lookback_days,
    )


def fetch_finviz_insider_trades(
    ticker: str,
    *,
    session: requests.Session | None = None,
) -> list[dict[str, Any]]:
    normalized = str(ticker or "").strip().upper()
    if not normalized:
        raise FinvizInsiderError("Missing ticker.")
    active_session = session or requests.Session()
    response = active_session.get(
        FINVIZ_QUOTE_URL,
        params={"t": normalized, "p": "d"},
        timeout=FINVIZ_REQUEST_TIMEOUT,
    )
    response.raise_for_status()
    body = response.text
    lowered = body.lower()
    if any(marker in lowered for marker in _BLOCK_MARKERS):
        raise FinvizInsiderError(f"Finviz blocked insider fetch for {normalized}.")
    parser = _FinvizInsiderTableParser(base_url=str(response.url))
    parser.feed(body)
    if not parser.headers:
        return []
    return _normalize_parsed_rows(normalized, parser.headers, parser.rows)


def _build_signal_map(
    *,
    caches: dict[str, Any],
    tickers: list[str],
    as_of_date: dt.date,
    lookback_days: int,
) -> dict[str, dict[str, float | int]]:
    allowed = set(tickers)
    window_start = as_of_date - dt.timedelta(days=max(1, int(lookback_days)))
    summary_map: dict[str, dict[str, float | int]] = {}
    for window in caches.values():
        if not isinstance(window, dict):
            continue
        entries = window.get("entries")
        if not isinstance(entries, list):
            continue
        for entry in entries:
            if not isinstance(entry, dict):
                continue
            ticker = str(entry.get("ticker") or "").strip().upper()
            if ticker not in allowed:
                continue
            event_date = _coerce_date(entry.get("transaction_date")) or _coerce_date(entry.get("filing_date"))
            if event_date is None or event_date < window_start or event_date > as_of_date:
                continue
            entry_type = str(entry.get("type") or "").strip().upper()
            if entry_type not in {"BUY", "SELL"}:
                continue
            gross_amount = _coerce_float(entry.get("gross_amount")) or 0.0
            if gross_amount <= 0:
                continue
            summary = summary_map.setdefault(
                ticker,
                {
                    "buy_count": 0,
                    "sell_count": 0,
                    "buy_amount": 0.0,
                    "sell_amount": 0.0,
                    "discretionary_sell_count": 0,
                    "discretionary_sell_amount": 0.0,
                    "net_amount_excl_10b5_1": 0.0,
                },
            )
            if entry_type == "BUY":
                summary["buy_count"] = int(summary["buy_count"]) + 1
                summary["buy_amount"] = float(summary["buy_amount"]) + gross_amount
                summary["net_amount_excl_10b5_1"] = float(summary["net_amount_excl_10b5_1"]) + gross_amount
                continue
            summary["sell_count"] = int(summary["sell_count"]) + 1
            summary["sell_amount"] = float(summary["sell_amount"]) + gross_amount
            summary["discretionary_sell_count"] = int(summary["discretionary_sell_count"]) + 1
            summary["discretionary_sell_amount"] = float(summary["discretionary_sell_amount"]) + gross_amount
            summary["net_amount_excl_10b5_1"] = float(summary["net_amount_excl_10b5_1"]) - gross_amount
    return summary_map


def _load_cache_payload(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _cache_key(*, ticker: str, as_of_date: str, lookback_days: int) -> str:
    return f"{str(ticker).strip().upper()}|{str(as_of_date).strip()}|{max(1, int(lookback_days))}"


def _is_cache_window_fresh(window: object, *, ttl_hours: int) -> bool:
    if not isinstance(window, dict):
        return False
    refreshed_at = str(window.get("refreshed_at") or "").strip()
    if not refreshed_at:
        return False
    try:
        refreshed = dt.datetime.fromisoformat(refreshed_at.replace("Z", "+00:00"))
    except ValueError:
        return False
    if refreshed.tzinfo is None:
        refreshed = refreshed.replace(tzinfo=dt.timezone.utc)
    age = dt.datetime.now(dt.timezone.utc) - refreshed.astimezone(dt.timezone.utc)
    return age <= dt.timedelta(hours=max(1, int(ttl_hours)))


def _coerce_date(value: object) -> dt.date | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return dt.date.fromisoformat(text[:10])
    except ValueError:
        return None


def _coerce_float(value: object) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(str(value).replace(",", ""))
    except (TypeError, ValueError):
        return None


def _parse_finviz_trade_date(value: str) -> str | None:
    text = str(value or "").strip()
    if not text:
        return None
    for fmt in ("%b %d '%y", "%b %d %Y"):
        try:
            return dt.datetime.strptime(text, fmt).date().isoformat()
        except ValueError:
            continue
    return None


def _normalize_parsed_rows(ticker: str, headers: list[str], rows: list[list[str]]) -> list[dict[str, Any]]:
    output: list[dict[str, Any]] = []
    for row in rows:
        if len(row) < len(headers):
            continue
        payload = {headers[index]: row[index] for index in range(len(headers))}
        trade_text = str(payload.get("Transaction") or "").strip()
        if trade_text == "Buy":
            trade_type = "BUY"
        elif trade_text == "Sale":
            trade_type = "SELL"
        else:
            continue
        shares = int(round(_coerce_float(payload.get("#Shares")) or 0.0))
        price = _coerce_float(payload.get("Cost"))
        gross_amount = _coerce_float(payload.get("Value ($)"))
        shares_owned_after = int(round(_coerce_float(payload.get("#Shares Total")) or 0.0))
        if shares <= 0 or gross_amount is None or gross_amount <= 0:
            continue
        output.append(
            {
                "ticker": ticker,
                "filing_date": None,
                "transaction_date": _parse_finviz_trade_date(str(payload.get("Date") or "")),
                "owner_name": str(payload.get("Insider Trading") or "").strip(),
                "position": str(payload.get("Relationship") or "").strip(),
                "type": trade_type,
                "shares": shares,
                "price": round(price, 4) if price is not None else None,
                "gross_amount": round(gross_amount, 2),
                "net_amount": round(gross_amount if trade_type == "BUY" else -gross_amount, 2),
                "shares_owned_after": shares_owned_after,
                "is_10b5_1": False,
                "source_url": str(payload.get("SEC Form 4 URL") or "").strip(),
            }
        )
    return output


class _FinvizInsiderTableParser(HTMLParser):
    def __init__(self, *, base_url: str) -> None:
        super().__init__()
        self.base_url = base_url
        self._matching_table_depth: int | None = None
        self._table_depth = 0
        self._in_table = False
        self._in_thead = False
        self._in_tbody = False
        self._in_th = False
        self._in_td = False
        self._current_cell: list[str] = []
        self._current_link: str = ""
        self._current_row: list[str] = []
        self.headers: list[str] = []
        self.rows: list[list[str]] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        attr_map = {key: value or "" for key, value in attrs}
        if tag == "table":
            self._table_depth += 1
            classes = attr_map.get("class", "")
            if self._matching_table_depth is None and ("styled-table-new" in classes or "insider-trading-table" in classes):
                self._matching_table_depth = self._table_depth
                self._in_table = True
            return
        if not self._in_table:
            return
        if tag == "thead":
            self._in_thead = True
        elif tag == "tbody":
            self._in_tbody = True
        elif tag == "tr":
            self._current_row = []
        elif tag == "th":
            self._in_th = True
            self._current_cell = []
        elif tag == "td":
            self._in_td = True
            self._current_cell = []
            self._current_link = ""
        elif tag == "a" and self._in_td:
            href = attr_map.get("href", "")
            if href:
                self._current_link = urljoin(self.base_url, href)

    def handle_endtag(self, tag: str) -> None:
        if tag == "table":
            if self._matching_table_depth == self._table_depth:
                if "Insider Trading" not in self.headers:
                    self.headers = []
                    self.rows = []
                self._matching_table_depth = None
                self._in_table = False
            self._table_depth = max(0, self._table_depth - 1)
            return
        if not self._in_table:
            return
        if tag == "thead":
            self._in_thead = False
        elif tag == "tbody":
            self._in_tbody = False
        elif tag == "th" and self._in_th:
            self.headers.append(" ".join("".join(self._current_cell).split()))
            self._in_th = False
        elif tag == "td" and self._in_td:
            value = " ".join("".join(self._current_cell).split())
            if self.headers and len(self._current_row) < len(self.headers) and self.headers[len(self._current_row)] == "SEC Form 4":
                self._current_row.append(value)
                self._current_row.append(self._current_link)
            else:
                self._current_row.append(value)
            self._in_td = False
        elif tag == "tr" and self._current_row:
            if self._in_tbody or (self.headers and len(self._current_row) >= len(self.headers)):
                if self.headers and len(self._current_row) == len(self.headers) + 1:
                    headers = list(self.headers)
                    if headers[-1] == "SEC Form 4":
                        headers[-1] = "SEC Form 4"
                        headers.append("SEC Form 4 URL")
                        self.headers = headers
                self.rows.append(self._current_row)
            self._current_row = []

    def handle_data(self, data: str) -> None:
        if self._in_th or self._in_td:
            self._current_cell.append(data)
