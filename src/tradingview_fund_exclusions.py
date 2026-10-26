"""Refresh the durable leveraged/inverse ETF denylist from TradingView."""
from __future__ import annotations

import datetime as dt
import hashlib
import json
import os
from pathlib import Path
import tempfile
from typing import Any, Callable


from .config import project_root
from .ticker_filters import normalize_ticker_symbol

URL = "https://scanner.tradingview.com/america/scan"
FLAGS = ("Leveraged", "Inverse")
COLUMNS = ["name", "description", "type", "leveraged_flag", "leverage_ratio"]
PAGE_SIZE = 1000
MAX_ROWS = 10000
CONTROL_FLAGS = {"KORU": "Leveraged", "SOXS": "Inverse", "INTW": "Leveraged", "MVLL": "Leveraged"}


def _fetch_page(flag: str, offset: int, limit: int, *, post: Callable[..., Any] | None = None) -> dict[str, Any]:
    if post is None:
        import requests
        post = requests.post
    response = post(URL, json={
        "filter": [{"left": "type", "operation": "equal", "right": "fund"},
                   {"left": "leveraged_flag", "operation": "equal", "right": flag}],
        "options": {"lang": "en"}, "symbols": {"query": {"types": []}, "tickers": []},
        "columns": COLUMNS, "range": [offset, offset + limit],
        "sort": {"sortBy": "name", "sortOrder": "asc"},
    }, timeout=60)
    response.raise_for_status()
    payload = response.json()
    if not isinstance(payload, dict) or not isinstance(payload.get("data"), list):
        raise ValueError("Invalid TradingView response")
    if not isinstance(payload.get("totalCount"), int):
        raise ValueError("TradingView response is missing totalCount")
    return payload


def fetch_classified_funds(*, fetch_page: Callable[[str, int, int], dict[str, Any]] | None = None,
                           page_size: int = PAGE_SIZE) -> list[dict[str, Any]]:
    if not 1 <= page_size <= 1000:
        raise ValueError("page_size must be between 1 and 1000")
    fetch = fetch_page or (lambda flag, offset, limit: _fetch_page(flag, offset, limit))
    by_symbol: dict[str, dict[str, Any]] = {}
    for flag in FLAGS:
        offset = 0
        expected: int | None = None
        qualified_seen: set[str] = set()
        while expected is None or offset < expected:
            payload = fetch(flag, offset, page_size)
            count = payload.get("totalCount")
            rows = payload.get("data")
            if not isinstance(count, int) or count < 0 or not isinstance(rows, list):
                raise ValueError(f"Invalid {flag} page")
            if count > MAX_ROWS:
                raise ValueError(f"TradingView {flag} result exceeds bounded limit of {MAX_ROWS}")
            if expected is None:
                expected = count
            elif count != expected:
                raise ValueError(f"{flag} result count changed during pagination")
            if count == 0:
                raise ValueError(f"TradingView returned zero {flag} funds")
            if not rows and offset < expected:
                raise ValueError(f"Incomplete {flag} response at offset {offset}")
            for item in rows:
                values = item.get("d") if isinstance(item, dict) else None
                ticker = item.get("s") if isinstance(item, dict) else None
                if not isinstance(values, list) or len(values) != len(COLUMNS) or not ticker:
                    raise ValueError(f"Malformed {flag} row at offset {offset}")
                if str(values[2]).lower() != "fund" or values[3] != flag:
                    raise ValueError(f"Mismatched {flag} classification for {ticker}")
                exchange, sep, raw_symbol = str(ticker).partition(":")
                qualified = f"{exchange}:{raw_symbol}" if sep else str(ticker)
                if qualified in qualified_seen:
                    raise ValueError(f"Duplicate {flag} row {qualified}")
                qualified_seen.add(qualified)
                symbol = normalize_ticker_symbol(raw_symbol if sep else ticker)
                if not symbol:
                    raise ValueError("Empty TradingView symbol")
                prior = by_symbol.get(symbol)
                if prior and (prior["exchange"] != (exchange if sep else "") or prior["leveraged_flag"] != flag):
                    raise ValueError(f"Ambiguous exchange or leveraged classification for {symbol}")
                by_symbol[symbol] = {"ticker": symbol, "exchange": exchange if sep else "",
                    "name": values[0], "description": values[1], "type": values[2],
                    "leveraged_flag": flag, "leverage_ratio": values[4]}
            offset += len(rows)
            if offset > expected:
                raise ValueError(f"TradingView returned too many {flag} rows")
            if len(rows) < page_size and offset < expected:
                raise ValueError(f"Incomplete {flag} response: {offset}/{expected}")
        if offset != expected:
            raise ValueError(f"Incomplete {flag} response: {offset}/{expected}")
        if len(qualified_seen) != expected:
            raise ValueError(f"Incomplete {flag} classification: {len(qualified_seen)}/{expected}")
    for ticker, flag in CONTROL_FLAGS.items():
        if ticker not in by_symbol or by_symbol[ticker]["leveraged_flag"] != flag:
            raise ValueError(f"TradingView control classification missing or changed for {ticker}")
    return sorted(by_symbol.values(), key=lambda row: row["ticker"])


def refresh(*, root: Path | None = None, fetch_page: Callable[[str, int, int], dict[str, Any]] | None = None,
            now: dt.datetime | None = None) -> dict[str, Any]:
    root = root or project_root()
    config = root / "config"
    target = config / "tradingview_leveraged_inverse_etfs.txt"
    artifact_root = Path(os.environ.get("TICKER_SCREENER_ARTIFACTS_DIR") or (root / "artifacts"))
    artifact_dir = artifact_root / "tradingview_fund_exclusions"
    config.mkdir(parents=True, exist_ok=True)
    old = target.read_text(encoding="utf-8").splitlines() if target.exists() else []
    old_symbols = {line.split("#", 1)[0].strip() for line in old if line.strip() and not line.lstrip().startswith("#")}
    rows = fetch_classified_funds(fetch_page=fetch_page)
    if not rows:
        raise ValueError("Refusing to promote an empty TradingView classification")
    current_symbols = {row["ticker"] for row in rows}
    symbols = current_symbols | old_symbols
    for old_symbol in sorted(old_symbols - current_symbols):
        rows.append({"ticker": old_symbol, "exchange": "", "name": "", "description": "",
                     "type": "fund", "leveraged_flag": "retained_prior", "leverage_ratio": None})
    rows.sort(key=lambda row: row["ticker"])
    run_time = (now or dt.datetime.now(dt.timezone.utc)).astimezone(dt.timezone.utc)
    timestamp = run_time.isoformat()
    file_body = ("# Generated from TradingView America fund classifications; do not edit.\n"
                 f"# refreshed_at_utc={timestamp}\n" + "".join(f"{s}\n" for s in sorted(symbols)))
    digest = hashlib.sha256(file_body.encode()).hexdigest()
    artifact = {"source": URL, "timestamp_utc": timestamp, "count": len(rows), "fresh_count": len(current_symbols), "sha256": digest,
                "added": sorted(current_symbols - old_symbols), "removed": [], "funds": rows}
    dated = artifact_dir / f"{run_time.strftime('%Y%m%dT%H%M%SZ')}_{digest[:16]}.json"
    artifact_dir.mkdir(parents=True, exist_ok=True)
    artifact_text = json.dumps(artifact, indent=2, sort_keys=True) + "\n"
    if dated.exists():
        if dated.read_text(encoding="utf-8") != artifact_text:
            raise ValueError(f"Refusing to overwrite immutable provenance artifact {dated}")
    else:
        dated.write_text(artifact_text, encoding="utf-8")
    fd, temp_name = tempfile.mkstemp(prefix=f".{target.name}.", dir=config)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            handle.write(file_body)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temp_name, target)
    finally:
        if os.path.exists(temp_name):
            os.unlink(temp_name)
    return artifact
