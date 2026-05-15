from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable

import requests

from .config import AppConfig


NASDAQ_HEADERS = {
    "authority": "api.nasdaq.com",
    "accept": "application/json, text/plain, */*",
    "user-agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/87.0.4280.141 Safari/537.36"
    ),
    "origin": "https://www.nasdaq.com",
    "sec-fetch-site": "same-site",
    "sec-fetch-mode": "cors",
    "sec-fetch-dest": "empty",
    "referer": "https://www.nasdaq.com/",
    "accept-language": "en-US,en;q=0.9",
}


@dataclass(frozen=True)
class UniverseTicker:
    symbol: str
    sector: str | None = None
    exchange: str | None = None


def _is_supported_symbol(symbol: str) -> bool:
    return bool(symbol) and "." not in symbol and "^" not in symbol


def fetch_exchange_universe(exchange: str, timeout_seconds: int) -> list[UniverseTicker]:
    response = requests.get(
        "https://api.nasdaq.com/api/screener/stocks",
        headers=NASDAQ_HEADERS,
        params={
            "tableonly": "true",
            "limit": "10000",
            "offset": "0",
            "download": "true",
            "exchange": exchange,
        },
        timeout=timeout_seconds,
    )
    response.raise_for_status()
    rows = response.json().get("data", {}).get("rows", [])
    tickers: list[UniverseTicker] = []
    for row in rows:
        symbol = str(row.get("symbol", "")).upper()
        if not _is_supported_symbol(symbol):
            continue
        sector = row.get("sector")
        tickers.append(
            UniverseTicker(
                symbol=symbol,
                sector=sector if isinstance(sector, str) and sector.strip() else None,
                exchange=exchange.upper(),
            )
        )
    return tickers


def dedupe_tickers(tickers: Iterable[UniverseTicker]) -> list[UniverseTicker]:
    deduped: dict[str, UniverseTicker] = {}
    for ticker in tickers:
        deduped.setdefault(ticker.symbol, ticker)
    return list(deduped.values())


def load_universe(config: AppConfig, limit: int | None = None) -> list[UniverseTicker]:
    collected: list[UniverseTicker] = []
    for exchange in config.exchanges:
        collected.extend(fetch_exchange_universe(exchange=exchange, timeout_seconds=config.request_timeout_seconds))
    deduped = dedupe_tickers(collected)
    final_limit = limit if limit is not None else config.max_tickers
    if final_limit is not None:
        return deduped[:final_limit]
    return deduped
