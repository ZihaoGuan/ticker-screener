from __future__ import annotations

from dataclasses import replace
from pathlib import Path
from typing import TYPE_CHECKING, Iterable

from .config import AppConfig, project_root

if TYPE_CHECKING:
    from .peg_screen import EarningsEvent
    from .pre_earnings_screen import PreEarningsEvent
    from .universe import UniverseTicker


def excluded_tickers_path(config: AppConfig) -> Path:
    raw_value = str(config.excluded_tickers_file or "").strip()
    if not raw_value:
        return project_root() / "config" / "smallcap_exclude_tickers.txt"
    candidate = Path(raw_value)
    if candidate.is_absolute():
        return candidate
    return project_root() / candidate


def load_excluded_tickers(config: AppConfig) -> set[str]:
    path = excluded_tickers_path(config)
    if not path.exists():
        return set()
    excluded: set[str] = set()
    for line in path.read_text(encoding="utf-8").splitlines():
        ticker = line.strip().upper()
        if ticker:
            excluded.add(ticker)
    return excluded


def filter_symbols(symbols: Iterable[str], excluded: set[str]) -> list[str]:
    filtered: list[str] = []
    seen: set[str] = set()
    for symbol in symbols:
        ticker = str(symbol).upper().strip()
        if not ticker or ticker in seen or ticker in excluded:
            continue
        seen.add(ticker)
        filtered.append(ticker)
    return filtered


def filter_universe_tickers(tickers: Iterable["UniverseTicker"], excluded: set[str]) -> list["UniverseTicker"]:
    filtered: list["UniverseTicker"] = []
    seen: set[str] = set()
    for item in tickers:
        ticker = item.symbol.upper().strip()
        if not ticker or ticker in seen or ticker in excluded:
            continue
        seen.add(ticker)
        filtered.append(item if ticker == item.symbol else replace(item, symbol=ticker))
    return filtered


def filter_earnings_events(events: Iterable["EarningsEvent"], excluded: set[str]) -> list["EarningsEvent"]:
    filtered: list["EarningsEvent"] = []
    seen: set[str] = set()
    for item in events:
        ticker = item.ticker.upper().strip()
        if not ticker or ticker in seen or ticker in excluded:
            continue
        seen.add(ticker)
        filtered.append(item if ticker == item.ticker else replace(item, ticker=ticker))
    return filtered


def filter_pre_earnings_events(events: Iterable["PreEarningsEvent"], excluded: set[str]) -> list["PreEarningsEvent"]:
    filtered: list["PreEarningsEvent"] = []
    seen: set[str] = set()
    for item in events:
        ticker = item.ticker.upper().strip()
        if not ticker or ticker in seen or ticker in excluded:
            continue
        seen.add(ticker)
        filtered.append(item if ticker == item.ticker else replace(item, ticker=ticker))
    return filtered
