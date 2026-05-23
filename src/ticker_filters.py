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


def manual_excluded_tickers_path(config: AppConfig) -> Path:
    raw_value = str(getattr(config, "manual_excluded_tickers_file", "") or "").strip()
    if not raw_value:
        return project_root() / "config" / "manual_exclude_tickers.txt"
    candidate = Path(raw_value)
    if candidate.is_absolute():
        return candidate
    return project_root() / candidate


def auto_excluded_tickers_dir(config: AppConfig) -> Path:
    raw_value = str(getattr(config, "auto_excluded_tickers_dir", "") or "").strip()
    if not raw_value:
        return project_root() / "config" / "auto_exclude_tickers"
    candidate = Path(raw_value)
    if candidate.is_absolute():
        return candidate
    return project_root() / candidate


def _load_ticker_file(path: Path) -> set[str]:
    if not path.exists():
        return set()
    excluded: set[str] = set()
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.split("#", 1)[0].strip()
        if not line:
            continue
        parts = [part.strip().upper() for part in line.replace(",", " ").split()]
        for ticker in parts:
            if ticker:
                excluded.add(ticker)
    return excluded


def load_excluded_tickers(config: AppConfig) -> set[str]:
    excluded: set[str] = set()
    for path in (excluded_tickers_path(config), manual_excluded_tickers_path(config)):
        excluded.update(_load_ticker_file(path))
    auto_dir = auto_excluded_tickers_dir(config)
    if auto_dir.exists():
        for path in sorted(auto_dir.glob("*.txt")):
            excluded.update(_load_ticker_file(path))
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
