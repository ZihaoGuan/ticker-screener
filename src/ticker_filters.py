from __future__ import annotations

from dataclasses import replace
from contextvars import ContextVar
import hashlib
import csv
from pathlib import Path
import re
from typing import TYPE_CHECKING, Iterable

from .config import AppConfig, project_root

if TYPE_CHECKING:
    from .peg_screen import EarningsEvent
    from .pre_earnings_screen import PreEarningsEvent
    from .universe import UniverseTicker


_FUND_NAME_PATTERN = re.compile(r"\b(?:ETF|FUND|SHARES|TRUST|PROSHARES)\b", re.IGNORECASE)
_LEVERAGED_OR_INVERSE_NAME_PATTERN = re.compile(
    r"\b(?:[2-9](?:\.\d+)?X|DAILY\s+(?:BULL|BEAR|LONG|SHORT)|ULTRA(?:PRO|SHORT)?|LEVERAGED|INVERSE)\b",
    re.IGNORECASE,
)

# Confirmed leveraged/inverse funds omitted by the local ETF catalog. This is
# deliberately separate from configurable/manual exclusions: manual includes
# must never make leveraged products eligible for Martin paper trading.
_LEVERAGED_OR_INVERSE_SYMBOLS = {"KORU", "SOXS", "INTW", "MVLL"}
_REPLAY_FUND_EXCLUSIONS: ContextVar[frozenset[str] | None] = ContextVar("replay_fund_exclusions", default=None)
_FUNDS_CACHE_KEY: tuple[object, ...] | None = None
_FUNDS_CACHE: frozenset[str] = frozenset()


def set_replay_fund_exclusions(symbols: Iterable[str] | None):
    """Pin the hard fund guard for one historical replay context."""
    return _REPLAY_FUND_EXCLUSIONS.set(None if symbols is None else frozenset(normalize_ticker_symbol(s) for s in symbols))


def reset_replay_fund_exclusions(token) -> None:
    _REPLAY_FUND_EXCLUSIONS.reset(token)


def leveraged_or_inverse_provenance() -> dict[str, object]:
    version = "tradingview_fund_exclusions_v1"
    symbols = sorted(leveraged_or_inverse_tickers())
    canonical = version + "\n" + "\n".join(symbols)
    digest = hashlib.sha256(canonical.encode("utf-8")).hexdigest()
    return {"version": version, "sha256": digest, "symbols": symbols}


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


def manual_included_tickers_path(config: AppConfig) -> Path:
    raw_value = str(getattr(config, "manual_included_tickers_file", "") or "").strip()
    if not raw_value:
        return project_root() / "config" / "manual_include_tickers.txt"
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


def special_security_tickers_path(config: AppConfig) -> Path:
    raw_value = str(getattr(config, "special_security_tickers_file", "") or "").strip()
    if not raw_value:
        return project_root() / "artifacts" / "special_security_tickers_to_filter.csv"
    candidate = Path(raw_value)
    if candidate.is_absolute():
        return candidate
    return project_root() / candidate


def normalize_ticker_symbol(symbol: str) -> str:
    return str(symbol).upper().strip().replace("/", ".")


def is_leveraged_or_inverse_fund_name(name: object) -> bool:
    text = str(name or "").strip()
    return bool(text and _FUND_NAME_PATTERN.search(text) and _LEVERAGED_OR_INVERSE_NAME_PATTERN.search(text))


def leveraged_or_inverse_catalog_symbols(catalog: Iterable[dict[str, object]]) -> set[str]:
    return {
        normalize_ticker_symbol(str(item.get("ticker") or ""))
        for item in catalog
        if normalize_ticker_symbol(str(item.get("ticker") or "")) and is_leveraged_or_inverse_fund_name(item.get("name"))
    }


def leveraged_or_inverse_tickers() -> frozenset[str]:
    global _FUNDS_CACHE_KEY, _FUNDS_CACHE
    symbols = set(_LEVERAGED_OR_INVERSE_SYMBOLS)
    generated = project_root() / "config" / "tradingview_leveraged_inverse_etfs.txt"
    try:
        generated_stat = generated.stat()
        generated_key: tuple[object, ...] = (str(generated), generated_stat.st_ino, generated_stat.st_mtime_ns, generated_stat.st_size)
    except OSError:
        generated_key = (str(generated), None, None, None)
    try:
        from .etf_matcher import ETF_CATALOG_PATH
        catalog_stat = ETF_CATALOG_PATH.stat()
        catalog_key: tuple[object, ...] = (str(ETF_CATALOG_PATH), catalog_stat.st_ino, catalog_stat.st_mtime_ns, catalog_stat.st_size)
    except (OSError, ImportError):
        ETF_CATALOG_PATH = None
        catalog_key = (None, None, None, None)
    key = (*generated_key, *catalog_key)
    if key == _FUNDS_CACHE_KEY:
        return _FUNDS_CACHE
    symbols.update(_load_ticker_file(generated))
    try:
        from .etf_matcher import load_etf_catalog

        symbols.update(leveraged_or_inverse_catalog_symbols(load_etf_catalog()))
    except Exception:
        pass
    _FUNDS_CACHE = frozenset(symbols)
    _FUNDS_CACHE_KEY = key
    return _FUNDS_CACHE


def is_leveraged_or_inverse_ticker(ticker: str) -> bool:
    replay_set = _REPLAY_FUND_EXCLUSIONS.get()
    return normalize_ticker_symbol(ticker) in (replay_set if replay_set is not None else leveraged_or_inverse_tickers())


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
                excluded.add(normalize_ticker_symbol(ticker))
    return excluded


def _load_special_security_tickers(path: Path) -> set[str]:
    if not path.exists():
        return set()
    excluded: set[str] = set()
    with path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            ticker = normalize_ticker_symbol(str(row.get("ticker", "")))
            filter_reason = str(row.get("filter_reason", "")).strip()
            if not ticker or ticker == "DXYZ":
                continue
            # Keep common share classes like BRK.A in the universe after slash-to-dot normalization.
            if filter_reason == "share_class_or_structured_suffix" and _is_share_class_ticker(ticker):
                continue
            excluded.add(ticker)
    return excluded


def _is_share_class_ticker(ticker: str) -> bool:
    root, dot, suffix = ticker.partition(".")
    if not root or dot != ".":
        return False
    return len(suffix) == 1 and suffix.isalpha()


def _is_special_suffix_excluded_ticker(ticker: str) -> bool:
    if len(ticker) != 5:
        return False
    return ticker.endswith(("WS", "W", "U", "R"))


def is_excluded_ticker(ticker: str, excluded: set[str]) -> bool:
    return ticker in excluded or _is_special_suffix_excluded_ticker(ticker)


def load_excluded_tickers(config: AppConfig) -> set[str]:
    excluded: set[str] = set()
    for path in (excluded_tickers_path(config), manual_excluded_tickers_path(config)):
        excluded.update(_load_ticker_file(path))
    auto_dir = auto_excluded_tickers_dir(config)
    if auto_dir.exists():
        for path in sorted(auto_dir.glob("*.txt")):
            excluded.update(_load_ticker_file(path))
    try:
        from .etf_matcher import load_etf_catalog

        excluded.update(leveraged_or_inverse_catalog_symbols(load_etf_catalog()))
    except Exception:
        pass
    excluded.update(_load_special_security_tickers(special_security_tickers_path(config)))
    replay_set = _REPLAY_FUND_EXCLUSIONS.get()
    hard_funds = set(replay_set if replay_set is not None else leveraged_or_inverse_tickers())
    excluded.update(hard_funds)
    excluded.difference_update(load_included_tickers(config) - hard_funds)
    return excluded


def load_included_tickers(config: AppConfig) -> set[str]:
    return _load_ticker_file(manual_included_tickers_path(config))


def filter_symbols(symbols: Iterable[str], excluded: set[str]) -> list[str]:
    filtered: list[str] = []
    seen: set[str] = set()
    for symbol in symbols:
        ticker = normalize_ticker_symbol(symbol)
        if not ticker or ticker in seen or is_excluded_ticker(ticker, excluded):
            continue
        seen.add(ticker)
        filtered.append(ticker)
    return filtered


def filter_universe_tickers(tickers: Iterable["UniverseTicker"], excluded: set[str]) -> list["UniverseTicker"]:
    filtered: list["UniverseTicker"] = []
    seen: set[str] = set()
    for item in tickers:
        ticker = normalize_ticker_symbol(item.symbol)
        if not ticker or ticker in seen or is_excluded_ticker(ticker, excluded):
            continue
        seen.add(ticker)
        filtered.append(item if ticker == item.symbol else replace(item, symbol=ticker))
    return filtered


def filter_earnings_events(events: Iterable["EarningsEvent"], excluded: set[str]) -> list["EarningsEvent"]:
    filtered: list["EarningsEvent"] = []
    seen: set[str] = set()
    for item in events:
        ticker = normalize_ticker_symbol(item.ticker)
        if not ticker or ticker in seen or is_excluded_ticker(ticker, excluded):
            continue
        seen.add(ticker)
        filtered.append(item if ticker == item.ticker else replace(item, ticker=ticker))
    return filtered


def filter_pre_earnings_events(events: Iterable["PreEarningsEvent"], excluded: set[str]) -> list["PreEarningsEvent"]:
    filtered: list["PreEarningsEvent"] = []
    seen: set[str] = set()
    for item in events:
        ticker = normalize_ticker_symbol(item.ticker)
        if not ticker or ticker in seen or is_excluded_ticker(ticker, excluded):
            continue
        seen.add(ticker)
        filtered.append(item if ticker == item.ticker else replace(item, ticker=ticker))
    return filtered
