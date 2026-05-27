from __future__ import annotations

import argparse
from dataclasses import dataclass
from typing import Iterable, TypeVar

from .config import AppConfig
from .etf_matcher import (
    infer_theme_tags_for_ticker,
    load_etf_catalog,
    load_ticker_theme_overrides,
    normalize_match_text,
)
from .universe import UniverseTicker, load_universe


T = TypeVar("T")


@dataclass(frozen=True)
class UniverseFilterCriteria:
    filter_precedence: str = "exclude"
    include_sectors: tuple[str, ...] = ()
    exclude_sectors: tuple[str, ...] = ()
    include_industries: tuple[str, ...] = ()
    exclude_industries: tuple[str, ...] = ()
    include_themes: tuple[str, ...] = ()
    exclude_themes: tuple[str, ...] = ()

    @property
    def is_active(self) -> bool:
        return any(
            (
                self.include_sectors,
                self.exclude_sectors,
                self.include_industries,
                self.exclude_industries,
                self.include_themes,
                self.exclude_themes,
            )
        )


def add_universe_filter_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--filter-precedence",
        choices=("exclude", "include"),
        default="exclude",
        help="Resolve overlapping include/exclude matches by giving priority to exclude or include.",
    )
    parser.add_argument("--include-sectors", nargs="+", help="Only include selected sectors.")
    parser.add_argument("--exclude-sectors", nargs="+", help="Exclude selected sectors.")
    parser.add_argument("--include-industries", nargs="+", help="Only include selected industries.")
    parser.add_argument("--exclude-industries", nargs="+", help="Exclude selected industries.")
    parser.add_argument("--include-themes", nargs="+", help="Only include selected theme tags.")
    parser.add_argument("--exclude-themes", nargs="+", help="Exclude selected theme tags.")


def build_filter_criteria_from_args(args: argparse.Namespace) -> UniverseFilterCriteria:
    return UniverseFilterCriteria(
        filter_precedence=_normalize_precedence(getattr(args, "filter_precedence", None)),
        include_sectors=_normalize_tuple(getattr(args, "include_sectors", None)),
        exclude_sectors=_normalize_tuple(getattr(args, "exclude_sectors", None)),
        include_industries=_normalize_tuple(getattr(args, "include_industries", None)),
        exclude_industries=_normalize_tuple(getattr(args, "exclude_industries", None)),
        include_themes=_normalize_tuple(getattr(args, "include_themes", None)),
        exclude_themes=_normalize_tuple(getattr(args, "exclude_themes", None)),
    )


def build_universe_index(config: AppConfig) -> dict[str, UniverseTicker]:
    return {ticker.symbol: ticker for ticker in load_universe(config)}


def filter_universe_by_criteria(
    tickers: Iterable[UniverseTicker],
    criteria: UniverseFilterCriteria,
) -> list[UniverseTicker]:
    if not criteria.is_active:
        return list(tickers)

    catalog = load_etf_catalog()
    overrides = load_ticker_theme_overrides()
    filtered: list[UniverseTicker] = []
    for ticker in tickers:
        sector_key = normalize_match_text(ticker.sector)
        industry_key = normalize_match_text(ticker.industry)
        theme_keys = {
            normalize_match_text(tag)
            for tag in infer_theme_tags_for_ticker(
                ticker=ticker.symbol,
                sector=ticker.sector,
                industry=ticker.industry,
                catalog=catalog,
                overrides=overrides,
            )
        }
        if not _matches_dimension(
            value=sector_key,
            include_values=criteria.include_sectors,
            exclude_values=criteria.exclude_sectors,
            precedence=criteria.filter_precedence,
        ):
            continue
        if not _matches_dimension(
            value=industry_key,
            include_values=criteria.include_industries,
            exclude_values=criteria.exclude_industries,
            precedence=criteria.filter_precedence,
        ):
            continue
        if not _matches_multi_dimension(
            values=theme_keys,
            include_values=criteria.include_themes,
            exclude_values=criteria.exclude_themes,
            precedence=criteria.filter_precedence,
        ):
            continue
        filtered.append(ticker)
    return filtered


def filter_records_by_criteria(
    records: Iterable[T],
    criteria: UniverseFilterCriteria,
    metadata_by_symbol: dict[str, UniverseTicker],
) -> list[T]:
    if not criteria.is_active:
        return list(records)

    catalog = load_etf_catalog()
    overrides = load_ticker_theme_overrides()
    filtered: list[T] = []
    for record in records:
        symbol = str(getattr(record, "ticker", "") or "").upper()
        metadata = metadata_by_symbol.get(symbol)
        sector = getattr(record, "sector", None) or (metadata.sector if metadata else None)
        industry = getattr(record, "industry", None) or (metadata.industry if metadata else None)
        sector_key = normalize_match_text(sector)
        industry_key = normalize_match_text(industry)
        theme_keys = {
            normalize_match_text(tag)
            for tag in infer_theme_tags_for_ticker(
                ticker=symbol,
                sector=sector,
                industry=industry,
                catalog=catalog,
                overrides=overrides,
            )
        }

        if not _matches_dimension(
            value=sector_key,
            include_values=criteria.include_sectors,
            exclude_values=criteria.exclude_sectors,
            precedence=criteria.filter_precedence,
        ):
            continue
        if not _matches_dimension(
            value=industry_key,
            include_values=criteria.include_industries,
            exclude_values=criteria.exclude_industries,
            precedence=criteria.filter_precedence,
        ):
            continue
        if not _matches_multi_dimension(
            values=theme_keys,
            include_values=criteria.include_themes,
            exclude_values=criteria.exclude_themes,
            precedence=criteria.filter_precedence,
        ):
            continue
        filtered.append(record)
    return filtered


def build_filter_option_catalog(config: AppConfig) -> dict[str, list[str]]:
    universe = load_universe(config)
    catalog = load_etf_catalog()
    overrides = load_ticker_theme_overrides()
    sectors = sorted({ticker.sector.strip() for ticker in universe if ticker.sector and ticker.sector.strip()})
    industries = sorted({ticker.industry.strip() for ticker in universe if ticker.industry and ticker.industry.strip()})
    theme_tags: set[str] = set()
    for ticker in universe:
        theme_tags.update(
            tag
            for tag in infer_theme_tags_for_ticker(
                ticker=ticker.symbol,
                sector=ticker.sector,
                industry=ticker.industry,
                catalog=catalog,
                overrides=overrides,
            )
            if tag
        )
    themes = sorted(theme_tags)
    return {"sectors": sectors, "industries": industries, "themes": themes}


def _normalize_tuple(values: Iterable[str] | None) -> tuple[str, ...]:
    if not values:
        return ()
    return tuple(
        normalized
        for normalized in (normalize_match_text(str(value)) for value in values)
        if normalized
    )


def _normalize_precedence(value: str | None) -> str:
    normalized = str(value or "exclude").strip().lower()
    return "include" if normalized == "include" else "exclude"


def _matches_dimension(
    *,
    value: str,
    include_values: tuple[str, ...],
    exclude_values: tuple[str, ...],
    precedence: str,
) -> bool:
    include_match = bool(include_values) and value in include_values
    exclude_match = bool(exclude_values) and value in exclude_values
    if precedence == "include":
        if include_values:
            return include_match
        return not exclude_match
    if exclude_match:
        return False
    if include_values:
        return include_match
    return True


def _matches_multi_dimension(
    *,
    values: set[str],
    include_values: tuple[str, ...],
    exclude_values: tuple[str, ...],
    precedence: str,
) -> bool:
    include_set = set(include_values)
    exclude_set = set(exclude_values)
    include_match = bool(include_set) and bool(values & include_set)
    exclude_match = bool(exclude_set) and bool(values & exclude_set)
    if precedence == "include":
        if include_set:
            return include_match
        return not exclude_match
    if exclude_match:
        return False
    if include_set:
        return include_match
    return True
