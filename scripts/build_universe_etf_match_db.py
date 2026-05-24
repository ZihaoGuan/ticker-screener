#!/usr/bin/env python3
from __future__ import annotations

import argparse
import datetime as dt
import sqlite3
from pathlib import Path
import sys


PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.config import load_app_config
from src.etf_matcher import (
    infer_theme_tags_for_ticker,
    load_etf_catalog,
    load_ticker_theme_overrides,
    match_etfs_for_ticker,
)
from src.ticker_filters import filter_symbols, load_excluded_tickers
from src.universe import UniverseTicker, load_universe


DEFAULT_DB_PATH = PROJECT_ROOT / "data" / "universe_etf_matches.sqlite"
SCHEMA_PATH = PROJECT_ROOT / "sql" / "universe_etf_match_schema.sql"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build a SQLite database for universe ticker to ETF matches.")
    parser.add_argument("--config", default=str(PROJECT_ROOT / "config" / "market_config.json"))
    parser.add_argument("--db-path", default=str(DEFAULT_DB_PATH))
    parser.add_argument("--limit", type=int, help="Optional universe limit for smoke runs.")
    parser.add_argument("--tickers", nargs="+", help="Optional explicit tickers instead of the full universe.")
    return parser.parse_args()


def _manual_tickers(symbols: list[str], config_path: str) -> list[UniverseTicker]:
    config = load_app_config(config_path)
    excluded = load_excluded_tickers(config)
    tickers: list[UniverseTicker] = []
    for symbol in filter_symbols(symbols, excluded):
        tickers.append(UniverseTicker(symbol=symbol))
    return tickers


def _apply_schema(connection: sqlite3.Connection) -> None:
    connection.executescript(
        """
        DROP TABLE IF EXISTS ticker_etf_matches;
        DROP TABLE IF EXISTS ticker_themes;
        DROP TABLE IF EXISTS ticker_metadata;
        DROP TABLE IF EXISTS etf_match_rules;
        DROP TABLE IF EXISTS etf_catalog;
        """
    )
    connection.executescript(SCHEMA_PATH.read_text(encoding="utf-8"))


def _replace_catalog(connection: sqlite3.Connection, catalog: list[dict[str, object]]) -> None:
    connection.execute("DELETE FROM etf_match_rules")
    connection.execute("DELETE FROM etf_catalog")
    for item in catalog:
        etf_ticker = str(item.get("ticker", "")).strip().upper()
        etf_name = str(item.get("name", "")).strip()
        if not etf_ticker or not etf_name:
            continue
        connection.execute(
            "INSERT INTO etf_catalog (etf_ticker, etf_name) VALUES (?, ?)",
            (etf_ticker, etf_name),
        )
        for sector in item.get("match_sectors", []):
            normalized = str(sector).strip()
            if not normalized:
                continue
            connection.execute(
                "INSERT INTO etf_match_rules (etf_ticker, rule_type, rule_value) VALUES (?, 'sector', ?)",
                (etf_ticker, normalized),
            )
        for theme in item.get("match_themes", []):
            normalized = str(theme).strip()
            if not normalized:
                continue
            connection.execute(
                "INSERT INTO etf_match_rules (etf_ticker, rule_type, rule_value) VALUES (?, 'theme', ?)",
                (etf_ticker, normalized),
            )


def _replace_universe(
    connection: sqlite3.Connection,
    tickers: list[UniverseTicker],
    catalog: list[dict[str, object]],
    overrides: dict[str, list[str]],
) -> tuple[int, int]:
    updated_at = dt.datetime.now(dt.UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    connection.execute("DELETE FROM ticker_etf_matches")
    connection.execute("DELETE FROM ticker_themes")
    connection.execute("DELETE FROM ticker_metadata")
    ticker_count = 0
    match_count = 0
    for item in tickers:
        ticker = item.symbol.strip().upper()
        if not ticker:
            continue
        ticker_count += 1
        connection.execute(
            """
            INSERT INTO ticker_metadata (ticker, sector, industry, exchange, source, updated_at)
            VALUES (?, ?, ?, ?, 'nasdaq-universe', ?)
            """,
            (ticker, item.sector, item.industry, item.exchange, updated_at),
        )
        ticker_themes = infer_theme_tags_for_ticker(
            ticker=ticker,
            sector=item.sector,
            industry=item.industry,
            catalog=catalog,
            overrides=overrides,
        )
        for theme in ticker_themes:
            connection.execute(
                "INSERT INTO ticker_themes (ticker, theme) VALUES (?, ?)",
                (ticker, theme),
            )
        matches = match_etfs_for_ticker(
            sector=item.sector,
            ticker_themes=ticker_themes,
            catalog=catalog,
        )
        for match in matches:
            match_count += 1
            connection.execute(
                """
                INSERT INTO ticker_etf_matches (ticker, etf_ticker, etf_name, match_reason, updated_at)
                VALUES (?, ?, ?, ?, ?)
                """,
                (ticker, match["ticker"], match["name"], match["reason"], updated_at),
            )
    return ticker_count, match_count


def main() -> int:
    args = parse_args()
    config = load_app_config(args.config)
    tickers = _manual_tickers(args.tickers, args.config) if args.tickers else load_universe(config, limit=args.limit)
    catalog = load_etf_catalog()
    overrides = load_ticker_theme_overrides()

    db_path = Path(args.db_path).resolve()
    db_path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(db_path)
    try:
        _apply_schema(connection)
        _replace_catalog(connection, catalog)
        ticker_count, match_count = _replace_universe(connection, tickers, catalog, overrides)
        connection.commit()
    finally:
        connection.close()

    print(f"Wrote universe ETF match database to {db_path}")
    print(f"Tickers loaded: {ticker_count}")
    print(f"ETF matches written: {match_count}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
