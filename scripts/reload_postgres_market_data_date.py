#!/usr/bin/env python3
from __future__ import annotations

import argparse
import datetime as dt
from pathlib import Path
import sys

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scripts.sync_postgres_market_data import (  # noqa: E402
    _build_daily_bar_rows,
    _connect,
    _diagnose_missing_ticker,
    _download_history,
    _ensure_schema,
    _normalize_history_frame,
    _upsert_daily_bars,
    _utc_now,
)
from src.exclusion_registry import add_manual_exclusion  # noqa: E402
from src.config import load_app_config  # noqa: E402
from scripts.sync_postgres_market_data import TickerSyncOutcome  # noqa: E402
from src.webapp.config import load_webapp_config  # noqa: E402


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Delete one trade_date from Postgres daily_bars, then re-fetch that date for all tickers in ticker_metadata."
    )
    parser.add_argument("trade_date", help="Trade date to repair (YYYY-MM-DD).")
    parser.add_argument(
        "--database-url",
        default="",
        help="Optional Postgres connection string. Defaults to TICKER_SCREENER_DATABASE_URL.",
    )
    parser.add_argument(
        "--source-label",
        default="yfinance",
        help="Source label stored in reloaded rows.",
    )
    parser.add_argument("--chunk-size", type=int, default=100, help="Number of tickers per yfinance download call.")
    parser.add_argument("--max-retries", type=int, default=4, help="Maximum retry attempts for transient/rate-limit errors.")
    parser.add_argument(
        "--retry-base-seconds",
        type=float,
        default=2.0,
        help="Base backoff seconds for retry delays. Actual delay grows exponentially with jitter.",
    )
    parser.add_argument(
        "--chunk-sleep-seconds",
        type=float,
        default=1.0,
        help="Sleep between chunk download attempts to reduce rate-limit pressure.",
    )
    parser.add_argument(
        "--single-ticker-sleep-seconds",
        type=float,
        default=0.5,
        help="Sleep before single-ticker diagnostic fetches to reduce rate-limit pressure.",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=5000,
        help="Number of daily bar rows per Postgres executemany batch.",
    )
    parser.add_argument(
        "--ensure-schema",
        action="store_true",
        help="Apply sql/postgres_app_schema.sql before deleting/reloading.",
    )
    return parser.parse_args()


def _load_all_metadata_tickers(connection) -> list[str]:
    sql = """
        SELECT ticker
        FROM ticker_metadata
        ORDER BY ticker ASC
    """
    with connection.cursor() as cursor:
        cursor.execute(sql)
        rows = cursor.fetchall()
    return [str(row[0]).strip().upper() for row in rows if row and str(row[0]).strip()]


def _delete_trade_date(connection, trade_date: dt.date) -> int:
    with connection.cursor() as cursor:
        cursor.execute("DELETE FROM daily_bars WHERE trade_date = %s", (trade_date,))
        deleted = cursor.rowcount or 0
    connection.commit()
    return int(deleted)


def _should_auto_exclude_delisted(outcome: TickerSyncOutcome) -> bool:
    return outcome.status in {"failed_no_history_available", "skipped_delisted_before_window"}


def _auto_exclude_delisted_tickers(outcomes: list[TickerSyncOutcome]) -> int:
    candidates = sorted({outcome.ticker for outcome in outcomes if _should_auto_exclude_delisted(outcome)})
    if not candidates:
        return 0
    config = load_app_config(None)
    added = 0
    for ticker in candidates:
        add_manual_exclusion(config, ticker=ticker, reason="delisted")
        added += 1
    return added


def main() -> int:
    args = parse_args()
    trade_date = dt.date.fromisoformat(args.trade_date)
    database_url = (args.database_url or load_webapp_config().database_url).strip()
    if not database_url:
      raise RuntimeError("No Postgres connection string configured. Pass --database-url or set TICKER_SCREENER_DATABASE_URL.")

    with _connect(database_url) as connection:
        if connection is None:
            raise RuntimeError("psycopg is not available; cannot connect to Postgres.")

        if args.ensure_schema:
            _ensure_schema(connection)
            print("schema=ensured", flush=True)

        tickers = _load_all_metadata_tickers(connection)
        if not tickers:
            raise RuntimeError("No tickers found in ticker_metadata.")

        deleted = _delete_trade_date(connection, trade_date)
        print(f"trade_date={trade_date.isoformat()} deleted_rows={deleted}", flush=True)
        print(f"target_ticker_count={len(tickers)}", flush=True)

        updated_at = _utc_now()
        total_rows = 0
        total_chunks = 0
        total_failures = 0
        total_overflow_skipped = 0
        outcomes: list[TickerSyncOutcome] = []

        for chunk_result in _download_history(
            tickers,
            trade_date.isoformat(),
            trade_date.isoformat(),
            args.chunk_size,
            max_retries=args.max_retries,
            retry_base_seconds=args.retry_base_seconds,
            chunk_sleep_seconds=args.chunk_sleep_seconds,
        ):
            total_chunks += 1
            normalized_histories = {
                ticker: history
                for ticker in chunk_result.tickers
                if not (history := _normalize_history_frame(chunk_result.history_by_ticker.get(ticker))).empty
            }
            direct_history_tickers = set(normalized_histories)
            missing_tickers = [ticker for ticker in chunk_result.tickers if ticker not in normalized_histories]

            for ticker in missing_tickers:
                diagnosed_history, outcome = _diagnose_missing_ticker(
                    ticker,
                    trade_date,
                    trade_date,
                    30,
                    chunk_error=chunk_result.error,
                    max_retries=args.max_retries,
                    retry_base_seconds=args.retry_base_seconds,
                    single_ticker_sleep_seconds=args.single_ticker_sleep_seconds,
                )
                if not diagnosed_history.empty and outcome.status.startswith("synced"):
                    normalized_histories[ticker] = diagnosed_history
                outcomes.append(outcome)

            for ticker in direct_history_tickers:
                history = normalized_histories[ticker]
                first_date = history.index.min().date().isoformat()
                last_date = history.index.max().date().isoformat()
                outcomes.append(
                    TickerSyncOutcome(
                        ticker=ticker,
                        status="reloaded_trade_date",
                        reason="reload script found trade-date history",
                        bar_count=len(history),
                        first_trade_date=first_date,
                        last_trade_date=last_date,
                        is_active=True,
                    )
                )
            total_failures = len([outcome for outcome in outcomes if outcome.status.startswith("failed") or outcome.status.startswith("skipped")])

            bar_rows = _build_daily_bar_rows(normalized_histories, args.source_label, updated_at)
            applied, skipped_overflow = _upsert_daily_bars(connection, bar_rows, args.batch_size)
            total_rows += applied
            total_overflow_skipped += skipped_overflow

            print(
                " ".join(
                    [
                        f"chunk={total_chunks}",
                        f"tickers={len(chunk_result.tickers)}",
                        f"reloaded={len(normalized_histories)}",
                        f"missing={len(missing_tickers)}",
                        f"applied_rows={applied}",
                        f"overflow_skipped={skipped_overflow}",
                        f"running_rows={total_rows}",
                    ]
                ),
                flush=True,
            )
            if chunk_result.error:
                print(f"chunk_error={chunk_result.error}", flush=True)

        print(f"summary_trade_date={trade_date.isoformat()}", flush=True)
        print(f"summary_target_tickers={len(tickers)}", flush=True)
        print(f"summary_reloaded_rows={total_rows}", flush=True)
        print(f"summary_missing_tickers={total_failures}", flush=True)
        print(f"summary_overflow_skipped={total_overflow_skipped}", flush=True)
        auto_excluded = _auto_exclude_delisted_tickers(outcomes)
        print(f"summary_auto_excluded_delisted={auto_excluded}", flush=True)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
