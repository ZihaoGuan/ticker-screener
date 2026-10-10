#!/usr/bin/env python3
from __future__ import annotations

import argparse
import datetime as dt
import json
import math
import os
from pathlib import Path
import sys
from urllib import parse as urllib_parse
from urllib import request as urllib_request

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scripts.sync_postgres_market_data import (  # noqa: E402
    _DAILY_BAR_UPSERT_SQL,
    _build_daily_bar_rows,
    _connect,
    _download_history,
    _ensure_schema,
    _normalize_history_frame,
    _utc_now,
)
from src.config import load_app_config  # noqa: E402
from src.ticker_filters import filter_symbols, load_excluded_tickers  # noqa: E402
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
        help="Deprecated compatibility option; missing-ticker recovery is batched.",
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
        help="Apply sql/postgres_app_schema.sql before atomically replacing the repaired date.",
    )
    return parser.parse_args()


def _load_active_metadata_tickers(connection) -> tuple[list[str], int]:
    sql = """
        SELECT ticker, COALESCE(is_active, TRUE)
        FROM ticker_metadata
        ORDER BY ticker ASC
    """
    with connection.cursor() as cursor:
        cursor.execute(sql)
        rows = cursor.fetchall()
    active: list[str] = []
    inactive_count = 0
    for row in rows:
        ticker = str(row[0]).strip().upper() if row else ""
        if not ticker:
            continue
        if not bool(row[1]):
            inactive_count += 1
            continue
        active.append(ticker)
    return active, inactive_count


def _filter_reload_tickers(tickers: list[str], excluded: set[str]) -> list[str]:
    return filter_symbols(tickers, excluded)


def _build_massive_histories(
    payload: dict[str, object],
    trade_date: dt.date,
    requested_tickers: set[str],
) -> dict[str, "pd.DataFrame"]:
    import pandas as pd

    histories: dict[str, pd.DataFrame] = {}
    rows = payload.get("results")
    if not isinstance(rows, list):
        return histories
    for row in rows:
        if not isinstance(row, dict):
            continue
        ticker = str(row.get("T") or "").strip().upper()
        if ticker not in requested_tickers:
            continue
        try:
            open_price = float(row["o"])
            high = float(row["h"])
            low = float(row["l"])
            close = float(row["c"])
            volume = int(float(row["v"]))
        except (KeyError, TypeError, ValueError):
            continue
        if min(open_price, high, low, close) <= 0 or volume < 0 or high < max(open_price, close) or low > min(open_price, close):
            continue
        histories[ticker] = pd.DataFrame(
            {
                "Open": [open_price],
                "High": [high],
                "Low": [low],
                "Close": [close],
                # The date repair is for the latest daily bar, where close is the current adjusted close.
                "Adj Close": [close],
                "Volume": [volume],
                "Dividends": [0.0],
                "Stock Splits": [1.0],
            },
            index=pd.DatetimeIndex([pd.Timestamp(trade_date)], name="Date"),
        )
    return histories


def _download_massive_histories(
    trade_date: dt.date,
    requested_tickers: set[str],
    api_key: str,
) -> dict[str, "pd.DataFrame"]:
    query = urllib_parse.urlencode({"adjusted": "false", "apiKey": api_key})
    url = f"https://api.massive.com/v2/aggs/grouped/locale/us/market/stocks/{trade_date.isoformat()}?{query}"
    request = urllib_request.Request(url, headers={"accept": "application/json", "user-agent": "ticker-screener/1.0"})
    with urllib_request.urlopen(request, timeout=60) as response:
        payload = json.loads(response.read().decode("utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("Massive grouped-daily response is not an object.")
    if payload.get("status") not in {None, "OK"}:
        raise ValueError(f"Massive grouped-daily request failed with status={payload.get('status')!r}.")
    return _build_massive_histories(payload, trade_date, requested_tickers)


def _existing_trade_date_row_count(connection, trade_date: dt.date) -> int:
    with connection.cursor() as cursor:
        cursor.execute("SELECT COUNT(*) FROM daily_bars WHERE trade_date = %s", (trade_date,))
        row = cursor.fetchone()
    return int(row[0] or 0) if row else 0


def _validate_replacement_rows(
    rows: list[tuple[object, ...]],
    trade_date: dt.date,
    *,
    minimum_row_count: int,
) -> None:
    if len(rows) < minimum_row_count:
        raise RuntimeError(f"Replacement coverage is too low: rows={len(rows)} minimum={minimum_row_count}.")

    seen: set[str] = set()
    for row in rows:
        ticker, row_date, open_price, high, low, close, adj_close, volume, *_ = row
        if not ticker or row_date != trade_date or str(ticker) in seen:
            raise RuntimeError(f"Invalid replacement row for ticker={ticker!r} date={row_date!r}.")
        seen.add(str(ticker))
        try:
            open_value, high_value, low_value, close_value, adj_close_value = (
                float(open_price),
                float(high),
                float(low),
                float(close),
                float(adj_close),
            )
            volume_value = int(volume)
        except (TypeError, ValueError) as exc:
            raise RuntimeError(f"Invalid OHLCV replacement row for ticker={ticker}.") from exc
        if (
            min(open_value, high_value, low_value, close_value, adj_close_value) <= 0
            or volume_value < 0
            or high_value < max(open_value, close_value)
            or low_value > min(open_value, close_value)
        ):
            raise RuntimeError(f"Invalid OHLCV replacement row for ticker={ticker}.")


def _replace_trade_date_atomically(
    connection,
    trade_date: dt.date,
    rows: list[tuple[object, ...]],
    batch_size: int,
) -> tuple[int, int]:
    try:
        with connection.cursor() as cursor:
            cursor.execute("DELETE FROM daily_bars WHERE trade_date = %s", (trade_date,))
            deleted = int(cursor.rowcount or 0)
            for index in range(0, len(rows), batch_size):
                cursor.executemany(_DAILY_BAR_UPSERT_SQL, rows[index : index + batch_size])
        connection.commit()
    except Exception:
        connection.rollback()
        raise
    return deleted, len(rows)


def _download_yfinance_pass(
    tickers: list[str],
    trade_date: dt.date,
    args: argparse.Namespace,
    *,
    pass_name: str,
    starting_chunk: int,
) -> tuple[dict[str, "pd.DataFrame"], int, list[str]]:
    chunk_total = 0
    unresolved: list[str] = []
    histories: dict[str, "pd.DataFrame"] = {}

    for chunk_result in _download_history(
        tickers,
        trade_date.isoformat(),
        trade_date.isoformat(),
        args.chunk_size,
        max_retries=args.max_retries,
        retry_base_seconds=args.retry_base_seconds,
        chunk_sleep_seconds=args.chunk_sleep_seconds,
    ):
        chunk_total += 1
        normalized_histories = {
            ticker: history
            for ticker in chunk_result.tickers
            if not (history := _normalize_history_frame(chunk_result.history_by_ticker.get(ticker))).empty
        }
        missing_tickers = [ticker for ticker in chunk_result.tickers if ticker not in normalized_histories]
        unresolved.extend(missing_tickers)
        histories.update(normalized_histories)

        print(
            " ".join(
                [
                    f"yfinance_pass={pass_name}",
                    f"chunk={starting_chunk + chunk_total}",
                    f"tickers={len(chunk_result.tickers)}",
                    f"reloaded={len(normalized_histories)}",
                    f"missing={len(missing_tickers)}",
                ]
            ),
            flush=True,
        )
        if chunk_result.error:
            print(f"chunk_error={chunk_result.error}", flush=True)

    return histories, chunk_total, unresolved


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

        active_tickers, inactive_ticker_count = _load_active_metadata_tickers(connection)
        config = load_app_config(None)
        tickers = _filter_reload_tickers(active_tickers, load_excluded_tickers(config))
        if not tickers:
            raise RuntimeError("No eligible tickers found in ticker_metadata.")

        existing_row_count = _existing_trade_date_row_count(connection, trade_date)
        print(f"trade_date={trade_date.isoformat()} existing_rows={existing_row_count}", flush=True)
        print(f"metadata_active_ticker_count={len(active_tickers)}", flush=True)
        print(f"metadata_inactive_ticker_count={inactive_ticker_count}", flush=True)
        print(f"excluded_ticker_count={len(active_tickers) - len(tickers)}", flush=True)
        print(f"target_ticker_count={len(tickers)}", flush=True)

        updated_at = _utc_now()
        total_chunks = 0

        massive_histories: dict[str, "pd.DataFrame"] = {}
        massive_api_key = os.getenv("MASSIVE_API_KEY", "").strip()
        if not massive_api_key:
            print("massive=skipped reason=missing_api_key", flush=True)
        else:
            try:
                massive_histories = _download_massive_histories(trade_date, set(tickers), massive_api_key)
                print(
                    f"massive=success matched_tickers={len(massive_histories)}",
                    flush=True,
                )
            except Exception as exc:
                print(f"massive=failed reason={exc}; falling_back=yfinance", flush=True)
                massive_histories = {}

        yahoo_tickers = [ticker for ticker in tickers if ticker not in massive_histories]
        print(f"yfinance_target_ticker_count={len(yahoo_tickers)}", flush=True)

        yfinance_histories, chunks, retry_tickers = _download_yfinance_pass(
            yahoo_tickers,
            trade_date,
            args,
            pass_name="primary",
            starting_chunk=total_chunks,
        )
        total_chunks += chunks

        # Retry missing bars in batches once, rather than probing each ticker's full history serially.
        retry_histories, chunks, unresolved_tickers = _download_yfinance_pass(
            retry_tickers,
            trade_date,
            args,
            pass_name="retry",
            starting_chunk=total_chunks,
        )
        total_chunks += chunks

        histories = {**massive_histories, **yfinance_histories, **retry_histories}
        source_by_ticker = {ticker: "massive" for ticker in massive_histories}
        source_by_ticker.update({ticker: args.source_label for ticker in yfinance_histories})
        source_by_ticker.update({ticker: args.source_label for ticker in retry_histories})
        bar_rows = _build_daily_bar_rows(histories, args.source_label, updated_at, source_by_ticker)
        baseline_rows = existing_row_count if existing_row_count else len(tickers)
        minimum_row_count = max(1, math.ceil(min(baseline_rows, len(tickers)) * 0.9))
        _validate_replacement_rows(bar_rows, trade_date, minimum_row_count=minimum_row_count)
        deleted, total_rows = _replace_trade_date_atomically(connection, trade_date, bar_rows, args.batch_size)

        print(f"summary_trade_date={trade_date.isoformat()}", flush=True)
        print(f"summary_target_tickers={len(tickers)}", flush=True)
        print(f"summary_reloaded_rows={total_rows}", flush=True)
        print(f"summary_deleted_rows={deleted}", flush=True)
        print(f"summary_missing_tickers={len(unresolved_tickers)}", flush=True)
        print(f"summary_yfinance_retry_tickers={len(retry_tickers)}", flush=True)
        print(f"summary_source_massive_rows={len(massive_histories)}", flush=True)
        print(f"summary_source_{args.source_label}_rows={len(yfinance_histories) + len(retry_histories)}", flush=True)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
