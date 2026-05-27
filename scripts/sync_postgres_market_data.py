#!/usr/bin/env python3
from __future__ import annotations

import argparse
from dataclasses import dataclass
import datetime as dt
import json
from pathlib import Path
import random
import sys
import time
from typing import TYPE_CHECKING, Any, Iterable, Sequence

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.config import load_app_config, today_label
from src.ticker_filters import load_excluded_tickers
from src.webapp.config import load_webapp_config

if TYPE_CHECKING:
    import pandas as pd
    from psycopg import Connection
    from src.universe import UniverseTicker


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Sync daily market data and ticker metadata into Postgres."
    )
    parser.add_argument("--config", default="", help="Optional market config path.")
    parser.add_argument("--start-date", default="2020-01-01", help="Inclusive history start date (YYYY-MM-DD).")
    parser.add_argument("--end-date", default=today_label(), help="Inclusive history end date (YYYY-MM-DD).")
    parser.add_argument(
        "--incremental-days",
        type=int,
        default=0,
        help="If set, ignore --start-date and sync only the last N calendar days through --end-date.",
    )
    parser.add_argument("--limit", type=int, help="Optional universe limit for smoke runs.")
    parser.add_argument("--tickers", nargs="+", help="Optional explicit ticker list instead of the configured universe.")
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
        "--stale-after-days",
        type=int,
        default=30,
        help="Mark a ticker inactive if its latest available bar is older than this many calendar days from --end-date.",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=5000,
        help="Number of daily bar rows per Postgres executemany batch.",
    )
    parser.add_argument(
        "--database-url",
        default="",
        help="Optional Postgres connection string. Defaults to TICKER_SCREENER_DATABASE_URL.",
    )
    parser.add_argument(
        "--source-label",
        default="yfinance",
        help="Source label stored in synced rows.",
    )
    parser.add_argument(
        "--ensure-schema",
        action="store_true",
        help="Apply sql/postgres_app_schema.sql before syncing.",
    )
    parser.add_argument(
        "--manifest-path",
        default="",
        help="Optional explicit path for a JSON sync manifest.",
    )
    return parser.parse_args()


@dataclass(frozen=True)
class DownloadChunkResult:
    tickers: list[str]
    history_by_ticker: dict[str, "pd.DataFrame"]
    error: str | None = None


@dataclass(frozen=True)
class TickerSyncOutcome:
    ticker: str
    status: str
    reason: str
    bar_count: int = 0
    first_trade_date: str | None = None
    last_trade_date: str | None = None
    is_active: bool | None = None
    recovered_individually: bool = False


def _looks_retryable_error(message: str) -> bool:
    lowered = message.lower()
    retry_markers = [
        "429",
        "too many requests",
        "rate limit",
        "timed out",
        "timeout",
        "temporarily unavailable",
        "connection reset",
        "connection aborted",
        "connection refused",
        "server error",
        "bad gateway",
        "gateway timeout",
        "service unavailable",
        "remote end closed connection",
    ]
    return any(marker in lowered for marker in retry_markers)


def _sleep_with_jitter(base_seconds: float, attempt: int) -> float:
    delay = max(base_seconds, 0.0) * (2 ** max(attempt - 1, 0))
    jitter = random.uniform(0.0, max(base_seconds, 0.0))
    total = delay + jitter
    if total > 0:
        time.sleep(total)
    return total


def _resolve_date_window(args: argparse.Namespace) -> tuple[str, str]:
    end_date = dt.date.fromisoformat(args.end_date)
    if int(args.incremental_days) > 0:
        start_date = end_date - dt.timedelta(days=int(args.incremental_days))
        return start_date.isoformat(), end_date.isoformat()
    return args.start_date, end_date.isoformat()


def _manual_tickers(symbols: list[str], excluded: set[str]) -> list["UniverseTicker"]:
    from src.universe import UniverseTicker

    seen: set[str] = set()
    result: list[UniverseTicker] = []
    for raw in symbols:
        symbol = str(raw).strip().upper()
        if not symbol or symbol in excluded or symbol in seen:
            continue
        seen.add(symbol)
        result.append(UniverseTicker(symbol=symbol))
    return result


def _load_target_universe(args: argparse.Namespace) -> tuple[Any, list["UniverseTicker"]]:
    from src.universe import load_universe

    config = load_app_config(args.config or None)
    excluded = load_excluded_tickers(config)
    if args.tickers:
        return config, _manual_tickers(args.tickers, excluded)
    return config, load_universe(config, limit=args.limit)


def _chunked(items: Sequence[str], size: int) -> Iterable[Sequence[str]]:
    if size <= 0:
        raise ValueError("chunk size must be positive")
    for index in range(0, len(items), size):
        yield items[index : index + size]


def _normalize_download_chunk(data: "pd.DataFrame", tickers: list[str]) -> dict[str, "pd.DataFrame"]:
    import pandas as pd

    frames: dict[str, pd.DataFrame] = {}
    if data is None or data.empty:
        return frames

    if isinstance(data.columns, pd.MultiIndex):
        available = set(data.columns.get_level_values(0))
        for ticker in tickers:
            if ticker not in available:
                continue
            frame = data[ticker].copy()
            frame.columns = [str(column) for column in frame.columns]
            frames[ticker] = frame
        return frames

    if tickers:
        frame = data.copy()
        frame.columns = [str(column) for column in frame.columns]
        frames[tickers[0]] = frame
    return frames


def _download_history(
    tickers: list[str],
    start_date: str,
    end_date: str,
    chunk_size: int,
    *,
    max_retries: int,
    retry_base_seconds: float,
    chunk_sleep_seconds: float,
) -> Iterable[DownloadChunkResult]:
    import pandas as pd
    import yfinance as yf

    for chunk_index, chunk in enumerate(_chunked(tickers, chunk_size), start=1):
        chunk_list = list(chunk)
        print(
            f"downloading chunk {chunk_index}: {chunk_list[0]}..{chunk_list[-1]} ({len(chunk_list)} tickers)",
            flush=True,
        )
        if chunk_sleep_seconds > 0:
            time.sleep(chunk_sleep_seconds)
        last_error: str | None = None
        data = None
        for attempt in range(1, max_retries + 1):
            try:
                data = yf.download(
                    tickers=chunk_list,
                    start=start_date,
                    end=(pd.Timestamp(end_date) + pd.Timedelta(days=1)).date().isoformat(),
                    interval="1d",
                    auto_adjust=False,
                    progress=False,
                    group_by="ticker",
                    threads=False,
                )
                last_error = None
                break
            except Exception as exc:
                last_error = f"chunk download failed: {exc}"
                if attempt >= max_retries or not _looks_retryable_error(str(exc)):
                    break
                waited = _sleep_with_jitter(retry_base_seconds, attempt)
                print(
                    f"retrying chunk={chunk_index} attempt={attempt + 1}/{max_retries} wait={waited:.1f}s reason={exc}",
                    flush=True,
                )
        if data is None and last_error is not None:
            yield DownloadChunkResult(
                tickers=chunk_list,
                history_by_ticker={},
                error=last_error,
            )
            continue
        yield DownloadChunkResult(
            tickers=chunk_list,
            history_by_ticker=_normalize_download_chunk(data, chunk_list),
            error=None,
        )


def _clean_nullable_number(value: object) -> float | None:
    import pandas as pd

    if value is None or pd.isna(value):
        return None
    return float(value)


def _clean_nullable_int(value: object) -> int | None:
    import pandas as pd

    if value is None or pd.isna(value):
        return None
    return int(value)


def _normalize_history_frame(history: "pd.DataFrame" | None) -> "pd.DataFrame":
    import pandas as pd

    if history is None or history.empty:
        return pd.DataFrame()
    frame = history.copy()
    frame.columns = [str(column) for column in frame.columns]
    if "Adj Close" not in frame.columns and "Close" in frame.columns:
        frame["Adj Close"] = frame["Close"]
    if "Dividends" not in frame.columns:
        frame["Dividends"] = 0.0
    if "Stock Splits" not in frame.columns:
        frame["Stock Splits"] = 0.0
    frame = frame.sort_index()
    if "Close" in frame.columns:
        frame = frame[frame["Close"].notna()]
    return frame


def _frame_window(frame: "pd.DataFrame", start_date: dt.date, end_date: dt.date) -> "pd.DataFrame":
    import pandas as pd

    if frame.empty:
        return frame
    index = pd.to_datetime(frame.index)
    return frame[(index.date >= start_date) & (index.date <= end_date)]


def _download_single_history(
    ticker: str,
    start_date: str,
    end_date: str,
    *,
    max_retries: int,
    retry_base_seconds: float,
    single_ticker_sleep_seconds: float,
) -> tuple["pd.DataFrame", str | None]:
    import pandas as pd
    import yfinance as yf

    if single_ticker_sleep_seconds > 0:
        time.sleep(single_ticker_sleep_seconds)
    last_error: str | None = None
    frame = None
    for attempt in range(1, max_retries + 1):
        try:
            frame = yf.download(
                tickers=[ticker],
                start=start_date,
                end=(pd.Timestamp(end_date) + pd.Timedelta(days=1)).date().isoformat(),
                interval="1d",
                auto_adjust=False,
                progress=False,
                group_by="ticker",
                threads=False,
            )
            last_error = None
            break
        except Exception as exc:
            last_error = f"single-window fetch failed: {exc}"
            if attempt >= max_retries or not _looks_retryable_error(str(exc)):
                break
            waited = _sleep_with_jitter(retry_base_seconds, attempt)
            print(
                f"retrying ticker={ticker} window_fetch attempt={attempt + 1}/{max_retries} wait={waited:.1f}s reason={exc}",
                flush=True,
            )
    if frame is None and last_error is not None:
        return pd.DataFrame(), last_error
    normalized = _normalize_download_chunk(frame, [ticker]).get(ticker)
    return _normalize_history_frame(normalized), None


def _download_max_history(
    ticker: str,
    *,
    max_retries: int,
    retry_base_seconds: float,
    single_ticker_sleep_seconds: float,
) -> tuple["pd.DataFrame", str | None]:
    import yfinance as yf

    if single_ticker_sleep_seconds > 0:
        time.sleep(single_ticker_sleep_seconds)
    frame = None
    last_error: str | None = None
    for attempt in range(1, max_retries + 1):
        try:
            frame = yf.Ticker(ticker).history(period="max", interval="1d", auto_adjust=False, actions=True)
            last_error = None
            break
        except Exception as exc:
            last_error = f"max-history fetch failed: {exc}"
            if attempt >= max_retries or not _looks_retryable_error(str(exc)):
                break
            waited = _sleep_with_jitter(retry_base_seconds, attempt)
            print(
                f"retrying ticker={ticker} max_history attempt={attempt + 1}/{max_retries} wait={waited:.1f}s reason={exc}",
                flush=True,
            )
    if frame is None and last_error is not None:
        return _normalize_history_frame(None), last_error
    return _normalize_history_frame(frame), None


def _classify_history_frame(
    ticker: str,
    frame: "pd.DataFrame",
    requested_start: dt.date,
    requested_end: dt.date,
    stale_after_days: int,
    *,
    recovered_individually: bool = False,
) -> TickerSyncOutcome:
    import pandas as pd

    if frame.empty:
        return TickerSyncOutcome(
            ticker=ticker,
            status="failed_no_window_history",
            reason="source returned no daily bars in requested window",
            is_active=None,
            recovered_individually=recovered_individually,
        )

    index = pd.to_datetime(frame.index)
    first_date = pd.Timestamp(index.min()).date()
    last_date = pd.Timestamp(index.max()).date()
    bar_count = len(frame)
    stale_cutoff = requested_end - dt.timedelta(days=stale_after_days)

    status = "synced_full_window"
    reason_parts: list[str] = []
    is_active = True

    if first_date > requested_start:
        status = "synced_partial_window"
        reason_parts.append(f"history starts {first_date.isoformat()} after requested start {requested_start.isoformat()}")

    if last_date < stale_cutoff:
        status = "synced_inactive"
        is_active = False
        reason_parts.append(f"latest bar {last_date.isoformat()} is older than stale cutoff {stale_cutoff.isoformat()}")

    if not reason_parts:
        reason_parts.append("full requested window available")

    return TickerSyncOutcome(
        ticker=ticker,
        status=status,
        reason="; ".join(reason_parts),
        bar_count=bar_count,
        first_trade_date=first_date.isoformat(),
        last_trade_date=last_date.isoformat(),
        is_active=is_active,
        recovered_individually=recovered_individually,
    )


def _diagnose_missing_ticker(
    ticker: str,
    requested_start: dt.date,
    requested_end: dt.date,
    stale_after_days: int,
    *,
    chunk_error: str | None = None,
    max_retries: int,
    retry_base_seconds: float,
    single_ticker_sleep_seconds: float,
) -> tuple["pd.DataFrame", TickerSyncOutcome]:
    single_window, single_error = _download_single_history(
        ticker=ticker,
        start_date=requested_start.isoformat(),
        end_date=requested_end.isoformat(),
        max_retries=max_retries,
        retry_base_seconds=retry_base_seconds,
        single_ticker_sleep_seconds=single_ticker_sleep_seconds,
    )
    if not single_window.empty:
        return single_window, _classify_history_frame(
            ticker,
            single_window,
            requested_start,
            requested_end,
            stale_after_days,
            recovered_individually=True,
        )

    max_history, max_error = _download_max_history(
        ticker,
        max_retries=max_retries,
        retry_base_seconds=retry_base_seconds,
        single_ticker_sleep_seconds=single_ticker_sleep_seconds,
    )
    if max_history.empty:
        reason_parts = [part for part in [chunk_error, single_error, max_error, "no usable history available from source"] if part]
        if any(_looks_retryable_error(part) for part in reason_parts):
            status = "failed_rate_limited"
        else:
            status = "failed_no_history_available"
        return max_history, TickerSyncOutcome(
            ticker=ticker,
            status=status,
            reason="; ".join(reason_parts),
            is_active=None,
        )

    first_date = max_history.index.min().date()
    last_date = max_history.index.max().date()
    window = _frame_window(max_history, requested_start, requested_end)
    if not window.empty:
        outcome = _classify_history_frame(
            ticker,
            window,
            requested_start,
            requested_end,
            stale_after_days,
            recovered_individually=True,
        )
        return window, outcome

    if last_date < requested_start:
        return window, TickerSyncOutcome(
            ticker=ticker,
            status="skipped_delisted_before_window",
            reason=(
                f"max history ends {last_date.isoformat()} before requested start {requested_start.isoformat()}"
            ),
            bar_count=len(max_history),
            first_trade_date=first_date.isoformat(),
            last_trade_date=last_date.isoformat(),
            is_active=False,
        )

    if first_date > requested_start:
        return window, TickerSyncOutcome(
            ticker=ticker,
            status="skipped_listed_after_requested_end",
            reason=(
                f"max history starts {first_date.isoformat()} after requested end {requested_end.isoformat()}"
            ),
            bar_count=len(max_history),
            first_trade_date=first_date.isoformat(),
            last_trade_date=last_date.isoformat(),
            is_active=True,
        )

    return window, TickerSyncOutcome(
        ticker=ticker,
        status="failed_missing_window_overlap",
        reason=(
            f"history exists ({first_date.isoformat()}..{last_date.isoformat()}) but no overlap rows were returned for requested window"
        ),
        bar_count=len(max_history),
        first_trade_date=first_date.isoformat(),
        last_trade_date=last_date.isoformat(),
        is_active=(last_date >= requested_end - dt.timedelta(days=stale_after_days)),
    )


def _utc_now() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc).replace(microsecond=0)


def _connect(database_url: str) -> "Connection[Any]":
    try:
        import psycopg
    except ImportError as exc:
        raise RuntimeError(
            "psycopg is not installed. Install requirements-web.txt before running this script."
        ) from exc

    return psycopg.connect(database_url)


def _ensure_schema(connection: "Connection[Any]") -> None:
    schema_path = PROJECT_ROOT / "sql" / "postgres_app_schema.sql"
    with connection.cursor() as cursor:
        cursor.execute(schema_path.read_text(encoding="utf-8"))
    connection.commit()


def _upsert_metadata(connection: "Connection[Any]", universe: list["UniverseTicker"], source_label: str, updated_at: dt.datetime) -> int:
    rows = [
        (
            item.symbol,
            item.exchange,
            item.sector,
            item.industry,
            True,
            "USD",
            source_label,
            updated_at,
        )
        for item in universe
    ]
    sql = """
        INSERT INTO ticker_metadata (
          ticker, exchange, sector, industry, is_active, currency, source, updated_at
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (ticker) DO UPDATE SET
          exchange = EXCLUDED.exchange,
          sector = EXCLUDED.sector,
          industry = EXCLUDED.industry,
          is_active = EXCLUDED.is_active,
          currency = EXCLUDED.currency,
          source = EXCLUDED.source,
          updated_at = EXCLUDED.updated_at
    """
    with connection.cursor() as cursor:
        cursor.executemany(sql, rows)
    connection.commit()
    return len(rows)


def _update_metadata_active_flags(
    connection: "Connection[Any]",
    outcomes: list[TickerSyncOutcome],
    updated_at: dt.datetime,
) -> int:
    rows = [
        (outcome.is_active, updated_at, outcome.ticker)
        for outcome in outcomes
        if outcome.is_active is not None
    ]
    if not rows:
        return 0
    sql = """
        UPDATE ticker_metadata
        SET is_active = %s,
            updated_at = %s
        WHERE ticker = %s
    """
    with connection.cursor() as cursor:
        cursor.executemany(sql, rows)
    connection.commit()
    return len(rows)


def _build_daily_bar_rows(
    history_by_ticker: dict[str, "pd.DataFrame"],
    source_label: str,
    updated_at: dt.datetime,
) -> list[tuple[object, ...]]:
    import pandas as pd

    rows: list[tuple[object, ...]] = []
    for ticker, history in history_by_ticker.items():
        frame = _normalize_history_frame(history)
        if frame.empty:
            continue
        frame = frame.reset_index()
        date_column = "Date" if "Date" in frame.columns else str(frame.columns[0])

        for _, row in frame.iterrows():
            rows.append(
                (
                    ticker,
                    pd.Timestamp(row[date_column]).date(),
                    _clean_nullable_number(row.get("Open")),
                    _clean_nullable_number(row.get("High")),
                    _clean_nullable_number(row.get("Low")),
                    _clean_nullable_number(row.get("Close")),
                    _clean_nullable_number(row.get("Adj Close")),
                    _clean_nullable_int(row.get("Volume")),
                    _clean_nullable_number(row.get("Dividends")) or 0.0,
                    _clean_nullable_number(row.get("Stock Splits")) or 1.0,
                    source_label,
                    updated_at,
                )
            )
    rows.sort(key=lambda item: (str(item[0]), item[1]))
    return rows


def _upsert_daily_bars(connection: "Connection[Any]", rows: list[tuple[object, ...]], batch_size: int) -> int:
    if not rows:
        return 0
    sql = """
        INSERT INTO daily_bars (
          ticker, trade_date, open, high, low, close, adj_close, volume,
          dividend, split_factor, source, updated_at
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (ticker, trade_date) DO UPDATE SET
          open = EXCLUDED.open,
          high = EXCLUDED.high,
          low = EXCLUDED.low,
          close = EXCLUDED.close,
          adj_close = EXCLUDED.adj_close,
          volume = EXCLUDED.volume,
          dividend = EXCLUDED.dividend,
          split_factor = EXCLUDED.split_factor,
          source = EXCLUDED.source,
          updated_at = EXCLUDED.updated_at
    """
    applied = 0
    with connection.cursor() as cursor:
        for index in range(0, len(rows), batch_size):
            batch = rows[index : index + batch_size]
            cursor.executemany(sql, batch)
            applied += len(batch)
    connection.commit()
    return applied


def _write_manifest(
    manifest_path: Path,
    *,
    start_date: str,
    end_date: str,
    ticker_count: int,
    metadata_row_count: int,
    bar_row_count: int,
    downloaded_ticker_count: int,
    source_label: str,
    database_url: str,
    updated_active_count: int,
    outcomes: list[TickerSyncOutcome],
) -> None:
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    failures = [
        {
            "ticker": outcome.ticker,
            "status": outcome.status,
            "reason": outcome.reason,
            "first_trade_date": outcome.first_trade_date,
            "last_trade_date": outcome.last_trade_date,
        }
        for outcome in outcomes
        if outcome.status.startswith("failed") or outcome.status.startswith("skipped")
    ]
    partials = [
        {
            "ticker": outcome.ticker,
            "status": outcome.status,
            "reason": outcome.reason,
            "first_trade_date": outcome.first_trade_date,
            "last_trade_date": outcome.last_trade_date,
        }
        for outcome in outcomes
        if outcome.status in {"synced_partial_window", "synced_inactive"}
    ]
    payload = {
        "generated_at": _utc_now().isoformat(),
        "start_date": start_date,
        "end_date": end_date,
        "ticker_count": ticker_count,
        "ticker_count_with_history": downloaded_ticker_count,
        "ticker_metadata_rows": metadata_row_count,
        "daily_bar_rows": bar_row_count,
        "source_label": source_label,
        "database_url_configured": bool(database_url),
        "updated_active_flags": updated_active_count,
        "failure_count": len(failures),
        "partial_count": len(partials),
        "failures": failures,
        "partials": partials,
    }
    manifest_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def main() -> int:
    args = parse_args()
    _, universe = _load_target_universe(args)
    start_date, end_date = _resolve_date_window(args)
    requested_start = dt.date.fromisoformat(start_date)
    requested_end = dt.date.fromisoformat(end_date)

    web_config = load_webapp_config()
    database_url = (args.database_url or web_config.database_url).strip()
    if not database_url:
        raise RuntimeError(
            "No Postgres connection string configured. Pass --database-url or set TICKER_SCREENER_DATABASE_URL."
        )

    symbols = [item.symbol for item in universe]
    print(f"target_ticker_count={len(symbols)}", flush=True)
    print(f"date_window={start_date}..{end_date}", flush=True)

    manifest_path = (
        Path(args.manifest_path)
        if args.manifest_path
        else PROJECT_ROOT / "artifacts" / "raw" / f"postgres_sync_manifest_{today_label()}.json"
    )

    metadata_rows = 0
    total_bar_rows = 0
    downloaded_ticker_count = 0
    updated_active_count = 0
    updated_at = _utc_now()
    outcomes: list[TickerSyncOutcome] = []

    with _connect(database_url) as connection:
        if args.ensure_schema:
            _ensure_schema(connection)
            print("schema=ensured", flush=True)

        metadata_rows = _upsert_metadata(connection, universe, args.source_label, updated_at)
        print(f"ticker_metadata_rows={metadata_rows}", flush=True)

        for chunk_result in _download_history(
            symbols,
            start_date,
            end_date,
            args.chunk_size,
            max_retries=args.max_retries,
            retry_base_seconds=args.retry_base_seconds,
            chunk_sleep_seconds=args.chunk_sleep_seconds,
        ):
            normalized_histories: dict[str, Any] = {}
            chunk_outcomes: list[TickerSyncOutcome] = []

            for ticker in chunk_result.tickers:
                history = _normalize_history_frame(chunk_result.history_by_ticker.get(ticker))
                if not history.empty:
                    outcome = _classify_history_frame(
                        ticker,
                        history,
                        requested_start,
                        requested_end,
                        args.stale_after_days,
                    )
                    normalized_histories[ticker] = history
                    chunk_outcomes.append(outcome)
                    continue

                diagnosed_history, outcome = _diagnose_missing_ticker(
                    ticker,
                    requested_start,
                    requested_end,
                    args.stale_after_days,
                    chunk_error=chunk_result.error,
                    max_retries=args.max_retries,
                    retry_base_seconds=args.retry_base_seconds,
                    single_ticker_sleep_seconds=args.single_ticker_sleep_seconds,
                )
                if not diagnosed_history.empty and outcome.status.startswith("synced"):
                    normalized_histories[ticker] = diagnosed_history
                chunk_outcomes.append(outcome)

            outcomes.extend(chunk_outcomes)
            downloaded_ticker_count += len(normalized_histories)

            bar_rows = _build_daily_bar_rows(normalized_histories, args.source_label, updated_at)
            applied = _upsert_daily_bars(connection, bar_rows, args.batch_size)
            total_bar_rows += applied
            failed = [outcome for outcome in chunk_outcomes if outcome.status.startswith("failed") or outcome.status.startswith("skipped")]
            partial = [outcome for outcome in chunk_outcomes if outcome.status in {"synced_partial_window", "synced_inactive"}]
            print(
                " ".join(
                    [
                        f"chunk_tickers_with_history={len(normalized_histories)}",
                        f"chunk_daily_bar_rows={applied}",
                        f"chunk_failures={len(failed)}",
                        f"chunk_partials={len(partial)}",
                        f"total_daily_bar_rows={total_bar_rows}",
                    ]
                ),
                flush=True,
            )
            for outcome in failed:
                print(f"failed_ticker={outcome.ticker} status={outcome.status} reason={outcome.reason}", flush=True)
            for outcome in partial:
                print(f"partial_ticker={outcome.ticker} status={outcome.status} reason={outcome.reason}", flush=True)

        updated_active_count = _update_metadata_active_flags(connection, outcomes, updated_at)
        print(f"updated_active_flags={updated_active_count}", flush=True)

    _write_manifest(
        manifest_path,
        start_date=start_date,
        end_date=end_date,
        ticker_count=len(symbols),
        metadata_row_count=metadata_rows,
        bar_row_count=total_bar_rows,
        downloaded_ticker_count=downloaded_ticker_count,
        source_label=args.source_label,
        database_url=database_url,
        updated_active_count=updated_active_count,
        outcomes=outcomes,
    )
    total_failures = len([outcome for outcome in outcomes if outcome.status.startswith("failed") or outcome.status.startswith("skipped")])
    total_partials = len([outcome for outcome in outcomes if outcome.status in {"synced_partial_window", "synced_inactive"}])
    print(f"summary_failures={total_failures}", flush=True)
    print(f"summary_partials={total_partials}", flush=True)
    print(f"manifest_path={manifest_path}", flush=True)
    print("postgres_sync=done", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
