#!/usr/bin/env python3
from __future__ import annotations

import argparse
import calendar
import datetime as dt
import fcntl
import json
import math
import os
from pathlib import Path
import sys
import tempfile
from zoneinfo import ZoneInfo
from urllib import parse as urllib_parse
from urllib import request as urllib_request

import requests

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


def _load_metadata_exchanges(connection, tickers: list[str]) -> dict[str, str]:
    with connection.cursor() as cursor:
        cursor.execute("SELECT ticker, exchange FROM ticker_metadata WHERE ticker = ANY(%s)", (tickers,))
        return {str(ticker).strip().upper(): str(exchange).strip().upper() for ticker, exchange in cursor.fetchall() if exchange}


def _filter_reload_tickers(tickers: list[str], excluded: set[str]) -> list[str]:
    return filter_symbols(tickers, excluded)


def _valid_bar(row: tuple[object, ...], trade_date: dt.date) -> bool:
    if row[1] != trade_date:
        return False
    try:
        open_price, high, low, close, adj_close, volume = (float(value) for value in row[2:8])
    except (TypeError, ValueError, OverflowError):
        return False
    return (
        all(math.isfinite(value) and value > 0 for value in (open_price, high, low, close, adj_close))
        and high >= max(open_price, close)
        and low <= min(open_price, close)
        and math.isfinite(volume)
        and volume >= 0
        and volume.is_integer()
    )


def _valid_histories(histories: dict[str, "pd.DataFrame"], trade_date: dt.date) -> dict[str, "pd.DataFrame"]:
    valid = {}
    for ticker, history in histories.items():
        rows = _build_daily_bar_rows({ticker: history}, "validation", _utc_now())
        if len(rows) == 1 and _valid_bar(rows[0], trade_date):
            valid[ticker] = history
    return valid


def _tradingview_eligible(trade_date: dt.date, now: dt.datetime | None = None) -> bool:
    eastern = (now or _utc_now()).astimezone(ZoneInfo("America/New_York"))
    today = eastern.date()
    if _is_trade_session(today) and dt.time(16) <= eastern.time() < dt.time(16, 15):
        return False
    # TradingView's scanner can lag the regular-session close; wait 15 minutes.
    latest = today if _is_trade_session(today) and eastern.time() >= dt.time(16, 15) else today - dt.timedelta(days=1)
    while not _is_trade_session(latest):
        latest -= dt.timedelta(days=1)
    # Scanner OHLC is live during the next session and has no per-row trade date.
    market_open = _is_trade_session(today) and dt.time(9, 30) <= eastern.time() < dt.time(16)
    return trade_date == latest and not market_open


def _download_tradingview_histories(
    trade_date: dt.date, tickers: list[str], exchanges: dict[str, str]
) -> tuple[dict[str, "pd.DataFrame"], int, int, int]:
    import pandas as pd

    columns = ["name", "exchange", "open", "high", "low", "close", "volume", "update_mode"]
    histories: dict[str, pd.DataFrame] = {}
    invalid = ambiguous = failed_batches = 0
    for start in range(0, len(tickers), 500):
        batch = tickers[start : start + 500]
        try:
            response = requests.post(
                "https://scanner.tradingview.com/america/scan",
                json={
                    "filter": [{"left": "name", "operation": "in_range", "right": batch}],
                    "options": {"lang": "en"},
                    "symbols": {"query": {"types": []}, "tickers": []},
                    "columns": columns,
                    "range": [0, 10000],
                },
                timeout=60,
            )
            response.raise_for_status()
            payload = response.json()
            if not isinstance(payload, dict) or not isinstance(payload.get("data"), list):
                raise ValueError("Invalid TradingView scanner response")
        except (requests.RequestException, ValueError) as exc:
            failed_batches += 1
            print(f"tradingview=failed batch={start // 500 + 1} reason={exc}", flush=True)
            continue
        matches: dict[str, list[list[object]]] = {ticker: [] for ticker in batch}
        for item in payload["data"]:
            values = item.get("d") if isinstance(item, dict) else None
            if isinstance(values, list) and len(values) == len(columns) and values[0] is not None:
                ticker = str(values[0]).strip().upper()
                if ticker in matches:
                    matches[ticker].append(values)
        for ticker, candidates in matches.items():
            exchange = exchanges.get(ticker)
            if len(candidates) > 1 and exchange:
                preferred = [row for row in candidates if str(row[1]).upper() == exchange]
                if len(preferred) == 1:
                    candidates = preferred
            if len(candidates) > 1:
                ambiguous += 1
                continue
            if not candidates:
                continue
            _, _, open_price, high, low, close, volume, _ = candidates[0]
            try:
                frame = pd.DataFrame(
                    {"Open": [float(open_price)], "High": [float(high)], "Low": [float(low)],
                     "Close": [float(close)], "Adj Close": [float(close)], "Volume": [float(volume)],
                     "Dividends": [0.0], "Stock Splits": [1.0]},
                    index=pd.DatetimeIndex([pd.Timestamp(trade_date)], name="Date"),
                )
                if _valid_histories({ticker: frame}, trade_date):
                    histories[ticker] = frame
                else:
                    invalid += 1
            except (TypeError, ValueError, OverflowError):
                invalid += 1
    return histories, invalid, ambiguous, failed_batches


def _build_massive_histories(
    payload: dict[str, object],
    trade_date: dt.date,
    requested_tickers: set[str],
) -> dict[str, "pd.DataFrame"]:
    import pandas as pd

    histories: dict[str, pd.DataFrame] = {}
    seen: set[str] = set()
    rows = payload.get("results")
    if not isinstance(rows, list):
        return histories
    for row in rows:
        if not isinstance(row, dict):
            continue
        ticker = str(row.get("T") or "").strip().upper()
        if ticker not in requested_tickers:
            continue
        if ticker in seen:
            histories.pop(ticker)
            continue
        seen.add(ticker)
        try:
            open_price = float(row["o"])
            high = float(row["h"])
            low = float(row["l"])
            close = float(row["c"])
            volume_value = float(row["v"])
            if not math.isfinite(volume_value) or not volume_value.is_integer():
                continue
            volume = int(volume_value)
        except (KeyError, TypeError, ValueError, OverflowError):
            continue
        if not all(math.isfinite(value) and value > 0 for value in (open_price, high, low, close)) or volume < 0 or high < max(open_price, close) or low > min(open_price, close):
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


def _minimum_replacement_row_count(target_ticker_count: int) -> int:
    return max(1, math.ceil(target_ticker_count * 0.9))


def _nth_weekday(year: int, month: int, weekday: int, occurrence: int) -> dt.date:
    first_weekday, days_in_month = calendar.monthrange(year, month)
    day = 1 + (weekday - first_weekday) % 7 + (occurrence - 1) * 7
    if day > days_in_month:
        day -= 7
    return dt.date(year, month, day)


def _last_weekday(year: int, month: int, weekday: int) -> dt.date:
    last = dt.date(year, month, calendar.monthrange(year, month)[1])
    return last - dt.timedelta(days=(last.weekday() - weekday) % 7)


def _easter_sunday(year: int) -> dt.date:
    a = year % 19
    b, c = divmod(year, 100)
    d, e = divmod(b, 4)
    f = (b + 8) // 25
    g = (b - f + 1) // 3
    h = (19 * a + b - d - g + 15) % 30
    i, k = divmod(c, 4)
    l = (32 + 2 * e + 2 * i - h - k) % 7
    m = (a + 11 * h + 22 * l) // 451
    month = (h + l - 7 * m + 114) // 31
    day = (h + l - 7 * m + 114) % 31 + 1
    return dt.date(year, month, day)


def _observed_holiday(day: dt.date) -> dt.date:
    if day.weekday() == 5:
        return day - dt.timedelta(days=1)
    if day.weekday() == 6:
        return day + dt.timedelta(days=1)
    return day


def _us_market_holidays(year: int) -> set[dt.date]:
    holidays: set[dt.date] = set()
    for holiday_year in (year - 1, year, year + 1):
        observed_holidays = {
            _observed_holiday(dt.date(holiday_year, 1, 1)),
            _nth_weekday(holiday_year, 1, calendar.MONDAY, 3),
            _nth_weekday(holiday_year, 2, calendar.MONDAY, 3),
            _easter_sunday(holiday_year) - dt.timedelta(days=2),
            _last_weekday(holiday_year, 5, calendar.MONDAY),
            _observed_holiday(dt.date(holiday_year, 7, 4)),
            _nth_weekday(holiday_year, 9, calendar.MONDAY, 1),
            _nth_weekday(holiday_year, 11, calendar.THURSDAY, 4),
            _observed_holiday(dt.date(holiday_year, 12, 25)),
        }
        if holiday_year >= 2022:
            observed_holidays.add(_observed_holiday(dt.date(holiday_year, 6, 19)))
        holidays.update(
            observed_holidays
        )
    return holidays


def _is_trade_session(day: dt.date) -> bool:
    return day.weekday() < 5 and day not in _us_market_holidays(day.year)


def _next_trade_session(day: dt.date) -> dt.date:
    candidate = day + dt.timedelta(days=1)
    while not _is_trade_session(candidate):
        candidate += dt.timedelta(days=1)
    return candidate


def _is_consecutive_trade_session(previous: dt.date, current: dt.date) -> bool:
    return _next_trade_session(previous) == current


def _load_text_symbols(path: Path) -> set[str]:
    if not path.exists():
        return set()
    symbols: set[str] = set()
    for line in path.read_text(encoding="utf-8").splitlines():
        symbol = line.split("#", 1)[0].strip().split()
        if symbol:
            symbols.add(symbol[0].upper())
    return symbols


def _atomic_write(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary_name = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            handle.write(content)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary_name, path)
    finally:
        if os.path.exists(temporary_name):
            os.unlink(temporary_name)


def _provider_incident_detected(*, target_count: int, missing_count: int, failed_chunks: int) -> bool:
    # Provider errors matter to the no-price policy only when fallback also left gaps.
    return (failed_chunks > 0 and missing_count > 0) or (
        target_count > 0 and missing_count >= max(10, math.ceil(target_count * 0.05))
    )


def _select_recovery_probes(
    state_path: Path, registry_path: Path, trade_date: dt.date, *, max_probes: int = 10
) -> list[str]:
    auto_excluded = _load_text_symbols(registry_path)
    if not auto_excluded:
        return []
    try:
        state = json.loads(state_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        state = {}
    records = state.get("tickers", {}) if isinstance(state, dict) else {}
    rotation = (trade_date.toordinal() // 5 * max_probes) % len(auto_excluded)
    ordered_tickers = sorted(auto_excluded)
    ordered_tickers = ordered_tickers[rotation:] + ordered_tickers[:rotation]
    due: list[str] = []
    for ticker in ordered_tickers:
        record = records.get(ticker, {}) if isinstance(records, dict) else {}
        last_probe = record.get("last_probe_date") if isinstance(record, dict) else None
        if not last_probe:
            due.append(ticker)
        else:
            try:
                cursor = dt.date.fromisoformat(str(last_probe))
            except ValueError:
                due.append(ticker)
            else:
                sessions_elapsed = 0
                while cursor < trade_date and sessions_elapsed < 5:
                    cursor = _next_trade_session(cursor)
                    sessions_elapsed += 1
                if sessions_elapsed >= 5:
                    due.append(ticker)
        if len(due) >= max_probes:
            break
    return due


def _advance_no_price_observations(
    state_path: Path,
    registry_path: Path,
    trade_date: dt.date,
    missing_tickers: set[str],
    valid_tickers: set[str],
    *,
    probe_tickers: set[str],
    allow_observations: bool,
) -> dict[str, int]:
    if not allow_observations or not _is_trade_session(trade_date):
        return {"observations": 0, "new_exclusions": 0, "recoveries": 0}
    lock_path = state_path.with_suffix(state_path.suffix + ".lock")
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    with lock_path.open("a+") as lock_handle:
        fcntl.flock(lock_handle.fileno(), fcntl.LOCK_EX)
        try:
            try:
                state = json.loads(state_path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError):
                state = {}
            records = state.get("tickers", {}) if isinstance(state, dict) else {}
            if not isinstance(records, dict):
                records = {}
            lines = registry_path.read_text(encoding="utf-8").splitlines() if registry_path.exists() else []
            registered = _load_text_symbols(registry_path)
            new_exclusions = recoveries = observations = 0
            for ticker in sorted(valid_tickers):
                record = records.get(ticker, {})
                dates = record.get("missing_dates", []) if isinstance(record, dict) else []
                newest = max((dt.date.fromisoformat(value) for value in dates), default=None)
                if newest is None or trade_date >= newest:
                    records.pop(ticker, None)
                    if ticker in probe_tickers:
                        records[ticker] = {"missing_dates": [], "last_probe_date": trade_date.isoformat()}
                    if ticker in registered and ticker in probe_tickers:
                        lines = [line for line in lines if line.split("#", 1)[0].strip().split()[:1] != [ticker]]
                        registered.discard(ticker)
                        recoveries += 1
            for ticker in sorted(missing_tickers):
                observations += 1
                prior = records.get(ticker, {})
                dates = prior.get("missing_dates", []) if isinstance(prior, dict) else []
                parsed_dates = sorted({dt.date.fromisoformat(value) for value in dates})
                newest = parsed_dates[-1] if parsed_dates else None
                if newest is not None and trade_date <= newest:
                    continue
                parsed_dates = parsed_dates + [trade_date] if newest is not None and _is_consecutive_trade_session(newest, trade_date) else [trade_date]
                parsed_dates = parsed_dates[-3:]
                last_probe = trade_date.isoformat() if ticker in probe_tickers else prior.get("last_probe_date")
                records[ticker] = {"missing_dates": [value.isoformat() for value in parsed_dates], "last_probe_date": last_probe}
                if len(parsed_dates) >= 3 and ticker not in registered:
                    evidence = ", ".join(value.isoformat() for value in parsed_dates)
                    lines.append(f"{ticker} # reason=no valid provider daily bar for three consecutive trade sessions: {evidence}")
                    registered.add(ticker)
                    new_exclusions += 1
            for ticker in probe_tickers:
                if ticker in records:
                    records[ticker]["last_probe_date"] = trade_date.isoformat()
            _atomic_write(state_path, json.dumps({"version": 1, "tickers": records}, indent=2, sort_keys=True) + "\n")
            _atomic_write(registry_path, "\n".join(lines).rstrip() + ("\n" if lines else ""))
            return {"observations": observations, "new_exclusions": new_exclusions, "recoveries": recoveries}
        finally:
            fcntl.flock(lock_handle.fileno(), fcntl.LOCK_UN)


def _rows_for_trade_date(rows: list[tuple[object, ...]], trade_date: dt.date) -> tuple[list[tuple[object, ...]], set[str]]:
    matching_rows = [row for row in rows if row[1] == trade_date]
    stale_tickers = {str(row[0]) for row in rows if row[1] != trade_date}
    return matching_rows, stale_tickers


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
        ticker, row_date, *_ = row
        if not ticker or str(ticker) in seen or not _valid_bar(row, trade_date):
            raise RuntimeError(f"Invalid replacement row for ticker={ticker!r} date={row_date!r}.")
        seen.add(str(ticker))


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


def _replace_and_record_no_price_observations(
    connection,
    trade_date: dt.date,
    rows: list[tuple[object, ...]],
    batch_size: int,
    state_path: Path,
    registry_path: Path,
    missing_tickers: set[str],
    valid_tickers: set[str],
    *,
    probe_tickers: set[str],
    allow_observations: bool,
) -> tuple[int, int, dict[str, int]]:
    deleted, applied = _replace_trade_date_atomically(connection, trade_date, rows, batch_size)
    summary = _advance_no_price_observations(
        state_path,
        registry_path,
        trade_date,
        missing_tickers,
        valid_tickers,
        probe_tickers=probe_tickers,
        allow_observations=allow_observations,
    )
    return deleted, applied, summary


def _download_yfinance_pass(
    tickers: list[str],
    trade_date: dt.date,
    args: argparse.Namespace,
    *,
    pass_name: str,
    starting_chunk: int,
) -> tuple[dict[str, "pd.DataFrame"], int, list[str], int]:
    chunk_total = 0
    failed_chunks = 0
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
            and _valid_histories({ticker: history}, trade_date)
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
            failed_chunks += 1
            print(f"chunk_error={chunk_result.error}", flush=True)

    return histories, chunk_total, unresolved, failed_chunks


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
        registry_path = PROJECT_ROOT / "config" / "auto_exclude_tickers" / "no-price-3d.txt"
        artifacts_dir = Path(os.getenv("TICKER_SCREENER_ARTIFACTS_DIR", str(PROJECT_ROOT / "artifacts")))
        state_path = artifacts_dir / "status" / "no-price-3d-state.json"
        probe_tickers = set(_select_recovery_probes(state_path, registry_path, trade_date)) if _is_trade_session(trade_date) else set()
        excluded = load_excluded_tickers(config)
        tickers = _filter_reload_tickers(active_tickers, excluded)
        probe_tickers.intersection_update(active_tickers)
        tickers = list(dict.fromkeys([*tickers, *sorted(probe_tickers)]))
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

        tradingview_histories: dict[str, "pd.DataFrame"] = {}
        tradingview_invalid = tradingview_ambiguous = tradingview_failed_batches = 0
        if _tradingview_eligible(trade_date):
            exchanges = _load_metadata_exchanges(connection, tickers)
            tradingview_histories, tradingview_invalid, tradingview_ambiguous, tradingview_failed_batches = (
                _download_tradingview_histories(trade_date, tickers, exchanges)
            )
            print(f"tradingview=complete matched_tickers={len(tradingview_histories)}", flush=True)
        else:
            print("tradingview=skipped reason=not_latest_completed_session", flush=True)

        massive_histories: dict[str, "pd.DataFrame"] = {}
        massive_failed_batches = 0
        massive_api_key = os.getenv("MASSIVE_API_KEY", "").strip()
        if not massive_api_key:
            print("massive=skipped reason=missing_api_key", flush=True)
        else:
            try:
                massive_tickers = set(tickers) - set(tradingview_histories)
                if massive_tickers:
                    massive_histories = _valid_histories(
                        _download_massive_histories(trade_date, massive_tickers, massive_api_key), trade_date
                    )
                print(
                    f"massive=success matched_tickers={len(massive_histories)}",
                    flush=True,
                )
            except Exception as exc:
                massive_failed_batches = 1
                print(f"massive=failed reason={exc}; falling_back=yfinance", flush=True)
                massive_histories = {}

        yahoo_tickers = [ticker for ticker in tickers if ticker not in tradingview_histories and ticker not in massive_histories]
        print(f"yfinance_target_ticker_count={len(yahoo_tickers)}", flush=True)

        yfinance_histories, chunks, retry_tickers, primary_failed_chunks = _download_yfinance_pass(
            yahoo_tickers,
            trade_date,
            args,
            pass_name="primary",
            starting_chunk=total_chunks,
        )
        total_chunks += chunks

        # Retry missing bars in batches once, rather than probing each ticker's full history serially.
        retry_histories, chunks, unresolved_tickers, retry_failed_chunks = _download_yfinance_pass(
            retry_tickers,
            trade_date,
            args,
            pass_name="retry",
            starting_chunk=total_chunks,
        )
        total_chunks += chunks

        histories = {**tradingview_histories, **massive_histories, **yfinance_histories, **retry_histories}
        source_by_ticker = {ticker: "tradingview_screener" for ticker in tradingview_histories}
        source_by_ticker.update({ticker: "massive" for ticker in massive_histories})
        source_by_ticker.update({ticker: args.source_label for ticker in yfinance_histories})
        source_by_ticker.update({ticker: args.source_label for ticker in retry_histories})
        bar_rows = _build_daily_bar_rows(histories, args.source_label, updated_at, source_by_ticker)
        bar_rows, stale_tickers = _rows_for_trade_date(bar_rows, trade_date)
        minimum_row_count = _minimum_replacement_row_count(len(tickers))
        _validate_replacement_rows(bar_rows, trade_date, minimum_row_count=minimum_row_count)
        reloaded_tickers = {str(row[0]).upper() for row in bar_rows}
        missing_tickers = set(tickers) - reloaded_tickers
        provider_incident = _provider_incident_detected(
            target_count=len(tickers),
            missing_count=len(missing_tickers),
            failed_chunks=tradingview_failed_batches + massive_failed_batches + primary_failed_chunks + retry_failed_chunks,
        )
        deleted, total_rows, observation_summary = _replace_and_record_no_price_observations(
            connection,
            trade_date,
            bar_rows,
            args.batch_size,
            state_path,
            registry_path,
            missing_tickers,
            reloaded_tickers,
            probe_tickers=probe_tickers,
            allow_observations=not provider_incident,
        )

        print(f"summary_trade_date={trade_date.isoformat()}", flush=True)
        print(f"summary_target_tickers={len(tickers)}", flush=True)
        print(f"summary_reloaded_rows={total_rows}", flush=True)
        print(f"summary_deleted_rows={deleted}", flush=True)
        print(f"summary_missing_tickers={len(missing_tickers)}", flush=True)
        print(f"summary_stale_provider_tickers={len(stale_tickers)}", flush=True)
        print(f"summary_yfinance_retry_tickers={len(retry_tickers)}", flush=True)
        print(f"summary_source_tradingview_screener_rows={len(tradingview_histories)}", flush=True)
        print(f"summary_tradingview_invalid_rows={tradingview_invalid}", flush=True)
        print(f"summary_tradingview_ambiguous_tickers={tradingview_ambiguous}", flush=True)
        print(f"summary_source_massive_rows={len(massive_histories)}", flush=True)
        print(f"summary_source_{args.source_label}_rows={len(yfinance_histories) + len(retry_histories)}", flush=True)
        print(f"summary_provider_incident={str(provider_incident).lower()}", flush=True)
        print(f"summary_no_price_observations={observation_summary['observations']}", flush=True)
        print(f"summary_new_auto_exclusions={observation_summary['new_exclusions']}", flush=True)
        print(f"summary_auto_exclusion_recoveries={observation_summary['recoveries']}", flush=True)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
