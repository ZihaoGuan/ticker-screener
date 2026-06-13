from __future__ import annotations

import datetime as dt
from typing import Any
from typing import Iterable

from .market_data_access import resolve_database_url


TRENDLINE_SNAPSHOT_COLUMNS = (
    "close",
    "daily_ema9",
    "daily_ema21",
    "daily_sma50",
    "daily_sma200",
    "weekly_ema8",
    "weekly_sma200",
)


def build_trendline_snapshot_frame(frame: Any):
    import pandas as pd

    if frame is None or getattr(frame, "empty", False):
        return pd.DataFrame(columns=TRENDLINE_SNAPSHOT_COLUMNS)

    normalized = frame.copy().sort_index()
    close = pd.to_numeric(normalized.get("Close"), errors="coerce")
    snapshot = pd.DataFrame(index=pd.DatetimeIndex(normalized.index))
    snapshot["close"] = close
    snapshot["daily_ema9"] = close.ewm(span=9, adjust=False).mean()
    snapshot["daily_ema21"] = close.ewm(span=21, adjust=False).mean()
    snapshot["daily_sma50"] = close.rolling(50).mean()
    snapshot["daily_sma200"] = close.rolling(200).mean()

    weekly_close = close.resample("W-FRI").last().dropna()
    weekly_ema8 = weekly_close.ewm(span=8, adjust=False).mean()
    weekly_sma200 = weekly_close.rolling(200).mean()
    snapshot["weekly_ema8"] = weekly_ema8.reindex(snapshot.index, method="ffill")
    snapshot["weekly_sma200"] = weekly_sma200.reindex(snapshot.index, method="ffill")
    return snapshot


def build_snapshot_rows_for_range(
    ticker: str,
    frame: Any,
    *,
    start_date: dt.date,
    end_date: dt.date,
) -> list[tuple[object, ...]]:
    import pandas as pd

    snapshots = build_trendline_snapshot_frame(frame)
    if snapshots.empty:
        return []

    ticker_symbol = str(ticker or "").strip().upper()
    if not ticker_symbol:
        return []

    rows: list[tuple[object, ...]] = []
    for index, row in snapshots.iterrows():
        trade_date = pd.Timestamp(index).date()
        if trade_date < start_date or trade_date > end_date:
            continue
        rows.append(
            (
                ticker_symbol,
                trade_date,
                _safe_float(row.get("close")),
                _safe_float(row.get("daily_ema9")),
                _safe_float(row.get("daily_ema21")),
                _safe_float(row.get("daily_sma50")),
                _safe_float(row.get("daily_sma200")),
                _safe_float(row.get("weekly_ema8")),
                _safe_float(row.get("weekly_sma200")),
            )
        )
    return rows


def upsert_trendline_snapshot_rows(
    rows: list[tuple[object, ...]],
    *,
    database_url: str | None = None,
    batch_size: int = 5000,
) -> int:
    resolved_url = resolve_database_url(database_url)
    if not resolved_url or not rows:
        return 0

    try:
        import psycopg
    except ImportError:
        return 0

    sql = """
        INSERT INTO ticker_trendline_snapshots (
            ticker,
            trade_date,
            close,
            daily_ema9,
            daily_ema21,
            daily_sma50,
            daily_sma200,
            weekly_ema8,
            weekly_sma200
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (ticker, trade_date) DO UPDATE SET
            close = EXCLUDED.close,
            daily_ema9 = EXCLUDED.daily_ema9,
            daily_ema21 = EXCLUDED.daily_ema21,
            daily_sma50 = EXCLUDED.daily_sma50,
            daily_sma200 = EXCLUDED.daily_sma200,
            weekly_ema8 = EXCLUDED.weekly_ema8,
            weekly_sma200 = EXCLUDED.weekly_sma200,
            updated_at = NOW()
    """
    with psycopg.connect(resolved_url) as connection:
        with connection.cursor() as cursor:
            for start in range(0, len(rows), max(1, int(batch_size))):
                cursor.executemany(sql, rows[start : start + max(1, int(batch_size))])
        connection.commit()
    return len(rows)


def load_latest_trendline_snapshot_map(
    tickers: Iterable[str],
    *,
    as_of_date: dt.date,
    database_url: str | None = None,
) -> dict[str, dict[str, object]]:
    normalized = sorted({str(item).strip().upper() for item in tickers if str(item).strip()})
    if not normalized:
        return {}

    resolved_url = resolve_database_url(database_url)
    if not resolved_url:
        return {}

    try:
        import psycopg
    except ImportError:
        return {}

    sql = """
        SELECT DISTINCT ON (ticker)
            ticker,
            trade_date,
            close,
            daily_ema9,
            daily_ema21,
            daily_sma50,
            daily_sma200,
            weekly_ema8,
            weekly_sma200
        FROM ticker_trendline_snapshots
        WHERE ticker = ANY(%s)
          AND trade_date <= %s
        ORDER BY ticker ASC, trade_date DESC
    """
    with psycopg.connect(resolved_url) as connection:
        with connection.cursor() as cursor:
            try:
                cursor.execute(sql, (normalized, as_of_date))
            except Exception:
                return {}
            rows = cursor.fetchall()

    payload: dict[str, dict[str, object]] = {}
    for row in rows:
        ticker, trade_date, close, daily_ema9, daily_ema21, daily_sma50, daily_sma200, weekly_ema8, weekly_sma200 = row
        payload[str(ticker).upper()] = {
            "ticker": str(ticker).upper(),
            "trade_date": trade_date,
            "close": close,
            "daily_ema9": daily_ema9,
            "daily_ema21": daily_ema21,
            "daily_sma50": daily_sma50,
            "daily_sma200": daily_sma200,
            "weekly_ema8": weekly_ema8,
            "weekly_sma200": weekly_sma200,
        }
    return payload


def load_trendline_snapshot_history_map(
    tickers: Iterable[str],
    *,
    start_date: dt.date,
    end_date: dt.date,
    database_url: str | None = None,
) -> dict[str, list[dict[str, object]]]:
    normalized = sorted({str(item).strip().upper() for item in tickers if str(item).strip()})
    if not normalized:
        return {}

    resolved_url = resolve_database_url(database_url)
    if not resolved_url:
        return {}

    try:
        import psycopg
    except ImportError:
        return {}

    sql = """
        SELECT
            ticker,
            trade_date,
            close,
            daily_ema9,
            daily_ema21,
            daily_sma50,
            daily_sma200,
            weekly_ema8,
            weekly_sma200
        FROM ticker_trendline_snapshots
        WHERE ticker = ANY(%s)
          AND trade_date >= %s
          AND trade_date <= %s
        ORDER BY ticker ASC, trade_date ASC
    """
    with psycopg.connect(resolved_url) as connection:
        with connection.cursor() as cursor:
            try:
                cursor.execute(sql, (normalized, start_date, end_date))
            except Exception:
                return {}
            rows = cursor.fetchall()

    payload: dict[str, list[dict[str, object]]] = {ticker: [] for ticker in normalized}
    for row in rows:
        ticker, trade_date, close, daily_ema9, daily_ema21, daily_sma50, daily_sma200, weekly_ema8, weekly_sma200 = row
        payload[str(ticker).upper()].append(
            {
                "ticker": str(ticker).upper(),
                "trade_date": trade_date,
                "close": close,
                "daily_ema9": daily_ema9,
                "daily_ema21": daily_ema21,
                "daily_sma50": daily_sma50,
                "daily_sma200": daily_sma200,
                "weekly_ema8": weekly_ema8,
                "weekly_sma200": weekly_sma200,
            }
        )
    return payload


def _safe_float(value: object) -> float | None:
    try:
        import math

        if value is None:
            return None
        parsed = float(value)
        return None if math.isnan(parsed) else parsed
    except (TypeError, ValueError):
        return None
