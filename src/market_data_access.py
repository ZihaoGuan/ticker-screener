from __future__ import annotations

import datetime as dt
from typing import Iterable
from typing import Any

from .webapp.config import load_webapp_config


MarketDataSource = str


def resolve_market_data_source(value: str | None = None) -> MarketDataSource:
    normalized = (value or "").strip().lower()
    if not normalized:
        normalized = load_webapp_config().market_data_source.strip().lower()
    if normalized == "database-first":
        return "database-first"
    return "internet"


def resolve_database_url(database_url: str | None = None) -> str:
    return (database_url or load_webapp_config().database_url).strip()


def _connect(database_url: str | None = None):
    resolved_url = resolve_database_url(database_url)
    if not resolved_url:
        return None

    try:
        import psycopg
    except ImportError:
        return None
    return psycopg.connect(resolved_url)


def load_daily_bars_frame_from_db(
    ticker: str,
    start_date: dt.date,
    end_date: dt.date,
    *,
    database_url: str | None = None,
):
    import pandas as pd

    connection = _connect(database_url)
    if connection is None:
        return None

    sql = """
        SELECT trade_date, open, high, low, close, adj_close, volume
        FROM daily_bars
        WHERE ticker = %s
          AND trade_date >= %s
          AND trade_date <= %s
        ORDER BY trade_date ASC
    """
    with connection:
        with connection.cursor() as cursor:
            cursor.execute(sql, (ticker.upper(), start_date, end_date))
            rows = cursor.fetchall()

    if not rows:
        return None

    frame = pd.DataFrame(
        rows,
        columns=["trade_date", "Open", "High", "Low", "Close", "Adj Close", "Volume"],
    )
    frame["trade_date"] = pd.to_datetime(frame["trade_date"])
    frame = _normalize_ohlcv_frame(frame.set_index("trade_date"))
    return frame if not frame.empty else None


def db_frame_has_recent_coverage(frame: Any, end_date: dt.date, *, tolerance_days: int = 7) -> bool:
    if frame is None or frame.empty:
        return False
    last_index = frame.index.max()
    last_date = last_index.date() if hasattr(last_index, "date") else last_index
    return bool(last_date >= (end_date - dt.timedelta(days=max(0, int(tolerance_days)))))


def build_cookstock_payload_from_frame(ticker: str, frame: Any) -> dict[str, dict[str, object]] | None:
    import pandas as pd

    if frame is None or frame.empty:
        return None

    prices: list[dict[str, object]] = []
    for index, row in frame.iterrows():
        timestamp = int(pd.Timestamp(index).to_pydatetime().replace(tzinfo=dt.timezone.utc).timestamp())
        prices.append(
            {
                "date": timestamp,
                "formatted_date": pd.Timestamp(index).date().isoformat(),
                "open": float(row["Open"]) if row["Open"] is not None else None,
                "high": float(row["High"]) if row["High"] is not None else None,
                "low": float(row["Low"]) if row["Low"] is not None else None,
                "close": float(row["Close"]) if row["Close"] is not None else None,
                "adjclose": float(row["Adj Close"]) if row.get("Adj Close") is not None else float(row["Close"]),
                "volume": int(row["Volume"]) if row["Volume"] is not None else 0,
            }
        )

    if not prices:
        return None

    first_trade_date = prices[0]["date"]
    return {
        ticker.upper(): {
            "eventsData": [],
            "firstTradeDate": first_trade_date,
            "currency": "USD",
            "instrumentType": "EQUITY",
            "timeZone": "UTC",
            "prices": prices,
        }
    }


def build_cookstock_price_list_from_frame(frame: Any) -> list[dict[str, object]]:
    payload = build_cookstock_payload_from_frame("TEMP", frame)
    if not payload:
        return []
    return list(payload.values())[0]["prices"]  # type: ignore[index]


def _coerce_frame(rows: list[tuple[object, ...]]):
    import pandas as pd

    if not rows:
        return None

    frame = pd.DataFrame(
        rows,
        columns=["trade_date", "Open", "High", "Low", "Close", "Adj Close", "Volume"],
    )
    frame["trade_date"] = pd.to_datetime(frame["trade_date"])
    frame = _normalize_ohlcv_frame(frame.set_index("trade_date").sort_index())
    return frame if not frame.empty else None


def _normalize_ohlcv_frame(frame: Any):
    import pandas as pd

    normalized = frame.copy()
    numeric_columns = ["Open", "High", "Low", "Close", "Adj Close", "Volume"]
    for column in numeric_columns:
        if column in normalized.columns:
            normalized[column] = pd.to_numeric(normalized[column], errors="coerce")
    normalized = normalized.dropna(subset=["Open", "High", "Low", "Close", "Volume"])
    return normalized


def resolve_trading_window_start(
    as_of_date: dt.date,
    trading_days_needed: int,
    *,
    database_url: str | None = None,
) -> dt.date:
    connection = _connect(database_url)
    if connection is None:
        return as_of_date - dt.timedelta(days=max(3, int(trading_days_needed) * 2))

    sql = """
        SELECT trade_date
        FROM (
            SELECT DISTINCT trade_date
            FROM daily_bars
            WHERE trade_date <= %s
            ORDER BY trade_date DESC
            LIMIT %s
        ) window_dates
        ORDER BY trade_date ASC
    """
    with connection:
        with connection.cursor() as cursor:
            cursor.execute(sql, (as_of_date, max(1, int(trading_days_needed))))
            rows = cursor.fetchall()
    if not rows:
        return as_of_date - dt.timedelta(days=max(3, int(trading_days_needed) * 2))
    first_value = rows[0][0]
    return first_value if isinstance(first_value, dt.date) else dt.date.fromisoformat(str(first_value))


def load_ticker_window(
    ticker: str,
    as_of_date: dt.date,
    trading_days_needed: int,
    *,
    database_url: str | None = None,
):
    connection = _connect(database_url)
    if connection is None:
        return None

    sql = """
        SELECT trade_date, open, high, low, close, adj_close, volume
        FROM (
            SELECT trade_date, open, high, low, close, adj_close, volume
            FROM daily_bars
            WHERE ticker = %s
              AND trade_date <= %s
            ORDER BY trade_date DESC
            LIMIT %s
        ) ticker_window
        ORDER BY trade_date ASC
    """
    with connection:
        with connection.cursor() as cursor:
            cursor.execute(sql, (ticker.upper(), as_of_date, max(1, int(trading_days_needed))))
            rows = cursor.fetchall()
    return _coerce_frame(rows)


def load_many_ticker_windows(
    tickers: Iterable[str],
    as_of_date: dt.date,
    trading_days_needed: int,
    *,
    database_url: str | None = None,
) -> dict[str, Any]:
    normalized = sorted({str(item).strip().upper() for item in tickers if str(item).strip()})
    if not normalized:
        return {}

    connection = _connect(database_url)
    if connection is None:
        return {}

    start_date = resolve_trading_window_start(as_of_date, trading_days_needed, database_url=database_url)
    sql = """
        SELECT ticker, trade_date, open, high, low, close, adj_close, volume
        FROM daily_bars
        WHERE ticker = ANY(%s)
          AND trade_date >= %s
          AND trade_date <= %s
        ORDER BY ticker ASC, trade_date ASC
    """
    with connection:
        with connection.cursor() as cursor:
            cursor.execute(sql, (normalized, start_date, as_of_date))
            rows = cursor.fetchall()

    grouped: dict[str, list[tuple[object, ...]]] = {}
    for ticker, trade_date, open_price, high, low, close, adj_close, volume in rows:
        grouped.setdefault(str(ticker).upper(), []).append((trade_date, open_price, high, low, close, adj_close, volume))

    frames: dict[str, Any] = {}
    for ticker in normalized:
        frame = _coerce_frame(grouped.get(ticker, []))
        if frame is not None:
            frames[ticker] = frame
    return frames


def load_many_ticker_windows_for_range(
    tickers: Iterable[str],
    start_date: dt.date,
    end_date: dt.date,
    trading_days_needed: int,
    *,
    database_url: str | None = None,
) -> dict[str, Any]:
    normalized = sorted({str(item).strip().upper() for item in tickers if str(item).strip()})
    if not normalized:
        return {}

    connection = _connect(database_url)
    if connection is None:
        return {}

    warmup_start = resolve_trading_window_start(start_date, trading_days_needed, database_url=database_url)
    sql = """
        SELECT ticker, trade_date, open, high, low, close, adj_close, volume
        FROM daily_bars
        WHERE ticker = ANY(%s)
          AND trade_date >= %s
          AND trade_date <= %s
        ORDER BY ticker ASC, trade_date ASC
    """
    with connection:
        with connection.cursor() as cursor:
            cursor.execute(sql, (normalized, warmup_start, end_date))
            rows = cursor.fetchall()

    grouped: dict[str, list[tuple[object, ...]]] = {}
    for ticker, trade_date, open_price, high, low, close, adj_close, volume in rows:
        grouped.setdefault(str(ticker).upper(), []).append((trade_date, open_price, high, low, close, adj_close, volume))

    frames: dict[str, Any] = {}
    for ticker in normalized:
        frame = _coerce_frame(grouped.get(ticker, []))
        if frame is not None:
            frames[ticker] = frame
    return frames


def load_ticker_metadata_map(
    tickers: Iterable[str],
    *,
    database_url: str | None = None,
) -> dict[str, dict[str, object]]:
    normalized = sorted({str(item).strip().upper() for item in tickers if str(item).strip()})
    if not normalized:
        return {}

    connection = _connect(database_url)
    if connection is None:
        return {}

    sql = """
        SELECT ticker, exchange, sector, industry, is_active, currency, source
        FROM ticker_metadata
        WHERE ticker = ANY(%s)
    """
    with connection:
        with connection.cursor() as cursor:
            cursor.execute(sql, (normalized,))
            rows = cursor.fetchall()

    payload: dict[str, dict[str, object]] = {}
    for ticker, exchange, sector, industry, is_active, currency, source in rows:
        payload[str(ticker).upper()] = {
            "ticker": str(ticker).upper(),
            "exchange": exchange,
            "sector": sector,
            "industry": industry,
            "is_active": is_active,
            "currency": currency,
            "source": source,
        }
    return payload
