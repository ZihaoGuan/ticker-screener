from __future__ import annotations

import datetime as dt
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


def load_daily_bars_frame_from_db(
    ticker: str,
    start_date: dt.date,
    end_date: dt.date,
    *,
    database_url: str | None = None,
):
    import pandas as pd

    resolved_url = (database_url or load_webapp_config().database_url).strip()
    if not resolved_url:
        return None

    try:
        import psycopg
    except ImportError:
        return None

    sql = """
        SELECT trade_date, open, high, low, close, adj_close, volume
        FROM daily_bars
        WHERE ticker = %s
          AND trade_date >= %s
          AND trade_date <= %s
        ORDER BY trade_date ASC
    """
    with psycopg.connect(resolved_url) as connection:
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
    frame = frame.set_index("trade_date")
    frame = frame.dropna(subset=["Open", "High", "Low", "Close", "Volume"])
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
