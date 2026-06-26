"""FMP wrapper + OHLCV quality validation.

Returns most-recent-first list[dict] (no pandas). Records data quality
issues as audit_flags / skipped_sessions for the report layer.
"""

from __future__ import annotations

import datetime as dt
from typing import Any


def normalize_history(payload: Any) -> list[dict]:
    """Coerce FMP payload variants into a flat most-recent-first list[dict].

    - dict with "historical" key (v3 shape): return payload["historical"]
    - list (stable EOD flat list, after normalizer): return as-is
    - None or unrecognized shape: return []
    """
    if payload is None:
        return []
    if isinstance(payload, dict):
        return list(payload.get("historical") or [])
    if isinstance(payload, list):
        return list(payload)
    return []


def validate_history_quality(history: list[dict]) -> tuple[list[str], list[dict]]:
    """Identify data quality problems.

    Returns (audit_flags, skipped_sessions). Each skipped session has
    {date, reason}. The caller's downstream code is expected to treat
    skipped sessions as no-op (no DD detection / enrichment for them).
    """
    flags: list[str] = []
    skipped: list[dict] = []
    for row in history:
        date = row.get("date")
        close = row.get("close")
        volume = row.get("volume")

        if close is None:
            skipped.append({"date": date, "reason": "missing_close"})
        elif close <= 0:
            skipped.append({"date": date, "reason": "invalid_close"})

        if volume is None:
            skipped.append({"date": date, "reason": "missing_volume"})
        elif volume <= 0:
            skipped.append({"date": date, "reason": "invalid_volume"})

    if skipped:
        flags.append("data_quality_warnings")
    return flags, skipped


def fetch_ohlcv(
    client: Any,
    symbol: str,
    days: int,
) -> tuple[list[dict], dict]:
    """Fetch most-recent-first OHLCV for `symbol` via the provided FMP client.

    Returns (history, audit) where audit has:
        - data_source: "fmp"
        - symbol: str
        - days_requested: int
        - sessions_loaded: int
        - audit_flags: list[str]
        - skipped_sessions: list[dict]
    """
    audit: dict = {
        "data_source": "fmp",
        "symbol": symbol,
        "days_requested": days,
        "sessions_loaded": 0,
        "audit_flags": [],
        "skipped_sessions": [],
    }

    payload = client.get_historical_prices(symbol, days=days)
    history = normalize_history(payload)

    if not history:
        audit["audit_flags"].append("no_data_returned")
        return [], audit

    flags, skipped = validate_history_quality(history)
    audit["sessions_loaded"] = len(history)
    audit["audit_flags"].extend(flags)
    audit["skipped_sessions"] = skipped
    return history, audit


def fetch_ohlcv_from_db(
    symbol: str,
    days: int,
    *,
    as_of: str | None = None,
    database_url: str | None = None,
) -> tuple[list[dict], dict]:
    """Fetch most-recent-first OHLCV for `symbol` from Postgres daily_bars."""
    from src.market_data_access import load_daily_bars_frame_from_db

    resolved_end_date = dt.date.fromisoformat(as_of) if as_of else dt.date.today()
    start_date = resolved_end_date - dt.timedelta(days=max(10, int(days) * 3))
    audit: dict = {
        "data_source": "database",
        "symbol": symbol,
        "days_requested": days,
        "sessions_loaded": 0,
        "audit_flags": [],
        "skipped_sessions": [],
    }

    frame = load_daily_bars_frame_from_db(
        symbol,
        start_date,
        resolved_end_date,
        database_url=database_url,
    )
    if frame is None or frame.empty:
        audit["audit_flags"].append("no_data_returned")
        return [], audit

    history: list[dict] = []
    for index, row in frame.sort_index(ascending=False).iterrows():
        history.append(
            {
                "date": index.date().isoformat(),
                "open": float(row["Open"]),
                "high": float(row["High"]),
                "low": float(row["Low"]),
                "close": float(row["Close"]),
                "volume": float(row["Volume"]),
            }
        )

    flags, skipped = validate_history_quality(history)
    audit["sessions_loaded"] = len(history)
    audit["audit_flags"].extend(flags)
    audit["skipped_sessions"] = skipped
    return history, audit


def build_fmp_client(api_key: str | None = None, max_api_calls: int = 200):
    """Lazy import to avoid pulling requests in unit tests that mock the client."""
    from fmp_client import FMPClient

    return FMPClient(api_key=api_key, max_api_calls=max_api_calls)
