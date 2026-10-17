from __future__ import annotations

import datetime as dt
import os
from typing import Any

import pandas as pd

from .market_data_access import _connect, load_active_universe_from_db, load_many_ticker_windows
from .ticker_filters import filter_universe_tickers


FEATURE_CODE_VERSION = os.environ.get("SCREENING_FEATURES_CODE_VERSION", "daily-screening-features-v1")
FEATURE_HISTORY_SESSIONS = 520


def _feature_row(frame: Any, ticker: str, trade_date: dt.date) -> tuple[object, ...] | None:
    if frame is None or frame.empty:
        return None
    bars = frame.loc[frame.index.date <= trade_date].copy().sort_index()
    if bars.empty or bars.index[-1].date() != trade_date:
        return None
    close = pd.to_numeric(bars["Close"], errors="coerce")
    volume = pd.to_numeric(bars["Volume"], errors="coerce")
    high = pd.to_numeric(bars["High"], errors="coerce")
    low = pd.to_numeric(bars["Low"], errors="coerce")
    if len(bars) < 253 or close.isna().any() or volume.isna().any():
        return None
    avg_volume = volume.tail(20).mean()
    previous_close = close.iloc[-2]
    delta = close.diff()
    gain = delta.clip(lower=0).ewm(alpha=1 / 14, min_periods=14, adjust=False).mean()
    loss = (-delta.clip(upper=0)).ewm(alpha=1 / 14, min_periods=14, adjust=False).mean()
    rs = gain.iloc[-1] / loss.iloc[-1] if loss.iloc[-1] else float("inf")
    rsi14 = 100 - 100 / (1 + rs)
    true_range = pd.concat(
        [high - low, (high - close.shift()).abs(), (low - close.shift()).abs()],
        axis=1,
    ).max(axis=1)
    weekly_close = close.resample("W-FRI").last().dropna()
    weekly_ema = weekly_close.ewm(span=30, adjust=False).mean()
    if len(weekly_ema) < 2:
        return None
    current_close = float(close.iloc[-1])
    daily_range_pct = ((high - low) / close.where(close != 0) * 100).tail(20).mean()
    return (
        trade_date, ticker, current_close, int(volume.iloc[-1]), float(avg_volume),
        float(volume.iloc[-1] / avg_volume) if avg_volume else None,
        float((current_close / float(previous_close) - 1) * 100) if previous_close else None,
        float((current_close / float(close.iloc[-253]) - 1) * 100) if close.iloc[-253] else None,
        float(close.rolling(20).mean().iloc[-1]), float(close.rolling(50).mean().iloc[-1]),
        float(close.rolling(150).mean().iloc[-1]), float(close.rolling(200).mean().iloc[-1]),
        float(close.rolling(200).mean().iloc[-2]), float(close.ewm(span=21, adjust=False).mean().iloc[-1]),
        float(close.ewm(span=100, adjust=False).mean().iloc[-1]), float(rsi14),
        float(true_range.ewm(alpha=1 / 14, min_periods=14, adjust=False).mean().iloc[-1]),
        float(daily_range_pct), float(high.tail(252).max()), float(low.tail(252).min()),
        float(weekly_ema.iloc[-1]), float(weekly_ema.iloc[-2]), "daily_bars", FEATURE_CODE_VERSION,
    )


def build_daily_screening_features(
    trade_date: dt.date,
    *,
    database_url: str | None = None,
    excluded_tickers: set[str] | None = None,
    minimum_coverage: float = 0.90,
) -> dict[str, int | str]:
    connection = _connect(database_url)
    if connection is None:
        raise RuntimeError("Postgres database is unavailable.")
    universe = load_active_universe_from_db(as_of_date=trade_date, database_url=database_url)
    if excluded_tickers is not None:
        universe = filter_universe_tickers(universe, excluded_tickers)
    tickers = [item.symbol for item in universe]
    frames = load_many_ticker_windows(tickers, trade_date, FEATURE_HISTORY_SESSIONS, database_url=database_url)
    rows = [row for ticker, frame in frames.items() if (row := _feature_row(frame, ticker, trade_date)) is not None]
    coverage = len(rows) / len(tickers) if tickers else 0.0
    if not tickers or coverage < minimum_coverage:
        connection.close()
        raise RuntimeError(f"Feature coverage {coverage:.1%} is below required {minimum_coverage:.1%}; prior snapshot retained.")
    try:
        with connection.cursor() as cursor:
            cursor.execute("DELETE FROM ticker_daily_screening_features WHERE trade_date = %s", (trade_date,))
            cursor.executemany(
                """INSERT INTO ticker_daily_screening_features (
                trade_date,ticker,close,volume,avg_volume,relative_volume,daily_change_pct,return_1y_pct,
                sma20,sma50,sma150,sma200,prior_sma200,ema21,ema100,rsi14,atr14,adr20_pct,high_52w,low_52w,
                weekly_ema30,prior_weekly_ema30,source,code_version) VALUES (
                %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                rows,
            )
            cursor.execute("SELECT COUNT(*) FROM ticker_daily_screening_features WHERE trade_date = %s", (trade_date,))
            stored = int(cursor.fetchone()[0])
            if stored != len(rows):
                raise RuntimeError(f"Stored feature count {stored} does not match built count {len(rows)}.")
        connection.commit()
    except Exception:
        connection.rollback()
        raise
    finally:
        connection.close()
    return {"trade_date": trade_date.isoformat(), "eligible_tickers": len(tickers), "feature_rows": len(rows), "coverage_pct": round(coverage * 100, 2)}
