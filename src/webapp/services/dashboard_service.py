from __future__ import annotations

import datetime as dt
from pathlib import Path
from typing import Any

import pandas as pd
import yfinance as yf

from ...config import load_app_config
from ...market_data_access import db_frame_has_recent_coverage, load_daily_bars_frame_from_db
from ...market_extension import build_moving_average, compute_extension_frame, resample_to_weekly
from ...rsi_divergence import find_latest_bearish_rsi_divergence_top
from ...td_sequential_screen import find_recent_td_sequential_hit
from ...universe import UniverseTicker
from ..repositories.dashboard_repository import DashboardRepository
from ..repositories.watchlist_repository import WatchlistRepository


class DashboardService:
    def __init__(self, database_url: str, artifacts_dir: Path) -> None:
        self.dashboard_repository = DashboardRepository(database_url=database_url, artifacts_dir=artifacts_dir)
        self.watchlist_repository = WatchlistRepository(artifacts_dir=artifacts_dir)

    def get_dashboard_context(self) -> dict[str, Any]:
        overview = self.dashboard_repository.get_overview()
        recent_watchlists = self.watchlist_repository.list_recent_watchlists(limit=8)
        try:
            market_health = self._build_market_health()
        except Exception:
            benchmark = load_app_config().benchmark_ticker.upper()
            market_health = _build_unavailable_market_health(benchmark=benchmark, data_source="unavailable")
        return {
            "overview": overview,
            "market_health": market_health,
            "recent_watchlists": recent_watchlists,
            "strategy_cards": [
                {"id": "rs", "label": "RS", "description": "Daily RS new high before price."},
                {"id": "vcp", "label": "VCP", "description": "Volatility contraction pattern scan."},
                {"id": "cup_handle", "label": "Cup and Handle", "description": "Breakout candidate scan."},
                {"id": "ftd_sweep", "label": "FTD Sweep", "description": "Recent FTD sweep breakout within the lookback window."},
                {"id": "overlap", "label": "Report", "description": "Daily cross-strategy overlap report."},
            ],
        }

    def _build_market_health(self) -> dict[str, Any]:
        benchmark = load_app_config().benchmark_ticker.upper()
        end_date = dt.date.today()
        start_date = end_date - dt.timedelta(days=900)
        try:
            db_frame = load_daily_bars_frame_from_db(benchmark, start_date, end_date, database_url=self.dashboard_repository.database_url)
        except Exception:
            db_frame = None

        db_payload = _build_payload_if_possible(frame=db_frame, ticker=benchmark, data_source="database")
        if (
            db_payload is not None
            and db_frame is not None
            and not db_frame.empty
            and db_frame_has_recent_coverage(db_frame, end_date, tolerance_days=7)
            and not _market_health_payload_has_no_latest(db_payload)
        ):
            return db_payload

        internet_frame = _download_history_frame(benchmark, start_date, end_date)
        internet_payload = _build_payload_if_possible(frame=internet_frame, ticker=benchmark, data_source="internet")
        if internet_payload is not None and not _market_health_payload_has_no_latest(internet_payload):
            return internet_payload

        if db_payload is not None and not _market_health_payload_has_no_latest(db_payload):
            return db_payload

        return _build_unavailable_market_health(benchmark=benchmark, data_source="unavailable")


def _build_market_health_payload(*, frame: pd.DataFrame, ticker: str, data_source: str) -> dict[str, Any]:
    weekly = resample_to_weekly(frame[["Open", "High", "Low", "Close", "Volume"]])
    regime = _build_regime_payload(frame=frame, weekly=weekly, ticker=ticker, data_source=data_source)
    rsi_divergence = _build_rsi_divergence_payload(frame=frame, ticker=ticker, data_source=data_source)
    bearish_td9 = _build_bearish_td9_payload(frame=frame, ticker=ticker, data_source=data_source)
    enriched = compute_extension_frame(weekly, length=10, ma_type="sma", warning_pct=11.0, extreme_pct=15.0)
    latest_valid = enriched.dropna(subset=["moving_average", "extension_pct"]).tail(1)
    latest = None
    if not latest_valid.empty:
        row = latest_valid.iloc[0]
        moving_average = float(row["moving_average"])
        latest = {
            "time": latest_valid.index[-1].date().isoformat(),
            "state": str(row["threshold_state"]),
            "close": round(float(row["Close"]), 2),
            "moving_average": round(moving_average, 2),
            "distance": round(float(row["Close"]) - moving_average, 2),
            "extension_pct": round(float(row["extension_pct"]), 2),
        }
    return {
        "regime": regime,
        "rsi_divergence": rsi_divergence,
        "bearish_td9": bearish_td9,
        "spy_extension": {
            "ticker": ticker,
            "label": "10W SMA",
            "timeframe": "weekly",
            "ma_type": "sma",
            "length": 10,
            "warning_pct": 11.0,
            "extreme_pct": 15.0,
            "data_source": data_source,
            "latest": latest,
        }
    }


def _build_payload_if_possible(*, frame: pd.DataFrame | None, ticker: str, data_source: str) -> dict[str, Any] | None:
    if frame is None or frame.empty:
        return None
    try:
        return _build_market_health_payload(frame=frame, ticker=ticker, data_source=data_source)
    except Exception:
        return None


def _market_health_payload_has_no_latest(payload: dict[str, Any]) -> bool:
    market_health = payload or {}
    regime_latest = ((market_health.get("regime") or {}).get("latest")) if isinstance(market_health.get("regime"), dict) else None
    rsi_latest = ((market_health.get("rsi_divergence") or {}).get("latest")) if isinstance(market_health.get("rsi_divergence"), dict) else None
    td9_latest = ((market_health.get("bearish_td9") or {}).get("latest")) if isinstance(market_health.get("bearish_td9"), dict) else None
    extension_latest = ((market_health.get("spy_extension") or {}).get("latest")) if isinstance(market_health.get("spy_extension"), dict) else None
    return regime_latest is None and rsi_latest is None and td9_latest is None and extension_latest is None


def _build_unavailable_market_health(*, benchmark: str, data_source: str) -> dict[str, Any]:
    return {
        "regime": {
            "ticker": benchmark,
            "data_source": data_source,
            "latest": None,
        },
        "rsi_divergence": {
            "ticker": benchmark,
            "data_source": data_source,
            "latest": None,
        },
        "bearish_td9": {
            "ticker": benchmark,
            "data_source": data_source,
            "latest": None,
        },
        "spy_extension": {
            "ticker": benchmark,
            "label": "10W SMA",
            "timeframe": "weekly",
            "ma_type": "sma",
            "length": 10,
            "warning_pct": 11.0,
            "extreme_pct": 15.0,
            "data_source": data_source,
            "latest": None,
        },
    }


def _download_history_frame(ticker: str, start_date: dt.date, end_date: dt.date) -> pd.DataFrame | None:
    try:
        history = yf.download(
            tickers=ticker,
            start=start_date.isoformat(),
            end=(end_date + dt.timedelta(days=1)).isoformat(),
            interval="1d",
            auto_adjust=False,
            progress=False,
            threads=False,
        )
    except Exception:
        return None
    if history is None or history.empty:
        return None
    frame = history.copy()
    if isinstance(frame.columns, pd.MultiIndex):
        frame.columns = frame.columns.get_level_values(0)
    frame = frame.rename(columns=str)
    for column in ("Open", "High", "Low", "Close", "Volume"):
        if column in frame.columns:
            frame[column] = pd.to_numeric(frame[column], errors="coerce")
    frame = frame.dropna(subset=["Open", "High", "Low", "Close", "Volume"]).sort_index()
    return frame if not frame.empty else None


def _build_regime_payload(*, frame: pd.DataFrame, weekly: pd.DataFrame, ticker: str, data_source: str) -> dict[str, Any]:
    daily_ema21 = build_moving_average(frame["Close"], length=21, ma_type="ema")
    weekly_ema21 = build_moving_average(weekly["Close"], length=21, ma_type="ema")

    latest_daily = frame.dropna(subset=["Close"]).tail(1)
    latest_weekly = weekly.dropna(subset=["Close"]).tail(1)
    latest_daily_ema = daily_ema21.dropna().tail(1)
    latest_weekly_ema = weekly_ema21.dropna().tail(1)

    if latest_daily.empty or latest_weekly.empty or latest_daily_ema.empty or latest_weekly_ema.empty:
        return {"ticker": ticker, "data_source": data_source, "latest": None}

    daily_close = float(latest_daily["Close"].iloc[0])
    weekly_close = float(latest_weekly["Close"].iloc[0])
    daily_ema = float(latest_daily_ema.iloc[0])
    weekly_ema = float(latest_weekly_ema.iloc[0])

    weekly_uptrend = weekly_close > weekly_ema
    daily_downtrend = daily_close < daily_ema

    if weekly_uptrend and daily_downtrend:
        regime = "healthy_chaos"
        regime_label = "Healthy Chaos"
        summary = "Weekly uptrend, daily reset"
        explanation = (
            "Macro trend still up. Daily weakness is a normal exhale inside the weekly 21EMA uptrend and often "
            "becomes the buy-the-dip zone."
        )
    elif weekly_uptrend and not daily_downtrend:
        regime = "perfect_convergence_bull"
        regime_label = "Perfect Convergence (Bull Market)"
        summary = "Trend and short-term action aligned"
        explanation = "Price is above both weekly and daily 21EMA. Trend is your friend here: ride, hold, or add carefully."
    elif (not weekly_uptrend) and daily_downtrend:
        regime = "perfect_convergence_bear"
        regime_label = "Perfect Convergence (Bear Market)"
        summary = "Maximum chaos"
        explanation = "Price is below both weekly and daily 21EMA. Structural trend weak, short-term action weak, capital preservation first."
    else:
        regime = "bear_market_rally"
        regime_label = "Bear Market Rally"
        summary = "Short-term euphoria in structural downtrend"
        explanation = (
            "Daily reclaimed 21EMA, but weekly trend is still below its 21EMA anchor. Treat strength as a bounce attempt "
            "until macro trend improves."
        )

    return {
        "ticker": ticker,
        "data_source": data_source,
        "latest": {
            "date": latest_daily.index[-1].date().isoformat(),
            "weekly_bar_date": latest_weekly.index[-1].date().isoformat(),
            "daily_close": round(daily_close, 2),
            "daily_ema21": round(daily_ema, 2),
            "weekly_close": round(weekly_close, 2),
            "weekly_ema21": round(weekly_ema, 2),
            "weekly_uptrend": weekly_uptrend,
            "daily_downtrend": daily_downtrend,
            "regime": regime,
            "regime_label": regime_label,
            "summary": summary,
            "explanation": explanation,
            "daily_distance_pct": round(((daily_close / daily_ema) - 1.0) * 100.0, 2) if daily_ema else None,
            "weekly_distance_pct": round(((weekly_close / weekly_ema) - 1.0) * 100.0, 2) if weekly_ema else None,
        },
    }


def _build_rsi_divergence_payload(*, frame: pd.DataFrame, ticker: str, data_source: str) -> dict[str, Any]:
    signal = find_latest_bearish_rsi_divergence_top(frame)
    if signal is None:
        return {"ticker": ticker, "data_source": data_source, "latest": None}

    explanation = "Daily RSI divergence top from Charles Edwards style pivot logic, with fresh, active, lifted, and invalidated states."
    return {
        "ticker": ticker,
        "data_source": data_source,
        "latest": {
            **signal.to_dict(),
            "explanation": explanation,
        },
    }


def _build_bearish_td9_payload(*, frame: pd.DataFrame, ticker: str, data_source: str) -> dict[str, Any]:
    signal = find_recent_td_sequential_hit(frame, ticker=UniverseTicker(symbol=ticker), direction="bearish")
    if signal is None:
        return {"ticker": ticker, "data_source": data_source, "latest": None}

    latest_close = float(frame["Close"].dropna().iloc[-1]) if "Close" in frame.columns and not frame["Close"].dropna().empty else None
    distance_pct = None
    if latest_close and signal.comparison_close:
        distance_pct = round(((latest_close / signal.comparison_close) - 1.0) * 100.0, 2)

    return {
        "ticker": ticker,
        "data_source": data_source,
        "latest": {
            **signal.to_dict(),
            "label": "Bearish TD9",
            "explanation": "Nine straight closes above the close four bars earlier. Often marks short-term upside exhaustion.",
            "distance_from_compare_pct": distance_pct,
        },
    }
