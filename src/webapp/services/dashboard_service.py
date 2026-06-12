from __future__ import annotations

import datetime as dt
from pathlib import Path
from typing import Any

import pandas as pd
import yfinance as yf

from ...config import load_app_config
from ...market_data_access import load_daily_bars_frame_from_db
from ...market_extension import compute_extension_frame, resample_to_weekly
from ..repositories.dashboard_repository import DashboardRepository
from ..repositories.watchlist_repository import WatchlistRepository


class DashboardService:
    def __init__(self, database_url: str, artifacts_dir: Path) -> None:
        self.dashboard_repository = DashboardRepository(database_url=database_url, artifacts_dir=artifacts_dir)
        self.watchlist_repository = WatchlistRepository(artifacts_dir=artifacts_dir)

    def get_dashboard_context(self) -> dict[str, Any]:
        overview = self.dashboard_repository.get_overview()
        recent_watchlists = self.watchlist_repository.list_recent_watchlists(limit=8)
        return {
            "overview": overview,
            "market_health": self._build_market_health(),
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
        frame = load_daily_bars_frame_from_db(benchmark, start_date, end_date, database_url=self.dashboard_repository.database_url)
        data_source = "database"
        if frame is None or frame.empty:
            frame = _download_history_frame(benchmark, start_date, end_date)
            data_source = "internet" if frame is not None and not frame.empty else "unavailable"
        if frame is None or frame.empty:
            return {
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
                }
            }

        weekly = resample_to_weekly(frame[["Open", "High", "Low", "Close", "Volume"]])
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
            "spy_extension": {
                "ticker": benchmark,
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


def _download_history_frame(ticker: str, start_date: dt.date, end_date: dt.date) -> pd.DataFrame | None:
    history = yf.download(
        tickers=ticker,
        start=start_date.isoformat(),
        end=(end_date + dt.timedelta(days=1)).isoformat(),
        interval="1d",
        auto_adjust=False,
        progress=False,
        threads=False,
    )
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
