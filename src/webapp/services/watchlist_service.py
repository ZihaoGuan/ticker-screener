from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd
import yfinance as yf

from ..repositories.watchlist_repository import WatchlistRepository


class WatchlistService:
    def __init__(self, artifacts_dir: Path) -> None:
        self.repository = WatchlistRepository(artifacts_dir=artifacts_dir)

    def list_recent(self) -> list[dict[str, Any]]:
        return self.repository.list_recent_watchlists(limit=50)

    def get_watchlist_detail(self, stem: str) -> dict[str, Any]:
        entries = self.repository.load_watchlist(stem)
        return {
            "stem": stem,
            "entry_count": len(entries),
            "entries": entries[:200],
        }

    def get_chart_payload(self, ticker: str, period: str = "18mo") -> dict[str, Any]:
        history = yf.download(
            tickers=ticker,
            period=period,
            interval="1d",
            auto_adjust=False,
            progress=False,
            threads=False,
        )
        if history is None or history.empty:
            return {
                "ticker": ticker,
                "candles": [],
                "volume": [],
                "ma20": [],
                "ma50": [],
                "ma200": [],
            }

        if isinstance(history.columns, pd.MultiIndex):
            history.columns = history.columns.get_level_values(0)

        frame = history.rename(columns=str).copy()
        frame = frame.dropna(subset=["Open", "High", "Low", "Close", "Volume"])
        if frame.empty:
            return {
                "ticker": ticker,
                "candles": [],
                "volume": [],
                "ma20": [],
                "ma50": [],
                "ma200": [],
            }

        frame["ma20"] = frame["Close"].rolling(20).mean()
        frame["ma50"] = frame["Close"].rolling(50).mean()
        frame["ma200"] = frame["Close"].rolling(200).mean()

        candles: list[dict[str, Any]] = []
        volume: list[dict[str, Any]] = []
        ma20: list[dict[str, Any]] = []
        ma50: list[dict[str, Any]] = []
        ma200: list[dict[str, Any]] = []

        for index, row in frame.iterrows():
            time_value = pd.Timestamp(index).date().isoformat()
            open_value = float(row["Open"])
            close_value = float(row["Close"])
            candles.append(
                {
                    "time": time_value,
                    "open": open_value,
                    "high": float(row["High"]),
                    "low": float(row["Low"]),
                    "close": close_value,
                }
            )
            volume.append(
                {
                    "time": time_value,
                    "value": int(row["Volume"]),
                    "color": "rgba(34, 197, 94, 0.72)" if close_value >= open_value else "rgba(239, 68, 68, 0.72)",
                }
            )
            if pd.notna(row["ma20"]):
                ma20.append({"time": time_value, "value": float(row["ma20"])})
            if pd.notna(row["ma50"]):
                ma50.append({"time": time_value, "value": float(row["ma50"])})
            if pd.notna(row["ma200"]):
                ma200.append({"time": time_value, "value": float(row["ma200"])})

        return {
            "ticker": ticker,
            "candles": candles,
            "volume": volume,
            "ma20": ma20,
            "ma50": ma50,
            "ma200": ma200,
        }
