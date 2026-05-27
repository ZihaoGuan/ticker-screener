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
            return _empty_chart_payload(ticker)

        if isinstance(history.columns, pd.MultiIndex):
            history.columns = history.columns.get_level_values(0)

        frame = history.rename(columns=str).copy()
        frame = frame.dropna(subset=["Open", "High", "Low", "Close", "Volume"])
        if frame.empty:
            return _empty_chart_payload(ticker)

        benchmark = yf.download(
            tickers="SPY",
            period=period,
            interval="1d",
            auto_adjust=False,
            progress=False,
            threads=False,
        )
        benchmark_frame = _normalize_download_frame(benchmark)

        frame["ma20"] = frame["Close"].rolling(20).mean()
        frame["ma50"] = frame["Close"].rolling(50).mean()
        frame["ma200"] = frame["Close"].rolling(200).mean()
        frame["ema8"] = frame["Close"].ewm(span=8, adjust=False).mean()
        frame["ema21"] = frame["Close"].ewm(span=21, adjust=False).mean()
        weekly_close = frame["Close"].resample("W-FRI").last().dropna()
        weekly_ema8 = weekly_close.ewm(span=8, adjust=False).mean()
        frame["weekly_ema8"] = weekly_ema8.reindex(frame.index, method="ffill")
        frame["ipo_vwap"] = _compute_ipo_vwap(frame)

        rs_line: pd.Series | None = None
        rs_new_high: pd.Series | None = None
        rs_new_high_before_price: pd.Series | None = None
        if benchmark_frame is not None and not benchmark_frame.empty:
            rs_line = _compute_rs_line(frame["Close"], benchmark_frame["Close"])
            rs_new_high, rs_new_high_before_price = _compute_rs_new_high_flags(
                rs_line=rs_line,
                price_reference=frame["High"].reindex(rs_line.index),
                lookback=250,
            )

        candles: list[dict[str, Any]] = []
        volume: list[dict[str, Any]] = []
        ma20: list[dict[str, Any]] = []
        ma50: list[dict[str, Any]] = []
        ma200: list[dict[str, Any]] = []
        ema8: list[dict[str, Any]] = []
        ema21: list[dict[str, Any]] = []
        weekly_ema8_points: list[dict[str, Any]] = []
        ipo_vwap: list[dict[str, Any]] = []
        rs_points: list[dict[str, Any]] = []
        rs_markers: list[dict[str, Any]] = []

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
            if pd.notna(row["ema8"]):
                ema8.append({"time": time_value, "value": float(row["ema8"])})
            if pd.notna(row["ema21"]):
                ema21.append({"time": time_value, "value": float(row["ema21"])})
            if pd.notna(row["weekly_ema8"]):
                weekly_ema8_points.append({"time": time_value, "value": float(row["weekly_ema8"])})
            if pd.notna(row["ipo_vwap"]):
                ipo_vwap.append({"time": time_value, "value": float(row["ipo_vwap"])})
            if rs_line is not None and index in rs_line.index and pd.notna(rs_line.loc[index]):
                rs_points.append({"time": time_value, "value": float(rs_line.loc[index])})
                if rs_new_high is not None and bool(rs_new_high.loc[index]):
                    rs_markers.append(
                        {
                            "time": time_value,
                            "kind": "daily_new_high_before_price" if rs_new_high_before_price is not None and bool(rs_new_high_before_price.loc[index]) else "daily_new_high",
                        }
                    )

        return {
            "ticker": ticker,
            "benchmark_ticker": "SPY",
            "candles": candles,
            "volume": volume,
            "ma20": ma20,
            "ma50": ma50,
            "ma200": ma200,
            "ema8": ema8,
            "ema21": ema21,
            "weekly_ema8": weekly_ema8_points,
            "ipo_vwap": ipo_vwap,
            "rs_line": rs_points,
            "rs_markers": rs_markers,
        }


def _empty_chart_payload(ticker: str) -> dict[str, Any]:
    return {
        "ticker": ticker,
        "benchmark_ticker": "SPY",
        "candles": [],
        "volume": [],
        "ma20": [],
        "ma50": [],
        "ma200": [],
        "ema8": [],
        "ema21": [],
        "weekly_ema8": [],
        "ipo_vwap": [],
        "rs_line": [],
        "rs_markers": [],
    }


def _normalize_download_frame(history: pd.DataFrame | None) -> pd.DataFrame | None:
    if history is None or history.empty:
        return None
    frame = history.copy()
    if isinstance(frame.columns, pd.MultiIndex):
        frame.columns = frame.columns.get_level_values(0)
    frame = frame.rename(columns=str)
    frame = frame.dropna(subset=["Open", "High", "Low", "Close", "Volume"])
    return frame if not frame.empty else None


def _compute_ipo_vwap(frame: pd.DataFrame) -> pd.Series:
    source = frame.dropna(subset=["High", "Low", "Close", "Volume"]).copy()
    typical_price = (source["High"] + source["Low"] + source["Close"]) / 3.0
    cumulative_value = (typical_price * source["Volume"]).cumsum()
    cumulative_volume = source["Volume"].cumsum().replace(0, pd.NA)
    ipo_vwap = cumulative_value / cumulative_volume
    return ipo_vwap.reindex(frame.index)


def _compute_rs_line(stock: pd.Series, benchmark: pd.Series) -> pd.Series:
    aligned = pd.concat([stock, benchmark], axis=1, join="inner").dropna()
    aligned.columns = ["stock", "benchmark"]
    return aligned["stock"] / aligned["benchmark"]


def _compute_rs_new_high_flags(rs_line: pd.Series, price_reference: pd.Series, lookback: int) -> tuple[pd.Series, pd.Series]:
    aligned = pd.concat([rs_line, price_reference], axis=1, join="inner").dropna()
    aligned.columns = ["rs_line", "price_reference"]
    rolling_rs_high = aligned["rs_line"].rolling(window=lookback, min_periods=1).max()
    rolling_price_high = aligned["price_reference"].rolling(window=lookback, min_periods=1).max()
    tolerance = 1e-12
    new_high = aligned["rs_line"] >= (rolling_rs_high - tolerance)
    new_high_before_price = new_high & (aligned["price_reference"] < (rolling_price_high - tolerance))
    return new_high.reindex(rs_line.index, fill_value=False), new_high_before_price.reindex(rs_line.index, fill_value=False)
