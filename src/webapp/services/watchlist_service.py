from __future__ import annotations

import datetime as dt
from pathlib import Path
from typing import Any
import logging

import numpy as np
import pandas as pd
import yfinance as yf

from ...config import AppConfig
from ...etf_matcher import infer_theme_tags_for_ticker, load_etf_catalog, load_ticker_theme_overrides
from ...market_data_access import load_many_ticker_windows_for_range, resolve_database_url, resolve_market_data_source
from ...ticker_filters import normalize_ticker_symbol
from ...universe import UniverseTicker, load_universe
from ...config import load_app_config
from ..repositories.watchlist_repository import WatchlistRepository


logger = logging.getLogger(__name__)


class WatchlistService:
    def __init__(
        self,
        artifacts_dir: Path,
        *,
        database_url: str | None = None,
        market_data_source: str | None = None,
        benchmark_ticker: str = "SPY",
    ) -> None:
        self.repository = WatchlistRepository(artifacts_dir=artifacts_dir)
        self._universe_index: dict[str, UniverseTicker] | None = None
        self._theme_catalog: list[dict[str, object]] | None = None
        self.database_url = resolve_database_url(database_url)
        self.market_data_source = resolve_market_data_source(market_data_source)
        self.benchmark_ticker = str(benchmark_ticker or "SPY").strip().upper() or "SPY"

    def list_recent(self) -> list[dict[str, Any]]:
        return self.repository.list_recent_watchlists(limit=50)

    def get_watchlist_detail(self, stem: str) -> dict[str, Any]:
        entries = self._enrich_entries(self.repository.load_watchlist(stem))
        return {
            "stem": stem,
            "entry_count": len(entries),
            "entries": entries[:200],
        }

    def get_chart_payload(
        self,
        ticker: str,
        period: str = "18mo",
        *,
        as_of_date: dt.date | None = None,
    ) -> dict[str, Any]:
        normalized_ticker = str(ticker or "").strip().upper()
        requested_as_of_date = as_of_date
        chart_request = _resolve_chart_request(period=period, as_of_date=as_of_date)
        frame, benchmark_frame, data_source = self._load_chart_frames(
            ticker=normalized_ticker,
            benchmark_ticker=self.benchmark_ticker,
            start_date=chart_request.fetch_start_date,
            end_date=chart_request.fetch_end_date,
            warmup_trading_days=chart_request.warmup_trading_days,
        )
        if frame is None or frame.empty:
            return _empty_chart_payload(
                normalized_ticker,
                period=period,
                requested_as_of_date=requested_as_of_date,
                benchmark_ticker=self.benchmark_ticker,
                data_source=data_source,
            )

        frame = frame.sort_index()
        resolved_as_of_timestamp = frame.index.max()
        resolved_as_of_date = pd.Timestamp(resolved_as_of_timestamp).date()
        visible_start_date = chart_request.visible_start_date
        visible_frame = frame.loc[(frame.index.date >= visible_start_date) & (frame.index.date <= resolved_as_of_date)].copy()
        if visible_frame.empty:
            return _empty_chart_payload(
                normalized_ticker,
                period=period,
                requested_as_of_date=requested_as_of_date,
                resolved_as_of_date=resolved_as_of_date,
                benchmark_ticker=self.benchmark_ticker,
                data_source=data_source,
            )

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
        fearzone_panel = _filter_fearzone_panel(
            _compute_fearzone_panel(frame),
            visible_dates={pd.Timestamp(index).date().isoformat() for index in visible_frame.index},
        )
        if benchmark_frame is not None and not benchmark_frame.empty:
            benchmark_frame = benchmark_frame.sort_index()
            benchmark_frame = benchmark_frame.loc[benchmark_frame.index <= pd.Timestamp(resolved_as_of_date)].copy()
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

        visible_index_set = set(visible_frame.index)

        for index, row in frame.iterrows():
            if index not in visible_index_set:
                continue
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
            "ticker": normalized_ticker,
            "benchmark_ticker": self.benchmark_ticker,
            "period": period,
            "requested_as_of_date": requested_as_of_date.isoformat() if requested_as_of_date else None,
            "resolved_as_of_date": resolved_as_of_date.isoformat(),
            "latest_available_date": resolved_as_of_date.isoformat(),
            "data_source": data_source,
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
            "fearzone_panel": fearzone_panel,
        }

    def _load_chart_frames(
        self,
        *,
        ticker: str,
        benchmark_ticker: str,
        start_date: dt.date,
        end_date: dt.date,
        warmup_trading_days: int,
    ) -> tuple[pd.DataFrame | None, pd.DataFrame | None, str]:
        if self.market_data_source == "database-first" and self.database_url:
            frames = load_many_ticker_windows_for_range(
                [ticker, benchmark_ticker],
                start_date=start_date,
                end_date=end_date,
                trading_days_needed=warmup_trading_days,
                database_url=self.database_url,
            )
            ticker_frame = _normalize_download_frame(frames.get(ticker))
            benchmark_frame = _normalize_download_frame(frames.get(benchmark_ticker))
            if ticker_frame is not None and not ticker_frame.empty:
                return ticker_frame, benchmark_frame, "database"

        ticker_frame = _download_history_frame(ticker=ticker, start_date=start_date, end_date=end_date)
        benchmark_frame = _download_history_frame(ticker=benchmark_ticker, start_date=start_date, end_date=end_date)
        return ticker_frame, benchmark_frame, "internet"

    def _enrich_entries(self, entries: list[dict[str, Any]]) -> list[dict[str, Any]]:
        universe_index = self._get_universe_index()
        overrides = load_ticker_theme_overrides()
        theme_catalog = self._get_theme_catalog()
        enriched: list[dict[str, Any]] = []
        for raw_entry in entries:
            entry = dict(raw_entry)
            ticker = normalize_ticker_symbol(str(entry.get("ticker", "")))
            metadata = universe_index.get(ticker)
            sector = _coalesce_text(entry.get("sector"), metadata.sector if metadata else None)
            industry = _coalesce_text(entry.get("industry"), metadata.industry if metadata else None)
            exchange = _coalesce_text(entry.get("exchange"), metadata.exchange if metadata else None)
            theme_tags = _normalize_theme_tags(entry.get("theme_tags"))
            if not theme_tags and ticker:
                theme_tags = infer_theme_tags_for_ticker(
                    ticker=ticker,
                    sector=sector,
                    industry=industry,
                    catalog=theme_catalog,
                    overrides=overrides,
                )
            if ticker:
                entry["ticker"] = ticker
            if sector:
                entry["sector"] = sector
            if industry:
                entry["industry"] = industry
            if exchange:
                entry["exchange"] = exchange
            if theme_tags:
                entry["theme_tags"] = theme_tags
            enriched.append(entry)
        return enriched

    def _get_universe_index(self) -> dict[str, UniverseTicker]:
        if self._universe_index is not None:
            return self._universe_index
        try:
            universe = load_universe(load_app_config())
        except Exception as exc:
            logger.warning("Watchlist universe enrichment unavailable; continuing without universe metadata: %s", exc)
            universe = []
        self._universe_index = {
            normalize_ticker_symbol(item.symbol): item
            for item in universe
            if getattr(item, "symbol", "")
        }
        return self._universe_index

    def _get_theme_catalog(self) -> list[dict[str, object]]:
        if self._theme_catalog is not None:
            return self._theme_catalog
        try:
            self._theme_catalog = load_etf_catalog()
        except Exception as exc:
            logger.warning("Watchlist theme enrichment unavailable; continuing without ETF catalog: %s", exc)
            self._theme_catalog = []
        return self._theme_catalog


def _empty_chart_payload(
    ticker: str,
    *,
    period: str = "18mo",
    requested_as_of_date: dt.date | None = None,
    resolved_as_of_date: dt.date | None = None,
    benchmark_ticker: str = "SPY",
    data_source: str = "internet",
) -> dict[str, Any]:
    return {
        "ticker": ticker,
        "benchmark_ticker": benchmark_ticker,
        "period": period,
        "requested_as_of_date": requested_as_of_date.isoformat() if requested_as_of_date else None,
        "resolved_as_of_date": resolved_as_of_date.isoformat() if resolved_as_of_date else None,
        "latest_available_date": resolved_as_of_date.isoformat() if resolved_as_of_date else None,
        "data_source": data_source,
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
        "fearzone_panel": {"rows": [], "signals": []},
    }


def _normalize_download_frame(history: pd.DataFrame | None) -> pd.DataFrame | None:
    if history is None or history.empty:
        return None
    frame = history.copy()
    if isinstance(frame.columns, pd.MultiIndex):
        frame.columns = frame.columns.get_level_values(0)
    frame = frame.rename(columns=str)
    for column in ("Open", "High", "Low", "Close", "Adj Close", "Volume"):
        if column in frame.columns:
            frame[column] = pd.to_numeric(frame[column], errors="coerce")
    frame = frame.dropna(subset=["Open", "High", "Low", "Close", "Volume"])
    return frame if not frame.empty else None


def _download_history_frame(*, ticker: str, start_date: dt.date, end_date: dt.date) -> pd.DataFrame | None:
    history = yf.download(
        tickers=ticker,
        start=start_date.isoformat(),
        end=(end_date + dt.timedelta(days=1)).isoformat(),
        interval="1d",
        auto_adjust=False,
        progress=False,
        threads=False,
    )
    return _normalize_download_frame(history)


class _ChartRequest:
    def __init__(
        self,
        *,
        visible_start_date: dt.date,
        fetch_start_date: dt.date,
        fetch_end_date: dt.date,
        warmup_trading_days: int,
    ) -> None:
        self.visible_start_date = visible_start_date
        self.fetch_start_date = fetch_start_date
        self.fetch_end_date = fetch_end_date
        self.warmup_trading_days = warmup_trading_days


def _resolve_chart_request(*, period: str, as_of_date: dt.date | None) -> _ChartRequest:
    resolved_period = str(period or "18mo").strip().lower() or "18mo"
    anchor_date = as_of_date or dt.date.today()
    visible_days = _period_to_calendar_days(resolved_period)
    visible_start_date = anchor_date - dt.timedelta(days=visible_days)
    warmup_days = 420
    fetch_start_date = visible_start_date - dt.timedelta(days=warmup_days * 2)
    return _ChartRequest(
        visible_start_date=visible_start_date,
        fetch_start_date=fetch_start_date,
        fetch_end_date=anchor_date,
        warmup_trading_days=warmup_days,
    )


def _period_to_calendar_days(period: str) -> int:
    lookup = {
        "6mo": 190,
        "12mo": 380,
        "18mo": 570,
        "24mo": 760,
        "3y": 1140,
        "5y": 1900,
    }
    return lookup.get(period, 570)


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


def _compute_fearzone_panel(frame: pd.DataFrame) -> dict[str, Any]:
    if frame.empty:
        return {"rows": [], "signals": []}

    config = AppConfig()
    source = frame[["Open", "High", "Low", "Close"]].mean(axis=1)
    high_period = int(config.fearzone_high_period)
    band_period = int(config.fearzone_band_period)

    highest_source = source.rolling(high_period).max()
    fz1_value = (highest_source - source) / highest_source.replace(0, np.nan)
    fz1_basis = fz1_value.rolling(band_period).mean()
    fz1_std = fz1_value.rolling(band_period).std(ddof=0)
    fz1_upper = fz1_basis + (fz1_std * float(config.fearzone_band_std_multiplier))
    in_fz1 = (fz1_value > fz1_upper).fillna(False)

    source_ma = source.rolling(high_period).mean()
    fz2_value = source - source_ma
    fz2_basis = fz2_value.rolling(band_period).mean()
    fz2_std = fz2_value.rolling(band_period).std(ddof=0)
    fz2_lower = fz2_basis - (fz2_std * float(config.fearzone_band_std_multiplier))
    in_fz2 = (fz2_value < fz2_lower).fillna(False)

    impulse_pct = ((frame["Close"] / frame["Close"].shift(int(config.fearzone_negative_impulse_lookback_days))) - 1.0) * 100.0
    negative_impulse = (impulse_pct <= (-abs(float(config.fearzone_negative_impulse_pct)))).fillna(False)

    bar_range = frame["High"] - frame["Low"]
    range_floor = frame["Low"] + (bar_range * float(config.fearzone_ricochet_zone_pct))
    in_ricochet_zone = (frame["Close"] <= range_floor).fillna(False)

    lowest_low = frame["Low"].rolling(int(config.fearzone_stochastic_k)).min()
    highest_high = frame["High"].rolling(int(config.fearzone_stochastic_k)).max()
    stoch_range = highest_high - lowest_low
    raw_k = pd.Series(
        np.where(stoch_range > 0, (frame["Close"] - lowest_low) * 100.0 / stoch_range, np.nan),
        index=frame.index,
    )
    fast_k = raw_k.rolling(int(config.fearzone_stochastic_d)).mean()
    slow_k = fast_k.rolling(int(config.fearzone_stochastic_d)).mean()
    magic_k1 = (slow_k < float(config.fearzone_magic_k1_threshold)).fillna(False)

    ma200 = frame["Close"].rolling(int(config.fearzone_ma_long_period)).mean()
    above_ma200 = (frame["Close"] > ma200).fillna(False)

    signals = (in_fz1 & in_fz2 & above_ma200 & (negative_impulse | in_ricochet_zone | magic_k1)).fillna(False)

    row_defs = [
        ("fz1", "FZ1", "#4ade80", in_fz1),
        ("fz2", "FZ2", "#4ade80", in_fz2),
        ("negative_impulse", "Down 10%", "#60a5fa", negative_impulse),
        ("ricochet_zone", "Ricochet", "#fde047", in_ricochet_zone),
        ("magic_k1", "Magic-K1", "#f8fafc", magic_k1),
        ("above_ma200", "Above MA200", "#4ade80", above_ma200),
    ]

    rows: list[dict[str, Any]] = []
    for key, label, active_color, series in row_defs:
        points: list[dict[str, Any]] = []
        for index, active in series.items():
            points.append(
                {
                    "time": pd.Timestamp(index).date().isoformat(),
                    "active": bool(active),
                }
            )
        rows.append(
            {
                "key": key,
                "label": label,
                "active_color": active_color,
                "inactive_color": "#71717a",
                "points": points,
            }
        )

    signal_points = [{"time": pd.Timestamp(index).date().isoformat()} for index, active in signals.items() if bool(active)]
    return {"rows": rows, "signals": signal_points}


def _filter_fearzone_panel(panel: dict[str, Any], *, visible_dates: set[str]) -> dict[str, Any]:
    if not visible_dates:
        return {"rows": [], "signals": []}
    return {
        "rows": [
            {
                **row,
                "points": [point for point in row.get("points", []) if point.get("time") in visible_dates],
            }
            for row in panel.get("rows", [])
        ],
        "signals": [signal for signal in panel.get("signals", []) if signal.get("time") in visible_dates],
    }


def _coalesce_text(*values: object) -> str | None:
    for value in values:
        text = str(value or "").strip()
        if text:
            return text
    return None


def _normalize_theme_tags(value: object) -> list[str]:
    if not isinstance(value, list):
        return []
    tags: list[str] = []
    for item in value:
        text = str(item or "").strip()
        if text and text not in tags:
            tags.append(text)
    return tags
