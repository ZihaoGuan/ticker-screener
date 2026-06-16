from __future__ import annotations

import copy
import datetime as dt
import html
from io import StringIO
import json
from pathlib import Path
import re
import subprocess
import logging
import threading
import time
from typing import Any
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd
import requests
import yfinance as yf

from ...config import AppConfig
from ...etf_matcher import infer_theme_tags_for_ticker, load_etf_catalog, load_ticker_theme_overrides
from ...ftd_sweep_screen import find_recent_ftd_sweep_hit
from ...market_extension import compute_extension_frame, resample_to_weekly
from ...market_data_access import (
    db_frame_has_recent_coverage,
    load_many_ticker_windows,
    load_many_ticker_windows_for_range,
    resolve_database_url,
    resolve_market_data_source,
)
from ...ratings.repository import RatingsRepository
from ...rs_rating_screen import approximate_rs_rating, compute_weighted_rs_score
from ...sepa_vcp_screen import build_sepa_dashboard_snapshot
from ...ticker_filters import is_excluded_ticker, load_excluded_tickers, normalize_ticker_symbol
from ...universe import UniverseTicker, load_universe
from ...vcs_indicator import latest_vcs_snapshot
from ...config import load_app_config
from ..repositories.insider_repository import InsiderRepository
from ..repositories.watchlist_repository import WatchlistRepository
from .insider_fetcher import fetch_insider_trades_window


logger = logging.getLogger(__name__)
_YAHOO_BROWSER_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
}
_YAHOO_SCRAPE_TIMEOUT = (3.05, 4.0)
_YAHOO_PLAYWRIGHT_TIMEOUT_SECONDS = 40
_YAHOO_BLOCK_MARKERS = (
    "Too Many Requests",
    "Edge: Too Many Requests",
    "Will be right back",
)
_REPO_ROOT = Path(__file__).resolve().parents[3]
_YAHOO_ANALYSIS_HOLDERS_PROBE_SCRIPT = _REPO_ROOT / "frontend" / "scripts" / "probe_yahoo_playwright.mjs"
_YAHOO_OPTIONS_PROBE_SCRIPT = _REPO_ROOT / "frontend" / "scripts" / "probe_yahoo_options_playwright.mjs"
_INSIDER_CACHE_TTL_HOURS = 12
_CHART_PAYLOAD_CACHE_TTL_SECONDS = 5 * 60
_NEW_YORK_TZ = ZoneInfo("America/New_York")
_SCANNER_BOARD_CUTOFF_HOUR = 20
_SCANNER_BOARD_CUTOFF_MINUTE = 30
_chart_payload_cache: dict[tuple[str, str, str, str, str, str], tuple[float, dict[str, Any]]] = {}
_chart_payload_cache_lock = threading.Lock()
_chart_overlay_cache: dict[tuple[str, str, str, str, str, str], tuple[float, dict[str, Any]]] = {}
_chart_overlay_cache_lock = threading.Lock()
_SCANNER_BOARD_CONFIG: tuple[dict[str, str], ...] = (
    {
        "id": "weekly_rs",
        "strategy_id": "weekly_rs",
        "label": "Weekly RS New High",
        "description": "Relative-strength leaders holding leadership while price still has room to catch up.",
        "timeframe": "Weekly",
        "accent": "violet",
    },
    {
        "id": "sean_gap_up",
        "strategy_id": "sean_peg",
        "label": "Sean Gap Up",
        "description": "Post-earnings gap leaders with HVE or HV1 volume, tight structure, and continuation context.",
        "timeframe": "Daily",
        "accent": "amber",
    },
    {
        "id": "sepa_vcp",
        "strategy_id": "sepa_vcp",
        "label": "SEPA VCP",
        "description": "Recent 5D squeeze names with Minervini trend, risk, pressure, and RS dashboard context persisted together.",
        "timeframe": "Daily",
        "accent": "cyan",
    },
    {
        "id": "trend_template",
        "strategy_id": "trend_template",
        "label": "Trend Template",
        "description": "Minervini trend-template names already above stacked 50/150/200 moving averages and still near 52-week highs.",
        "timeframe": "Daily",
        "accent": "lime",
    },
    {
        "id": "sean_breakout",
        "strategy_id": "sean_breakout",
        "label": "Sean Breakout",
        "description": "Daily leaders closing above 21 and 50 EMA with price, volume, and ADR thresholds already cleared.",
        "timeframe": "Daily",
        "accent": "amber",
    },
    {
        "id": "fearzone",
        "strategy_id": "fearzone",
        "label": "Fearzone",
        "description": "High-velocity dislocation setups where panic can create asymmetric snapback entries.",
        "timeframe": "Daily",
        "accent": "cyan",
    },
    {
        "id": "td9_bullish",
        "strategy_id": "td9_bullish",
        "label": "TD9 Bullish",
        "description": "Bullish TD Sequential exhaustion names where downside pressure may be finishing.",
        "timeframe": "Daily",
        "accent": "lime",
    },
)


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
        self.insider_repository = InsiderRepository(artifacts_dir=artifacts_dir)
        self._universe_index: dict[str, UniverseTicker] | None = None
        self._theme_catalog: list[dict[str, object]] | None = None
        self.database_url = resolve_database_url(database_url)
        self.market_data_source = resolve_market_data_source(market_data_source)
        self.benchmark_ticker = str(benchmark_ticker or "SPY").strip().upper() or "SPY"
        self._excluded_tickers: set[str] | None = None

    def list_recent(self) -> list[dict[str, Any]]:
        return self.repository.list_recent_watchlists(limit=50)

    def get_scanner_board(self, *, now: dt.datetime | None = None) -> dict[str, Any]:
        reference_now = _normalize_scanner_now(now)
        target_trading_date = _latest_completed_trading_day(reference_now)
        recent_watchlists = self.repository.list_recent_watchlists(limit=400)
        cards: list[dict[str, Any]] = []

        for config in _SCANNER_BOARD_CONFIG:
            selected_meta = _select_scanner_board_watchlist(
                recent_watchlists,
                strategy_id=config["strategy_id"],
                target_date=target_trading_date,
            )
            entries = self._filter_excluded_entries(
                self.repository.load_watchlist(str(selected_meta.get("stem") or "")) if selected_meta else []
            )
            preview_tickers = [
                str(item.get("ticker") or "").strip().upper()
                for item in entries
                if isinstance(item, dict) and str(item.get("ticker") or "").strip()
            ][:6]
            cards.append(
                {
                    "id": config["id"],
                    "strategy_id": config["strategy_id"],
                    "label": config["label"],
                    "description": config["description"],
                    "timeframe": config["timeframe"],
                    "accent": config["accent"],
                    "available": selected_meta is not None and len(entries) > 0,
                    "stem": str(selected_meta.get("stem") or "") if selected_meta else "",
                    "group_label": str(selected_meta.get("group_label") or "") if selected_meta else "",
                    "captured_at": str(selected_meta.get("captured_at") or "") if selected_meta else "",
                    "sort_date": str(selected_meta.get("sort_date") or "") if selected_meta else "",
                    "entry_count": len(entries),
                    "preview_tickers": preview_tickers,
                    "list_href": (
                        f"/watchlists?stem={selected_meta.get('stem')}"
                        if selected_meta and selected_meta.get("stem")
                        else None
                    ),
                }
            )

        available_cards = [item for item in cards if item["available"]]
        latest_update_at = max((str(item["captured_at"] or "") for item in available_cards), default="")
        latest_signal_date = max((str(item["sort_date"] or "") for item in available_cards), default="")
        return {
            "generated_at": reference_now.astimezone(dt.timezone.utc).isoformat().replace("+00:00", "Z"),
            "reference_now_new_york": reference_now.astimezone(_NEW_YORK_TZ).isoformat(),
            "target_trading_date": target_trading_date.isoformat(),
            "cutoff_time_label": "20:30 America/New_York",
            "latest_update_at": latest_update_at,
            "latest_signal_date": latest_signal_date,
            "cards": cards,
        }

    def get_weekly_watchlist_board(self, stem: str | None = None) -> dict[str, Any]:
        weekly_files = [item for item in self.repository.list_recent_watchlists(limit=200) if item.get("group_key") == "weekly_rs"]
        selected_meta = None
        normalized_stem = str(stem or "").strip()
        if normalized_stem:
            selected_meta = next((item for item in weekly_files if str(item.get("stem") or "") == normalized_stem), None)
        if selected_meta is None and weekly_files:
            selected_meta = weekly_files[0]
        selected_stem = str(selected_meta.get("stem") or "") if isinstance(selected_meta, dict) else ""
        entries = self._enrich_entries(self._filter_excluded_entries(self.repository.load_watchlist(selected_stem))) if selected_stem else []
        board_entries: list[dict[str, Any]] = []
        for raw_entry in entries:
            entry = dict(raw_entry)
            signal_badges = _build_weekly_signal_badges(entry)
            if signal_badges:
                entry["signal_badges"] = signal_badges
            board_entries.append(entry)
        return {
            "source_stem": selected_stem,
            "source_name": str(selected_meta.get("name") or "") if isinstance(selected_meta, dict) else "",
            "captured_at": str(selected_meta.get("captured_at") or "") if isinstance(selected_meta, dict) else "",
            "sort_date": str(selected_meta.get("sort_date") or "") if isinstance(selected_meta, dict) else "",
            "group_label": "Weekly RS",
            "entry_count": len(board_entries),
            "entries": board_entries,
            "available_files": weekly_files[:24],
        }

    def get_watchlist_detail(self, stem: str) -> dict[str, Any]:
        entries = self._enrich_entries(self._filter_excluded_entries(self.repository.load_watchlist(stem)))
        return {
            "stem": stem,
            "entry_count": len(entries),
            "entries": entries,
        }

    def get_chart_payload(
        self,
        ticker: str,
        period: str = "18mo",
        *,
        as_of_date: dt.date | None = None,
        include_setup_markers: bool = False,
    ) -> dict[str, Any]:
        normalized_ticker = str(ticker or "").strip().upper()
        requested_as_of_date = as_of_date
        cache_key = _build_chart_payload_cache_key(
            ticker=normalized_ticker,
            period=period,
            as_of_date=as_of_date,
            benchmark_ticker=self.benchmark_ticker,
            market_data_source=self.market_data_source,
            include_setup_markers=include_setup_markers,
        )
        cached_payload = _read_chart_payload_cache(cache_key)
        if cached_payload is not None:
            return cached_payload
        chart_request = _resolve_chart_request(period=period, as_of_date=as_of_date)
        frame, benchmark_frame, data_source = self._load_chart_frames(
            ticker=normalized_ticker,
            benchmark_ticker=self.benchmark_ticker,
            required_start_date=chart_request.visible_start_date,
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
        candles: list[dict[str, Any]] = []
        volume: list[dict[str, Any]] = []
        ma20: list[dict[str, Any]] = []
        ma50: list[dict[str, Any]] = []
        ma200: list[dict[str, Any]] = []
        ema8: list[dict[str, Any]] = []
        ema21: list[dict[str, Any]] = []
        weekly_ema8_points: list[dict[str, Any]] = []
        ipo_vwap: list[dict[str, Any]] = []

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

        payload = {
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
            "market_extension": _empty_market_extension_overlay(),
            "rs_line": [],
            "daily_rs_rating": [],
            "weekly_rs_rating": [],
            "rs_markers": [],
            "setup_markers": [],
            "fearzone_panel": {"rows": [], "signals": []},
            "vcs": None,
            "sepa_dashboard": None,
        }
        if payload["candles"]:
            _write_chart_payload_cache(cache_key, payload)
        return payload

    def get_chart_overlays_payload(
        self,
        ticker: str,
        period: str = "18mo",
        *,
        as_of_date: dt.date | None = None,
        include_setup_markers: bool = False,
    ) -> dict[str, Any]:
        normalized_ticker = str(ticker or "").strip().upper()
        requested_as_of_date = as_of_date
        cache_key = _build_chart_overlay_cache_key(
            ticker=normalized_ticker,
            period=period,
            as_of_date=as_of_date,
            benchmark_ticker=self.benchmark_ticker,
            market_data_source=self.market_data_source,
            include_setup_markers=include_setup_markers,
        )
        cached_payload = _read_chart_overlay_cache(cache_key)
        if cached_payload is not None:
            return cached_payload
        chart_request = _resolve_chart_request(period=period, as_of_date=as_of_date)
        frame, benchmark_frame, data_source = self._load_chart_frames(
            ticker=normalized_ticker,
            benchmark_ticker=self.benchmark_ticker,
            required_start_date=chart_request.visible_start_date,
            start_date=chart_request.fetch_start_date,
            end_date=chart_request.fetch_end_date,
            warmup_trading_days=chart_request.warmup_trading_days,
        )
        if frame is None or frame.empty:
            return _empty_chart_overlay_payload(
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
            return _empty_chart_overlay_payload(
                normalized_ticker,
                period=period,
                requested_as_of_date=requested_as_of_date,
                resolved_as_of_date=resolved_as_of_date,
                benchmark_ticker=self.benchmark_ticker,
                data_source=data_source,
            )

        visible_dates = {pd.Timestamp(index).date().isoformat() for index in visible_frame.index}
        market_extension = _compute_market_extension_overlay(frame, visible_dates=visible_dates)

        rs_line: pd.Series | None = None
        rs_new_high: pd.Series | None = None
        rs_new_high_before_price: pd.Series | None = None
        daily_rs_rating: pd.Series | None = None
        weekly_rs_rating: pd.Series | None = None
        fearzone_panel = _filter_fearzone_panel(
            _compute_fearzone_panel(frame),
            visible_dates=visible_dates,
        )
        vcs_snapshot = latest_vcs_snapshot(frame)
        setup_markers = (
            _compute_ftd_sweep_markers(
                frame=frame,
                visible_dates=visible_dates,
                ticker=normalized_ticker,
                benchmark_ticker=self.benchmark_ticker,
            )
            if include_setup_markers
            else []
        )
        if benchmark_frame is not None and not benchmark_frame.empty:
            benchmark_frame = benchmark_frame.sort_index()
            benchmark_frame = benchmark_frame.loc[benchmark_frame.index <= pd.Timestamp(resolved_as_of_date)].copy()
            rs_line = _compute_rs_line(frame["Close"], benchmark_frame["Close"])
            daily_rs_rating = _compute_rs_rating_series(frame["Close"], benchmark_frame["Close"])
            if daily_rs_rating is not None and not daily_rs_rating.empty:
                weekly_rs_rating = daily_rs_rating.resample("W-FRI").last().dropna()
        sepa_dashboard = (
            build_sepa_dashboard_snapshot(
                frame,
                benchmark_frame if benchmark_frame is not None else pd.DataFrame(),
                benchmark_ticker=self.benchmark_ticker,
            )
            if benchmark_frame is not None and not benchmark_frame.empty
            else None
        )

        rs_points: list[dict[str, Any]] = []
        daily_rs_rating_points: list[dict[str, Any]] = []
        weekly_rs_rating_points: list[dict[str, Any]] = []
        rs_markers: list[dict[str, Any]] = []
        visible_index_set = set(visible_frame.index)
        if rs_line is not None and benchmark_frame is not None and not benchmark_frame.empty:
            rs_new_high, rs_new_high_before_price = _compute_rs_new_high_flags(
                rs_line=rs_line,
                price_reference=frame["High"].reindex(rs_line.index),
                lookback=250,
            )
        for index, row in frame.iterrows():
            if index not in visible_index_set:
                continue
            time_value = pd.Timestamp(index).date().isoformat()
            if rs_line is not None and index in rs_line.index and pd.notna(rs_line.loc[index]):
                rs_points.append({"time": time_value, "value": float(rs_line.loc[index])})
                if rs_new_high is not None and bool(rs_new_high.loc[index]):
                    rs_markers.append(
                        {
                            "time": time_value,
                            "kind": "daily_new_high_before_price" if rs_new_high_before_price is not None and bool(rs_new_high_before_price.loc[index]) else "daily_new_high",
                        }
                    )
            if daily_rs_rating is not None and index in daily_rs_rating.index and pd.notna(daily_rs_rating.loc[index]):
                daily_rs_rating_points.append({"time": time_value, "value": float(daily_rs_rating.loc[index])})

        if weekly_rs_rating is not None and not weekly_rs_rating.empty:
            for index, value in weekly_rs_rating.items():
                week_date = pd.Timestamp(index).date()
                if week_date < visible_start_date or week_date > resolved_as_of_date:
                    continue
                if pd.notna(value):
                    weekly_rs_rating_points.append({"time": week_date.isoformat(), "value": float(value)})

        payload = {
            "ticker": normalized_ticker,
            "benchmark_ticker": self.benchmark_ticker,
            "period": period,
            "requested_as_of_date": requested_as_of_date.isoformat() if requested_as_of_date else None,
            "resolved_as_of_date": resolved_as_of_date.isoformat(),
            "latest_available_date": resolved_as_of_date.isoformat(),
            "data_source": data_source,
            "market_extension": market_extension,
            "rs_line": rs_points,
            "daily_rs_rating": daily_rs_rating_points,
            "weekly_rs_rating": weekly_rs_rating_points,
            "rs_markers": rs_markers,
            "setup_markers": setup_markers,
            "fearzone_panel": fearzone_panel,
            "vcs": vcs_snapshot.to_dict() if vcs_snapshot is not None else None,
            "sepa_dashboard": sepa_dashboard.to_dict() if sepa_dashboard is not None else None,
        }
        _write_chart_overlay_cache(cache_key, payload)
        return payload

    def get_chart_fundamentals_payload(self, ticker: str, *, earnings_limit: int = 4) -> dict[str, Any]:
        normalized_ticker = str(ticker or "").strip().upper()
        ratings_repository = RatingsRepository(self.database_url) if self.database_url else None
        ratings_bundle = ratings_repository.load_latest_ticker_rating_bundle(normalized_ticker) if ratings_repository else None
        cached_entry = ratings_repository.load_latest_chart_fundamentals_cache_entry(normalized_ticker) if ratings_repository else None
        if _chart_fundamentals_cache_is_complete(cached_entry):
            return {
                "ticker": normalized_ticker,
                "earnings_eps_history": list(cached_entry.get("earnings_eps_history") or [])[: max(1, earnings_limit)],
                "holders_float_held_by_institutions_pct": cached_entry.get("holders_float_held_by_institutions_pct"),
                "revenue_yoy_pct": cached_entry.get("revenue_yoy_pct"),
                "earnings_yoy_pct": cached_entry.get("earnings_yoy_pct"),
                "implied_move": cached_entry.get("implied_move"),
                "fundamentals_snapshot": ratings_bundle.get("fundamentals_snapshot") if ratings_bundle else None,
                "rating_snapshot": ratings_bundle.get("rating_snapshot") if ratings_bundle else None,
                "rating_diagnostics": ratings_bundle.get("rating_diagnostics") if ratings_bundle else None,
                "diagnostics": _chart_cache_diagnostics(cached_entry),
            }

        earnings_rows, holders_pct, revenue_yoy_pct, earnings_yoy_pct, browser_diagnostics = _load_yahoo_earnings_and_holders_playwright(
            normalized_ticker,
            earnings_limit=max(earnings_limit, 8),
        )
        implied_move, options_diagnostics = _load_yahoo_implied_move_playwright(normalized_ticker)
        merged_payload = _merge_chart_fundamentals_cache_fields(
            cached_entry,
            earnings_rows=earnings_rows,
            holders_pct=holders_pct,
            revenue_yoy_pct=revenue_yoy_pct,
            earnings_yoy_pct=earnings_yoy_pct,
            implied_move=implied_move,
        )
        if ratings_repository:
            ratings_repository.ensure_ticker_metadata_stub(normalized_ticker, source="chart-fundamentals-cache")
            ratings_repository.upsert_chart_fundamentals_cache_entry(
                ticker=normalized_ticker,
                as_of_date=dt.date.today(),
                earnings_eps_history=list(merged_payload["earnings_eps_history"]),
                holders_float_held_by_institutions_pct=merged_payload["holders_float_held_by_institutions_pct"],
                revenue_yoy_pct=merged_payload["revenue_yoy_pct"],
                earnings_yoy_pct=merged_payload["earnings_yoy_pct"],
                implied_move=merged_payload["implied_move"],
                source_summary={
                    "source": "yahoo-playwright",
                    "diagnostics": {
                        "earnings": browser_diagnostics["earnings"],
                        "holders": browser_diagnostics["holders"],
                        "statistics": browser_diagnostics["statistics"],
                        "options": options_diagnostics,
                    },
                    "cached_entry_used": bool(cached_entry),
                },
            )
        return {
            "ticker": normalized_ticker,
            "earnings_eps_history": list(merged_payload["earnings_eps_history"])[: max(1, earnings_limit)],
            "holders_float_held_by_institutions_pct": merged_payload["holders_float_held_by_institutions_pct"],
            "revenue_yoy_pct": merged_payload["revenue_yoy_pct"],
            "earnings_yoy_pct": merged_payload["earnings_yoy_pct"],
            "implied_move": merged_payload["implied_move"],
            "fundamentals_snapshot": ratings_bundle.get("fundamentals_snapshot") if ratings_bundle else None,
            "rating_snapshot": ratings_bundle.get("rating_snapshot") if ratings_bundle else None,
            "rating_diagnostics": ratings_bundle.get("rating_diagnostics") if ratings_bundle else None,
            "diagnostics": {
                "earnings": browser_diagnostics["earnings"],
                "holders": browser_diagnostics["holders"],
                "statistics": browser_diagnostics["statistics"],
                "options": options_diagnostics,
            },
        }

    def get_top_ratings_payload(
        self,
        *,
        as_of_date: dt.date | None = None,
        limit: int = 100,
        rating_status: str = "ok",
    ) -> dict[str, Any]:
        if not self.database_url:
            return {
                "as_of_date": None,
                "limit": limit,
                "rating_status": rating_status,
                "rows": [],
                "status_counts": {},
                "database_configured": False,
            }
        payload = RatingsRepository(self.database_url).list_top_rating_snapshots(
            as_of_date=as_of_date,
            limit=limit,
            rating_status=rating_status,
        )
        payload["limit"] = max(1, min(int(limit), 500))
        payload["rating_status"] = str(rating_status or "").strip().lower() or "ok"
        payload["database_configured"] = True
        return payload

    def get_top_technical_ratings_payload(
        self,
        *,
        as_of_date: dt.date | None = None,
        limit: int = 100,
        technical_status: str = "ok",
    ) -> dict[str, Any]:
        if not self.database_url:
            return {
                "as_of_date": None,
                "limit": limit,
                "technical_status": technical_status,
                "rows": [],
                "status_counts": {},
                "database_configured": False,
            }
        payload = RatingsRepository(self.database_url).list_top_technical_rating_snapshots(
            as_of_date=as_of_date,
            limit=limit,
            technical_status=technical_status,
        )
        payload["limit"] = max(1, min(int(limit), 500))
        payload["technical_status"] = str(technical_status or "").strip().lower() or "ok"
        payload["database_configured"] = True
        return payload

    def get_chart_insider_payload(
        self,
        ticker: str,
        *,
        lookback_days: int = 14,
        as_of_date: dt.date | None = None,
    ) -> dict[str, Any]:
        normalized_ticker = str(ticker or "").strip().upper()
        resolved_as_of_date = as_of_date or dt.date.today()
        normalized_lookback_days = max(1, int(lookback_days))
        window_start_date = resolved_as_of_date - dt.timedelta(days=normalized_lookback_days)
        resolved_as_of_iso = resolved_as_of_date.isoformat()
        cache_window = self.insider_repository.load_cache_window(
            ticker=normalized_ticker,
            as_of_date=resolved_as_of_iso,
            lookback_days=normalized_lookback_days,
        )
        cache_status = "hit" if cache_window else "miss"
        fetch_status = "skipped"
        notice: str | None = None

        if not self.insider_repository.is_cache_window_fresh(cache_window, ttl_hours=_INSIDER_CACHE_TTL_HOURS):
            cache_status = "stale" if cache_window else "miss"
            try:
                fetched = fetch_insider_trades_window(
                    tickers=[normalized_ticker],
                    as_of_date=resolved_as_of_date,
                    lookback_days=normalized_lookback_days,
                )
                cache_window = {
                    "ticker": normalized_ticker,
                    "requested_tickers": fetched.get("requested_tickers", [normalized_ticker]),
                    "as_of_date": resolved_as_of_iso,
                    "lookback_days": normalized_lookback_days,
                    "refreshed_at": str(fetched.get("generated_at") or ""),
                    "entries": fetched.get("entries", []),
                }
                self.insider_repository.save_cache_window(
                    ticker=normalized_ticker,
                    as_of_date=resolved_as_of_iso,
                    lookback_days=normalized_lookback_days,
                    refreshed_at=cache_window["refreshed_at"],
                    entries=list(cache_window.get("entries", [])) if isinstance(cache_window.get("entries"), list) else [],
                    requested_tickers=list(cache_window.get("requested_tickers", [normalized_ticker])),
                    source=str(fetched.get("source") or "sec_form4_submissions"),
                )
                fetch_status = "fetched"
            except Exception as exc:
                logger.warning("Insider fetch failed for %s: %s", normalized_ticker, exc)
                fetch_status = "failed"
                notice = f"Live insider refresh failed: {exc}"

        entries = _normalize_insider_entries(
            cache_window.get("entries") if isinstance(cache_window, dict) else [],
            ticker=normalized_ticker,
            window_start_date=window_start_date,
            resolved_as_of_date=resolved_as_of_date,
        )

        entries.sort(
            key=lambda item: (
                item.get("transaction_date") or "",
                item.get("filing_date") or "",
                item.get("gross_amount") or 0.0,
            ),
            reverse=True,
        )

        total_buy_amount = round(
            sum(float(item.get("gross_amount") or 0.0) for item in entries if item.get("type") == "BUY"),
            2,
        )
        total_sell_amount = round(
            sum(float(item.get("gross_amount") or 0.0) for item in entries if item.get("type") == "SELL"),
            2,
        )
        return {
            "ticker": normalized_ticker,
            "requested_as_of_date": as_of_date.isoformat() if as_of_date else None,
            "resolved_as_of_date": resolved_as_of_date.isoformat(),
            "lookback_days": normalized_lookback_days,
            "window_start_date": window_start_date.isoformat(),
            "window_end_date": resolved_as_of_date.isoformat(),
            "generated_at": _coerce_iso_datetime(cache_window.get("refreshed_at")) if isinstance(cache_window, dict) else None,
            "cache_status": cache_status,
            "fetch_status": fetch_status,
            "notice": notice,
            "entries": entries,
            "summary": {
                "total_count": len(entries),
                "buy_count": sum(1 for item in entries if item.get("type") == "BUY"),
                "sell_count": sum(1 for item in entries if item.get("type") == "SELL"),
                "total_buy_amount": total_buy_amount,
                "total_sell_amount": total_sell_amount,
                "net_amount": round(total_buy_amount - total_sell_amount, 2),
            },
        }

    def _load_chart_frames(
        self,
        *,
        ticker: str,
        benchmark_ticker: str,
        required_start_date: dt.date,
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
            if _frame_covers_requested_window(ticker_frame, start_date=required_start_date, end_date=end_date):
                if benchmark_frame is None or benchmark_frame.empty:
                    benchmark_frame = _download_history_frame(ticker=benchmark_ticker, start_date=start_date, end_date=end_date)
                    if benchmark_frame is not None and not benchmark_frame.empty:
                        return ticker_frame, benchmark_frame, "database+ticker/internet+benchmark"
                elif not _frame_covers_requested_window(benchmark_frame, start_date=required_start_date, end_date=end_date):
                    benchmark_frame = _download_history_frame(ticker=benchmark_ticker, start_date=start_date, end_date=end_date)
                    if benchmark_frame is not None and not benchmark_frame.empty:
                        return ticker_frame, benchmark_frame, "database+ticker/internet+benchmark"
                return ticker_frame, benchmark_frame, "database"

        ticker_frame = _download_history_frame(ticker=ticker, start_date=start_date, end_date=end_date)
        benchmark_frame = _download_history_frame(ticker=benchmark_ticker, start_date=start_date, end_date=end_date)
        return ticker_frame, benchmark_frame, "internet"

    def _enrich_entries(self, entries: list[dict[str, Any]]) -> list[dict[str, Any]]:
        universe_index = self._get_universe_index()
        overrides = load_ticker_theme_overrides()
        theme_catalog = self._get_theme_catalog()
        latest_market_map = self._load_latest_market_snapshot_map(entries)
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
            latest_market = latest_market_map.get(ticker)
            if latest_market:
                entry["latest_trade_date"] = latest_market["trade_date"]
                entry["current_volume"] = latest_market["volume"]
                entry["daily_change_pct"] = latest_market["change_pct"]
            enriched.append(entry)
        return enriched

    def _load_latest_market_snapshot_map(self, entries: list[dict[str, Any]]) -> dict[str, dict[str, float | int | str | None]]:
        if not self.database_url:
            return {}
        tickers = [
            normalize_ticker_symbol(str(item.get("ticker") or ""))
            for item in entries
            if isinstance(item, dict) and str(item.get("ticker") or "").strip()
        ]
        normalized = [ticker for ticker in tickers if ticker]
        if not normalized:
            return {}
        try:
            frames = load_many_ticker_windows(
                normalized,
                dt.date.today(),
                2,
                database_url=self.database_url,
            )
        except Exception as exc:
            logger.warning("Watchlist latest market enrichment unavailable; continuing without DB day-volume/change data: %s", exc)
            return {}

        snapshots: dict[str, dict[str, float | int | str | None]] = {}
        for ticker, frame in frames.items():
            if frame is None or frame.empty:
                continue
            latest = frame.iloc[-1]
            latest_index = frame.index.max()
            latest_date = latest_index.date().isoformat() if hasattr(latest_index, "date") else str(latest_index)
            latest_close = _coerce_optional_float(latest.get("Close"))
            previous_close = None
            if len(frame.index) >= 2:
                previous_close = _coerce_optional_float(frame.iloc[-2].get("Close"))
            change_pct = None
            if latest_close is not None and previous_close is not None and previous_close > 0:
                change_pct = ((latest_close / previous_close) - 1.0) * 100.0
            snapshots[ticker] = {
                "trade_date": latest_date,
                "volume": int(round(_coerce_float(latest.get("Volume")))),
                "change_pct": change_pct,
            }
        return snapshots

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

    def _get_excluded_tickers(self) -> set[str]:
        if self._excluded_tickers is not None:
            return self._excluded_tickers
        try:
            self._excluded_tickers = load_excluded_tickers(load_app_config())
        except Exception as exc:
            logger.warning("Watchlist exclusion filter unavailable; continuing without exclusions: %s", exc)
            self._excluded_tickers = set()
        return self._excluded_tickers

    def _filter_excluded_entries(self, entries: list[dict[str, Any]]) -> list[dict[str, Any]]:
        excluded = self._get_excluded_tickers()
        if not excluded:
            return [item for item in entries if isinstance(item, dict)]
        filtered: list[dict[str, Any]] = []
        for item in entries:
            if not isinstance(item, dict):
                continue
            ticker = normalize_ticker_symbol(str(item.get("ticker") or ""))
            if not ticker or is_excluded_ticker(ticker, excluded):
                continue
            filtered.append(item)
        return filtered


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
        "market_extension": _empty_market_extension_overlay(),
        "rs_line": [],
        "daily_rs_rating": [],
        "weekly_rs_rating": [],
        "rs_markers": [],
        "setup_markers": [],
        "fearzone_panel": {"rows": [], "signals": []},
        "vcs": None,
        "sepa_dashboard": None,
    }


def _empty_chart_overlay_payload(
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
        "market_extension": _empty_market_extension_overlay(),
        "rs_line": [],
        "daily_rs_rating": [],
        "weekly_rs_rating": [],
        "rs_markers": [],
        "setup_markers": [],
        "fearzone_panel": {"rows": [], "signals": []},
        "vcs": None,
        "sepa_dashboard": None,
    }


def _empty_market_extension_overlay() -> dict[str, Any]:
    return {
        "config": {
            "timeframe": "weekly",
            "ma_type": "sma",
            "length": 10,
            "warning_pct": 11.0,
            "extreme_pct": 15.0,
            "label": "10W SMA",
        },
        "line": [],
        "signals": [],
        "latest": None,
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


def _frame_covers_requested_window(
    frame: pd.DataFrame | None,
    *,
    start_date: dt.date,
    end_date: dt.date,
) -> bool:
    if frame is None or frame.empty:
        return False
    first_index = frame.index.min()
    first_date = first_index.date() if hasattr(first_index, "date") else first_index
    if first_date is None or first_date > start_date:
        return False
    return db_frame_has_recent_coverage(frame, end_date)


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


def _build_chart_payload_cache_key(
    *,
    ticker: str,
    period: str,
    as_of_date: dt.date | None,
    benchmark_ticker: str,
    market_data_source: str,
    include_setup_markers: bool,
) -> tuple[str, str, str, str, str, str]:
    return (
        str(ticker or "").strip().upper(),
        str(period or "18mo").strip().lower() or "18mo",
        as_of_date.isoformat() if as_of_date else "latest",
        str(benchmark_ticker or "SPY").strip().upper() or "SPY",
        str(market_data_source or "").strip().lower() or "internet",
        "setup-markers" if include_setup_markers else "base",
    )


def _read_chart_payload_cache(key: tuple[str, str, str, str, str, str]) -> dict[str, Any] | None:
    now = time.time()
    with _chart_payload_cache_lock:
        cached_entry = _chart_payload_cache.get(key)
        if cached_entry is None:
            return None
        expires_at, payload = cached_entry
        if expires_at <= now:
            _chart_payload_cache.pop(key, None)
            return None
        return copy.deepcopy(payload)


def _write_chart_payload_cache(key: tuple[str, str, str, str, str, str], payload: dict[str, Any]) -> None:
    with _chart_payload_cache_lock:
        _chart_payload_cache[key] = (time.time() + _CHART_PAYLOAD_CACHE_TTL_SECONDS, copy.deepcopy(payload))


def _clear_chart_payload_cache() -> None:
    with _chart_payload_cache_lock:
        _chart_payload_cache.clear()
    with _chart_overlay_cache_lock:
        _chart_overlay_cache.clear()


def _build_chart_overlay_cache_key(
    *,
    ticker: str,
    period: str,
    as_of_date: dt.date | None,
    benchmark_ticker: str,
    market_data_source: str,
    include_setup_markers: bool,
) -> tuple[str, str, str, str, str, str]:
    return (
        str(ticker or "").strip().upper(),
        str(period or "18mo").strip().lower() or "18mo",
        as_of_date.isoformat() if as_of_date else "latest",
        str(benchmark_ticker or "SPY").strip().upper() or "SPY",
        str(market_data_source or "").strip().lower() or "internet",
        "setup-markers" if include_setup_markers else "base",
    )


def _read_chart_overlay_cache(key: tuple[str, str, str, str, str, str]) -> dict[str, Any] | None:
    now = time.time()
    with _chart_overlay_cache_lock:
        cached_entry = _chart_overlay_cache.get(key)
        if cached_entry is None:
            return None
        expires_at, payload = cached_entry
        if expires_at <= now:
            _chart_overlay_cache.pop(key, None)
            return None
        return copy.deepcopy(payload)


def _write_chart_overlay_cache(key: tuple[str, str, str, str, str, str], payload: dict[str, Any]) -> None:
    with _chart_overlay_cache_lock:
        _chart_overlay_cache[key] = (time.time() + _CHART_PAYLOAD_CACHE_TTL_SECONDS, copy.deepcopy(payload))


def _chart_fundamentals_cache_is_complete(entry: dict[str, Any] | None) -> bool:
    if not isinstance(entry, dict):
        return False
    earnings_rows = entry.get("earnings_eps_history")
    implied_move = entry.get("implied_move")
    return bool(
        isinstance(earnings_rows, list)
        and len(earnings_rows) > 0
        and entry.get("holders_float_held_by_institutions_pct") is not None
        and entry.get("revenue_yoy_pct") is not None
        and entry.get("earnings_yoy_pct") is not None
        and isinstance(implied_move, dict)
        and implied_move.get("percent_move") is not None
    )


def _merge_chart_fundamentals_cache_fields(
    cached_entry: dict[str, Any] | None,
    *,
    earnings_rows: list[dict[str, Any]],
    holders_pct: float | None,
    revenue_yoy_pct: float | None,
    earnings_yoy_pct: float | None,
    implied_move: dict[str, Any] | None,
) -> dict[str, Any]:
    cached_entry = cached_entry if isinstance(cached_entry, dict) else {}
    cached_earnings_rows = cached_entry.get("earnings_eps_history")
    cached_implied_move = cached_entry.get("implied_move")
    return {
        "earnings_eps_history": earnings_rows if earnings_rows else (cached_earnings_rows if isinstance(cached_earnings_rows, list) else []),
        "holders_float_held_by_institutions_pct": (
            holders_pct
            if holders_pct is not None
            else cached_entry.get("holders_float_held_by_institutions_pct")
        ),
        "revenue_yoy_pct": revenue_yoy_pct if revenue_yoy_pct is not None else cached_entry.get("revenue_yoy_pct"),
        "earnings_yoy_pct": earnings_yoy_pct if earnings_yoy_pct is not None else cached_entry.get("earnings_yoy_pct"),
        "implied_move": implied_move if implied_move is not None else (cached_implied_move if isinstance(cached_implied_move, dict) else None),
    }


def _chart_cache_diagnostics(entry: dict[str, Any]) -> dict[str, Any]:
    source_summary = entry.get("source_summary") if isinstance(entry.get("source_summary"), dict) else {}
    diagnostics = source_summary.get("diagnostics") if isinstance(source_summary.get("diagnostics"), dict) else {}
    if diagnostics:
        return diagnostics
    cache_attempt = {
        "cache": True,
        "as_of_date": entry.get("as_of_date"),
        "scraped_at": entry.get("scraped_at"),
        "updated_at": entry.get("updated_at"),
    }
    return {
        "earnings": {"status": "cache", "attempts": [cache_attempt]},
        "holders": {"status": "cache", "attempts": [cache_attempt]},
        "statistics": {"status": "cache", "attempts": [cache_attempt]},
        "options": {"status": "cache", "attempts": [cache_attempt]},
    }


def _load_yahoo_earnings_and_holders_playwright(
    ticker: str,
    *,
    earnings_limit: int = 4,
) -> tuple[list[dict[str, Any]], float | None, float | None, float | None, dict[str, dict[str, Any]]]:
    normalized_ticker = str(ticker or "").strip().upper()
    empty_result = (
        [],
        None,
        None,
        None,
        {
            "earnings": {"status": "skipped", "reason": "missing_ticker", "attempts": []},
            "holders": {"status": "skipped", "reason": "missing_ticker", "attempts": []},
            "statistics": {"status": "skipped", "reason": "missing_ticker", "attempts": []},
        },
    )
    if not normalized_ticker:
        return empty_result
    if not _YAHOO_ANALYSIS_HOLDERS_PROBE_SCRIPT.exists():
        diagnostics = {
            "earnings": {
                "status": "error",
                "reason": "missing_probe_script",
                "attempts": [{"script": str(_YAHOO_ANALYSIS_HOLDERS_PROBE_SCRIPT)}],
            },
            "holders": {
                "status": "error",
                "reason": "missing_probe_script",
                "attempts": [{"script": str(_YAHOO_ANALYSIS_HOLDERS_PROBE_SCRIPT)}],
            },
            "statistics": {
                "status": "error",
                "reason": "missing_probe_script",
                "attempts": [{"script": str(_YAHOO_ANALYSIS_HOLDERS_PROBE_SCRIPT)}],
            },
        }
        return [], None, None, None, diagnostics

    command = ["node", str(_YAHOO_ANALYSIS_HOLDERS_PROBE_SCRIPT), normalized_ticker]
    try:
        result = subprocess.run(
            command,
            cwd=str(_REPO_ROOT),
            capture_output=True,
            text=True,
            timeout=_YAHOO_PLAYWRIGHT_TIMEOUT_SECONDS,
            check=False,
        )
    except subprocess.TimeoutExpired as exc:
        diagnostics = {
            "earnings": {"status": "error", "reason": "timeout", "attempts": [{"command": command, "error": str(exc)}]},
            "holders": {"status": "error", "reason": "timeout", "attempts": [{"command": command, "error": str(exc)}]},
            "statistics": {"status": "error", "reason": "timeout", "attempts": [{"command": command, "error": str(exc)}]},
        }
        return [], None, None, None, diagnostics
    except Exception as exc:
        diagnostics = {
            "earnings": {"status": "error", "reason": "launch_failed", "attempts": [{"command": command, "error": str(exc)}]},
            "holders": {"status": "error", "reason": "launch_failed", "attempts": [{"command": command, "error": str(exc)}]},
            "statistics": {"status": "error", "reason": "launch_failed", "attempts": [{"command": command, "error": str(exc)}]},
        }
        return [], None, None, None, diagnostics

    stdout = (result.stdout or "").strip()
    stderr = (result.stderr or "").strip()
    if result.returncode != 0:
        diagnostics = {
            "earnings": {
                "status": "error",
                "reason": "nonzero_exit",
                "attempts": [{"command": command, "returncode": result.returncode, "stderr": stderr[-1000:], "stdout": stdout[-1000:]}],
            },
            "holders": {
                "status": "error",
                "reason": "nonzero_exit",
                "attempts": [{"command": command, "returncode": result.returncode, "stderr": stderr[-1000:], "stdout": stdout[-1000:]}],
            },
            "statistics": {
                "status": "error",
                "reason": "nonzero_exit",
                "attempts": [{"command": command, "returncode": result.returncode, "stderr": stderr[-1000:], "stdout": stdout[-1000:]}],
            },
        }
        return [], None, None, None, diagnostics

    try:
        payload = json.loads(stdout)
    except json.JSONDecodeError as exc:
        diagnostics = {
            "earnings": {
                "status": "error",
                "reason": "invalid_json",
                "attempts": [{"command": command, "error": str(exc), "stdout": stdout[-1000:], "stderr": stderr[-1000:]}],
            },
            "holders": {
                "status": "error",
                "reason": "invalid_json",
                "attempts": [{"command": command, "error": str(exc), "stdout": stdout[-1000:], "stderr": stderr[-1000:]}],
            },
            "statistics": {
                "status": "error",
                "reason": "invalid_json",
                "attempts": [{"command": command, "error": str(exc), "stdout": stdout[-1000:], "stderr": stderr[-1000:]}],
            },
        }
        return [], None, None, None, diagnostics

    earnings_rows_raw = payload.get("earnings_eps_history")
    earnings_rows: list[dict[str, Any]] = []
    if isinstance(earnings_rows_raw, list):
        for row in earnings_rows_raw[: max(1, earnings_limit)]:
            if not isinstance(row, dict):
                continue
            date_value = str(row.get("date") or "").strip()
            if not date_value:
                continue
            earnings_rows.append(
                {
                    "date": date_value,
                    "eps_estimate": _coerce_optional_float(row.get("eps_estimate")),
                    "reported_eps": _coerce_optional_float(row.get("reported_eps")),
                    "surprise_pct": _coerce_optional_float(row.get("surprise_pct")),
                }
            )

    holders_pct = _coerce_optional_float(payload.get("holders_float_held_by_institutions_pct"))
    revenue_yoy_pct = _coerce_optional_float(payload.get("revenue_yoy_pct"))
    earnings_yoy_pct = _coerce_optional_float(payload.get("earnings_yoy_pct"))
    analysis_payload = payload.get("analysis_nz") if isinstance(payload.get("analysis_nz"), dict) else payload.get("analysis")
    holders_payload = payload.get("holders") if isinstance(payload.get("holders"), dict) else {}
    statistics_payload = payload.get("key_statistics_nz") if isinstance(payload.get("key_statistics_nz"), dict) else {}

    earnings_status = "ok" if earnings_rows else "empty"
    holders_status = "ok" if holders_pct is not None else "empty"
    statistics_status = "ok" if revenue_yoy_pct is not None or earnings_yoy_pct is not None else "empty"
    diagnostics = {
        "earnings": {
            "status": earnings_status,
            "attempts": [
                {
                    "command": command,
                    "final_url": analysis_payload.get("final_url") if isinstance(analysis_payload, dict) else "",
                    "status_code": analysis_payload.get("status") if isinstance(analysis_payload, dict) else None,
                    "title": analysis_payload.get("title") if isinstance(analysis_payload, dict) else "",
                    "row_count": len(earnings_rows),
                    "stderr": stderr[-400:] if stderr else "",
                }
            ],
        },
        "holders": {
            "status": holders_status,
            "attempts": [
                {
                    "command": command,
                    "final_url": holders_payload.get("final_url") if isinstance(holders_payload, dict) else "",
                    "status_code": holders_payload.get("status") if isinstance(holders_payload, dict) else None,
                    "title": holders_payload.get("title") if isinstance(holders_payload, dict) else "",
                    "value": holders_pct,
                    "stderr": stderr[-400:] if stderr else "",
                }
            ],
        },
        "statistics": {
            "status": statistics_status,
            "attempts": [
                {
                    "command": command,
                    "final_url": statistics_payload.get("final_url") if isinstance(statistics_payload, dict) else "",
                    "status_code": statistics_payload.get("status") if isinstance(statistics_payload, dict) else None,
                    "title": statistics_payload.get("title") if isinstance(statistics_payload, dict) else "",
                    "revenue_yoy_pct": revenue_yoy_pct,
                    "earnings_yoy_pct": earnings_yoy_pct,
                    "stderr": stderr[-400:] if stderr else "",
                }
            ],
        },
    }
    return earnings_rows, holders_pct, revenue_yoy_pct, earnings_yoy_pct, diagnostics


def _scrape_yahoo_earnings_eps_history(ticker: str, *, limit: int = 12) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    normalized_ticker = str(ticker or "").strip().upper()
    if not normalized_ticker or limit <= 0:
        return [], {"status": "skipped", "reason": "missing_ticker_or_limit", "attempts": []}

    session = requests.Session()
    rows: list[dict[str, Any]] = []
    attempts: list[dict[str, Any]] = []
    page_size = min(limit, 12)
    page_offset = 0
    url = (
        "https://finance.yahoo.com/calendar/earnings"
        f"?symbol={normalized_ticker}&offset={page_offset}&size={page_size}"
    )
    try:
        response = session.get(url, headers=_YAHOO_BROWSER_HEADERS, timeout=_YAHOO_SCRAPE_TIMEOUT)
        blocked_reason = _detect_yahoo_block_reason(response)
        if blocked_reason is not None:
            attempts.append({"url": url, "offset": page_offset, "size": page_size, "status_code": response.status_code, "blocked": blocked_reason})
        else:
            response.raise_for_status()
            tables = pd.read_html(StringIO(response.text))
            if not tables:
                attempts.append({"url": url, "offset": page_offset, "size": page_size, "status_code": response.status_code, "table_count": 0})
            else:
                table = tables[0]
                column_names = [str(column) for column in table.columns]
                required_columns = {"EPS Estimate", "Reported EPS"}
                if "Surprise(%)" in column_names:
                    surprise_column = "Surprise(%)"
                elif "Surprise (%)" in column_names:
                    surprise_column = "Surprise (%)"
                else:
                    surprise_column = None
                has_date_column = "Earnings Date" in column_names
                schema_ok = has_date_column and required_columns.issubset(set(column_names)) and surprise_column is not None
                attempt_payload: dict[str, Any] = {
                    "url": url,
                    "offset": page_offset,
                    "size": page_size,
                    "status_code": response.status_code,
                    "table_count": len(tables),
                    "row_count": int(len(table)),
                    "columns": column_names,
                }
                if not schema_ok:
                    attempt_payload["schema_mismatch"] = True
                    attempt_payload["reason"] = "missing_expected_earnings_columns"
                attempts.append(attempt_payload)
                if schema_ok and not table.empty:
                    for _, raw_row in table.iterrows():
                        earnings_date = _parse_yahoo_earnings_date(raw_row.get("Earnings Date"))
                        if earnings_date is None:
                            continue
                        rows.append(
                            {
                                "date": earnings_date.isoformat(),
                                "eps_estimate": _coerce_optional_float(raw_row.get("EPS Estimate")),
                                "reported_eps": _coerce_optional_float(raw_row.get("Reported EPS")),
                                "surprise_pct": _coerce_optional_float(raw_row.get(surprise_column)),
                            }
                        )
                        if len(rows) >= limit:
                            break
    except Exception as exc:
        logger.warning("Unable to scrape Yahoo earnings EPS history for %s: %s", normalized_ticker, exc)
        attempts.append({"url": url, "offset": page_offset, "size": page_size, "error": str(exc)})

    deduped_rows: list[dict[str, Any]] = []
    seen_dates: set[str] = set()
    for row in rows:
        date_value = row["date"]
        if date_value in seen_dates:
            continue
        seen_dates.add(date_value)
        deduped_rows.append(row)
    if deduped_rows:
        status = "ok"
    elif any("schema_mismatch" in attempt for attempt in attempts):
        status = "schema_mismatch"
    elif any("error" in attempt for attempt in attempts):
        status = "error"
    else:
        status = "empty"
    return deduped_rows, {"status": status, "attempts": attempts}


def _scrape_yahoo_float_held_by_institutions_pct(ticker: str) -> tuple[float | None, dict[str, Any]]:
    normalized_ticker = str(ticker or "").strip().upper()
    if not normalized_ticker:
        return None, {"status": "skipped", "reason": "missing_ticker", "attempts": []}

    urls = [
        f"https://finance.yahoo.com/quote/{normalized_ticker}/holders",
        f"https://uk.finance.yahoo.com/quote/{normalized_ticker}/holders",
        f"https://au.finance.yahoo.com/quote/{normalized_ticker}/holders",
    ]
    patterns = [
        r"([0-9][0-9,]*\.?[0-9]*)%\s*\|\s*%\s+of\s+float\s+held\s+by\s+institutions",
        r"%\s+of\s+float\s+held\s+by\s+institutions[^0-9]{0,40}([0-9][0-9,]*\.?[0-9]*)%",
    ]

    session = requests.Session()
    attempts: list[dict[str, Any]] = []
    for url in urls:
        try:
            response = session.get(url, headers=_YAHOO_BROWSER_HEADERS, timeout=_YAHOO_SCRAPE_TIMEOUT)
            blocked_reason = _detect_yahoo_block_reason(response)
            if blocked_reason is not None:
                attempts.append({"url": url, "status_code": response.status_code, "blocked": blocked_reason})
                break
            response.raise_for_status()
        except Exception as exc:
            logger.warning("Unable to scrape Yahoo holders page for %s at %s: %s", normalized_ticker, url, exc)
            attempts.append({"url": url, "error": str(exc)})
            continue

        normalized_text = _normalize_html_text(response.text)
        for pattern in patterns:
            match = re.search(pattern, normalized_text, flags=re.IGNORECASE)
            if not match:
                continue
            value = _coerce_optional_float(match.group(1))
            attempts.append({"url": url, "status_code": response.status_code, "matched": True, "value": value})
            return value, {"status": "ok" if value is not None else "empty", "attempts": attempts}
        attempts.append({"url": url, "status_code": response.status_code, "matched": False})
    status = "error" if any("error" in attempt for attempt in attempts) else "empty"
    return None, {"status": status, "attempts": attempts}


def _load_yahoo_implied_move_playwright(ticker: str) -> tuple[dict[str, Any] | None, dict[str, Any]]:
    normalized_ticker = str(ticker or "").strip().upper()
    if not normalized_ticker:
        return None, {"status": "skipped", "reason": "missing_ticker", "attempts": []}
    if not _YAHOO_OPTIONS_PROBE_SCRIPT.exists():
        return None, {"status": "error", "reason": "missing_probe_script", "attempts": [{"script": str(_YAHOO_OPTIONS_PROBE_SCRIPT)}]}

    command = ["node", str(_YAHOO_OPTIONS_PROBE_SCRIPT), normalized_ticker]
    try:
        result = subprocess.run(
            command,
            cwd=str(_REPO_ROOT),
            capture_output=True,
            text=True,
            timeout=_YAHOO_PLAYWRIGHT_TIMEOUT_SECONDS,
            check=False,
        )
    except subprocess.TimeoutExpired as exc:
        return None, {"status": "error", "reason": "timeout", "attempts": [{"command": command, "error": str(exc)}]}
    except Exception as exc:
        return None, {"status": "error", "reason": "launch_failed", "attempts": [{"command": command, "error": str(exc)}]}

    stdout = (result.stdout or "").strip()
    stderr = (result.stderr or "").strip()
    if result.returncode != 0:
        return None, {
            "status": "error",
            "reason": "nonzero_exit",
            "attempts": [{"command": command, "returncode": result.returncode, "stderr": stderr[-1000:], "stdout": stdout[-1000:]}],
        }

    try:
        payload = json.loads(stdout)
    except json.JSONDecodeError as exc:
        return None, {
            "status": "error",
            "reason": "invalid_json",
            "attempts": [{"command": command, "error": str(exc), "stdout": stdout[-1000:], "stderr": stderr[-1000:]}],
        }

    implied_move_raw = payload.get("implied_move")
    implied_move: dict[str, Any] | None = None
    if isinstance(implied_move_raw, dict):
        implied_move = {
            "strike": _coerce_optional_float(implied_move_raw.get("strike")),
            "straddle_mid": _coerce_optional_float(implied_move_raw.get("straddle_mid")),
            "dollar_move": _coerce_optional_float(implied_move_raw.get("dollar_move")),
            "percent_move": _coerce_optional_float(implied_move_raw.get("percent_move")),
        }

    options_diagnostics = {
        "status": "ok" if implied_move is not None else "empty",
        "attempts": [
            {
                "command": command,
                "status_code": payload.get("status"),
                "final_url": payload.get("final_url"),
                "price": payload.get("price"),
                "closest_call": payload.get("closest_call"),
                "closest_put": payload.get("closest_put"),
                "stderr": stderr[-400:] if stderr else "",
            }
        ],
    }
    return implied_move, options_diagnostics


def _parse_yahoo_earnings_date(value: object) -> dt.date | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text or text == "-":
        return None
    cleaned = text.replace("AM", "").replace("PM", "")
    for separator in (" EDT", " EST", " UTC", " GMT", " NZDT", " NZST"):
        if separator in cleaned:
            cleaned = cleaned.split(separator, 1)[0]
    primary = cleaned.split(",", 2)
    if len(primary) >= 2:
        candidate = ",".join(primary[:2]).strip()
    else:
        candidate = cleaned
    try:
        return dt.datetime.strptime(candidate, "%b %d, %Y").date()
    except ValueError:
        return None


def _detect_yahoo_block_reason(response: requests.Response) -> str | None:
    if response.status_code == 429:
        return "http_429"
    text = (response.text or "")[:512]
    for marker in _YAHOO_BLOCK_MARKERS:
        if marker.lower() in text.lower():
            return marker
    if len(response.text or "") < 128:
        return "short_block_page"
    return None


def _build_weekly_signal_badges(entry: dict[str, Any]) -> list[str]:
    setup_label = str(entry.get("setup_label") or "").strip().lower()
    summary = str(entry.get("summary") or "").strip().lower()
    master_note = str(entry.get("master_note") or "").strip().lower()
    badges: list[str] = []

    def add_badge(label: str) -> None:
        if label not in badges:
            badges.append(label)

    if "weekly rs new high" in setup_label or "weekly rs new high" in master_note:
        add_badge("Weekly RS")
    if "weekly rs nh before price: true" in summary or "weekly_rs_new_high_before_price" in summary:
        add_badge("Before Price")
    if "daily rs new high" in master_note or "daily rs nh before price: true" in summary:
        add_badge("Daily RS")
    if "strong rs window performance" in master_note:
        add_badge("Strong RS")
    if "sector etf  strong" in master_note or "sector etf strong" in master_note:
        add_badge("Sector Strong")
    signal_tags = entry.get("signal_tags")
    if isinstance(signal_tags, list):
        for tag in signal_tags:
            normalized_tag = str(tag or "").strip()
            if normalized_tag:
                add_badge(normalized_tag)

    distance_match = re.search(r"distance from year high:\s*([0-9.]+)%", summary)
    if distance_match:
        try:
            if float(distance_match.group(1)) <= 3.0:
                add_badge("Near 52W High")
        except ValueError:
            pass

    return badges


def _normalize_scanner_now(now: dt.datetime | None) -> dt.datetime:
    if now is None:
        return dt.datetime.now(dt.timezone.utc)
    if now.tzinfo is None:
        return now.replace(tzinfo=dt.timezone.utc)
    return now


def _previous_weekday(value: dt.date) -> dt.date:
    current = value
    while current.weekday() >= 5:
        current -= dt.timedelta(days=1)
    return current


def _latest_completed_trading_day(now: dt.datetime) -> dt.date:
    local_now = now.astimezone(_NEW_YORK_TZ)
    local_date = local_now.date()
    weekday = local_date.weekday()
    if weekday >= 5:
        return _previous_weekday(local_date)
    if local_now.time() >= dt.time(hour=_SCANNER_BOARD_CUTOFF_HOUR, minute=_SCANNER_BOARD_CUTOFF_MINUTE):
        return local_date
    return _previous_weekday(local_date - dt.timedelta(days=1))


def _select_scanner_board_watchlist(
    watchlists: list[dict[str, Any]],
    *,
    strategy_id: str,
    target_date: dt.date,
) -> dict[str, Any] | None:
    eligible: list[dict[str, Any]] = []
    fallback: list[dict[str, Any]] = []
    for item in watchlists:
        if _strategy_id_for_watchlist_meta(item) != strategy_id:
            continue
        fallback.append(item)
        sort_date_raw = str(item.get("sort_date") or "").strip()
        if not sort_date_raw:
            continue
        try:
            sort_date = dt.date.fromisoformat(sort_date_raw)
        except ValueError:
            continue
        if sort_date <= target_date:
            eligible.append(item)

    candidates = eligible if eligible else fallback
    if not candidates:
        return None
    candidates.sort(
        key=lambda item: (
            str(item.get("sort_date") or ""),
            str(item.get("captured_at") or ""),
        ),
        reverse=True,
    )
    return candidates[0]


def _strategy_id_for_watchlist_meta(item: dict[str, Any]) -> str:
    stem = str(item.get("stem") or "").strip()
    if not stem:
        return ""
    if stem.startswith("weekly_rs_new_high_"):
        return "weekly_rs"
    if stem.startswith("fearzone_zeiierman_"):
        return "fearzone_zeiierman"
    return _stem_strategy_id(stem)


def _stem_strategy_id(stem: str) -> str:
    from ...artifact_paths import strategy_id_from_legacy_stem

    return strategy_id_from_legacy_stem(stem)


def _normalize_html_text(value: str) -> str:
    stripped = re.sub(r"<[^>]+>", " ", value or "")
    stripped = html.unescape(stripped)
    return re.sub(r"\s+", " ", stripped).strip()


def _coerce_optional_float(value: object) -> float | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text or text == "-" or text.lower() == "nan":
        return None
    if text.endswith("%"):
        text = text[:-1].strip()
    try:
        return float(text.replace(",", ""))
    except ValueError:
        return None


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


def _compute_rs_rating_series(stock: pd.Series, benchmark: pd.Series) -> pd.Series:
    score_series = compute_weighted_rs_score(stock, benchmark)
    if score_series.empty:
        return pd.Series(dtype=float)
    rating_series = score_series.apply(approximate_rs_rating).dropna()
    return rating_series.astype(float)


def _compute_rs_new_high_flags(rs_line: pd.Series, price_reference: pd.Series, lookback: int) -> tuple[pd.Series, pd.Series]:
    aligned = pd.concat([rs_line, price_reference], axis=1, join="inner").dropna()
    aligned.columns = ["rs_line", "price_reference"]
    rolling_rs_high = aligned["rs_line"].rolling(window=lookback, min_periods=1).max()
    rolling_price_high = aligned["price_reference"].rolling(window=lookback, min_periods=1).max()
    tolerance = 1e-12
    new_high = aligned["rs_line"] >= (rolling_rs_high - tolerance)
    new_high_before_price = new_high & (aligned["price_reference"] < (rolling_price_high - tolerance))
    return new_high.reindex(rs_line.index, fill_value=False), new_high_before_price.reindex(rs_line.index, fill_value=False)


def _compute_ftd_sweep_markers(
    *,
    frame: pd.DataFrame,
    visible_dates: set[str],
    ticker: str,
    benchmark_ticker: str,
) -> list[dict[str, Any]]:
    normalized = _normalize_download_frame(frame)
    if normalized is None or normalized.empty or not visible_dates:
        return []
    config = load_app_config()
    ticker_meta = UniverseTicker(symbol=ticker.upper(), sector=None, industry=None, exchange=None)
    markers: list[dict[str, Any]] = []
    seen_dates: set[str] = set()
    minimum_bars = max(int(config.ftd_sweep_pivot_lookback_left) + 15, 40)
    for end_index in range(minimum_bars - 1, len(normalized)):
        prefix = normalized.iloc[: end_index + 1]
        current_date = prefix.index[-1].date().isoformat()
        if current_date not in visible_dates:
            continue
        hit = find_recent_ftd_sweep_hit(
            prefix,
            ticker=ticker_meta,
            benchmark_ticker=benchmark_ticker,
            config=config,
        )
        if hit is None or hit.sweep_breakout_date != current_date or current_date in seen_dates:
            continue
        seen_dates.add(current_date)
        markers.append(
            {
                "time": current_date,
                "kind": "ftd_sweep_breakout",
                "label": "FTD Sweep",
            }
        )
    return markers


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


def _compute_market_extension_overlay(frame: pd.DataFrame, *, visible_dates: set[str]) -> dict[str, Any]:
    config = {
        "timeframe": "weekly",
        "ma_type": "sma",
        "length": 10,
        "warning_pct": 11.0,
        "extreme_pct": 15.0,
        "label": "10W SMA",
    }
    if frame.empty:
        return {"config": config, "line": [], "signals": [], "latest": None}

    weekly_frame = resample_to_weekly(frame[["Open", "High", "Low", "Close", "Volume"]])
    enriched = compute_extension_frame(
        weekly_frame,
        length=int(config["length"]),
        ma_type=str(config["ma_type"]),
        warning_pct=float(config["warning_pct"]),
        extreme_pct=float(config["extreme_pct"]),
    )
    if enriched.empty:
        return {"config": config, "line": [], "signals": [], "latest": None}

    market_extension_ma = enriched["moving_average"].reindex(frame.index, method="ffill")
    line = [
        {
            "time": pd.Timestamp(index).date().isoformat(),
            "value": float(value),
        }
        for index, value in market_extension_ma.items()
        if pd.notna(value) and pd.Timestamp(index).date().isoformat() in visible_dates
    ]

    extension_series = enriched["extension_pct"]
    signals: list[dict[str, Any]] = []
    for idx in range(1, len(enriched) - 1):
        current = extension_series.iloc[idx]
        if pd.isna(current):
            continue
        if current < extension_series.iloc[idx - 1] or current < extension_series.iloc[idx + 1]:
            continue
        state = str(enriched["threshold_state"].iloc[idx])
        if state == "normal":
            continue
        row = enriched.iloc[idx]
        moving_average = row.get("moving_average")
        signal_time = enriched.index[idx].date().isoformat()
        if signal_time not in visible_dates or pd.isna(moving_average):
            continue
        signals.append(
            {
                "time": signal_time,
                "state": state,
                "close": round(float(row["Close"]), 2),
                "moving_average": round(float(moving_average), 2),
                "distance": round(float(row["Close"] - moving_average), 2),
                "extension_pct": round(float(current), 2),
            }
        )

    latest_valid = enriched.dropna(subset=["moving_average", "extension_pct"]).tail(1)
    latest: dict[str, Any] | None = None
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
        "config": config,
        "line": line,
        "signals": signals,
        "latest": latest,
    }


def _coalesce_text(*values: object) -> str | None:
    for value in values:
        text = str(value or "").strip()
        if text:
            return text
    return None


def _normalize_insider_entries(
    raw_entries: object,
    *,
    ticker: str,
    window_start_date: dt.date,
    resolved_as_of_date: dt.date,
) -> list[dict[str, Any]]:
    entries: list[dict[str, Any]] = []
    if not isinstance(raw_entries, list):
        return entries
    for raw_entry in raw_entries:
        if not isinstance(raw_entry, dict):
            continue
        entry_ticker = str(raw_entry.get("ticker") or "").strip().upper()
        if entry_ticker != ticker:
            continue
        event_date = _coerce_insider_event_date(raw_entry)
        if event_date is None:
            continue
        if event_date < window_start_date or event_date > resolved_as_of_date:
            continue
        entries.append(
            {
                "ticker": entry_ticker,
                "filing_date": _coerce_iso_date(raw_entry.get("filing_date")),
                "transaction_date": _coerce_iso_date(raw_entry.get("transaction_date")),
                "owner_name": str(raw_entry.get("owner_name") or "").strip(),
                "position": str(raw_entry.get("position") or "").strip(),
                "type": str(raw_entry.get("type") or "").strip().upper(),
                "shares": int(round(_coerce_float(raw_entry.get("shares")))),
                "price": _coerce_optional_float(raw_entry.get("price")),
                "gross_amount": _coerce_optional_float(raw_entry.get("gross_amount")),
                "net_amount": _coerce_optional_float(raw_entry.get("net_amount")),
                "shares_owned_after": int(round(_coerce_float(raw_entry.get("shares_owned_after")))),
                "is_10b5_1": bool(raw_entry.get("is_10b5_1")),
                "source_url": str(raw_entry.get("source_url") or "").strip(),
            }
        )
    return entries


def _coerce_insider_event_date(payload: dict[str, Any]) -> dt.date | None:
    transaction_date = _coerce_iso_date(payload.get("transaction_date"))
    if transaction_date:
        return dt.date.fromisoformat(transaction_date)
    filing_date = _coerce_iso_date(payload.get("filing_date"))
    if filing_date:
        return dt.date.fromisoformat(filing_date)
    return None


def _coerce_iso_date(value: object) -> str | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return dt.date.fromisoformat(text[:10]).isoformat()
    except ValueError:
        return None


def _coerce_iso_datetime(value: object) -> str | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return dt.datetime.fromisoformat(text.replace("Z", "+00:00")).isoformat()
    except ValueError:
        return None


def _coerce_float(value: object) -> float:
    if value is None or value == "":
        return 0.0
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _coerce_optional_float(value: object) -> float | None:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
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
