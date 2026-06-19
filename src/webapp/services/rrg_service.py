from __future__ import annotations

from dataclasses import asdict, dataclass
import datetime as dt
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Literal

import pandas as pd

from src.config import AppConfig, load_app_config
from src.fearzone_screen import find_recent_fearzone_hit
from src.market_data_access import db_frame_has_recent_coverage, load_daily_bars_frame_from_db, resolve_database_url
from src.universe import UniverseTicker
from scripts.render_sector_rotation_rrg import build_theme_universe, chunked
from vendor.trade_master_signals.render_sector_rotation_rrg import (
    DEFAULT_INDUSTRY_ETFS,
    DEFAULT_SECTOR_ETFS,
    compute_rotation_series,
    fetch_history,
    to_weekly_close,
)


UniverseName = Literal["sector", "industry", "theme"]
CadenceName = Literal["weekly", "daily-2m"]
THEME_BATCH_SIZE = 12
DEFAULT_PERIOD = "3y"
DEFAULT_TRAIL_WEEKS = 12
DAILY_PERIOD = "2mo"
DAILY_TRAIL_POINTS = 40
DEFAULT_RATIO_WINDOW = 10
DEFAULT_MOMENTUM_WINDOW = 4
THEME_GROUP_TOPICS: tuple[tuple[str, tuple[str, ...]], ...] = (
    ("Resources", ("copper", "gold", "silver", "lithium", "uranium", "rare earth", "metals", "mining", "steel", "natural resources")),
    ("Energy", ("electrification", "energy", "oil", "gas", "battery", "low carbon")),
    ("Health Care", ("health", "heart", "oncology", "glp-1", "pharma", "pharmaceutical", "biotech", "medical devices", "cannabis")),
    ("AI & Automation", ("ai", "robot", "robotics", "robotaxi", "humanoid", "autonomous")),
    ("Semis & Software", ("technology", "semiconductor", "software", "memory", "telecom")),
    ("Digital Assets", ("bitcoin", "blockchain", "digital", "metaverse", "meme", "social sentiment")),
    ("Space & Defense", ("space", "aerospace", "defense")),
    ("Financials", ("asset manager", "asset managers", "bank", "capital markets", "insurance")),
    ("Consumer Themes", ("retail", "sports betting", "gaming", "video games", "esports", "magnificent seven")),
    ("Macro Themes", ("reshoring", "africa", "agribusiness", "transportation")),
)


@dataclass(frozen=True)
class RrgPoint:
    x: float
    y: float
    date: str


@dataclass(frozen=True)
class RrgSeries:
    ticker: str
    label: str
    points: list[RrgPoint]
    latest: RrgPoint
    quadrant: str
    distance: float
    fearzone: dict[str, Any]


@dataclass(frozen=True)
class RrgGroup:
    id: str
    title: str
    series: list[RrgSeries]


class RrgService:
    def __init__(self, output_dir: Path, reports_fqdn: str = "", app_config: AppConfig | None = None, database_url: str = "") -> None:
        self.output_dir = output_dir
        self.reports_fqdn = reports_fqdn.strip()
        self.app_config = app_config or load_app_config()
        self.database_url = resolve_database_url(database_url)

    def get_latest_report(self) -> dict[str, Any]:
        report_dir = self._resolve_latest_report_dir()
        if report_dir is None:
            return {
                "available": False,
                "date_label": "",
                "report_root": "",
                "report_index_url": "",
                "sections": [],
            }

        relative_root = report_dir.relative_to(self.output_dir).as_posix()
        return {
            "available": True,
            "date_label": relative_root.removeprefix("sector_rotation_rrg_"),
            "report_root": relative_root,
            "report_index_url": self._reports_url(f"{relative_root}/index.html"),
            "sections": [
                {
                    "id": "sector",
                    "title": "Sector Rotation",
                    "description": "Official 11 sector ETFs rendered as the main daily-refreshed weekly RRG map.",
                    "index_url": self._reports_url(f"{relative_root}/sector/index.html"),
                    "image_url": self._reports_url(f"{relative_root}/sector/sector_rrg.svg"),
                },
                {
                    "id": "industry",
                    "title": "Industry Rotation",
                    "description": "Focused industry ETF basket for tactical leadership and rotation checks.",
                    "index_url": self._reports_url(f"{relative_root}/industry/index.html"),
                    "image_url": self._reports_url(f"{relative_root}/industry/sector_rrg.svg"),
                },
                {
                    "id": "theme",
                    "title": "Theme Rotation",
                    "description": "Theme ETF batches split into smaller RRG maps for readability.",
                    "index_url": self._reports_url(f"{relative_root}/theme/index.html"),
                    "image_url": self._reports_url(f"{relative_root}/theme/theme_batch_01/sector_rrg.svg"),
                },
            ],
        }

    def get_universe_report(
        self,
        universe: UniverseName,
        *,
        benchmark: str,
        period: str,
        trail_weeks: int,
        cadence: CadenceName = "weekly",
    ) -> dict[str, Any]:
        benchmark_symbol = benchmark.upper()
        generated_at = datetime.now(UTC).isoformat()
        static_report_url = self._static_report_url(universe)
        notes: list[str] = []
        effective_period = DAILY_PERIOD if cadence == "daily-2m" else period
        effective_trail = DAILY_TRAIL_POINTS if cadence == "daily-2m" else trail_weeks
        try:
            if universe == "theme":
                groups, failures = self._build_theme_groups(
                    benchmark=benchmark_symbol,
                    period=effective_period,
                    trail_weeks=effective_trail,
                    cadence=cadence,
                )
                if failures:
                    notes.append(f"Skipped {len(failures)} tickers with missing or unusable history.")
                if groups:
                    notes.append(f"Theme universe grouped into batches of {THEME_BATCH_SIZE}.")
                if cadence == "daily-2m":
                    notes.append("Daily mode uses raw daily closes from the most recent two months.")
                flat_series = [series for group in groups for series in group.series]
                return {
                    "universe": universe,
                    "benchmark": benchmark_symbol,
                    "period": effective_period,
                    "trail_weeks": effective_trail,
                    "cadence": cadence,
                    "generated_at": generated_at,
                    "series": [self._series_payload(series) for series in flat_series],
                    "groups": [self._group_payload(group) for group in groups],
                    "quadrants": self._quadrants_payload(),
                    "meta": {
                        "count": len(flat_series),
                        "notes": notes,
                        "failed_tickers": failures,
                    },
                    "static_report_url": static_report_url,
                }

            series_list, failures = self._build_series_for_universe(
                self._universe_entries(universe),
                benchmark=benchmark_symbol,
                period=effective_period,
                trail_weeks=effective_trail,
                cadence=cadence,
            )
            if failures:
                notes.append(f"Skipped {len(failures)} tickers with missing or unusable history.")
            if not series_list:
                notes.append("No RRG series could be computed for the requested universe.")
            if cadence == "daily-2m":
                notes.append("Daily mode uses raw daily closes from the most recent two months.")
            return {
                "universe": universe,
                "benchmark": benchmark_symbol,
                "period": effective_period,
                "trail_weeks": effective_trail,
                "cadence": cadence,
                "generated_at": generated_at,
                "series": [self._series_payload(series) for series in series_list],
                "quadrants": self._quadrants_payload(),
                "meta": {
                    "count": len(series_list),
                    "notes": notes,
                    "failed_tickers": failures,
                },
                "static_report_url": static_report_url,
            }
        except Exception as exc:
            notes.append(f"Interactive RRG refresh failed: {exc}")
            if cadence == "daily-2m":
                notes.append("Daily mode uses raw daily closes from the most recent two months.")
            return {
                "universe": universe,
                "benchmark": benchmark_symbol,
                "period": effective_period,
                "trail_weeks": effective_trail,
                "cadence": cadence,
                "generated_at": generated_at,
                "series": [],
                "groups": [],
                "quadrants": self._quadrants_payload(),
                "meta": {
                    "count": 0,
                    "notes": notes,
                    "failed_tickers": [],
                },
                "static_report_url": static_report_url,
            }

    def _build_theme_groups(
        self,
        *,
        benchmark: str,
        period: str,
        trail_weeks: int,
        cadence: CadenceName,
    ) -> tuple[list[RrgGroup], list[str]]:
        theme_universe = build_theme_universe()
        all_series, failures = self._build_series_for_universe(
            theme_universe,
            benchmark=benchmark,
            period=period,
            trail_weeks=trail_weeks,
            cadence=cadence,
        )
        groups: list[RrgGroup] = []
        for index, batch in enumerate(chunked(all_series, THEME_BATCH_SIZE), start=1):
            groups.append(
                RrgGroup(
                    id=f"theme-batch-{index:02d}",
                    title=self._theme_group_title(batch),
                    series=batch,
                )
            )
        return groups, failures

    def _build_series_for_universe(
        self,
        entries: list[tuple[str, str]],
        *,
        benchmark: str,
        period: str,
        trail_weeks: int,
        cadence: CadenceName,
    ) -> tuple[list[RrgSeries], list[str]]:
        benchmark_history = self._close_series(self._history_frame(benchmark, period), cadence).rename(benchmark)
        failures: list[str] = []

        series_list: list[RrgSeries] = []
        for index, (label, ticker) in enumerate(entries):
            symbol = ticker.upper()
            try:
                ticker_frame = self._history_frame(symbol, period)
                ticker_history = self._close_series(ticker_frame, cadence).rename(symbol)
                closes = pd.concat([benchmark_history, ticker_history], axis=1, join="inner").dropna()
                if closes.empty:
                    failures.append(symbol)
                    continue
                series = None
                max_trail = max(1, min(trail_weeks, len(closes)))
                for effective_trail in range(max_trail, 0, -1):
                    series = compute_rotation_series(
                        label=label,
                        ticker=symbol,
                        color=f"series-{index}",
                        closes=closes,
                        benchmark=benchmark,
                        ratio_window=DEFAULT_RATIO_WINDOW,
                        momentum_window=DEFAULT_MOMENTUM_WINDOW,
                        trail_weeks=effective_trail,
                    )
                    if series is not None:
                        break
                if series is None:
                    failures.append(symbol)
                    continue
                points = [
                    RrgPoint(
                        x=round(float(row["x"]), 2),
                        y=round(float(row["y"]), 2),
                        date=self._normalize_date(point_date),
                    )
                    for point_date, row in series.trail.iterrows()
                ]
                if not points:
                    failures.append(symbol)
                    continue
                series_list.append(
                    RrgSeries(
                        ticker=symbol,
                        label=label,
                        points=points,
                        latest=points[-1],
                        quadrant=series.quadrant,
                        distance=round(float(series.distance), 3),
                        fearzone=self._fearzone_payload(symbol, ticker_frame),
                    )
                )
            except Exception:
                failures.append(symbol)
                continue
        return series_list, sorted(set(failures))

    def _history_frame(self, ticker: str, period: str) -> pd.DataFrame:
        db_history = self._history_frame_from_db(ticker, period)
        if db_history is not None and not db_history.empty:
            return db_history
        history = fetch_history(ticker, period)
        if history is None or history.empty:
            raise ValueError(f"No history for {ticker}")
        return history

    def _history_frame_from_db(self, ticker: str, period: str) -> pd.DataFrame | None:
        if not self.database_url:
            return None
        end_date = dt.date.today()
        start_date = _period_start_date(end_date, period)
        history = load_daily_bars_frame_from_db(
            ticker,
            start_date,
            end_date,
            database_url=self.database_url,
        )
        if history is None or history.empty:
            return None
        if not db_frame_has_recent_coverage(history, end_date, tolerance_days=7):
            return None
        return history

    def _close_series(self, history: pd.DataFrame, cadence: CadenceName) -> pd.Series:
        if cadence == "daily-2m":
            return history["Close"].dropna()
        return to_weekly_close(history)

    def _fearzone_payload(self, ticker: str, history: pd.DataFrame) -> dict[str, Any]:
        fearzone_history = self._normalize_ohlc_frame(history)
        if len(fearzone_history) < self._fearzone_min_history_bars():
            fearzone_history = self._normalize_ohlc_frame(self._history_frame(ticker, "3y"))

        hit = find_recent_fearzone_hit(
            fearzone_history,
            ticker=UniverseTicker(symbol=ticker),
            benchmark_ticker=self.app_config.benchmark_ticker,
            config=self.app_config,
        )
        source = fearzone_history[["Open", "High", "Low", "Close"]].mean(axis=1)
        high_period = int(self.app_config.fearzone_high_period)
        band_period = int(self.app_config.fearzone_band_period)

        highest_source = source.rolling(high_period).max()
        fz1_value = (highest_source - source) / highest_source.replace(0, pd.NA)
        fz1_basis = fz1_value.rolling(band_period).mean()
        fz1_std = fz1_value.rolling(band_period).std(ddof=0)
        fz1_upper = fz1_basis + (fz1_std * float(self.app_config.fearzone_band_std_multiplier))
        in_fz1 = (fz1_value > fz1_upper).fillna(False)

        source_ma = source.rolling(high_period).mean()
        fz2_value = source - source_ma
        fz2_basis = fz2_value.rolling(band_period).mean()
        fz2_std = fz2_value.rolling(band_period).std(ddof=0)
        fz2_lower = fz2_basis - (fz2_std * float(self.app_config.fearzone_band_std_multiplier))
        in_fz2 = (fz2_value < fz2_lower).fillna(False)

        impulse_pct = (
            (fearzone_history["Close"] / fearzone_history["Close"].shift(int(self.app_config.fearzone_negative_impulse_lookback_days))) - 1.0
        ) * 100.0
        negative_impulse = (impulse_pct <= (-abs(float(self.app_config.fearzone_negative_impulse_pct)))).fillna(False)

        bar_range = fearzone_history["High"] - fearzone_history["Low"]
        range_floor = fearzone_history["Low"] + (bar_range * float(self.app_config.fearzone_ricochet_zone_pct))
        in_ricochet_zone = (fearzone_history["Close"] <= range_floor).fillna(False)

        lowest_low = fearzone_history["Low"].rolling(int(self.app_config.fearzone_stochastic_k)).min()
        highest_high = fearzone_history["High"].rolling(int(self.app_config.fearzone_stochastic_k)).max()
        stoch_range = highest_high - lowest_low
        raw_k = ((fearzone_history["Close"] - lowest_low) * 100.0 / stoch_range.where(stoch_range != 0)).astype(float)
        fast_k = raw_k.rolling(int(self.app_config.fearzone_stochastic_d)).mean()
        slow_k = fast_k.rolling(int(self.app_config.fearzone_stochastic_d)).mean()
        magic_k1 = (slow_k < float(self.app_config.fearzone_magic_k1_threshold)).fillna(False)

        ma200 = fearzone_history["Close"].rolling(int(self.app_config.fearzone_ma_long_period)).mean()
        above_ma200 = (fearzone_history["Close"] > ma200).fillna(False)

        condition_defs = [
            ("fz1", "FZ1", in_fz1),
            ("fz2", "FZ2", in_fz2),
            ("negative_impulse", "Down 10%", negative_impulse),
            ("ricochet_zone", "Ricochet", in_ricochet_zone),
            ("magic_k1", "Magic-K1", magic_k1),
            ("above_ma200", "Above MA200", above_ma200),
        ]
        latest_conditions = [
            {"key": key, "label": label, "active": bool(series.iloc[-1])}
            for key, label, series in condition_defs
            if not series.empty
        ]

        trigger_labels: list[str] = []
        if hit is not None:
            if hit.trigger_negative_impulse:
                trigger_labels.append("Down 10%")
            if hit.trigger_ricochet_zone:
                trigger_labels.append("Ricochet")
            if hit.trigger_magic_k1:
                trigger_labels.append("Magic-K1")

        return {
            "active": hit is not None,
            "signal_date": hit.signal_date if hit is not None else None,
            "signal_age_bars": hit.signal_age_bars if hit is not None else None,
            "trigger_labels": trigger_labels,
            "conditions": latest_conditions,
        }

    def _normalize_ohlc_frame(self, history: pd.DataFrame) -> pd.DataFrame:
        normalized = history.copy()
        close_column = "Close" if "Close" in normalized.columns else None
        if close_column is None and "Adj Close" in normalized.columns:
            normalized["Close"] = normalized["Adj Close"]
            close_column = "Close"
        if close_column is None:
            raise ValueError("Fearzone payload requires Close history.")
        for column in ("Open", "High", "Low"):
            if column not in normalized.columns:
                normalized[column] = normalized[close_column]
        return normalized

    def _fearzone_min_history_bars(self) -> int:
        return max(
            int(self.app_config.fearzone_high_period),
            int(self.app_config.fearzone_band_period),
            int(self.app_config.fearzone_ma_long_period),
            int(self.app_config.fearzone_negative_impulse_lookback_days) + 5,
        )

    def _universe_entries(self, universe: UniverseName) -> list[tuple[str, str]]:
        if universe == "sector":
            return list(DEFAULT_SECTOR_ETFS)
        if universe == "industry":
            return list(DEFAULT_INDUSTRY_ETFS)
        if universe == "theme":
            return build_theme_universe()
        raise ValueError(f"Unsupported universe: {universe}")

    def _normalize_date(self, value: Any) -> str:
        if hasattr(value, "strftime"):
            return value.strftime("%Y-%m-%d")
        return str(value)

    def _series_payload(self, series: RrgSeries) -> dict[str, Any]:
        payload = asdict(series)
        payload["distance"] = round(series.distance, 3)
        return payload

    def _group_payload(self, group: RrgGroup) -> dict[str, Any]:
        return {
            "id": group.id,
            "title": group.title,
            "series": [self._series_payload(series) for series in group.series],
        }

    def _theme_group_title(self, batch: list[RrgSeries]) -> str:
        topic_order = {label: index for index, (label, _) in enumerate(THEME_GROUP_TOPICS)}
        scores: dict[str, int] = {label: 0 for label, _ in THEME_GROUP_TOPICS}
        for series in batch:
            haystack = f"{series.label} {series.ticker}".lower()
            for topic, keywords in THEME_GROUP_TOPICS:
                if any(keyword in haystack for keyword in keywords):
                    scores[topic] += 1
        ranked = [(topic, count) for topic, count in scores.items() if count > 0]
        ranked.sort(key=lambda item: (-item[1], topic_order[item[0]]))
        if len(ranked) >= 2:
            return f"{ranked[0][0]} / {ranked[1][0]}"
        if ranked:
            return f"{ranked[0][0]} Focus"
        lead = batch[0].label.strip() if batch else "Theme"
        return f"{lead} Focus"

    def _quadrants_payload(self) -> dict[str, Any]:
        return {
            "center_x": 100.0,
            "center_y": 100.0,
            "definitions": [
                {"name": "Leading", "x": "gte_100", "y": "gte_100"},
                {"name": "Weakening", "x": "gte_100", "y": "lt_100"},
                {"name": "Lagging", "x": "lt_100", "y": "lt_100"},
                {"name": "Improving", "x": "lt_100", "y": "gte_100"},
            ],
        }

    def _resolve_latest_report_dir(self) -> Path | None:
        if not self.output_dir.exists():
            return None
        candidates = sorted(
            (path for path in self.output_dir.glob("sector_rotation_rrg_*") if path.is_dir()),
            reverse=True,
        )
        return candidates[0] if candidates else None

    def _static_report_url(self, universe: UniverseName) -> str:
        report_dir = self._resolve_latest_report_dir()
        if report_dir is None:
            return ""
        relative_root = report_dir.relative_to(self.output_dir).as_posix()
        return self._reports_url(f"{relative_root}/{universe}/index.html")

    def _reports_url(self, relative_path: str) -> str:
        normalized = relative_path.lstrip("/")
        if self.reports_fqdn:
            return f"https://{self.reports_fqdn}/{normalized}"
        return f"/{normalized}"


def _period_start_date(end_date: dt.date, period: str) -> dt.date:
    normalized = str(period or "").strip().lower()
    if not normalized:
        return end_date - dt.timedelta(days=365 * 3)
    digits = "".join(character for character in normalized if character.isdigit())
    unit = normalized[len(digits) :] if digits else normalized
    value = int(digits) if digits else 1
    if unit == "d":
        return end_date - dt.timedelta(days=value)
    if unit == "w":
        return end_date - dt.timedelta(days=value * 7)
    if unit == "mo":
        return end_date - dt.timedelta(days=value * 31)
    if unit == "y":
        return end_date - dt.timedelta(days=value * 366)
    return end_date - dt.timedelta(days=365 * 3)
