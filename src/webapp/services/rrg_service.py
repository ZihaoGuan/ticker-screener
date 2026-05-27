from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Literal

import pandas as pd

from scripts.render_sector_rotation_rrg import build_theme_universe, chunked
from vendor.trade_master_signals.render_sector_rotation_rrg import (
    DEFAULT_INDUSTRY_ETFS,
    DEFAULT_SECTOR_ETFS,
    compute_rotation_series,
    fetch_history,
    to_weekly_close,
)


UniverseName = Literal["sector", "industry", "theme"]
THEME_BATCH_SIZE = 12
DEFAULT_PERIOD = "3y"
DEFAULT_TRAIL_WEEKS = 12
DEFAULT_RATIO_WINDOW = 10
DEFAULT_MOMENTUM_WINDOW = 4


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


@dataclass(frozen=True)
class RrgGroup:
    id: str
    title: str
    series: list[RrgSeries]


class RrgService:
    def __init__(self, output_dir: Path, reports_fqdn: str = "") -> None:
        self.output_dir = output_dir
        self.reports_fqdn = reports_fqdn.strip()

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
    ) -> dict[str, Any]:
        benchmark_symbol = benchmark.upper()
        generated_at = datetime.now(UTC).isoformat()
        static_report_url = self._static_report_url(universe)
        notes: list[str] = []

        if universe == "theme":
            groups, failures = self._build_theme_groups(
                benchmark=benchmark_symbol,
                period=period,
                trail_weeks=trail_weeks,
            )
            if failures:
                notes.append(f"Skipped {len(failures)} tickers with missing or unusable history.")
            if groups:
                notes.append(f"Theme universe grouped into batches of {THEME_BATCH_SIZE}.")
            flat_series = [series for group in groups for series in group.series]
            return {
                "universe": universe,
                "benchmark": benchmark_symbol,
                "period": period,
                "trail_weeks": trail_weeks,
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
            period=period,
            trail_weeks=trail_weeks,
        )
        if failures:
            notes.append(f"Skipped {len(failures)} tickers with missing or unusable history.")
        if not series_list:
            notes.append("No RRG series could be computed for the requested universe.")
        return {
            "universe": universe,
            "benchmark": benchmark_symbol,
            "period": period,
            "trail_weeks": trail_weeks,
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

    def _build_theme_groups(
        self,
        *,
        benchmark: str,
        period: str,
        trail_weeks: int,
    ) -> tuple[list[RrgGroup], list[str]]:
        theme_universe = build_theme_universe()
        all_series, failures = self._build_series_for_universe(
            theme_universe,
            benchmark=benchmark,
            period=period,
            trail_weeks=trail_weeks,
        )
        groups: list[RrgGroup] = []
        for index, batch in enumerate(chunked(all_series, THEME_BATCH_SIZE), start=1):
            groups.append(
                RrgGroup(
                    id=f"theme-batch-{index:02d}",
                    title=f"Theme Batch {index:02d}",
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
    ) -> tuple[list[RrgSeries], list[str]]:
        benchmark_history = to_weekly_close(fetch_history(benchmark, period)).rename(benchmark)
        weekly_closes: dict[str, pd.Series] = {benchmark: benchmark_history}
        failures: list[str] = []

        for label, ticker in entries:
            symbol = ticker.upper()
            try:
                weekly_closes[symbol] = to_weekly_close(fetch_history(symbol, period)).rename(symbol)
            except Exception:
                failures.append(symbol)

        closes = pd.concat(weekly_closes.values(), axis=1, join="inner").dropna()
        if closes.empty:
            return [], sorted(set(failures))

        series_list: list[RrgSeries] = []
        for index, (label, ticker) in enumerate(entries):
            symbol = ticker.upper()
            if symbol not in closes.columns:
                failures.append(symbol)
                continue
            series = compute_rotation_series(
                label=label,
                ticker=symbol,
                color=f"series-{index}",
                closes=closes,
                benchmark=benchmark,
                ratio_window=DEFAULT_RATIO_WINDOW,
                momentum_window=DEFAULT_MOMENTUM_WINDOW,
                trail_weeks=trail_weeks,
            )
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
                )
            )
        return series_list, sorted(set(failures))

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
