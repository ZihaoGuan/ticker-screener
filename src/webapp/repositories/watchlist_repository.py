from __future__ import annotations

import datetime as dt
import json
from pathlib import Path
import re
from typing import Any

from src.artifact_paths import strategy_id_from_legacy_stem, watchlist_stem_from_path
from .history_repository import HistoryRepository


class WatchlistRepository:
    def __init__(self, artifacts_dir: Path, database_url: str = "") -> None:
        self.artifacts_dir = artifacts_dir
        self.watchlist_dir = artifacts_dir / "watchlists"
        self.history_repository = HistoryRepository(database_url=database_url, artifacts_dir=artifacts_dir)

    def list_recent_watchlists(self, limit: int = 200, *, include_deprecated: bool = True) -> list[dict[str, Any]]:
        rows = list(self._build_watchlist_index().values())
        if not include_deprecated:
            rows = [item for item in rows if not bool(item.get("is_deprecated"))]
        rows.sort(key=lambda item: str(item.get("captured_at") or ""), reverse=True)
        return rows[:limit]

    def load_watchlist(self, stem: str) -> list[dict[str, Any]]:
        db_payload = self._load_watchlist_from_db(stem)
        if db_payload is not None:
            return db_payload
        path = self.resolve_watchlist_path(stem)
        if not path.exists():
            return []
        payload = json.loads(path.read_text(encoding="utf-8"))
        if isinstance(payload, list):
            return [item for item in payload if isinstance(item, dict)]
        return []

    def resolve_watchlist_path(self, stem: str) -> Path:
        metadata = self._build_watchlist_index().get(stem)
        path_value = str(metadata.get("path") or "") if isinstance(metadata, dict) else ""
        return Path(path_value) if path_value else (self.watchlist_dir / f"{stem}.json")

    def get_watchlist_metadata(self, stem: str) -> dict[str, Any] | None:
        metadata = self._build_watchlist_index().get(stem)
        return dict(metadata) if isinstance(metadata, dict) else None

    def _build_watchlist_index(self) -> dict[str, dict[str, Any]]:
        index: dict[str, dict[str, Any]] = {}

        if self.history_repository.is_configured():
            for item in self.history_repository.list_screen_runs(limit=2000, has_hits=True):
                self._upsert_db_entry(index, item)

        for path in sorted(self.watchlist_dir.glob("*.json"), key=lambda item: item.stat().st_mtime, reverse=True):
            self._upsert_index_entry(index, path, layout="legacy")

        screeners_dir = self.artifacts_dir / "screeners"
        if screeners_dir.exists():
            for path in sorted(screeners_dir.glob("*/*/watchlist.json"), key=lambda item: item.stat().st_mtime, reverse=True):
                self._upsert_index_entry(index, path, layout="dated")

        return index

    def _upsert_index_entry(self, index: dict[str, dict[str, Any]], path: Path, *, layout: str) -> None:
        try:
            stat = path.stat()
        except FileNotFoundError:
            return
        stem = watchlist_stem_from_path(path)
        if not stem:
            return
        existing = index.get(stem)
        if existing is not None and existing.get("layout") == "db":
            return
        if existing is not None and existing.get("layout") == "dated":
            return
        if existing is not None and layout != "dated":
            return
        group_key, group_label = _group_for_stem(stem)
        captured_at = dt.datetime.fromtimestamp(stat.st_mtime, tz=dt.timezone.utc).replace(microsecond=0)
        index[stem] = {
            "name": f"{stem}.json",
            "stem": stem,
            "path": str(path),
            "group_key": group_key,
            "group_label": group_label,
            "captured_at": captured_at.isoformat(),
            "sort_date": _first_date_in_stem(stem),
            "layout": layout,
            "is_deprecated": layout == "legacy",
            "deprecation_reason": _deprecation_reason_for_layout(layout),
        }

    def _upsert_db_entry(self, index: dict[str, dict[str, Any]], row: dict[str, Any]) -> None:
        raw_path = str(row.get("watchlist_artifact_path") or "").strip()
        stem = watchlist_stem_from_path(raw_path)
        if not stem:
            return
        existing = index.get(stem)
        if existing is not None and existing.get("layout") == "db":
            existing_created_at = str(existing.get("captured_at") or "")
            current_created_at = _isoformat_or_empty(row.get("created_at"))
            if existing_created_at >= current_created_at:
                return
        group_key, group_label = _group_for_stem(stem)
        index[stem] = {
            "name": Path(raw_path).name if raw_path else f"{stem}.json",
            "stem": stem,
            "path": raw_path or str(self.watchlist_dir / f"{stem}.json"),
            "group_key": group_key,
            "group_label": group_label,
            "captured_at": _isoformat_or_empty(row.get("created_at")),
            "sort_date": _date_or_empty(row.get("run_date")) or _first_date_in_stem(stem),
            "layout": "db",
            "is_deprecated": False,
            "deprecation_reason": None,
            "screen_run_id": int(row["id"]) if row.get("id") is not None else None,
            "strategy_id": str(row.get("strategy_id") or ""),
        }

    def _load_watchlist_from_db(self, stem: str) -> list[dict[str, Any]] | None:
        if not self.history_repository.is_configured():
            return None
        run_payload = self.history_repository.find_screen_run_by_watchlist_stem(stem, include_hits=True)
        if run_payload is None:
            return None
        hits = run_payload.get("hits")
        if not isinstance(hits, list):
            return []
        entries: list[dict[str, Any]] = []
        for item in hits:
            if not isinstance(item, dict) or not bool(item.get("passed")):
                continue
            payload = item.get("hit_payload_json")
            if isinstance(payload, dict):
                entries.append(payload)
        return entries


def _group_for_stem(stem: str) -> tuple[str, str]:
    rules = (
        ("weekly_htf_pullback", "weekly_htf_pullback", "Weekly HTF Pullback"),
        ("eight_week_100_runup", "eight_week_100_runup", "8W 100% Runup"),
        ("htf_8w_runup", "eight_week_100_runup", "8W 100% Runup"),
        ("weekly_rs", "weekly_rs", "Weekly RS"),
        ("rs_new_high_before_price", "rs", "RS"),
        ("cup_handle", "cup_handle", "Cup Handle"),
        ("gap_fill", "gap_fill", "Gap Fill"),
        ("ftd_sweep", "ftd_sweep", "FTD Sweep"),
        ("fearzone_zeiierman", "fearzone_zeiierman", "Fearzone Zeiierman"),
        ("td9_bullish", "td9", "TD9"),
        ("td9_bearish", "td9", "TD9"),
        ("macd_golden_cross", "macd", "MACD"),
        ("macd_dead_cross", "macd", "MACD"),
        ("rsi_ma_bb_bullish", "rsi_ma_bb", "RSI MA/BB"),
        ("rsi_ma_bb_bearish", "rsi_ma_bb", "RSI MA/BB"),
        ("inside_dryup_v2", "inside_dryup_v2", "Inside Day + Extreme Dry-Up"),
        ("wyckoff_buy_signal", "wyckoff_buy_signal", "Wyckoff Buy Signal"),
        ("wyckoff_sell_signal", "wyckoff_sell_signal", "Wyckoff Sell Signal"),
        ("bb_squeeze", "bb_squeeze", "BB Squeeze"),
        ("ema21_pullback_buy", "ema21_pullback_buy", "EMA21 Pullback Buy"),
        ("sma200_pullback_buy", "sma200_pullback_buy", "200 SMA Pullback Buy"),
        ("high_tight_flag_setup", "high_tight_flag_setup", "High Tight Flag Setup"),
        ("high_tight_flag", "high_tight_flag", "High Tight Flag"),
        ("leif_high_tight_flag", "leif_high_tight_flag", "Leif High Tight Flag"),
        ("sepa_vcp", "sepa_vcp", "SEPA VCP"),
        ("rti", "rti", "RTI"),
        ("sean_breakout", "sean_breakout", "Sean Breakout"),
        ("vcs_critical_tightness", "vcs", "VCS"),
        ("vcs_setup_stage", "vcs", "VCS"),
        ("base_detection", "base_detection", "Base Detection"),
        ("cup_detection", "cup_detection", "Cup Detection"),
        ("double_bottom_detection", "double_bottom_detection", "Double Bottom Detection"),
        ("weekly_tight_close_breakout", "weekly_tight_close_breakout", "Weekly Tight Close Breakout"),
        ("weekly_tight_close", "weekly_tight_close", "Weekly Tight Close"),
        ("weinstein_stage2_early", "weinstein_stage2_early", "Weinstein Stage 2 Early"),
        ("three_weeks_tight", "three_weeks_tight", "Three Weeks Tight"),
        ("fearzone", "fearzone", "Fearzone"),
        ("near_200ma", "near_200ma", "Near 200MA"),
        ("lost_21ema", "lost_21ema", "Lost 21EMA"),
        ("trend_template", "trend_template", "Trend Template"),
        ("pre_earnings_ma_stack", "pre_earnings_ma_stack", "Pre Earnings MA Stack"),
        ("earnings_weekly_criteria", "earnings_weekly_criteria", "Earnings Weekly Criteria"),
        ("earnings_growth", "earnings_growth", "Earnings Growth"),
        ("peg", "peg", "PEG"),
        ("rs", "rs", "RS"),
    )
    lower = stem.lower()
    strategy_id = strategy_id_from_legacy_stem(stem)
    if strategy_id in {"legacy_peg", "sean_peg"}:
        return "peg", "PEG"
    for prefix, group_key, group_label in rules:
        if lower.startswith(prefix):
            return group_key, group_label
    return "other", "Other"


def _first_date_in_stem(stem: str) -> str | None:
    match = re.search(r"\d{4}-\d{2}-\d{2}", stem)
    return match.group(0) if match else None


def _isoformat_or_empty(value: Any) -> str:
    if isinstance(value, dt.datetime):
        return value.isoformat()
    return str(value or "")


def _date_or_empty(value: Any) -> str:
    if isinstance(value, dt.date):
        return value.isoformat()
    return str(value or "")


def _deprecation_reason_for_layout(layout: str) -> str | None:
    if layout == "legacy":
        return "Legacy report watchlist from artifacts/watchlists. Deprecated in favor of scanner boards, dated screener artifacts, and the report page."
    return None
