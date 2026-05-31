from __future__ import annotations

import datetime as dt
import json
from pathlib import Path
import re
from typing import Any


class WatchlistRepository:
    def __init__(self, artifacts_dir: Path) -> None:
        self.watchlist_dir = artifacts_dir / "watchlists"

    def list_recent_watchlists(self, limit: int = 200) -> list[dict[str, Any]]:
        if not self.watchlist_dir.exists():
            return []
        rows: list[dict[str, Any]] = []
        paths = sorted(self.watchlist_dir.glob("*.json"), key=lambda item: item.stat().st_mtime, reverse=True)
        for path in paths[:limit]:
            group_key, group_label = _group_for_stem(path.stem)
            captured_at = dt.datetime.fromtimestamp(path.stat().st_mtime, tz=dt.timezone.utc).replace(microsecond=0)
            rows.append(
                {
                    "name": path.name,
                    "stem": path.stem,
                    "path": str(path),
                    "group_key": group_key,
                    "group_label": group_label,
                    "captured_at": captured_at.isoformat(),
                    "sort_date": _first_date_in_stem(path.stem),
                }
            )
        return rows

    def load_watchlist(self, stem: str) -> list[dict[str, Any]]:
        path = self.watchlist_dir / f"{stem}.json"
        if not path.exists():
            return []
        payload = json.loads(path.read_text(encoding="utf-8"))
        if isinstance(payload, list):
            return [item for item in payload if isinstance(item, dict)]
        return []


def _group_for_stem(stem: str) -> tuple[str, str]:
    rules = (
        ("weekly_htf_pullback", "weekly_htf_pullback", "Weekly HTF Pullback"),
        ("htf_8w_runup", "htf_8w_runup", "HTF 8W Runup"),
        ("weekly_rs", "weekly_rs", "Weekly RS"),
        ("rs_new_high_before_price", "rs", "RS"),
        ("cup_handle", "cup_handle", "Cup Handle"),
        ("gap_fill", "gap_fill", "Gap Fill"),
        ("ftd_sweep", "ftd_sweep", "FTD Sweep"),
        ("fearzone", "fearzone", "Fearzone"),
        ("near_200ma", "near_200ma", "Near 200MA"),
        ("lost_21ema", "lost_21ema", "Lost 21EMA"),
        ("pre_earnings_ma_stack", "pre_earnings_ma_stack", "Pre Earnings MA Stack"),
        ("earnings_growth", "earnings_growth", "Earnings Growth"),
        ("peg", "peg", "PEG"),
        ("rs", "rs", "RS"),
    )
    lower = stem.lower()
    for prefix, group_key, group_label in rules:
        if lower.startswith(prefix):
            return group_key, group_label
    return "other", "Other"


def _first_date_in_stem(stem: str) -> str | None:
    match = re.search(r"\d{4}-\d{2}-\d{2}", stem)
    return match.group(0) if match else None
