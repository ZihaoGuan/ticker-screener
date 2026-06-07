from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import re
from typing import Any


DATE_LABEL_RE = re.compile(r"\d{4}-\d{2}-\d{2}")

_STRATEGY_SPECS: dict[str, dict[str, Any]] = {
    "rs": {
        "stem_template": "rs_new_high_before_price_{date_label}",
        "legacy_watchlist_templates": ("rs_new_high_before_price_{date_label}.json",),
        "legacy_raw_templates": ("rs_new_high_before_price_{date_label}.json",),
        "legacy_summary_templates": ("run_summary_{date_label}.json",),
    },
    "weekly_rs": {
        "stem_template": "weekly_rs_new_high_{date_label}",
        "legacy_watchlist_templates": ("weekly_rs_new_high_{date_label}.json",),
        "legacy_raw_templates": ("weekly_rs_new_high_{date_label}.json",),
        "legacy_summary_templates": ("weekly_rs_run_summary_{date_label}.json",),
    },
    "weekly_htf_pullback": {
        "stem_template": "weekly_htf_pullback_{date_label}",
        "legacy_watchlist_templates": ("weekly_htf_pullback_{date_label}.json",),
        "legacy_raw_templates": ("weekly_htf_pullback_{date_label}.json",),
        "legacy_summary_templates": ("weekly_htf_pullback_run_summary_{date_label}.json",),
    },
    "vcp": {
        "stem_template": "vcp_{date_label}",
        "legacy_watchlist_templates": ("vcp_{date_label}.json",),
        "legacy_raw_templates": ("vcp_{date_label}.json",),
        "legacy_summary_templates": ("vcp_run_summary_{date_label}.json",),
    },
    "cup_handle": {
        "stem_template": "cup_handle_{date_label}",
        "legacy_watchlist_templates": ("cup_handle_{date_label}.json",),
        "legacy_raw_templates": ("cup_handle_{date_label}.json",),
        "legacy_summary_templates": ("cup_handle_run_summary_{date_label}.json",),
    },
    "gap_fill": {
        "stem_template": "gap_fill_{date_label}",
        "legacy_watchlist_templates": ("gap_fill_{date_label}.json",),
        "legacy_raw_templates": ("gap_fill_{date_label}.json",),
        "legacy_summary_templates": ("gap_fill_run_summary_{date_label}.json",),
    },
    "hve": {
        "stem_template": "hve_{date_label}",
        "legacy_watchlist_templates": ("hve_{date_label}.json",),
        "legacy_raw_templates": ("hve_{date_label}.json",),
        "legacy_summary_templates": ("hve_run_summary_{date_label}.json",),
    },
    "inside_dryup": {
        "stem_template": "inside_dryup_{date_label}",
        "legacy_watchlist_templates": ("inside_dryup_{date_label}.json",),
        "legacy_raw_templates": ("inside_dryup_{date_label}.json",),
        "legacy_summary_templates": ("inside_dryup_run_summary_{date_label}.json",),
    },
    "ftd_sweep": {
        "stem_template": "ftd_sweep_{date_label}",
        "legacy_watchlist_templates": ("ftd_sweep_{date_label}.json",),
        "legacy_raw_templates": ("ftd_sweep_{date_label}.json",),
        "legacy_summary_templates": ("ftd_sweep_run_summary_{date_label}.json",),
    },
    "fearzone": {
        "stem_template": "fearzone_{date_label}",
        "legacy_watchlist_templates": ("fearzone_{date_label}.json",),
        "legacy_raw_templates": ("fearzone_{date_label}.json",),
        "legacy_summary_templates": ("fearzone_run_summary_{date_label}.json",),
    },
    "fearzone_zeiierman": {
        "stem_template": "fearzone_zeiierman_{date_label}",
        "legacy_watchlist_templates": ("fearzone_zeiierman_{date_label}.json",),
        "legacy_raw_templates": ("fearzone_zeiierman_{date_label}.json",),
        "legacy_summary_templates": ("fearzone_zeiierman_run_summary_{date_label}.json",),
    },
    "near_200ma": {
        "stem_template": "near_200ma_{date_label}",
        "legacy_watchlist_templates": ("near_200ma_{date_label}.json",),
        "legacy_raw_templates": ("near_200ma_{date_label}.json",),
        "legacy_summary_templates": ("near_200ma_run_summary_{date_label}.json",),
    },
    "lost_21ema": {
        "stem_template": "lost_21ema_{date_label}",
        "legacy_watchlist_templates": ("lost_21ema_{date_label}.json",),
        "legacy_raw_templates": ("lost_21ema_{date_label}.json",),
        "legacy_summary_templates": ("lost_21ema_run_summary_{date_label}.json",),
    },
    "htf_8w_runup": {
        "stem_template": "htf_8w_runup_{date_label}",
        "legacy_watchlist_templates": ("htf_8w_runup_{date_label}.json",),
        "legacy_raw_templates": ("htf_8w_runup_{date_label}.json",),
        "legacy_summary_templates": ("htf_8w_runup_run_summary_{date_label}.json",),
    },
    "pre_earnings_ma_stack": {
        "stem_template": "pre_earnings_ma_stack_{date_label}",
        "legacy_watchlist_templates": ("pre_earnings_ma_stack_{date_label}.json",),
        "legacy_raw_templates": ("pre_earnings_ma_stack_{date_label}.json",),
        "legacy_summary_templates": ("pre_earnings_ma_stack_run_summary_{date_label}.json",),
    },
    "pre_earnings_focus": {
        "stem_template": "pre_earnings_focus_{date_label}",
        "legacy_watchlist_templates": ("pre_earnings_focus_{date_label}.json",),
        "legacy_raw_templates": ("pre_earnings_focus_{date_label}.json",),
        "legacy_summary_templates": ("pre_earnings_run_summary_{date_label}.json",),
    },
    "earnings_growth": {
        "stem_template": "earnings_growth_{date_label}",
        "legacy_watchlist_templates": ("earnings_growth_{date_label}.json",),
        "legacy_raw_templates": ("earnings_growth_{date_label}.json",),
        "legacy_summary_templates": ("earnings_growth_run_summary_{date_label}.json",),
    },
    "earnings_weekly_criteria": {
        "stem_template": "earnings_weekly_criteria_{date_label}",
        "legacy_watchlist_templates": ("earnings_weekly_criteria_{date_label}.json",),
        "legacy_raw_templates": ("earnings_weekly_criteria_{date_label}.json",),
        "legacy_summary_templates": ("earnings_weekly_criteria_run_summary_{date_label}.json",),
    },
    "legacy_peg": {
        "stem_template": "legacy_peg_earnings_gap_{date_label}",
        "legacy_watchlist_templates": ("legacy_peg_earnings_gap_{date_label}.json", "peg_earnings_gap_{date_label}.json"),
        "legacy_raw_templates": ("legacy_peg_earnings_gap_{date_label}.json", "peg_earnings_gap_{date_label}.json"),
        "legacy_summary_templates": ("legacy_peg_run_summary_{date_label}.json", "peg_run_summary_{date_label}.json"),
    },
    "sean_peg": {
        "stem_template": "sean_peg_earnings_gap_{date_label}",
        "legacy_watchlist_templates": ("sean_peg_earnings_gap_{date_label}.json", "peg_earnings_gap_{date_label}.json"),
        "legacy_raw_templates": ("sean_peg_earnings_gap_{date_label}.json", "peg_earnings_gap_{date_label}.json"),
        "legacy_summary_templates": ("sean_peg_run_summary_{date_label}.json", "peg_run_summary_{date_label}.json"),
    },
}

_LEGACY_PREFIX_TO_STRATEGY: tuple[tuple[str, str], ...] = (
    ("weekly_htf_pullback", "weekly_htf_pullback"),
    ("weekly_rs_new_high", "weekly_rs"),
    ("rs_new_high_before_price", "rs"),
    ("legacy_peg_earnings_gap", "legacy_peg"),
    ("sean_peg_earnings_gap", "sean_peg"),
    ("peg_earnings_gap", "legacy_peg"),
    ("cup_handle", "cup_handle"),
    ("gap_fill", "gap_fill"),
    ("ftd_sweep", "ftd_sweep"),
    ("fearzone_zeiierman", "fearzone_zeiierman"),
    ("fearzone", "fearzone"),
    ("near_200ma", "near_200ma"),
    ("lost_21ema", "lost_21ema"),
    ("pre_earnings_ma_stack", "pre_earnings_ma_stack"),
    ("pre_earnings_focus", "pre_earnings_focus"),
    ("earnings_weekly_criteria", "earnings_weekly_criteria"),
    ("earnings_growth", "earnings_growth"),
    ("inside_dryup", "inside_dryup"),
    ("htf_8w_runup", "htf_8w_runup"),
    ("hve", "hve"),
    ("vcp", "vcp"),
)


@dataclass(frozen=True)
class ScreenerArtifactPaths:
    strategy_id: str
    date_label: str
    date_folder: str
    logical_stem: str
    root_dir: Path
    raw_results_path: Path
    watchlist_path: Path
    summary_path: Path


def logical_stem_for_strategy(strategy_id: str, date_label: str) -> str:
    spec = _STRATEGY_SPECS.get(strategy_id)
    if spec is None:
        return f"{strategy_id}_{date_label}"
    return str(spec["stem_template"]).format(date_label=date_label)


def resolve_artifact_date_folder(date_label: str) -> str:
    normalized = str(date_label or "").strip()
    match = DATE_LABEL_RE.search(normalized)
    if match:
        return match.group(0)
    return normalized or "unknown-date"


def build_screener_artifact_paths(artifacts_dir: Path, *, strategy_id: str, date_label: str) -> ScreenerArtifactPaths:
    logical_stem = logical_stem_for_strategy(strategy_id, date_label)
    date_folder = resolve_artifact_date_folder(date_label)
    root_dir = artifacts_dir / "screeners" / date_folder / strategy_id
    return ScreenerArtifactPaths(
        strategy_id=strategy_id,
        date_label=date_label,
        date_folder=date_folder,
        logical_stem=logical_stem,
        root_dir=root_dir,
        raw_results_path=root_dir / "raw_results.json",
        watchlist_path=root_dir / "watchlist.json",
        summary_path=root_dir / "run_summary.json",
    )


def strategy_spec(strategy_id: str) -> dict[str, Any] | None:
    return _STRATEGY_SPECS.get(strategy_id)


def strategy_id_from_legacy_stem(stem: str) -> str:
    lower = str(stem or "").strip().lower()
    for prefix, strategy_id in _LEGACY_PREFIX_TO_STRATEGY:
        if lower.startswith(prefix):
            return strategy_id
    return ""


def date_label_from_text(value: str) -> str:
    match = DATE_LABEL_RE.search(str(value or ""))
    return match.group(0) if match else ""


def watchlist_stem_from_path(path_value: str | Path) -> str:
    path = Path(path_value)
    if path.suffix.lower() != ".json":
        return ""
    if path.name == "watchlist.json":
        parts = path.parts
        try:
            index = parts.index("screeners")
        except ValueError:
            return ""
        if len(parts) <= index + 3:
            return ""
        date_folder = parts[index + 1]
        strategy_id = parts[index + 2]
        summary_path = path.parent / "run_summary.json"
        date_label = date_folder
        if summary_path.exists():
            try:
                import json

                payload = json.loads(summary_path.read_text(encoding="utf-8"))
            except Exception:
                payload = {}
            summary_label = str(payload.get("date_label") or "").strip()
            if summary_label:
                date_label = summary_label
            profile = str(payload.get("strategy_profile") or "").strip().lower()
            if strategy_id == "legacy_peg" and profile == "sean-peg":
                strategy_id = "sean_peg"
        return logical_stem_for_strategy(strategy_id, date_label)
    return path.stem


def resolve_legacy_paths(artifacts_dir: Path, *, strategy_id: str, date_label: str) -> dict[str, list[Path]]:
    spec = strategy_spec(strategy_id)
    if spec is None:
        return {"watchlist": [], "raw": [], "summary": []}
    return {
        "watchlist": [(artifacts_dir / "watchlists" / template.format(date_label=date_label)) for template in spec["legacy_watchlist_templates"]],
        "raw": [(artifacts_dir / "raw" / template.format(date_label=date_label)) for template in spec["legacy_raw_templates"]],
        "summary": [(artifacts_dir / "raw" / template.format(date_label=date_label)) for template in spec["legacy_summary_templates"]],
    }
