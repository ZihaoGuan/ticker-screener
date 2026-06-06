#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path
import shutil
import sys
from typing import Any


PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.artifact_paths import (
    build_screener_artifact_paths,
    date_label_from_text,
    resolve_legacy_paths,
    strategy_id_from_legacy_stem,
)
from src.webapp.repositories.history_repository import HistoryRepository


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Migrate flat screener artifacts into date-rooted folders.")
    parser.add_argument("--artifacts-dir", default=str(PROJECT_ROOT / "artifacts"))
    parser.add_argument("--database-url", default="")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--copy", action="store_true", help="Copy files instead of moving them.")
    return parser.parse_args()


def _load_json(path: Path) -> dict[str, Any] | None:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None
    return payload if isinstance(payload, dict) else None


def _resolve_strategy_id(summary_path: Path, payload: dict[str, Any]) -> str:
    strategy_id = str(payload.get("strategy_id") or "").strip()
    if strategy_id:
        return strategy_id
    profile = str(payload.get("strategy_profile") or "").strip().lower()
    if profile == "sean-peg":
        return "sean_peg"
    if profile == "legacy":
        return "legacy_peg"
    for key in ("watchlist_file", "raw_results_file"):
        path_value = str(payload.get(key) or "").strip()
        if path_value:
            candidate = strategy_id_from_legacy_stem(Path(path_value).stem)
            if candidate:
                return candidate
    return strategy_id_from_legacy_stem(summary_path.stem)


def _resolve_date_label(summary_path: Path, payload: dict[str, Any]) -> str:
    for key in ("date_label", "as_of_date", "reference_date"):
        value = str(payload.get(key) or "").strip()
        if value:
            return value
    for key in ("watchlist_file", "raw_results_file"):
        path_value = str(payload.get(key) or "").strip()
        if path_value:
            value = date_label_from_text(path_value)
            if value:
                return value
    return date_label_from_text(summary_path.name)


def _first_existing(paths: list[Path], fallback: str = "") -> Path | None:
    for path in paths:
        if path.exists():
            return path
    if fallback:
        path = Path(fallback)
        if path.exists():
            return path
    return None


def _write_summary(path: Path, payload: dict[str, Any], *, dry_run: bool) -> None:
    if dry_run:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def _transfer_file(source: Path, target: Path, *, dry_run: bool, copy_mode: bool) -> None:
    if dry_run:
        return
    target.parent.mkdir(parents=True, exist_ok=True)
    if target.exists():
        if source.resolve() == target.resolve():
            return
        if source.read_bytes() == target.read_bytes():
            if not copy_mode and source.exists():
                source.unlink()
            return
    if copy_mode:
        shutil.copy2(source, target)
    else:
        shutil.move(str(source), str(target))


def migrate_artifacts(*, artifacts_dir: Path, database_url: str = "", dry_run: bool = False, copy_mode: bool = False) -> dict[str, Any]:
    raw_dir = artifacts_dir / "raw"
    summary_paths = sorted(path for path in raw_dir.glob("*.json") if "summary" in path.stem)
    migrated_runs = 0
    skipped: list[str] = []
    path_map: dict[str, str] = {}

    for summary_path in summary_paths:
        payload = _load_json(summary_path)
        if payload is None:
            skipped.append(f"{summary_path.name}: invalid summary json")
            continue
        strategy_id = _resolve_strategy_id(summary_path, payload)
        date_label = _resolve_date_label(summary_path, payload)
        if not strategy_id or not date_label:
            skipped.append(f"{summary_path.name}: ambiguous strategy/date")
            continue

        targets = build_screener_artifact_paths(artifacts_dir, strategy_id=strategy_id, date_label=date_label)
        legacy_paths = resolve_legacy_paths(artifacts_dir, strategy_id=strategy_id, date_label=date_label)
        raw_source = _first_existing(legacy_paths["raw"], str(payload.get("raw_results_file") or ""))
        watchlist_source = _first_existing(legacy_paths["watchlist"], str(payload.get("watchlist_file") or ""))

        if raw_source is None or watchlist_source is None:
            skipped.append(f"{summary_path.name}: missing raw/watchlist source")
            continue

        _transfer_file(raw_source, targets.raw_results_path, dry_run=dry_run, copy_mode=copy_mode)
        _transfer_file(watchlist_source, targets.watchlist_path, dry_run=dry_run, copy_mode=copy_mode)

        updated_payload = dict(payload)
        updated_payload["strategy_id"] = strategy_id
        updated_payload["date_label"] = date_label
        updated_payload["raw_results_file"] = str(targets.raw_results_path)
        updated_payload["watchlist_file"] = str(targets.watchlist_path)
        _write_summary(targets.summary_path, updated_payload, dry_run=dry_run)

        if not dry_run and not copy_mode and summary_path.exists() and summary_path.resolve() != targets.summary_path.resolve():
            summary_path.unlink()

        path_map[str(raw_source)] = str(targets.raw_results_path)
        path_map[str(watchlist_source)] = str(targets.watchlist_path)
        migrated_runs += 1

    updated_rows = 0
    if path_map and database_url:
        updated_rows = HistoryRepository(database_url=database_url, artifacts_dir=artifacts_dir).rewrite_screen_run_artifact_paths(path_map)

    return {
        "artifacts_dir": str(artifacts_dir),
        "dry_run": dry_run,
        "copy_mode": copy_mode,
        "migrated_runs": migrated_runs,
        "db_rows_updated": updated_rows,
        "skipped": skipped,
    }


def main() -> int:
    args = parse_args()
    payload = migrate_artifacts(
        artifacts_dir=Path(args.artifacts_dir).resolve(),
        database_url=str(args.database_url or ""),
        dry_run=bool(args.dry_run),
        copy_mode=bool(args.copy),
    )
    print(json.dumps(payload, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
