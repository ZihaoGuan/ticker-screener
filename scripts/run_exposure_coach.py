#!/usr/bin/env python3
from __future__ import annotations

import argparse
import datetime as dt
import json
from pathlib import Path
import subprocess
import sys


PROJECT_ROOT = Path(__file__).resolve().parents[1]
EXPOSURE_SCRIPT = PROJECT_ROOT / "src" / "exposure_coach" / "calculate_exposure.py"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run Exposure Coach from latest cached regime artifacts.")
    parser.add_argument(
        "--date-label",
        help="Optional artifact folder label. Defaults to today's date.",
    )
    return parser.parse_args()


def _latest_artifact(pattern: str, *, exclude_names: set[str] | None = None) -> Path | None:
    excluded = exclude_names or set()
    candidates = [
        path
        for path in (PROJECT_ROOT / "artifacts").rglob(pattern)
        if path.is_file() and path.name not in excluded
    ]
    if not candidates:
        return None
    return max(candidates, key=lambda path: (path.stat().st_mtime, str(path)))


def _build_ibd_top_risk_proxy(ibd_artifact_path: Path, output_dir: Path) -> Path | None:
    try:
        payload = json.loads(ibd_artifact_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    state = payload.get("market_distribution_state") if isinstance(payload, dict) else {}
    index_results = state.get("index_results") if isinstance(state, dict) else []
    if not isinstance(index_results, list):
        return None
    qqq_result = next(
        (
            item
            for item in index_results
            if isinstance(item, dict) and str(item.get("symbol") or "").strip().upper() == "QQQ"
        ),
        None,
    )
    primary_result = qqq_result
    if primary_result is None:
        primary_symbol = str(state.get("primary_signal_symbol") or "").strip().upper()
        primary_result = next(
            (
                item
                for item in index_results
                if isinstance(item, dict) and str(item.get("symbol") or "").strip().upper() == primary_symbol
            ),
            None,
        )
    if primary_result is None:
        primary_result = next((item for item in index_results if isinstance(item, dict)), None)
    distribution_days = None if primary_result is None else primary_result.get("d25_count")
    if distribution_days in (None, ""):
        return None
    proxy_path = output_dir / "top_risk_proxy_from_ibd.json"
    proxy_path.write_text(
        json.dumps({"distribution_days": distribution_days}, indent=2),
        encoding="utf-8",
    )
    return proxy_path


def main() -> int:
    args = parse_args()
    date_label = str(args.date_label or "").strip() or dt.date.today().isoformat()
    output_dir = PROJECT_ROOT / "artifacts" / "reports" / "exposure_coach" / date_label
    output_dir.mkdir(parents=True, exist_ok=True)

    if not EXPOSURE_SCRIPT.exists():
        print(f"ERROR: exposure script not found: {EXPOSURE_SCRIPT}", file=sys.stderr)
        return 1

    breadth_path = _latest_artifact("market_breadth_*.json", exclude_names={"market_breadth_history.json"})
    uptrend_path = _latest_artifact("uptrend_analysis_*.json")
    ibd_path = _latest_artifact("ibd_distribution_day_monitor_*.json")
    top_risk_proxy_path = _build_ibd_top_risk_proxy(ibd_path, output_dir) if ibd_path else None

    if breadth_path is None and uptrend_path is None and top_risk_proxy_path is None:
        print(
            "ERROR: no upstream artifacts found. Need at least one of market breadth, uptrend, or IBD monitor output.",
            file=sys.stderr,
        )
        return 1

    command = [
        sys.executable,
        str(EXPOSURE_SCRIPT),
        "--output-dir",
        str(output_dir),
    ]
    if breadth_path is not None:
        command.extend(["--breadth", str(breadth_path)])
    if uptrend_path is not None:
        command.extend(["--uptrend", str(uptrend_path)])
    if top_risk_proxy_path is not None:
        command.extend(["--top-risk", str(top_risk_proxy_path)])

    print(f"Running Exposure Coach into {output_dir}")
    completed = subprocess.run(command, cwd=str(PROJECT_ROOT))
    if completed.returncode == 0:
        print(f"Exposure Coach artifacts directory: {output_dir}")
    return int(completed.returncode)


if __name__ == "__main__":
    raise SystemExit(main())
