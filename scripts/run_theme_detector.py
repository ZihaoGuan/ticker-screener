#!/usr/bin/env python3
from __future__ import annotations

import argparse
import importlib
import json
from pathlib import Path
import subprocess
import sys


PROJECT_ROOT = Path(__file__).resolve().parents[1]
THEME_DETECTOR_SCRIPTS_DIR = PROJECT_ROOT / "src" / "theme_detector_runtime"
THEME_DETECTOR_ENTRY = THEME_DETECTOR_SCRIPTS_DIR / "theme_detector.py"
UPTREND_RUNNER = PROJECT_ROOT / "scripts" / "run_uptrend_analysis.py"
REPO_VENV_PYTHON = PROJECT_ROOT.parent / "venv" / "bin" / "python"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run Theme Detector using FINVIZ/finvizfinance, yfinance fallback, and local uptrend_analysis artifacts."
    )
    parser.add_argument(
        "--date-label",
        help="Optional artifact folder label. Defaults to today's date.",
    )
    parser.add_argument(
        "--top",
        type=int,
        default=3,
        help="Number of top themes to show in detail sections.",
    )
    parser.add_argument(
        "--max-themes",
        type=int,
        default=10,
        help="Maximum themes to analyze.",
    )
    parser.add_argument(
        "--max-stocks-per-theme",
        type=int,
        default=10,
        help="Maximum representative stocks per theme.",
    )
    parser.add_argument(
        "--finviz-mode",
        choices=("public", "elite"),
        default="public",
        help="Preferred FINVIZ mode passed through to the detector.",
    )
    parser.add_argument(
        "--themes-config",
        help="Optional custom themes.yaml path.",
    )
    parser.add_argument(
        "--discover-themes",
        action="store_true",
        help="Enable automatic theme discovery for unmatched industries.",
    )
    parser.add_argument(
        "--dynamic-stocks",
        action="store_true",
        default=True,
        help="Enable FINVIZ-based dynamic representative stock selection.",
    )
    parser.add_argument(
        "--dynamic-min-cap",
        choices=("micro", "small", "mid"),
        default="small",
        help="Minimum market cap bucket for dynamic stock selection.",
    )
    parser.add_argument(
        "--ensure-uptrend",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Generate a local uptrend_analysis artifact when none exists for the requested date label.",
    )
    parser.add_argument(
        "--uptrend-artifact",
        help="Optional explicit uptrend_analysis JSON artifact to use instead of auto-discovery.",
    )
    return parser.parse_args()


def _preferred_python() -> Path:
    if REPO_VENV_PYTHON.exists():
        return REPO_VENV_PYTHON
    return Path(sys.executable)


def _maybe_reexec() -> int | None:
    preferred = _preferred_python().resolve()
    current = Path(sys.executable).resolve()
    if preferred == current:
        return None
    completed = subprocess.run(
        [str(preferred), str(Path(__file__).resolve()), *sys.argv[1:]],
        cwd=str(PROJECT_ROOT),
    )
    return int(completed.returncode)


def _latest_artifact(pattern: str, *, date_label: str | None = None) -> Path | None:
    roots: list[Path]
    if date_label:
        roots = [PROJECT_ROOT / "artifacts" / "reports" / "uptrend_analysis" / date_label]
    else:
        roots = [PROJECT_ROOT / "artifacts"]

    candidates: list[Path] = []
    for root in roots:
        if not root.exists():
            continue
        candidates.extend(path for path in root.rglob(pattern) if path.is_file())
    if not candidates:
        return None
    return max(candidates, key=lambda path: (path.stat().st_mtime, str(path)))


def _ensure_uptrend_artifact(date_label: str) -> Path | None:
    artifact = _latest_artifact("uptrend_analysis_*.json", date_label=date_label)
    if artifact is not None:
        return artifact
    if not UPTREND_RUNNER.exists():
        return None
    print(f"No local uptrend artifact for {date_label}; generating one first.", file=sys.stderr)
    completed = subprocess.run(
        [sys.executable, str(UPTREND_RUNNER), "--date-label", date_label],
        cwd=str(PROJECT_ROOT),
    )
    if completed.returncode != 0:
        return None
    return _latest_artifact("uptrend_analysis_*.json", date_label=date_label)


def _normalize_trend(value: object) -> str:
    text = str(value or "").strip().lower()
    if text in {"up", "down"}:
        return text
    return text


def _load_local_uptrend_payload(path: Path) -> dict[str, dict]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    metadata = payload.get("metadata") if isinstance(payload, dict) else {}
    components = payload.get("components") if isinstance(payload, dict) else {}
    breadth = components.get("market_breadth") if isinstance(components, dict) else {}
    participation = (
        components.get("sector_participation") if isinstance(components, dict) else {}
    )
    latest_date = str(
        (breadth or {}).get("date")
        or (metadata or {}).get("latest_data_date")
        or ""
    ).strip()
    sector_details = participation.get("sector_details") if isinstance(participation, dict) else []
    result: dict[str, dict] = {}
    for row in sector_details or []:
        if not isinstance(row, dict):
            continue
        sector_name = str(row.get("sector") or "").strip()
        if not sector_name:
            continue
        result[sector_name] = {
            "ratio": row.get("ratio"),
            "ma_10": row.get("ma_10"),
            "slope": row.get("slope"),
            "trend": _normalize_trend(row.get("trend")),
            "latest_date": latest_date,
        }
    return result


def _build_theme_detector_argv(args: argparse.Namespace, output_dir: Path) -> list[str]:
    argv = [
        str(THEME_DETECTOR_ENTRY),
        "--output-dir",
        str(output_dir),
        "--top",
        str(args.top),
        "--max-themes",
        str(args.max_themes),
        "--max-stocks-per-theme",
        str(args.max_stocks_per_theme),
        "--finviz-mode",
        str(args.finviz_mode),
        "--dynamic-min-cap",
        str(args.dynamic_min_cap),
    ]
    if args.dynamic_stocks:
        argv.append("--dynamic-stocks")
    if args.discover_themes:
        argv.append("--discover-themes")
    if args.themes_config:
        argv.extend(["--themes-config", str(args.themes_config)])
    return argv


def _run_theme_detector(args: argparse.Namespace, output_dir: Path, uptrend_artifact: Path | None) -> int:
    sys.path.insert(0, str(THEME_DETECTOR_SCRIPTS_DIR))
    uptrend_module = importlib.import_module("uptrend_client")
    original_fetch = getattr(uptrend_module, "fetch_sector_uptrend_data")

    if uptrend_artifact is not None:
        local_uptrend = _load_local_uptrend_payload(uptrend_artifact)

        def _fetch_from_local_artifact() -> dict[str, dict]:
            return local_uptrend

        uptrend_module.fetch_sector_uptrend_data = _fetch_from_local_artifact

    theme_module = importlib.import_module("theme_detector")
    original_argv = sys.argv[:]
    sys.argv = _build_theme_detector_argv(args, output_dir)
    try:
        theme_module.main()
        return 0
    except SystemExit as exc:
        code = exc.code
        return int(code) if isinstance(code, int) else 1
    finally:
        sys.argv = original_argv
        uptrend_module.fetch_sector_uptrend_data = original_fetch


def main() -> int:
    reexec_code = _maybe_reexec()
    if reexec_code is not None:
        return reexec_code

    args = parse_args()
    date_label = str(args.date_label or "").strip() or __import__("datetime").date.today().isoformat()
    output_dir = PROJECT_ROOT / "artifacts" / "reports" / "theme_detector" / date_label
    output_dir.mkdir(parents=True, exist_ok=True)

    if not THEME_DETECTOR_ENTRY.exists():
        print(f"ERROR: detector script not found: {THEME_DETECTOR_ENTRY}", file=sys.stderr)
        return 1

    uptrend_artifact: Path | None = None
    if args.uptrend_artifact:
        uptrend_artifact = Path(args.uptrend_artifact).expanduser().resolve()
        if not uptrend_artifact.exists():
            print(f"ERROR: uptrend artifact not found: {uptrend_artifact}", file=sys.stderr)
            return 1
    elif args.ensure_uptrend:
        uptrend_artifact = _ensure_uptrend_artifact(date_label)
        if uptrend_artifact is None:
            print(
                "WARNING: unable to prepare local uptrend_analysis artifact; detector will continue without local uptrend confirmation.",
                file=sys.stderr,
            )
    else:
        uptrend_artifact = _latest_artifact("uptrend_analysis_*.json", date_label=date_label)

    if uptrend_artifact is not None:
        print(f"Using local uptrend artifact: {uptrend_artifact}", file=sys.stderr)

    print(f"Running Theme Detector into {output_dir}")
    code = _run_theme_detector(args, output_dir, uptrend_artifact)
    if code == 0:
        print(f"Theme Detector artifacts directory: {output_dir}")
    return code


if __name__ == "__main__":
    raise SystemExit(main())
