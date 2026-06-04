#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys


PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.config import today_label
from src.overlap_summary import build_html_summary, build_overlap_payload, build_text_summary


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build a daily overlap summary across RS, Sean PEG, Legacy PEG, VCP, Cup and Handle, Weekly HTF 8W Pullback, 8W 100% Runup, and Gap Fill watchlists."
    )
    parser.add_argument("--date-label", default=today_label(), help="Date label in YYYY-MM-DD format.")
    parser.add_argument(
        "--watchlist-dir",
        default=str(PROJECT_ROOT / "artifacts" / "watchlists"),
        help="Directory containing downloaded watchlist JSON files.",
    )
    parser.add_argument(
        "--output-json",
        default="",
        help="Optional explicit path for the overlap JSON output.",
    )
    parser.add_argument(
        "--output-text",
        default="",
        help="Optional explicit path for the overlap text summary output.",
    )
    parser.add_argument(
        "--output-html",
        default="",
        help="Optional explicit path for the overlap HTML summary output.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    watchlist_dir = Path(args.watchlist_dir)
    output_json = (
        Path(args.output_json)
        if args.output_json
        else PROJECT_ROOT / "artifacts" / "raw" / f"daily_overlap_summary_{args.date_label}.json"
    )
    output_text = (
        Path(args.output_text)
        if args.output_text
        else PROJECT_ROOT / "artifacts" / "raw" / f"daily_overlap_summary_{args.date_label}.txt"
    )
    output_html = (
        Path(args.output_html)
        if args.output_html
        else PROJECT_ROOT / "artifacts" / "output" / f"daily_overlap_summary_{args.date_label}" / "index.html"
    )
    payload = build_overlap_payload(args.date_label, watchlist_dir)

    output_json.parent.mkdir(parents=True, exist_ok=True)
    output_html.parent.mkdir(parents=True, exist_ok=True)
    output_json.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    output_text.write_text(build_text_summary(payload), encoding="utf-8")
    output_html.write_text(build_html_summary(payload), encoding="utf-8")
    print(f"Wrote overlap summary to {output_json}")
    print(f"Wrote overlap text summary to {output_text}")
    print(f"Wrote overlap HTML summary to {output_html}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
