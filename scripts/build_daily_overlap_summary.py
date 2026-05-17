#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from collections import defaultdict
from pathlib import Path
import sys


PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.config import today_label


PIPELINE_FILES = {
    "rs": "rs_new_high_before_price_{date}.json",
    "peg": "peg_earnings_gap_{date}.json",
    "vcp": "vcp_{date}.json",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build a daily overlap summary across RS, PEG, and VCP watchlists."
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
    return parser.parse_args()


def _load_watchlist(path: Path) -> list[dict[str, object]]:
    if not path.exists():
        return []
    payload = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, dict)]
    return []


def _extract_tickers(entries: list[dict[str, object]]) -> list[str]:
    seen: set[str] = set()
    tickers: list[str] = []
    for item in entries:
        ticker = str(item.get("ticker", "")).strip().upper()
        if not ticker or ticker in seen:
            continue
        seen.add(ticker)
        tickers.append(ticker)
    return tickers


def _build_text_summary(payload: dict[str, object]) -> str:
    date_label = str(payload["date_label"])
    available = payload["available_pipelines"]
    pipeline_counts = payload["pipeline_counts"]
    overlap_two_plus = payload["overlap_two_plus"]
    overlap_all_three = payload["overlap_all_three"]

    lines = [
        f"Daily overlap summary for {date_label}",
        f"Available pipelines: {', '.join(available) if available else 'none'}",
        (
            "Counts: "
            + ", ".join(f"{name}={pipeline_counts.get(name, 0)}" for name in ("rs", "peg", "vcp"))
        ),
        f"Overlap >=2 pipelines: {len(overlap_two_plus)}",
        f"Overlap all 3 pipelines: {len(overlap_all_three)}",
        "",
        "Top overlaps:",
    ]
    for item in overlap_two_plus[:25]:
        ticker = item["ticker"]
        pipelines = ", ".join(item["pipelines"])
        lines.append(f"- {ticker}: {pipelines}")
    if len(overlap_two_plus) > 25:
        lines.append(f"- ... and {len(overlap_two_plus) - 25} more")
    return "\n".join(lines) + "\n"


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

    pipeline_tickers: dict[str, list[str]] = {}
    pipeline_counts: dict[str, int] = {}
    ticker_to_pipelines: dict[str, set[str]] = defaultdict(set)

    for pipeline, filename_template in PIPELINE_FILES.items():
        path = watchlist_dir / filename_template.format(date=args.date_label)
        tickers = _extract_tickers(_load_watchlist(path))
        pipeline_tickers[pipeline] = tickers
        pipeline_counts[pipeline] = len(tickers)
        for ticker in tickers:
            ticker_to_pipelines[ticker].add(pipeline)

    overlap_two_plus = [
        {
            "ticker": ticker,
            "pipelines": sorted(pipelines),
            "pipeline_count": len(pipelines),
        }
        for ticker, pipelines in ticker_to_pipelines.items()
        if len(pipelines) >= 2
    ]
    overlap_two_plus.sort(key=lambda item: (-int(item["pipeline_count"]), str(item["ticker"])))

    overlap_all_three = [item for item in overlap_two_plus if int(item["pipeline_count"]) == 3]

    payload = {
        "date_label": args.date_label,
        "available_pipelines": [name for name, count in pipeline_counts.items() if count > 0],
        "pipeline_counts": pipeline_counts,
        "pipeline_tickers": pipeline_tickers,
        "overlap_two_plus": overlap_two_plus,
        "overlap_all_three": overlap_all_three,
    }

    output_json.parent.mkdir(parents=True, exist_ok=True)
    output_json.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    output_text.write_text(_build_text_summary(payload), encoding="utf-8")
    print(f"Wrote overlap summary to {output_json}")
    print(f"Wrote overlap text summary to {output_text}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
