#!/usr/bin/env python3
from __future__ import annotations

import argparse
import datetime as dt
import json
import os
from pathlib import Path
import sys
from typing import Any


PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.config import load_app_config
from src.earnings_enrichment import (
    build_earnings_annotations,
    enrich_raw_hits,
    enrich_watchlist_entries,
    infer_output_path,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Enrich watchlist or raw screen JSON with earnings date/session and beat-miss fields."
    )
    parser.add_argument("--input-file", required=True, help="Input JSON file (watchlist list or raw hits payload).")
    parser.add_argument("--output-file", help="Output JSON file. Defaults to <input>_earnings.json.")
    parser.add_argument("--config", default=str(PROJECT_ROOT / "config" / "market_config.json"))
    parser.add_argument("--as-of-date", help="Reference date in YYYY-MM-DD. Defaults to today.")
    parser.add_argument("--upcoming-days", type=int, default=14, help="How many forward calendar days to scan.")
    parser.add_argument(
        "--provider",
        choices=("auto", "fmp", "ainvest", "yfinance"),
        help="Override earnings provider. Defaults to config or auto.",
    )
    parser.add_argument(
        "--no-append-summary",
        action="store_true",
        help="Do not prepend the normalized earnings status text to watchlist summaries.",
    )
    return parser.parse_args()


def _write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def _extract_tickers(payload: Any) -> list[str]:
    if isinstance(payload, list):
        return [
            str(item.get("ticker", "")).upper().strip()
            for item in payload
            if isinstance(item, dict) and str(item.get("ticker", "")).strip()
        ]
    if isinstance(payload, dict):
        hits = payload.get("hits")
        if isinstance(hits, list):
            return [
                str(item.get("ticker", "")).upper().strip()
                for item in hits
                if isinstance(item, dict) and str(item.get("ticker", "")).strip()
            ]
    raise ValueError("Unsupported payload shape. Expected a watchlist JSON array or a raw results object with hits[].")


def main() -> int:
    args = parse_args()
    input_path = Path(args.input_file)
    output_path = Path(args.output_file) if args.output_file else infer_output_path(input_path)

    payload = json.loads(input_path.read_text(encoding="utf-8"))
    tickers = _extract_tickers(payload)
    if not tickers:
        raise ValueError(f"No tickers found in {input_path}")

    config = load_app_config(args.config)
    as_of_date = dt.date.fromisoformat(args.as_of_date) if args.as_of_date else dt.date.today()
    annotations = build_earnings_annotations(
        tickers,
        config,
        as_of_date=as_of_date,
        upcoming_days=args.upcoming_days,
        provider=args.provider,
        ainvest_api_key=os.getenv("AINVEST_API_KEY", "").strip() or None,
        fmp_api_key=os.getenv("FMP_API_KEY", "").strip() or None,
    )

    if isinstance(payload, list):
        enriched = enrich_watchlist_entries(
            payload,
            annotations,
            append_summary=not args.no_append_summary,
        )
    else:
        enriched = enrich_raw_hits(payload, annotations)

    _write_json(output_path, enriched)
    print(f"Wrote earnings-enriched JSON to {output_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
