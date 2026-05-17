#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path
import sys

import requests


PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.config import load_app_config
from src.ticker_filters import excluded_tickers_path


DEFAULT_SMALLCAP_ICS_URL = "https://earnings.beavern.com/ics/smallcap.ics"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Refresh the project small-cap exclusion list from a Beavern ICS feed."
    )
    parser.add_argument(
        "--config",
        default=str(PROJECT_ROOT / "config" / "market_config.json"),
        help="Path to the app config JSON.",
    )
    parser.add_argument(
        "--ics-url",
        default=DEFAULT_SMALLCAP_ICS_URL,
        help="ICS feed URL to parse for the small-cap ticker list.",
    )
    parser.add_argument(
        "--output-file",
        default="",
        help="Optional override for the exclusion-list output file.",
    )
    parser.add_argument(
        "--timeout-seconds",
        type=int,
        default=30,
        help="HTTP timeout for the ICS request.",
    )
    return parser.parse_args()


def _iter_ics_events(ics_text: str) -> list[list[str]]:
    unfolded: list[str] = []
    for raw_line in ics_text.splitlines():
        line = raw_line.rstrip("\r")
        if not line:
            continue
        if line.startswith((" ", "\t")) and unfolded:
            unfolded[-1] += line[1:]
        else:
            unfolded.append(line)

    events: list[list[str]] = []
    current: list[str] = []
    in_event = False
    for line in unfolded:
        if line == "BEGIN:VEVENT":
            current = []
            in_event = True
            continue
        if line == "END:VEVENT":
            if in_event and current:
                events.append(current)
            current = []
            in_event = False
            continue
        if in_event:
            current.append(line)
    return events


def _extract_tickers_from_ics(ics_text: str) -> list[str]:
    tickers: list[str] = []
    seen: set[str] = set()
    for event_lines in _iter_ics_events(ics_text):
        for line in event_lines:
            if not line.startswith("SUMMARY:"):
                continue
            summary = line.split(":", 1)[1].strip()
            if not summary:
                continue
            ticker = summary.split()[0].strip().upper()
            if not ticker or ticker in seen:
                continue
            seen.add(ticker)
            tickers.append(ticker)
            break
    return tickers


def main() -> int:
    args = parse_args()
    config = load_app_config(args.config)
    output_path = Path(args.output_file) if args.output_file else excluded_tickers_path(config)
    if not output_path.is_absolute():
        output_path = PROJECT_ROOT / output_path

    response = requests.get(
        args.ics_url,
        timeout=args.timeout_seconds,
        headers={"User-Agent": "ticker-screener/1.0"},
    )
    response.raise_for_status()

    tickers = _extract_tickers_from_ics(response.text)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    payload = "\n".join(tickers)
    if payload:
        payload += "\n"
    output_path.write_text(payload, encoding="utf-8")

    print(f"wrote {len(tickers)} tickers to {output_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
