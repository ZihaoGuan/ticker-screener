#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path
import sys


PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


from src.etf_matcher import (
    ETF_CATALOG_PATH,
    ETF_THEME_OVERRIDES_PATH,
    load_etf_catalog,
    load_ticker_theme_overrides,
    match_etfs_for_ticker,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build a candidate ticker to ETF match table.")
    parser.add_argument("--raw-file", required=True, help="Raw screener results JSON file with hits[].")
    parser.add_argument("--output-json", help="Optional JSON output path.")
    parser.add_argument("--output-csv", help="Optional CSV output path.")
    parser.add_argument("--output-md", help="Optional Markdown output path.")
    return parser.parse_args()


def _default_output(path: Path, suffix: str) -> Path:
    return path.parent / f"{path.stem}_etf_matches{suffix}"


def _load_json(path: Path) -> object:
    return json.loads(path.read_text(encoding="utf-8"))


def _write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def _load_hits(raw_path: Path) -> list[dict[str, object]]:
    payload = _load_json(raw_path)
    if not isinstance(payload, dict):
        raise ValueError("raw screener payload must be a JSON object")
    hits = payload.get("hits", [])
    if not isinstance(hits, list):
        raise ValueError("raw screener payload must contain hits[]")
    return [item for item in hits if isinstance(item, dict)]


def _build_rows(raw_path: Path) -> dict[str, object]:
    hits = _load_hits(raw_path)
    catalog = load_etf_catalog()
    overrides = load_ticker_theme_overrides()
    rows: list[dict[str, object]] = []
    for hit in hits:
        ticker = str(hit.get("ticker", "")).strip().upper()
        if not ticker:
            continue
        sector = str(hit.get("sector", "")).strip() or None
        ticker_themes = overrides.get(ticker, [])
        matches = match_etfs_for_ticker(
            sector=sector,
            ticker_themes=ticker_themes,
            catalog=catalog,
        )
        rows.append(
            {
                "ticker": ticker,
                "sector": sector,
                "earnings_date": hit.get("earnings_date"),
                "etf_count": len(matches),
                "ticker_themes": ticker_themes,
                "matched_etfs": matches,
            }
        )
    return {
        "source_file": str(raw_path),
        "catalog_file": str(ETF_CATALOG_PATH),
        "ticker_theme_overrides_file": str(ETF_THEME_OVERRIDES_PATH),
        "rows": rows,
    }


def _write_csv(path: Path, rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(["ticker", "sector", "earnings_date", "etf_ticker", "etf_name", "match_reason"])
        for row in rows:
            matched = row.get("matched_etfs", [])
            if not matched:
                writer.writerow([row.get("ticker"), row.get("sector"), row.get("earnings_date"), "", "", ""])
                continue
            for match in matched:
                writer.writerow(
                    [
                        row.get("ticker"),
                        row.get("sector"),
                        row.get("earnings_date"),
                        match.get("ticker"),
                        match.get("name"),
                        match.get("reason"),
                    ]
                )


def _write_markdown(path: Path, rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    lines = [
        "# Candidate ETF Match Table",
        "",
        "| Ticker | Sector | Earnings Date | ETF Count | Matched ETFs |",
        "| --- | --- | --- | ---: | --- |",
    ]
    for row in rows:
        matched = row.get("matched_etfs", [])
        if matched:
            etf_text = "<br>".join(
                f"`{match['ticker']}` {match['name']} ({match['reason']})"
                for match in matched
            )
        else:
            etf_text = "-"
        lines.append(
            f"| `{row.get('ticker', '')}` | {row.get('sector') or '-'} | {row.get('earnings_date') or '-'} | {row.get('etf_count', 0)} | {etf_text} |"
        )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> int:
    args = parse_args()
    raw_path = Path(args.raw_file).resolve()
    payload = _build_rows(raw_path)
    rows = payload["rows"]

    output_json = Path(args.output_json).resolve() if args.output_json else _default_output(raw_path, ".json")
    output_csv = Path(args.output_csv).resolve() if args.output_csv else _default_output(raw_path, ".csv")
    output_md = Path(args.output_md).resolve() if args.output_md else _default_output(raw_path, ".md")

    _write_json(output_json, payload)
    _write_csv(output_csv, rows)
    _write_markdown(output_md, rows)

    print(f"Wrote ETF match JSON to {output_json}")
    print(f"Wrote ETF match CSV to {output_csv}")
    print(f"Wrote ETF match Markdown to {output_md}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
