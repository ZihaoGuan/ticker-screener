#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from collections import defaultdict
from html import escape
from pathlib import Path
import sys


PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.config import today_label


PIPELINES = (
    {
        "id": "rs",
        "label": "RS",
        "filename": "rs_new_high_before_price_{date}.json",
    },
    {
        "id": "sean_peg",
        "label": "Sean PEG",
        "filename": "sean_peg_earnings_gap_{date}.json",
        "fallback_filename": "peg_earnings_gap_{date}.json",
    },
    {
        "id": "legacy_peg",
        "label": "Legacy PEG",
        "filename": "legacy_peg_earnings_gap_{date}.json",
        "fallback_filename": "peg_earnings_gap_{date}.json",
    },
    {
        "id": "vcp",
        "label": "VCP",
        "filename": "vcp_{date}.json",
    },
    {
        "id": "cup_handle",
        "label": "Cup and Handle",
        "filename": "cup_handle_{date}.json",
    },
    {
        "id": "weekly_htf_pullback",
        "label": "Weekly HTF 8W Pullback",
        "filename": "weekly_htf_pullback_{date}.json",
    },
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build a daily overlap summary across RS, Sean PEG, Legacy PEG, VCP, Cup and Handle, and Weekly HTF 8W Pullback watchlists."
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


def _resolve_pipeline_path(watchlist_dir: Path, date_label: str, pipeline: dict[str, str]) -> tuple[Path | None, str]:
    primary = watchlist_dir / pipeline["filename"].format(date=date_label)
    if primary.exists():
        return primary, "primary"
    fallback_name = pipeline.get("fallback_filename")
    if fallback_name:
        fallback = watchlist_dir / fallback_name.format(date=date_label)
        if fallback.exists():
            return fallback, "fallback"
    return None, "missing"


def _build_text_summary(payload: dict[str, object]) -> str:
    date_label = str(payload["date_label"])
    pipeline_status = payload["pipeline_status"]
    overlap_two_plus = payload["overlap_two_plus"]
    overlap_three_plus = payload["overlap_three_plus"]

    lines = [
        f"Daily overlap summary for {date_label}",
        f"Unique tickers across present pipelines: {payload['unique_ticker_count']}",
        f"Overlap >=2 pipelines: {len(overlap_two_plus)}",
        f"Overlap >=3 pipelines: {len(overlap_three_plus)}",
        "",
        "Pipeline status:",
    ]
    for pipeline in pipeline_status:
        label = pipeline["label"]
        present = "present" if pipeline["file_present"] else "missing"
        source = pipeline.get("source_filename") or "n/a"
        lines.append(f"- {label}: {present}, count={pipeline['count']}, source={source}")

    lines.extend(["", "Top overlaps:"])
    for item in overlap_two_plus[:25]:
        ticker = item["ticker"]
        pipelines = ", ".join(item["pipeline_labels"])
        lines.append(f"- {ticker}: {pipelines}")
    if len(overlap_two_plus) > 25:
        lines.append(f"- ... and {len(overlap_two_plus) - 25} more")
    return "\n".join(lines) + "\n"


def _build_html_summary(payload: dict[str, object]) -> str:
    date_label = escape(str(payload["date_label"]))
    unique_count = int(payload["unique_ticker_count"])
    overlap_two_plus = payload["overlap_two_plus"]
    overlap_three_plus = payload["overlap_three_plus"]
    pipeline_status = payload["pipeline_status"]

    status_cards = []
    for pipeline in pipeline_status:
        badge = "present" if pipeline["file_present"] else "missing"
        badge_class = "ok" if pipeline["file_present"] else "missing"
        source_filename = escape(str(pipeline.get("source_filename") or "n/a"))
        resolution = escape(str(pipeline.get("resolution") or "missing"))
        status_cards.append(
            f"""
            <article class="status-card">
              <div class="status-head">
                <h3>{escape(str(pipeline['label']))}</h3>
                <span class="badge {badge_class}">{badge}</span>
              </div>
              <p>{int(pipeline['count'])} tickers</p>
              <pre>source: {source_filename}\nresolution: {resolution}</pre>
            </article>
            """
        )

    rows = []
    for item in overlap_two_plus[:100]:
        rows.append(
            f"""
            <tr>
              <td>{escape(str(item['ticker']))}</td>
              <td>{int(item['pipeline_count'])}</td>
              <td>{escape(', '.join(item['pipeline_labels']))}</td>
            </tr>
            """
        )
    table_rows = "".join(rows) or '<tr><td colspan="3">No overlaps found.</td></tr>'

    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Daily Overlap Summary {date_label}</title>
  <style>
    :root {{
      --bg: #0b1220;
      --panel: #111c2f;
      --panel-2: #16233a;
      --ink: #edf2f7;
      --muted: #9fb0c6;
      --line: #24354d;
      --good: #22c55e;
      --warn: #f59e0b;
      --bad: #ef4444;
      --accent: #38bdf8;
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      font-family: "SF Mono", "Menlo", "Consolas", monospace;
      background: radial-gradient(circle at top left, rgba(56, 189, 248, 0.12), transparent 28%), var(--bg);
      color: var(--ink);
    }}
    main {{
      max-width: 1160px;
      margin: 0 auto;
      padding: 32px 20px 56px;
    }}
    header, section {{
      border: 1px solid var(--line);
      border-radius: 20px;
      background: rgba(17, 28, 47, 0.92);
      padding: 24px;
      margin-bottom: 20px;
    }}
    h1, h2, h3 {{
      margin: 0;
    }}
    p {{
      color: var(--muted);
      line-height: 1.6;
    }}
    .stats, .status-grid {{
      display: grid;
      gap: 14px;
    }}
    .stats {{
      grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
      margin-top: 18px;
    }}
    .stat, .status-card {{
      border: 1px solid var(--line);
      border-radius: 16px;
      background: var(--panel-2);
      padding: 16px;
    }}
    .stat strong {{
      display: block;
      font-size: 28px;
      color: var(--accent);
      margin-bottom: 6px;
    }}
    .status-grid {{
      grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
    }}
    .status-head {{
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 12px;
      margin-bottom: 8px;
    }}
    .badge {{
      border-radius: 999px;
      padding: 4px 10px;
      font-size: 12px;
      text-transform: uppercase;
    }}
    .badge.ok {{
      background: rgba(34, 197, 94, 0.16);
      color: #86efac;
    }}
    .badge.missing {{
      background: rgba(239, 68, 68, 0.16);
      color: #fca5a5;
    }}
    pre {{
      margin: 0;
      white-space: pre-wrap;
      color: var(--muted);
      font-size: 12px;
    }}
    table {{
      width: 100%;
      border-collapse: collapse;
    }}
    th, td {{
      text-align: left;
      padding: 12px 10px;
      border-bottom: 1px solid var(--line);
      vertical-align: top;
    }}
    th {{
      color: #cbd5e1;
      font-size: 12px;
      letter-spacing: 0.08em;
      text-transform: uppercase;
    }}
  </style>
</head>
<body>
  <main>
    <header>
      <h1>Daily Overlap Summary</h1>
      <p>{date_label}</p>
      <div class="stats">
        <div class="stat"><strong>{unique_count}</strong>Unique tickers</div>
        <div class="stat"><strong>{len(overlap_two_plus)}</strong>Overlap in 2+ pipelines</div>
        <div class="stat"><strong>{len(overlap_three_plus)}</strong>Overlap in 3+ pipelines</div>
      </div>
    </header>
    <section>
      <h2>Pipeline Status</h2>
      <div class="status-grid">
        {''.join(status_cards)}
      </div>
    </section>
    <section>
      <h2>Top Overlaps</h2>
      <table>
        <thead>
          <tr>
            <th>Ticker</th>
            <th>Count</th>
            <th>Pipelines</th>
          </tr>
        </thead>
        <tbody>
          {table_rows}
        </tbody>
      </table>
    </section>
  </main>
</body>
</html>
"""


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

    pipeline_tickers: dict[str, list[str]] = {}
    pipeline_counts: dict[str, int] = {}
    pipeline_status: list[dict[str, object]] = []
    ticker_to_pipelines: dict[str, set[str]] = defaultdict(set)

    labels_by_id = {pipeline["id"]: pipeline["label"] for pipeline in PIPELINES}

    for pipeline in PIPELINES:
        pipeline_id = pipeline["id"]
        path, resolution = _resolve_pipeline_path(watchlist_dir, args.date_label, pipeline)
        entries = _load_watchlist(path) if path is not None else []
        tickers = _extract_tickers(entries)
        pipeline_tickers[pipeline_id] = tickers
        pipeline_counts[pipeline_id] = len(tickers)
        pipeline_status.append(
            {
                "id": pipeline_id,
                "label": pipeline["label"],
                "file_present": path is not None,
                "count": len(tickers),
                "source_filename": path.name if path is not None else "",
                "resolution": resolution,
            }
        )
        for ticker in tickers:
            ticker_to_pipelines[ticker].add(pipeline_id)

    overlap_two_plus = [
        {
            "ticker": ticker,
            "pipelines": sorted(pipelines),
            "pipeline_labels": [labels_by_id[pipeline_id] for pipeline_id in sorted(pipelines)],
            "pipeline_count": len(pipelines),
        }
        for ticker, pipelines in ticker_to_pipelines.items()
        if len(pipelines) >= 2
    ]
    overlap_two_plus.sort(key=lambda item: (-int(item["pipeline_count"]), str(item["ticker"])))
    overlap_three_plus = [item for item in overlap_two_plus if int(item["pipeline_count"]) >= 3]

    payload = {
        "date_label": args.date_label,
        "pipeline_status": pipeline_status,
        "pipeline_counts": pipeline_counts,
        "pipeline_tickers": pipeline_tickers,
        "present_pipelines": [item["id"] for item in pipeline_status if item["file_present"]],
        "missing_pipelines": [item["id"] for item in pipeline_status if not item["file_present"]],
        "unique_ticker_count": len(ticker_to_pipelines),
        "overlap_two_plus": overlap_two_plus,
        "overlap_three_plus": overlap_three_plus,
    }

    output_json.parent.mkdir(parents=True, exist_ok=True)
    output_html.parent.mkdir(parents=True, exist_ok=True)
    output_json.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    output_text.write_text(_build_text_summary(payload), encoding="utf-8")
    output_html.write_text(_build_html_summary(payload), encoding="utf-8")
    print(f"Wrote overlap summary to {output_json}")
    print(f"Wrote overlap text summary to {output_text}")
    print(f"Wrote overlap HTML summary to {output_html}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
