#!/usr/bin/env python3
from __future__ import annotations

import argparse
from collections import defaultdict
from html import escape
import json
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parent.parent


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build a pre-earnings calendar index page.")
    parser.add_argument("--watchlist-file", required=True, help="Watchlist JSON file.")
    parser.add_argument("--output-dir", required=True, help="Rendered output directory containing charts.")
    parser.add_argument("--title", default="Pre-Earnings MA Stack", help="Page title.")
    return parser.parse_args()


def _anchor_id(ticker: str) -> str:
    safe = "".join(ch.lower() if ch.isalnum() else "-" for ch in ticker)
    return f"ticker-{safe}"


def _load_watchlist(path: Path) -> list[dict[str, object]]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, list):
        raise ValueError("watchlist file must contain a JSON array")
    return [item for item in payload if isinstance(item, dict)]


def _group_by_event_date(entries: list[dict[str, object]]) -> list[tuple[str, list[dict[str, object]]]]:
    grouped: dict[str, list[dict[str, object]]] = defaultdict(list)
    for item in entries:
        event_date = str(item.get("event_date") or "Date TBD")
        grouped[event_date].append(item)
    return sorted(grouped.items(), key=lambda pair: pair[0])


def _render_calendar_table(grouped: list[tuple[str, list[dict[str, object]]]]) -> str:
    rows: list[str] = []
    for event_date, entries in grouped:
        ticker_links = " ".join(
            f'<a class="ticker-pill" href="#{_anchor_id(str(item.get("ticker", "")))}">{escape(str(item.get("ticker", "")))}</a>'
            for item in entries
            if str(item.get("ticker", "")).strip()
        )
        rows.append(
            f"""
            <tr>
              <td>{escape(event_date)}</td>
              <td>{len(entries)}</td>
              <td>{ticker_links}</td>
            </tr>
            """
        )
    return "\n".join(rows)


def _render_cards(entries: list[dict[str, object]]) -> str:
    cards: list[str] = []
    for item in entries:
        ticker = str(item.get("ticker", "")).strip()
        if not ticker:
            continue
        setup_label = str(item.get("setup_label", "")).strip()
        summary = str(item.get("summary", "")).strip()
        note = str(item.get("master_note", "")).strip()
        event_date = str(item.get("event_date") or "Date TBD")
        chart_path = f"charts/{ticker}.svg"
        cards.append(
            f"""
            <article class="card" id="{_anchor_id(ticker)}">
              <div class="card-head">
                <div>
                  <h2>{escape(ticker)} <span>{escape(setup_label)}</span></h2>
                  <p class="event-date">Earnings: {escape(event_date)}</p>
                </div>
                <a class="back-link" href="#calendar">Back to calendar</a>
              </div>
              <p>{escape(summary)}</p>
              <a class="chart-link" href="{escape(chart_path)}" target="_blank" rel="noopener noreferrer">
                <img src="{escape(chart_path)}" alt="{escape(ticker)} chart" loading="lazy" />
              </a>
              <pre>{escape(note)}</pre>
            </article>
            """
        )
    return "\n".join(cards)


def build_index(entries: list[dict[str, object]], title: str) -> str:
    grouped = _group_by_event_date(entries)
    calendar_rows = _render_calendar_table(grouped)
    cards = _render_cards(entries)
    total = len(entries)
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>{escape(title)}</title>
  <style>
    :root {{
      color-scheme: dark;
      --bg: #071018;
      --panel: rgba(12, 20, 34, 0.88);
      --panel-strong: rgba(8, 15, 28, 0.96);
      --border: rgba(148, 163, 184, 0.16);
      --text: #e2e8f0;
      --muted: #94a3b8;
      --accent: #38bdf8;
      --accent-soft: rgba(56, 189, 248, 0.16);
      --good: #22c55e;
    }}
    * {{ box-sizing: border-box; }}
    html {{ scroll-behavior: smooth; }}
    body {{
      margin: 0;
      font-family: "SF Mono", "Menlo", "Consolas", monospace;
      background:
        radial-gradient(circle at top left, rgba(56, 189, 248, 0.18), transparent 32%),
        radial-gradient(circle at top right, rgba(34, 197, 94, 0.12), transparent 28%),
        linear-gradient(180deg, #030812 0%, var(--bg) 100%);
      color: var(--text);
    }}
    main {{
      width: min(1500px, calc(100vw - 32px));
      margin: 0 auto;
      padding: 28px 0 64px;
    }}
    header, .calendar-panel {{
      margin-bottom: 24px;
      padding: 24px;
      border: 1px solid var(--border);
      border-radius: 22px;
      background: var(--panel-strong);
      backdrop-filter: blur(12px);
      box-shadow: 0 24px 60px rgba(2, 8, 23, 0.28);
    }}
    h1 {{
      margin: 0 0 8px;
      font-size: clamp(28px, 4vw, 42px);
    }}
    header p, .calendar-panel p {{
      margin: 0;
      color: var(--muted);
      line-height: 1.6;
      max-width: 980px;
    }}
    .meta {{
      margin-top: 14px;
      display: flex;
      gap: 10px;
      flex-wrap: wrap;
    }}
    .meta span {{
      display: inline-flex;
      align-items: center;
      gap: 8px;
      padding: 8px 12px;
      border-radius: 999px;
      background: var(--accent-soft);
      color: var(--text);
      font-size: 13px;
    }}
    h2.section-title {{
      margin: 0 0 14px;
      font-size: 22px;
    }}
    table {{
      width: 100%;
      border-collapse: collapse;
      overflow: hidden;
      border-radius: 16px;
      border: 1px solid var(--border);
    }}
    th, td {{
      padding: 14px 16px;
      text-align: left;
      vertical-align: top;
      border-bottom: 1px solid var(--border);
    }}
    th {{
      font-size: 12px;
      letter-spacing: 0.08em;
      text-transform: uppercase;
      color: var(--muted);
      background: rgba(15, 23, 42, 0.88);
    }}
    tr:last-child td {{
      border-bottom: none;
    }}
    .ticker-pill {{
      display: inline-flex;
      align-items: center;
      padding: 7px 10px;
      margin: 0 8px 8px 0;
      border-radius: 999px;
      border: 1px solid rgba(56, 189, 248, 0.26);
      background: rgba(15, 23, 42, 0.8);
      color: var(--accent);
      text-decoration: none;
      font-size: 13px;
    }}
    .ticker-pill:hover {{
      border-color: rgba(56, 189, 248, 0.48);
      background: rgba(30, 41, 59, 0.92);
    }}
    .grid {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(340px, 1fr));
      gap: 18px;
    }}
    .card {{
      margin: 0;
      padding: 18px;
      border: 1px solid var(--border);
      border-radius: 20px;
      background: var(--panel);
      box-shadow: 0 24px 60px rgba(2, 8, 23, 0.28);
      scroll-margin-top: 20px;
    }}
    .card-head {{
      display: flex;
      justify-content: space-between;
      gap: 16px;
      align-items: flex-start;
      margin-bottom: 10px;
    }}
    .card h2 {{
      margin: 0 0 6px;
      display: flex;
      gap: 10px;
      flex-wrap: wrap;
      align-items: baseline;
      font-size: 22px;
    }}
    .card h2 span {{
      color: var(--accent);
      font-size: 13px;
      letter-spacing: 0.06em;
      text-transform: uppercase;
    }}
    .event-date {{
      margin: 0;
      color: var(--good);
      font-size: 13px;
    }}
    .back-link {{
      color: var(--muted);
      text-decoration: none;
      font-size: 12px;
      white-space: nowrap;
    }}
    .card p {{
      margin: 0 0 14px;
      color: var(--text);
      line-height: 1.55;
    }}
    .chart-link {{
      display: block;
    }}
    .card img {{
      width: 100%;
      border-radius: 16px;
      border: 1px solid rgba(148, 163, 184, 0.14);
      background: #07101e;
      display: block;
    }}
    .card pre {{
      margin: 14px 0 0;
      white-space: pre-wrap;
      line-height: 1.5;
      color: var(--muted);
      font-size: 12px;
    }}
  </style>
</head>
<body>
  <main>
    <header>
      <h1>{escape(title)}</h1>
      <p>Next-week earnings names that currently meet a bullish daily moving-average stack: MA20 above MA50 above MA200. The calendar table links directly to each chart below.</p>
      <div class="meta">
        <span>{total} chart{'' if total == 1 else 's'}</span>
        <span>Rule set: next-week earnings + MA20 &gt; MA50 &gt; MA200</span>
        <span>Post-screen low-cap check: market cap above $1B when data is available</span>
      </div>
    </header>

    <section class="calendar-panel" id="calendar">
      <h2 class="section-title">Next Week Earnings Calendar</h2>
      <p>Click a ticker to jump straight to its chart card. Opening the chart image itself will show the standalone SVG.</p>
      <table>
        <thead>
          <tr>
            <th>Date</th>
            <th>Count</th>
            <th>Tickers</th>
          </tr>
        </thead>
        <tbody>
          {calendar_rows}
        </tbody>
      </table>
    </section>

    <section class="grid">
      {cards}
    </section>
  </main>
</body>
</html>
"""


def main() -> int:
    args = parse_args()
    watchlist_path = Path(args.watchlist_file).resolve()
    output_dir = Path(args.output_dir).resolve()
    entries = _load_watchlist(watchlist_path)
    output_dir.mkdir(parents=True, exist_ok=True)
    html = build_index(entries, args.title)
    (output_dir / "index.html").write_text(html, encoding="utf-8")
    print(f"Wrote pre-earnings calendar index to {output_dir / 'index.html'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
