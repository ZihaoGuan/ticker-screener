#!/usr/bin/env python3
from __future__ import annotations

import argparse
from html import escape
import json
from pathlib import Path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build a grouped index for the near-200D MA report.")
    parser.add_argument("--watchlist-file", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--title", default="Near 200D MA")
    return parser.parse_args()


def _anchor_id(ticker: str) -> str:
    safe = "".join(ch.lower() if ch.isalnum() else "-" for ch in ticker)
    return f"ticker-{safe}"


def _load_watchlist(path: Path) -> list[dict[str, object]]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, list):
        raise ValueError("watchlist file must contain a JSON array")
    return [item for item in payload if isinstance(item, dict)]


def _group(entries: list[dict[str, object]], case_group: str) -> list[dict[str, object]]:
    return [item for item in entries if str(item.get("case_group", "")).strip().lower() == case_group]


def _ticker_pills(entries: list[dict[str, object]]) -> str:
    return " ".join(
        f'<a class="ticker-pill" href="#{_anchor_id(str(item.get("ticker", "")))}">{escape(str(item.get("ticker", "")))}</a>'
        for item in entries
        if str(item.get("ticker", "")).strip()
    )


def _cards(entries: list[dict[str, object]]) -> str:
    cards: list[str] = []
    for item in entries:
        ticker = str(item.get("ticker", "")).strip()
        if not ticker:
            continue
        setup_label = str(item.get("setup_label", "")).strip()
        summary = str(item.get("summary", "")).strip()
        note = str(item.get("master_note", "")).strip()
        sector = str(item.get("sector") or "Unknown")
        industry = str(item.get("industry") or "Unknown")
        chart_path = f"charts/{ticker}.svg"
        cards.append(
            f"""
            <article class="card" id="{_anchor_id(ticker)}">
              <div class="card-head">
                <div>
                  <h2>{escape(ticker)} <span>{escape(setup_label)}</span></h2>
                  <p class="meta-line">{escape(sector)} | {escape(industry)}</p>
                </div>
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
    bull_entries = _group(entries, "bull")
    bear_entries = _group(entries, "bear")
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
      --bull: #22c55e;
      --bear: #f87171;
      --accent: #38bdf8;
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      font-family: "SF Mono", "Menlo", "Consolas", monospace;
      background:
        radial-gradient(circle at top left, rgba(56, 189, 248, 0.18), transparent 32%),
        linear-gradient(180deg, #030812 0%, var(--bg) 100%);
      color: var(--text);
    }}
    main {{
      width: min(1500px, calc(100vw - 32px));
      margin: 0 auto;
      padding: 28px 0 64px;
    }}
    section {{
      margin-bottom: 24px;
      padding: 24px;
      border: 1px solid var(--border);
      border-radius: 22px;
      background: var(--panel-strong);
    }}
    .grid {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(340px, 1fr));
      gap: 18px;
    }}
    .card {{
      padding: 18px;
      border: 1px solid var(--border);
      border-radius: 20px;
      background: var(--panel);
    }}
    .card-head {{
      margin-bottom: 10px;
    }}
    h1 {{
      margin: 0 0 8px;
      font-size: clamp(28px, 4vw, 42px);
    }}
    h2 {{
      margin: 0 0 8px;
      display: flex;
      gap: 10px;
      flex-wrap: wrap;
      align-items: baseline;
      font-size: 22px;
    }}
    h2 span {{
      color: var(--accent);
      font-size: 13px;
      letter-spacing: 0.06em;
      text-transform: uppercase;
    }}
    h3 {{
      margin: 0 0 12px;
      font-size: 24px;
    }}
    p, pre {{
      color: var(--muted);
      line-height: 1.6;
    }}
    .meta-line {{
      margin: 0;
      color: var(--muted);
      font-size: 13px;
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
    img {{
      width: 100%;
      border-radius: 16px;
      border: 1px solid rgba(148, 163, 184, 0.14);
      display: block;
      background: #07101e;
      margin-top: 14px;
    }}
    .group-label.bull {{ color: var(--bull); }}
    .group-label.bear {{ color: var(--bear); }}
  </style>
</head>
<body>
  <main>
    <section>
      <h1>{escape(title)}</h1>
      <p>Grouped around the 200-day moving average. Bull case names are below 200D MA but held up by short and medium moving averages. Bear case names are above 200D MA but being pressed down by short and medium moving averages.</p>
    </section>
    <section>
      <h3 class="group-label bull">Bull Case</h3>
      <p>{len(bull_entries)} tickers</p>
      <div>{_ticker_pills(bull_entries)}</div>
      <div class="grid">{_cards(bull_entries)}</div>
    </section>
    <section>
      <h3 class="group-label bear">Bear Case</h3>
      <p>{len(bear_entries)} tickers</p>
      <div>{_ticker_pills(bear_entries)}</div>
      <div class="grid">{_cards(bear_entries)}</div>
    </section>
  </main>
</body>
</html>
"""


def main() -> int:
    args = parse_args()
    entries = _load_watchlist(Path(args.watchlist_file))
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "index.html").write_text(build_index(entries, args.title), encoding="utf-8")
    print(f"Wrote grouped index to {output_dir / 'index.html'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
