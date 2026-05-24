#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import subprocess
import sys


PROJECT_ROOT = Path(__file__).resolve().parent.parent
RENDERER_PATH = PROJECT_ROOT / "vendor" / "trade_master_signals" / "render_sector_rotation_rrg.py"
ETF_CATALOG_PATH = PROJECT_ROOT / "config" / "etf_match_catalog.json"

DEFAULT_SECTOR_ETFS = [
    ("Communication Services", "XLC"),
    ("Consumer Discretionary", "XLY"),
    ("Consumer Staples", "XLP"),
    ("Energy", "XLE"),
    ("Financials", "XLF"),
    ("Health Care", "XLV"),
    ("Industrials", "XLI"),
    ("Materials", "XLB"),
    ("Real Estate", "XLRE"),
    ("Information Technology", "XLK"),
    ("Utilities", "XLU"),
]

DEFAULT_INDUSTRY_ETFS = [
    ("Semiconductors", "SOXX"),
    ("Software", "IGV"),
    ("Biotech", "XBI"),
    ("Homebuilders", "XHB"),
    ("Regional Banks", "KRE"),
    ("Retail", "XRT"),
    ("Transportation", "IYT"),
    ("Medical Devices", "IHI"),
    ("Cybersecurity", "CIBR"),
    ("Oil Services", "OIH"),
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Render RRG-style sector, industry, and theme rotation maps.")
    parser.add_argument("--output-dir", help="Directory for rendered files.")
    parser.add_argument("--benchmark", default="SPY")
    parser.add_argument("--period", default="3y")
    parser.add_argument("--trail-weeks", type=int, default=12)
    parser.add_argument("--ratio-window", type=int, default=10)
    parser.add_argument("--momentum-window", type=int, default=4)
    parser.add_argument("--universe", choices=("all", "sector", "industry", "theme"), default="all")
    parser.add_argument("--theme-batch-size", type=int, default=12)
    parser.add_argument("--tickers", nargs="*")
    return parser.parse_args()


def load_etf_catalog() -> list[dict[str, object]]:
    payload = json.loads(ETF_CATALOG_PATH.read_text(encoding="utf-8"))
    if not isinstance(payload, list):
        raise ValueError("ETF catalog must be a JSON list")
    return [item for item in payload if isinstance(item, dict)]


def is_leveraged_theme_etf(item: dict[str, object]) -> bool:
    name = str(item.get("name", "")).lower()
    ticker = str(item.get("ticker", "")).upper()
    if "bull 3x" in name or "bear 3x" in name:
        return True
    return ticker.endswith("I")


def chunked(items: list[tuple[str, str]], size: int) -> list[list[tuple[str, str]]]:
    return [items[index : index + size] for index in range(0, len(items), size)]


def build_theme_universe() -> list[tuple[str, str]]:
    catalog = load_etf_catalog()
    excluded_tickers = {ticker for _, ticker in DEFAULT_SECTOR_ETFS} | {ticker for _, ticker in DEFAULT_INDUSTRY_ETFS}
    theme_entries: list[tuple[str, str]] = []
    seen: set[str] = set()
    for item in catalog:
        ticker = str(item.get("ticker", "")).strip().upper()
        name = str(item.get("name", "")).strip()
        if not ticker or ticker in seen or ticker in excluded_tickers:
            continue
        if is_leveraged_theme_etf(item):
            continue
        seen.add(ticker)
        theme_entries.append((name or ticker, ticker))
    theme_entries.sort(key=lambda current: (current[0].lower(), current[1]))
    return theme_entries


def run_renderer(
    *,
    output_dir: Path,
    benchmark: str,
    period: str,
    trail_weeks: int,
    ratio_window: int,
    momentum_window: int,
    universe: str | None = None,
    tickers: list[str] | None = None,
    tickers_file: Path | None = None,
) -> None:
    command = [
        sys.executable,
        str(RENDERER_PATH),
        "--output-dir",
        str(output_dir),
        "--benchmark",
        benchmark,
        "--period",
        period,
        "--trail-weeks",
        str(trail_weeks),
        "--ratio-window",
        str(ratio_window),
        "--momentum-window",
        str(momentum_window),
    ]
    if universe:
        command.extend(["--universe", universe])
    if tickers:
        command.extend(["--tickers", *tickers])
    if tickers_file:
        command.extend(["--tickers-file", str(tickers_file)])
    subprocess.run(command, check=True)


def render_theme_batches(
    *,
    output_dir: Path,
    benchmark: str,
    period: str,
    trail_weeks: int,
    ratio_window: int,
    momentum_window: int,
    batch_size: int,
) -> list[dict[str, object]]:
    theme_universe = build_theme_universe()
    manifests_dir = output_dir / "_theme_manifests"
    manifests_dir.mkdir(parents=True, exist_ok=True)
    rendered_batches: list[dict[str, object]] = []
    for index, batch in enumerate(chunked(theme_universe, batch_size), start=1):
        batch_slug = f"theme_batch_{index:02d}"
        batch_dir = output_dir / batch_slug
        manifest_path = manifests_dir / f"{batch_slug}.json"
        manifest_path.write_text(
            json.dumps(
                [{"ticker": ticker, "label": label} for label, ticker in batch],
                indent=2,
            ),
            encoding="utf-8",
        )
        run_renderer(
            output_dir=batch_dir,
            benchmark=benchmark,
            period=period,
            trail_weeks=trail_weeks,
            ratio_window=ratio_window,
            momentum_window=momentum_window,
            tickers_file=manifest_path,
        )
        rendered_batches.append(
            {
                "slug": batch_slug,
                "title": f"Theme Batch {index}",
                "index": f"{batch_slug}/index.html",
                "svg": f"{batch_slug}/sector_rrg.svg",
                "count": len(batch),
                "tickers": [ticker for _, ticker in batch],
            }
        )
    return rendered_batches


def render_top_level_index(
    *,
    output_dir: Path,
    benchmark: str,
    period: str,
    trail_weeks: int,
    universe_sections: list[dict[str, str]],
    theme_batches: list[dict[str, object]],
    theme_path_prefix: str = "",
) -> None:
    section_cards = "\n".join(
        f"""
        <article class="card">
          <h2>{section['title']}</h2>
          <p>{section['description']}</p>
          <a class="button" href="{section['index']}">Open {section['title']}</a>
          <img src="{section['svg']}" alt="{section['title']}" loading="lazy" />
        </article>
        """
        for section in universe_sections
    )
    theme_cards = "\n".join(
        f"""
        <article class="theme-card">
          <h3>{batch['title']} <span>{batch['count']} ETFs</span></h3>
          <a class="button" href="{theme_path_prefix}{batch['index']}">Open batch</a>
          <img src="{theme_path_prefix}{batch['svg']}" alt="{batch['title']}" loading="lazy" />
        </article>
        """
        for batch in theme_batches
    )
    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Market Rotation Maps</title>
  <style>
    :root {{
      color-scheme: dark;
      --bg: #06101d;
      --panel: rgba(15, 23, 42, 0.88);
      --border: rgba(148, 163, 184, 0.18);
      --text: #e2e8f0;
      --muted: #94a3b8;
      --accent: #38bdf8;
    }}
    body {{
      margin: 0;
      font-family: "SF Mono", "Menlo", "Consolas", monospace;
      background:
        radial-gradient(circle at top left, rgba(56, 189, 248, 0.18), transparent 30%),
        linear-gradient(180deg, #050c16 0%, var(--bg) 100%);
      color: var(--text);
    }}
    main {{
      width: min(1520px, calc(100vw - 32px));
      margin: 0 auto;
      padding: 32px 0 64px;
    }}
    section {{
      padding: 24px;
      border: 1px solid var(--border);
      border-radius: 24px;
      background: var(--panel);
      margin-bottom: 24px;
    }}
    p {{
      color: var(--muted);
      line-height: 1.6;
    }}
    .grid {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(340px, 1fr));
      gap: 18px;
    }}
    .card, .theme-card {{
      padding: 18px;
      border: 1px solid var(--border);
      border-radius: 20px;
      background: rgba(10, 18, 32, 0.92);
    }}
    .card h2, .theme-card h3 {{
      margin: 0 0 12px;
      display: flex;
      justify-content: space-between;
      gap: 12px;
      align-items: baseline;
    }}
    .theme-card h3 span {{
      color: var(--muted);
      font-size: 12px;
      text-transform: uppercase;
      letter-spacing: 0.08em;
    }}
    img {{
      width: 100%;
      border-radius: 16px;
      border: 1px solid rgba(148, 163, 184, 0.14);
      display: block;
      background: #07101e;
      margin-top: 14px;
    }}
    .button {{
      display: inline-block;
      padding: 10px 14px;
      border-radius: 999px;
      text-decoration: none;
      background: rgba(56, 189, 248, 0.16);
      color: #7dd3fc;
      border: 1px solid rgba(56, 189, 248, 0.24);
    }}
  </style>
</head>
<body>
  <main>
    <section>
      <h1>Market Rotation Maps</h1>
      <p>Daily-refreshed RRG report using weekly relative rotation math versus {benchmark}. Weekly trails are more stable for sector and thematic leadership than daily RRG swings, so this report updates once per day but keeps the chart model weekly. Period: {period}. Trail: {trail_weeks} weeks.</p>
    </section>
    <section>
      <h2>Core Universes</h2>
      <div class="grid">
        {section_cards}
      </div>
    </section>
    <section>
      <h2>Theme Batches</h2>
      <p>The theme ETF set is too large to read cleanly on one canvas, so it is split into multiple smaller RRG charts.</p>
      <div class="grid">
        {theme_cards}
      </div>
    </section>
  </main>
</body>
</html>
"""
    (output_dir / "index.html").write_text(html, encoding="utf-8")


def main() -> int:
    args = parse_args()
    os.environ.setdefault("MPLBACKEND", "Agg")
    os.environ.setdefault("MPLCONFIGDIR", "/tmp/mpl")

    output_dir = (
        Path(args.output_dir).resolve()
        if args.output_dir
        else PROJECT_ROOT / "artifacts" / "output" / "sector_rotation_rrg"
    )
    output_dir.mkdir(parents=True, exist_ok=True)

    if args.tickers:
        run_renderer(
            output_dir=output_dir,
            benchmark=args.benchmark,
            period=args.period,
            trail_weeks=args.trail_weeks,
            ratio_window=args.ratio_window,
            momentum_window=args.momentum_window,
            tickers=args.tickers,
        )
        print(f"Rendered RRG output to {output_dir}")
        return 0

    if args.universe in {"sector", "industry"}:
        run_renderer(
            output_dir=output_dir,
            benchmark=args.benchmark,
            period=args.period,
            trail_weeks=args.trail_weeks,
            ratio_window=args.ratio_window,
            momentum_window=args.momentum_window,
            universe=args.universe,
        )
        print(f"Rendered RRG output to {output_dir}")
        return 0

    if args.universe == "theme":
        theme_batches = render_theme_batches(
            output_dir=output_dir,
            benchmark=args.benchmark,
            period=args.period,
            trail_weeks=args.trail_weeks,
            ratio_window=args.ratio_window,
            momentum_window=args.momentum_window,
            batch_size=max(4, args.theme_batch_size),
        )
        render_top_level_index(
            output_dir=output_dir,
            benchmark=args.benchmark,
            period=args.period,
            trail_weeks=args.trail_weeks,
            universe_sections=[],
            theme_batches=theme_batches,
        )
        print(f"Rendered theme RRG output to {output_dir}")
        return 0

    sector_dir = output_dir / "sector"
    industry_dir = output_dir / "industry"
    theme_dir = output_dir / "theme"

    run_renderer(
        output_dir=sector_dir,
        benchmark=args.benchmark,
        period=args.period,
        trail_weeks=args.trail_weeks,
        ratio_window=args.ratio_window,
        momentum_window=args.momentum_window,
        universe="sector",
    )
    run_renderer(
        output_dir=industry_dir,
        benchmark=args.benchmark,
        period=args.period,
        trail_weeks=args.trail_weeks,
        ratio_window=args.ratio_window,
        momentum_window=args.momentum_window,
        universe="industry",
    )
    theme_batches = render_theme_batches(
        output_dir=theme_dir,
        benchmark=args.benchmark,
        period=args.period,
        trail_weeks=args.trail_weeks,
        ratio_window=args.ratio_window,
        momentum_window=args.momentum_window,
        batch_size=max(4, args.theme_batch_size),
    )

    render_top_level_index(
        output_dir=output_dir,
        benchmark=args.benchmark,
        period=args.period,
        trail_weeks=args.trail_weeks,
        universe_sections=[
            {
                "title": "Sector Rotation",
                "description": "Official 11 sector ETFs, rendered as one weekly RRG plus single-sector views.",
                "index": "sector/index.html",
                "svg": "sector/sector_rrg.svg",
            },
            {
                "title": "Industry Rotation",
                "description": "Focused industry ETF basket for tactical leadership checks.",
                "index": "industry/index.html",
                "svg": "industry/sector_rrg.svg",
            },
        ],
        theme_batches=theme_batches,
        theme_path_prefix="theme/",
    )
    print(f"Rendered RRG output to {output_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
