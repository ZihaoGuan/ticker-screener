#!/usr/bin/env python3
from __future__ import annotations

import argparse
from dataclasses import dataclass
from html import escape
import json
from pathlib import Path

import pandas as pd
import requests


YAHOO_HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "application/json",
}

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

SERIES_COLORS = [
    "#38bdf8",
    "#22c55e",
    "#f59e0b",
    "#ef4444",
    "#a78bfa",
    "#14b8a6",
    "#f472b6",
    "#eab308",
    "#60a5fa",
    "#fb7185",
    "#34d399",
]


@dataclass
class RotationSeries:
    label: str
    ticker: str
    color: str
    trail: pd.DataFrame
    latest_x: float
    latest_y: float
    quadrant: str
    distance: float


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Render a sector rotation RRG-style chart.")
    parser.add_argument("--output-dir", required=True, help="Directory for the SVG, HTML, and summary artifacts.")
    parser.add_argument("--benchmark", default="SPY", help="Benchmark ticker, such as SPY or QQQ.")
    parser.add_argument("--period", default="3y", help="Yahoo daily history range, such as 2y, 3y, or 5y.")
    parser.add_argument("--trail-weeks", type=int, default=12, help="Number of weekly trail points to show.")
    parser.add_argument("--ratio-window", type=int, default=10, help="Weeks used to normalize the relative trend axis.")
    parser.add_argument("--momentum-window", type=int, default=4, help="Weeks used to normalize the relative momentum axis.")
    parser.add_argument("--universe", choices=["sector", "industry"], default="sector", help="Use the official 11 sector ETFs or a more tactical industry ETF basket.")
    parser.add_argument("--tickers", nargs="*", help="Optional ETF tickers to use instead of the default ETF universe.")
    return parser.parse_args()


def fetch_history(ticker: str, period: str) -> pd.DataFrame:
    response = requests.get(
        f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}",
        params={
            "interval": "1d",
            "range": period,
            "includeAdjustedClose": "true",
        },
        headers=YAHOO_HEADERS,
        timeout=30,
    )
    response.raise_for_status()
    payload = response.json()
    result = payload.get("chart", {}).get("result", [])
    if not result:
        error = payload.get("chart", {}).get("error")
        raise ValueError(f"No Yahoo chart result returned for {ticker}: {error}")

    chart = result[0]
    timestamps = chart.get("timestamp", [])
    quote_list = chart.get("indicators", {}).get("quote", [])
    if not timestamps or not quote_list:
        raise ValueError(f"Incomplete Yahoo chart payload for {ticker}")

    quote = quote_list[0]
    history = pd.DataFrame(
        {"Close": quote.get("close", [])},
        index=pd.to_datetime(timestamps, unit="s", utc=True),
    )
    exchange_timezone = chart.get("meta", {}).get("exchangeTimezoneName")
    if exchange_timezone:
        history.index = history.index.tz_convert(exchange_timezone).normalize().tz_localize(None)
    else:
        history.index = history.index.tz_localize(None)
    history = history.dropna(subset=["Close"]).copy()
    if history.empty:
        raise ValueError(f"No usable price history returned for {ticker}")
    return history


def to_weekly_close(history: pd.DataFrame) -> pd.Series:
    return history["Close"].resample("W-FRI").last().dropna()


def compute_rotation_series(
    label: str,
    ticker: str,
    color: str,
    closes: pd.DataFrame,
    benchmark: str,
    ratio_window: int,
    momentum_window: int,
    trail_weeks: int,
) -> RotationSeries | None:
    relative = closes[ticker] / closes[benchmark]
    rs_ratio = 100.0 * relative / relative.rolling(ratio_window).mean()
    rs_ratio = rs_ratio.rolling(2).mean()
    rs_momentum = 100.0 * rs_ratio / rs_ratio.rolling(momentum_window).mean()
    rs_momentum = rs_momentum.rolling(2).mean()
    frame = pd.DataFrame({"x": rs_ratio, "y": rs_momentum}).dropna()
    if len(frame) < trail_weeks:
        return None
    trail = frame.tail(trail_weeks).copy()
    latest_x = float(trail["x"].iloc[-1])
    latest_y = float(trail["y"].iloc[-1])
    if latest_x >= 100 and latest_y >= 100:
        quadrant = "Leading"
    elif latest_x >= 100 and latest_y < 100:
        quadrant = "Weakening"
    elif latest_x < 100 and latest_y < 100:
        quadrant = "Lagging"
    else:
        quadrant = "Improving"
    distance = ((latest_x - 100.0) ** 2 + (latest_y - 100.0) ** 2) ** 0.5
    return RotationSeries(
        label=label,
        ticker=ticker,
        color=color,
        trail=trail,
        latest_x=latest_x,
        latest_y=latest_y,
        quadrant=quadrant,
        distance=distance,
    )


def scale_x(value: float, min_value: float, max_value: float, left: int, width: int) -> float:
    span = max(max_value - min_value, 1e-6)
    return left + ((value - min_value) / span) * width


def scale_y(value: float, min_value: float, max_value: float, top: int, height: int) -> float:
    span = max(max_value - min_value, 1e-6)
    return top + height - ((value - min_value) / span) * height


def render_rrg_chart(
    series_list: list[RotationSeries],
    output_path: Path,
    benchmark: str,
    trail_weeks: int,
    ratio_window: int,
    momentum_window: int,
    universe_label: str,
) -> None:
    width = 1560
    height = 980
    left = 100
    top = 110
    plot_width = 920
    plot_height = 720
    right_panel_x = 1080

    all_x = [100.0]
    all_y = [100.0]
    for item in series_list:
        all_x.extend(item.trail["x"].tolist())
        all_y.extend(item.trail["y"].tolist())
    x_min = min(all_x) - 1.5
    x_max = max(all_x) + 1.5
    y_min = min(all_y) - 1.5
    y_max = max(all_y) + 1.5

    x_axis = scale_x(100.0, x_min, x_max, left, plot_width)
    y_axis = scale_y(100.0, y_min, y_max, top, plot_height)

    svg: list[str] = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}">',
        '<rect width="100%" height="100%" fill="#07101e" />',
        f'<text x="{left}" y="38" fill="#f8fafc" font-size="28" font-family="Menlo, Consolas, monospace">{escape(universe_label)} Rotation Map</text>',
        f'<text x="{left}" y="64" fill="#94a3b8" font-size="15" font-family="Menlo, Consolas, monospace">RRG-style weekly chart | benchmark: {escape(benchmark)} | trail: {trail_weeks} weeks</text>',
    ]

    quadrant_rects = [
        (x_axis, top, left + plot_width - x_axis, y_axis - top, "#14532d", "Leading"),
        (x_axis, y_axis, left + plot_width - x_axis, top + plot_height - y_axis, "#4d3b00", "Weakening"),
        (left, y_axis, x_axis - left, top + plot_height - y_axis, "#4c0519", "Lagging"),
        (left, top, x_axis - left, y_axis - top, "#0c4a6e", "Improving"),
    ]
    for x, y, rect_width, rect_height, color, _ in quadrant_rects:
        svg.append(
            f'<rect x="{x:.1f}" y="{y:.1f}" width="{rect_width:.1f}" height="{rect_height:.1f}" fill="{color}" opacity="0.16" />'
        )

    for step in range(6):
        grid_x = left + step * (plot_width / 5)
        grid_y = top + step * (plot_height / 5)
        x_value = x_min + step * ((x_max - x_min) / 5)
        y_value = y_max - step * ((y_max - y_min) / 5)
        svg.append(f'<line x1="{grid_x:.1f}" y1="{top}" x2="{grid_x:.1f}" y2="{top + plot_height}" stroke="#1e293b" stroke-width="1" />')
        svg.append(f'<line x1="{left}" y1="{grid_y:.1f}" x2="{left + plot_width}" y2="{grid_y:.1f}" stroke="#1e293b" stroke-width="1" />')
        svg.append(
            f'<text x="{grid_x:.1f}" y="{top + plot_height + 24}" fill="#94a3b8" font-size="12" text-anchor="middle" font-family="Menlo, Consolas, monospace">{x_value:.1f}</text>'
        )
        svg.append(
            f'<text x="{left - 14}" y="{grid_y + 4:.1f}" fill="#94a3b8" font-size="12" text-anchor="end" font-family="Menlo, Consolas, monospace">{y_value:.1f}</text>'
        )

    svg.append(f'<line x1="{x_axis:.1f}" y1="{top}" x2="{x_axis:.1f}" y2="{top + plot_height}" stroke="#f8fafc" stroke-width="1.5" opacity="0.65" />')
    svg.append(f'<line x1="{left}" y1="{y_axis:.1f}" x2="{left + plot_width}" y2="{y_axis:.1f}" stroke="#f8fafc" stroke-width="1.5" opacity="0.65" />')
    svg.append(f'<text x="{x_axis + 10:.1f}" y="{top + 18}" fill="#86efac" font-size="14" font-family="Menlo, Consolas, monospace">Leading</text>')
    svg.append(f'<text x="{x_axis + 10:.1f}" y="{top + plot_height - 12}" fill="#fde68a" font-size="14" font-family="Menlo, Consolas, monospace">Weakening</text>')
    svg.append(f'<text x="{left + 10}" y="{top + plot_height - 12}" fill="#fca5a5" font-size="14" font-family="Menlo, Consolas, monospace">Lagging</text>')
    svg.append(f'<text x="{left + 10}" y="{top + 18}" fill="#7dd3fc" font-size="14" font-family="Menlo, Consolas, monospace">Improving</text>')
    svg.append(f'<text x="{left + plot_width / 2:.1f}" y="{top + plot_height + 52}" fill="#cbd5e1" font-size="14" text-anchor="middle" font-family="Menlo, Consolas, monospace">Relative trend axis (normalized, centered on 100)</text>')
    svg.append(
        f'<text x="{left - 76}" y="{top + plot_height / 2:.1f}" fill="#cbd5e1" font-size="14" text-anchor="middle" transform="rotate(-90 {left - 76},{top + plot_height / 2:.1f})" font-family="Menlo, Consolas, monospace">Relative momentum axis (normalized, centered on 100)</text>'
    )

    for item in series_list:
        coords: list[tuple[float, float]] = []
        for index, (_, row) in enumerate(item.trail.iterrows()):
            x = scale_x(float(row["x"]), x_min, x_max, left, plot_width)
            y = scale_y(float(row["y"]), y_min, y_max, top, plot_height)
            coords.append((x, y))
            opacity = 0.16 + 0.70 * ((index + 1) / len(item.trail))
            radius = 1.8 + 2.6 * ((index + 1) / len(item.trail))
            svg.append(f'<circle cx="{x:.1f}" cy="{y:.1f}" r="{radius:.1f}" fill="{item.color}" opacity="{opacity:.2f}" />')
        for index in range(1, len(coords)):
            x1, y1 = coords[index - 1]
            x2, y2 = coords[index]
            progress = index / (len(coords) - 1)
            opacity = 0.18 + 0.72 * progress
            stroke_width = 1.0 + 2.8 * progress
            svg.append(
                f'<line x1="{x1:.1f}" y1="{y1:.1f}" x2="{x2:.1f}" y2="{y2:.1f}" stroke="{item.color}" stroke-width="{stroke_width:.2f}" stroke-linecap="round" opacity="{opacity:.2f}" />'
            )
        latest_x, latest_y = coords[-1]
        svg.append(f'<circle cx="{latest_x:.1f}" cy="{latest_y:.1f}" r="6.2" fill="{item.color}" stroke="#f8fafc" stroke-width="1.2" />')
        svg.append(
            f'<text x="{latest_x + 8:.1f}" y="{latest_y - 8:.1f}" fill="{item.color}" font-size="13" font-family="Menlo, Consolas, monospace">{escape(item.ticker)} · {escape(item.label)}</text>'
        )

    info_lines = [
        "RRG-style note:",
        "This chart follows the RRG concept but uses an open, internal normalization model.",
        f"Trend axis: weekly ETF/{benchmark} ratio vs {ratio_window}w trend baseline.",
        f"Momentum axis: trend axis vs {momentum_window}w momentum baseline.",
    ]
    for index, line in enumerate(info_lines):
        color = "#38bdf8" if index == 0 else "#94a3b8"
        font_size = 14 if index == 0 else 13
        svg.append(
            f'<text x="{right_panel_x}" y="{120 + index * 22}" fill="{color}" font-size="{font_size}" font-family="Menlo, Consolas, monospace">{escape(line)}</text>'
        )

    quadrant_rank = {"Leading": 0, "Improving": 1, "Weakening": 2, "Lagging": 3}
    table_header_y = 238
    svg.append(f'<text x="{right_panel_x}" y="{table_header_y}" fill="#f8fafc" font-size="15" font-family="Menlo, Consolas, monospace">Latest {escape(universe_label)} Positions</text>')
    for index, item in enumerate(sorted(series_list, key=lambda current: (quadrant_rank[current.quadrant], -current.distance, -current.latest_x, -current.latest_y))):
        y = table_header_y + 30 + index * 24
        svg.append(f'<circle cx="{right_panel_x + 6}" cy="{y - 4:.1f}" r="5" fill="{item.color}" />')
        svg.append(
            f'<text x="{right_panel_x + 18}" y="{y}" fill="#e2e8f0" font-size="13" font-family="Menlo, Consolas, monospace">{escape(item.ticker)} · {escape(item.label)} | {escape(item.quadrant)} | d {item.distance:.1f}</text>'
        )

    svg.append("</svg>")
    output_path.write_text("\n".join(svg))


def render_index(
    output_path: Path,
    svg_name: str,
    benchmark: str,
    trail_weeks: int,
    universe_label: str,
    single_chart_entries: list[tuple[str, str]] | None = None,
) -> None:
    cards = ""
    if single_chart_entries:
        cards = "".join(
            f"""
            <article class="card">
              <h2>{escape(label)} <span>{escape(ticker)}</span></h2>
              <img src="charts/{escape(ticker)}.svg" alt="{escape(label)} rotation map" loading="lazy" />
            </article>
            """
            for ticker, label in single_chart_entries
        )
    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>{escape(universe_label)} Rotation Map</title>
  <style>
    :root {{
      color-scheme: dark;
      --bg: #06101d;
      --panel: rgba(15, 23, 42, 0.88);
      --border: rgba(148, 163, 184, 0.18);
      --text: #e2e8f0;
      --muted: #94a3b8;
    }}
    body {{
      margin: 0;
      font-family: "SF Mono", "Menlo", "Consolas", monospace;
      background:
        radial-gradient(circle at top left, rgba(56, 189, 248, 0.16), transparent 32%),
        linear-gradient(180deg, #050c16 0%, var(--bg) 100%);
      color: var(--text);
    }}
    main {{
      width: min(1500px, calc(100vw - 32px));
      margin: 0 auto;
      padding: 32px 0 64px;
    }}
    section {{
      padding: 24px;
      border: 1px solid var(--border);
      border-radius: 22px;
      background: var(--panel);
      margin-bottom: 22px;
    }}
    p {{
      color: var(--muted);
      line-height: 1.6;
      max-width: 980px;
    }}
    img {{
      width: 100%;
      border-radius: 18px;
      border: 1px solid rgba(148, 163, 184, 0.14);
      background: #07101e;
      display: block;
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
      background: rgba(10, 18, 32, 0.92);
    }}
    .card h2 {{
      margin: 0 0 14px;
      display: flex;
      gap: 10px;
      flex-wrap: wrap;
      align-items: baseline;
      font-size: 20px;
    }}
    .card h2 span {{
      color: var(--muted);
      font-size: 13px;
      letter-spacing: 0.06em;
      text-transform: uppercase;
    }}
  </style>
</head>
<body>
  <main>
    <section>
      <h1>{escape(universe_label)} Rotation Map</h1>
      <p>RRG-style weekly sector rotation chart versus {escape(benchmark)} with {trail_weeks}-week trails. This renderer uses an internal normalized relative trend and momentum model, not the proprietary JdK implementation.</p>
      <img src="{escape(svg_name)}" alt="Sector rotation map" />
    </section>
    <section>
      <h1>Single Industry Views</h1>
      <p>Each industry also gets a standalone chart using the same benchmark and trail settings so you can inspect its path without the full-universe clutter.</p>
      <div class="grid">
        {cards}
      </div>
    </section>
  </main>
</body>
</html>
"""
    output_path.write_text(html)


def main() -> int:
    args = parse_args()
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    if args.tickers:
        universe = [(ticker, ticker) for ticker in args.tickers]
        universe_label = "Custom ETF"
    elif args.universe == "industry":
        universe = DEFAULT_INDUSTRY_ETFS
        universe_label = "Industry"
    else:
        universe = DEFAULT_SECTOR_ETFS
        universe_label = "Sector"

    benchmark_history = to_weekly_close(fetch_history(args.benchmark, args.period)).rename(args.benchmark)
    weekly_closes: dict[str, pd.Series] = {args.benchmark: benchmark_history}
    failures: dict[str, str] = {}

    for _, ticker in universe:
        try:
            weekly_closes[ticker] = to_weekly_close(fetch_history(ticker, args.period)).rename(ticker)
        except Exception as exc:
            failures[ticker] = str(exc)

    close_frame = pd.concat(weekly_closes.values(), axis=1, join="inner").dropna()
    series_list: list[RotationSeries] = []
    for index, (label, ticker) in enumerate(universe):
        if ticker not in close_frame.columns:
            continue
        series = compute_rotation_series(
            label=label,
            ticker=ticker,
            color=SERIES_COLORS[index % len(SERIES_COLORS)],
            closes=close_frame,
            benchmark=args.benchmark,
            ratio_window=args.ratio_window,
            momentum_window=args.momentum_window,
            trail_weeks=args.trail_weeks,
        )
        if series is not None:
            series_list.append(series)

    if not series_list:
        raise ValueError("No sector series could be rendered after normalization.")

    svg_path = output_dir / "sector_rrg.svg"
    single_chart_dir = output_dir / "charts"
    single_chart_dir.mkdir(parents=True, exist_ok=True)
    render_rrg_chart(
        series_list=series_list,
        output_path=svg_path,
        benchmark=args.benchmark,
        trail_weeks=args.trail_weeks,
        ratio_window=args.ratio_window,
        momentum_window=args.momentum_window,
        universe_label=universe_label,
    )
    for item in series_list:
        render_rrg_chart(
            series_list=[item],
            output_path=single_chart_dir / f"{item.ticker}.svg",
            benchmark=args.benchmark,
            trail_weeks=args.trail_weeks,
            ratio_window=args.ratio_window,
            momentum_window=args.momentum_window,
            universe_label=f"{item.label} ({item.ticker})",
        )
    render_index(
        output_dir / "index.html",
        svg_path.name,
        args.benchmark,
        args.trail_weeks,
        universe_label,
        single_chart_entries=[(item.ticker, item.label) for item in series_list],
    )

    summary = {
        "universe": universe_label,
        "benchmark": args.benchmark,
        "trail_weeks": args.trail_weeks,
        "ratio_window": args.ratio_window,
        "momentum_window": args.momentum_window,
        "rendered": [
            {
                "label": item.label,
                "ticker": item.ticker,
                "quadrant": item.quadrant,
                "distance": round(item.distance, 2),
                "x": round(item.latest_x, 2),
                "y": round(item.latest_y, 2),
            }
            for item in series_list
        ],
        "failed": failures,
    }
    (output_dir / "run_summary.json").write_text(json.dumps(summary, indent=2))
    print(f"Wrote sector rotation output to {output_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
