#!/usr/bin/env python3
from __future__ import annotations

import argparse
import datetime as dt
import json
from html import escape
from pathlib import Path
import statistics
import sys
from typing import Any

import pandas as pd
import yfinance as yf


PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.config import today_label


HORIZONS = (
    ("1w", 5),
    ("2w", 10),
    ("3w", 15),
    ("4w", 20),
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Backtest overlap summary signals using forward 1/2/3/4 week holding periods."
    )
    parser.add_argument(
        "--overlap-dir",
        default=str(PROJECT_ROOT / "artifacts" / "raw" / "overlap_history"),
        help="Directory containing daily_overlap_summary_YYYY-MM-DD.json files.",
    )
    parser.add_argument("--start-date", default="", help="Inclusive start date (YYYY-MM-DD).")
    parser.add_argument("--end-date", default=today_label(), help="Inclusive end date (YYYY-MM-DD).")
    parser.add_argument(
        "--lookback-days",
        type=int,
        default=180,
        help="Used when start-date is omitted; backtest from end-date minus this many days.",
    )
    parser.add_argument(
        "--min-overlap-count",
        type=int,
        default=4,
        help="Minimum overlap pipeline count required to treat a ticker as a buy signal.",
    )
    parser.add_argument(
        "--output-json",
        default="",
        help="Optional explicit JSON output path.",
    )
    parser.add_argument(
        "--output-html",
        default="",
        help="Optional explicit HTML output path.",
    )
    parser.add_argument(
        "--price-fixture",
        default="",
        help="Optional local JSON price fixture for offline validation.",
    )
    return parser.parse_args()


def _parse_date(value: str) -> dt.date:
    return dt.date.fromisoformat(value)


def _discover_overlap_files(overlap_dir: Path, start_date: dt.date, end_date: dt.date) -> list[Path]:
    files: list[Path] = []
    for path in sorted(overlap_dir.glob("daily_overlap_summary_*.json")):
        suffix = path.stem.removeprefix("daily_overlap_summary_")
        try:
            date_value = _parse_date(suffix)
        except ValueError:
            continue
        if start_date <= date_value <= end_date:
            files.append(path)
    return files


def _load_signals(paths: list[Path], min_overlap_count: int) -> tuple[list[dict[str, Any]], set[str]]:
    signal_days: list[dict[str, Any]] = []
    tickers: set[str] = set()
    for path in paths:
        payload = json.loads(path.read_text(encoding="utf-8"))
        signal_date = str(payload.get("date_label", ""))
        entries = payload.get("overlap_two_plus", [])
        selected = []
        for item in entries:
            if not isinstance(item, dict):
                continue
            pipeline_count = int(item.get("pipeline_count", 0) or 0)
            if pipeline_count < min_overlap_count:
                continue
            ticker = str(item.get("ticker", "")).strip().upper()
            if not ticker:
                continue
            selected.append(
                {
                    "ticker": ticker,
                    "pipeline_count": pipeline_count,
                    "pipeline_labels": list(item.get("pipeline_labels", [])),
                    "theme_tags": list(item.get("theme_tags", [])),
                    "is_drug_ticker": bool(item.get("is_drug_ticker", False)),
                }
            )
            tickers.add(ticker)
        signal_days.append(
            {
                "date_label": signal_date,
                "source_file": path.name,
                "signals": selected,
            }
        )
    return signal_days, tickers


def _load_fixture_prices(path: Path) -> dict[str, pd.Series]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    prices: dict[str, pd.Series] = {}
    for ticker, rows in payload.items():
        if not isinstance(rows, dict):
            continue
        series = pd.Series({pd.Timestamp(k): float(v) for k, v in rows.items()})
        series = series.sort_index()
        if not series.empty:
            prices[str(ticker).upper()] = series
    return prices


def _download_prices(tickers: set[str], start_date: dt.date, end_date: dt.date) -> dict[str, pd.Series]:
    if not tickers:
        return {}
    tickers_list = sorted(tickers)
    prices: dict[str, pd.Series] = {}
    download_start = (start_date - dt.timedelta(days=10)).isoformat()
    download_end = (end_date + dt.timedelta(days=35)).isoformat()
    chunk_size = 100
    for index in range(0, len(tickers_list), chunk_size):
        chunk = tickers_list[index : index + chunk_size]
        print(f"Downloading prices for tickers {index + 1}-{index + len(chunk)} / {len(tickers_list)}")
        data = yf.download(
            tickers=chunk,
            start=download_start,
            end=download_end,
            interval="1d",
            auto_adjust=True,
            progress=False,
            group_by="ticker",
            threads=False,
        )
        if data is None or data.empty:
            continue

        if isinstance(data.columns, pd.MultiIndex):
            for ticker in chunk:
                if ticker not in data.columns.get_level_values(0):
                    continue
                frame = data[ticker]
                close_col = "Close" if "Close" in frame.columns else frame.columns[0]
                series = frame[close_col].dropna()
                if not series.empty:
                    prices[ticker] = series
        else:
            close_col = "Close" if "Close" in data.columns else data.columns[0]
            series = data[close_col].dropna()
            if chunk:
                prices[chunk[0]] = series
    return prices


def _resolve_entry(series: pd.Series, signal_date: dt.date) -> tuple[pd.Timestamp | None, float | None, int | None]:
    if series.empty:
        return None, None, None
    target = pd.Timestamp(signal_date)
    later = series[series.index >= target]
    if later.empty:
        return None, None, None
    entry_date = later.index[0]
    entry_position = series.index.get_loc(entry_date)
    if isinstance(entry_position, slice):
        entry_position = entry_position.start
    return entry_date, float(later.iloc[0]), int(entry_position)


def _compute_trade(
    signal_date: dt.date,
    ticker: str,
    pipeline_count: int,
    series: pd.Series,
    theme_tags: list[str],
    is_drug_ticker: bool,
) -> dict[str, Any] | None:
    entry_date, entry_price, entry_position = _resolve_entry(series, signal_date)
    if entry_date is None or entry_price is None or entry_position is None:
        return None

    returns: dict[str, float | None] = {}
    exit_dates: dict[str, str | None] = {}
    for label, bars in HORIZONS:
        exit_position = entry_position + bars
        if exit_position >= len(series):
            returns[label] = None
            exit_dates[label] = None
            continue
        exit_date = series.index[exit_position]
        exit_price = float(series.iloc[exit_position])
        returns[label] = ((exit_price / entry_price) - 1.0) * 100.0
        exit_dates[label] = exit_date.date().isoformat()

    return {
        "signal_date": signal_date.isoformat(),
        "ticker": ticker,
        "pipeline_count": pipeline_count,
        "theme_tags": theme_tags,
        "is_drug_ticker": is_drug_ticker,
        "entry_date": entry_date.date().isoformat(),
        "entry_price": entry_price,
        "returns_pct": returns,
        "exit_dates": exit_dates,
    }


def _summarize_returns(values: list[float]) -> dict[str, float | int | None]:
    if not values:
        return {
            "avg_return_pct": None,
            "median_return_pct": None,
            "win_rate_pct": None,
            "sample_count": 0,
        }
    wins = sum(1 for value in values if value > 0)
    return {
        "avg_return_pct": round(sum(values) / len(values), 2),
        "median_return_pct": round(statistics.median(values), 2),
        "win_rate_pct": round((wins / len(values)) * 100.0, 1),
        "sample_count": len(values),
    }


def _build_payload(signal_days: list[dict[str, Any]], prices_by_ticker: dict[str, pd.Series], min_overlap_count: int) -> dict[str, Any]:
    date_rows: list[dict[str, Any]] = []
    trades: list[dict[str, Any]] = []
    overall_buckets: dict[str, list[float]] = {label: [] for label, _ in HORIZONS}
    grouped_buckets: dict[str, dict[str, list[float]]] = {
        "drug_buys": {label: [] for label, _ in HORIZONS},
        "non_drug_buys": {label: [] for label, _ in HORIZONS},
    }

    for signal_day in signal_days:
        signal_date = _parse_date(signal_day["date_label"])
        day_trades: list[dict[str, Any]] = []
        for item in signal_day["signals"]:
            ticker = item["ticker"]
            series = prices_by_ticker.get(ticker)
            if series is None:
                continue
            trade = _compute_trade(
                signal_date,
                ticker,
                int(item["pipeline_count"]),
                series,
                list(item.get("theme_tags", [])),
                bool(item.get("is_drug_ticker", False)),
            )
            if trade is None:
                continue
            day_trades.append(trade)
            trades.append(trade)
            group_key = "drug_buys" if trade["is_drug_ticker"] else "non_drug_buys"
            for label, _bars in HORIZONS:
                value = trade["returns_pct"][label]
                if value is not None:
                    overall_buckets[label].append(float(value))
                    grouped_buckets[group_key][label].append(float(value))

        row_summary: dict[str, Any] = {
            "signal_date": signal_day["date_label"],
            "source_file": signal_day["source_file"],
            "signal_count": len(signal_day["signals"]),
            "filled_trade_count": len(day_trades),
            "tickers": [item["ticker"] for item in signal_day["signals"]],
            "trades": day_trades,
            "returns": {},
        }
        for label, _bars in HORIZONS:
            values = [float(trade["returns_pct"][label]) for trade in day_trades if trade["returns_pct"][label] is not None]
            row_summary["returns"][label] = _summarize_returns(values)
        date_rows.append(row_summary)

    overall_summary = {label: _summarize_returns(values) for label, values in overall_buckets.items()}
    grouped_summary = {
        group_key: {label: _summarize_returns(values) for label, values in horizon_map.items()}
        for group_key, horizon_map in grouped_buckets.items()
    }

    return {
        "generated_at": dt.datetime.now(dt.timezone.utc).isoformat(),
        "min_overlap_count": min_overlap_count,
        "signal_days": date_rows,
        "trades": trades,
        "overall_summary": overall_summary,
        "grouped_summary": grouped_summary,
        "signal_day_count": len(date_rows),
        "trade_count": len(trades),
        "unique_ticker_count": len({trade["ticker"] for trade in trades}),
    }


def _fmt_pct(value: float | None) -> str:
    if value is None:
        return "-"
    return f"{value:+.2f}%"


def _fmt_num(value: float | int | None) -> str:
    if value is None:
        return "-"
    if isinstance(value, int):
        return str(value)
    return f"{value:.1f}"


def _build_html(payload: dict[str, Any], start_date: str, end_date: str) -> str:
    overall_rows = []
    for label, _bars in HORIZONS:
        summary = payload["overall_summary"][label]
        overall_rows.append(
            f"""
            <tr>
              <td>{escape(label.upper())}</td>
              <td>{_fmt_pct(summary['avg_return_pct'])}</td>
              <td>{_fmt_pct(summary['median_return_pct'])}</td>
              <td>{_fmt_num(summary['win_rate_pct'])}%</td>
              <td>{int(summary['sample_count'])}</td>
            </tr>
            """
        )

    grouped_sections = []
    for group_key, title in (
        ("drug_buys", "Drug Buy Returns"),
        ("non_drug_buys", "Non-Drug Buy Returns"),
    ):
        rows = []
        for label, _bars in HORIZONS:
            summary = payload.get("grouped_summary", {}).get(group_key, {}).get(label, {})
            rows.append(
                f"""
                <tr>
                  <td>{escape(label.upper())}</td>
                  <td>{_fmt_pct(summary.get('avg_return_pct'))}</td>
                  <td>{_fmt_pct(summary.get('median_return_pct'))}</td>
                  <td>{_fmt_num(summary.get('win_rate_pct'))}%</td>
                  <td>{int(summary.get('sample_count', 0) or 0)}</td>
                </tr>
                """
            )
        grouped_sections.append(
            f"""
            <section>
              <h2>{escape(title)}</h2>
              <table>
                <thead>
                  <tr>
                    <th>Horizon</th>
                    <th>Average</th>
                    <th>Median</th>
                    <th>Win rate</th>
                    <th>Samples</th>
                  </tr>
                </thead>
                <tbody>
                  {''.join(rows) or '<tr><td colspan="5">No data.</td></tr>'}
                </tbody>
              </table>
            </section>
            """
        )

    date_rows = []
    for row in payload["signal_days"]:
        links = ", ".join(escape(ticker) for ticker in row["tickers"][:12])
        if len(row["tickers"]) > 12:
            links += f", +{len(row['tickers']) - 12} more"
        date_rows.append(
            f"""
            <tr>
              <td>{escape(row['signal_date'])}</td>
              <td>{int(row['signal_count'])}</td>
              <td>{int(row['filled_trade_count'])}</td>
              <td>{_fmt_pct(row['returns']['1w']['avg_return_pct'])}</td>
              <td>{_fmt_pct(row['returns']['2w']['avg_return_pct'])}</td>
              <td>{_fmt_pct(row['returns']['3w']['avg_return_pct'])}</td>
              <td>{_fmt_pct(row['returns']['4w']['avg_return_pct'])}</td>
              <td>{links or '-'}</td>
            </tr>
            """
        )

    trade_rows = []
    for trade in payload["trades"][:250]:
        tags = escape(", ".join(trade.get("theme_tags", [])) or "-")
        drug_flag = "Yes" if trade.get("is_drug_ticker") else "No"
        trade_rows.append(
            f"""
            <tr>
              <td>{escape(trade['signal_date'])}</td>
              <td>{escape(trade['ticker'])}</td>
              <td>{tags}</td>
              <td>{drug_flag}</td>
              <td>{int(trade['pipeline_count'])}</td>
              <td>{escape(trade['entry_date'])}</td>
              <td>{trade['entry_price']:.2f}</td>
              <td>{_fmt_pct(trade['returns_pct']['1w'])}</td>
              <td>{_fmt_pct(trade['returns_pct']['2w'])}</td>
              <td>{_fmt_pct(trade['returns_pct']['3w'])}</td>
              <td>{_fmt_pct(trade['returns_pct']['4w'])}</td>
            </tr>
            """
        )

    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Overlap Backtest Report</title>
  <style>
    :root {{
      --bg: #08111f;
      --panel: #111d32;
      --panel-2: #162743;
      --ink: #eff6ff;
      --muted: #9fb1ca;
      --line: #26415f;
      --accent: #38bdf8;
      --good: #22c55e;
      --bad: #ef4444;
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      font-family: "SF Mono", Menlo, Consolas, monospace;
      background: radial-gradient(circle at top left, rgba(56, 189, 248, 0.18), transparent 26%), var(--bg);
      color: var(--ink);
    }}
    main {{
      max-width: 1240px;
      margin: 0 auto;
      padding: 28px 20px 48px;
    }}
    section {{
      border: 1px solid var(--line);
      border-radius: 20px;
      background: rgba(17, 29, 50, 0.94);
      padding: 24px;
      margin-bottom: 20px;
    }}
    h1, h2 {{ margin: 0 0 10px; }}
    p {{ color: var(--muted); line-height: 1.6; }}
    .stats {{
      display: grid;
      gap: 14px;
      grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
      margin-top: 18px;
    }}
    .stat {{
      border: 1px solid var(--line);
      border-radius: 16px;
      background: var(--panel-2);
      padding: 14px 16px;
    }}
    .stat strong {{
      display: block;
      color: var(--accent);
      font-size: 26px;
      margin-bottom: 6px;
    }}
    table {{
      width: 100%;
      border-collapse: collapse;
      margin-top: 14px;
      font-size: 14px;
    }}
    th, td {{
      border-bottom: 1px solid var(--line);
      padding: 10px 8px;
      text-align: left;
      vertical-align: top;
    }}
    th {{ color: var(--muted); font-weight: 600; }}
  </style>
</head>
<body>
  <main>
    <section>
      <h1>Overlap Count Backtest</h1>
      <p>Buy tickers from historical overlap summaries when <code>pipeline_count &gt;= {int(payload['min_overlap_count'])}</code>. Entry uses the first available trading close on or after the signal date. Returns are measured after 5, 10, 15, and 20 trading sessions.</p>
      <div class="stats">
        <div class="stat"><strong>{escape(start_date)}</strong>Start date</div>
        <div class="stat"><strong>{escape(end_date)}</strong>End date</div>
        <div class="stat"><strong>{int(payload['signal_day_count'])}</strong>Signal days</div>
        <div class="stat"><strong>{int(payload['trade_count'])}</strong>Total trades</div>
        <div class="stat"><strong>{int(payload['unique_ticker_count'])}</strong>Unique tickers</div>
      </div>
    </section>
    <section>
      <h2>Overall Returns</h2>
      <table>
        <thead>
          <tr>
            <th>Horizon</th>
            <th>Average</th>
            <th>Median</th>
            <th>Win rate</th>
            <th>Samples</th>
          </tr>
        </thead>
        <tbody>
          {''.join(overall_rows) or '<tr><td colspan="5">No data.</td></tr>'}
        </tbody>
      </table>
    </section>
    {''.join(grouped_sections)}
    <section>
      <h2>Signal Dates</h2>
      <table>
        <thead>
          <tr>
            <th>Signal date</th>
            <th>Signals</th>
            <th>Filled</th>
            <th>1W avg</th>
            <th>2W avg</th>
            <th>3W avg</th>
            <th>4W avg</th>
            <th>Tickers</th>
          </tr>
        </thead>
        <tbody>
          {''.join(date_rows) or '<tr><td colspan="8">No signal dates found.</td></tr>'}
        </tbody>
      </table>
    </section>
    <section>
      <h2>Trade Detail</h2>
      <table>
        <thead>
          <tr>
            <th>Signal date</th>
            <th>Ticker</th>
            <th>Tags</th>
            <th>Drug</th>
            <th>Count</th>
            <th>Entry date</th>
            <th>Entry price</th>
            <th>1W</th>
            <th>2W</th>
            <th>3W</th>
            <th>4W</th>
          </tr>
        </thead>
        <tbody>
          {''.join(trade_rows) or '<tr><td colspan="11">No trades found.</td></tr>'}
        </tbody>
      </table>
    </section>
  </main>
</body>
</html>
"""


def main() -> int:
    args = parse_args()
    end_date = _parse_date(args.end_date)
    start_date = _parse_date(args.start_date) if args.start_date else end_date - dt.timedelta(days=int(args.lookback_days))

    overlap_dir = Path(args.overlap_dir)
    overlap_files = _discover_overlap_files(overlap_dir, start_date, end_date)
    signal_days, tickers = _load_signals(overlap_files, int(args.min_overlap_count))

    if args.price_fixture:
        prices_by_ticker = _load_fixture_prices(Path(args.price_fixture))
    else:
        latest_required_date = end_date + dt.timedelta(days=35)
        prices_by_ticker = _download_prices(tickers, start_date, latest_required_date)

    payload = _build_payload(signal_days, prices_by_ticker, int(args.min_overlap_count))
    payload["start_date"] = start_date.isoformat()
    payload["end_date"] = end_date.isoformat()
    payload["overlap_files_found"] = len(overlap_files)
    payload["tickers_requested"] = len(tickers)
    payload["tickers_with_prices"] = len(prices_by_ticker)

    default_suffix = f"{start_date.isoformat()}_to_{end_date.isoformat()}"
    output_json = (
        Path(args.output_json)
        if args.output_json
        else PROJECT_ROOT / "artifacts" / "raw" / f"overlap_backtest_{default_suffix}.json"
    )
    output_html = (
        Path(args.output_html)
        if args.output_html
        else PROJECT_ROOT / "artifacts" / "output" / f"overlap_backtest_{default_suffix}" / "index.html"
    )
    output_json.parent.mkdir(parents=True, exist_ok=True)
    output_html.parent.mkdir(parents=True, exist_ok=True)
    output_json.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    output_html.write_text(_build_html(payload, start_date.isoformat(), end_date.isoformat()), encoding="utf-8")

    print(f"Signal days found: {payload['signal_day_count']}")
    print(f"Trades computed: {payload['trade_count']}")
    print(f"Wrote JSON report to {output_json}")
    print(f"Wrote HTML report to {output_html}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
