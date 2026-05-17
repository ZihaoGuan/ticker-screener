#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys
import time


PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.config import load_app_config, today_label
from src.universe import load_universe


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build ticker lists by market-cap bucket using yfinance."
    )
    parser.add_argument("--config", default=str(PROJECT_ROOT / "config" / "market_config.json"))
    parser.add_argument("--limit", type=int, help="Optional universe limit for smoke runs.")
    parser.add_argument("--date-label", help="Override artifact date label (YYYY-MM-DD).")
    parser.add_argument(
        "--sleep-seconds",
        type=float,
        default=0.15,
        help="Delay between ticker requests to reduce yfinance rate limiting.",
    )
    return parser.parse_args()


def _write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def _load_yfinance():
    try:
        import yfinance as yf
    except ImportError as exc:
        raise RuntimeError(
            "yfinance is not installed. Install requirements.txt before running this script."
        ) from exc
    return yf


def _market_cap_from_ticker(stock) -> float | None:
    try:
        fast_info = getattr(stock, "fast_info", None)
        if fast_info:
            value = fast_info.get("marketCap")
            if value not in (None, "", "NA", "n/a"):
                return float(value)
    except Exception:
        pass

    try:
        info = stock.info
        value = info.get("marketCap") if isinstance(info, dict) else None
        if value not in (None, "", "NA", "n/a"):
            return float(value)
    except Exception:
        pass
    return None


def main() -> int:
    args = parse_args()
    config = load_app_config(args.config)
    yf = _load_yfinance()
    date_label = args.date_label or today_label()
    universe = load_universe(config, limit=args.limit)

    lt_500m: list[dict[str, object]] = []
    between_500m_1b: list[dict[str, object]] = []
    missing: list[dict[str, str]] = []

    for index, item in enumerate(universe, start=1):
        ticker = item.symbol
        print(f"[{index}/{len(universe)}] fetching {ticker}")
        try:
            stock = yf.Ticker(ticker)
            market_cap = _market_cap_from_ticker(stock)
            if market_cap is None:
                missing.append({"ticker": ticker, "reason": "marketCap unavailable"})
            else:
                row = {
                    "ticker": ticker,
                    "market_cap": market_cap,
                    "market_cap_b": market_cap / 1_000_000_000,
                    "sector": item.sector,
                    "exchange": item.exchange,
                }
                if market_cap < 500_000_000:
                    lt_500m.append(row)
                elif market_cap < 1_000_000_000:
                    between_500m_1b.append(row)
        except Exception as exc:
            missing.append({"ticker": ticker, "reason": str(exc)})
            print(f"warning: {ticker}: {exc}")
        if args.sleep_seconds > 0:
            time.sleep(args.sleep_seconds)

    lt_500m.sort(key=lambda row: float(row["market_cap"]))
    between_500m_1b.sort(key=lambda row: float(row["market_cap"]))

    raw_dir = PROJECT_ROOT / "artifacts" / "raw"
    summary_path = raw_dir / f"market_cap_lists_summary_{date_label}.json"
    lt_500m_path = raw_dir / f"market_cap_lt_500m_{date_label}.json"
    between_path = raw_dir / f"market_cap_500m_to_1b_{date_label}.json"

    _write_json(
        summary_path,
        {
            "date_label": date_label,
            "total_tickers": len(universe),
            "lt_500m_count": len(lt_500m),
            "between_500m_1b_count": len(between_500m_1b),
            "missing_count": len(missing),
            "lt_500m_file": str(lt_500m_path),
            "between_500m_1b_file": str(between_path),
        },
    )
    _write_json(lt_500m_path, lt_500m)
    _write_json(between_path, between_500m_1b)

    if missing:
        _write_json(raw_dir / f"market_cap_missing_{date_label}.json", missing)

    print(f"Wrote summary to {summary_path}")
    print(f"Wrote <500M list to {lt_500m_path}")
    print(f"Wrote 500M-1B list to {between_path}")
    if missing:
        print(f"Missing market-cap rows: {len(missing)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
