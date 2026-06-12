from __future__ import annotations

import argparse
import datetime as dt
from pathlib import Path
import sys

import pandas as pd
import yfinance as yf

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.market_data_access import load_daily_bars_frame_from_db
from src.market_extension import compute_extension_frame, filter_extension_window, find_extension_peaks, resample_to_weekly


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Verify market extension bars against local DB or yfinance.")
    parser.add_argument("--ticker", default="SPY")
    parser.add_argument("--timeframe", choices=["daily", "weekly"], default="weekly")
    parser.add_argument("--lengths", nargs="+", type=int, default=[8, 10, 20, 30, 40])
    parser.add_argument("--ma-types", nargs="+", default=["sma", "ema"])
    parser.add_argument("--warning-pct", type=float, default=11.0)
    parser.add_argument("--extreme-pct", type=float, default=15.0)
    parser.add_argument("--start-date", type=str, default="2021-01-01")
    parser.add_argument("--end-date", type=str, default=dt.date.today().isoformat())
    parser.add_argument("--top", type=int, default=10)
    parser.add_argument("--database-url", default="")
    parser.add_argument("--min-extension-pct", type=float, default=11.0)
    parser.add_argument("--max-extension-pct", type=float, default=15.5)
    return parser.parse_args()


def _load_history(ticker: str, start_date: dt.date, end_date: dt.date, database_url: str) -> tuple[pd.DataFrame, str]:
    frame = load_daily_bars_frame_from_db(ticker, start_date, end_date, database_url=database_url or None)
    if frame is not None and not frame.empty:
        return frame, "database"

    history = yf.download(
        tickers=ticker,
        start=start_date.isoformat(),
        end=(end_date + dt.timedelta(days=1)).isoformat(),
        interval="1d",
        auto_adjust=False,
        progress=False,
        threads=False,
    )
    if history is None or history.empty:
        raise RuntimeError(f"No history found for {ticker}.")
    if isinstance(history.columns, pd.MultiIndex):
        history.columns = history.columns.get_level_values(0)
    frame = history.rename(columns=str)
    frame = frame.dropna(subset=["Open", "High", "Low", "Close", "Volume"]).sort_index()
    return frame, "yfinance"


def main() -> int:
    args = _parse_args()
    start_date = dt.date.fromisoformat(args.start_date)
    end_date = dt.date.fromisoformat(args.end_date)
    frame, source = _load_history(args.ticker.upper(), start_date, end_date, args.database_url)
    if args.timeframe == "weekly":
        frame = resample_to_weekly(frame)
    frame = filter_extension_window(frame, start_date=start_date, end_date=end_date)

    print(
        f"ticker={args.ticker.upper()} timeframe={args.timeframe} "
        f"bars={len(frame)} source={source} window={start_date.isoformat()}..{end_date.isoformat()}"
    )

    for ma_type in args.ma_types:
        for length in args.lengths:
            enriched = compute_extension_frame(
                frame,
                length=length,
                ma_type=ma_type,
                warning_pct=args.warning_pct,
                extreme_pct=args.extreme_pct,
            )
            latest = enriched.dropna(subset=["moving_average", "extension_pct"]).tail(1)
            peaks = find_extension_peaks(
                frame,
                length=length,
                ma_type=ma_type,
                warning_pct=args.warning_pct,
                extreme_pct=args.extreme_pct,
                min_extension_pct=args.min_extension_pct,
                max_extension_pct=args.max_extension_pct,
            )
            print("")
            print(f"{ma_type.upper()} {length}")
            if latest.empty:
                print("  latest: n/a")
            else:
                row = latest.iloc[0]
                print(
                    "  latest: "
                    f"date={latest.index[-1].date().isoformat()} "
                    f"close={float(row['Close']):.2f} "
                    f"ma={float(row['moving_average']):.2f} "
                    f"ext={float(row['extension_pct']):.2f}% "
                    f"state={row['threshold_state']}"
                )
            if not peaks:
                print("  peaks: none in filter window")
                continue
            print(f"  peaks: showing {min(len(peaks), args.top)} of {len(peaks)}")
            for peak in peaks[: args.top]:
                print(
                    "   - "
                    f"{peak.trade_date} close={peak.close:.2f} "
                    f"ma={peak.moving_average:.2f} ext={peak.extension_pct:.2f}% "
                    f"state={peak.threshold_state}"
                )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
