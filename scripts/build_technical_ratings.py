#!/usr/bin/env python3
from __future__ import annotations

import argparse
import datetime as dt
from pathlib import Path
import sys

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.config import today_label
from src.market_data_access import load_many_ticker_windows
from src.ratings.calculator import build_technical_rating
from src.ratings.models import TechnicalSnapshotInput
from src.ratings.repository import RatingsRepository
from src.webapp.config import load_webapp_config


RS_RATING_REPLAY_THRESHOLDS = (
    195.93,
    117.11,
    99.04,
    91.66,
    80.96,
    53.64,
    24.86,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build technical ratings from daily bars and benchmark-relative strength data.")
    parser.add_argument("--as-of-date", default=today_label())
    parser.add_argument("--tickers", nargs="+", help="Only rebuild ratings for selected tickers.")
    parser.add_argument("--include-sectors", nargs="+", help="Only rebuild ratings for selected sectors.")
    parser.add_argument("--limit", type=int, default=None, help="Optional universe cap for faster rebuilds.")
    parser.add_argument("--benchmark-ticker", default="", help="Benchmark ticker used for RS line and RS rating. Defaults to app config benchmark.")
    parser.add_argument("--database-url", default="")
    return parser.parse_args()


def _true_range(frame: pd.DataFrame) -> pd.Series:
    import pandas as pd

    high = frame["High"].astype(float)
    low = frame["Low"].astype(float)
    close = frame["Close"].astype(float)
    prev_close = close.shift(1)
    components = pd.concat(
        [
            high - low,
            (high - prev_close).abs(),
            (low - prev_close).abs(),
        ],
        axis=1,
    )
    return components.max(axis=1)


def _compute_rs_line(stock_close: pd.Series, benchmark_close: pd.Series) -> pd.Series:
    import pandas as pd

    aligned = pd.concat([stock_close, benchmark_close], axis=1, join="inner").dropna()
    aligned.columns = ["stock", "benchmark"]
    return aligned["stock"] / aligned["benchmark"]


def _attribute_percentile(score: float, taller_perf: float, smaller_perf: float, range_up: int, range_dn: int, weight: float) -> float:
    adjusted_score = score + (score - smaller_perf) * weight
    if adjusted_score > taller_perf - 1:
        adjusted_score = taller_perf - 1
    k1 = smaller_perf / range_dn
    k2 = (taller_perf - 1) / range_up
    k3 = (k1 - k2) / (taller_perf - 1 - smaller_perf)
    rating = adjusted_score / (k1 - k3 * (score - smaller_perf))
    return max(min(rating, range_up), range_dn)


def _approximate_rs_rating(score: float) -> float | None:
    if score != score:
        return None
    first, scnd, thrd, frth, ffth, sxth, svth = RS_RATING_REPLAY_THRESHOLDS
    if score >= first:
        return 99.0
    if score <= svth:
        return 0.0
    if scnd <= score < first:
        return max(0.0, min(99.0, _attribute_percentile(score, first, scnd, 98, 90, 0.33)))
    if thrd <= score < scnd:
        return max(0.0, min(99.0, _attribute_percentile(score, scnd, thrd, 89, 70, 2.1)))
    if frth <= score < thrd:
        return max(0.0, min(99.0, _attribute_percentile(score, thrd, frth, 69, 50, 0.0)))
    if ffth <= score < frth:
        return max(0.0, min(99.0, _attribute_percentile(score, frth, ffth, 49, 30, 0.0)))
    if sxth <= score < ffth:
        return max(0.0, min(99.0, _attribute_percentile(score, ffth, sxth, 29, 10, 0.0)))
    return max(0.0, min(99.0, _attribute_percentile(score, sxth, svth, 9, 2, 0.0)))


def _compute_weighted_rs_score(stock: pd.Series, benchmark: pd.Series) -> pd.Series:
    import pandas as pd

    aligned = pd.concat([stock, benchmark], axis=1, join="inner").dropna()
    aligned.columns = ["stock", "benchmark"]
    perf_stock63 = aligned["stock"] / aligned["stock"].shift(63)
    perf_stock126 = aligned["stock"] / aligned["stock"].shift(126)
    perf_stock189 = aligned["stock"] / aligned["stock"].shift(189)
    perf_stock252 = aligned["stock"] / aligned["stock"].shift(252)
    perf_bench63 = aligned["benchmark"] / aligned["benchmark"].shift(63)
    perf_bench126 = aligned["benchmark"] / aligned["benchmark"].shift(126)
    perf_bench189 = aligned["benchmark"] / aligned["benchmark"].shift(189)
    perf_bench252 = aligned["benchmark"] / aligned["benchmark"].shift(252)
    rs_stock = 0.4 * perf_stock63 + 0.2 * perf_stock126 + 0.2 * perf_stock189 + 0.2 * perf_stock252
    rs_benchmark = 0.4 * perf_bench63 + 0.2 * perf_bench126 + 0.2 * perf_bench189 + 0.2 * perf_bench252
    return (rs_stock / rs_benchmark) * 100


def _compute_rs_rating_series(stock_close: pd.Series, benchmark_close: pd.Series) -> pd.Series:
    score_series = _compute_weighted_rs_score(stock_close, benchmark_close)
    return score_series.apply(_approximate_rs_rating).dropna().astype(float)


def _build_technical_snapshot_input(
    ticker: str,
    frame: pd.DataFrame | None,
    benchmark_frame: pd.DataFrame | None,
    *,
    as_of_date: dt.date,
) -> TechnicalSnapshotInput:
    import pandas as pd

    snapshot = TechnicalSnapshotInput(ticker=ticker.upper(), as_of_date=as_of_date)
    if frame is None or frame.empty or benchmark_frame is None or benchmark_frame.empty:
        return snapshot

    normalized = frame.sort_index().loc[frame.index <= pd.Timestamp(as_of_date)].copy()
    benchmark = benchmark_frame.sort_index().loc[benchmark_frame.index <= pd.Timestamp(as_of_date)].copy()
    if normalized.empty or benchmark.empty:
        return snapshot

    close = normalized["Close"].astype(float)
    high = normalized["High"].astype(float)
    low = normalized["Low"].astype(float)
    volume = normalized["Volume"].astype(float)
    if len(close) < 252:
        return snapshot

    tr = _true_range(normalized)
    atr20 = tr.rolling(20).mean()
    sma20 = close.rolling(20).mean()
    sma50 = close.rolling(50).mean()
    sma100 = close.rolling(100).mean()
    sma200 = close.rolling(200).mean()
    rs_line = _compute_rs_line(close, benchmark["Close"].astype(float))
    daily_rs_rating = _compute_rs_rating_series(close, benchmark["Close"].astype(float))
    weekly_rs_rating = daily_rs_rating.resample("W-FRI").last().dropna() if not daily_rs_rating.empty else pd.Series(dtype=float)
    midpoint = (high + low) / 2.0
    close_above_midpoint = (close > midpoint).astype(int)
    up_days = close.diff() > 0
    down_days = close.diff() < 0
    up_volume_sum = volume.where(up_days, 0.0).rolling(20).sum()
    down_volume_sum = volume.where(down_days, 0.0).rolling(20).sum()
    prior_20_high = high.shift(1).rolling(20).max()
    volume_avg_20 = volume.rolling(20).mean()
    distribution_days = ((close < close.shift(1)) & (volume > volume.shift(1))).astype(int).rolling(20).sum()

    snapshot.close = float(close.iloc[-1])
    snapshot.atr20 = float(atr20.iloc[-1]) if pd.notna(atr20.iloc[-1]) else None
    snapshot.sma20 = float(sma20.iloc[-1]) if pd.notna(sma20.iloc[-1]) else None
    snapshot.sma50 = float(sma50.iloc[-1]) if pd.notna(sma50.iloc[-1]) else None
    snapshot.sma100 = float(sma100.iloc[-1]) if pd.notna(sma100.iloc[-1]) else None
    snapshot.sma200 = float(sma200.iloc[-1]) if pd.notna(sma200.iloc[-1]) else None
    snapshot.sma20_5d_ago = float(sma20.shift(5).iloc[-1]) if pd.notna(sma20.shift(5).iloc[-1]) else None
    snapshot.sma50_10d_ago = float(sma50.shift(10).iloc[-1]) if pd.notna(sma50.shift(10).iloc[-1]) else None
    snapshot.sma100_10d_ago = float(sma100.shift(10).iloc[-1]) if pd.notna(sma100.shift(10).iloc[-1]) else None
    snapshot.sma50_20d_ago = float(sma50.shift(20).iloc[-1]) if pd.notna(sma50.shift(20).iloc[-1]) else None
    snapshot.sma200_20d_ago = float(sma200.shift(20).iloc[-1]) if pd.notna(sma200.shift(20).iloc[-1]) else None
    snapshot.daily_rs_rating = float(daily_rs_rating.iloc[-1]) if not daily_rs_rating.empty else None
    snapshot.weekly_rs_rating = float(weekly_rs_rating.iloc[-1]) if not weekly_rs_rating.empty else None
    snapshot.rs_line = float(rs_line.iloc[-1]) if not rs_line.empty else None
    snapshot.rs_line_sma50 = float(rs_line.rolling(50).mean().iloc[-1]) if not rs_line.empty and pd.notna(rs_line.rolling(50).mean().iloc[-1]) else None
    snapshot.rs_line_3m_high = float(rs_line.tail(63).max()) if len(rs_line) >= 63 else None
    snapshot.rs_line_12m_high = float(rs_line.tail(252).max()) if len(rs_line) >= 252 else None
    snapshot.high_52w = float(high.tail(252).max())
    snapshot.low_52w = float(low.tail(252).min())
    snapshot.tr_10d_avg = float(tr.tail(10).mean()) if len(tr.dropna()) >= 10 else None
    snapshot.tr_20d_avg = float(tr.tail(20).mean()) if len(tr.dropna()) >= 20 else None
    snapshot.close_above_bar_midpoint_count_10d = int(close_above_midpoint.tail(10).sum()) if len(close_above_midpoint) >= 10 else None
    down_volume_latest = float(down_volume_sum.iloc[-1]) if pd.notna(down_volume_sum.iloc[-1]) else 0.0
    up_volume_latest = float(up_volume_sum.iloc[-1]) if pd.notna(up_volume_sum.iloc[-1]) else 0.0
    snapshot.up_down_volume_ratio_20d = None if down_volume_latest <= 0 else float(up_volume_latest / down_volume_latest)
    is_breakout = pd.notna(prior_20_high.iloc[-1]) and float(close.iloc[-1]) > float(prior_20_high.iloc[-1])
    if is_breakout and pd.notna(volume_avg_20.iloc[-1]) and float(volume_avg_20.iloc[-1]) > 0:
        snapshot.breakout_volume_ratio = float(volume.iloc[-1] / volume_avg_20.iloc[-1])
    else:
        snapshot.breakout_volume_ratio = 0.0
    snapshot.distribution_day_count_20d = int(distribution_days.iloc[-1]) if pd.notna(distribution_days.iloc[-1]) else None
    return snapshot


def main() -> int:
    args = parse_args()
    config = load_webapp_config()
    database_url = (args.database_url or config.database_url).strip()
    if not database_url:
        raise RuntimeError("No Postgres connection string configured. Pass --database-url or set TICKER_SCREENER_DATABASE_URL.")
    as_of_date = dt.date.fromisoformat(str(args.as_of_date))
    benchmark_ticker = str(args.benchmark_ticker or config.benchmark_ticker).strip().upper() or "SPY"
    repository = RatingsRepository(database_url)
    selected_tickers = tuple(str(item).strip().upper() for item in (args.tickers or []) if str(item).strip())
    selected_sectors = tuple(str(item).strip() for item in (args.include_sectors or []) if str(item).strip())
    target_tickers = repository.list_active_tickers(
        tickers=selected_tickers or None,
        sectors=selected_sectors or None,
        limit=args.limit,
    )
    print(
        "loading_technical_universe "
        f"as_of_date={as_of_date.isoformat()} "
        f"benchmark={benchmark_ticker} "
        f"tickers={len(target_tickers)} "
        f"include_sectors={','.join(selected_sectors) or '-'}",
        flush=True,
    )
    if not target_tickers:
        repository.replace_technical_rating_snapshots(as_of_date, [], tickers=[])
        print("technical_ratings=0", flush=True)
        print("technical_ratings_ok=0", flush=True)
        return 0

    frame_map = load_many_ticker_windows(
        [*target_tickers, benchmark_ticker],
        as_of_date,
        320,
        database_url=database_url,
    )
    benchmark_frame = frame_map.get(benchmark_ticker)
    if benchmark_frame is None or benchmark_frame.empty:
        raise RuntimeError(f"No benchmark daily_bars coverage found for {benchmark_ticker} on or before {as_of_date.isoformat()}.")
    ratings = []
    ok_count = 0
    total = len(target_tickers)
    for index, ticker in enumerate(target_tickers, start=1):
        frame = frame_map.get(ticker)
        snapshot = _build_technical_snapshot_input(ticker, frame, benchmark_frame, as_of_date=as_of_date)
        rating = build_technical_rating(snapshot)
        ratings.append(rating)
        if rating.technical_status == "ok":
            ok_count += 1
        print(f"[{index}/{total}] technical_rating {ticker} status={rating.technical_status}", flush=True)

    count = repository.replace_technical_rating_snapshots(
        as_of_date,
        ratings,
        tickers=target_tickers,
    )
    print(f"technical_ratings={count}", flush=True)
    print(f"technical_ratings_ok={ok_count}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
