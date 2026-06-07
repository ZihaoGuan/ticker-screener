from __future__ import annotations

from dataclasses import asdict, dataclass
import datetime as dt

import numpy as np
import pandas as pd

from .config import AppConfig
from .cookstock_bridge import freeze_cookstock_today, iter_prefetched_cookstock_batches, load_configured_cookstock
from .rs_rating_screen import approximate_rs_rating, compute_latest_weighted_rs_score
from .universe import UniverseTicker


@dataclass(frozen=True)
class ScreenHit:
    ticker: str
    sector: str | None
    exchange: str | None
    signal_date: str
    benchmark_ticker: str
    current_price: float
    current_high: float
    current_rs_line: float
    daily_rs_line_high: float
    daily_price_high: float
    daily_lookback_days: int
    weekly_lookback_weeks: int
    daily_rs_new_high: bool
    daily_rs_new_high_before_price: bool
    weekly_rs_new_high: bool
    weekly_rs_new_high_before_price: bool
    weekly_rs_new_high_recent: bool
    weekly_signal_weeks_ago: int | None
    weekly_recent_signal_weeks: int
    recent_golden_cross: bool
    recent_golden_cross_days: int
    recent_inside_day: bool
    recent_inside_day_days: int
    require_before_price: bool
    is_near_year_high: bool
    year_high: float
    distance_from_year_high_pct: float
    is_strong_rs: bool
    stock_return_vs_rs_window_pct: float
    benchmark_return_vs_rs_window_pct: float
    rs_line_high: float
    is_sector_etf_strong: bool
    sector_etf: str
    sector_etf_near_year_high: bool
    sector_etf_distance_from_year_high_pct: float | str
    sector_etf_return_vs_rs_window_pct: float | str
    sector_benchmark_return_vs_rs_window_pct: float | str
    rs_score: float
    rs_rating: float
    min_rs_rating: float
    reasons: list[str]

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True)
class ScreenResult:
    run_date: str
    benchmark_ticker: str
    total_tickers: int
    passed_tickers: int
    failed_tickers: list[dict[str, str]]
    hits: list[ScreenHit]

    def to_dict(self) -> dict[str, object]:
        return {
            "run_date": self.run_date,
            "benchmark_ticker": self.benchmark_ticker,
            "total_tickers": self.total_tickers,
            "passed_tickers": self.passed_tickers,
            "failed_tickers": self.failed_tickers,
            "hits": [item.to_dict() for item in self.hits],
        }


def _has_recent_golden_cross(closes: list[float], *, window_days: int = 2) -> bool:
    if len(closes) < 201:
        return False
    close_series = np.asarray(closes, dtype=float)
    sma50 = np.convolve(close_series, np.ones(50, dtype=float) / 50.0, mode="valid")
    sma200 = np.convolve(close_series, np.ones(200, dtype=float) / 200.0, mode="valid")
    offset = 200 - 50
    sma50_aligned = sma50[offset:]
    if sma50_aligned.size == 0 or sma200.size == 0:
        return False
    lookback = min(max(1, int(window_days)), sma200.size - 1)
    start_index = sma200.size - lookback
    for idx in range(start_index, sma200.size):
        previous_idx = idx - 1
        if previous_idx < 0:
            continue
        if sma50_aligned[previous_idx] <= sma200[previous_idx] and sma50_aligned[idx] > sma200[idx]:
            return True
    return False


def _has_recent_inside_day(price_data: list[dict[str, object]], *, window_days: int = 2) -> bool:
    if len(price_data) < 2:
        return False
    recent_checks = min(max(1, int(window_days)), len(price_data) - 1)
    start_index = len(price_data) - recent_checks
    for idx in range(start_index, len(price_data)):
        latest_bar = price_data[idx]
        previous_bar = price_data[idx - 1]
        latest_high = latest_bar.get("high")
        latest_low = latest_bar.get("low")
        previous_high = previous_bar.get("high")
        previous_low = previous_bar.get("low")
        if None in (latest_high, latest_low, previous_high, previous_low):
            continue
        if float(latest_high) < float(previous_high) and float(latest_low) > float(previous_low):
            return True
    return False


def _to_hit(ticker: UniverseTicker, summary: dict[str, object]) -> ScreenHit:
    return ScreenHit(
        ticker=ticker.symbol,
        sector=ticker.sector,
        exchange=ticker.exchange,
        signal_date=str(summary["signal_date"]),
        benchmark_ticker=str(summary["benchmark_ticker"]),
        current_price=float(summary["current_price"]),
        current_high=float(summary["current_high"]),
        current_rs_line=float(summary["current_rs_line"]),
        daily_rs_line_high=float(summary["daily_rs_line_high"]),
        daily_price_high=float(summary["daily_price_high"]),
        daily_lookback_days=int(summary["daily_lookback_days"]),
        weekly_lookback_weeks=int(summary["weekly_lookback_weeks"]),
        daily_rs_new_high=bool(summary["daily_rs_new_high"]),
        daily_rs_new_high_before_price=bool(summary["daily_rs_new_high_before_price"]),
        weekly_rs_new_high=bool(summary["weekly_rs_new_high"]),
        weekly_rs_new_high_before_price=bool(summary["weekly_rs_new_high_before_price"]),
        weekly_rs_new_high_recent=bool(summary["weekly_rs_new_high_recent"]),
        weekly_signal_weeks_ago=int(summary["weekly_signal_weeks_ago"]) if summary.get("weekly_signal_weeks_ago") is not None else None,
        weekly_recent_signal_weeks=int(summary["weekly_recent_signal_weeks"]),
        recent_golden_cross=bool(summary.get("recent_golden_cross", False)),
        recent_golden_cross_days=int(summary.get("recent_golden_cross_days", 2)),
        recent_inside_day=bool(summary.get("recent_inside_day", False)),
        recent_inside_day_days=int(summary.get("recent_inside_day_days", 2)),
        require_before_price=bool(summary["require_before_price"]),
        is_near_year_high=bool(summary["is_near_year_high"]),
        year_high=float(summary["year_high"]),
        distance_from_year_high_pct=float(summary["distance_from_year_high_pct"]),
        is_strong_rs=bool(summary["is_strong_rs"]),
        stock_return_vs_rs_window_pct=float(summary["stock_return_vs_rs_window_pct"]),
        benchmark_return_vs_rs_window_pct=float(summary["benchmark_return_vs_rs_window_pct"]),
        rs_line_high=float(summary["rs_line_high"]),
        is_sector_etf_strong=bool(summary["is_sector_etf_strong"]),
        sector_etf=str(summary["sector_etf"]),
        sector_etf_near_year_high=bool(summary["sector_etf_near_year_high"]),
        sector_etf_distance_from_year_high_pct=summary["sector_etf_distance_from_year_high_pct"],
        sector_etf_return_vs_rs_window_pct=summary["sector_etf_return_vs_rs_window_pct"],
        sector_benchmark_return_vs_rs_window_pct=summary["sector_benchmark_return_vs_rs_window_pct"],
        rs_score=float(summary["rs_score"]),
        rs_rating=float(summary["rs_rating"]),
        min_rs_rating=float(summary["min_rs_rating"]),
        reasons=list(summary.get("reasons", [])),
    )


def _build_close_frame_from_rows(rows: list[dict[str, object]]) -> pd.DataFrame:
    if not rows:
        return pd.DataFrame()
    frame = pd.DataFrame(
        {
            "Date": pd.to_datetime([row.get("formatted_date") for row in rows]),
            "Close": [row.get("close") for row in rows],
        }
    )
    return frame.dropna(subset=["Date", "Close"]).set_index("Date").sort_index()


def _build_high_close_frame_from_rows(rows: list[dict[str, object]]) -> pd.DataFrame:
    if not rows:
        return pd.DataFrame()
    frame = pd.DataFrame(
        {
            "Date": pd.to_datetime([row.get("formatted_date") for row in rows]),
            "High": [row.get("high") for row in rows],
            "Close": [row.get("close") for row in rows],
        }
    )
    return frame.dropna(subset=["Date", "High", "Close"]).set_index("Date").sort_index()


def _compute_latest_rs_rating(
    stock_rows: list[dict[str, object]],
    benchmark_rows: list[dict[str, object]],
) -> tuple[float, float] | None:
    stock_frame = _build_close_frame_from_rows(stock_rows)
    benchmark_frame = _build_close_frame_from_rows(benchmark_rows)
    if stock_frame.empty or benchmark_frame.empty:
        return None
    aligned = stock_frame.join(benchmark_frame.rename(columns={"Close": "benchmark_close"}), how="inner").dropna()
    if len(aligned) < 2:
        return None
    latest_score = compute_latest_weighted_rs_score(aligned["Close"], aligned["benchmark_close"])
    if latest_score is None:
        return None
    latest_rating = approximate_rs_rating(float(latest_score)) if pd.notna(latest_score) else None
    if latest_rating is None:
        return None
    return float(latest_score), float(latest_rating)


def _compute_weekly_rs_before_price_context(
    stock_rows: list[dict[str, object]],
    benchmark_rows: list[dict[str, object]],
    *,
    weekly_lookback_weeks: int,
    recent_signal_weeks: int,
) -> dict[str, object] | None:
    stock_frame = _build_high_close_frame_from_rows(stock_rows)
    benchmark_frame = _build_close_frame_from_rows(benchmark_rows)
    if stock_frame.empty or benchmark_frame.empty:
        return None

    aligned = stock_frame.join(benchmark_frame.rename(columns={"Close": "benchmark_close"}), how="inner").dropna()
    if aligned.empty:
        return None

    weekly_stock = aligned[["Close", "High"]].resample("W-FRI").agg({"Close": "last", "High": "max"}).dropna()
    weekly_benchmark = aligned[["benchmark_close"]].resample("W-FRI").agg({"benchmark_close": "last"}).dropna()
    weekly_aligned = weekly_stock.join(weekly_benchmark, how="inner").dropna()
    if weekly_aligned.empty:
        return None

    weekly_rs_line = weekly_aligned["Close"] / weekly_aligned["benchmark_close"]
    rolling_rs_high = weekly_rs_line.rolling(window=max(1, int(weekly_lookback_weeks)), min_periods=1).max()
    rolling_price_high = weekly_aligned["High"].rolling(window=max(1, int(weekly_lookback_weeks)), min_periods=1).max()
    tolerance = 1e-12
    weekly_before_price = (weekly_rs_line >= (rolling_rs_high - tolerance)) & (
        weekly_aligned["High"] < (rolling_price_high - tolerance)
    )

    latest_flag = bool(weekly_before_price.iloc[-1])
    recent_flags = weekly_before_price.tail(max(1, int(recent_signal_weeks)))
    recent_any = bool(recent_flags.any()) if not recent_flags.empty else False
    weeks_ago = None
    signal_date = None
    if recent_any:
        recent_flags_list = [bool(value) for value in recent_flags.tolist()]
        true_positions = [idx for idx, value in enumerate(recent_flags_list) if value]
        if true_positions:
            last_true_position = true_positions[-1]
            weeks_ago = len(recent_flags_list) - 1 - last_true_position
            signal_date = recent_flags.index[last_true_position].date().isoformat()

    return {
        "latest_weekly_before_price": latest_flag,
        "recent_weekly_before_price": recent_any,
        "weekly_before_price_weeks_ago": weeks_ago,
        "weekly_before_price_signal_date": signal_date,
    }


def run_rs_screen(
    config: AppConfig,
    tickers: list[UniverseTicker],
    *,
    signal_profile: str = "daily",
    as_of_date: dt.date | None = None,
) -> ScreenResult:
    cookstock = load_configured_cookstock(config)
    hits: list[ScreenHit] = []
    failures: list[dict[str, str]] = []
    run_date = as_of_date or dt.date.today()
    total_tickers = len(tickers)

    with freeze_cookstock_today(cookstock, as_of_date):
        position = 0
        for ticker_batch in iter_prefetched_cookstock_batches(
            config,
            tickers,
            as_of_date=as_of_date,
            history_lookback_days=config.rs_new_high_history_days,
            benchmark_ticker=config.benchmark_ticker,
        ):
            for ticker in ticker_batch:
                position += 1
                print(f"[{position}/{total_tickers}] screening {ticker.symbol} | passed={len(hits)}")
                try:
                    financials = cookstock.cookFinancials(
                        ticker.symbol,
                        benchmarkTicker=config.benchmark_ticker,
                        historyLookbackDays=config.rs_new_high_history_days,
                    )
                    price_data = [
                        item
                        for item in financials.priceData.get("priceData", [])
                        if isinstance(item, dict)
                    ]
                    benchmark_rows = [
                        item
                        for item in financials._get_benchmark_price_data(config.benchmark_ticker)
                        if isinstance(item, dict)
                    ]
                    rs_metrics = _compute_latest_rs_rating(price_data, benchmark_rows)
                    latest_rs_score: float | None = None
                    latest_rs_rating: float | None = None
                    if rs_metrics is not None:
                        latest_rs_score, latest_rs_rating = rs_metrics
                        print(f"{ticker.symbol} rs_rating={latest_rs_rating:.1f}")
                    else:
                        print(f"{ticker.symbol} rs_rating=n/a")
                    summary = financials.get_rs_new_high_before_price_summary(
                        sectorName=ticker.sector,
                        benchmarkTicker=config.benchmark_ticker,
                        signalProfile=signal_profile,
                    )
                    if summary:
                        if str(signal_profile).strip().lower() == "weekly":
                            weekly_context = _compute_weekly_rs_before_price_context(
                                price_data,
                                benchmark_rows,
                                weekly_lookback_weeks=int(summary.get("weekly_lookback_weeks", config.rs_new_high_weekly_lookback_weeks)),
                                recent_signal_weeks=int(summary.get("weekly_recent_signal_weeks", config.rs_weekly_recent_signal_weeks)),
                            )
                            if not weekly_context or not bool(weekly_context["recent_weekly_before_price"]):
                                continue
                            summary["weekly_rs_new_high_before_price"] = bool(weekly_context["latest_weekly_before_price"])
                            summary["weekly_signal_weeks_ago"] = weekly_context["weekly_before_price_weeks_ago"]
                            if weekly_context["weekly_before_price_signal_date"]:
                                summary["signal_date"] = str(weekly_context["weekly_before_price_signal_date"])
                        if latest_rs_score is None or latest_rs_rating is None:
                            continue
                        reasons = list(summary.get("reasons", []))
                        if str(signal_profile).strip().lower() == "weekly":
                            weeks_ago = summary.get("weekly_signal_weeks_ago")
                            if weeks_ago in (None, 0):
                                reasons.append("weekly RS new high before price this week")
                            else:
                                reasons.append(f"weekly RS new high before price {int(weeks_ago)} week(s) ago")
                        reasons.append(f"RS rating {latest_rs_rating:.1f}")
                        summary["reasons"] = reasons
                        summary["rs_score"] = latest_rs_score
                        summary["rs_rating"] = latest_rs_rating
                        summary["min_rs_rating"] = 0.0
                        closes = [
                            float(item["close"])
                            for item in price_data
                            if item.get("close") is not None
                        ]
                        summary["recent_golden_cross_days"] = 2
                        summary["recent_golden_cross"] = _has_recent_golden_cross(closes, window_days=2)
                        summary["recent_inside_day_days"] = 2
                        summary["recent_inside_day"] = _has_recent_inside_day(price_data, window_days=2)
                        hits.append(_to_hit(ticker, summary))
                except Exception as exc:
                    failures.append({"ticker": ticker.symbol, "error": str(exc)})
                    print(f"[{position}/{total_tickers}] {ticker.symbol} error: {exc} | passed={len(hits)}")

    print(f"screen complete: passed={len(hits)}, failed={len(failures)}, total={total_tickers}")

    return ScreenResult(
        run_date=run_date.isoformat(),
        benchmark_ticker=config.benchmark_ticker,
        total_tickers=total_tickers,
        passed_tickers=len(hits),
        failed_tickers=failures,
        hits=hits,
    )
