from __future__ import annotations

from dataclasses import asdict, dataclass
import datetime as dt

import pandas as pd

from .config import AppConfig
from .cookstock_bridge import freeze_cookstock_today, iter_prefetched_cookstock_batches, load_configured_cookstock
from .universe import UniverseTicker


RS_RATING_REPLAY_THRESHOLDS = (
    195.93,
    117.11,
    99.04,
    91.66,
    80.96,
    53.64,
    24.86,
)


@dataclass(frozen=True)
class RsRatingHit:
    ticker: str
    sector: str | None
    industry: str | None
    exchange: str | None
    signal_date: str
    benchmark_ticker: str
    current_price: float
    current_rs_line: float
    rs_score: float
    rs_rating: float
    min_rating: float
    reasons: list[str]

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True)
class RsRatingScreenResult:
    run_date: str
    benchmark_ticker: str
    total_tickers: int
    passed_tickers: int
    failed_tickers: list[dict[str, str]]
    hits: list[RsRatingHit]

    def to_dict(self) -> dict[str, object]:
        return {
            "run_date": self.run_date,
            "benchmark_ticker": self.benchmark_ticker,
            "total_tickers": self.total_tickers,
            "passed_tickers": self.passed_tickers,
            "failed_tickers": self.failed_tickers,
            "hits": [item.to_dict() for item in self.hits],
        }


def _build_price_frame_from_rows(rows: list[dict[str, object]]) -> pd.DataFrame:
    if not rows:
        return pd.DataFrame()
    frame = pd.DataFrame(
        {
            "Date": pd.to_datetime([row.get("formatted_date") for row in rows]),
            "Open": [row.get("open") for row in rows],
            "High": [row.get("high") for row in rows],
            "Low": [row.get("low") for row in rows],
            "Close": [row.get("close") for row in rows],
            "Volume": [row.get("volume") for row in rows],
        }
    )
    return frame.dropna(subset=["Date", "Close"]).set_index("Date").sort_index()


def _normalize_close_frame(frame: pd.DataFrame) -> pd.DataFrame:
    required = ["Close"]
    available = {str(column).lower(): column for column in frame.columns}
    missing = [column for column in required if column.lower() not in available]
    if missing:
        return pd.DataFrame()
    normalized = frame[[available["close"]]].copy()
    normalized.columns = ["Close"]
    normalized = normalized.dropna(subset=["Close"]).sort_index()
    if not isinstance(normalized.index, pd.DatetimeIndex):
        normalized.index = pd.to_datetime(normalized.index)
    return normalized


def compute_weighted_rs_score(stock: pd.Series, benchmark: pd.Series) -> pd.Series:
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


def compute_latest_weighted_rs_score(stock: pd.Series, benchmark: pd.Series) -> float | None:
    aligned = pd.concat([stock, benchmark], axis=1, join="inner").dropna()
    aligned.columns = ["stock", "benchmark"]
    if len(aligned) < 2:
        return None

    latest_index = len(aligned) - 1
    lookback_63 = min(latest_index, 63)
    lookback_126 = min(latest_index, 126)
    lookback_189 = min(latest_index, 189)
    lookback_252 = min(latest_index, 252)
    if min(lookback_63, lookback_126, lookback_189, lookback_252) <= 0:
        return None

    latest_stock = float(aligned["stock"].iloc[-1])
    latest_benchmark = float(aligned["benchmark"].iloc[-1])
    stock_63 = float(aligned["stock"].iloc[-1 - lookback_63])
    stock_126 = float(aligned["stock"].iloc[-1 - lookback_126])
    stock_189 = float(aligned["stock"].iloc[-1 - lookback_189])
    stock_252 = float(aligned["stock"].iloc[-1 - lookback_252])
    benchmark_63 = float(aligned["benchmark"].iloc[-1 - lookback_63])
    benchmark_126 = float(aligned["benchmark"].iloc[-1 - lookback_126])
    benchmark_189 = float(aligned["benchmark"].iloc[-1 - lookback_189])
    benchmark_252 = float(aligned["benchmark"].iloc[-1 - lookback_252])

    if min(stock_63, stock_126, stock_189, stock_252, benchmark_63, benchmark_126, benchmark_189, benchmark_252) <= 0:
        return None

    rs_stock = (
        0.4 * (latest_stock / stock_63)
        + 0.2 * (latest_stock / stock_126)
        + 0.2 * (latest_stock / stock_189)
        + 0.2 * (latest_stock / stock_252)
    )
    rs_benchmark = (
        0.4 * (latest_benchmark / benchmark_63)
        + 0.2 * (latest_benchmark / benchmark_126)
        + 0.2 * (latest_benchmark / benchmark_189)
        + 0.2 * (latest_benchmark / benchmark_252)
    )
    if rs_benchmark == 0:
        return None
    return (rs_stock / rs_benchmark) * 100.0


def _attribute_percentile(score: float, taller_perf: float, smaller_perf: float, range_up: int, range_dn: int, weight: float) -> float:
    adjusted_score = score + (score - smaller_perf) * weight
    if adjusted_score > taller_perf - 1:
        adjusted_score = taller_perf - 1
    k1 = smaller_perf / range_dn
    k2 = (taller_perf - 1) / range_up
    k3 = (k1 - k2) / (taller_perf - 1 - smaller_perf)
    rating = adjusted_score / (k1 - k3 * (score - smaller_perf))
    return max(min(rating, range_up), range_dn)


def approximate_rs_rating(score: float) -> float | None:
    if pd.isna(score):
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


def find_recent_rs_rating_hit(
    stock_frame: pd.DataFrame,
    benchmark_frame: pd.DataFrame,
    *,
    ticker: UniverseTicker,
    benchmark_ticker: str,
    min_rating: float,
) -> RsRatingHit | None:
    stock = _normalize_close_frame(stock_frame)
    benchmark = _normalize_close_frame(benchmark_frame)
    if stock.empty or benchmark.empty:
        return None

    aligned = stock.join(benchmark.rename(columns={"Close": "BenchmarkClose"}), how="inner").dropna()
    if len(aligned) < 2:
        return None

    latest_score = compute_latest_weighted_rs_score(aligned["Close"], aligned["BenchmarkClose"])
    if latest_score is None:
        return None
    latest_rating = approximate_rs_rating(float(latest_score)) if pd.notna(latest_score) else None
    if latest_rating is None or latest_rating < float(min_rating):
        return None

    latest_date = aligned.index[-1]
    latest_rs_line = float(aligned["Close"].iloc[-1] / aligned["BenchmarkClose"].iloc[-1])
    reasons = [
        f"RS rating {latest_rating:.1f} >= minimum {float(min_rating):.1f}",
        f"weighted RS score {float(latest_score):.2f}",
    ]
    return RsRatingHit(
        ticker=ticker.symbol,
        sector=ticker.sector,
        industry=ticker.industry,
        exchange=ticker.exchange,
        signal_date=latest_date.date().isoformat(),
        benchmark_ticker=benchmark_ticker,
        current_price=float(aligned["Close"].iloc[-1]),
        current_rs_line=latest_rs_line,
        rs_score=float(latest_score),
        rs_rating=float(latest_rating),
        min_rating=float(min_rating),
        reasons=reasons,
    )


def run_rs_rating_screen(
    config: AppConfig,
    tickers: list[UniverseTicker],
    *,
    as_of_date: dt.date | None = None,
) -> RsRatingScreenResult:
    cookstock = load_configured_cookstock(config)
    hits: list[RsRatingHit] = []
    failures: list[dict[str, str]] = []
    total_tickers = len(tickers)
    run_date = as_of_date or dt.date.today()
    history_days = max(int(config.rs_new_high_history_days), 320)

    print(
        "starting rs rating screen: "
        f"total={total_tickers}, "
        f"min_rating={config.rs_rating_min}, "
        f"benchmark={config.benchmark_ticker}"
    )

    with freeze_cookstock_today(cookstock, as_of_date):
        position = 0
        for ticker_batch in iter_prefetched_cookstock_batches(
            config,
            tickers,
            as_of_date=as_of_date,
            history_lookback_days=history_days,
            benchmark_ticker=config.benchmark_ticker,
        ):
            for ticker in ticker_batch:
                position += 1
                print(f"[{position}/{total_tickers}] screening {ticker.symbol} | passed={len(hits)}")
                try:
                    financials = cookstock.cookFinancials(
                        ticker.symbol,
                        benchmarkTicker=config.benchmark_ticker,
                        historyLookbackDays=history_days,
                    )
                    stock_frame = _build_price_frame_from_rows(financials._get_clean_price_data())
                    benchmark_frame = _build_price_frame_from_rows(financials._get_benchmark_price_data(config.benchmark_ticker))
                    hit = find_recent_rs_rating_hit(
                        stock_frame,
                        benchmark_frame,
                        ticker=ticker,
                        benchmark_ticker=config.benchmark_ticker,
                        min_rating=float(config.rs_rating_min),
                    )
                    if hit is None:
                        continue
                    hits.append(hit)
                except Exception as exc:
                    failures.append({"ticker": ticker.symbol, "error": str(exc)})
                    print(f"[{position}/{total_tickers}] {ticker.symbol} error: {exc} | passed={len(hits)}")

    print(f"rs rating screen complete: passed={len(hits)}, failed={len(failures)}, total={total_tickers}")
    return RsRatingScreenResult(
        run_date=run_date.isoformat(),
        benchmark_ticker=config.benchmark_ticker,
        total_tickers=total_tickers,
        passed_tickers=len(hits),
        failed_tickers=failures,
        hits=hits,
    )
