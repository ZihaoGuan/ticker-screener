from __future__ import annotations

from dataclasses import asdict, dataclass
import datetime as dt

from .config import AppConfig
from .cookstock_bridge import load_configured_cookstock
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
        reasons=list(summary.get("reasons", [])),
    )


def run_rs_screen(config: AppConfig, tickers: list[UniverseTicker]) -> ScreenResult:
    cookstock = load_configured_cookstock(config)
    hits: list[ScreenHit] = []
    failures: list[dict[str, str]] = []

    for position, ticker in enumerate(tickers, start=1):
        print(f"[{position}/{len(tickers)}] screening {ticker.symbol}")
        try:
            financials = cookstock.cookFinancials(
                ticker.symbol,
                benchmarkTicker=config.benchmark_ticker,
                historyLookbackDays=config.rs_new_high_history_days,
            )
            summary = financials.get_rs_new_high_before_price_summary(
                sectorName=ticker.sector,
                benchmarkTicker=config.benchmark_ticker,
            )
            if summary:
                hits.append(_to_hit(ticker, summary))
        except Exception as exc:
            failures.append({"ticker": ticker.symbol, "error": str(exc)})
            print(f"screening failed for {ticker.symbol}: {exc}")

    return ScreenResult(
        run_date=dt.date.today().isoformat(),
        benchmark_ticker=config.benchmark_ticker,
        total_tickers=len(tickers),
        passed_tickers=len(hits),
        failed_tickers=failures,
        hits=hits,
    )
