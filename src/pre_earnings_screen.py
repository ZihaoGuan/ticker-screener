from __future__ import annotations

from dataclasses import asdict, dataclass
import datetime as dt

from .config import AppConfig
from .cookstock_bridge import load_configured_cookstock


@dataclass(frozen=True)
class PreEarningsEvent:
    ticker: str
    earnings_date: str | None = None
    summary: str | None = None
    sector: str | None = None
    exchange: str | None = None


@dataclass(frozen=True)
class PreEarningsHit:
    ticker: str
    earnings_date: str | None
    earnings_summary: str | None
    sector: str | None
    exchange: str | None
    focus_score: float
    focus_grade: str
    trade_plan: str
    current_price: float
    market_cap_b: float | None
    avg_dollar_volume: float
    ema_fast_length: int
    ema_slow_length: int
    ema_long_length: int
    ema_fast: float
    ema_slow: float
    ema_long: float
    is_near_year_high: bool
    year_high: float
    distance_from_year_high_pct: float
    is_strong_rs: bool
    stock_return_vs_rs_window_pct: float
    benchmark_return_vs_rs_window_pct: float
    current_rs_line: float
    rs_line_high: float
    is_sector_etf_strong: bool
    sector_etf: str
    sector_etf_near_year_high: bool
    sector_etf_distance_from_year_high_pct: float | None
    sector_etf_return_vs_rs_window_pct: float | None
    sector_benchmark_return_vs_rs_window_pct: float | None
    recent_range_pct: float | None
    distribution_warning: bool
    distribution_days_count: int
    latest_distribution_date: str | None
    latest_distribution_volume_ratio: float | None
    market_memory_trend: str | None
    market_memory_strength_label: str | None
    market_memory_strength_score: float | None
    market_memory_price_position: str | None
    focus_reasons: str
    benchmark_ticker: str
    sector_name: str | None

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True)
class PreEarningsScreenResult:
    run_date: str
    benchmark_ticker: str
    total_tickers: int
    passed_tickers: int
    failed_tickers: list[dict[str, str]]
    hits: list[PreEarningsHit]

    def to_dict(self) -> dict[str, object]:
        return {
            "run_date": self.run_date,
            "benchmark_ticker": self.benchmark_ticker,
            "total_tickers": self.total_tickers,
            "passed_tickers": self.passed_tickers,
            "failed_tickers": self.failed_tickers,
            "hits": [item.to_dict() for item in self.hits],
        }


def _parse_bool(value: object) -> bool:
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() == "true"


def _parse_float(value: object) -> float | None:
    if value in (None, "", "NA", "n/a", "None"):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _parse_int(value: object) -> int:
    if value in (None, "", "NA", "n/a", "None"):
        return 0
    return int(value)


def _to_hit(event: PreEarningsEvent, payload: dict[str, object]) -> PreEarningsHit:
    return PreEarningsHit(
        ticker=event.ticker,
        earnings_date=event.earnings_date,
        earnings_summary=event.summary,
        sector=event.sector,
        exchange=event.exchange,
        focus_score=float(payload["earnings_focus_score"]),
        focus_grade=str(payload["earnings_focus_grade"]),
        trade_plan=str(payload["earnings_trade_plan"]),
        current_price=float(payload["current_price"]),
        market_cap_b=_parse_float(payload.get("market_cap_b")),
        avg_dollar_volume=float(payload["avg_dollar_volume"]),
        ema_fast_length=_parse_int(payload["ema_fast_length"]),
        ema_slow_length=_parse_int(payload["ema_slow_length"]),
        ema_long_length=_parse_int(payload["ema_long_length"]),
        ema_fast=float(payload["ema_fast"]),
        ema_slow=float(payload["ema_slow"]),
        ema_long=float(payload["ema_long"]),
        is_near_year_high=_parse_bool(payload["is_near_year_high"]),
        year_high=float(payload["year_high"]),
        distance_from_year_high_pct=float(payload["distance_from_year_high_pct"]),
        is_strong_rs=_parse_bool(payload["is_strong_rs"]),
        stock_return_vs_rs_window_pct=float(payload["stock_return_vs_rs_window_pct"]),
        benchmark_return_vs_rs_window_pct=float(payload["benchmark_return_vs_rs_window_pct"]),
        current_rs_line=float(payload["current_rs_line"]),
        rs_line_high=float(payload["rs_line_high"]),
        is_sector_etf_strong=_parse_bool(payload["is_sector_etf_strong"]),
        sector_etf=str(payload["sector_etf"]),
        sector_etf_near_year_high=_parse_bool(payload["sector_etf_near_year_high"]),
        sector_etf_distance_from_year_high_pct=_parse_float(payload.get("sector_etf_distance_from_year_high_pct")),
        sector_etf_return_vs_rs_window_pct=_parse_float(payload.get("sector_etf_return_vs_rs_window_pct")),
        sector_benchmark_return_vs_rs_window_pct=_parse_float(payload.get("sector_benchmark_return_vs_rs_window_pct")),
        recent_range_pct=_parse_float(payload.get("recent_range_pct")),
        distribution_warning=_parse_bool(payload["distribution_warning"]),
        distribution_days_count=_parse_int(payload["distribution_days_count"]),
        latest_distribution_date=None
        if str(payload.get("latest_distribution_date", "")).strip() in {"", "NA", "None"}
        else str(payload["latest_distribution_date"]),
        latest_distribution_volume_ratio=_parse_float(payload.get("latest_distribution_volume_ratio")),
        market_memory_trend=None
        if str(payload.get("market_memory_trend", "")).strip() in {"", "NA", "None"}
        else str(payload["market_memory_trend"]),
        market_memory_strength_label=None
        if str(payload.get("market_memory_strength_label", "")).strip() in {"", "NA", "None"}
        else str(payload["market_memory_strength_label"]),
        market_memory_strength_score=_parse_float(payload.get("market_memory_strength_score")),
        market_memory_price_position=None
        if str(payload.get("market_memory_price_position", "")).strip() in {"", "NA", "None"}
        else str(payload["market_memory_price_position"]),
        focus_reasons=str(payload.get("focus_reasons", "")),
        benchmark_ticker=str(payload["benchmark_ticker"]),
        sector_name=None
        if str(payload.get("sector_name", "")).strip() in {"", "NA", "None"}
        else str(payload["sector_name"]),
    )


def run_pre_earnings_screen(
    config: AppConfig,
    events: list[PreEarningsEvent],
) -> PreEarningsScreenResult:
    cookstock = load_configured_cookstock(config)
    batch = cookstock.batch_process(
        [event.ticker for event in events],
        "pre_earnings_focus_batch",
        sector_by_ticker={event.ticker: event.sector for event in events},
        benchmark_ticker=config.benchmark_ticker,
        earnings_event_by_ticker={
            event.ticker: event.earnings_date for event in events if event.earnings_date
        },
    )
    event_by_ticker = {event.ticker: event for event in events}
    hits: list[PreEarningsHit] = []
    failures: list[dict[str, str]] = []
    date_from = dt.date.today() - dt.timedelta(days=180)
    total_tickers = len(events)
    completed = 0
    passed_count = 0
    timeout_seconds = batch._get_ticker_timeout_seconds()
    retry_timeout_seconds = batch._get_pre_earnings_retry_timeout_seconds()

    print(
        f"starting pre-earnings screen: total={total_tickers}, "
        f"next_week={min(batch.earnings_event_by_ticker.values()) if batch.earnings_event_by_ticker else 'NA'}, "
        f"timeout={timeout_seconds}s, retry_timeout={retry_timeout_seconds}s"
    )

    for event in events:
        ticker = event.ticker
        try:
            batch._print_progress(completed, total_tickers, ticker, "running", passed_count)
            ticker_data, timed_out, retried = batch._run_pre_earnings_ticker_with_timeout(
                ticker, date_from
            )
            completed += 1
            if timed_out:
                failures.append(
                    {
                        "ticker": ticker,
                        "error": f"timeout after {retry_timeout_seconds if retried else timeout_seconds}s",
                    }
                )
                batch._print_progress(completed, total_tickers, ticker, "timeout", passed_count)
                continue
            if not ticker_data:
                status = "filtered_on_retry" if retried else "filtered"
                batch._print_progress(completed, total_tickers, ticker, status, passed_count)
                continue

            _, payload = next(iter(ticker_data.items()))
            hits.append(_to_hit(event_by_ticker[ticker], payload))
            passed_count += 1
            status = "scored_on_retry" if retried else "scored"
            batch._print_progress(completed, total_tickers, ticker, status, passed_count)
        except Exception as exc:
            completed += 1
            failures.append({"ticker": ticker, "error": str(exc)})
            batch._print_progress(completed, total_tickers, ticker, f"error: {exc}", passed_count)
            print(f"pre-earnings screening failed for {ticker}: {exc}")

    return PreEarningsScreenResult(
        run_date=dt.date.today().isoformat(),
        benchmark_ticker=config.benchmark_ticker,
        total_tickers=total_tickers,
        passed_tickers=len(hits),
        failed_tickers=failures,
        hits=hits,
    )
