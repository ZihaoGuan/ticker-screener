from __future__ import annotations

from dataclasses import asdict, dataclass
import datetime as dt

from .config import AppConfig
from .cookstock_bridge import load_configured_cookstock
from .pre_earnings_screen import PreEarningsEvent


MA_SHORT = 20
MA_MEDIUM = 50
MA_LONG = 200
PRICE_HISTORY_DAYS = 320


@dataclass(frozen=True)
class PreEarningsMaStackHit:
    ticker: str
    earnings_date: str | None
    earnings_summary: str | None
    sector: str | None
    exchange: str | None
    benchmark_ticker: str
    current_price: float
    ma20: float
    ma50: float
    ma200: float
    distance_from_ma20_pct: float
    distance_from_ma50_pct: float
    distance_from_ma200_pct: float
    year_high: float
    distance_from_year_high_pct: float
    avg_volume_20: float
    avg_dollar_volume_20: float
    reasons: list[str]

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True)
class PreEarningsMaStackScreenResult:
    run_date: str
    benchmark_ticker: str
    total_tickers: int
    passed_tickers: int
    failed_tickers: list[dict[str, str]]
    hits: list[PreEarningsMaStackHit]

    def to_dict(self) -> dict[str, object]:
        return {
            "run_date": self.run_date,
            "benchmark_ticker": self.benchmark_ticker,
            "total_tickers": self.total_tickers,
            "passed_tickers": self.passed_tickers,
            "failed_tickers": self.failed_tickers,
            "hits": [item.to_dict() for item in self.hits],
        }


def _sma(values: list[float], length: int) -> float | None:
    if len(values) < length:
        return None
    window = values[-length:]
    return sum(window) / float(length)


def _pct_from_level(current_price: float, level: float) -> float:
    if level <= 0:
        return 0.0
    return ((current_price / level) - 1.0) * 100.0


def _to_hit(
    event: PreEarningsEvent,
    benchmark_ticker: str,
    *,
    current_price: float,
    ma20: float,
    ma50: float,
    ma200: float,
    year_high: float,
    avg_volume_20: float,
    avg_dollar_volume_20: float,
) -> PreEarningsMaStackHit:
    distance_from_ma20_pct = _pct_from_level(current_price, ma20)
    distance_from_ma50_pct = _pct_from_level(current_price, ma50)
    distance_from_ma200_pct = _pct_from_level(current_price, ma200)
    distance_from_year_high_pct = 0.0 if year_high <= 0 else ((year_high / current_price) - 1.0) * 100.0
    reasons = [
        f"earnings {event.earnings_date or 'next week'}",
        f"ma stack {ma20:.2f} > {ma50:.2f} > {ma200:.2f}",
        f"{distance_from_ma20_pct:+.1f}% vs MA20",
        f"{distance_from_year_high_pct:+.1f}% below 52W high",
    ]
    return PreEarningsMaStackHit(
        ticker=event.ticker,
        earnings_date=event.earnings_date,
        earnings_summary=event.summary,
        sector=event.sector,
        exchange=event.exchange,
        benchmark_ticker=benchmark_ticker,
        current_price=current_price,
        ma20=ma20,
        ma50=ma50,
        ma200=ma200,
        distance_from_ma20_pct=distance_from_ma20_pct,
        distance_from_ma50_pct=distance_from_ma50_pct,
        distance_from_ma200_pct=distance_from_ma200_pct,
        year_high=year_high,
        distance_from_year_high_pct=distance_from_year_high_pct,
        avg_volume_20=avg_volume_20,
        avg_dollar_volume_20=avg_dollar_volume_20,
        reasons=reasons,
    )


def run_pre_earnings_ma_stack_screen(
    config: AppConfig,
    events: list[PreEarningsEvent],
) -> PreEarningsMaStackScreenResult:
    cookstock = load_configured_cookstock(config)
    hits: list[PreEarningsMaStackHit] = []
    failures: list[dict[str, str]] = []
    total_tickers = len(events)

    print(
        "starting pre-earnings MA stack screen: "
        f"total={total_tickers}, "
        f"ma_stack=MA{MA_SHORT}>MA{MA_MEDIUM}>MA{MA_LONG}, "
        "market_cap_filter=post-screen > $1B"
    )

    for position, event in enumerate(events, start=1):
        ticker = event.ticker
        print(f"[{position}/{total_tickers}] screening {ticker} | passed={len(hits)}")
        try:
            financials = cookstock.cookFinancials(
                ticker,
                benchmarkTicker=config.benchmark_ticker,
                historyLookbackDays=PRICE_HISTORY_DAYS,
            )
            price_rows = financials._get_clean_price_data()
            closes = [float(item["close"]) for item in price_rows if item.get("close") is not None]
            if len(closes) < MA_LONG:
                print(f"[{position}/{total_tickers}] {ticker} filtered: insufficient price history | passed={len(hits)}")
                continue

            current_price = closes[-1]
            ma20 = _sma(closes, MA_SHORT)
            ma50 = _sma(closes, MA_MEDIUM)
            ma200 = _sma(closes, MA_LONG)
            if ma20 is None or ma50 is None or ma200 is None:
                print(f"[{position}/{total_tickers}] {ticker} filtered: missing moving averages | passed={len(hits)}")
                continue
            if not (ma20 > ma50 > ma200):
                print(
                    f"[{position}/{total_tickers}] {ticker} filtered: "
                    f"MA stack failed ({ma20:.2f}, {ma50:.2f}, {ma200:.2f}) | passed={len(hits)}"
                )
                continue

            year_high = max(float(item["high"]) for item in price_rows if item.get("high") is not None)
            avg_volume_20 = float(financials._get_average_volume(20))
            avg_dollar_volume_20 = float(financials._get_average_dollar_volume(20))
            hit = _to_hit(
                event,
                config.benchmark_ticker,
                current_price=current_price,
                ma20=ma20,
                ma50=ma50,
                ma200=ma200,
                year_high=year_high,
                avg_volume_20=avg_volume_20,
                avg_dollar_volume_20=avg_dollar_volume_20,
            )
            hits.append(hit)
            print(
                f"[{position}/{total_tickers}] {ticker} passed: "
                f"MA20 {ma20:.2f} > MA50 {ma50:.2f} > MA200 {ma200:.2f}, "
                f"{hit.distance_from_ma20_pct:+.2f}% vs MA20 | passed={len(hits)}"
            )
        except Exception as exc:
            failures.append({"ticker": ticker, "error": str(exc)})
            print(f"[{position}/{total_tickers}] {ticker} error: {exc}")

    hits.sort(
        key=lambda item: (
            abs(item.distance_from_ma20_pct),
            item.distance_from_year_high_pct,
            -item.avg_dollar_volume_20,
            item.ticker,
        )
    )

    return PreEarningsMaStackScreenResult(
        run_date=dt.date.today().isoformat(),
        benchmark_ticker=config.benchmark_ticker,
        total_tickers=total_tickers,
        passed_tickers=len(hits),
        failed_tickers=failures,
        hits=hits,
    )
