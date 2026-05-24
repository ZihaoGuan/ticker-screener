from __future__ import annotations

from dataclasses import asdict, dataclass
import datetime as dt

import pandas as pd

from .config import AppConfig
from .cookstock_bridge import load_configured_cookstock
from .universe import UniverseTicker


EMA_FAST = 21
SMA_MEDIUM = 50
SMA_LONG = 200
PRICE_HISTORY_DAYS = 320
RECENT_LOSS_LOOKBACK_DAYS = 5
RECENT_RANGE_LOOKBACK_DAYS = 10
MAX_DISTANCE_TO_SMA50_ABOVE_PCT = 6.0
MAX_DISTANCE_TO_SMA50_BELOW_PCT = 2.0
MAX_DISTANCE_FROM_YEAR_HIGH_PCT = 15.0


@dataclass(frozen=True)
class Lost21EmaHit:
    ticker: str
    sector: str | None
    industry: str | None
    exchange: str | None
    benchmark_ticker: str
    current_price: float
    ema21: float
    sma50: float
    sma200: float
    distance_to_ema21_pct: float
    distance_to_sma50_pct: float
    distance_from_year_high_pct: float
    avg_volume_20: float
    avg_dollar_volume_20: float
    recent_low: float
    recent_high: float
    days_since_lost_ema21: int
    weekly_rs_new_high_recent: bool
    is_strong_rs: bool
    support_state: str
    reasons: list[str]

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True)
class Lost21EmaScreenResult:
    run_date: str
    benchmark_ticker: str
    total_tickers: int
    passed_tickers: int
    failed_tickers: list[dict[str, str]]
    hits: list[Lost21EmaHit]

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
    return sum(values[-length:]) / float(length)


def _pct_from_level(current_price: float, level: float) -> float:
    if level <= 0:
        return 0.0
    return ((current_price / level) - 1.0) * 100.0


def _days_since_recent_cross_under(closes: list[float], ema_series: pd.Series, lookback_days: int) -> int | None:
    close_series = pd.Series(closes, dtype=float)
    crosses = (close_series < ema_series) & (close_series.shift(1) >= ema_series.shift(1))
    recent_crosses = crosses.tail(max(1, lookback_days))
    if not bool(recent_crosses.any()):
        return None
    last_cross_index = int(recent_crosses[recent_crosses].index[-1])
    return len(close_series) - 1 - last_cross_index


def _to_hit(
    ticker: UniverseTicker,
    benchmark_ticker: str,
    *,
    current_price: float,
    ema21: float,
    sma50: float,
    sma200: float,
    distance_from_year_high_pct: float,
    avg_volume_20: float,
    avg_dollar_volume_20: float,
    recent_low: float,
    recent_high: float,
    days_since_lost_ema21: int,
    weekly_rs_new_high_recent: bool,
    is_strong_rs: bool,
) -> Lost21EmaHit:
    distance_to_ema21_pct = _pct_from_level(current_price, ema21)
    distance_to_sma50_pct = _pct_from_level(current_price, sma50)
    support_state = "testing_50d_support" if current_price >= sma50 else "lost_50d_support"
    reasons = [
        f"lost 21 EMA {days_since_lost_ema21}d ago",
        f"{distance_to_ema21_pct:+.1f}% vs 21 EMA {ema21:.2f}",
        f"{distance_to_sma50_pct:+.1f}% vs 50D MA {sma50:.2f}",
        f"50D MA {sma50:.2f} > 200D MA {sma200:.2f}",
        f"{distance_from_year_high_pct:.1f}% from 52-week high",
    ]
    if weekly_rs_new_high_recent:
        reasons.append("recent weekly RS new high")
    if is_strong_rs:
        reasons.append("strong RS vs benchmark")
    if support_state == "testing_50d_support":
        reasons.append("still testing 50D support from above")
    else:
        reasons.append("already slipped slightly below 50D support")
    return Lost21EmaHit(
        ticker=ticker.symbol,
        sector=ticker.sector,
        industry=ticker.industry,
        exchange=ticker.exchange,
        benchmark_ticker=benchmark_ticker,
        current_price=current_price,
        ema21=ema21,
        sma50=sma50,
        sma200=sma200,
        distance_to_ema21_pct=distance_to_ema21_pct,
        distance_to_sma50_pct=distance_to_sma50_pct,
        distance_from_year_high_pct=distance_from_year_high_pct,
        avg_volume_20=avg_volume_20,
        avg_dollar_volume_20=avg_dollar_volume_20,
        recent_low=recent_low,
        recent_high=recent_high,
        days_since_lost_ema21=days_since_lost_ema21,
        weekly_rs_new_high_recent=weekly_rs_new_high_recent,
        is_strong_rs=is_strong_rs,
        support_state=support_state,
        reasons=reasons,
    )


def run_lost_21ema_screen(config: AppConfig, tickers: list[UniverseTicker]) -> Lost21EmaScreenResult:
    cookstock = load_configured_cookstock(config)
    hits: list[Lost21EmaHit] = []
    failures: list[dict[str, str]] = []
    total_tickers = len(tickers)

    print(
        "starting lost-21EMA screen: "
        f"total={total_tickers}, "
        f"recent_loss_window={RECENT_LOSS_LOOKBACK_DAYS}d, "
        f"max_distance_above_50d={MAX_DISTANCE_TO_SMA50_ABOVE_PCT:.1f}%, "
        f"max_distance_below_50d={MAX_DISTANCE_TO_SMA50_BELOW_PCT:.1f}%, "
        f"leader_distance_from_high<={MAX_DISTANCE_FROM_YEAR_HIGH_PCT:.1f}%"
    )

    for position, ticker in enumerate(tickers, start=1):
        print(f"[{position}/{total_tickers}] screening {ticker.symbol} | passed={len(hits)}")
        try:
            financials = cookstock.cookFinancials(
                ticker.symbol,
                benchmarkTicker=config.benchmark_ticker,
                historyLookbackDays=PRICE_HISTORY_DAYS,
            )
            price_rows = financials._get_clean_price_data()
            closes = [float(item["close"]) for item in price_rows if item.get("close") is not None]
            if len(closes) < SMA_LONG:
                print(f"[{position}/{total_tickers}] {ticker.symbol} filtered: insufficient price history | passed={len(hits)}")
                continue

            close_series = pd.Series(closes, dtype=float)
            ema21_series = close_series.ewm(span=EMA_FAST, adjust=False).mean()
            current_price = float(close_series.iloc[-1])
            ema21 = float(ema21_series.iloc[-1])
            sma50 = _sma(closes, SMA_MEDIUM)
            sma200 = _sma(closes, SMA_LONG)
            if sma50 is None or sma200 is None:
                print(f"[{position}/{total_tickers}] {ticker.symbol} filtered: missing moving averages | passed={len(hits)}")
                continue
            if current_price >= ema21:
                print(
                    f"[{position}/{total_tickers}] {ticker.symbol} filtered: "
                    f"current {current_price:.2f} >= 21 EMA {ema21:.2f} | passed={len(hits)}"
                )
                continue
            if not (ema21 > sma50 > sma200):
                print(
                    f"[{position}/{total_tickers}] {ticker.symbol} filtered: "
                    f"trend stack failed (21EMA {ema21:.2f}, 50D {sma50:.2f}, 200D {sma200:.2f}) | passed={len(hits)}"
                )
                continue
            if current_price <= sma200:
                print(
                    f"[{position}/{total_tickers}] {ticker.symbol} filtered: "
                    f"current {current_price:.2f} <= 200D {sma200:.2f} | passed={len(hits)}"
                )
                continue

            days_since_lost_ema21 = _days_since_recent_cross_under(closes, ema21_series, RECENT_LOSS_LOOKBACK_DAYS)
            if days_since_lost_ema21 is None:
                print(
                    f"[{position}/{total_tickers}] {ticker.symbol} filtered: "
                    f"no recent 21 EMA cross-under in last {RECENT_LOSS_LOOKBACK_DAYS}d | passed={len(hits)}"
                )
                continue

            distance_to_sma50_pct = _pct_from_level(current_price, sma50)
            if distance_to_sma50_pct > MAX_DISTANCE_TO_SMA50_ABOVE_PCT or distance_to_sma50_pct < -MAX_DISTANCE_TO_SMA50_BELOW_PCT:
                print(
                    f"[{position}/{total_tickers}] {ticker.symbol} filtered: "
                    f"{distance_to_sma50_pct:+.2f}% vs 50D MA | passed={len(hits)}"
                )
                continue

            rs_summary = financials.get_rs_new_high_before_price_summary(
                sectorName=ticker.sector,
                benchmarkTicker=config.benchmark_ticker,
                signalProfile="weekly",
            )
            if not rs_summary:
                print(f"[{position}/{total_tickers}] {ticker.symbol} filtered: missing RS summary | passed={len(hits)}")
                continue
            distance_from_year_high_pct = float(rs_summary.get("distance_from_year_high_pct", 999.0))
            weekly_rs_new_high_recent = bool(rs_summary.get("weekly_rs_new_high_recent"))
            is_strong_rs = bool(rs_summary.get("is_strong_rs"))
            if not (weekly_rs_new_high_recent or is_strong_rs or distance_from_year_high_pct <= MAX_DISTANCE_FROM_YEAR_HIGH_PCT):
                print(
                    f"[{position}/{total_tickers}] {ticker.symbol} filtered: "
                    f"not a recent leader (weekly_rs_recent={weekly_rs_new_high_recent}, strong_rs={is_strong_rs}, "
                    f"distance_from_high={distance_from_year_high_pct:.2f}%) | passed={len(hits)}"
                )
                continue

            avg_volume_20 = float(financials._get_average_volume(20))
            avg_dollar_volume_20 = float(financials._get_average_dollar_volume(20))
            recent_rows = price_rows[-RECENT_RANGE_LOOKBACK_DAYS:]
            recent_low = min(float(item["low"]) for item in recent_rows if item.get("low") is not None)
            recent_high = max(float(item["high"]) for item in recent_rows if item.get("high") is not None)

            hit = _to_hit(
                ticker,
                config.benchmark_ticker,
                current_price=current_price,
                ema21=ema21,
                sma50=sma50,
                sma200=sma200,
                distance_from_year_high_pct=distance_from_year_high_pct,
                avg_volume_20=avg_volume_20,
                avg_dollar_volume_20=avg_dollar_volume_20,
                recent_low=recent_low,
                recent_high=recent_high,
                days_since_lost_ema21=days_since_lost_ema21,
                weekly_rs_new_high_recent=weekly_rs_new_high_recent,
                is_strong_rs=is_strong_rs,
            )
            hits.append(hit)
            print(
                f"[{position}/{total_tickers}] {ticker.symbol} passed: "
                f"{hit.support_state}, {hit.distance_to_sma50_pct:+.2f}% vs 50D, lost 21 EMA {hit.days_since_lost_ema21}d ago | passed={len(hits)}"
            )
        except Exception as exc:
            failures.append({"ticker": ticker.symbol, "error": str(exc)})
            print(f"[{position}/{total_tickers}] {ticker.symbol} error: {exc} | passed={len(hits)}")

    hits.sort(
        key=lambda item: (
            0 if item.support_state == "testing_50d_support" else 1,
            item.days_since_lost_ema21,
            abs(item.distance_to_sma50_pct),
            0 if item.weekly_rs_new_high_recent else 1,
            0 if item.is_strong_rs else 1,
            item.distance_from_year_high_pct,
            -item.avg_dollar_volume_20,
            item.ticker,
        )
    )

    return Lost21EmaScreenResult(
        run_date=dt.date.today().isoformat(),
        benchmark_ticker=config.benchmark_ticker,
        total_tickers=total_tickers,
        passed_tickers=len(hits),
        failed_tickers=failures,
        hits=hits,
    )
