from __future__ import annotations

from dataclasses import asdict, dataclass
import datetime as dt

import pandas as pd

from .config import AppConfig
from .cookstock_bridge import freeze_cookstock_today, iter_prefetched_cookstock_batches, load_configured_cookstock
from .universe import UniverseTicker


EMA10_PERIOD = 10
EMA21_PERIOD = 21
SMA50_PERIOD = 50
EMA21_PULLBACK_HISTORY_DAYS = 180
STRICT_MIN_DAYS_ABOVE_EMA21 = 5
STRICT_MIN_DAYS_SMA50_RISING = 5
MAX_EMA21_TESTS_PER_TREND = 6
EMA21_SLOPE_MARGIN = 1.001
EMA21_ABOVE_SMA50_MARGIN = 1.01


@dataclass(frozen=True)
class Ema21PullbackBuyHit:
    ticker: str
    sector: str | None
    industry: str | None
    exchange: str | None
    signal_date: str
    test_date: str
    current_price: float
    breakout_high: float
    breakout_open: float
    breakout_close: float
    ema10: float
    ema21: float
    sma50: float
    test_high: float
    test_low: float
    test_count: int
    reasons: list[str]

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True)
class Ema21PullbackBuyScreenResult:
    run_date: str
    total_tickers: int
    passed_tickers: int
    failed_tickers: list[dict[str, str]]
    hits: list[Ema21PullbackBuyHit]

    def to_dict(self) -> dict[str, object]:
        return {
            "run_date": self.run_date,
            "total_tickers": self.total_tickers,
            "passed_tickers": self.passed_tickers,
            "failed_tickers": self.failed_tickers,
            "hits": [item.to_dict() for item in self.hits],
        }


def _normalize_bars_frame(frame: pd.DataFrame) -> pd.DataFrame:
    required = ["Open", "High", "Low", "Close", "Volume"]
    available = {str(column).lower(): column for column in frame.columns}
    missing = [column for column in required if column.lower() not in available]
    if missing:
        return pd.DataFrame()
    normalized = frame[[available[column.lower()] for column in required]].copy()
    normalized.columns = required
    normalized = normalized.dropna(subset=required).sort_index()
    if not isinstance(normalized.index, pd.DatetimeIndex):
        normalized.index = pd.to_datetime(normalized.index)
    return normalized


def _build_price_frame(financials) -> pd.DataFrame:
    rows = financials._get_clean_price_data()
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
    return frame.dropna(subset=["Date", "Open", "High", "Low", "Close", "Volume"]).set_index("Date").sort_index()


def _true_streak(values: pd.Series) -> pd.Series:
    streak: list[int] = []
    current = 0
    for value in values.fillna(False).tolist():
        current = current + 1 if bool(value) else 0
        streak.append(current)
    return pd.Series(streak, index=values.index, dtype=int)


def find_recent_ema21_pullback_buy_hit(
    frame: pd.DataFrame,
    *,
    ticker: UniverseTicker,
) -> Ema21PullbackBuyHit | None:
    bars = _normalize_bars_frame(frame)
    if bars.empty or len(bars) < max(SMA50_PERIOD + 10, 70):
        return None

    close = bars["Close"].astype(float)
    open_ = bars["Open"].astype(float)
    high = bars["High"].astype(float)
    low = bars["Low"].astype(float)

    ema10 = close.ewm(span=EMA10_PERIOD, adjust=False).mean()
    ema21 = close.ewm(span=EMA21_PERIOD, adjust=False).mean()
    sma50 = close.rolling(SMA50_PERIOD).mean()
    ema_ready = ema21.notna() & sma50.notna()

    sma_rising = sma50 > sma50.shift(1)
    close_above_ema21 = close >= ema21
    sma_rising_streak = _true_streak(sma_rising)
    above_ema21_streak = _true_streak(close_above_ema21)

    strict_trend = (
        ema_ready
        & (close > ema21)
        & (close > sma50)
        & (ema21 > (sma50 * EMA21_ABOVE_SMA50_MARGIN))
        & (ema21 > (ema21.shift(1) * EMA21_SLOPE_MARGIN))
        & (sma_rising_streak >= STRICT_MIN_DAYS_SMA50_RISING)
        & (above_ema21_streak >= (STRICT_MIN_DAYS_ABOVE_EMA21 + 1))
    )

    test_count = 0
    last_test_high: float | None = None
    last_test_low: float | None = None
    last_test_index: int | None = None

    for index in range(1, len(bars)):
        if not bool(strict_trend.iloc[index]):
            test_count = 0
            last_test_high = None
            last_test_low = None
            last_test_index = None
            continue

        is_test_candidate = (
            bool(strict_trend.iloc[index])
            and bool(close.iloc[index - 1] > ema21.iloc[index - 1])
            and bool(low.iloc[index] <= ema21.iloc[index])
            and bool(close.iloc[index] > ema21.iloc[index])
        )
        if is_test_candidate:
            test_count += 1
            if test_count <= MAX_EMA21_TESTS_PER_TREND:
                last_test_high = float(high.iloc[index])
                last_test_low = float(low.iloc[index])
                last_test_index = index
            continue

        if (
            last_test_high is None
            or last_test_low is None
            or last_test_index is None
            or test_count > MAX_EMA21_TESTS_PER_TREND
            or index <= last_test_index
        ):
            continue

        is_buy = (
            bool(strict_trend.iloc[index])
            and bool(high.iloc[index] > last_test_high)
            and bool(close.iloc[index] > open_.iloc[index])
            and bool(open_.iloc[index] > ema21.iloc[index])
            and bool(close.iloc[index] > ema21.iloc[index])
        )
        if not is_buy:
            continue
        if index != len(bars) - 1:
            continue

        signal_date = bars.index[index].date().isoformat()
        test_date = bars.index[last_test_index].date().isoformat()
        reasons = [
            "strict uptrend: close above 21 EMA and 50 SMA with rising 21 EMA / 50 SMA",
            f"EMA21 test candle on {test_date} held above 21 EMA into the close",
            f"breakout bar took out test high {last_test_high:.2f} with bullish body above 21 EMA",
            f"test count {test_count}/{MAX_EMA21_TESTS_PER_TREND} in current trend",
        ]
        return Ema21PullbackBuyHit(
            ticker=ticker.symbol,
            sector=ticker.sector,
            industry=ticker.industry,
            exchange=ticker.exchange,
            signal_date=signal_date,
            test_date=test_date,
            current_price=float(close.iloc[index]),
            breakout_high=float(high.iloc[index]),
            breakout_open=float(open_.iloc[index]),
            breakout_close=float(close.iloc[index]),
            ema10=float(ema10.iloc[index]),
            ema21=float(ema21.iloc[index]),
            sma50=float(sma50.iloc[index]),
            test_high=last_test_high,
            test_low=last_test_low,
            test_count=test_count,
            reasons=reasons,
        )

    return None


def run_ema21_pullback_buy_screen(
    config: AppConfig,
    tickers: list[UniverseTicker],
    *,
    as_of_date: dt.date | None = None,
) -> Ema21PullbackBuyScreenResult:
    cookstock = load_configured_cookstock(config)
    hits: list[Ema21PullbackBuyHit] = []
    failures: list[dict[str, str]] = []
    total_tickers = len(tickers)
    run_date = as_of_date or dt.date.today()

    print(
        "starting ema21 pullback buy screen: "
        f"total={total_tickers}, strict_above_ema_days={STRICT_MIN_DAYS_ABOVE_EMA21}, "
        f"sma_rising_days={STRICT_MIN_DAYS_SMA50_RISING}, max_tests={MAX_EMA21_TESTS_PER_TREND}"
    )

    with freeze_cookstock_today(cookstock, as_of_date):
        position = 0
        for ticker_batch in iter_prefetched_cookstock_batches(
            config,
            tickers,
            as_of_date=as_of_date,
            history_lookback_days=EMA21_PULLBACK_HISTORY_DAYS,
            benchmark_ticker=config.benchmark_ticker,
        ):
            for ticker in ticker_batch:
                position += 1
                print(f"[{position}/{total_tickers}] screening {ticker.symbol} | passed={len(hits)}")
                try:
                    financials = cookstock.cookFinancials(
                        ticker.symbol,
                        benchmarkTicker=config.benchmark_ticker,
                        historyLookbackDays=EMA21_PULLBACK_HISTORY_DAYS,
                    )
                    frame = _build_price_frame(financials)
                    hit = find_recent_ema21_pullback_buy_hit(frame, ticker=ticker)
                    if hit is None:
                        print(f"[{position}/{total_tickers}] {ticker.symbol} filtered: no EMA21 pullback buy | passed={len(hits)}")
                        continue
                    hits.append(hit)
                    print(
                        f"[{position}/{total_tickers}] {ticker.symbol} passed EMA21 pullback buy "
                        f"test={hit.test_date} signal={hit.signal_date} | passed={len(hits)}"
                    )
                except Exception as exc:
                    failures.append({"ticker": ticker.symbol, "error": str(exc)})
                    print(f"[{position}/{total_tickers}] {ticker.symbol} failed: {exc}")

    return Ema21PullbackBuyScreenResult(
        run_date=run_date.isoformat(),
        total_tickers=total_tickers,
        passed_tickers=len(hits),
        failed_tickers=failures,
        hits=hits,
    )
