from __future__ import annotations

from dataclasses import asdict, dataclass
import datetime as dt
from typing import Literal

import pandas as pd

from .config import AppConfig
from .cookstock_bridge import freeze_cookstock_today, iter_prefetched_cookstock_batches, load_configured_cookstock
from .universe import UniverseTicker


SignalDirection = Literal["bullish", "bearish"]
HISTORY_DAYS = 120
SETUP_BARS = 9
COMPARE_LOOKBACK = 4


@dataclass(frozen=True)
class TdSequentialHit:
    ticker: str
    sector: str | None
    industry: str | None
    exchange: str | None
    signal_date: str
    direction: SignalDirection
    setup_count: int
    current_price: float
    signal_close: float
    comparison_close: float
    reasons: list[str]

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True)
class TdSequentialScreenResult:
    run_date: str
    direction: SignalDirection
    total_tickers: int
    passed_tickers: int
    failed_tickers: list[dict[str, str]]
    hits: list[TdSequentialHit]

    def to_dict(self) -> dict[str, object]:
        return {
            "run_date": self.run_date,
            "direction": self.direction,
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


def _setup_counts(condition: pd.Series) -> pd.Series:
    counts: list[int] = []
    running = 0
    for value in condition.fillna(False).tolist():
        if bool(value):
            running += 1
        else:
            running = 0
        counts.append(running)
    return pd.Series(counts, index=condition.index, dtype=int)


def find_recent_td_sequential_hit(
    frame: pd.DataFrame,
    *,
    ticker: UniverseTicker,
    direction: SignalDirection,
) -> TdSequentialHit | None:
    bars = _normalize_bars_frame(frame)
    minimum_bars = SETUP_BARS + COMPARE_LOOKBACK + 2
    if bars.empty or len(bars) < minimum_bars:
        return None

    comparison_close = bars["Close"].shift(COMPARE_LOOKBACK)
    if direction == "bullish":
        condition = bars["Close"] > comparison_close
        direction_label = "bullish"
        reason_template = "close {close:.2f} > close[4] {comparison:.2f}"
    else:
        condition = bars["Close"] < comparison_close
        direction_label = "bearish"
        reason_template = "close {close:.2f} < close[4] {comparison:.2f}"

    counts = _setup_counts(condition)
    latest_count = int(counts.iloc[-1])
    if latest_count != SETUP_BARS:
        return None

    latest_close = float(bars["Close"].iloc[-1])
    latest_comparison = float(comparison_close.iloc[-1])
    reasons = [
        f"{direction_label} TD Sequential setup completed on latest bar",
        reason_template.format(close=latest_close, comparison=latest_comparison),
        f"consecutive setup count reached {SETUP_BARS}",
    ]
    return TdSequentialHit(
        ticker=ticker.symbol,
        sector=ticker.sector,
        industry=ticker.industry,
        exchange=ticker.exchange,
        signal_date=bars.index[-1].date().isoformat(),
        direction=direction,
        setup_count=latest_count,
        current_price=latest_close,
        signal_close=latest_close,
        comparison_close=latest_comparison,
        reasons=reasons,
    )


def run_td_sequential_screen(
    config: AppConfig,
    tickers: list[UniverseTicker],
    *,
    direction: SignalDirection,
    as_of_date: dt.date | None = None,
) -> TdSequentialScreenResult:
    cookstock = load_configured_cookstock(config)
    hits: list[TdSequentialHit] = []
    failures: list[dict[str, str]] = []
    total_tickers = len(tickers)
    run_date = as_of_date or dt.date.today()

    print(f"starting td sequential {direction} screen: total={total_tickers}")

    with freeze_cookstock_today(cookstock, as_of_date):
        position = 0
        for ticker_batch in iter_prefetched_cookstock_batches(
            config,
            tickers,
            as_of_date=as_of_date,
            history_lookback_days=HISTORY_DAYS,
            benchmark_ticker=config.benchmark_ticker,
        ):
            for ticker in ticker_batch:
                position += 1
                print(f"[{position}/{total_tickers}] screening {ticker.symbol} | passed={len(hits)}")
                try:
                    financials = cookstock.cookFinancials(
                        ticker.symbol,
                        benchmarkTicker=config.benchmark_ticker,
                        historyLookbackDays=HISTORY_DAYS,
                    )
                    frame = _build_price_frame(financials)
                    hit = find_recent_td_sequential_hit(frame, ticker=ticker, direction=direction)
                    if hit is None:
                        print(f"[{position}/{total_tickers}] {ticker.symbol} filtered: no {direction} TD9 | passed={len(hits)}")
                        continue
                    hits.append(hit)
                    print(f"[{position}/{total_tickers}] {ticker.symbol} passed {direction} TD9 | passed={len(hits)}")
                except Exception as exc:
                    failures.append({"ticker": ticker.symbol, "error": str(exc)})
                    print(f"[{position}/{total_tickers}] {ticker.symbol} failed: {exc}")

    print(f"finished td sequential {direction} screen: passed={len(hits)}, failed={len(failures)}, total={total_tickers}")
    return TdSequentialScreenResult(
        run_date=run_date.isoformat(),
        direction=direction,
        total_tickers=total_tickers,
        passed_tickers=len(hits),
        failed_tickers=failures,
        hits=hits,
    )
