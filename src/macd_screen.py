from __future__ import annotations

from dataclasses import asdict, dataclass
import datetime as dt
from typing import Literal

import pandas as pd

from .config import AppConfig
from .cookstock_bridge import freeze_cookstock_today, iter_prefetched_cookstock_batches, load_configured_cookstock
from .universe import UniverseTicker


SignalDirection = Literal["golden_cross", "dead_cross"]
MACD_FAST_LENGTH = 12
MACD_SLOW_LENGTH = 26
MACD_SIGNAL_LENGTH = 9
MACD_RECENT_WINDOW_BARS = 2
MACD_HISTORY_DAYS = 180


@dataclass(frozen=True)
class MacdHit:
    ticker: str
    sector: str | None
    industry: str | None
    exchange: str | None
    signal_date: str
    signal_age_bars: int
    direction: SignalDirection
    current_price: float
    signal_close: float
    macd_value: float
    signal_value: float
    histogram_value: float
    reasons: list[str]

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True)
class MacdScreenResult:
    run_date: str
    direction: SignalDirection
    total_tickers: int
    passed_tickers: int
    failed_tickers: list[dict[str, str]]
    hits: list[MacdHit]

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


def find_recent_macd_hit(
    frame: pd.DataFrame,
    *,
    ticker: UniverseTicker,
    direction: SignalDirection,
) -> MacdHit | None:
    bars = _normalize_bars_frame(frame)
    minimum_bars = MACD_SLOW_LENGTH + MACD_SIGNAL_LENGTH + MACD_RECENT_WINDOW_BARS + 5
    if bars.empty or len(bars) < minimum_bars:
        return None

    close = bars["Close"]
    ema_fast = close.ewm(span=MACD_FAST_LENGTH, adjust=False).mean()
    ema_slow = close.ewm(span=MACD_SLOW_LENGTH, adjust=False).mean()
    macd_line = ema_fast - ema_slow
    signal_line = macd_line.ewm(span=MACD_SIGNAL_LENGTH, adjust=False).mean()
    histogram = macd_line - signal_line

    previous_macd = macd_line.shift(1)
    previous_signal = signal_line.shift(1)
    golden_cross = (macd_line > signal_line) & (previous_macd <= previous_signal)
    dead_cross = (macd_line < signal_line) & (previous_macd >= previous_signal)
    crosses = golden_cross if direction == "golden_cross" else dead_cross

    recent_crosses = crosses[crosses].tail(MACD_RECENT_WINDOW_BARS)
    if recent_crosses.empty:
        return None

    signal_date = recent_crosses.index[-1]
    signal_position = int(bars.index.get_loc(signal_date))
    signal_age_bars = len(bars) - 1 - signal_position
    if signal_age_bars >= MACD_RECENT_WINDOW_BARS:
        return None

    direction_label = "golden cross" if direction == "golden_cross" else "dead cross"
    reasons = [
        f"MACD {direction_label} fired within last {MACD_RECENT_WINDOW_BARS} bars",
        f"MACD {macd_line.iloc[signal_position]:+.4f} vs signal {signal_line.iloc[signal_position]:+.4f}",
    ]
    if signal_age_bars == 0:
        reasons.append("cross happened on latest bar")
    else:
        reasons.append(f"cross happened {signal_age_bars} bar(s) ago")

    return MacdHit(
        ticker=ticker.symbol,
        sector=ticker.sector,
        industry=ticker.industry,
        exchange=ticker.exchange,
        signal_date=signal_date.date().isoformat(),
        signal_age_bars=signal_age_bars,
        direction=direction,
        current_price=float(close.iloc[-1]),
        signal_close=float(close.iloc[signal_position]),
        macd_value=float(macd_line.iloc[signal_position]),
        signal_value=float(signal_line.iloc[signal_position]),
        histogram_value=float(histogram.iloc[signal_position]),
        reasons=reasons,
    )


def run_macd_screen(
    config: AppConfig,
    tickers: list[UniverseTicker],
    *,
    direction: SignalDirection,
    as_of_date: dt.date | None = None,
) -> MacdScreenResult:
    cookstock = load_configured_cookstock(config)
    hits: list[MacdHit] = []
    failures: list[dict[str, str]] = []
    total_tickers = len(tickers)
    run_date = as_of_date or dt.date.today()

    print(
        f"starting macd {direction} screen: total={total_tickers}, fast={MACD_FAST_LENGTH}, slow={MACD_SLOW_LENGTH}, signal={MACD_SIGNAL_LENGTH}, recent_window={MACD_RECENT_WINDOW_BARS}"
    )

    with freeze_cookstock_today(cookstock, as_of_date):
        position = 0
        for ticker_batch in iter_prefetched_cookstock_batches(
            config,
            tickers,
            as_of_date=as_of_date,
            history_lookback_days=MACD_HISTORY_DAYS,
            benchmark_ticker=config.benchmark_ticker,
        ):
            for ticker in ticker_batch:
                position += 1
                print(f"[{position}/{total_tickers}] screening {ticker.symbol} | passed={len(hits)}")
                try:
                    financials = cookstock.cookFinancials(
                        ticker.symbol,
                        benchmarkTicker=config.benchmark_ticker,
                        historyLookbackDays=MACD_HISTORY_DAYS,
                    )
                    frame = _build_price_frame(financials)
                    hit = find_recent_macd_hit(frame, ticker=ticker, direction=direction)
                    if hit is None:
                        print(f"[{position}/{total_tickers}] {ticker.symbol} filtered: no MACD {direction} | passed={len(hits)}")
                        continue
                    hits.append(hit)
                    print(f"[{position}/{total_tickers}] {ticker.symbol} passed MACD {direction} | passed={len(hits)}")
                except Exception as exc:
                    failures.append({"ticker": ticker.symbol, "error": str(exc)})
                    print(f"[{position}/{total_tickers}] {ticker.symbol} failed: {exc}")

    print(f"finished macd {direction} screen: passed={len(hits)}, failed={len(failures)}, total={total_tickers}")
    return MacdScreenResult(
        run_date=run_date.isoformat(),
        direction=direction,
        total_tickers=total_tickers,
        passed_tickers=len(hits),
        failed_tickers=failures,
        hits=hits,
    )
