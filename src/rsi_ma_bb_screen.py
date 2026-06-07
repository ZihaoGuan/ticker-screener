from __future__ import annotations

from dataclasses import asdict, dataclass
import datetime as dt
from typing import Literal

import pandas as pd

from .config import AppConfig
from .cookstock_bridge import freeze_cookstock_today, iter_prefetched_cookstock_batches, load_configured_cookstock
from .universe import UniverseTicker


RsiSignalDirection = Literal["bullish", "bearish"]
RSI_LENGTH = 14
RSI_SIGNAL_LENGTH = 14
RSI_BB_LENGTH = 14
RSI_BB_MULTIPLIER = 2.0
RSI_MIN_BARS_BETWEEN_SIGNALS = 5
RSI_OVERSOLD_LOWER = 20.0
RSI_OVERSOLD_UPPER = 40.0
RSI_OVERBOUGHT_LOWER = 60.0
RSI_OVERBOUGHT_UPPER = 80.0
RSI_RECENT_WINDOW_BARS = 2
RSI_HISTORY_DAYS = 240


@dataclass(frozen=True)
class RsiMaBbHit:
    ticker: str
    sector: str | None
    industry: str | None
    exchange: str | None
    signal_date: str
    signal_age_bars: int
    direction: RsiSignalDirection
    signal_sources: list[str]
    current_price: float
    signal_close: float
    rsi_value: float
    rsi_ma_value: float
    rsi_bb_mid_value: float
    rsi_bb_upper_value: float
    rsi_bb_lower_value: float
    reasons: list[str]

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True)
class RsiMaBbScreenResult:
    run_date: str
    direction: RsiSignalDirection
    total_tickers: int
    passed_tickers: int
    failed_tickers: list[dict[str, str]]
    hits: list[RsiMaBbHit]

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


def _compute_rsi(close: pd.Series, length: int) -> pd.Series:
    change = close.diff()
    up = change.clip(lower=0.0)
    down = -change.clip(upper=0.0)
    avg_up = up.ewm(alpha=1 / length, adjust=False, min_periods=length).mean()
    avg_down = down.ewm(alpha=1 / length, adjust=False, min_periods=length).mean()
    rs = avg_up / avg_down
    rsi = 100 - (100 / (1 + rs))
    rsi = rsi.where(avg_down != 0, 100.0)
    rsi = rsi.where(avg_up != 0, 0.0)
    both_flat = (avg_up == 0) & (avg_down == 0)
    return rsi.where(~both_flat, 50.0)


def find_recent_rsi_ma_bb_hit(
    frame: pd.DataFrame,
    *,
    ticker: UniverseTicker,
    direction: RsiSignalDirection,
) -> RsiMaBbHit | None:
    bars = _normalize_bars_frame(frame)
    minimum_bars = max(RSI_LENGTH, RSI_SIGNAL_LENGTH, RSI_BB_LENGTH) + RSI_MIN_BARS_BETWEEN_SIGNALS + RSI_RECENT_WINDOW_BARS + 5
    if bars.empty or len(bars) < minimum_bars:
        return None

    close = bars["Close"]
    rsi = _compute_rsi(close, RSI_LENGTH)
    rsi_ma = rsi.rolling(RSI_SIGNAL_LENGTH, min_periods=RSI_SIGNAL_LENGTH).mean()
    rsi_bb_mid = rsi.rolling(RSI_BB_LENGTH, min_periods=RSI_BB_LENGTH).mean()
    rsi_bb_std = rsi.rolling(RSI_BB_LENGTH, min_periods=RSI_BB_LENGTH).std(ddof=0) * RSI_BB_MULTIPLIER
    rsi_bb_upper = rsi_bb_mid + rsi_bb_std
    rsi_bb_lower = rsi_bb_mid - rsi_bb_std

    last_buy_bar: int | None = None
    last_sell_bar: int | None = None
    triggered: list[tuple[int, list[str]]] = []

    for index in range(1, len(bars)):
        current_rsi = rsi.iloc[index]
        if pd.isna(current_rsi):
            continue
        previous_rsi = rsi.iloc[index - 1]
        current_ma = rsi_ma.iloc[index]
        previous_ma = rsi_ma.iloc[index - 1]
        current_bb_upper = rsi_bb_upper.iloc[index]
        previous_bb_upper = rsi_bb_upper.iloc[index - 1]
        current_bb_lower = rsi_bb_lower.iloc[index]
        previous_bb_lower = rsi_bb_lower.iloc[index - 1]

        ma_buy = (
            pd.notna(previous_ma)
            and pd.notna(current_ma)
            and previous_rsi <= previous_ma
            and current_rsi > current_ma
            and current_rsi >= RSI_OVERSOLD_LOWER
            and current_rsi <= RSI_OVERSOLD_UPPER
            and (last_buy_bar is None or index - last_buy_bar >= RSI_MIN_BARS_BETWEEN_SIGNALS)
        )
        ma_sell = (
            pd.notna(previous_ma)
            and pd.notna(current_ma)
            and previous_rsi >= previous_ma
            and current_rsi < current_ma
            and current_rsi >= RSI_OVERBOUGHT_LOWER
            and current_rsi <= RSI_OVERBOUGHT_UPPER
            and (last_sell_bar is None or index - last_sell_bar >= RSI_MIN_BARS_BETWEEN_SIGNALS)
        )
        bb_buy = (
            pd.notna(previous_bb_lower)
            and pd.notna(current_bb_lower)
            and previous_rsi <= previous_bb_lower
            and current_rsi > current_bb_lower
            and current_rsi >= RSI_OVERSOLD_LOWER
            and current_rsi <= RSI_OVERSOLD_UPPER
            and (last_buy_bar is None or index - last_buy_bar >= RSI_MIN_BARS_BETWEEN_SIGNALS)
        )
        bb_sell = (
            pd.notna(previous_bb_upper)
            and pd.notna(current_bb_upper)
            and previous_rsi >= previous_bb_upper
            and current_rsi < current_bb_upper
            and current_rsi >= RSI_OVERBOUGHT_LOWER
            and current_rsi <= RSI_OVERBOUGHT_UPPER
            and (last_sell_bar is None or index - last_sell_bar >= RSI_MIN_BARS_BETWEEN_SIGNALS)
        )

        if direction == "bullish":
            sources = [label for active, label in ((ma_buy, "MA"), (bb_buy, "BB")) if active]
            if sources:
                last_buy_bar = index
                triggered.append((index, sources))
        else:
            sources = [label for active, label in ((ma_sell, "MA"), (bb_sell, "BB")) if active]
            if sources:
                last_sell_bar = index
                triggered.append((index, sources))

    if not triggered:
        return None

    signal_position, signal_sources = triggered[-1]
    signal_age_bars = len(bars) - 1 - signal_position
    if signal_age_bars >= RSI_RECENT_WINDOW_BARS:
        return None

    signal_date = bars.index[signal_position]
    current_rsi = float(rsi.iloc[signal_position])
    source_phrase = "+".join(signal_sources)
    side_label = "buy" if direction == "bullish" else "sell"
    reasons = [
        f"RSI {side_label} signal from {source_phrase} within last {RSI_RECENT_WINDOW_BARS} bars",
        f"RSI {current_rsi:.2f} inside {'oversold' if direction == 'bullish' else 'overbought'} filter zone",
    ]
    if signal_age_bars == 0:
        reasons.append("signal happened on latest bar")
    else:
        reasons.append(f"signal happened {signal_age_bars} bar(s) ago")

    return RsiMaBbHit(
        ticker=ticker.symbol,
        sector=ticker.sector,
        industry=ticker.industry,
        exchange=ticker.exchange,
        signal_date=signal_date.date().isoformat(),
        signal_age_bars=signal_age_bars,
        direction=direction,
        signal_sources=signal_sources,
        current_price=float(close.iloc[-1]),
        signal_close=float(close.iloc[signal_position]),
        rsi_value=current_rsi,
        rsi_ma_value=float(rsi_ma.iloc[signal_position]),
        rsi_bb_mid_value=float(rsi_bb_mid.iloc[signal_position]),
        rsi_bb_upper_value=float(rsi_bb_upper.iloc[signal_position]),
        rsi_bb_lower_value=float(rsi_bb_lower.iloc[signal_position]),
        reasons=reasons,
    )


def run_rsi_ma_bb_screen(
    config: AppConfig,
    tickers: list[UniverseTicker],
    *,
    direction: RsiSignalDirection,
    as_of_date: dt.date | None = None,
) -> RsiMaBbScreenResult:
    cookstock = load_configured_cookstock(config)
    hits: list[RsiMaBbHit] = []
    failures: list[dict[str, str]] = []
    total_tickers = len(tickers)
    run_date = as_of_date or dt.date.today()

    print(
        f"starting rsi ma/bb {direction} screen: total={total_tickers}, rsi_length={RSI_LENGTH}, recent_window={RSI_RECENT_WINDOW_BARS}, reentry_pause={RSI_MIN_BARS_BETWEEN_SIGNALS}"
    )

    with freeze_cookstock_today(cookstock, as_of_date):
        position = 0
        for ticker_batch in iter_prefetched_cookstock_batches(
            config,
            tickers,
            as_of_date=as_of_date,
            history_lookback_days=RSI_HISTORY_DAYS,
            benchmark_ticker=config.benchmark_ticker,
        ):
            for ticker in ticker_batch:
                position += 1
                print(f"[{position}/{total_tickers}] screening {ticker.symbol} | passed={len(hits)}")
                try:
                    financials = cookstock.cookFinancials(
                        ticker.symbol,
                        benchmarkTicker=config.benchmark_ticker,
                        historyLookbackDays=RSI_HISTORY_DAYS,
                    )
                    frame = _build_price_frame(financials)
                    hit = find_recent_rsi_ma_bb_hit(frame, ticker=ticker, direction=direction)
                    if hit is None:
                        print(f"[{position}/{total_tickers}] {ticker.symbol} filtered: no RSI MA/BB {direction} trigger | passed={len(hits)}")
                        continue
                    hits.append(hit)
                    print(f"[{position}/{total_tickers}] {ticker.symbol} passed RSI MA/BB {direction} | passed={len(hits)}")
                except Exception as exc:
                    failures.append({"ticker": ticker.symbol, "error": str(exc)})
                    print(f"[{position}/{total_tickers}] {ticker.symbol} failed: {exc}")

    print(f"finished rsi ma/bb {direction} screen: passed={len(hits)}, failed={len(failures)}, total={total_tickers}")
    return RsiMaBbScreenResult(
        run_date=run_date.isoformat(),
        direction=direction,
        total_tickers=total_tickers,
        passed_tickers=len(hits),
        failed_tickers=failures,
        hits=hits,
    )
