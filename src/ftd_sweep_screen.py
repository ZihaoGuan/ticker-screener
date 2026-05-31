from __future__ import annotations

from dataclasses import asdict, dataclass
import datetime as dt

import numpy as np
import pandas as pd

from .config import AppConfig
from .cookstock_bridge import freeze_cookstock_today, iter_prefetched_cookstock_batches, load_configured_cookstock
from .universe import UniverseTicker


@dataclass(frozen=True)
class FtdSweepHit:
    ticker: str
    sector: str | None
    exchange: str | None
    benchmark_ticker: str
    current_price: float
    ftd_date: str
    sweep_start_date: str
    sweep_breakout_date: str
    bars_since_breakout: int
    bars_from_ftd_to_breakout: int
    ftd_high: float
    ftd_pivot_low: float
    sweep_low: float
    sweep_depth_pct: float
    breakout_level: float
    breakout_distance_pct: float
    breakout_volume_ratio: float
    ftd_volume_ratio: float
    avg_volume_20: float
    avg_dollar_volume_20: float
    reasons: list[str]

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True)
class FtdSweepScreenResult:
    run_date: str
    benchmark_ticker: str
    total_tickers: int
    passed_tickers: int
    failed_tickers: list[dict[str, str]]
    hits: list[FtdSweepHit]

    def to_dict(self) -> dict[str, object]:
        return {
            "run_date": self.run_date,
            "benchmark_ticker": self.benchmark_ticker,
            "total_tickers": self.total_tickers,
            "passed_tickers": self.passed_tickers,
            "failed_tickers": self.failed_tickers,
            "hits": [item.to_dict() for item in self.hits],
        }


@dataclass(frozen=True)
class _FtdEvent:
    index: int
    date: str
    high: float
    pivot_low: float
    volume_ratio: float


@dataclass(frozen=True)
class _SweepCandidate:
    ftd_event: _FtdEvent
    sweep_start_index: int
    breakout_index: int
    sweep_low: float


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


def _compute_obv(close: np.ndarray, volume: np.ndarray) -> np.ndarray:
    obv = np.zeros(len(close), dtype=float)
    for index in range(1, len(close)):
        if close[index] > close[index - 1]:
            obv[index] = obv[index - 1] + volume[index]
        elif close[index] < close[index - 1]:
            obv[index] = obv[index - 1] - volume[index]
        else:
            obv[index] = obv[index - 1]
    return obv


def _pivot_lows(values: np.ndarray, left: int, right: int) -> np.ndarray:
    pivots = np.full(len(values), np.nan, dtype=float)
    if left < 1 or right < 0:
        return pivots
    for index in range(left, len(values) - right):
        window = values[index - left : index + right + 1]
        current = values[index]
        if np.isfinite(current) and current == np.min(window):
            pivots[index] = current
    return pivots


def _find_ftd_events(frame: pd.DataFrame, config: AppConfig) -> list[_FtdEvent]:
    if frame.empty or len(frame) < max(int(config.ftd_sweep_pivot_lookback_left) + 5, 35):
        return []

    left = int(config.ftd_sweep_pivot_lookback_left)
    right = int(config.ftd_sweep_pivot_lookback_right)
    confirm_window = int(config.ftd_sweep_confirm_window_days)
    ftd_window = int(config.ftd_sweep_ftd_window_days)
    exhaustion_days = int(config.ftd_sweep_exhaustion_days)
    recover_body_multiplier = float(config.ftd_sweep_recovery_body_multiplier)
    recover_volume_multiplier = float(config.ftd_sweep_recovery_volume_multiplier)
    breakout_volume_multiplier = float(config.ftd_sweep_breakout_volume_multiplier)

    open_values = frame["Open"].to_numpy(dtype=float)
    high_values = frame["High"].to_numpy(dtype=float)
    low_values = frame["Low"].to_numpy(dtype=float)
    close_values = frame["Close"].to_numpy(dtype=float)
    volume_values = frame["Volume"].to_numpy(dtype=float)
    dates = frame.index

    obv_values = _compute_obv(close_values, volume_values)
    close_sma_21 = pd.Series(close_values, index=dates).rolling(21).mean().to_numpy(dtype=float)
    volume_sma_21 = pd.Series(volume_values, index=dates).rolling(21).mean().to_numpy(dtype=float)
    obv_sma_9 = pd.Series(obv_values, index=dates).rolling(9).mean().to_numpy(dtype=float)

    pivot_lows = _pivot_lows(low_values, left, right)
    bullish_body_pct = np.where(close_values > open_values, (close_values - open_values) * 100.0 / close_values, 0.0)

    events: list[_FtdEvent] = []
    last_pivot_low = np.nan
    downtrend = -1
    recover_index: int | None = None
    confirm_index: int | None = None
    exhaust_accum = 0
    exhaustion_ready = False

    for index in range(1, len(frame)):
        pivot_value = pivot_lows[index]
        if np.isfinite(pivot_value):
            previous_pivot_low = last_pivot_low
            last_pivot_low = float(pivot_value)
            if np.isfinite(previous_pivot_low) and previous_pivot_low > last_pivot_low and downtrend == -1:
                downtrend = 1
                recover_index = None
                confirm_index = None
                exhaust_accum = 0
                exhaustion_ready = False
            elif np.isfinite(previous_pivot_low) and previous_pivot_low < last_pivot_low and downtrend == 1:
                downtrend = -1
                recover_index = None
                confirm_index = None
                exhaust_accum = 0
                exhaustion_ready = False

        if downtrend != 1 or not np.isfinite(last_pivot_low):
            continue

        recover_body = close_values[index] - open_values[index]
        previous_body = close_values[index - 1] - open_values[index - 1]
        recover_ready = (
            recover_body > 0
            and recover_body > recover_body_multiplier * previous_body
            and np.isfinite(volume_sma_21[index])
            and volume_values[index] > recover_volume_multiplier * volume_values[index - 1]
            and volume_values[index] > volume_sma_21[index]
        )
        if recover_ready and confirm_index is None:
            recover_index = index
            confirm_index = None
            exhaust_accum = 0
            exhaustion_ready = False

        if recover_index is None:
            continue
        if confirm_index is None and index > recover_index + confirm_window:
            recover_index = None
            confirm_index = None
            exhaust_accum = 0
            exhaustion_ready = False
            continue

        confirm_recover = close_values[index] > last_pivot_low and close_values[index - 1] > last_pivot_low
        if confirm_recover and confirm_index is None:
            confirm_index = index

        if confirm_index is None:
            continue
        if index > confirm_index + ftd_window:
            recover_index = None
            confirm_index = None
            exhaust_accum = 0
            exhaustion_ready = False
            continue

        exhaustion_bar = (
            open_values[index] > close_values[index]
            and np.isfinite(obv_sma_9[index])
            and np.isfinite(volume_sma_21[index])
            and obv_values[index] > obv_sma_9[index]
            and volume_values[index] < volume_values[index - 1]
            and volume_values[index] < volume_sma_21[index]
        )
        if exhaustion_bar and not exhaustion_ready:
            exhaust_accum += 1
            if exhaust_accum >= exhaustion_days:
                exhaustion_ready = True

        bullish_average = bullish_body_pct[: index + 1]
        bullish_average = bullish_average[bullish_average > 0]
        avg_bullish_body_pct = float(bullish_average.mean()) if bullish_average.size else 0.0
        ftd_ready = (
            exhaustion_ready
            and confirm_recover
            and bullish_body_pct[index] > avg_bullish_body_pct
            and np.isfinite(close_sma_21[index])
            and close_values[index] > close_sma_21[index]
            and volume_values[index] > breakout_volume_multiplier * volume_values[index - 1]
            and obv_values[index] > obv_values[index - 1]
            and np.isfinite(obv_sma_9[index])
            and obv_values[index] > obv_sma_9[index]
        )
        if not ftd_ready:
            continue

        events.append(
            _FtdEvent(
                index=index,
                date=dates[index].date().isoformat(),
                high=float(high_values[index]),
                pivot_low=float(last_pivot_low),
                volume_ratio=float(volume_values[index] / volume_values[index - 1]) if volume_values[index - 1] > 0 else 0.0,
            )
        )
        recover_index = None
        confirm_index = None
        exhaust_accum = 0
        exhaustion_ready = False

    return events


def _find_recent_sweep_breakout(
    frame: pd.DataFrame,
    config: AppConfig,
    ftd_events: list[_FtdEvent],
) -> _SweepCandidate | None:
    if not ftd_events:
        return None

    close_values = frame["Close"].to_numpy(dtype=float)
    low_values = frame["Low"].to_numpy(dtype=float)
    post_ftd_days = int(config.ftd_sweep_post_ftd_days)
    sweep_range_days = int(config.ftd_sweep_sweep_range_days)
    recent_lookback_days = int(config.ftd_sweep_recent_breakout_lookback_days)
    latest_index = len(frame) - 1
    candidates: list[_SweepCandidate] = []

    for event in ftd_events:
        threshold = event.high
        post_end = min(latest_index, event.index + post_ftd_days)
        index = event.index + 1
        while index <= post_end:
            if close_values[index] < threshold:
                sweep_start = index
                sweep_window_end = min(post_end, sweep_start + sweep_range_days)
                sweep_low = float(low_values[sweep_start])
                for breakout_index in range(sweep_start + 1, sweep_window_end + 1):
                    sweep_low = min(sweep_low, float(low_values[breakout_index]))
                    if close_values[breakout_index - 1] <= threshold and close_values[breakout_index] > threshold:
                        bars_since_breakout = latest_index - breakout_index
                        held_breakout = bool(np.all(close_values[breakout_index:] >= threshold))
                        if bars_since_breakout <= recent_lookback_days and held_breakout:
                            candidates.append(
                                _SweepCandidate(
                                    ftd_event=event,
                                    sweep_start_index=sweep_start,
                                    breakout_index=breakout_index,
                                    sweep_low=sweep_low,
                                )
                            )
                        break
                index = sweep_start + 1
                continue
            index += 1

    if not candidates:
        return None
    candidates.sort(key=lambda item: (item.breakout_index, item.ftd_event.index))
    return candidates[-1]


def find_recent_ftd_sweep_hit(
    frame: pd.DataFrame,
    *,
    ticker: UniverseTicker,
    benchmark_ticker: str,
    config: AppConfig,
) -> FtdSweepHit | None:
    normalized = _normalize_bars_frame(frame)
    if normalized.empty or len(normalized) < max(int(config.ftd_sweep_pivot_lookback_left) + 15, 40):
        return None

    avg_volume_20 = float(normalized["Volume"].tail(20).mean()) if len(normalized) >= 20 else float(normalized["Volume"].mean())
    current_price = float(normalized["Close"].iloc[-1])
    avg_dollar_volume_20 = float((normalized["Close"] * normalized["Volume"]).tail(20).mean())
    if avg_volume_20 < int(config.ftd_sweep_min_avg_volume):
        return None
    if avg_dollar_volume_20 < float(config.ftd_sweep_min_avg_dollar_volume):
        return None

    ftd_events = _find_ftd_events(normalized, config)
    candidate = _find_recent_sweep_breakout(normalized, config, ftd_events)
    if candidate is None:
        return None

    volume_sma_21 = normalized["Volume"].rolling(21).mean()
    breakout_level = float(candidate.ftd_event.high)
    breakout_index = int(candidate.breakout_index)
    breakout_volume_ma = float(volume_sma_21.iloc[breakout_index]) if pd.notna(volume_sma_21.iloc[breakout_index]) else 0.0
    breakout_volume_ratio = (
        float(normalized["Volume"].iloc[breakout_index] / breakout_volume_ma) if breakout_volume_ma > 0 else 0.0
    )
    sweep_depth_pct = ((breakout_level - float(candidate.sweep_low)) / breakout_level) * 100.0 if breakout_level > 0 else 0.0
    breakout_distance_pct = ((current_price - breakout_level) / breakout_level) * 100.0 if breakout_level > 0 else 0.0
    bars_since_breakout = len(normalized) - 1 - breakout_index
    bars_from_ftd_to_breakout = breakout_index - int(candidate.ftd_event.index)

    reasons = [
        f"FTD on {candidate.ftd_event.date}",
        f"sweep reclaim on {normalized.index[breakout_index].date().isoformat()}",
        f"held above FTD high for {bars_since_breakout + 1} bar(s)",
        f"sweep depth {sweep_depth_pct:.1f}%",
        f"breakout still {breakout_distance_pct:+.1f}% vs FTD high",
    ]

    return FtdSweepHit(
        ticker=ticker.symbol,
        sector=ticker.sector,
        exchange=ticker.exchange,
        benchmark_ticker=benchmark_ticker,
        current_price=current_price,
        ftd_date=candidate.ftd_event.date,
        sweep_start_date=normalized.index[candidate.sweep_start_index].date().isoformat(),
        sweep_breakout_date=normalized.index[breakout_index].date().isoformat(),
        bars_since_breakout=bars_since_breakout,
        bars_from_ftd_to_breakout=bars_from_ftd_to_breakout,
        ftd_high=breakout_level,
        ftd_pivot_low=float(candidate.ftd_event.pivot_low),
        sweep_low=float(candidate.sweep_low),
        sweep_depth_pct=sweep_depth_pct,
        breakout_level=breakout_level,
        breakout_distance_pct=breakout_distance_pct,
        breakout_volume_ratio=breakout_volume_ratio,
        ftd_volume_ratio=float(candidate.ftd_event.volume_ratio),
        avg_volume_20=avg_volume_20,
        avg_dollar_volume_20=avg_dollar_volume_20,
        reasons=reasons,
    )


def run_ftd_sweep_screen(
    config: AppConfig,
    tickers: list[UniverseTicker],
    *,
    as_of_date: dt.date | None = None,
) -> FtdSweepScreenResult:
    cookstock = load_configured_cookstock(config)
    hits: list[FtdSweepHit] = []
    failures: list[dict[str, str]] = []
    history_days = max(int(config.ftd_sweep_history_days), 120)
    total_tickers = len(tickers)
    run_date = as_of_date or dt.date.today()

    print(
        "starting FTD sweep screen: "
        f"total={total_tickers}, "
        f"history={history_days}, "
        f"recent_breakout_window={int(config.ftd_sweep_recent_breakout_lookback_days)}, "
        f"sweep_range={int(config.ftd_sweep_sweep_range_days)}"
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
                    frame = _build_price_frame(financials)
                    hit = find_recent_ftd_sweep_hit(
                        frame,
                        ticker=ticker,
                        benchmark_ticker=config.benchmark_ticker,
                        config=config,
                    )
                    if hit is None:
                        print(f"[{position}/{total_tickers}] {ticker.symbol} filtered: no recent FTD sweep breakout | passed={len(hits)}")
                        continue
                    hits.append(hit)
                    print(
                        f"[{position}/{total_tickers}] {ticker.symbol} passed: "
                        f"breakout {hit.sweep_breakout_date}, "
                        f"depth {hit.sweep_depth_pct:.1f}%, "
                        f"distance {hit.breakout_distance_pct:+.1f}% | passed={len(hits)}"
                    )
                except Exception as exc:
                    failures.append({"ticker": ticker.symbol, "error": str(exc)})
                    print(f"[{position}/{total_tickers}] {ticker.symbol} error: {exc} | passed={len(hits)}")

    return FtdSweepScreenResult(
        run_date=run_date.isoformat(),
        benchmark_ticker=config.benchmark_ticker,
        total_tickers=total_tickers,
        passed_tickers=len(hits),
        failed_tickers=failures,
        hits=hits,
    )
