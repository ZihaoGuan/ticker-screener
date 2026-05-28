from __future__ import annotations

from dataclasses import asdict, dataclass
import datetime as dt
from typing import NamedTuple

import numpy as np
import pandas as pd

from .config import AppConfig
from .market_data_access import db_frame_has_recent_coverage, load_daily_bars_frame_from_db, resolve_market_data_source
from .universe import UniverseTicker


class Swing(NamedTuple):
    index: int
    price: float
    direction: int  # +1 high pivot, -1 low pivot


@dataclass(frozen=True)
class CupHandleHit:
    ticker: str
    sector: str | None
    exchange: str | None
    signal_date: str
    benchmark_ticker: str
    pattern_direction: str
    breakout_date: str
    current_price: float
    breakout_price: float
    stop_price: float
    target_price: float
    left_rim_date: str
    left_rim_price: float
    bowl_date: str
    bowl_price: float
    right_rim_date: str
    right_rim_price: float
    handle_date: str
    handle_price: float
    cup_width_bars: int
    handle_width_bars: int
    depth_pct: float
    handle_retrace_pct: float
    rim_difference_pct: float
    containment_ratio: float
    breakout_volume_ratio: float
    average_volume_50: float
    breakout_volume: float
    neckline_slope_pct: float
    reasons: list[str]

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True)
class CupHandleScreenResult:
    run_date: str
    benchmark_ticker: str
    total_tickers: int
    passed_tickers: int
    failed_tickers: list[dict[str, str]]
    hits: list[CupHandleHit]

    def to_dict(self) -> dict[str, object]:
        return {
            "run_date": self.run_date,
            "benchmark_ticker": self.benchmark_ticker,
            "total_tickers": self.total_tickers,
            "passed_tickers": self.passed_tickers,
            "failed_tickers": self.failed_tickers,
            "hits": [item.to_dict() for item in self.hits],
        }


def _load_yfinance():
    try:
        import yfinance as yf
    except ImportError as exc:
        raise RuntimeError("yfinance is not installed.") from exc
    return yf


def _period_to_start_date(period: str, as_of_date: dt.date) -> dt.date:
    value = str(period).strip().lower()
    if value.endswith("mo"):
        months = int(value[:-2])
        return as_of_date - dt.timedelta(days=max(31, months * 31))
    if value.endswith("y"):
        years = int(value[:-1])
        return as_of_date - dt.timedelta(days=max(366, years * 366))
    if value.endswith("d"):
        days = int(value[:-1])
        return as_of_date - dt.timedelta(days=max(30, days))
    return as_of_date - dt.timedelta(days=550)


def _fetch_history(ticker: str, period: str, *, as_of_date: dt.date | None = None) -> pd.DataFrame:
    market_data_source = resolve_market_data_source()
    start_date = _period_to_start_date(period, as_of_date or dt.date.today())
    end_date = as_of_date or dt.date.today()
    if market_data_source == "database-first":
        db_frame = load_daily_bars_frame_from_db(ticker, start_date, end_date)
        if db_frame is not None and db_frame_has_recent_coverage(db_frame, end_date):
            clean = db_frame.loc[:, ["Open", "High", "Low", "Close", "Volume"]].copy()
            clean = clean.dropna(subset=["High", "Low", "Close", "Volume"])
            if not clean.empty:
                return clean

    yf = _load_yfinance()
    if as_of_date is not None:
        history = yf.download(
            tickers=ticker,
            start=start_date.isoformat(),
            end=(as_of_date + dt.timedelta(days=1)).isoformat(),
            interval="1d",
            auto_adjust=False,
            progress=False,
            threads=False,
        )
    else:
        history = yf.download(
            tickers=ticker,
            period=period,
            interval="1d",
            auto_adjust=False,
            progress=False,
            threads=False,
        )
    if history is None or history.empty:
        raise RuntimeError("No history returned")
    if isinstance(history.columns, pd.MultiIndex):
        history.columns = history.columns.get_level_values(0)
    history = history.rename(columns=str)
    required = {"Open", "High", "Low", "Close", "Volume"}
    missing = required.difference(history.columns)
    if missing:
        raise RuntimeError(f"Missing required columns: {sorted(missing)}")
    clean = history.loc[:, ["Open", "High", "Low", "Close", "Volume"]].copy()
    clean = clean.dropna(subset=["High", "Low", "Close", "Volume"])
    return clean


def _build_swings(df: pd.DataFrame, span: int) -> list[Swing]:
    highs = df["High"].to_numpy(dtype=float)
    lows = df["Low"].to_numpy(dtype=float)
    swings: list[Swing] = []

    for idx in range(span, len(df) - span):
        high_window = highs[idx - span : idx + span + 1]
        low_window = lows[idx - span : idx + span + 1]
        is_high = highs[idx] >= np.max(high_window)
        is_low = lows[idx] <= np.min(low_window)

        if is_high:
            swings.append(Swing(idx, float(highs[idx]), +1))
        if is_low:
            swings.append(Swing(idx, float(lows[idx]), -1))

    compressed: list[Swing] = []
    for swing in sorted(swings, key=lambda item: (item.index, -item.direction)):
        if not compressed:
            compressed.append(swing)
            continue
        last = compressed[-1]
        if swing.index == last.index:
            if swing.direction == last.direction:
                better_same_bar = swing.price > last.price if swing.direction == 1 else swing.price < last.price
                if better_same_bar:
                    compressed[-1] = swing
            continue
        if swing.direction == last.direction:
            better = swing.price > last.price if swing.direction == 1 else swing.price < last.price
            if better:
                compressed[-1] = swing
            continue
        compressed.append(swing)
    return compressed


def _linear_baseline(start_price: float, end_price: float, length: int) -> np.ndarray:
    return np.linspace(start_price, end_price, length)


def _cup_containment_ratio(
    closes: np.ndarray,
    highs: np.ndarray,
    lows: np.ndarray,
    left_rim: float,
    right_rim: float,
    bowl: float,
    direction: int,
    tolerance_ratio: float,
) -> float:
    length = len(closes)
    if length <= 2:
        return 0.0

    baseline = _linear_baseline(left_rim, right_rim, length)
    rim_reference = min(left_rim, right_rim) if direction == 1 else max(left_rim, right_rim)
    depth = abs(rim_reference - bowl)
    curve = np.sin(np.linspace(0.0, np.pi, length))
    tolerance = max(depth * tolerance_ratio, 1e-9)

    if direction == 1:
        bowl_curve = baseline - depth * curve
        contained = (
            (closes <= baseline + tolerance)
            & (closes >= bowl_curve - tolerance)
            & (lows >= bowl_curve - tolerance * 1.25)
            & (highs <= baseline + tolerance * 1.5)
        )
    else:
        bowl_curve = baseline + depth * curve
        contained = (
            (closes >= baseline - tolerance)
            & (closes <= bowl_curve + tolerance)
            & (highs <= bowl_curve + tolerance * 1.25)
            & (lows >= baseline - tolerance * 1.5)
        )
    return float(np.mean(contained.astype(float)))


def _avg_volume(volumes: pd.Series, end_index: int, window: int) -> float:
    start_index = max(0, end_index - window + 1)
    segment = volumes.iloc[start_index : end_index + 1]
    if segment.empty:
        return 0.0
    return float(segment.mean())


def _find_breakout_index(
    closes: np.ndarray,
    *,
    start_index: int,
    breakout_price: float,
    direction: int,
    lookback_bars: int,
) -> int | None:
    window_start = max(start_index + 1, len(closes) - max(1, lookback_bars))
    for idx in range(window_start, len(closes)):
        previous_close = closes[idx - 1] if idx > 0 else closes[idx]
        if direction == 1:
            crossed = closes[idx] > breakout_price and previous_close <= breakout_price
        else:
            crossed = closes[idx] < breakout_price and previous_close >= breakout_price
        if crossed:
            return idx
    return None


def _build_reasons(
    *,
    direction: int,
    cup_width_bars: int,
    handle_width_bars: int,
    depth_pct: float,
    handle_retrace_pct: float,
    containment_ratio: float,
    breakout_volume_ratio: float,
) -> list[str]:
    direction_label = "bullish" if direction == 1 else "bearish"
    return [
        f"{direction_label} cup width {cup_width_bars} bars",
        f"handle width {handle_width_bars} bars",
        f"cup depth {depth_pct * 100:.1f}%",
        f"handle retrace {handle_retrace_pct * 100:.1f}%",
        f"containment {containment_ratio * 100:.1f}%",
        f"breakout volume {breakout_volume_ratio:.2f}x 50-day average",
    ]


def _candidate_score(
    *,
    containment_ratio: float,
    breakout_volume_ratio: float,
    cup_width_bars: int,
    handle_retrace_pct: float,
) -> float:
    return (
        containment_ratio * 100.0
        + min(breakout_volume_ratio, 3.0) * 10.0
        + min(cup_width_bars / 10.0, 15.0)
        - abs(handle_retrace_pct - 0.25) * 25.0
    )


def _find_best_pattern(df: pd.DataFrame, config: AppConfig) -> dict[str, object] | None:
    swings = _build_swings(df, max(2, int(config.cup_handle_pivot_span)))
    if len(swings) < 4:
        return None

    highs = df["High"].to_numpy(dtype=float)
    lows = df["Low"].to_numpy(dtype=float)
    closes = df["Close"].to_numpy(dtype=float)
    volumes = df["Volume"]
    best_candidate: dict[str, object] | None = None
    best_score = float("-inf")
    last_index = len(df) - 1

    for start in range(len(swings) - 3):
        s0, s1, s2, s3 = swings[start : start + 4]
        direction = 0
        if config.cup_handle_enable_bullish and (s0.direction, s1.direction, s2.direction, s3.direction) == (1, -1, 1, -1):
            direction = 1
        elif config.cup_handle_enable_bearish and (s0.direction, s1.direction, s2.direction, s3.direction) == (-1, 1, -1, 1):
            direction = -1
        if direction == 0:
            continue

        left_rim, bowl, right_rim, handle = s0, s1, s2, s3
        cup_width_bars = right_rim.index - left_rim.index
        handle_width_bars = handle.index - right_rim.index
        if cup_width_bars < config.cup_handle_min_cup_bars or cup_width_bars > config.cup_handle_max_cup_bars:
            continue
        if handle_width_bars < 3:
            continue
        max_handle_bars = max(4, int(round(cup_width_bars * config.cup_handle_max_handle_bars_ratio)))
        if handle_width_bars > max_handle_bars:
            continue
        if last_index - handle.index > cup_width_bars:
            continue

        rim_reference = min(left_rim.price, right_rim.price) if direction == 1 else max(left_rim.price, right_rim.price)
        if rim_reference <= 0:
            continue
        depth = abs(rim_reference - bowl.price)
        if depth <= 0:
            continue
        depth_pct = depth / rim_reference
        if depth_pct < config.cup_handle_min_depth_pct or depth_pct > config.cup_handle_max_depth_pct:
            continue

        rim_difference_pct = abs(left_rim.price - right_rim.price) / rim_reference
        if rim_difference_pct > config.cup_handle_rim_tolerance_pct:
            continue

        midpoint_index = left_rim.index + (cup_width_bars // 2)
        midpoint_offset = abs(bowl.index - midpoint_index) / max(cup_width_bars, 1)
        if midpoint_offset > 0.30:
            continue

        if direction == 1:
            handle_retrace_pct = (rim_reference - handle.price) / depth
            breakout_price = max(left_rim.price, right_rim.price)
            invalid_handle = handle.price <= bowl.price
            stop_price = float(handle.price)
            target_price = float(breakout_price + depth)
        else:
            handle_retrace_pct = (handle.price - rim_reference) / depth
            breakout_price = min(left_rim.price, right_rim.price)
            invalid_handle = handle.price >= bowl.price
            stop_price = float(handle.price)
            target_price = float(breakout_price - depth)
        if invalid_handle:
            continue
        if handle_retrace_pct < config.cup_handle_min_handle_retrace or handle_retrace_pct > config.cup_handle_max_handle_retrace:
            continue
        breakout_index = _find_breakout_index(
            closes,
            start_index=handle.index,
            breakout_price=float(breakout_price),
            direction=direction,
            lookback_bars=int(config.cup_handle_breakout_lookback_bars),
        )
        if breakout_index is None:
            continue
        if direction == 1 and closes[last_index] <= breakout_price:
            continue
        if direction == -1 and closes[last_index] >= breakout_price:
            continue

        cup_slice = slice(left_rim.index, right_rim.index + 1)
        containment_ratio = _cup_containment_ratio(
            closes=closes[cup_slice],
            highs=highs[cup_slice],
            lows=lows[cup_slice],
            left_rim=left_rim.price,
            right_rim=right_rim.price,
            bowl=bowl.price,
            direction=direction,
            tolerance_ratio=config.cup_handle_curve_tolerance_ratio,
        )
        if containment_ratio < config.cup_handle_min_containment_ratio:
            continue

        avg_volume_50 = _avg_volume(volumes, breakout_index, int(config.cup_handle_volume_average_days))
        breakout_volume = float(volumes.iloc[breakout_index])
        breakout_volume_ratio = breakout_volume / avg_volume_50 if avg_volume_50 > 0 else 0.0
        if config.cup_handle_require_volume_confirmation and breakout_volume_ratio < config.breakout_volume_ratio:
            continue

        neckline_slope_pct = abs(left_rim.price - right_rim.price) / max(depth, 1e-9)
        candidate_score = _candidate_score(
            containment_ratio=containment_ratio,
            breakout_volume_ratio=breakout_volume_ratio,
            cup_width_bars=cup_width_bars,
            handle_retrace_pct=handle_retrace_pct,
        )
        if candidate_score <= best_score:
            continue

        best_score = candidate_score
        best_candidate = {
            "direction": direction,
            "left_rim": left_rim,
            "bowl": bowl,
            "right_rim": right_rim,
            "handle": handle,
            "breakout_index": breakout_index,
            "breakout_price": float(breakout_price),
            "stop_price": stop_price,
            "target_price": target_price,
            "cup_width_bars": cup_width_bars,
            "handle_width_bars": handle_width_bars,
            "depth_pct": float(depth_pct),
            "handle_retrace_pct": float(handle_retrace_pct),
            "rim_difference_pct": float(rim_difference_pct),
            "containment_ratio": float(containment_ratio),
            "avg_volume_50": float(avg_volume_50),
            "breakout_volume": breakout_volume,
            "breakout_volume_ratio": float(breakout_volume_ratio),
            "neckline_slope_pct": float(neckline_slope_pct),
            "current_price": float(closes[last_index]),
        }
    return best_candidate


def _date_at(df: pd.DataFrame, index: int) -> str:
    value = df.index[index]
    if hasattr(value, "date"):
        return value.date().isoformat()
    return str(value)


def _to_hit(ticker: UniverseTicker, config: AppConfig, df: pd.DataFrame, candidate: dict[str, object]) -> CupHandleHit:
    direction = int(candidate["direction"])
    left_rim = candidate["left_rim"]
    bowl = candidate["bowl"]
    right_rim = candidate["right_rim"]
    handle = candidate["handle"]
    breakout_index = int(candidate["breakout_index"])
    reasons = _build_reasons(
        direction=direction,
        cup_width_bars=int(candidate["cup_width_bars"]),
        handle_width_bars=int(candidate["handle_width_bars"]),
        depth_pct=float(candidate["depth_pct"]),
        handle_retrace_pct=float(candidate["handle_retrace_pct"]),
        containment_ratio=float(candidate["containment_ratio"]),
        breakout_volume_ratio=float(candidate["breakout_volume_ratio"]),
    )
    return CupHandleHit(
        ticker=ticker.symbol,
        sector=ticker.sector,
        exchange=ticker.exchange,
        signal_date=df.index[-1].date().isoformat(),
        benchmark_ticker=config.benchmark_ticker,
        pattern_direction="bullish" if direction == 1 else "bearish",
        breakout_date=_date_at(df, breakout_index),
        current_price=float(candidate["current_price"]),
        breakout_price=float(candidate["breakout_price"]),
        stop_price=float(candidate["stop_price"]),
        target_price=float(candidate["target_price"]),
        left_rim_date=_date_at(df, left_rim.index),
        left_rim_price=float(left_rim.price),
        bowl_date=_date_at(df, bowl.index),
        bowl_price=float(bowl.price),
        right_rim_date=_date_at(df, right_rim.index),
        right_rim_price=float(right_rim.price),
        handle_date=_date_at(df, handle.index),
        handle_price=float(handle.price),
        cup_width_bars=int(candidate["cup_width_bars"]),
        handle_width_bars=int(candidate["handle_width_bars"]),
        depth_pct=float(candidate["depth_pct"]),
        handle_retrace_pct=float(candidate["handle_retrace_pct"]),
        rim_difference_pct=float(candidate["rim_difference_pct"]),
        containment_ratio=float(candidate["containment_ratio"]),
        breakout_volume_ratio=float(candidate["breakout_volume_ratio"]),
        average_volume_50=float(candidate["avg_volume_50"]),
        breakout_volume=float(candidate["breakout_volume"]),
        neckline_slope_pct=float(candidate["neckline_slope_pct"]),
        reasons=reasons,
    )


def run_cup_handle_screen(
    config: AppConfig,
    tickers: list[UniverseTicker],
    *,
    as_of_date: dt.date | None = None,
) -> CupHandleScreenResult:
    hits: list[CupHandleHit] = []
    failures: list[dict[str, str]] = []
    run_date = as_of_date or dt.date.today()
    total_tickers = len(tickers)

    for position, ticker in enumerate(tickers, start=1):
        print(f"[{position}/{total_tickers}] screening {ticker.symbol} | passed={len(hits)}")
        try:
            history = _fetch_history(ticker.symbol, config.cup_handle_history_period, as_of_date=as_of_date)
            candidate = _find_best_pattern(history, config)
            if candidate is None:
                continue
            hits.append(_to_hit(ticker, config, history, candidate))
        except Exception as exc:
            failures.append({"ticker": ticker.symbol, "error": str(exc)})
            print(f"[{position}/{total_tickers}] {ticker.symbol} error: {exc} | passed={len(hits)}")

    print(f"screen complete: passed={len(hits)}, failed={len(failures)}, total={total_tickers}")

    return CupHandleScreenResult(
        run_date=run_date.isoformat(),
        benchmark_ticker=config.benchmark_ticker,
        total_tickers=total_tickers,
        passed_tickers=len(hits),
        failed_tickers=failures,
        hits=hits,
    )
