from __future__ import annotations

import datetime as dt
from typing import Any

import pandas as pd

from ..market_extension import resample_to_weekly
from .constants import (
    TECHNICAL_INDICATOR_LABEL_BUY,
    TECHNICAL_INDICATOR_LABEL_NEUTRAL,
    TECHNICAL_INDICATOR_LABEL_SELL,
    TECHNICAL_INDICATOR_LABEL_STRONG_BUY,
    TECHNICAL_INDICATOR_LABEL_STRONG_SELL,
    TECHNICAL_INDICATOR_STATUS_MISSING_METRICS,
    TECHNICAL_INDICATOR_STATUS_OK,
    TECHNICAL_INDICATOR_TIMEFRAMES,
)
from .models import TechnicalIndicatorRatingSnapshot


def resample_to_monthly(frame: pd.DataFrame) -> pd.DataFrame:
    required = ["Open", "High", "Low", "Close", "Volume"]
    missing = [column for column in required if column not in frame.columns]
    if missing:
        raise ValueError(f"Missing required columns: {', '.join(missing)}")
    normalized = frame.copy()
    if not isinstance(normalized.index, pd.DatetimeIndex):
        normalized.index = pd.to_datetime(normalized.index)
    normalized = normalized.sort_index()
    monthly = normalized.resample("ME").agg(
        {
            "Open": "first",
            "High": "max",
            "Low": "min",
            "Close": "last",
            "Volume": "sum",
        }
    )
    monthly = monthly.dropna(subset=["Open", "High", "Low", "Close"])
    return monthly


def build_multi_timeframe_technical_indicator_ratings(
    ticker: str,
    frame: pd.DataFrame | None,
    *,
    as_of_date: dt.date,
) -> list[TechnicalIndicatorRatingSnapshot]:
    normalized_ticker = str(ticker or "").strip().upper()
    base = frame.sort_index().copy() if frame is not None and not frame.empty else pd.DataFrame()
    if not base.empty and not isinstance(base.index, pd.DatetimeIndex):
        base.index = pd.to_datetime(base.index)
    if not base.empty:
        base = base.loc[base.index <= pd.Timestamp(as_of_date)]

    timeframe_map: dict[str, pd.DataFrame] = {
        "1d": base,
        "1w": resample_to_weekly(base[["Open", "High", "Low", "Close", "Volume"]]) if not base.empty else pd.DataFrame(),
        "1m": resample_to_monthly(base[["Open", "High", "Low", "Close", "Volume"]]) if not base.empty else pd.DataFrame(),
    }
    ratings: list[TechnicalIndicatorRatingSnapshot] = []
    for timeframe in TECHNICAL_INDICATOR_TIMEFRAMES:
        ratings.append(build_technical_indicator_rating(normalized_ticker, timeframe_map.get(timeframe), timeframe=timeframe, as_of_date=as_of_date))
    return ratings


def build_technical_indicator_rating(
    ticker: str,
    frame: pd.DataFrame | None,
    *,
    timeframe: str,
    as_of_date: dt.date,
) -> TechnicalIndicatorRatingSnapshot:
    rating = TechnicalIndicatorRatingSnapshot(ticker=ticker.upper(), as_of_date=as_of_date, timeframe=timeframe)
    if timeframe not in TECHNICAL_INDICATOR_TIMEFRAMES:
        rating.technical_status = TECHNICAL_INDICATOR_STATUS_MISSING_METRICS
        rating.technical_status_reason = f"Unsupported timeframe: {timeframe}"
        rating.missing_metric_names = ["timeframe"]
        return rating
    if frame is None or frame.empty:
        rating.technical_status = TECHNICAL_INDICATOR_STATUS_MISSING_METRICS
        rating.technical_status_reason = "No OHLCV bars available for timeframe."
        rating.missing_metric_names = ["ohlcv"]
        return rating

    normalized = frame.copy()
    if not isinstance(normalized.index, pd.DatetimeIndex):
        normalized.index = pd.to_datetime(normalized.index)
    normalized = normalized.sort_index()
    for column in ("Open", "High", "Low", "Close", "Volume"):
        normalized[column] = pd.to_numeric(normalized[column], errors="coerce")
    normalized = normalized.dropna(subset=["Open", "High", "Low", "Close", "Volume"])
    if normalized.empty:
        rating.technical_status = TECHNICAL_INDICATOR_STATUS_MISSING_METRICS
        rating.technical_status_reason = "OHLCV bars resolved empty after normalization."
        rating.missing_metric_names = ["ohlcv"]
        return rating

    components, missing_metric_names = _calculate_indicator_components(normalized)
    if missing_metric_names:
        rating.technical_status = TECHNICAL_INDICATOR_STATUS_MISSING_METRICS
        rating.technical_status_reason = "One or more technical indicator inputs are unavailable."
        rating.missing_metric_names = sorted(set(missing_metric_names))
        return rating

    ma_values = [
        components[f"sma{length}_signal"] for length in (10, 20, 30, 50, 100, 200)
    ] + [
        components[f"ema{length}_signal"] for length in (10, 20, 30, 50, 100, 200)
    ] + [
        components["hma9_signal"],
        components["vwma20_signal"],
        components["ichimoku_signal"],
    ]
    oscillator_values = [
        components["rsi14_signal"],
        components["stoch_signal"],
        components["cci20_signal"],
        components["adx14_signal"],
        components["ao_signal"],
        components["mom10_signal"],
        components["macd_signal"],
        components["stoch_rsi_signal"],
        components["williams_r14_signal"],
        components["bull_bear_power13_signal"],
        components["uo_signal"],
    ]
    rating.moving_average_score = round(sum(ma_values) / len(ma_values), 4)
    rating.oscillator_score = round(sum(oscillator_values) / len(oscillator_values), 4)
    rating.overall_score = round((rating.moving_average_score + rating.oscillator_score) / 2.0, 4)
    rating.rating_label = score_to_label(rating.overall_score)
    rating.technical_status = TECHNICAL_INDICATOR_STATUS_OK
    rating.technical_status_reason = None
    return rating


def score_to_label(value: float | None) -> str | None:
    if value is None:
        return None
    if value < -0.5:
        return TECHNICAL_INDICATOR_LABEL_STRONG_SELL
    if value < -0.1:
        return TECHNICAL_INDICATOR_LABEL_SELL
    if value <= 0.1:
        return TECHNICAL_INDICATOR_LABEL_NEUTRAL
    if value <= 0.5:
        return TECHNICAL_INDICATOR_LABEL_BUY
    return TECHNICAL_INDICATOR_LABEL_STRONG_BUY


def _calculate_indicator_components(frame: pd.DataFrame) -> tuple[dict[str, float], list[str]]:
    high = frame["High"]
    low = frame["Low"]
    close = frame["Close"]
    volume = frame["Volume"]
    midpoint = (high + low) / 2.0
    prev_close = close.shift(1)

    result: dict[str, float] = {}
    missing: list[str] = []

    for length in (10, 20, 30, 50, 100, 200):
        sma = close.rolling(length).mean()
        ema = close.ewm(span=length, adjust=False).mean()
        result[f"sma{length}_signal"] = _price_vs_average_signal(close, sma)
        result[f"ema{length}_signal"] = _price_vs_average_signal(close, ema)

    hma9 = _hull_moving_average(close, 9)
    vwma20 = (close * volume).rolling(20).sum() / volume.rolling(20).sum()
    result["hma9_signal"] = _price_vs_average_signal(close, hma9)
    result["vwma20_signal"] = _price_vs_average_signal(close, vwma20)
    result["ichimoku_signal"] = _ichimoku_signal(high, low, close)
    result["rsi14_signal"] = _rsi_signal(close, 14)
    result["stoch_signal"] = _stoch_signal(high, low, close)
    result["cci20_signal"] = _cci_signal(high, low, close, 20)
    result["adx14_signal"] = _adx_signal(high, low, close, 14)
    result["ao_signal"] = _ao_signal(midpoint)
    result["mom10_signal"] = _momentum_signal(close, 10)
    result["macd_signal"] = _macd_signal(close)
    result["stoch_rsi_signal"] = _stoch_rsi_signal(close)
    result["williams_r14_signal"] = _williams_r_signal(high, low, close, 14)
    result["bull_bear_power13_signal"] = _bull_bear_power_signal(high, low, close, 13)
    result["uo_signal"] = _ultimate_oscillator_signal(high, low, close)

    for key, value in list(result.items()):
        if pd.isna(value):
            missing.append(key.replace("_signal", ""))
        else:
            result[key] = float(value)
    return result, missing


def _last_value(series: pd.Series) -> float | None:
    if series.empty:
        return None
    value = series.iloc[-1]
    return None if pd.isna(value) else float(value)


def _safe_divide(numerator: pd.Series, denominator: pd.Series) -> pd.Series:
    return numerator.astype(float) / denominator.astype(float).where(denominator.astype(float) != 0.0)


def _signal_from_bool(buy: bool, sell: bool) -> float:
    if buy and not sell:
        return 1.0
    if sell and not buy:
        return -1.0
    return 0.0


def _price_vs_average_signal(close: pd.Series, average: pd.Series) -> float:
    price = _last_value(close)
    avg = _last_value(average)
    if price is None or avg is None:
        return float("nan")
    if price > avg:
        return 1.0
    if price < avg:
        return -1.0
    return 0.0


def _wma(series: pd.Series, length: int) -> pd.Series:
    weights = pd.Series(range(1, length + 1), dtype=float)
    return series.rolling(length).apply(lambda values: float((values * weights).sum() / weights.sum()), raw=True)


def _hull_moving_average(series: pd.Series, length: int) -> pd.Series:
    half = max(1, int(length / 2))
    sqrt_length = max(1, int(length ** 0.5))
    return _wma((2 * _wma(series, half)) - _wma(series, length), sqrt_length)


def _ichimoku_signal(high: pd.Series, low: pd.Series, close: pd.Series) -> float:
    conversion = (high.rolling(9).max() + low.rolling(9).min()) / 2.0
    base = (high.rolling(26).max() + low.rolling(26).min()) / 2.0
    span_a = ((conversion + base) / 2.0).shift(26)
    span_b = ((high.rolling(52).max() + low.rolling(52).min()) / 2.0).shift(26)
    conversion_value = _last_value(conversion)
    base_value = _last_value(base)
    span_a_value = _last_value(span_a)
    span_b_value = _last_value(span_b)
    close_value = _last_value(close)
    if None in {conversion_value, base_value, span_a_value, span_b_value, close_value}:
        return float("nan")
    buy = span_a_value > span_b_value and base_value > span_a_value and conversion_value > base_value and close_value > conversion_value
    sell = span_a_value < span_b_value and base_value < span_a_value and conversion_value < base_value and close_value < conversion_value
    return _signal_from_bool(buy, sell)


def _rsi(close: pd.Series, length: int) -> pd.Series:
    delta = close.diff()
    gains = delta.clip(lower=0.0)
    losses = -delta.clip(upper=0.0)
    avg_gain = gains.ewm(alpha=1 / length, adjust=False, min_periods=length).mean()
    avg_loss = losses.ewm(alpha=1 / length, adjust=False, min_periods=length).mean()
    rs = _safe_divide(avg_gain, avg_loss)
    rsi = 100.0 - (100.0 / (1.0 + rs))
    return rsi.astype(float)


def _rsi_signal(close: pd.Series, length: int) -> float:
    rsi = _rsi(close, length)
    current = _last_value(rsi)
    previous = _last_value(rsi.shift(1))
    if current is None or previous is None:
        return float("nan")
    buy = current < 30.0 and current > previous
    sell = current > 70.0 and current < previous
    return _signal_from_bool(buy, sell)


def _stoch_signal(high: pd.Series, low: pd.Series, close: pd.Series) -> float:
    lowest = low.rolling(14).min()
    highest = high.rolling(14).max()
    raw_k = 100.0 * _safe_divide(close - lowest, highest - lowest)
    k = raw_k.rolling(3).mean()
    d = k.rolling(3).mean()
    k_value = _last_value(k)
    d_value = _last_value(d)
    if k_value is None or d_value is None:
        return float("nan")
    buy = k_value < 20.0 and d_value < 20.0 and k_value > d_value
    sell = k_value > 80.0 and d_value > 80.0 and k_value < d_value
    return _signal_from_bool(buy, sell)


def _cci_signal(high: pd.Series, low: pd.Series, close: pd.Series, length: int) -> float:
    typical = (high + low + close) / 3.0
    sma = typical.rolling(length).mean()
    mad = typical.rolling(length).apply(lambda values: float((abs(values - values.mean())).mean()), raw=True)
    cci = _safe_divide(typical - sma, 0.015 * mad)
    current = _last_value(cci)
    previous = _last_value(cci.shift(1))
    if current is None or previous is None:
        return float("nan")
    buy = current < -100.0 and current > previous
    sell = current > 100.0 and current < previous
    return _signal_from_bool(buy, sell)


def _adx_components(high: pd.Series, low: pd.Series, close: pd.Series, length: int) -> tuple[pd.Series, pd.Series, pd.Series]:
    up_move = high.diff()
    down_move = -low.diff()
    plus_dm = up_move.where((up_move > down_move) & (up_move > 0.0), 0.0)
    minus_dm = down_move.where((down_move > up_move) & (down_move > 0.0), 0.0)
    true_range = pd.concat([(high - low), (high - close.shift(1)).abs(), (low - close.shift(1)).abs()], axis=1).max(axis=1)
    atr = true_range.ewm(alpha=1 / length, adjust=False, min_periods=length).mean()
    plus_di = 100.0 * _safe_divide(plus_dm.ewm(alpha=1 / length, adjust=False, min_periods=length).mean(), atr)
    minus_di = 100.0 * _safe_divide(minus_dm.ewm(alpha=1 / length, adjust=False, min_periods=length).mean(), atr)
    dx = 100.0 * _safe_divide((plus_di - minus_di).abs(), plus_di + minus_di)
    adx = dx.ewm(alpha=1 / length, adjust=False, min_periods=length).mean()
    return plus_di.astype(float), minus_di.astype(float), adx.astype(float)


def _adx_signal(high: pd.Series, low: pd.Series, close: pd.Series, length: int) -> float:
    plus_di, minus_di, adx = _adx_components(high, low, close, length)
    plus_value = _last_value(plus_di)
    minus_value = _last_value(minus_di)
    adx_value = _last_value(adx)
    adx_prev = _last_value(adx.shift(1))
    if None in {plus_value, minus_value, adx_value, adx_prev}:
        return float("nan")
    buy = plus_value > minus_value and adx_value > 20.0 and adx_value > adx_prev
    sell = plus_value < minus_value and adx_value > 20.0 and adx_value < adx_prev
    return _signal_from_bool(buy, sell)


def _ao_signal(midpoint: pd.Series) -> float:
    ao = midpoint.rolling(5).mean() - midpoint.rolling(34).mean()
    current = _last_value(ao)
    prev = _last_value(ao.shift(1))
    prev2 = _last_value(ao.shift(2))
    if None in {current, prev, prev2}:
        return float("nan")
    buy = (prev <= 0.0 < current) or (current > 0.0 and prev > 0.0 and current > prev and prev < prev2)
    sell = (prev >= 0.0 > current) or (current < 0.0 and prev < 0.0 and current < prev and prev > prev2)
    return _signal_from_bool(buy, sell)


def _momentum_signal(close: pd.Series, length: int) -> float:
    momentum = close - close.shift(length)
    current = _last_value(momentum)
    previous = _last_value(momentum.shift(1))
    if current is None or previous is None:
        return float("nan")
    if current > previous:
        return 1.0
    if current < previous:
        return -1.0
    return 0.0


def _macd_signal(close: pd.Series) -> float:
    macd = close.ewm(span=12, adjust=False).mean() - close.ewm(span=26, adjust=False).mean()
    signal = macd.ewm(span=9, adjust=False).mean()
    macd_value = _last_value(macd)
    signal_value = _last_value(signal)
    if macd_value is None or signal_value is None:
        return float("nan")
    if macd_value > signal_value:
        return 1.0
    if macd_value < signal_value:
        return -1.0
    return 0.0


def _stoch_rsi_signal(close: pd.Series) -> float:
    rsi = _rsi(close, 14)
    lowest = rsi.rolling(14).min()
    highest = rsi.rolling(14).max()
    raw = 100.0 * _safe_divide(rsi - lowest, highest - lowest)
    k = raw.rolling(3).mean()
    d = k.rolling(3).mean()
    close_sma50 = close.rolling(50).mean()
    current_close = _last_value(close)
    trend_ma = _last_value(close_sma50)
    k_value = _last_value(k)
    d_value = _last_value(d)
    if None in {current_close, trend_ma, k_value, d_value}:
        return float("nan")
    buy = current_close < trend_ma and k_value < 20.0 and d_value < 20.0 and k_value > d_value
    sell = current_close > trend_ma and k_value > 80.0 and d_value > 80.0 and k_value < d_value
    return _signal_from_bool(buy, sell)


def _williams_r_signal(high: pd.Series, low: pd.Series, close: pd.Series, length: int) -> float:
    highest = high.rolling(length).max()
    lowest = low.rolling(length).min()
    wr = -100.0 * _safe_divide(highest - close, highest - lowest)
    current = _last_value(wr)
    previous = _last_value(wr.shift(1))
    if current is None or previous is None:
        return float("nan")
    buy = current < -80.0 and current > previous
    sell = current > -20.0 and current < previous
    return _signal_from_bool(buy, sell)


def _bull_bear_power_signal(high: pd.Series, low: pd.Series, close: pd.Series, length: int) -> float:
    ema = close.ewm(span=length, adjust=False).mean()
    bull_power = high - ema
    bear_power = low - ema
    ema_value = _last_value(ema)
    ema_prev = _last_value(ema.shift(1))
    bull_value = _last_value(bull_power)
    bull_prev = _last_value(bull_power.shift(1))
    bear_value = _last_value(bear_power)
    bear_prev = _last_value(bear_power.shift(1))
    if None in {ema_value, ema_prev, bull_value, bull_prev, bear_value, bear_prev}:
        return float("nan")
    uptrend = ema_value > ema_prev
    downtrend = ema_value < ema_prev
    buy = uptrend and bear_value < 0.0 and bear_value > bear_prev
    sell = downtrend and bull_value > 0.0 and bull_value < bull_prev
    return _signal_from_bool(buy, sell)


def _ultimate_oscillator_signal(high: pd.Series, low: pd.Series, close: pd.Series) -> float:
    prev_close = close.shift(1)
    buying_pressure = close - pd.concat([low, prev_close], axis=1).min(axis=1)
    true_range = pd.concat([high, prev_close], axis=1).max(axis=1) - pd.concat([low, prev_close], axis=1).min(axis=1)
    avg7 = _safe_divide(buying_pressure.rolling(7).sum(), true_range.rolling(7).sum())
    avg14 = _safe_divide(buying_pressure.rolling(14).sum(), true_range.rolling(14).sum())
    avg28 = _safe_divide(buying_pressure.rolling(28).sum(), true_range.rolling(28).sum())
    uo = 100.0 * ((4.0 * avg7) + (2.0 * avg14) + avg28) / 7.0
    value = _last_value(uo)
    if value is None:
        return float("nan")
    if value > 70.0:
        return 1.0
    if value < 30.0:
        return -1.0
    return 0.0
