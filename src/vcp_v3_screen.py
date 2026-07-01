from __future__ import annotations

from dataclasses import asdict, dataclass
import datetime as dt

import numpy as np
import pandas as pd

from .config import AppConfig
from .cookstock_bridge import freeze_cookstock_today, iter_prefetched_cookstock_batches, load_configured_cookstock
from .universe import UniverseTicker


VCP_V3_HISTORY_DAYS = 420
VCP_V3_BASE_WINDOWS = (40, 60, 80, 100, 120)
VCP_V3_BREAKOUT_LOOKBACK = 20
VCP_V3_SWING_THRESHOLD_PCT = 3.0
VCP_V3_SWING_MIN_BARS = 3
VCP_V3_MAX_CONTRACTION_RATIO = 0.88
VCP_V3_MIN_CONTRACTION_COUNT = 2
VCP_V3_C1_MIN_PCT = 8.0
VCP_V3_C1_MAX_PCT = 45.0
VCP_V3_FINAL_SWING_MAX_PCT = 10.0
VCP_V3_NEAR_PIVOT_PCT = 3.0
VCP_V3_MAX_BELOW_52W_HIGH_PCT = 22.0
VCP_V3_MIN_PRIOR_UPTREND_PCT = 18.0
VCP_V3_RS_LOOKBACK_DAYS = 63
VCP_V3_MIN_RS_VS_BENCHMARK_PCT = 0.0
VCP_V3_MIN_PRICE = 5.0
VCP_V3_MIN_AVG_VOL_50 = 200_000.0
VCP_V3_BREAKOUT_VOL_RATIO = 1.4
VCP_V3_BREAKOUT_CLOSE_POS_MIN = 0.75
VCP_V3_BREAKOUT_MAX_EXTENSION_PCT = 20.0
VCP_V3_FALSE_BREAK_MAX_BELOW_PIVOT_PCT = 0.5
VCP_V3_STOP_ATR_MULT = 1.8
VCP_V3_TARGET1_PCT = 18.0
VCP_V3_TARGET2_PCT = 35.0
VCP_V3_MIN_RISK_REWARD = 2.0
VCP_V3_COIL_ATR_PCTILE_MAX = 40.0
VCP_V3_COIL_VOL_RATIO_MAX = 0.50
VCP_V3_VOL_SLOPE_SESSIONS = 15
VCP_V3_PIVOT_LOOKBACK_FRAC = 0.35


@dataclass(frozen=True)
class VcpV3Hit:
    ticker: str
    sector: str | None
    industry: str | None
    exchange: str | None
    signal_date: str
    benchmark_ticker: str
    category: str
    signal_kind: str
    current_price: float
    pivot_price: float
    entry_price: float
    stop_price: float
    target1_price: float
    target2_price: float
    risk_reward: float
    vcp_score: float
    contraction_count: int
    waves: list[float]
    contraction_ratios: list[float]
    rising_lows_count: int
    is_cup: bool
    is_stage2: bool
    volume_dry_up: bool
    coil_atr_percentile: float
    coil_volume_ratio: float
    volume_slope_ok: bool
    breakout_volume_ratio: float | None
    days_since_breakout: int | None
    extension_pct: float | None
    distance_from_pivot_pct: float
    prior_uptrend_pct: float
    rs_vs_benchmark_pct: float | None
    below_52w_high_pct: float | None
    ma50: float
    ma150: float
    ma200: float
    reasons: list[str]

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True)
class VcpV3ScreenResult:
    run_date: str
    benchmark_ticker: str
    total_tickers: int
    passed_tickers: int
    failed_tickers: list[dict[str, str]]
    hits: list[VcpV3Hit]

    def to_dict(self) -> dict[str, object]:
        return {
            "run_date": self.run_date,
            "benchmark_ticker": self.benchmark_ticker,
            "total_tickers": self.total_tickers,
            "passed_tickers": self.passed_tickers,
            "failed_tickers": self.failed_tickers,
            "hits": [item.to_dict() for item in self.hits],
        }


def _normalize_price_frame(frame: pd.DataFrame, *, include_volume: bool = True) -> pd.DataFrame:
    required = ["Open", "High", "Low", "Close", "Volume"] if include_volume else ["Close"]
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


def _build_price_frame(financials: object) -> pd.DataFrame:
    rows = financials._get_clean_price_data()  # type: ignore[attr-defined]
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


def _atr_series(bars: pd.DataFrame, length: int = 14) -> pd.Series:
    high = bars["High"].astype(float)
    low = bars["Low"].astype(float)
    close = bars["Close"].astype(float)
    prev_close = close.shift(1)
    tr = pd.concat([(high - low), (high - prev_close).abs(), (low - prev_close).abs()], axis=1).max(axis=1)
    return tr.rolling(length).mean()


def _atr_last(bars: pd.DataFrame, length: int = 14) -> float:
    atr = _atr_series(bars, length=length).dropna()
    return float(atr.iloc[-1]) if not atr.empty else 0.0


def _slope(series: pd.Series) -> float:
    values = series.dropna().astype(float).to_numpy()
    if values.size < 2:
        return 0.0
    x = np.arange(values.size, dtype=float)
    return float(np.polyfit(x, values, 1)[0])


def _calc_rs_pct(stock_close: pd.Series, benchmark_close: pd.Series, lookback: int = VCP_V3_RS_LOOKBACK_DAYS) -> float | None:
    aligned = benchmark_close.reindex(stock_close.index).ffill().bfill()
    if len(stock_close) <= lookback or len(aligned) <= lookback:
        return None
    stock_start = float(stock_close.iloc[-lookback - 1])
    bench_start = float(aligned.iloc[-lookback - 1])
    if stock_start <= 0 or bench_start <= 0:
        return None
    stock_return = (float(stock_close.iloc[-1]) / stock_start - 1.0) * 100.0
    bench_return = (float(aligned.iloc[-1]) / bench_start - 1.0) * 100.0
    return stock_return - bench_return

def _stage2_snapshot(bars: pd.DataFrame) -> tuple[bool, float, float, float]:
    close = bars["Close"].astype(float)
    ma50 = close.rolling(50).mean()
    ma150 = close.rolling(150).mean()
    ma200 = close.rolling(200).mean()
    if len(close) < 220 or pd.isna(ma200.iloc[-1]) or pd.isna(ma150.iloc[-1]) or pd.isna(ma50.iloc[-1]):
        return False, 0.0, 0.0, 0.0
    latest_close = float(close.iloc[-1])
    latest_ma50 = float(ma50.iloc[-1])
    latest_ma150 = float(ma150.iloc[-1])
    latest_ma200 = float(ma200.iloc[-1])
    ma200_prev = float(ma200.iloc[-20]) if pd.notna(ma200.iloc[-20]) else latest_ma200
    ma50_slope = _slope(ma50.tail(10))
    passed = (
        latest_close > latest_ma50 > latest_ma150 > latest_ma200
        and latest_ma150 > latest_ma200
        and latest_ma200 > ma200_prev
        and ma50_slope > 0.0
    )
    return bool(passed), latest_ma50, latest_ma150, latest_ma200


def _below_52w_high_pct(bars: pd.DataFrame) -> float | None:
    if bars.empty:
        return None
    lookback = min(252, len(bars))
    high_52w = float(bars["High"].astype(float).tail(lookback).max())
    price = float(bars["Close"].astype(float).iloc[-1])
    if high_52w <= 0:
        return None
    return ((high_52w - price) / high_52w) * 100.0


def _prior_uptrend_pct(bars: pd.DataFrame, base_start_idx: int, prior_window: int = 90) -> float | None:
    prior_end = max(0, int(base_start_idx))
    prior_start = max(0, prior_end - prior_window)
    if prior_end - prior_start < 15:
        return None
    segment = bars.iloc[prior_start:prior_end]
    if segment.empty:
        return None
    low = float(segment["Low"].astype(float).min())
    high = float(segment["High"].astype(float).max())
    if low <= 0:
        return None
    return ((high - low) / low) * 100.0


def find_swing_points(
    bars: pd.DataFrame,
    threshold_pct: float = VCP_V3_SWING_THRESHOLD_PCT,
    min_bars: int = VCP_V3_SWING_MIN_BARS,
) -> list[dict[str, object]]:
    if bars.empty or len(bars) < 8:
        return []
    highs = bars["High"].astype(float).to_numpy()
    lows = bars["Low"].astype(float).to_numpy()
    closes = bars["Close"].astype(float).to_numpy()
    volumes = bars["Volume"].astype(float).to_numpy()
    avg_volume = float(np.nanmean(volumes[-50:])) if len(volumes) >= 50 else float(np.nanmean(volumes))
    swings: list[dict[str, object]] = []
    direction = "H"
    extreme_idx = 0
    extreme_price = closes[0]

    for i in range(1, len(bars)):
        if direction == "H":
            if highs[i] >= extreme_price:
                extreme_idx = i
                extreme_price = highs[i]
            drawdown_pct = ((extreme_price - lows[i]) / extreme_price) * 100.0 if extreme_price > 0 else 0.0
            if drawdown_pct >= threshold_pct and (i - extreme_idx) >= min_bars:
                if volumes[extreme_idx] >= avg_volume * 0.25:
                    swings.append({"idx": extreme_idx, "price": float(extreme_price), "type": "H", "date": bars.index[extreme_idx]})
                direction = "L"
                extreme_idx = i
                extreme_price = lows[i]
        else:
            if lows[i] <= extreme_price:
                extreme_idx = i
                extreme_price = lows[i]
            rally_pct = ((highs[i] - extreme_price) / extreme_price) * 100.0 if extreme_price > 0 else 0.0
            if rally_pct >= threshold_pct and (i - extreme_idx) >= min_bars:
                if volumes[extreme_idx] >= avg_volume * 0.25:
                    swings.append({"idx": extreme_idx, "price": float(extreme_price), "type": "L", "date": bars.index[extreme_idx]})
                direction = "H"
                extreme_idx = i
                extreme_price = highs[i]

    deduped: list[dict[str, object]] = []
    for swing in swings:
        if deduped and deduped[-1]["type"] == swing["type"]:
            if swing["type"] == "H" and float(swing["price"]) >= float(deduped[-1]["price"]):
                deduped[-1] = swing
            elif swing["type"] == "L" and float(swing["price"]) <= float(deduped[-1]["price"]):
                deduped[-1] = swing
            continue
        deduped.append(swing)
    return deduped


def measure_contraction_waves(swings: list[dict[str, object]]) -> tuple[list[float], list[tuple[dict[str, object], dict[str, object]]]]:
    waves: list[float] = []
    pairs: list[tuple[dict[str, object], dict[str, object]]] = []
    if len(swings) < 2:
        return waves, pairs
    ordered = swings
    if ordered and str(ordered[0].get("type")) != "H":
        ordered = ordered[1:]
    for idx in range(0, len(ordered) - 1):
        current = ordered[idx]
        nxt = ordered[idx + 1]
        if str(current.get("type")) != "H" or str(nxt.get("type")) != "L":
            continue
        high = float(current.get("price") or 0.0)
        low = float(nxt.get("price") or 0.0)
        if high <= 0 or low <= 0 or low >= high:
            continue
        waves.append(round(((high - low) / high) * 100.0, 2))
        pairs.append((current, nxt))
    return waves, pairs


def validate_contraction(waves: list[float]) -> tuple[int, bool, list[float], list[float]]:
    if not waves:
        return 0, False, [], []
    valid: list[float] = []
    ratios: list[float] = []
    first = float(waves[0])
    if first < VCP_V3_C1_MIN_PCT or first > VCP_V3_C1_MAX_PCT:
        return 0, False, [], []
    valid.append(first)
    for wave in waves[1:]:
        value = float(wave)
        ratio = value / valid[-1] if valid[-1] > 0 else 1.0
        if value < valid[-1] and ratio <= VCP_V3_MAX_CONTRACTION_RATIO:
            valid.append(value)
            ratios.append(round(ratio, 3))
        else:
            break
    count = len(valid)
    is_valid = count >= VCP_V3_MIN_CONTRACTION_COUNT and valid[-1] <= VCP_V3_FINAL_SWING_MAX_PCT
    return count, bool(is_valid), [round(value, 2) for value in valid], ratios


def check_rising_lows(swings: list[dict[str, object]]) -> tuple[int, bool]:
    lows = [float(item.get("price") or 0.0) for item in swings if str(item.get("type")) == "L"]
    if len(lows) < 2:
        return 0, False
    rising = sum(1 for idx in range(1, len(lows)) if lows[idx] > lows[idx - 1])
    return rising, rising >= 1


def check_final_coil(base: pd.DataFrame) -> tuple[float, bool, float]:
    atr_values = _atr_series(base).dropna()
    if atr_values.empty:
        return 100.0, False, 1.0
    latest_atr = float(atr_values.iloc[-1])
    atr_percentile = float((atr_values <= latest_atr).mean() * 100.0)
    avg10 = float(base["Volume"].astype(float).tail(10).mean())
    avg50 = float(base["Volume"].astype(float).tail(50).mean()) if len(base) >= 50 else float(base["Volume"].astype(float).mean())
    vol_ratio = (avg10 / avg50) if avg50 > 0 else 1.0
    recent_volume = base["Volume"].astype(float).tail(VCP_V3_VOL_SLOPE_SESSIONS)
    vol_slope_ok = _slope(recent_volume) < 0.0 if len(recent_volume) >= 5 else False
    return round(atr_percentile, 2), bool(vol_slope_ok), round(vol_ratio, 3)


def identify_pivot(base: pd.DataFrame, swings: list[dict[str, object]]) -> float:
    if base.empty:
        return 0.0
    cutoff_idx = int(len(base) * (1.0 - VCP_V3_PIVOT_LOOKBACK_FRAC))
    recent_highs = [float(item.get("price") or 0.0) for item in swings if str(item.get("type")) == "H" and int(item.get("idx") or 0) >= cutoff_idx]
    if recent_highs:
        return float(max(recent_highs))
    return float(base["High"].astype(float).tail(max(10, len(base) // 3)).max())


def _detect_cup(base: pd.DataFrame) -> bool:
    n = len(base)
    if n < 20:
        return False
    seg = max(3, n // 5)
    lows: list[float] = []
    for idx in range(5):
        start = idx * seg
        end = n if idx == 4 else min(n, (idx + 1) * seg)
        section = base["Low"].astype(float).iloc[start:end]
        if section.empty:
            return False
        lows.append(float(section.min()))
    mid = lows[2]
    left_avg = (lows[0] + lows[1]) / 2.0
    right_avg = (lows[3] + lows[4]) / 2.0
    return bool(mid < left_avg * 0.97 and mid < right_avg * 0.97 and right_avg > mid)

def _base_context_ok(
    bars: pd.DataFrame,
    benchmark_close: pd.Series,
    *,
    base_start_idx: int,
) -> tuple[bool, dict[str, float | None]]:
    price = float(bars["Close"].astype(float).iloc[-1])
    avg_vol50 = float(bars["Volume"].astype(float).tail(50).mean()) if len(bars) >= 50 else float(bars["Volume"].astype(float).mean())
    if price < VCP_V3_MIN_PRICE or avg_vol50 < VCP_V3_MIN_AVG_VOL_50:
        return False, {}
    below_52w = _below_52w_high_pct(bars)
    if below_52w is None or below_52w > VCP_V3_MAX_BELOW_52W_HIGH_PCT:
        return False, {"below_52w_high_pct": below_52w}
    prior_uptrend = _prior_uptrend_pct(bars, base_start_idx)
    if prior_uptrend is None or prior_uptrend < VCP_V3_MIN_PRIOR_UPTREND_PCT:
        return False, {"prior_uptrend_pct": prior_uptrend}
    is_stage2, ma50, ma150, ma200 = _stage2_snapshot(bars)
    if not is_stage2:
        return False, {"ma50": ma50, "ma150": ma150, "ma200": ma200}
    rs_pct = _calc_rs_pct(bars["Close"].astype(float), benchmark_close)
    if rs_pct is not None and rs_pct < VCP_V3_MIN_RS_VS_BENCHMARK_PCT:
        return False, {"rs_vs_benchmark_pct": rs_pct, "ma50": ma50, "ma150": ma150, "ma200": ma200}
    return True, {
        "below_52w_high_pct": below_52w,
        "prior_uptrend_pct": prior_uptrend,
        "rs_vs_benchmark_pct": rs_pct,
        "ma50": ma50,
        "ma150": ma150,
        "ma200": ma200,
    }


def _score_pre_breakout(
    contraction_count: int,
    ratios: list[float],
    dist_pct: float,
    coil_vol_ratio: float,
    coil_pct: float,
    rising_lows: int,
    is_cup: bool,
    volume_slope_ok: bool,
) -> float:
    score = 0.0
    score += min(20.0, contraction_count * 7.0)
    if ratios:
        avg_ratio = sum(ratios) / len(ratios)
        score += max(0.0, (1.0 - avg_ratio) * 15.0)
    score += max(0.0, (1.0 - dist_pct / max(VCP_V3_NEAR_PIVOT_PCT, 0.1)) * 20.0)
    score += max(0.0, (1.0 - coil_vol_ratio / max(VCP_V3_COIL_VOL_RATIO_MAX, 0.01)) * 20.0)
    score += 5.0 if volume_slope_ok else 0.0
    score += max(0.0, (1.0 - coil_pct / max(VCP_V3_COIL_ATR_PCTILE_MAX, 1.0)) * 10.0)
    score += min(5.0, rising_lows * 2.0)
    score += 5.0 if is_cup else 0.0
    return round(max(0.0, min(100.0, score)), 1)


def _score_broken_out(
    contraction_count: int,
    volume_ratio: float,
    extension_pct: float,
    close_position: float,
) -> float:
    score = 0.0
    score += min(25.0, volume_ratio * 10.0)
    score += max(0.0, (1.0 - extension_pct / max(VCP_V3_BREAKOUT_MAX_EXTENSION_PCT, 1.0)) * 20.0)
    score += min(20.0, contraction_count * 7.0)
    score += max(0.0, close_position * 15.0)
    return round(max(0.0, min(100.0, score)), 1)


def _build_hit(
    *,
    ticker: UniverseTicker,
    benchmark_ticker: str,
    category: str,
    signal_date: dt.date,
    signal_kind: str,
    current_price: float,
    pivot_price: float,
    entry_price: float,
    stop_price: float,
    target1_price: float,
    target2_price: float,
    risk_reward: float,
    vcp_score: float,
    contraction_count: int,
    waves: list[float],
    contraction_ratios: list[float],
    rising_lows_count: int,
    is_cup: bool,
    volume_dry_up: bool,
    coil_atr_percentile: float,
    coil_volume_ratio: float,
    volume_slope_ok: bool,
    breakout_volume_ratio: float | None,
    days_since_breakout: int | None,
    extension_pct: float | None,
    distance_from_pivot_pct: float,
    prior_uptrend_pct: float,
    rs_vs_benchmark_pct: float | None,
    below_52w_high_pct: float | None,
    ma50: float,
    ma150: float,
    ma200: float,
    reasons: list[str],
) -> VcpV3Hit:
    return VcpV3Hit(
        ticker=ticker.symbol,
        sector=ticker.sector,
        industry=ticker.industry,
        exchange=ticker.exchange,
        signal_date=signal_date.isoformat(),
        benchmark_ticker=benchmark_ticker,
        category=category,
        signal_kind=signal_kind,
        current_price=round(current_price, 4),
        pivot_price=round(pivot_price, 4),
        entry_price=round(entry_price, 4),
        stop_price=round(stop_price, 4),
        target1_price=round(target1_price, 4),
        target2_price=round(target2_price, 4),
        risk_reward=round(risk_reward, 2),
        vcp_score=round(vcp_score, 1),
        contraction_count=int(contraction_count),
        waves=[round(float(value), 2) for value in waves],
        contraction_ratios=[round(float(value), 3) for value in contraction_ratios],
        rising_lows_count=int(rising_lows_count),
        is_cup=bool(is_cup),
        is_stage2=True,
        volume_dry_up=bool(volume_dry_up),
        coil_atr_percentile=round(coil_atr_percentile, 2),
        coil_volume_ratio=round(coil_volume_ratio, 3),
        volume_slope_ok=bool(volume_slope_ok),
        breakout_volume_ratio=round(float(breakout_volume_ratio), 2) if breakout_volume_ratio is not None else None,
        days_since_breakout=days_since_breakout,
        extension_pct=round(float(extension_pct), 2) if extension_pct is not None else None,
        distance_from_pivot_pct=round(distance_from_pivot_pct, 2),
        prior_uptrend_pct=round(prior_uptrend_pct, 2),
        rs_vs_benchmark_pct=round(float(rs_vs_benchmark_pct), 2) if rs_vs_benchmark_pct is not None else None,
        below_52w_high_pct=round(float(below_52w_high_pct), 2) if below_52w_high_pct is not None else None,
        ma50=round(ma50, 4),
        ma150=round(ma150, 4),
        ma200=round(ma200, 4),
        reasons=list(reasons),
    )

def _scan_pre_breakout(
    bars: pd.DataFrame,
    benchmark_close: pd.Series,
    *,
    ticker: UniverseTicker,
    benchmark_ticker: str,
) -> VcpV3Hit | None:
    best: VcpV3Hit | None = None
    for base_days in VCP_V3_BASE_WINDOWS:
        if len(bars) < base_days:
            continue
        base = bars.tail(base_days)
        base_start_idx = len(bars) - len(base)
        ok, context = _base_context_ok(bars, benchmark_close, base_start_idx=base_start_idx)
        if not ok:
            continue
        swings = find_swing_points(base)
        waves, _pairs = measure_contraction_waves(swings)
        contraction_count, is_contracting, valid_waves, ratios = validate_contraction(waves)
        if not is_contracting:
            continue
        rising_lows_count, rising_ok = check_rising_lows(swings)
        if not rising_ok:
            continue
        coil_pct, volume_slope_ok, coil_vol_ratio = check_final_coil(base)
        if coil_pct > VCP_V3_COIL_ATR_PCTILE_MAX or coil_vol_ratio > VCP_V3_COIL_VOL_RATIO_MAX or not volume_slope_ok:
            continue
        pivot = identify_pivot(base, swings)
        price = float(bars["Close"].astype(float).iloc[-1])
        if pivot <= 0 or price <= 0:
            continue
        dist_pct = ((pivot - price) / price) * 100.0
        if dist_pct > VCP_V3_NEAR_PIVOT_PCT or dist_pct < 0.0:
            continue
        atr = _atr_last(bars)
        if atr <= 0:
            continue
        entry = pivot * 1.003
        stop = entry - VCP_V3_STOP_ATR_MULT * atr
        risk = entry - stop
        if risk <= 0:
            continue
        target1 = pivot * (1.0 + VCP_V3_TARGET1_PCT / 100.0)
        target2 = pivot * (1.0 + VCP_V3_TARGET2_PCT / 100.0)
        risk_reward = (target1 - entry) / risk
        if risk_reward < VCP_V3_MIN_RISK_REWARD:
            continue
        is_cup = _detect_cup(base)
        score = _score_pre_breakout(contraction_count, ratios, dist_pct, coil_vol_ratio, coil_pct, rising_lows_count, is_cup, volume_slope_ok)
        reasons = [
            "Pre-breakout VCP v3",
            f"{contraction_count} tightening contractions",
            f"Pivot within {dist_pct:.1f}%",
            f"Coil ATR percentile {coil_pct:.0f}",
            f"10D/50D volume ratio {coil_vol_ratio:.2f}",
            f"Prior uptrend {float(context.get('prior_uptrend_pct') or 0.0):.1f}%",
        ]
        hit = _build_hit(
            ticker=ticker,
            benchmark_ticker=benchmark_ticker,
            category="pre_breakout",
            signal_date=bars.index[-1].date(),
            signal_kind="vcp_v3_pre_breakout",
            current_price=price,
            pivot_price=pivot,
            entry_price=entry,
            stop_price=stop,
            target1_price=target1,
            target2_price=target2,
            risk_reward=risk_reward,
            vcp_score=score,
            contraction_count=contraction_count,
            waves=valid_waves,
            contraction_ratios=ratios,
            rising_lows_count=rising_lows_count,
            is_cup=is_cup,
            volume_dry_up=True,
            coil_atr_percentile=coil_pct,
            coil_volume_ratio=coil_vol_ratio,
            volume_slope_ok=volume_slope_ok,
            breakout_volume_ratio=None,
            days_since_breakout=None,
            extension_pct=None,
            distance_from_pivot_pct=dist_pct,
            prior_uptrend_pct=float(context.get("prior_uptrend_pct") or 0.0),
            rs_vs_benchmark_pct=context.get("rs_vs_benchmark_pct"),
            below_52w_high_pct=context.get("below_52w_high_pct"),
            ma50=float(context.get("ma50") or 0.0),
            ma150=float(context.get("ma150") or 0.0),
            ma200=float(context.get("ma200") or 0.0),
            reasons=reasons,
        )
        if best is None or hit.vcp_score > best.vcp_score:
            best = hit
    return best


def _scan_broken_out(
    bars: pd.DataFrame,
    benchmark_close: pd.Series,
    *,
    ticker: UniverseTicker,
    benchmark_ticker: str,
) -> VcpV3Hit | None:
    best: VcpV3Hit | None = None
    close = bars["Close"].astype(float)
    high = bars["High"].astype(float)
    low = bars["Low"].astype(float)
    volume = bars["Volume"].astype(float)
    current_price = float(close.iloc[-1])
    for days_ago in range(1, min(VCP_V3_BREAKOUT_LOOKBACK, len(bars) - 20) + 1):
        bo_idx = len(bars) - days_ago
        if bo_idx <= 20:
            continue
        breakout_date = bars.index[bo_idx].date()
        for base_days in VCP_V3_BASE_WINDOWS:
            base_start = max(0, bo_idx - base_days)
            base = bars.iloc[base_start:bo_idx]
            if len(base) < 20:
                continue
            ok, context = _base_context_ok(bars, benchmark_close, base_start_idx=base_start)
            if not ok:
                continue
            swings = find_swing_points(base)
            waves, _pairs = measure_contraction_waves(swings)
            contraction_count, is_contracting, valid_waves, ratios = validate_contraction(waves)
            if not is_contracting:
                continue
            rising_lows_count, rising_ok = check_rising_lows(swings)
            if not rising_ok:
                continue
            coil_pct, volume_slope_ok, coil_vol_ratio = check_final_coil(base)
            if coil_pct > VCP_V3_COIL_ATR_PCTILE_MAX or coil_vol_ratio > VCP_V3_COIL_VOL_RATIO_MAX:
                continue
            pivot = identify_pivot(base, swings)
            if pivot <= 0:
                continue
            breakout_close = float(close.iloc[bo_idx])
            breakout_high = float(high.iloc[bo_idx])
            breakout_low = float(low.iloc[bo_idx])
            if breakout_close <= pivot:
                continue
            avg_vol20 = float(volume.iloc[max(0, bo_idx - 20):bo_idx].mean())
            if avg_vol20 <= 0:
                continue
            breakout_volume_ratio = float(volume.iloc[bo_idx]) / avg_vol20
            if breakout_volume_ratio < VCP_V3_BREAKOUT_VOL_RATIO:
                continue
            day_range = breakout_high - breakout_low
            close_position = ((breakout_close - breakout_low) / day_range) if day_range > 0 else 0.0
            if close_position < VCP_V3_BREAKOUT_CLOSE_POS_MIN:
                continue
            post_breakout = close.iloc[bo_idx:]
            if float(post_breakout.min()) < pivot * (1.0 - VCP_V3_FALSE_BREAK_MAX_BELOW_PIVOT_PCT / 100.0):
                continue
            extension_pct = ((current_price - pivot) / pivot) * 100.0
            if extension_pct > VCP_V3_BREAKOUT_MAX_EXTENSION_PCT:
                continue
            score = _score_broken_out(contraction_count, breakout_volume_ratio, extension_pct, close_position)
            entry = pivot * 1.01
            stop = pivot * 0.96
            risk = entry - stop
            target1 = pivot * (1.0 + VCP_V3_TARGET1_PCT / 100.0)
            target2 = pivot * (1.0 + VCP_V3_TARGET2_PCT / 100.0)
            risk_reward = ((target1 - entry) / risk) if risk > 0 else 0.0
            reasons = [
                "Broken-out VCP v3",
                f"Breakout {days_ago} day(s) ago",
                f"Breakout volume {breakout_volume_ratio:.2f}x 20D average",
                f"Current extension {extension_pct:.1f}% above pivot",
                f"{contraction_count} tightening contractions before breakout",
            ]
            hit = _build_hit(
                ticker=ticker,
                benchmark_ticker=benchmark_ticker,
                category="broken_out",
                signal_date=breakout_date,
                signal_kind="vcp_v3_broken_out",
                current_price=current_price,
                pivot_price=pivot,
                entry_price=entry,
                stop_price=stop,
                target1_price=target1,
                target2_price=target2,
                risk_reward=risk_reward,
                vcp_score=score,
                contraction_count=contraction_count,
                waves=valid_waves,
                contraction_ratios=ratios,
                rising_lows_count=rising_lows_count,
                is_cup=_detect_cup(base),
                volume_dry_up=coil_vol_ratio <= VCP_V3_COIL_VOL_RATIO_MAX,
                coil_atr_percentile=coil_pct,
                coil_volume_ratio=coil_vol_ratio,
                volume_slope_ok=volume_slope_ok,
                breakout_volume_ratio=breakout_volume_ratio,
                days_since_breakout=days_ago,
                extension_pct=extension_pct,
                distance_from_pivot_pct=((pivot - current_price) / current_price) * 100.0,
                prior_uptrend_pct=float(context.get("prior_uptrend_pct") or 0.0),
                rs_vs_benchmark_pct=context.get("rs_vs_benchmark_pct"),
                below_52w_high_pct=context.get("below_52w_high_pct"),
                ma50=float(context.get("ma50") or 0.0),
                ma150=float(context.get("ma150") or 0.0),
                ma200=float(context.get("ma200") or 0.0),
                reasons=reasons,
            )
            if best is None or hit.vcp_score > best.vcp_score:
                best = hit
    return best

def find_vcp_v3_hit(
    frame: pd.DataFrame,
    benchmark_frame: pd.DataFrame,
    *,
    ticker: UniverseTicker,
    benchmark_ticker: str,
) -> VcpV3Hit | None:
    bars = _normalize_price_frame(frame)
    benchmark_bars = _normalize_price_frame(benchmark_frame, include_volume=False)
    if bars.empty or benchmark_bars.empty or len(bars) < 220:
        return None
    benchmark_close = benchmark_bars["Close"].astype(float).reindex(bars.index).ffill().bfill()
    if benchmark_close.isna().any():
        return None
    pre_hit = _scan_pre_breakout(bars, benchmark_close, ticker=ticker, benchmark_ticker=benchmark_ticker)
    if pre_hit is not None:
        return pre_hit
    return _scan_broken_out(bars, benchmark_close, ticker=ticker, benchmark_ticker=benchmark_ticker)


def run_vcp_v3_screen(
    config: AppConfig,
    tickers: list[UniverseTicker],
    *,
    as_of_date: dt.date | None = None,
) -> VcpV3ScreenResult:
    cookstock = load_configured_cookstock(config)
    hits: list[VcpV3Hit] = []
    failures: list[dict[str, str]] = []
    total_tickers = len(tickers)
    run_date = as_of_date or dt.date.today()

    with freeze_cookstock_today(cookstock, as_of_date):
        benchmark_frame = pd.DataFrame()
        try:
            benchmark_financials = cookstock.cookFinancials(
                config.benchmark_ticker,
                benchmarkTicker=config.benchmark_ticker,
                historyLookbackDays=VCP_V3_HISTORY_DAYS,
            )
            benchmark_frame = _build_price_frame(benchmark_financials)
        except Exception as exc:
            failures.append({"ticker": config.benchmark_ticker.upper(), "error": f"benchmark load failed: {exc}"})

        position = 0
        for ticker_batch in iter_prefetched_cookstock_batches(
            config,
            tickers,
            as_of_date=as_of_date,
            history_lookback_days=VCP_V3_HISTORY_DAYS,
            benchmark_ticker=config.benchmark_ticker,
        ):
            for ticker in ticker_batch:
                position += 1
                print(f"[{position}/{total_tickers}] screening {ticker.symbol} | passed={len(hits)}")
                try:
                    if benchmark_frame.empty:
                        raise ValueError("benchmark frame unavailable")
                    financials = cookstock.cookFinancials(
                        ticker.symbol,
                        benchmarkTicker=config.benchmark_ticker,
                        historyLookbackDays=VCP_V3_HISTORY_DAYS,
                    )
                    frame = _build_price_frame(financials)
                    hit = find_vcp_v3_hit(
                        frame,
                        benchmark_frame,
                        ticker=ticker,
                        benchmark_ticker=config.benchmark_ticker,
                    )
                    if hit is None:
                        continue
                    hits.append(hit)
                    print(
                        f"[{position}/{total_tickers}] {ticker.symbol} passed VCP v3 "
                        f"{hit.category} score={hit.vcp_score:.1f} | passed={len(hits)}"
                    )
                except Exception as exc:
                    failures.append({"ticker": ticker.symbol, "error": str(exc)})
                    print(f"[{position}/{total_tickers}] {ticker.symbol} failed: {exc}")

    hits.sort(key=lambda item: (item.category != "pre_breakout", -item.vcp_score, item.ticker))
    print(f"finished VCP v3 screen: passed={len(hits)}, failed={len(failures)}, total={total_tickers}")
    return VcpV3ScreenResult(
        run_date=run_date.isoformat(),
        benchmark_ticker=config.benchmark_ticker,
        total_tickers=total_tickers,
        passed_tickers=len(hits),
        failed_tickers=failures,
        hits=hits,
    )
