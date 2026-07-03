from __future__ import annotations

from dataclasses import asdict, dataclass
import datetime as dt

import numpy as np
import pandas as pd

from .config import AppConfig
from .market_data_access import (
    load_active_universe_from_db,
    load_many_ticker_windows,
    load_ticker_metadata_map,
    resolve_database_url,
)
from .universe import UniverseTicker


VCP_SPEC_HISTORY_DAYS = 420
DB_BATCH_SIZE = 400
_BASE_MIN_BARS = 21
_BASE_MAX_BARS = 84
_SWING_THRESHOLD_PCT = 3.0
_SWING_MIN_BARS = 3
_PRIOR_UPTREND_MIN_PCT = 28.0
_PRIOR_UPTREND_MIN_WEEKS = 8
_MONOTONIC_TOLERANCE_PCT = 0.5
_VOLUME_DECLINE_TOLERANCE_RATIO = 1.10
_PIVOT_TOLERANCE_PCT = 1.5
_BREAKOUT_VOLUME_RATIO_MIN = 1.4
_DEPTH_RANGES = (
    (10.0, 35.0),
    (5.0, 20.0),
    (3.0, 15.0),
)


def _log(message: str) -> None:
    print(message, flush=True)


@dataclass(frozen=True)
class VcpSpecContraction:
    start_date: str
    end_date: str
    peak_price: float
    trough_price: float
    depth_pct: float
    duration_days: int
    avg_volume: float


@dataclass(frozen=True)
class VcpSpecHit:
    ticker: str
    sector: str | None
    industry: str | None
    exchange: str | None
    signal_date: str
    benchmark_ticker: str
    category: str
    current_price: float
    pivot_price: float
    stop_price: float
    base_start_date: str
    base_end_date: str
    base_duration_days: int
    base_top_price: float
    contractions_count: int
    contractions: list[dict[str, object]]
    contraction_depths: list[float]
    prior_uptrend_pct: float
    prior_uptrend_weeks: int
    pivot_within_top_pct: float
    breakout_observed: bool
    breakout_volume_ratio: float | None
    geometric_score: float
    criteria_pass: dict[str, bool]
    reasons: list[str]

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True)
class VcpSpecScreenResult:
    run_date: str
    benchmark_ticker: str
    total_tickers: int
    passed_tickers: int
    failed_tickers: list[dict[str, str]]
    hits: list[VcpSpecHit]

    def to_dict(self) -> dict[str, object]:
        return {
            "run_date": self.run_date,
            "benchmark_ticker": self.benchmark_ticker,
            "total_tickers": self.total_tickers,
            "passed_tickers": self.passed_tickers,
            "failed_tickers": self.failed_tickers,
            "hits": [item.to_dict() for item in self.hits],
        }


def _normalize_price_frame(frame: pd.DataFrame) -> pd.DataFrame:
    required = ["Open", "High", "Low", "Close", "Volume"]
    available = {str(column).lower(): column for column in frame.columns}
    if any(column.lower() not in available for column in required):
        return pd.DataFrame()
    normalized = frame[[available[column.lower()] for column in required]].copy()
    normalized.columns = required
    normalized = normalized.dropna(subset=required).sort_index()
    if not isinstance(normalized.index, pd.DatetimeIndex):
        normalized.index = pd.to_datetime(normalized.index)
    return normalized


def _slope(series: pd.Series) -> float:
    values = series.dropna().astype(float).to_numpy()
    if values.size < 2:
        return 0.0
    x = np.arange(values.size, dtype=float)
    return float(np.polyfit(x, values, 1)[0])


def _stage2_snapshot(bars: pd.DataFrame) -> bool:
    close = bars["Close"].astype(float)
    ma50 = close.rolling(50).mean()
    ma150 = close.rolling(150).mean()
    ma200 = close.rolling(200).mean()
    if len(close) < 220 or pd.isna(ma50.iloc[-1]) or pd.isna(ma150.iloc[-1]) or pd.isna(ma200.iloc[-1]):
        return False
    latest_close = float(close.iloc[-1])
    latest_ma50 = float(ma50.iloc[-1])
    latest_ma150 = float(ma150.iloc[-1])
    latest_ma200 = float(ma200.iloc[-1])
    prior_ma200 = float(ma200.iloc[-20]) if pd.notna(ma200.iloc[-20]) else latest_ma200
    return bool(
        latest_close > latest_ma50 > latest_ma150 > latest_ma200
        and latest_ma150 > latest_ma200
        and latest_ma200 > prior_ma200
        and _slope(ma50.tail(10)) > 0.0
    )


def _find_swings(
    bars: pd.DataFrame,
    *,
    threshold_pct: float = _SWING_THRESHOLD_PCT,
    min_bars: int = _SWING_MIN_BARS,
) -> list[dict[str, object]]:
    if bars.empty or len(bars) < 8:
        return []
    highs = bars["High"].astype(float).to_numpy()
    lows = bars["Low"].astype(float).to_numpy()
    direction = "H"
    extreme_idx = 0
    extreme_price = highs[0]
    swings: list[dict[str, object]] = []

    for index in range(1, len(bars)):
        if direction == "H":
            if highs[index] >= extreme_price:
                extreme_idx = index
                extreme_price = highs[index]
            drawdown_pct = ((extreme_price - lows[index]) / extreme_price) * 100.0 if extreme_price > 0 else 0.0
            if drawdown_pct >= threshold_pct and (index - extreme_idx) >= min_bars:
                swings.append({"idx": extreme_idx, "price": float(extreme_price), "type": "H", "date": bars.index[extreme_idx]})
                direction = "L"
                extreme_idx = index
                extreme_price = lows[index]
        else:
            if lows[index] <= extreme_price:
                extreme_idx = index
                extreme_price = lows[index]
            rally_pct = ((highs[index] - extreme_price) / extreme_price) * 100.0 if extreme_price > 0 else 0.0
            if rally_pct >= threshold_pct and (index - extreme_idx) >= min_bars:
                swings.append({"idx": extreme_idx, "price": float(extreme_price), "type": "L", "date": bars.index[extreme_idx]})
                direction = "H"
                extreme_idx = index
                extreme_price = highs[index]

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


def _build_contractions(base: pd.DataFrame, swings: list[dict[str, object]]) -> list[VcpSpecContraction]:
    contractions: list[VcpSpecContraction] = []
    ordered = swings[1:] if swings and str(swings[0].get("type")) != "H" else swings
    for index in range(len(ordered) - 1):
        peak = ordered[index]
        trough = ordered[index + 1]
        if str(peak.get("type")) != "H" or str(trough.get("type")) != "L":
            continue
        peak_price = float(peak.get("price") or 0.0)
        trough_price = float(trough.get("price") or 0.0)
        if peak_price <= 0 or trough_price <= 0 or trough_price >= peak_price:
            continue
        peak_idx = int(peak.get("idx") or 0)
        trough_idx = int(trough.get("idx") or 0)
        if trough_idx <= peak_idx:
            continue
        start_ts = pd.Timestamp(peak["date"])
        end_ts = pd.Timestamp(trough["date"])
        segment = base.iloc[peak_idx : trough_idx + 1]
        contractions.append(
            VcpSpecContraction(
                start_date=start_ts.date().isoformat(),
                end_date=end_ts.date().isoformat(),
                peak_price=round(peak_price, 4),
                trough_price=round(trough_price, 4),
                depth_pct=round(((peak_price - trough_price) / peak_price) * 100.0, 2),
                duration_days=max(1, (end_ts.date() - start_ts.date()).days),
                avg_volume=round(float(segment["Volume"].astype(float).mean()), 2),
            )
        )
    return contractions


def _prior_uptrend(base_start_idx: int, bars: pd.DataFrame) -> tuple[float, int]:
    prior_start = max(0, base_start_idx - 90)
    if base_start_idx - prior_start < 15:
        return 0.0, 0
    prior = bars.iloc[prior_start:base_start_idx]
    if prior.empty:
        return 0.0, 0
    low_idx = prior["Low"].astype(float).idxmin()
    low_price = float(prior.loc[low_idx, "Low"])
    base_start_close = float(bars["Close"].astype(float).iloc[base_start_idx])
    if low_price <= 0:
        return 0.0, 0
    uptrend_pct = ((base_start_close - low_price) / low_price) * 100.0
    weeks = max(0, (pd.Timestamp(bars.index[base_start_idx]).date() - pd.Timestamp(low_idx).date()).days // 7)
    return round(uptrend_pct, 2), weeks


def _check_monotonic(contractions: list[VcpSpecContraction]) -> bool:
    if len(contractions) < 2:
        return False
    for index in range(1, len(contractions)):
        previous = contractions[index - 1].depth_pct
        current = contractions[index].depth_pct
        if current >= previous + _MONOTONIC_TOLERANCE_PCT:
            return False
    return True


def _check_depth_bounds(contractions: list[VcpSpecContraction]) -> bool:
    if len(contractions) < 2:
        return False
    for index, contraction in enumerate(contractions):
        lower, upper = _DEPTH_RANGES[index] if index < len(_DEPTH_RANGES) else _DEPTH_RANGES[-1]
        if not (lower <= contraction.depth_pct <= upper):
            return False
    return True


def _check_volume_decline(contractions: list[VcpSpecContraction]) -> bool:
    if len(contractions) < 2:
        return False
    for index in range(1, len(contractions)):
        previous = contractions[index - 1].avg_volume
        current = contractions[index].avg_volume
        if previous <= 0 or current >= previous * _VOLUME_DECLINE_TOLERANCE_RATIO:
            return False
    return True


def _breakout_ratio(bars: pd.DataFrame, breakout_idx: int) -> float | None:
    if breakout_idx <= 0:
        return None
    prior = bars["Volume"].astype(float).iloc[max(0, breakout_idx - 50) : breakout_idx]
    if prior.empty:
        return None
    baseline = float(prior.mean())
    if baseline <= 0:
        return None
    return float(bars["Volume"].astype(float).iloc[breakout_idx]) / baseline


def _evaluate_candidate(
    bars: pd.DataFrame,
    *,
    base_start_idx: int,
    base_end_idx: int,
    ticker: UniverseTicker,
    benchmark_ticker: str,
) -> VcpSpecHit | None:
    base = bars.iloc[base_start_idx : base_end_idx + 1]
    context = bars.iloc[: base_end_idx + 1]
    if len(base) < _BASE_MIN_BARS or not _stage2_snapshot(context):
        return None
    contractions = _build_contractions(base, _find_swings(base))
    if len(contractions) < 2:
        return None

    prior_uptrend_pct, prior_uptrend_weeks = _prior_uptrend(base_start_idx, bars)
    base_start_date = pd.Timestamp(base.index[0]).date()
    base_end_date = pd.Timestamp(base.index[-1]).date()
    base_duration_days = len(base)
    base_top_price = float(contractions[-1].peak_price)
    current_price = float(base["Close"].astype(float).iloc[-1])
    pivot_within_top_pct = ((current_price / base_top_price) - 1.0) * 100.0 if base_top_price > 0 else 0.0
    breakout_ratio = _breakout_ratio(bars, base_end_idx)
    breakout_observed = bool(current_price > base_top_price and breakout_ratio is not None and breakout_ratio >= _BREAKOUT_VOLUME_RATIO_MIN)
    criteria_pass = {
        "criterion_1": True,
        "criterion_2": prior_uptrend_pct >= _PRIOR_UPTREND_MIN_PCT and prior_uptrend_weeks >= _PRIOR_UPTREND_MIN_WEEKS,
        "criterion_3": _check_monotonic(contractions),
        "criterion_4": _check_depth_bounds(contractions),
        "criterion_5": _check_volume_decline(contractions),
        "criterion_6": _BASE_MIN_BARS <= base_duration_days <= _BASE_MAX_BARS,
        "criterion_7": abs(pivot_within_top_pct) <= _PIVOT_TOLERANCE_PCT,
        "criterion_8": breakout_observed,
    }
    if not all(criteria_pass[f"criterion_{index}"] for index in range(1, 8)):
        return None

    reasons = [
        "strict stage 2 structure",
        f"{len(contractions)} contractions",
        f"prior uptrend {prior_uptrend_pct:.1f}% over {prior_uptrend_weeks} weeks",
        f"pivot {pivot_within_top_pct:+.2f}% vs base top",
    ]
    if breakout_observed:
        reasons.append(f"breakout volume {breakout_ratio:.2f}x 50D")
    else:
        reasons.append("pre-breakout coil near pivot")

    return VcpSpecHit(
        ticker=ticker.symbol,
        sector=ticker.sector,
        industry=ticker.industry,
        exchange=ticker.exchange,
        signal_date=base_end_date.isoformat(),
        benchmark_ticker=benchmark_ticker,
        category="breakout" if breakout_observed else "pre_breakout",
        current_price=round(current_price, 4),
        pivot_price=round(base_top_price, 4),
        stop_price=round(float(contractions[-1].trough_price), 4),
        base_start_date=base_start_date.isoformat(),
        base_end_date=base_end_date.isoformat(),
        base_duration_days=base_duration_days,
        base_top_price=round(base_top_price, 4),
        contractions_count=len(contractions),
        contractions=[asdict(item) for item in contractions],
        contraction_depths=[item.depth_pct for item in contractions],
        prior_uptrend_pct=prior_uptrend_pct,
        prior_uptrend_weeks=prior_uptrend_weeks,
        pivot_within_top_pct=round(pivot_within_top_pct, 2),
        breakout_observed=breakout_observed,
        breakout_volume_ratio=round(breakout_ratio, 2) if breakout_ratio is not None else None,
        geometric_score=1.0,
        criteria_pass=criteria_pass,
        reasons=reasons,
    )


def find_recent_vcp_spec_hit(
    frame: pd.DataFrame,
    *,
    ticker: UniverseTicker,
    benchmark_ticker: str = "SPY",
) -> VcpSpecHit | None:
    bars = _normalize_price_frame(frame)
    if bars.empty or len(bars) < 220:
        return None
    best: VcpSpecHit | None = None
    latest_base_end = len(bars) - 1
    for base_end_idx in range(latest_base_end, max(_BASE_MIN_BARS - 1, latest_base_end - 19) - 1, -1):
        for window_size in range(_BASE_MAX_BARS, _BASE_MIN_BARS - 1, -1):
            base_start_idx = base_end_idx - window_size + 1
            if base_start_idx < 120:
                continue
            hit = _evaluate_candidate(
                bars,
                base_start_idx=base_start_idx,
                base_end_idx=base_end_idx,
                ticker=ticker,
                benchmark_ticker=benchmark_ticker,
            )
            if hit is None:
                continue
            if best is None or (
                hit.contractions_count,
                -abs(hit.pivot_within_top_pct),
                hit.prior_uptrend_pct,
            ) > (
                best.contractions_count,
                -abs(best.pivot_within_top_pct),
                best.prior_uptrend_pct,
            ):
                best = hit
    return best


def run_vcp_spec_screen(
    config: AppConfig,
    tickers: list[UniverseTicker],
    *,
    as_of_date: dt.date | None = None,
    database_url: str | None = None,
) -> VcpSpecScreenResult:
    run_date = as_of_date or dt.date.today()
    resolved_database_url = resolve_database_url(database_url)
    total_tickers = len(tickers)
    hits: list[VcpSpecHit] = []
    failures: list[dict[str, str]] = []
    benchmark_ticker = config.benchmark_ticker

    _log(f"starting vcp spec screen: total={total_tickers}")
    for batch_start in range(0, total_tickers, DB_BATCH_SIZE):
        batch = tickers[batch_start : batch_start + DB_BATCH_SIZE]
        symbols = [ticker.symbol for ticker in batch]
        frame_map = load_many_ticker_windows(
            symbols,
            run_date,
            VCP_SPEC_HISTORY_DAYS,
            database_url=resolved_database_url,
        )
        metadata_map = load_ticker_metadata_map(symbols, database_url=resolved_database_url)
        for offset, ticker in enumerate(batch, start=1):
            position = batch_start + offset
            metadata = metadata_map.get(ticker.symbol, {})
            runtime_ticker = UniverseTicker(
                symbol=ticker.symbol,
                sector=ticker.sector or str(metadata.get("sector") or "") or None,
                industry=ticker.industry or str(metadata.get("industry") or "") or None,
                exchange=ticker.exchange or str(metadata.get("exchange") or "") or None,
            )
            _log(f"[{position}/{total_tickers}] screening {runtime_ticker.symbol} | passed={len(hits)}")
            frame = frame_map.get(runtime_ticker.symbol)
            if frame is None or getattr(frame, "empty", False):
                failures.append({"ticker": runtime_ticker.symbol, "error": "missing_daily_bars"})
                _log(f"[{position}/{total_tickers}] {runtime_ticker.symbol} failed: missing daily_bars")
                continue
            try:
                hit = find_recent_vcp_spec_hit(
                    frame,
                    ticker=runtime_ticker,
                    benchmark_ticker=benchmark_ticker,
                )
            except Exception as exc:
                failures.append({"ticker": runtime_ticker.symbol, "error": str(exc)})
                _log(f"[{position}/{total_tickers}] {runtime_ticker.symbol} failed: {exc}")
                continue
            if hit is None:
                continue
            hits.append(hit)
            _log(
                f"[{position}/{total_tickers}] {runtime_ticker.symbol} passed "
                f"{hit.category} contractions={hit.contractions_count} pivot_delta={hit.pivot_within_top_pct:+.2f}% | passed={len(hits)}"
            )

    hits.sort(
        key=lambda item: (
            item.category != "pre_breakout",
            -item.contractions_count,
            abs(item.pivot_within_top_pct),
            -item.prior_uptrend_pct,
        )
    )
    _log(f"completed vcp spec screen: total={total_tickers} hits={len(hits)} failed={len(failures)}")
    return VcpSpecScreenResult(
        run_date=run_date.isoformat(),
        benchmark_ticker=benchmark_ticker,
        total_tickers=total_tickers,
        passed_tickers=len(hits),
        failed_tickers=failures,
        hits=hits,
    )


def load_vcp_spec_universe(
    *,
    as_of_date: dt.date | None = None,
    limit: int | None = None,
    database_url: str | None = None,
) -> list[UniverseTicker]:
    return load_active_universe_from_db(
        as_of_date=as_of_date,
        limit=limit,
        database_url=resolve_database_url(database_url),
    )
