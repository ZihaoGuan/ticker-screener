from __future__ import annotations

from dataclasses import asdict
import datetime as dt

import pandas as pd

from .config import AppConfig
from .market_data_access import load_many_ticker_windows, load_ticker_metadata_map, resolve_database_url
from .universe import UniverseTicker
from .vcp_screen import VcpHit, VcpScreenResult, _build_reasons
from .vcp_v3_screen import (
    _calc_rs_pct,
    check_final_coil,
    check_rising_lows,
    find_swing_points,
    identify_pivot,
    measure_contraction_waves,
    validate_contraction,
)
from .weekly_vcp_utils import to_weekly_price_frame


WEEKLY_VCP_HISTORY_DAYS = 780
WEEKLY_VCP_BASE_WINDOWS = (8, 10, 12, 16, 20, 24, 30)
WEEKLY_VCP_MIN_BARS = 40
WEEKLY_VCP_RS_LOOKBACK_WEEKS = 13
WEEKLY_VCP_BREAKOUT_VOLUME_RATIO = 1.3
WEEKLY_VCP_MAX_BELOW_52W_HIGH_PCT = 25.0
WEEKLY_VCP_MIN_PRIOR_UPTREND_PCT = 18.0


def _log(message: str) -> None:
    print(message, flush=True)


def _weekly_stage2_snapshot(bars: pd.DataFrame) -> tuple[bool, float, float, float]:
    close = bars["Close"].astype(float)
    ma10 = close.rolling(10).mean()
    ma30 = close.rolling(30).mean()
    ma40 = close.rolling(40).mean()
    if len(close) < 44 or pd.isna(ma10.iloc[-1]) or pd.isna(ma30.iloc[-1]) or pd.isna(ma40.iloc[-1]):
        return False, 0.0, 0.0, 0.0
    latest_close = float(close.iloc[-1])
    latest_ma10 = float(ma10.iloc[-1])
    latest_ma30 = float(ma30.iloc[-1])
    latest_ma40 = float(ma40.iloc[-1])
    prior_ma40 = float(ma40.iloc[-4]) if pd.notna(ma40.iloc[-4]) else latest_ma40
    passed = (
        latest_close > latest_ma10 > latest_ma30 > latest_ma40
        and latest_ma30 > latest_ma40
        and latest_ma40 > prior_ma40
    )
    return bool(passed), latest_ma10, latest_ma30, latest_ma40


def _below_52w_high_pct(bars: pd.DataFrame) -> float | None:
    if bars.empty:
        return None
    high_52w = float(bars["High"].astype(float).tail(min(52, len(bars))).max())
    price = float(bars["Close"].astype(float).iloc[-1])
    if high_52w <= 0:
        return None
    return ((high_52w - price) / high_52w) * 100.0


def _prior_uptrend_pct(bars: pd.DataFrame, *, base_start_idx: int, prior_window: int = 26) -> float | None:
    prior_end = max(0, int(base_start_idx))
    prior_start = max(0, prior_end - prior_window)
    if prior_end - prior_start < 8:
        return None
    segment = bars.iloc[prior_start:prior_end]
    if segment.empty:
        return None
    low = float(segment["Low"].astype(float).min())
    high = float(segment["High"].astype(float).max())
    if low <= 0:
        return None
    return ((high - low) / low) * 100.0


def _build_vcp_record(
    pairs: list[tuple[dict[str, object], dict[str, object]]],
    waves: list[float],
) -> list[list[object]]:
    records: list[list[object]] = []
    for index, (peak, trough) in enumerate(pairs[: len(waves)]):
        records.append(
            [
                pd.Timestamp(peak["date"]).date().isoformat(),
                round(float(peak["price"]), 4),
                pd.Timestamp(trough["date"]).date().isoformat(),
                round(float(trough["price"]), 4),
                round(float(waves[index]), 2),
            ]
        )
    return records


def _build_footprint(base: pd.DataFrame) -> list[list[object]]:
    footprint: list[list[object]] = []
    for idx, row in base.tail(12).iterrows():
        footprint.append(
            [
                pd.Timestamp(idx).date().isoformat(),
                round(float(row["Open"]), 4),
                round(float(row["High"]), 4),
                round(float(row["Low"]), 4),
                round(float(row["Close"]), 4),
                round(float(row["Volume"]), 2),
            ]
        )
    return footprint


def find_weekly_vcp_hit(
    frame: pd.DataFrame,
    benchmark_frame: pd.DataFrame,
    *,
    ticker: UniverseTicker,
    benchmark_ticker: str,
    screen_profile: str = "weekly",
) -> VcpHit | None:
    bars = to_weekly_price_frame(frame)
    benchmark_bars = to_weekly_price_frame(benchmark_frame, include_volume=False)
    if bars.empty or benchmark_bars.empty or len(bars) < WEEKLY_VCP_MIN_BARS:
        return None

    benchmark_close = benchmark_bars["Close"].astype(float).reindex(bars.index).ffill().bfill()
    if benchmark_close.isna().any():
        return None

    is_stage2, ma10, ma30, ma40 = _weekly_stage2_snapshot(bars)
    if not is_stage2:
        return None

    below_52w = _below_52w_high_pct(bars)
    if below_52w is None or below_52w > WEEKLY_VCP_MAX_BELOW_52W_HIGH_PCT:
        return None

    best: VcpHit | None = None
    close = bars["Close"].astype(float)
    high = bars["High"].astype(float)
    volume = bars["Volume"].astype(float)
    current_price = float(close.iloc[-1])
    year_high = float(high.tail(min(52, len(bars))).max())
    current_rs_line = float(current_price / benchmark_close.iloc[-1]) if float(benchmark_close.iloc[-1]) > 0 else None
    rs_line_high = float((close / benchmark_close).tail(WEEKLY_VCP_RS_LOOKBACK_WEEKS).max()) if (benchmark_close > 0).all() else None
    stock_return_vs_rs_window_pct = _calc_rs_pct(close, benchmark_close, lookback=WEEKLY_VCP_RS_LOOKBACK_WEEKS)

    for base_weeks in WEEKLY_VCP_BASE_WINDOWS:
        if len(bars) < base_weeks:
            continue
        base = bars.tail(base_weeks)
        base_start_idx = len(bars) - len(base)
        prior_uptrend = _prior_uptrend_pct(bars, base_start_idx=base_start_idx)
        if prior_uptrend is None or prior_uptrend < WEEKLY_VCP_MIN_PRIOR_UPTREND_PCT:
            continue
        swings = find_swing_points(base, threshold_pct=4.0, min_bars=1)
        waves, pairs = measure_contraction_waves(swings)
        contraction_count, is_contracting, valid_waves, ratios = validate_contraction(waves)
        if not is_contracting:
            continue
        rising_lows_count, rising_ok = check_rising_lows(swings)
        if not rising_ok:
            continue
        pivot_price = identify_pivot(base, swings)
        if pivot_price <= 0:
            continue
        distance_from_pivot_pct = ((current_price / pivot_price) - 1.0) * 100.0
        if distance_from_pivot_pct < -8.0 or distance_from_pivot_pct > 12.0:
            continue
        low_swings = [float(item.get("price") or 0.0) for item in swings if str(item.get("type")) == "L"]
        support_price = low_swings[-1] if low_swings else float(base["Low"].astype(float).tail(4).min())
        coil_pct, volume_slope_ok, coil_vol_ratio = check_final_coil(base)
        recent_base_volume = float(base["Volume"].astype(float).tail(10).mean()) if len(base) >= 10 else float(base["Volume"].astype(float).mean())
        recent_3w_volume = float(base["Volume"].astype(float).tail(3).mean())
        breakout_avg_volume = float(volume.iloc[max(0, len(bars) - 11) : -1].mean()) if len(bars) > 1 else float(volume.iloc[-1])
        breakout_volume_ratio = (float(volume.iloc[-1]) / breakout_avg_volume) if breakout_avg_volume > 0 else 0.0
        is_breakout = current_price >= pivot_price and breakout_volume_ratio >= WEEKLY_VCP_BREAKOUT_VOLUME_RATIO
        is_demand_dry = bool(volume_slope_ok or (recent_base_volume > 0 and recent_3w_volume <= recent_base_volume * 0.8) or coil_vol_ratio <= 0.75)

        payload = {
            "current_price": current_price,
            "support_price": support_price,
            "pivot_price": pivot_price,
            "vcp_contractions_count": contraction_count,
            "vcp_record": _build_vcp_record(pairs, valid_waves),
            "footprint": _build_footprint(base),
            "is_vcp_structure_valid": bool(is_contracting),
            "is_good_pivot": bool(-3.0 <= distance_from_pivot_pct <= 8.0),
            "is_deep_correction": bool(valid_waves and valid_waves[0] > 35.0),
            "is_demand_dry": is_demand_dry,
            "demand_dry_start_date": pd.Timestamp(base.index[-3]).date().isoformat() if len(base) >= 3 and is_demand_dry else None,
            "demand_dry_end_date": pd.Timestamp(base.index[-1]).date().isoformat() if is_demand_dry else None,
            "demand_dry_volume_slope": -1.0 if volume_slope_ok else None,
            "demand_dry_recent_volume_slope": -1.0 if recent_3w_volume < recent_base_volume else None,
            "is_breakout_volume_confirmed": is_breakout,
            "breakout_day_volume": float(volume.iloc[-1]),
            "breakout_avg_volume_50": breakout_avg_volume,
            "is_near_year_high": below_52w <= 0.15,
            "year_high": year_high,
            "distance_from_year_high_pct": below_52w,
            "is_strong_rs": stock_return_vs_rs_window_pct is not None and stock_return_vs_rs_window_pct > 0.0,
            "stock_return_vs_rs_window_pct": stock_return_vs_rs_window_pct,
            "benchmark_return_vs_rs_window_pct": 0.0,
            "current_rs_line": current_rs_line,
            "rs_line_high": rs_line_high,
            "is_sector_etf_strong": None,
            "sector_etf": None,
            "sector_etf_near_year_high": None,
            "sector_etf_distance_from_year_high_pct": None,
            "sector_etf_return_vs_rs_window_pct": None,
            "sector_benchmark_return_vs_rs_window_pct": None,
        }
        payload["reasons"] = _build_reasons(payload)
        candidate = VcpHit(
            ticker=ticker.symbol,
            sector=ticker.sector,
            exchange=ticker.exchange,
            signal_date=pd.Timestamp(bars.index[-1]).date().isoformat(),
            benchmark_ticker=benchmark_ticker,
            screen_profile=screen_profile,
            current_price=round(float(payload["current_price"]), 4),
            support_price=round(float(payload["support_price"]), 4),
            pivot_price=round(float(payload["pivot_price"]), 4),
            vcp_contractions_count=int(payload["vcp_contractions_count"]),
            vcp_record=list(payload["vcp_record"]),
            footprint=list(payload["footprint"]),
            is_vcp_structure_valid=bool(payload["is_vcp_structure_valid"]),
            is_good_pivot=bool(payload["is_good_pivot"]),
            is_deep_correction=bool(payload["is_deep_correction"]),
            is_demand_dry=bool(payload["is_demand_dry"]),
            demand_dry_start_date=payload["demand_dry_start_date"],
            demand_dry_end_date=payload["demand_dry_end_date"],
            demand_dry_volume_slope=payload["demand_dry_volume_slope"],
            demand_dry_recent_volume_slope=payload["demand_dry_recent_volume_slope"],
            is_breakout_volume_confirmed=bool(payload["is_breakout_volume_confirmed"]),
            breakout_day_volume=round(float(payload["breakout_day_volume"]), 2),
            breakout_avg_volume_50=round(float(payload["breakout_avg_volume_50"]), 2),
            is_near_year_high=payload["is_near_year_high"],
            year_high=payload["year_high"],
            distance_from_year_high_pct=payload["distance_from_year_high_pct"],
            is_strong_rs=payload["is_strong_rs"],
            stock_return_vs_rs_window_pct=payload["stock_return_vs_rs_window_pct"],
            benchmark_return_vs_rs_window_pct=payload["benchmark_return_vs_rs_window_pct"],
            current_rs_line=payload["current_rs_line"],
            rs_line_high=payload["rs_line_high"],
            is_sector_etf_strong=None,
            sector_etf=None,
            sector_etf_near_year_high=None,
            sector_etf_distance_from_year_high_pct=None,
            sector_etf_return_vs_rs_window_pct=None,
            sector_benchmark_return_vs_rs_window_pct=None,
            reasons=list(payload["reasons"]),
        )
        if best is None or (
            candidate.is_breakout_volume_confirmed,
            candidate.vcp_contractions_count,
            -abs(distance_from_pivot_pct),
            rising_lows_count,
        ) > (
            best.is_breakout_volume_confirmed,
            best.vcp_contractions_count,
            -abs(((best.current_price / best.pivot_price) - 1.0) * 100.0 if best.pivot_price > 0 else 0.0),
            0,
        ):
            best = candidate
    return best


def run_weekly_vcp_screen(
    config: AppConfig,
    tickers: list[UniverseTicker],
    *,
    as_of_date: dt.date | None = None,
    database_url: str | None = None,
) -> VcpScreenResult:
    run_date = as_of_date or dt.date.today()
    resolved_database_url = resolve_database_url(database_url)
    benchmark_ticker = config.benchmark_ticker.upper()
    total_tickers = len(tickers)
    failures: list[dict[str, str]] = []
    hits: list[VcpHit] = []

    symbols = [ticker.symbol for ticker in tickers]
    frame_map = load_many_ticker_windows(
        symbols + [benchmark_ticker],
        run_date,
        WEEKLY_VCP_HISTORY_DAYS,
        database_url=resolved_database_url,
    )
    metadata_map = load_ticker_metadata_map(symbols, database_url=resolved_database_url)
    benchmark_frame = frame_map.get(benchmark_ticker)
    if benchmark_frame is None or getattr(benchmark_frame, "empty", False):
        failures.append({"ticker": benchmark_ticker, "error": "missing_daily_bars_for_benchmark"})
        benchmark_frame = pd.DataFrame()

    _log(f"starting weekly VCP screen: total={total_tickers}")
    for position, ticker in enumerate(tickers, start=1):
        metadata = metadata_map.get(ticker.symbol, {})
        runtime_ticker = UniverseTicker(
            symbol=ticker.symbol,
            sector=ticker.sector or str(metadata.get("sector") or "") or None,
            industry=ticker.industry or str(metadata.get("industry") or "") or None,
            exchange=ticker.exchange or str(metadata.get("exchange") or "") or None,
        )
        _log(f"[{position}/{total_tickers}] screening {runtime_ticker.symbol} | passed={len(hits)}")
        frame = frame_map.get(runtime_ticker.symbol)
        if frame is None or getattr(frame, "empty", False) or benchmark_frame.empty:
            failures.append({"ticker": runtime_ticker.symbol, "error": "missing_daily_bars"})
            _log(f"[{position}/{total_tickers}] {runtime_ticker.symbol} failed: missing daily_bars")
            continue
        try:
            hit = find_weekly_vcp_hit(
                frame,
                benchmark_frame,
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
            f"[{position}/{total_tickers}] {runtime_ticker.symbol} passed weekly VCP "
            f"{hit.vcp_contractions_count} contractions breakout={hit.is_breakout_volume_confirmed} | passed={len(hits)}"
        )

    hits.sort(key=lambda item: (item.is_breakout_volume_confirmed, item.vcp_contractions_count, item.ticker), reverse=True)
    _log(f"finished weekly VCP screen: passed={len(hits)}, failed={len(failures)}, total={total_tickers}")
    return VcpScreenResult(
        run_date=run_date.isoformat(),
        benchmark_ticker=benchmark_ticker,
        total_tickers=total_tickers,
        passed_tickers=len(hits),
        failed_tickers=failures,
        hits=hits,
    )
