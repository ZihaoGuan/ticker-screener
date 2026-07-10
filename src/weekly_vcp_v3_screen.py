from __future__ import annotations

import datetime as dt

import pandas as pd

from .config import AppConfig
from .market_data_access import load_many_ticker_windows, load_ticker_metadata_map, resolve_database_url
from .universe import UniverseTicker
from . import vcp_v3_screen as base
from .weekly_vcp_utils import temporary_attr_overrides, to_weekly_price_frame


WEEKLY_VCP_V3_HISTORY_DAYS = 1040
WEEKLY_VCP_V3_BASE_WINDOWS = (8, 10, 12, 16, 20, 24, 30)


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


def _weekly_below_52w_high_pct(bars: pd.DataFrame) -> float | None:
    if bars.empty:
        return None
    high_52w = float(bars["High"].astype(float).tail(min(52, len(bars))).max())
    price = float(bars["Close"].astype(float).iloc[-1])
    if high_52w <= 0:
        return None
    return ((high_52w - price) / high_52w) * 100.0


def _weekly_prior_uptrend_pct(bars: pd.DataFrame, base_start_idx: int, prior_window: int = 26) -> float | None:
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


def find_weekly_vcp_v3_hit(
    frame: pd.DataFrame,
    benchmark_frame: pd.DataFrame,
    *,
    ticker: UniverseTicker,
    benchmark_ticker: str,
) -> base.VcpV3Hit | None:
    bars = to_weekly_price_frame(frame)
    benchmark_bars = to_weekly_price_frame(benchmark_frame, include_volume=False)
    if bars.empty or benchmark_bars.empty or len(bars) < 44:
        return None
    benchmark_close = benchmark_bars["Close"].astype(float).reindex(bars.index).ffill().bfill()
    if benchmark_close.isna().any():
        return None
    with temporary_attr_overrides(
        base,
        VCP_V3_BASE_WINDOWS=WEEKLY_VCP_V3_BASE_WINDOWS,
        VCP_V3_BREAKOUT_LOOKBACK=6,
        VCP_V3_SWING_MIN_BARS=1,
        VCP_V3_RS_LOOKBACK_DAYS=13,
        VCP_V3_MAX_BELOW_52W_HIGH_PCT=25.0,
        VCP_V3_MIN_PRIOR_UPTREND_PCT=18.0,
        VCP_V3_BREAKOUT_VOL_RATIO=1.3,
        VCP_V3_BREAKOUT_MAX_EXTENSION_PCT=25.0,
        VCP_V3_VOL_SLOPE_SESSIONS=6,
        VCP_V3_COIL_ATR_PCTILE_MAX=55.0,
        VCP_V3_NEAR_PIVOT_PCT=5.0,
        _stage2_snapshot=_weekly_stage2_snapshot,
        _below_52w_high_pct=_weekly_below_52w_high_pct,
        _prior_uptrend_pct=_weekly_prior_uptrend_pct,
    ):
        pre_hit = base._scan_pre_breakout(bars, benchmark_close, ticker=ticker, benchmark_ticker=benchmark_ticker)
        if pre_hit is not None:
            return pre_hit
        return base._scan_broken_out(bars, benchmark_close, ticker=ticker, benchmark_ticker=benchmark_ticker)


def run_weekly_vcp_v3_screen(
    config: AppConfig,
    tickers: list[UniverseTicker],
    *,
    as_of_date: dt.date | None = None,
    database_url: str | None = None,
) -> base.VcpV3ScreenResult:
    run_date = as_of_date or dt.date.today()
    resolved_database_url = resolve_database_url(database_url)
    benchmark_ticker = config.benchmark_ticker.upper()
    symbols = [ticker.symbol for ticker in tickers]
    frame_map = load_many_ticker_windows(
        symbols + [benchmark_ticker],
        run_date,
        WEEKLY_VCP_V3_HISTORY_DAYS,
        database_url=resolved_database_url,
    )
    metadata_map = load_ticker_metadata_map(symbols, database_url=resolved_database_url)
    benchmark_frame = frame_map.get(benchmark_ticker)
    total_tickers = len(tickers)
    hits: list[base.VcpV3Hit] = []
    failures: list[dict[str, str]] = []

    _log(f"starting weekly VCP v3 screen: total={total_tickers}")
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
        if frame is None or getattr(frame, "empty", False) or benchmark_frame is None or getattr(benchmark_frame, "empty", False):
            failures.append({"ticker": runtime_ticker.symbol, "error": "missing_daily_bars"})
            _log(f"[{position}/{total_tickers}] {runtime_ticker.symbol} failed: missing daily_bars")
            continue
        try:
            hit = find_weekly_vcp_v3_hit(
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
            f"[{position}/{total_tickers}] {runtime_ticker.symbol} passed weekly VCP v3 "
            f"{hit.category} score={hit.vcp_score:.1f} | passed={len(hits)}"
        )

    hits.sort(key=lambda item: (item.category != "pre_breakout", -item.vcp_score, item.ticker))
    _log(f"finished weekly VCP v3 screen: passed={len(hits)}, failed={len(failures)}, total={total_tickers}")
    return base.VcpV3ScreenResult(
        run_date=run_date.isoformat(),
        benchmark_ticker=benchmark_ticker,
        total_tickers=total_tickers,
        passed_tickers=len(hits),
        failed_tickers=failures,
        hits=hits,
    )
