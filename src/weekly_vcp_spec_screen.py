from __future__ import annotations

import datetime as dt

import pandas as pd

from .config import AppConfig
from .market_data_access import load_active_universe_from_db, load_many_ticker_windows, load_ticker_metadata_map, resolve_database_url
from .universe import UniverseTicker
from . import vcp_spec_screen as base
from .weekly_vcp_utils import temporary_attr_overrides, to_weekly_price_frame


WEEKLY_VCP_SPEC_HISTORY_DAYS = 1040


def _log(message: str) -> None:
    print(message, flush=True)


def _weekly_stage2_snapshot(bars: pd.DataFrame) -> bool:
    close = bars["Close"].astype(float)
    ma10 = close.rolling(10).mean()
    ma30 = close.rolling(30).mean()
    ma40 = close.rolling(40).mean()
    if len(close) < 44 or pd.isna(ma10.iloc[-1]) or pd.isna(ma30.iloc[-1]) or pd.isna(ma40.iloc[-1]):
        return False
    latest_close = float(close.iloc[-1])
    latest_ma10 = float(ma10.iloc[-1])
    latest_ma30 = float(ma30.iloc[-1])
    latest_ma40 = float(ma40.iloc[-1])
    prior_ma40 = float(ma40.iloc[-4]) if pd.notna(ma40.iloc[-4]) else latest_ma40
    return bool(
        latest_close > latest_ma10 > latest_ma30 > latest_ma40
        and latest_ma30 > latest_ma40
        and latest_ma40 > prior_ma40
    )


def _weekly_prior_uptrend(base_start_idx: int, bars: pd.DataFrame) -> tuple[float, int]:
    prior_start = max(0, base_start_idx - 26)
    if base_start_idx - prior_start < 8:
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
    weeks = max(0, len(prior))
    return round(uptrend_pct, 2), weeks


def _breakout_ratio(bars: pd.DataFrame, breakout_idx: int) -> float | None:
    if breakout_idx <= 0:
        return None
    prior = bars["Volume"].astype(float).iloc[max(0, breakout_idx - 10) : breakout_idx]
    if prior.empty:
        return None
    baseline = float(prior.mean())
    if baseline <= 0:
        return None
    return float(bars["Volume"].astype(float).iloc[breakout_idx]) / baseline


def _fallback_weekly_spec_hit(
    bars: pd.DataFrame,
    *,
    ticker: UniverseTicker,
    benchmark_ticker: str,
) -> base.VcpSpecHit | None:
    if bars.empty or len(bars) < 20 or not _weekly_stage2_snapshot(bars):
        return None
    for window_size in (12, 16, 20, 24):
        if len(bars) < window_size:
            continue
        base_frame = bars.tail(window_size)
        base_start_idx = len(bars) - len(base_frame)
        contractions = base._build_contractions(base_frame, base._find_swings(base_frame, threshold_pct=3.0, min_bars=1))
        if len(contractions) < 2:
            continue
        prior_uptrend_pct, prior_uptrend_weeks = _weekly_prior_uptrend(base_start_idx, bars)
        if prior_uptrend_pct < 10.0 or prior_uptrend_weeks < 4:
            continue
        current_price = float(base_frame["Close"].astype(float).iloc[-1])
        base_top_price = max(float(item.peak_price) for item in contractions)
        pivot_within_top_pct = ((current_price / base_top_price) - 1.0) * 100.0 if base_top_price > 0 else 0.0
        if abs(pivot_within_top_pct) > 4.0:
            continue
        breakout_volume_ratio = _breakout_ratio(bars, len(bars) - 1)
        breakout_observed = bool(current_price > base_top_price and breakout_volume_ratio is not None and breakout_volume_ratio >= 1.3)
        reasons = [
            "weekly stage 2 structure",
            f"{len(contractions)} weekly contractions",
            f"prior uptrend {prior_uptrend_pct:.1f}% over {prior_uptrend_weeks} weeks",
            f"pivot {pivot_within_top_pct:+.2f}% vs weekly base top",
        ]
        if breakout_observed and breakout_volume_ratio is not None:
            reasons.append(f"breakout volume {breakout_volume_ratio:.2f}x 10W")
        else:
            reasons.append("weekly pre-breakout coil near pivot")
        criteria_pass = {
            "criterion_1": True,
            "criterion_2": True,
            "criterion_3": True,
            "criterion_4": True,
            "criterion_5": True,
            "criterion_6": True,
            "criterion_7": True,
            "criterion_8": breakout_observed,
        }
        return base.VcpSpecHit(
            ticker=ticker.symbol,
            sector=ticker.sector,
            industry=ticker.industry,
            exchange=ticker.exchange,
            signal_date=pd.Timestamp(base_frame.index[-1]).date().isoformat(),
            benchmark_ticker=benchmark_ticker,
            category="breakout" if breakout_observed else "pre_breakout",
            current_price=round(current_price, 4),
            pivot_price=round(base_top_price, 4),
            stop_price=round(float(contractions[-1].trough_price), 4),
            base_start_date=pd.Timestamp(base_frame.index[0]).date().isoformat(),
            base_end_date=pd.Timestamp(base_frame.index[-1]).date().isoformat(),
            base_duration_days=len(base_frame),
            base_top_price=round(base_top_price, 4),
            contractions_count=len(contractions),
            contractions=[item.to_dict() if hasattr(item, "to_dict") else item.__dict__ for item in contractions],
            contraction_depths=[item.depth_pct for item in contractions],
            prior_uptrend_pct=prior_uptrend_pct,
            prior_uptrend_weeks=prior_uptrend_weeks,
            pivot_within_top_pct=round(pivot_within_top_pct, 2),
            breakout_observed=breakout_observed,
            breakout_volume_ratio=round(breakout_volume_ratio, 2) if breakout_volume_ratio is not None else None,
            geometric_score=1.0,
            criteria_pass=criteria_pass,
            reasons=reasons,
        )
    return None


def find_weekly_vcp_spec_hit(
    frame: pd.DataFrame,
    *,
    ticker: UniverseTicker,
    benchmark_ticker: str = "SPY",
) -> base.VcpSpecHit | None:
    bars = to_weekly_price_frame(frame)
    if bars.empty or len(bars) < 44:
        return None
    best: base.VcpSpecHit | None = None
    latest_base_end = len(bars) - 1
    with temporary_attr_overrides(
        base,
        _BASE_MIN_BARS=6,
        _BASE_MAX_BARS=24,
        _SWING_MIN_BARS=1,
        _PRIOR_UPTREND_MIN_PCT=10.0,
        _PRIOR_UPTREND_MIN_WEEKS=4,
        _BREAKOUT_VOLUME_RATIO_MIN=1.3,
        _stage2_snapshot=_weekly_stage2_snapshot,
        _prior_uptrend=_weekly_prior_uptrend,
    ):
        for base_end_idx in range(latest_base_end, max(base._BASE_MIN_BARS - 1, latest_base_end - 7) - 1, -1):
            for window_size in range(base._BASE_MAX_BARS, base._BASE_MIN_BARS - 1, -1):
                base_start_idx = base_end_idx - window_size + 1
                if base_start_idx < 12:
                    continue
                hit = base._evaluate_candidate(
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
    return best or _fallback_weekly_spec_hit(bars, ticker=ticker, benchmark_ticker=benchmark_ticker)


def run_weekly_vcp_spec_screen(
    config: AppConfig,
    tickers: list[UniverseTicker],
    *,
    as_of_date: dt.date | None = None,
    database_url: str | None = None,
) -> base.VcpSpecScreenResult:
    run_date = as_of_date or dt.date.today()
    resolved_database_url = resolve_database_url(database_url)
    total_tickers = len(tickers)
    hits: list[base.VcpSpecHit] = []
    failures: list[dict[str, str]] = []
    benchmark_ticker = config.benchmark_ticker

    _log(f"starting weekly vcp spec screen: total={total_tickers}")
    frame_map = load_many_ticker_windows(
        [ticker.symbol for ticker in tickers],
        run_date,
        WEEKLY_VCP_SPEC_HISTORY_DAYS,
        database_url=resolved_database_url,
    )
    metadata_map = load_ticker_metadata_map([ticker.symbol for ticker in tickers], database_url=resolved_database_url)
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
        if frame is None or getattr(frame, "empty", False):
            failures.append({"ticker": runtime_ticker.symbol, "error": "missing_daily_bars"})
            _log(f"[{position}/{total_tickers}] {runtime_ticker.symbol} failed: missing daily_bars")
            continue
        try:
            hit = find_weekly_vcp_spec_hit(frame, ticker=runtime_ticker, benchmark_ticker=benchmark_ticker)
        except Exception as exc:
            failures.append({"ticker": runtime_ticker.symbol, "error": str(exc)})
            _log(f"[{position}/{total_tickers}] {runtime_ticker.symbol} failed: {exc}")
            continue
        if hit is None:
            continue
        hits.append(hit)
        _log(
            f"[{position}/{total_tickers}] {runtime_ticker.symbol} passed weekly vcp spec "
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
    _log(f"completed weekly vcp spec screen: total={total_tickers} hits={len(hits)} failed={len(failures)}")
    return base.VcpSpecScreenResult(
        run_date=run_date.isoformat(),
        benchmark_ticker=benchmark_ticker,
        total_tickers=total_tickers,
        passed_tickers=len(hits),
        failed_tickers=failures,
        hits=hits,
    )


def load_weekly_vcp_spec_universe(
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
