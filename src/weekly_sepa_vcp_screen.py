from __future__ import annotations

import datetime as dt

import pandas as pd

from .config import AppConfig
from .market_data_access import load_many_ticker_windows, load_ticker_metadata_map, resolve_database_url
from .universe import UniverseTicker
from . import sepa_vcp_screen as base
from .weekly_vcp_utils import temporary_attr_overrides, to_weekly_price_frame


WEEKLY_SEPA_VCP_HISTORY_DAYS = 1040


def _log(message: str) -> None:
    print(message, flush=True)


def find_weekly_sepa_vcp_hit(
    frame: pd.DataFrame,
    benchmark_frame: pd.DataFrame,
    *,
    ticker: UniverseTicker,
    benchmark_ticker: str,
) -> base.SepaVcpHit | None:
    weekly_frame = to_weekly_price_frame(frame)
    weekly_benchmark = to_weekly_price_frame(benchmark_frame)
    with temporary_attr_overrides(
        base,
        SEPA_MA50_LENGTH=10,
        SEPA_MA150_LENGTH=30,
        SEPA_MA200_LENGTH=40,
        SEPA_52W_LOOKBACK=52,
        SEPA_MA200_SLOPE_LOOKBACK=4,
        SEPA_PRESSURE_LOOKBACK=8,
        SEPA_RPR_3M_LENGTH=13,
        SEPA_RPR_6M_LENGTH=26,
        SEPA_RPR_9M_LENGTH=39,
        SEPA_RPR_12M_LENGTH=52,
        SEPA_VCP_LOOKBACK=8,
        SEPA_SIGNAL_LOOKBACK_BARS=8,
    ):
        return base.find_recent_sepa_vcp_hit(
            weekly_frame,
            weekly_benchmark,
            ticker=ticker,
            benchmark_ticker=benchmark_ticker,
            recent_signal_lookback_bars=base.SEPA_SIGNAL_LOOKBACK_BARS,
        )


def run_weekly_sepa_vcp_screen(
    config: AppConfig,
    tickers: list[UniverseTicker],
    *,
    as_of_date: dt.date | None = None,
    database_url: str | None = None,
) -> base.SepaVcpScreenResult:
    run_date = as_of_date or dt.date.today()
    resolved_database_url = resolve_database_url(database_url)
    benchmark_ticker = config.benchmark_ticker.upper()
    symbols = [ticker.symbol for ticker in tickers]
    frame_map = load_many_ticker_windows(
        symbols + [benchmark_ticker],
        run_date,
        WEEKLY_SEPA_VCP_HISTORY_DAYS,
        database_url=resolved_database_url,
    )
    metadata_map = load_ticker_metadata_map(symbols, database_url=resolved_database_url)
    benchmark_frame = frame_map.get(benchmark_ticker)
    total_tickers = len(tickers)
    hits: list[base.SepaVcpHit] = []
    failures: list[dict[str, str]] = []

    _log(f"starting weekly SEPA screen: total={total_tickers}")
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
            hit = find_weekly_sepa_vcp_hit(
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
            f"[{position}/{total_tickers}] {runtime_ticker.symbol} passed weekly SEPA "
            f"{hit.signal_date} RPR {hit.rpr_score:.1f} {hit.buy_risk_status} | passed={len(hits)}"
        )

    _log(f"finished weekly SEPA screen: passed={len(hits)}, failed={len(failures)}, total={total_tickers}")
    return base.SepaVcpScreenResult(
        run_date=run_date.isoformat(),
        benchmark_ticker=benchmark_ticker,
        total_tickers=total_tickers,
        passed_tickers=len(hits),
        failed_tickers=failures,
        hits=hits,
    )
