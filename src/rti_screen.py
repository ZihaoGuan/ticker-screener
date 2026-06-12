from __future__ import annotations

from dataclasses import asdict, dataclass
import datetime as dt

import numpy as np
import pandas as pd

from .config import AppConfig
from .cookstock_bridge import freeze_cookstock_today, iter_prefetched_cookstock_batches, load_configured_cookstock
from .universe import UniverseTicker


RTI_LOOKBACK_LENGTH = 5
RTI_BELOW_20_LEVEL = 20.0
RTI_RANGE_EXPANSION_MULTIPLE = 2.0
RTI_HISTORY_DAYS = 60


@dataclass(frozen=True)
class RtiHit:
    ticker: str
    sector: str | None
    industry: str | None
    exchange: str | None
    signal_date: str
    signal_kind: str
    current_price: float
    high_price: float
    low_price: float
    current_volatility: float
    min_volatility: float
    max_volatility: float
    rti_value: float
    previous_rti_value: float
    consecutive_below_20_count: int
    below_20: bool
    dot_condition: bool
    range_expansion_condition: bool
    reasons: list[str]

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True)
class RtiScreenResult:
    run_date: str
    total_tickers: int
    passed_tickers: int
    failed_tickers: list[dict[str, str]]
    hits: list[RtiHit]

    def to_dict(self) -> dict[str, object]:
        return {
            "run_date": self.run_date,
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


def _count_trailing_true(series: pd.Series) -> int:
    count = 0
    for value in reversed(series.tolist()):
        if bool(value):
            count += 1
            continue
        break
    return count


def find_recent_rti_hit(
    frame: pd.DataFrame,
    *,
    ticker: UniverseTicker,
    length: int = RTI_LOOKBACK_LENGTH,
) -> RtiHit | None:
    bars = _normalize_bars_frame(frame)
    if bars.empty or len(bars) < max(3, length):
        return None

    volatility = bars["High"].astype(float) - bars["Low"].astype(float)
    max_volatility = volatility.rolling(length).max()
    min_volatility = volatility.rolling(length).min()
    volatility_span = max_volatility - min_volatility
    rti = pd.Series(
        np.where(volatility_span > 0, 100.0 * (volatility - min_volatility) / volatility_span, np.nan),
        index=bars.index,
    )
    rti_prev = rti.shift(1)
    below_20 = rti < RTI_BELOW_20_LEVEL
    consecutive_below_20_count = _count_trailing_true(below_20.fillna(False))
    dot_condition = bool(consecutive_below_20_count >= 2 and below_20.iloc[-1])
    range_expansion_condition = bool(
        pd.notna(rti.iloc[-1])
        and pd.notna(rti_prev.iloc[-1])
        and float(rti_prev.iloc[-1]) > 0.0
        and float(rti_prev.iloc[-1]) <= RTI_BELOW_20_LEVEL
        and float(rti.iloc[-1]) >= RTI_RANGE_EXPANSION_MULTIPLE * float(rti_prev.iloc[-1])
    )
    below_20_condition = bool(below_20.iloc[-1]) if pd.notna(below_20.iloc[-1]) else False

    signal_kind = ""
    if range_expansion_condition:
        signal_kind = "range_expansion"
    elif dot_condition:
        signal_kind = "orange_dot"
    elif below_20_condition:
        signal_kind = "below_20"
    if not signal_kind:
        return None

    latest = bars.iloc[-1]
    current_rti = float(rti.iloc[-1])
    previous_rti = float(rti_prev.iloc[-1]) if pd.notna(rti_prev.iloc[-1]) else 0.0
    current_vol = float(volatility.iloc[-1])
    max_vol = float(max_volatility.iloc[-1]) if pd.notna(max_volatility.iloc[-1]) else current_vol
    min_vol = float(min_volatility.iloc[-1]) if pd.notna(min_volatility.iloc[-1]) else current_vol

    reasons = [
        f"RTI {current_rti:.1f} with {length}-bar lookback",
        f"current range {current_vol:.2f}, min {min_vol:.2f}, max {max_vol:.2f}",
    ]
    if signal_kind == "range_expansion":
        reasons.append(f"RTI expanded from {previous_rti:.1f} to {current_rti:.1f} after sub-20 reset")
    elif signal_kind == "orange_dot":
        reasons.append(f"RTI stayed below 20 for {consecutive_below_20_count} straight bars")
    else:
        reasons.append("RTI is below 20 on latest bar")

    return RtiHit(
        ticker=ticker.symbol,
        sector=ticker.sector,
        industry=ticker.industry,
        exchange=ticker.exchange,
        signal_date=bars.index[-1].date().isoformat(),
        signal_kind=signal_kind,
        current_price=float(latest["Close"]),
        high_price=float(latest["High"]),
        low_price=float(latest["Low"]),
        current_volatility=current_vol,
        min_volatility=min_vol,
        max_volatility=max_vol,
        rti_value=current_rti,
        previous_rti_value=previous_rti,
        consecutive_below_20_count=consecutive_below_20_count,
        below_20=below_20_condition,
        dot_condition=dot_condition,
        range_expansion_condition=range_expansion_condition,
        reasons=reasons,
    )


def run_rti_screen(
    config: AppConfig,
    tickers: list[UniverseTicker],
    *,
    as_of_date: dt.date | None = None,
) -> RtiScreenResult:
    cookstock = load_configured_cookstock(config)
    hits: list[RtiHit] = []
    failures: list[dict[str, str]] = []
    total_tickers = len(tickers)
    run_date = as_of_date or dt.date.today()

    print(
        "starting RTI screen: "
        f"total={total_tickers}, lookback={RTI_LOOKBACK_LENGTH}, below_20={RTI_BELOW_20_LEVEL:.0f}"
    )

    with freeze_cookstock_today(cookstock, as_of_date):
        position = 0
        for ticker_batch in iter_prefetched_cookstock_batches(
            config,
            tickers,
            as_of_date=as_of_date,
            history_lookback_days=RTI_HISTORY_DAYS,
            benchmark_ticker=config.benchmark_ticker,
        ):
            for ticker in ticker_batch:
                position += 1
                print(f"[{position}/{total_tickers}] screening {ticker.symbol} | passed={len(hits)}")
                try:
                    financials = cookstock.cookFinancials(
                        ticker.symbol,
                        benchmarkTicker=config.benchmark_ticker,
                        historyLookbackDays=RTI_HISTORY_DAYS,
                    )
                    frame = _build_price_frame(financials)
                    hit = find_recent_rti_hit(frame, ticker=ticker)
                    if hit is None:
                        print(f"[{position}/{total_tickers}] {ticker.symbol} filtered: no RTI signal | passed={len(hits)}")
                        continue
                    hits.append(hit)
                    print(
                        f"[{position}/{total_tickers}] {ticker.symbol} passed RTI {hit.signal_kind} "
                        f"at {hit.rti_value:.1f} | passed={len(hits)}"
                    )
                except Exception as exc:
                    failures.append({"ticker": ticker.symbol, "error": str(exc)})
                    print(f"[{position}/{total_tickers}] {ticker.symbol} failed: {exc}")

    print(f"finished RTI screen: passed={len(hits)}, failed={len(failures)}, total={total_tickers}")
    return RtiScreenResult(
        run_date=run_date.isoformat(),
        total_tickers=total_tickers,
        passed_tickers=len(hits),
        failed_tickers=failures,
        hits=hits,
    )
