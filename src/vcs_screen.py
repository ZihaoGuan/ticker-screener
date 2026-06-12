from __future__ import annotations

from dataclasses import asdict, dataclass
import datetime as dt
from typing import Literal

import pandas as pd

from .config import AppConfig
from .cookstock_bridge import freeze_cookstock_today, iter_prefetched_cookstock_batches, load_configured_cookstock
from .universe import UniverseTicker
from .vcs_indicator import latest_vcs_snapshot


VcsProfile = Literal["setup_stage", "critical_tightness"]
VCS_HISTORY_DAYS = 120


@dataclass(frozen=True)
class VcsHit:
    ticker: str
    sector: str | None
    industry: str | None
    exchange: str | None
    signal_date: str
    signal_profile: VcsProfile
    stage: str
    stage_label: str
    color_zone: str
    current_price: float
    high_price: float
    low_price: float
    vcs_score: float
    tr_short: float
    tr_long_avg: float
    std_short: float
    std_long_avg: float
    vol_short_avg: float
    vol_avg: float
    trend_factor: float
    efficiency: float
    days_tight: int
    is_higher_low: bool
    reasons: list[str]

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True)
class VcsScreenResult:
    run_date: str
    signal_profile: VcsProfile
    total_tickers: int
    passed_tickers: int
    failed_tickers: list[dict[str, str]]
    hits: list[VcsHit]

    def to_dict(self) -> dict[str, object]:
        return {
            "run_date": self.run_date,
            "signal_profile": self.signal_profile,
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


def find_recent_vcs_hit(
    frame: pd.DataFrame,
    *,
    ticker: UniverseTicker,
    signal_profile: VcsProfile,
) -> VcsHit | None:
    bars = _normalize_bars_frame(frame)
    if bars.empty:
        return None
    snapshot = latest_vcs_snapshot(bars)
    if snapshot is None:
        return None

    score = float(snapshot.score)
    if signal_profile == "critical_tightness":
        if score < 80.0:
            return None
    elif signal_profile == "setup_stage":
        if score < 60.0 or score >= 80.0:
            return None
    else:
        return None

    latest = bars.iloc[-1]
    reasons = [
        f"VCS {score:.1f}",
        f"stage {snapshot.stage_label}",
        f"trend factor {snapshot.trend_factor:.2f}, efficiency {snapshot.efficiency:.2f}",
        f"days tight {snapshot.days_tight}, higher low {'yes' if snapshot.is_higher_low else 'no'}",
    ]

    return VcsHit(
        ticker=ticker.symbol,
        sector=ticker.sector,
        industry=ticker.industry,
        exchange=ticker.exchange,
        signal_date=bars.index[-1].date().isoformat(),
        signal_profile=signal_profile,
        stage=snapshot.stage,
        stage_label=snapshot.stage_label,
        color_zone=snapshot.color_zone,
        current_price=float(latest["Close"]),
        high_price=float(latest["High"]),
        low_price=float(latest["Low"]),
        vcs_score=score,
        tr_short=snapshot.tr_short,
        tr_long_avg=snapshot.tr_long_avg,
        std_short=snapshot.std_short,
        std_long_avg=snapshot.std_long_avg,
        vol_short_avg=snapshot.vol_short_avg,
        vol_avg=snapshot.vol_avg,
        trend_factor=snapshot.trend_factor,
        efficiency=snapshot.efficiency,
        days_tight=snapshot.days_tight,
        is_higher_low=snapshot.is_higher_low,
        reasons=reasons,
    )


def run_vcs_screen(
    config: AppConfig,
    tickers: list[UniverseTicker],
    *,
    signal_profile: VcsProfile,
    as_of_date: dt.date | None = None,
) -> VcsScreenResult:
    cookstock = load_configured_cookstock(config)
    hits: list[VcsHit] = []
    failures: list[dict[str, str]] = []
    total_tickers = len(tickers)
    run_date = as_of_date or dt.date.today()

    print(f"starting vcs {signal_profile} screen: total={total_tickers}")

    with freeze_cookstock_today(cookstock, as_of_date):
        position = 0
        for ticker_batch in iter_prefetched_cookstock_batches(
            config,
            tickers,
            as_of_date=as_of_date,
            history_lookback_days=VCS_HISTORY_DAYS,
            benchmark_ticker=config.benchmark_ticker,
        ):
            for ticker in ticker_batch:
                position += 1
                print(f"[{position}/{total_tickers}] screening {ticker.symbol} | passed={len(hits)}")
                try:
                    financials = cookstock.cookFinancials(
                        ticker.symbol,
                        benchmarkTicker=config.benchmark_ticker,
                        historyLookbackDays=VCS_HISTORY_DAYS,
                    )
                    frame = _build_price_frame(financials)
                    hit = find_recent_vcs_hit(frame, ticker=ticker, signal_profile=signal_profile)
                    if hit is None:
                        print(f"[{position}/{total_tickers}] {ticker.symbol} filtered: no vcs {signal_profile} | passed={len(hits)}")
                        continue
                    hits.append(hit)
                    print(f"[{position}/{total_tickers}] {ticker.symbol} passed vcs {signal_profile} {hit.vcs_score:.1f} | passed={len(hits)}")
                except Exception as exc:
                    failures.append({"ticker": ticker.symbol, "error": str(exc)})
                    print(f"[{position}/{total_tickers}] {ticker.symbol} failed: {exc}")

    return VcsScreenResult(
        run_date=run_date.isoformat(),
        signal_profile=signal_profile,
        total_tickers=total_tickers,
        passed_tickers=len(hits),
        failed_tickers=failures,
        hits=hits,
    )
