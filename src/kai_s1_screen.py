from __future__ import annotations

from dataclasses import asdict, dataclass
import datetime as dt

import pandas as pd

from .config import AppConfig
from .cookstock_bridge import freeze_cookstock_today, load_configured_cookstock
from .kai_s2_screen import _build_price_frame, _normalize_price_frame
from .market_data_access import (
    db_frame_has_recent_coverage,
    load_active_universe_from_db,
    load_latest_market_caps,
    load_many_ticker_windows,
    load_ticker_metadata_map,
    resolve_database_url,
)
from .universe import UniverseTicker


KAI_S1_STRATEGY_ID = "kai_s1"
KAI_S1_HISTORY_DAYS = 320
DAILY_SMA_PERIOD = 50
WEEKLY_SMA_FAST_PERIOD = 30
WEEKLY_SMA_SLOW_PERIOD = 40
ADR_PERIOD_DAYS = 20
MIN_PRICE = 5.0
MIN_MARKET_CAP = 10_000_000_000.0
MIN_DOLLAR_VOLUME = 100_000_000.0
MIN_ADR_PCT = 2.0


@dataclass(frozen=True)
class KaiS1Snapshot:
    matched: bool
    current_price: float
    market_cap: float | None
    sma50: float
    weekly_sma30: float
    weekly_sma40: float
    dollar_volume: float
    adr_pct_20: float
    criteria_passed: int
    criteria_total: int
    criteria: dict[str, bool]
    reasons: list[str]


@dataclass(frozen=True)
class KaiS1Hit:
    ticker: str
    sector: str | None
    industry: str | None
    exchange: str | None
    signal_date: str
    current_price: float
    market_cap: float | None
    sma50: float
    weekly_sma30: float
    weekly_sma40: float
    dollar_volume: float
    adr_pct_20: float
    criteria_passed: int
    criteria_total: int
    reasons: list[str]

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True)
class KaiS1ScreenResult:
    run_date: str
    total_tickers: int
    passed_tickers: int
    failed_tickers: list[dict[str, str]]
    hits: list[KaiS1Hit]

    def to_dict(self) -> dict[str, object]:
        return {
            "run_date": self.run_date,
            "total_tickers": self.total_tickers,
            "passed_tickers": self.passed_tickers,
            "failed_tickers": self.failed_tickers,
            "hits": [item.to_dict() for item in self.hits],
        }


def evaluate_kai_s1(frame: pd.DataFrame, *, market_cap: float | None) -> KaiS1Snapshot | None:
    bars = _normalize_price_frame(frame)
    if bars.empty or len(bars) < 200:
        return None

    close = bars["Close"].astype(float)
    high = bars["High"].astype(float)
    low = bars["Low"].astype(float)
    volume = bars["Volume"].astype(float)
    weekly_close = close.resample("W-FRI").last().dropna()
    if len(weekly_close) < WEEKLY_SMA_SLOW_PERIOD:
        return None

    current_price = float(close.iloc[-1])
    sma50 = float(close.rolling(DAILY_SMA_PERIOD).mean().iloc[-1])
    weekly_sma30 = float(weekly_close.rolling(WEEKLY_SMA_FAST_PERIOD).mean().iloc[-1])
    weekly_sma40 = float(weekly_close.rolling(WEEKLY_SMA_SLOW_PERIOD).mean().iloc[-1])
    dollar_volume = current_price * float(volume.iloc[-1])
    adr_pct_20 = float((((high - low) / close) * 100.0).tail(ADR_PERIOD_DAYS).mean())
    if any(pd.isna(value) for value in (sma50, weekly_sma30, weekly_sma40, adr_pct_20)):
        return None

    criteria = {
        "price_gt_weekly_sma30": current_price > weekly_sma30,
        "price_gt_weekly_sma40": current_price > weekly_sma40,
        "weekly_sma30_gt_weekly_sma40": weekly_sma30 > weekly_sma40,
        "price_gt_5": current_price > MIN_PRICE,
        "market_cap_gt_10b": market_cap is not None and market_cap > MIN_MARKET_CAP,
        "price_gt_sma50": current_price > sma50,
        "price_times_volume_gt_100m": dollar_volume > MIN_DOLLAR_VOLUME,
        "adr20_gt_2pct": adr_pct_20 > MIN_ADR_PCT,
    }
    return KaiS1Snapshot(
        matched=all(criteria.values()),
        current_price=current_price,
        market_cap=market_cap,
        sma50=sma50,
        weekly_sma30=weekly_sma30,
        weekly_sma40=weekly_sma40,
        dollar_volume=dollar_volume,
        adr_pct_20=adr_pct_20,
        criteria_passed=sum(criteria.values()),
        criteria_total=len(criteria),
        criteria=criteria,
        reasons=[
            f"close {current_price:.2f} vs weekly SMA30 {weekly_sma30:.2f}, weekly SMA40 {weekly_sma40:.2f}, and daily SMA50 {sma50:.2f}",
            f"market cap {(market_cap or 0.0) / 1_000_000_000:.2f}B vs minimum 10.00B",
            f"close x session volume {dollar_volume:,.0f} vs minimum {MIN_DOLLAR_VOLUME:,.0f}",
            f"ADR20 {adr_pct_20:.2f}% vs minimum {MIN_ADR_PCT:.2f}%",
        ],
    )


def find_kai_s1_hit(frame: pd.DataFrame, *, ticker: UniverseTicker, market_cap: float | None, signal_date: dt.date) -> KaiS1Hit | None:
    snapshot = evaluate_kai_s1(frame, market_cap=market_cap)
    if snapshot is None or not snapshot.matched:
        return None
    return KaiS1Hit(
        ticker=ticker.symbol,
        sector=ticker.sector,
        industry=ticker.industry,
        exchange=ticker.exchange,
        signal_date=signal_date.isoformat(),
        current_price=snapshot.current_price,
        market_cap=snapshot.market_cap,
        sma50=snapshot.sma50,
        weekly_sma30=snapshot.weekly_sma30,
        weekly_sma40=snapshot.weekly_sma40,
        dollar_volume=snapshot.dollar_volume,
        adr_pct_20=snapshot.adr_pct_20,
        criteria_passed=snapshot.criteria_passed,
        criteria_total=snapshot.criteria_total,
        reasons=snapshot.reasons,
    )


def run_kai_s1_screen(config: AppConfig, tickers: list[UniverseTicker], *, as_of_date: dt.date | None = None, database_url: str | None = None) -> KaiS1ScreenResult:
    run_date = as_of_date or dt.date.today()
    resolved_database_url = resolve_database_url(database_url)
    symbols = [ticker.symbol for ticker in tickers]
    frame_map = load_many_ticker_windows(symbols, run_date, KAI_S1_HISTORY_DAYS, database_url=resolved_database_url)
    metadata_map = load_ticker_metadata_map(symbols, database_url=resolved_database_url)
    market_caps = load_latest_market_caps(symbols, as_of_date=run_date, database_url=resolved_database_url)
    hits: list[KaiS1Hit] = []
    failures: list[dict[str, str]] = []
    fallback_tickers: list[UniverseTicker] = []

    for ticker in tickers:
        frame = frame_map.get(ticker.symbol.upper())
        if frame is None or not db_frame_has_recent_coverage(frame, run_date) or len(frame) < 200:
            fallback_tickers.append(ticker)
            continue
        metadata = metadata_map.get(ticker.symbol.upper(), {})
        runtime_ticker = UniverseTicker(ticker.symbol.upper(), ticker.sector or metadata.get("sector"), ticker.industry or metadata.get("industry"), ticker.exchange or metadata.get("exchange"))
        try:
            hit = find_kai_s1_hit(frame, ticker=runtime_ticker, market_cap=market_caps.get(runtime_ticker.symbol), signal_date=run_date)
            if hit is not None:
                hits.append(hit)
        except Exception as exc:
            failures.append({"ticker": runtime_ticker.symbol, "error": str(exc)})

    if fallback_tickers:
        cookstock = load_configured_cookstock(config)
        with freeze_cookstock_today(cookstock, run_date):
            for ticker in fallback_tickers:
                try:
                    frame = _build_price_frame(cookstock.cookFinancials(ticker.symbol, benchmarkTicker=config.benchmark_ticker, historyLookbackDays=KAI_S1_HISTORY_DAYS))
                    hit = find_kai_s1_hit(frame, ticker=ticker, market_cap=market_caps.get(ticker.symbol.upper()), signal_date=run_date)
                    if hit is not None:
                        hits.append(hit)
                except Exception as exc:
                    failures.append({"ticker": ticker.symbol, "error": str(exc)})

    hits.sort(key=lambda item: (-item.dollar_volume, -(item.market_cap or 0.0), item.ticker))
    return KaiS1ScreenResult(run_date.isoformat(), len(tickers), len(hits), failures, hits)


def load_kai_s1_universe(*, as_of_date: dt.date | None = None, limit: int | None = None, database_url: str | None = None) -> list[UniverseTicker]:
    return load_active_universe_from_db(as_of_date=as_of_date, limit=limit, database_url=resolve_database_url(database_url))
