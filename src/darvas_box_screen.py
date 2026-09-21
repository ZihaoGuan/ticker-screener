from __future__ import annotations

from dataclasses import asdict, dataclass
import datetime as dt

import pandas as pd

from .config import AppConfig
from .cookstock_bridge import freeze_cookstock_today, load_configured_cookstock
from .market_data_access import db_frame_has_recent_coverage, load_many_ticker_windows, resolve_database_url
from .universe import UniverseTicker


DARVAS_BOX_HISTORY_DAYS = 252
DARVAS_BOX_DAYS = 20
DARVAS_MIN_BOX_DAYS = 15
DARVAS_MAX_BOX_WIDTH_PCT = 15.0
DARVAS_RESISTANCE_TOUCH_TOLERANCE_PCT = 1.0
DARVAS_MIN_RESISTANCE_TOUCHES = 2
DARVAS_BREAKOUT_BUFFER_PCT = 0.5
DARVAS_NEAR_52W_HIGH_PCT = 10.0
DARVAS_VOLUME_AVERAGE_DAYS = 50
DARVAS_MIN_VOLUME = 100_000.0
DARVAS_MIN_VOLUME_RATIO = 1.5


@dataclass(frozen=True)
class DarvasBoxBreakoutHit:
    ticker: str
    sector: str | None
    industry: str | None
    exchange: str | None
    signal_date: str
    current_price: float
    breakout_price: float
    box_high: float
    box_low: float
    box_days: int
    box_width_pct: float
    resistance_touches: int
    high_52w: float
    distance_from_52w_high_pct: float
    current_volume: float
    avg_volume_50: float
    volume_ratio_50: float
    reasons: list[str]

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True)
class DarvasBoxBreakoutScreenResult:
    run_date: str
    total_tickers: int
    passed_tickers: int
    failed_tickers: list[dict[str, str]]
    hits: list[DarvasBoxBreakoutHit]

    def to_dict(self) -> dict[str, object]:
        return {
            "run_date": self.run_date,
            "total_tickers": self.total_tickers,
            "passed_tickers": self.passed_tickers,
            "failed_tickers": self.failed_tickers,
            "hits": [item.to_dict() for item in self.hits],
        }


def _normalize_bars_frame(frame: pd.DataFrame) -> pd.DataFrame:
    required = ("High", "Low", "Close", "Volume")
    available = {str(column).lower(): column for column in frame.columns}
    if any(column.lower() not in available for column in required):
        return pd.DataFrame()
    bars = frame[[available[column.lower()] for column in required]].copy()
    bars.columns = required
    bars = bars.dropna(subset=required).sort_index()
    if not isinstance(bars.index, pd.DatetimeIndex):
        bars.index = pd.to_datetime(bars.index)
    return bars


def _build_price_frame(financials: object) -> pd.DataFrame:
    rows = financials._get_clean_price_data()  # type: ignore[attr-defined]
    if not rows:
        return pd.DataFrame()
    frame = pd.DataFrame(
        {
            "Date": pd.to_datetime([row.get("formatted_date") for row in rows]),
            "High": [row.get("high") for row in rows],
            "Low": [row.get("low") for row in rows],
            "Close": [row.get("close") for row in rows],
            "Volume": [row.get("volume") for row in rows],
        }
    )
    return frame.dropna(subset=["Date", "High", "Low", "Close", "Volume"]).set_index("Date").sort_index()


def find_darvas_box_breakout_hit(
    frame: pd.DataFrame,
    *,
    ticker: UniverseTicker,
) -> DarvasBoxBreakoutHit | None:
    """Find a close through a 20-session Darvas-style consolidation box.

    The box excludes the current breakout candle: it must be no wider than 15%,
    have at least two touches near its upper boundary, sit within 10% of the
    52-week high, and the breakout must close 0.5% above resistance on at least
    1.5x its preceding 50-session average volume.
    """
    bars = _normalize_bars_frame(frame)
    min_history = max(DARVAS_BOX_HISTORY_DAYS, DARVAS_VOLUME_AVERAGE_DAYS) + 1
    if bars.empty or len(bars) < min_history:
        return None

    box = bars.iloc[-(DARVAS_BOX_DAYS + 1) : -1]
    if len(box) < DARVAS_MIN_BOX_DAYS:
        return None
    latest = bars.iloc[-1]
    current_price = float(latest["Close"])
    current_volume = float(latest["Volume"])
    box_high = float(box["High"].max())
    box_low = float(box["Low"].min())
    if box_high <= 0.0 or box_low <= 0.0:
        return None
    box_width_pct = ((box_high - box_low) / box_low) * 100.0
    touch_floor = box_high * (1.0 - DARVAS_RESISTANCE_TOUCH_TOLERANCE_PCT / 100.0)
    resistance_touches = int((box["High"].astype(float) >= touch_floor).sum())
    high_52w = float(bars["High"].tail(DARVAS_BOX_HISTORY_DAYS).max())
    distance_from_52w_high_pct = ((high_52w - current_price) / high_52w) * 100.0 if high_52w > 0.0 else 100.0
    avg_volume_50 = float(bars["Volume"].iloc[-(DARVAS_VOLUME_AVERAGE_DAYS + 1) : -1].mean())
    volume_ratio_50 = current_volume / avg_volume_50 if avg_volume_50 > 0.0 else 0.0
    breakout_price = box_high * (1.0 + DARVAS_BREAKOUT_BUFFER_PCT / 100.0)

    if not (
        box_width_pct <= DARVAS_MAX_BOX_WIDTH_PCT
        and resistance_touches >= DARVAS_MIN_RESISTANCE_TOUCHES
        and current_price > breakout_price
        and current_price >= high_52w * (1.0 - DARVAS_NEAR_52W_HIGH_PCT / 100.0)
        and current_volume >= DARVAS_MIN_VOLUME
        and volume_ratio_50 >= DARVAS_MIN_VOLUME_RATIO
    ):
        return None

    return DarvasBoxBreakoutHit(
        ticker=ticker.symbol,
        sector=ticker.sector,
        industry=ticker.industry,
        exchange=ticker.exchange,
        signal_date=bars.index[-1].date().isoformat(),
        current_price=current_price,
        breakout_price=breakout_price,
        box_high=box_high,
        box_low=box_low,
        box_days=len(box),
        box_width_pct=box_width_pct,
        resistance_touches=resistance_touches,
        high_52w=high_52w,
        distance_from_52w_high_pct=distance_from_52w_high_pct,
        current_volume=current_volume,
        avg_volume_50=avg_volume_50,
        volume_ratio_50=volume_ratio_50,
        reasons=[
            f"{len(box)}-session Darvas box: {box_low:.2f}-{box_high:.2f} ({box_width_pct:.1f}% wide)",
            f"{resistance_touches} upper-boundary touches within {DARVAS_RESISTANCE_TOUCH_TOLERANCE_PCT:.0f}%",
            f"close {current_price:.2f} cleared {breakout_price:.2f} resistance trigger",
            f"volume {current_volume:,.0f} is {volume_ratio_50:.1f}x the prior 50-session average",
            f"close is {distance_from_52w_high_pct:.1f}% below the 52-week high",
        ],
    )


def run_darvas_box_breakout_screen(
    config: AppConfig,
    tickers: list[UniverseTicker],
    *,
    as_of_date: dt.date | None = None,
    database_url: str | None = None,
) -> DarvasBoxBreakoutScreenResult:
    run_date = as_of_date or dt.date.today()
    resolved_database_url = resolve_database_url(database_url)
    symbols = [ticker.symbol.upper() for ticker in tickers]
    frames = load_many_ticker_windows(
        symbols,
        run_date,
        DARVAS_BOX_HISTORY_DAYS + 1,
        database_url=resolved_database_url,
    )
    hits: list[DarvasBoxBreakoutHit] = []
    failures: list[dict[str, str]] = []
    fallback: list[UniverseTicker] = []

    for ticker in tickers:
        frame = frames.get(ticker.symbol.upper())
        if frame is None or not db_frame_has_recent_coverage(frame, run_date):
            fallback.append(ticker)
            continue
        try:
            hit = find_darvas_box_breakout_hit(frame, ticker=ticker)
            if hit is not None:
                hits.append(hit)
        except Exception as exc:
            failures.append({"ticker": ticker.symbol, "error": str(exc)})

    if fallback:
        cookstock = load_configured_cookstock(config)
        with freeze_cookstock_today(cookstock, as_of_date):
            for ticker in fallback:
                try:
                    financials = cookstock.cookFinancials(
                        ticker.symbol,
                        benchmarkTicker=config.benchmark_ticker,
                        historyLookbackDays=DARVAS_BOX_HISTORY_DAYS + 1,
                    )
                    hit = find_darvas_box_breakout_hit(_build_price_frame(financials), ticker=ticker)
                    if hit is not None:
                        hits.append(hit)
                except Exception as exc:
                    failures.append({"ticker": ticker.symbol, "error": str(exc)})

    hits.sort(key=lambda item: (-item.volume_ratio_50, item.box_width_pct, item.ticker))
    return DarvasBoxBreakoutScreenResult(
        run_date=run_date.isoformat(),
        total_tickers=len(tickers),
        passed_tickers=len(hits),
        failed_tickers=failures,
        hits=hits,
    )
