from __future__ import annotations

from dataclasses import asdict, dataclass
import datetime as dt

import pandas as pd

from .config import AppConfig
from .cookstock_bridge import freeze_cookstock_today, iter_prefetched_cookstock_batches, load_configured_cookstock
from .universe import UniverseTicker


THREE_WEEKS_TIGHT_HISTORY_DAYS = 80
THREE_WEEKS_TIGHT_THRESHOLD_PCT = 1.0
THREE_WEEKS_TIGHT_BUY_OFFSET = 0.1


@dataclass(frozen=True)
class ThreeWeeksTightHit:
    ticker: str
    sector: str | None
    industry: str | None
    exchange: str | None
    signal_date: str
    first_week_date: str
    second_week_date: str
    third_week_date: str
    current_price: float
    range_high: float
    buy_price: float
    close_change_1_pct: float
    close_change_2_pct: float
    threshold_pct: float
    reasons: list[str]

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True)
class ThreeWeeksTightScreenResult:
    run_date: str
    total_tickers: int
    passed_tickers: int
    failed_tickers: list[dict[str, str]]
    hits: list[ThreeWeeksTightHit]

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


def _to_weekly_frame(frame: pd.DataFrame) -> pd.DataFrame:
    weekly = frame.resample("W-FRI").agg(
        {
            "Open": "first",
            "High": "max",
            "Low": "min",
            "Close": "last",
            "Volume": "sum",
        }
    )
    return weekly.dropna(subset=["Open", "High", "Low", "Close"])


def find_three_weeks_tight_hit(
    frame: pd.DataFrame,
    *,
    ticker: UniverseTicker,
    threshold_pct: float = THREE_WEEKS_TIGHT_THRESHOLD_PCT,
) -> ThreeWeeksTightHit | None:
    daily = _normalize_bars_frame(frame)
    if daily.empty:
        return None
    weekly = _to_weekly_frame(daily)
    if len(weekly) < 3:
        return None

    window = weekly.iloc[-3:]
    closes = window["Close"].astype(float)
    highs = window["High"].astype(float)
    if (closes <= 0).any():
        return None

    close_change_1_pct = ((closes.iloc[-1] - closes.iloc[-2]) / closes.iloc[-2]) * 100.0
    close_change_2_pct = ((closes.iloc[-2] - closes.iloc[-3]) / closes.iloc[-3]) * 100.0
    if abs(close_change_1_pct) > threshold_pct or abs(close_change_2_pct) > threshold_pct:
        return None

    range_high = float(highs.max())
    buy_price = range_high + THREE_WEEKS_TIGHT_BUY_OFFSET
    reasons = [
        "3 weeks tight active now",
        f"close change week 3 vs week 2 {close_change_1_pct:.2f}%",
        f"close change week 2 vs week 1 {close_change_2_pct:.2f}%",
        f"range high {range_high:.2f}, buy trigger {buy_price:.2f}",
    ]

    return ThreeWeeksTightHit(
        ticker=ticker.symbol,
        sector=ticker.sector,
        industry=ticker.industry,
        exchange=ticker.exchange,
        signal_date=window.index[-1].date().isoformat(),
        first_week_date=window.index[0].date().isoformat(),
        second_week_date=window.index[1].date().isoformat(),
        third_week_date=window.index[2].date().isoformat(),
        current_price=float(closes.iloc[-1]),
        range_high=range_high,
        buy_price=buy_price,
        close_change_1_pct=close_change_1_pct,
        close_change_2_pct=close_change_2_pct,
        threshold_pct=float(threshold_pct),
        reasons=reasons,
    )


def run_three_weeks_tight_screen(
    config: AppConfig,
    tickers: list[UniverseTicker],
    *,
    as_of_date: dt.date | None = None,
) -> ThreeWeeksTightScreenResult:
    cookstock = load_configured_cookstock(config)
    hits: list[ThreeWeeksTightHit] = []
    failures: list[dict[str, str]] = []
    total_tickers = len(tickers)
    run_date = as_of_date or dt.date.today()

    print(
        "starting three weeks tight screen: "
        f"total={total_tickers}, threshold_pct={THREE_WEEKS_TIGHT_THRESHOLD_PCT:.2f}"
    )

    with freeze_cookstock_today(cookstock, as_of_date):
        position = 0
        for ticker_batch in iter_prefetched_cookstock_batches(
            config,
            cookstock,
            tickers,
            as_of_date=as_of_date,
            history_days=THREE_WEEKS_TIGHT_HISTORY_DAYS,
            require_history=False,
        ):
            for ticker, financials in ticker_batch:
                position += 1
                print(f"[{position}/{total_tickers}] screening {ticker.symbol}")
                try:
                    hit = find_three_weeks_tight_hit(financials._daily_df, ticker=ticker)
                except Exception as exc:
                    failures.append({"ticker": ticker.symbol, "error": str(exc)})
                    continue
                if hit is not None:
                    hits.append(hit)
                print(f"[{position}/{total_tickers}] screening {ticker.symbol} | passed={len(hits)}")

    return ThreeWeeksTightScreenResult(
        run_date=run_date.isoformat(),
        total_tickers=total_tickers,
        passed_tickers=len(hits),
        failed_tickers=failures,
        hits=hits,
    )
