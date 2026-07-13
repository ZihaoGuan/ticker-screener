from __future__ import annotations

from dataclasses import asdict, dataclass
import datetime as dt

import pandas as pd

from .config import AppConfig
from .cookstock_bridge import freeze_cookstock_today, load_configured_cookstock
from .market_data_access import db_frame_has_recent_coverage, load_many_ticker_windows, resolve_database_url
from .universe import UniverseTicker


YEAR_HIGH_LOOKBACK_DAYS = 252
PRICE_HISTORY_DAYS = 280
VOLUME_LOOKBACK_DAYS = 20
MAX_DISTANCE_FROM_52W_HIGH_PCT = 20.0


@dataclass(frozen=True)
class Near52WeekHighHit:
    ticker: str
    sector: str | None
    industry: str | None
    exchange: str | None
    signal_date: str
    current_price: float
    year_high: float
    distance_from_52wk_high_pct: float
    avg_volume_20: float
    avg_dollar_volume_20: float
    reasons: list[str]

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True)
class Near52WeekHighScreenResult:
    run_date: str
    total_tickers: int
    passed_tickers: int
    failed_tickers: list[dict[str, str]]
    hits: list[Near52WeekHighHit]

    def to_dict(self) -> dict[str, object]:
        return {
            "run_date": self.run_date,
            "total_tickers": self.total_tickers,
            "passed_tickers": self.passed_tickers,
            "failed_tickers": self.failed_tickers,
            "hits": [item.to_dict() for item in self.hits],
        }


def _normalize_price_frame(frame: pd.DataFrame) -> pd.DataFrame:
    required = ["High", "Close", "Volume"]
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
            "High": [row.get("high") for row in rows],
            "Close": [row.get("close") for row in rows],
            "Volume": [row.get("volume") for row in rows],
        }
    )
    return frame.dropna(subset=["Date", "High", "Close", "Volume"]).set_index("Date").sort_index()


def evaluate_near_52wk_high_frame(frame: pd.DataFrame, *, ticker: UniverseTicker, signal_date: dt.date) -> Near52WeekHighHit | None:
    bars = _normalize_price_frame(frame)
    if bars.empty or len(bars) < YEAR_HIGH_LOOKBACK_DAYS:
        return None

    high = bars["High"].astype(float)
    close = bars["Close"].astype(float)
    volume = bars["Volume"].astype(float)

    current_price = float(close.iloc[-1])
    year_high = float(high.tail(YEAR_HIGH_LOOKBACK_DAYS).max())
    if current_price <= 0 or year_high <= 0:
        return None

    distance_from_52wk_high_pct = ((year_high / current_price) - 1.0) * 100.0
    if distance_from_52wk_high_pct < 0 or distance_from_52wk_high_pct > MAX_DISTANCE_FROM_52W_HIGH_PCT:
        return None

    avg_volume_20 = float(volume.tail(VOLUME_LOOKBACK_DAYS).mean())
    avg_dollar_volume_20 = float((close.tail(VOLUME_LOOKBACK_DAYS) * volume.tail(VOLUME_LOOKBACK_DAYS)).mean())
    reasons = [
        f"{distance_from_52wk_high_pct:.1f}% below 52-week high {year_high:.2f}",
        f"current close {current_price:.2f}",
        f"20D average dollar volume {avg_dollar_volume_20:,.0f}",
    ]
    return Near52WeekHighHit(
        ticker=ticker.symbol,
        sector=ticker.sector,
        industry=ticker.industry,
        exchange=ticker.exchange,
        signal_date=signal_date.isoformat(),
        current_price=current_price,
        year_high=year_high,
        distance_from_52wk_high_pct=distance_from_52wk_high_pct,
        avg_volume_20=avg_volume_20,
        avg_dollar_volume_20=avg_dollar_volume_20,
        reasons=reasons,
    )


def run_near_52wk_high_screen(
    config: AppConfig,
    tickers: list[UniverseTicker],
    *,
    as_of_date: dt.date | None = None,
) -> Near52WeekHighScreenResult:
    run_date = as_of_date or dt.date.today()
    total_tickers = len(tickers)
    print(
        "starting near-52-week-high screen: "
        f"total={total_tickers}, "
        f"lookback={YEAR_HIGH_LOOKBACK_DAYS}d, "
        f"max_distance={MAX_DISTANCE_FROM_52W_HIGH_PCT:.0f}%"
    )

    database_url = resolve_database_url("")
    frame_map = load_many_ticker_windows(
        [item.symbol for item in tickers],
        run_date,
        PRICE_HISTORY_DAYS,
        database_url=database_url,
    )

    hits: list[Near52WeekHighHit] = []
    failures: list[dict[str, str]] = []
    fallback_tickers: list[tuple[int, UniverseTicker]] = []

    for position, ticker in enumerate(tickers, start=1):
        frame = frame_map.get(ticker.symbol.upper())
        if frame is None or not db_frame_has_recent_coverage(frame, run_date) or len(frame) < YEAR_HIGH_LOOKBACK_DAYS:
            fallback_tickers.append((position, ticker))
            continue
        print(f"[{position}/{total_tickers}] screening {ticker.symbol} from DB | passed={len(hits)}")
        try:
            hit = evaluate_near_52wk_high_frame(frame, ticker=ticker, signal_date=run_date)
            if hit is None:
                print(f"[{position}/{total_tickers}] {ticker.symbol} filtered: not within 0-20% of 52-week high | passed={len(hits)}")
                continue
            hits.append(hit)
            print(
                f"[{position}/{total_tickers}] {ticker.symbol} passed: "
                f"{hit.distance_from_52wk_high_pct:.1f}% below 52-week high | passed={len(hits)}"
            )
        except Exception as exc:
            failures.append({"ticker": ticker.symbol, "error": str(exc)})
            print(f"[{position}/{total_tickers}] {ticker.symbol} error: {exc} | passed={len(hits)}")

    if fallback_tickers:
        cookstock = load_configured_cookstock(config)
        with freeze_cookstock_today(cookstock, as_of_date):
            for position, ticker in fallback_tickers:
                print(f"[{position}/{total_tickers}] screening {ticker.symbol} from internet fallback | passed={len(hits)}")
                try:
                    financials = cookstock.cookFinancials(
                        ticker.symbol,
                        benchmarkTicker=config.benchmark_ticker,
                        historyLookbackDays=PRICE_HISTORY_DAYS,
                    )
                    frame = _build_price_frame(financials)
                    hit = evaluate_near_52wk_high_frame(frame, ticker=ticker, signal_date=run_date)
                    if hit is None:
                        print(f"[{position}/{total_tickers}] {ticker.symbol} filtered: not within 0-20% of 52-week high | passed={len(hits)}")
                        continue
                    hits.append(hit)
                    print(
                        f"[{position}/{total_tickers}] {ticker.symbol} passed: "
                        f"{hit.distance_from_52wk_high_pct:.1f}% below 52-week high | passed={len(hits)}"
                    )
                except Exception as exc:
                    failures.append({"ticker": ticker.symbol, "error": str(exc)})
                    print(f"[{position}/{total_tickers}] {ticker.symbol} error: {exc} | passed={len(hits)}")

    hits.sort(key=lambda item: (item.distance_from_52wk_high_pct, -item.avg_dollar_volume_20, item.ticker))
    return Near52WeekHighScreenResult(
        run_date=run_date.isoformat(),
        total_tickers=total_tickers,
        passed_tickers=len(hits),
        failed_tickers=failures,
        hits=hits,
    )
