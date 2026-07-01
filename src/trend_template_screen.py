from __future__ import annotations

from dataclasses import asdict, dataclass
import datetime as dt

import pandas as pd

from .config import AppConfig
from .cookstock_bridge import freeze_cookstock_today, load_configured_cookstock
from .market_data_access import db_frame_has_recent_coverage, load_many_ticker_windows, resolve_database_url
from .universe import UniverseTicker


MA50_LENGTH = 50
MA150_LENGTH = 150
MA200_LENGTH = 200
HIGH_LOW_LOOKBACK_DAYS = 260
MA200_UPTREND_LOOKBACK_DAYS = 22
PRICE_HISTORY_DAYS = 320
VOLUME_LOOKBACK_DAYS = 20
MIN_DISTANCE_FROM_52W_LOW_PCT = 30.0
MAX_DISTANCE_FROM_52W_HIGH_PCT = 25.0
MIN_RS_RATING = 70.0
RS_LOOKBACK_3M_DAYS = 13
RS_LOOKBACK_6M_DAYS = 26
RS_LOOKBACK_9M_DAYS = 39
RS_LOOKBACK_12M_DAYS = 52


@dataclass(frozen=True)
class TrendTemplateSnapshot:
    matched: bool
    current_price: float
    ma50: float
    ma150: float
    ma200: float
    ma200_lookback: float
    high_52wk: float
    low_52wk: float
    rs_rating: float
    avg_volume_20: float
    avg_dollar_volume_20: float
    distance_to_ma50_pct: float
    distance_to_ma150_pct: float
    distance_to_ma200_pct: float
    distance_from_52wk_high_pct: float
    distance_from_52wk_low_pct: float
    criteria_passed: int
    criteria_total: int
    criteria: dict[str, bool]
    reasons: list[str]

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True)
class TrendTemplateHit:
    ticker: str
    sector: str | None
    industry: str | None
    exchange: str | None
    signal_date: str
    current_price: float
    ma50: float
    ma150: float
    ma200: float
    ma200_lookback: float
    high_52wk: float
    low_52wk: float
    rs_rating: float
    avg_volume_20: float
    avg_dollar_volume_20: float
    distance_to_ma50_pct: float
    distance_to_ma150_pct: float
    distance_to_ma200_pct: float
    distance_from_52wk_high_pct: float
    distance_from_52wk_low_pct: float
    criteria_passed: int
    criteria_total: int
    reasons: list[str]

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True)
class TrendTemplateScreenResult:
    run_date: str
    total_tickers: int
    passed_tickers: int
    failed_tickers: list[dict[str, str]]
    hits: list[TrendTemplateHit]

    def to_dict(self) -> dict[str, object]:
        return {
            "run_date": self.run_date,
            "total_tickers": self.total_tickers,
            "passed_tickers": self.passed_tickers,
            "failed_tickers": self.failed_tickers,
            "hits": [item.to_dict() for item in self.hits],
        }


def _normalize_price_frame(frame: pd.DataFrame) -> pd.DataFrame:
    required = ["High", "Low", "Close", "Volume"]
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
            "Low": [row.get("low") for row in rows],
            "Close": [row.get("close") for row in rows],
            "Volume": [row.get("volume") for row in rows],
        }
    )
    return frame.dropna(subset=["Date", "High", "Low", "Close", "Volume"]).set_index("Date").sort_index()


def _pct_from_level(current_price: float, level: float) -> float:
    if level <= 0:
        return 0.0
    return ((current_price / level) - 1.0) * 100.0


def _compute_attached_rs_rating(close: pd.Series) -> float | None:
    latest_index = close.index[-1]
    current_close = float(close.loc[latest_index])
    close_3m = float(close.shift(RS_LOOKBACK_3M_DAYS).loc[latest_index])
    close_6m = float(close.shift(RS_LOOKBACK_6M_DAYS).loc[latest_index])
    close_9m = float(close.shift(RS_LOOKBACK_9M_DAYS).loc[latest_index])
    close_12m = float(close.shift(RS_LOOKBACK_12M_DAYS).loc[latest_index])
    if min(current_close, close_3m, close_6m, close_9m, close_12m) <= 0:
        return None
    return (
        (0.4 * (current_close / close_3m))
        + (0.2 * (current_close / (close_6m * 2.0)))
        + (0.2 * (current_close / (close_9m * 3.0)))
        + (0.2 * (current_close / (close_12m * 4.0)))
    ) * 100.0


def evaluate_trend_template(frame: pd.DataFrame) -> TrendTemplateSnapshot | None:
    bars = _normalize_price_frame(frame)
    min_required = max(HIGH_LOW_LOOKBACK_DAYS, MA200_LENGTH + MA200_UPTREND_LOOKBACK_DAYS)
    if bars.empty or len(bars) < min_required:
        return None

    close = bars["Close"].astype(float)
    high = bars["High"].astype(float)
    low = bars["Low"].astype(float)
    volume = bars["Volume"].astype(float)

    ma50_series = close.rolling(MA50_LENGTH).mean()
    ma150_series = close.rolling(MA150_LENGTH).mean()
    ma200_series = close.rolling(MA200_LENGTH).mean()
    high_52wk_series = high.rolling(HIGH_LOW_LOOKBACK_DAYS).max()
    low_52wk_series = low.rolling(HIGH_LOW_LOOKBACK_DAYS).min()

    latest_index = bars.index[-1]
    current_price = float(close.loc[latest_index])
    ma50 = float(ma50_series.loc[latest_index])
    ma150 = float(ma150_series.loc[latest_index])
    ma200 = float(ma200_series.loc[latest_index])
    ma200_lookback = float(ma200_series.shift(MA200_UPTREND_LOOKBACK_DAYS).loc[latest_index])
    high_52wk = float(high_52wk_series.loc[latest_index])
    low_52wk = float(low_52wk_series.loc[latest_index])
    rs_rating = _compute_attached_rs_rating(close)

    if rs_rating is None or any(pd.isna(value) for value in (ma50, ma150, ma200, ma200_lookback, high_52wk, low_52wk)):
        return None

    distance_to_ma50_pct = _pct_from_level(current_price, ma50)
    distance_to_ma150_pct = _pct_from_level(current_price, ma150)
    distance_to_ma200_pct = _pct_from_level(current_price, ma200)
    distance_from_52wk_high_pct = ((high_52wk / current_price) - 1.0) * 100.0 if current_price > 0 else 0.0
    distance_from_52wk_low_pct = _pct_from_level(current_price, low_52wk)
    avg_volume_20 = float(volume.tail(VOLUME_LOOKBACK_DAYS).mean())
    avg_dollar_volume_20 = float((close.tail(VOLUME_LOOKBACK_DAYS) * volume.tail(VOLUME_LOOKBACK_DAYS)).mean())

    criteria = {
        "price_above_ma150_and_ma200": current_price > ma150 and current_price > ma200,
        "ma150_above_ma200": ma150 > ma200,
        "ma200_trending_up_22d": ma200 > ma200_lookback,
        "ma50_above_ma150_and_ma200": ma50 > ma150 and ma50 > ma200,
        "price_above_ma50": current_price > ma50,
        "price_30pct_above_52w_low": distance_from_52wk_low_pct >= MIN_DISTANCE_FROM_52W_LOW_PCT,
        "price_within_25pct_of_52w_high": distance_from_52wk_high_pct <= MAX_DISTANCE_FROM_52W_HIGH_PCT,
        "rs_rating_above_70": rs_rating > MIN_RS_RATING,
    }
    criteria_passed = sum(1 for passed in criteria.values() if passed)
    criteria_total = len(criteria)
    reasons = [
        f"close {current_price:.2f} is {distance_to_ma50_pct:+.1f}% vs 50D MA {ma50:.2f}",
        f"close {distance_to_ma150_pct:+.1f}% vs 150D MA {ma150:.2f} and {distance_to_ma200_pct:+.1f}% vs 200D MA {ma200:.2f}",
        f"150D/200D stack is {ma150:.2f} / {ma200:.2f}",
        f"200D MA {ma200:.2f} vs 22-day lookback {ma200_lookback:.2f}",
        f"{distance_from_52wk_high_pct:.1f}% below 52-week high {high_52wk:.2f}",
        f"{distance_from_52wk_low_pct:.1f}% above 52-week low {low_52wk:.2f}",
        f"RS rating {rs_rating:.2f} vs minimum {MIN_RS_RATING:.0f}",
    ]
    return TrendTemplateSnapshot(
        matched=criteria_passed == criteria_total,
        current_price=current_price,
        ma50=ma50,
        ma150=ma150,
        ma200=ma200,
        ma200_lookback=ma200_lookback,
        high_52wk=high_52wk,
        low_52wk=low_52wk,
        rs_rating=rs_rating,
        avg_volume_20=avg_volume_20,
        avg_dollar_volume_20=avg_dollar_volume_20,
        distance_to_ma50_pct=distance_to_ma50_pct,
        distance_to_ma150_pct=distance_to_ma150_pct,
        distance_to_ma200_pct=distance_to_ma200_pct,
        distance_from_52wk_high_pct=distance_from_52wk_high_pct,
        distance_from_52wk_low_pct=distance_from_52wk_low_pct,
        criteria_passed=criteria_passed,
        criteria_total=criteria_total,
        criteria=criteria,
        reasons=reasons,
    )


def _build_hit(ticker: UniverseTicker, frame: pd.DataFrame, *, signal_date: dt.date) -> TrendTemplateHit | None:
    snapshot = evaluate_trend_template(frame)
    if snapshot is None or not snapshot.matched:
        return None
    return TrendTemplateHit(
        ticker=ticker.symbol,
        sector=ticker.sector,
        industry=ticker.industry,
        exchange=ticker.exchange,
        signal_date=signal_date.isoformat(),
        current_price=snapshot.current_price,
        ma50=snapshot.ma50,
        ma150=snapshot.ma150,
        ma200=snapshot.ma200,
        ma200_lookback=snapshot.ma200_lookback,
        high_52wk=snapshot.high_52wk,
        low_52wk=snapshot.low_52wk,
        rs_rating=snapshot.rs_rating,
        avg_volume_20=snapshot.avg_volume_20,
        avg_dollar_volume_20=snapshot.avg_dollar_volume_20,
        distance_to_ma50_pct=snapshot.distance_to_ma50_pct,
        distance_to_ma150_pct=snapshot.distance_to_ma150_pct,
        distance_to_ma200_pct=snapshot.distance_to_ma200_pct,
        distance_from_52wk_high_pct=snapshot.distance_from_52wk_high_pct,
        distance_from_52wk_low_pct=snapshot.distance_from_52wk_low_pct,
        criteria_passed=snapshot.criteria_passed,
        criteria_total=snapshot.criteria_total,
        reasons=snapshot.reasons,
    )


def run_trend_template_screen(
    config: AppConfig,
    tickers: list[UniverseTicker],
    *,
    as_of_date: dt.date | None = None,
) -> TrendTemplateScreenResult:
    run_date = as_of_date or dt.date.today()
    total_tickers = len(tickers)
    print(
        "starting trend-template screen: "
        f"total={total_tickers}, "
        f"ma_stack=50/150/200, "
        f"52w_high<={MAX_DISTANCE_FROM_52W_HIGH_PCT:.0f}%, "
        f"52w_low>={MIN_DISTANCE_FROM_52W_LOW_PCT:.0f}%, "
        f"rs>{MIN_RS_RATING:.0f}"
    )

    database_url = resolve_database_url("")
    frame_map = load_many_ticker_windows(
        [item.symbol for item in tickers],
        run_date,
        PRICE_HISTORY_DAYS,
        database_url=database_url,
    )

    hits: list[TrendTemplateHit] = []
    failures: list[dict[str, str]] = []
    fallback_tickers: list[tuple[int, UniverseTicker]] = []

    for position, ticker in enumerate(tickers, start=1):
        frame = frame_map.get(ticker.symbol.upper())
        if frame is None or not db_frame_has_recent_coverage(frame, run_date) or len(frame) < HIGH_LOW_LOOKBACK_DAYS:
            fallback_tickers.append((position, ticker))
            continue
        print(f"[{position}/{total_tickers}] screening {ticker.symbol} from DB | passed={len(hits)}")
        try:
            hit = _build_hit(ticker, frame, signal_date=run_date)
            if hit is None:
                print(f"[{position}/{total_tickers}] {ticker.symbol} filtered: trend template failed | passed={len(hits)}")
                continue
            hits.append(hit)
            print(
                f"[{position}/{total_tickers}] {ticker.symbol} passed: "
                f"{hit.distance_from_52wk_high_pct:.1f}% below 52-week high | RS {hit.rs_rating:.2f} | passed={len(hits)}"
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
                    hit = _build_hit(ticker, frame, signal_date=run_date)
                    if hit is None:
                        print(f"[{position}/{total_tickers}] {ticker.symbol} filtered: trend template failed | passed={len(hits)}")
                        continue
                    hits.append(hit)
                    print(
                        f"[{position}/{total_tickers}] {ticker.symbol} passed: "
                        f"{hit.distance_from_52wk_high_pct:.1f}% below 52-week high | RS {hit.rs_rating:.2f} | passed={len(hits)}"
                    )
                except Exception as exc:
                    failures.append({"ticker": ticker.symbol, "error": str(exc)})
                    print(f"[{position}/{total_tickers}] {ticker.symbol} error: {exc} | passed={len(hits)}")

    hits.sort(
        key=lambda item: (
            item.distance_from_52wk_high_pct,
            -item.distance_from_52wk_low_pct,
            -item.rs_rating,
            -item.avg_dollar_volume_20,
            item.ticker,
        )
    )
    return TrendTemplateScreenResult(
        run_date=run_date.isoformat(),
        total_tickers=total_tickers,
        passed_tickers=len(hits),
        failed_tickers=failures,
        hits=hits,
    )


