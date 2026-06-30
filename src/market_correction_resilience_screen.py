from __future__ import annotations

from dataclasses import asdict, dataclass
import datetime as dt

import pandas as pd
import yfinance as yf

from .config import AppConfig
from .cookstock_bridge import freeze_cookstock_today, load_configured_cookstock
from .market_data_access import db_frame_has_recent_coverage, load_many_ticker_windows, resolve_database_url
from .universe import UniverseTicker


MARKET_CORRECTION_YEAR_HIGH_LOOKBACK_DAYS = 260
MARKET_CORRECTION_VOLUME_LOOKBACK_DAYS = 20
MARKET_CORRECTION_RS_LOOKBACK_3M_DAYS = 13
MARKET_CORRECTION_RS_LOOKBACK_6M_DAYS = 26
MARKET_CORRECTION_RS_LOOKBACK_9M_DAYS = 39
MARKET_CORRECTION_RS_LOOKBACK_12M_DAYS = 52


@dataclass(frozen=True)
class MarketCorrectionState:
    benchmark_ticker: str
    signal_date: str
    current_price: float
    history_high_close: float
    drawdown_from_high_pct: float
    ma21: float
    ma50: float
    below_ma21: bool
    below_ma50: bool
    correction_threshold_pct: float
    market_in_correction: bool
    reasons: list[str]

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True)
class MarketCorrectionResilienceSnapshot:
    matched: bool
    current_price: float
    ema21: float
    ema21_prev: float
    ema40: float
    ema40_prev: float
    high_52wk: float
    rs_rating: float
    avg_volume_20: float
    avg_dollar_volume_20: float
    distance_from_52wk_high_pct: float
    criteria_passed: int
    criteria_total: int
    criteria: dict[str, bool]
    reasons: list[str]

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True)
class MarketCorrectionResilienceHit:
    ticker: str
    sector: str | None
    industry: str | None
    exchange: str | None
    signal_date: str
    benchmark_ticker: str
    benchmark_drawdown_pct: float
    current_price: float
    ema21: float
    ema21_prev: float
    ema40: float
    ema40_prev: float
    high_52wk: float
    rs_rating: float
    avg_volume_20: float
    avg_dollar_volume_20: float
    distance_from_52wk_high_pct: float
    criteria_passed: int
    criteria_total: int
    reasons: list[str]

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True)
class MarketCorrectionResilienceScreenResult:
    run_date: str
    total_tickers: int
    passed_tickers: int
    failed_tickers: list[dict[str, str]]
    hits: list[MarketCorrectionResilienceHit]
    skipped: bool = False
    skip_reason: str | None = None
    market_state: dict[str, object] | None = None

    def to_dict(self) -> dict[str, object]:
        return {
            "run_date": self.run_date,
            "total_tickers": self.total_tickers,
            "passed_tickers": self.passed_tickers,
            "failed_tickers": self.failed_tickers,
            "hits": [item.to_dict() for item in self.hits],
            "skipped": self.skipped,
            "skip_reason": self.skip_reason,
            "market_state": self.market_state,
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


def _download_history_frame(ticker: str, run_date: dt.date, history_days: int) -> pd.DataFrame | None:
    start_date = run_date - dt.timedelta(days=max(30, int(history_days) * 2))
    history = yf.download(
        tickers=ticker,
        start=start_date.isoformat(),
        end=(run_date + dt.timedelta(days=1)).isoformat(),
        interval="1d",
        auto_adjust=False,
        progress=False,
        threads=False,
    )
    if history is None or history.empty:
        return None
    frame = history.copy()
    if isinstance(frame.columns, pd.MultiIndex):
        frame.columns = frame.columns.get_level_values(0)
    frame = frame.rename(columns=str)
    for column in ("Open", "High", "Low", "Close", "Adj Close", "Volume"):
        if column in frame.columns:
            frame[column] = pd.to_numeric(frame[column], errors="coerce")
    frame = frame.dropna(subset=["High", "Low", "Close", "Volume"]).sort_index()
    return frame if not frame.empty else None


def _compute_attached_rs_rating(close: pd.Series) -> float | None:
    latest_index = close.index[-1]
    current_close = float(close.loc[latest_index])
    close_3m = float(close.shift(MARKET_CORRECTION_RS_LOOKBACK_3M_DAYS).loc[latest_index])
    close_6m = float(close.shift(MARKET_CORRECTION_RS_LOOKBACK_6M_DAYS).loc[latest_index])
    close_9m = float(close.shift(MARKET_CORRECTION_RS_LOOKBACK_9M_DAYS).loc[latest_index])
    close_12m = float(close.shift(MARKET_CORRECTION_RS_LOOKBACK_12M_DAYS).loc[latest_index])
    if min(current_close, close_3m, close_6m, close_9m, close_12m) <= 0:
        return None
    return (
        (0.4 * (current_close / close_3m))
        + (0.2 * (current_close / (close_6m * 2.0)))
        + (0.2 * (current_close / (close_9m * 3.0)))
        + (0.2 * (current_close / (close_12m * 4.0)))
    ) * 100.0


def evaluate_market_correction_state(frame: pd.DataFrame, config: AppConfig) -> MarketCorrectionState | None:
    bars = _normalize_price_frame(frame)
    ma21_period = int(config.market_correction_benchmark_ma_short_period)
    ma50_period = int(config.market_correction_benchmark_ma_medium_period)
    min_required = max(
        int(config.market_correction_benchmark_history_days),
        MARKET_CORRECTION_YEAR_HIGH_LOOKBACK_DAYS,
        ma50_period + 5,
    )
    if bars.empty or len(bars) < min_required:
        return None

    close = bars["Close"].astype(float)
    current_price = float(close.iloc[-1])
    history_high_close = float(close.max())
    ma21 = float(close.rolling(ma21_period).mean().iloc[-1])
    ma50 = float(close.rolling(ma50_period).mean().iloc[-1])
    if any(pd.isna(value) for value in (ma21, ma50)) or history_high_close <= 0:
        return None

    drawdown_from_high_pct = ((history_high_close / current_price) - 1.0) * 100.0 if current_price > 0 else 0.0
    below_ma21 = current_price < ma21
    below_ma50 = current_price < ma50
    threshold_pct = float(config.market_correction_benchmark_drawdown_pct)
    market_in_correction = drawdown_from_high_pct >= threshold_pct and (below_ma21 or below_ma50)
    reasons = [
        f"{config.benchmark_ticker.upper()} close {current_price:.2f}",
        f"drawdown {drawdown_from_high_pct:.2f}% from history high close {history_high_close:.2f}",
        f"vs 21D MA {ma21:.2f} ({'below' if below_ma21 else 'above'})",
        f"vs 50D MA {ma50:.2f} ({'below' if below_ma50 else 'above'})",
    ]
    return MarketCorrectionState(
        benchmark_ticker=config.benchmark_ticker.upper(),
        signal_date=bars.index[-1].date().isoformat(),
        current_price=current_price,
        history_high_close=history_high_close,
        drawdown_from_high_pct=drawdown_from_high_pct,
        ma21=ma21,
        ma50=ma50,
        below_ma21=below_ma21,
        below_ma50=below_ma50,
        correction_threshold_pct=threshold_pct,
        market_in_correction=market_in_correction,
        reasons=reasons,
    )


def evaluate_market_correction_resilience(frame: pd.DataFrame, config: AppConfig) -> MarketCorrectionResilienceSnapshot | None:
    bars = _normalize_price_frame(frame)
    ema21_period = int(config.market_correction_stock_ema_short_period)
    ema40_period = int(config.market_correction_stock_ema_weekly_period)
    min_required = max(
        MARKET_CORRECTION_YEAR_HIGH_LOOKBACK_DAYS,
        ema40_period + 5,
        MARKET_CORRECTION_RS_LOOKBACK_12M_DAYS + 5,
    )
    if bars.empty or len(bars) < min_required:
        return None

    close = bars["Close"].astype(float)
    high = bars["High"].astype(float)
    volume = bars["Volume"].astype(float)
    latest_index = close.index[-1]
    current_price = float(close.loc[latest_index])
    ema21_series = close.ewm(span=ema21_period, adjust=False).mean()
    ema40_series = close.ewm(span=ema40_period, adjust=False).mean()
    ema21 = float(ema21_series.loc[latest_index])
    ema40 = float(ema40_series.loc[latest_index])
    ema21_prev = float(ema21_series.shift(1).loc[latest_index])
    ema40_prev = float(ema40_series.shift(1).loc[latest_index])
    high_52wk = float(high.rolling(MARKET_CORRECTION_YEAR_HIGH_LOOKBACK_DAYS).max().loc[latest_index])
    rs_rating = _compute_attached_rs_rating(close)
    avg_volume_20 = float(volume.tail(MARKET_CORRECTION_VOLUME_LOOKBACK_DAYS).mean())
    avg_dollar_volume_20 = float((close.tail(MARKET_CORRECTION_VOLUME_LOOKBACK_DAYS) * volume.tail(MARKET_CORRECTION_VOLUME_LOOKBACK_DAYS)).mean())

    if rs_rating is None or any(pd.isna(value) for value in (ema21, ema40, ema21_prev, ema40_prev, high_52wk)):
        return None

    distance_from_52wk_high_pct = ((high_52wk / current_price) - 1.0) * 100.0 if current_price > 0 else 0.0
    ema21_ok = current_price > ema21 and ema21 > ema21_prev
    ema40_ok = current_price > ema40 and ema40 > ema40_prev
    criteria = {
        "above_rising_ema21": ema21_ok,
        "above_rising_8w_ema": ema40_ok,
        "within_10pct_of_52w_high": distance_from_52wk_high_pct <= float(config.market_correction_stock_max_distance_from_52wk_high_pct),
        "rs_rating_above_min": rs_rating >= float(config.market_correction_stock_min_rs_rating),
    }
    criteria_passed = sum(1 for passed in criteria.values() if passed)
    matched = criteria["within_10pct_of_52w_high"] and criteria["rs_rating_above_min"] and (ema21_ok or ema40_ok)
    reasons = [
        f"close {current_price:.2f} vs EMA21 {ema21:.2f} ({'rising' if ema21 > ema21_prev else 'flat/down'})",
        f"close {current_price:.2f} vs 8W EMA {ema40:.2f} ({'rising' if ema40 > ema40_prev else 'flat/down'})",
        f"{distance_from_52wk_high_pct:.2f}% below 52-week high {high_52wk:.2f}",
        f"RS rating {rs_rating:.2f} vs minimum {float(config.market_correction_stock_min_rs_rating):.0f}",
    ]
    return MarketCorrectionResilienceSnapshot(
        matched=matched,
        current_price=current_price,
        ema21=ema21,
        ema21_prev=ema21_prev,
        ema40=ema40,
        ema40_prev=ema40_prev,
        high_52wk=high_52wk,
        rs_rating=rs_rating,
        avg_volume_20=avg_volume_20,
        avg_dollar_volume_20=avg_dollar_volume_20,
        distance_from_52wk_high_pct=distance_from_52wk_high_pct,
        criteria_passed=criteria_passed,
        criteria_total=len(criteria),
        criteria=criteria,
        reasons=reasons,
    )


def _build_hit(
    ticker: UniverseTicker,
    frame: pd.DataFrame,
    *,
    signal_date: dt.date,
    config: AppConfig,
    market_state: MarketCorrectionState,
) -> MarketCorrectionResilienceHit | None:
    snapshot = evaluate_market_correction_resilience(frame, config)
    if snapshot is None or not snapshot.matched:
        return None
    return MarketCorrectionResilienceHit(
        ticker=ticker.symbol,
        sector=ticker.sector,
        industry=ticker.industry,
        exchange=ticker.exchange,
        signal_date=signal_date.isoformat(),
        benchmark_ticker=market_state.benchmark_ticker,
        benchmark_drawdown_pct=market_state.drawdown_from_high_pct,
        current_price=snapshot.current_price,
        ema21=snapshot.ema21,
        ema21_prev=snapshot.ema21_prev,
        ema40=snapshot.ema40,
        ema40_prev=snapshot.ema40_prev,
        high_52wk=snapshot.high_52wk,
        rs_rating=snapshot.rs_rating,
        avg_volume_20=snapshot.avg_volume_20,
        avg_dollar_volume_20=snapshot.avg_dollar_volume_20,
        distance_from_52wk_high_pct=snapshot.distance_from_52wk_high_pct,
        criteria_passed=snapshot.criteria_passed,
        criteria_total=snapshot.criteria_total,
        reasons=snapshot.reasons,
    )


def _load_benchmark_frame(config: AppConfig, run_date: dt.date, history_days: int, frame_map: dict[str, pd.DataFrame]) -> pd.DataFrame | None:
    benchmark_ticker = config.benchmark_ticker.upper()
    frame = frame_map.get(benchmark_ticker)
    min_required = max(int(config.market_correction_benchmark_history_days), int(config.market_correction_benchmark_ma_medium_period) + 5)
    if frame is not None and db_frame_has_recent_coverage(frame, run_date) and len(frame) >= min_required:
        return frame

    try:
        cookstock = load_configured_cookstock(config)
    except Exception:
        cookstock = None
    if cookstock is not None:
        with freeze_cookstock_today(cookstock, run_date):
            try:
                financials = cookstock.cookFinancials(
                    benchmark_ticker,
                    benchmarkTicker=benchmark_ticker,
                    historyLookbackDays=history_days,
                )
                frame = _build_price_frame(financials)
                if not frame.empty:
                    return frame
            except Exception:
                pass

    return _download_history_frame(benchmark_ticker, run_date, history_days)


def run_market_correction_resilience_screen(
    config: AppConfig,
    tickers: list[UniverseTicker],
    *,
    as_of_date: dt.date | None = None,
) -> MarketCorrectionResilienceScreenResult:
    run_date = as_of_date or dt.date.today()
    total_tickers = len(tickers)
    benchmark_history_days = max(int(config.market_correction_benchmark_history_days), MARKET_CORRECTION_YEAR_HIGH_LOOKBACK_DAYS, 320)
    stock_history_days = max(
        MARKET_CORRECTION_YEAR_HIGH_LOOKBACK_DAYS,
        int(config.market_correction_stock_ema_weekly_period) + 10,
        320,
    )
    print(
        "starting market-correction resilience screen: "
        f"total={total_tickers}, benchmark={config.benchmark_ticker.upper()}, "
        f"drawdown>={float(config.market_correction_benchmark_drawdown_pct):.1f}%, "
        f"within_high<={float(config.market_correction_stock_max_distance_from_52wk_high_pct):.1f}%, "
        f"rs>={float(config.market_correction_stock_min_rs_rating):.0f}"
    )

    database_url = resolve_database_url("")
    symbols = [item.symbol for item in tickers]
    frame_map = load_many_ticker_windows(
        symbols + [config.benchmark_ticker.upper()],
        run_date,
        max(benchmark_history_days, stock_history_days),
        database_url=database_url,
    )

    benchmark_frame = _load_benchmark_frame(config, run_date, benchmark_history_days, frame_map)
    market_state = evaluate_market_correction_state(benchmark_frame, config) if benchmark_frame is not None else None
    if market_state is None:
        message = f"Unable to evaluate {config.benchmark_ticker.upper()} correction state with available history."
        print(message)
        return MarketCorrectionResilienceScreenResult(
            run_date=run_date.isoformat(),
            total_tickers=total_tickers,
            passed_tickers=0,
            failed_tickers=[],
            hits=[],
            skipped=True,
            skip_reason=message,
            market_state=None,
        )

    if not market_state.market_in_correction:
        message = (
            f"Skipped: {market_state.benchmark_ticker} drawdown is {market_state.drawdown_from_high_pct:.2f}% "
            f"and market correction gate is not active."
        )
        print(message)
        return MarketCorrectionResilienceScreenResult(
            run_date=run_date.isoformat(),
            total_tickers=total_tickers,
            passed_tickers=0,
            failed_tickers=[],
            hits=[],
            skipped=True,
            skip_reason=message,
            market_state=market_state.to_dict(),
        )

    hits: list[MarketCorrectionResilienceHit] = []
    failures: list[dict[str, str]] = []
    fallback_tickers: list[tuple[int, UniverseTicker]] = []

    for position, ticker in enumerate(tickers, start=1):
        frame = frame_map.get(ticker.symbol.upper())
        if frame is None or not db_frame_has_recent_coverage(frame, run_date) or len(frame) < MARKET_CORRECTION_YEAR_HIGH_LOOKBACK_DAYS:
            fallback_tickers.append((position, ticker))
            continue
        print(f"[{position}/{total_tickers}] screening {ticker.symbol} from DB | passed={len(hits)}")
        try:
            hit = _build_hit(ticker, frame, signal_date=run_date, config=config, market_state=market_state)
            if hit is None:
                print(f"[{position}/{total_tickers}] {ticker.symbol} filtered: resilience criteria failed | passed={len(hits)}")
                continue
            hits.append(hit)
            print(
                f"[{position}/{total_tickers}] {ticker.symbol} passed: "
                f"RS {hit.rs_rating:.2f} | {hit.distance_from_52wk_high_pct:.2f}% below 52-week high | passed={len(hits)}"
            )
        except Exception as exc:
            failures.append({"ticker": ticker.symbol, "error": str(exc)})
            print(f"[{position}/{total_tickers}] {ticker.symbol} error: {exc} | passed={len(hits)}")

    if fallback_tickers:
        try:
            cookstock = load_configured_cookstock(config)
        except Exception:
            cookstock = None
        context = freeze_cookstock_today(cookstock, as_of_date) if cookstock is not None else None
        if context is None:
            class _NoopContext:
                def __enter__(self):
                    return None
                def __exit__(self, exc_type, exc, tb):
                    return False
            context = _NoopContext()
        with context:
            for position, ticker in fallback_tickers:
                print(f"[{position}/{total_tickers}] screening {ticker.symbol} from internet fallback | passed={len(hits)}")
                try:
                    frame = None
                    if cookstock is not None:
                        try:
                            financials = cookstock.cookFinancials(
                                ticker.symbol,
                                benchmarkTicker=config.benchmark_ticker,
                                historyLookbackDays=stock_history_days,
                            )
                            frame = _build_price_frame(financials)
                        except Exception:
                            frame = None
                    if frame is None or frame.empty:
                        frame = _download_history_frame(ticker.symbol, run_date, stock_history_days)
                    if frame is None or frame.empty:
                        raise ValueError("missing fallback daily bars")
                    hit = _build_hit(ticker, frame, signal_date=run_date, config=config, market_state=market_state)
                    if hit is None:
                        print(f"[{position}/{total_tickers}] {ticker.symbol} filtered: resilience criteria failed | passed={len(hits)}")
                        continue
                    hits.append(hit)
                    print(
                        f"[{position}/{total_tickers}] {ticker.symbol} passed: "
                        f"RS {hit.rs_rating:.2f} | {hit.distance_from_52wk_high_pct:.2f}% below 52-week high | passed={len(hits)}"
                    )
                except Exception as exc:
                    failures.append({"ticker": ticker.symbol, "error": str(exc)})
                    print(f"[{position}/{total_tickers}] {ticker.symbol} error: {exc} | passed={len(hits)}")

    hits.sort(
        key=lambda item: (
            -item.rs_rating,
            item.distance_from_52wk_high_pct,
            -item.avg_dollar_volume_20,
            item.ticker,
        )
    )
    return MarketCorrectionResilienceScreenResult(
        run_date=run_date.isoformat(),
        total_tickers=total_tickers,
        passed_tickers=len(hits),
        failed_tickers=failures,
        hits=hits,
        skipped=False,
        skip_reason=None,
        market_state=market_state.to_dict(),
    )
