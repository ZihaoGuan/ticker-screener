from __future__ import annotations

from dataclasses import asdict, dataclass
import datetime as dt

import pandas as pd

from .config import AppConfig
from .cookstock_bridge import freeze_cookstock_today, load_configured_cookstock
from .market_data_access import (
    db_frame_has_recent_coverage,
    load_active_universe_from_db,
    load_many_ticker_windows,
    load_ticker_metadata_map,
    load_ticker_window,
    resolve_database_url,
)
from .ratings.repository import RatingsRepository
from .universe import UniverseTicker


ONE_YEAR_WINNERS_STRATEGY_ID = "one_year_winners"
ONE_YEAR_WINNERS_HISTORY_DAYS = 380
ONE_YEAR_SESSIONS = 252
ONE_MONTH_SESSIONS = 21
ONE_WEEK_SESSIONS = 5
BETA_MIN_OBSERVATIONS = 120
MIN_PRICE = 20.0
MIN_MARKET_CAP = 10_000_000_000.0
MIN_ONE_YEAR_RETURN_PCT = 30.0
MIN_REVENUE_GROWTH_TTM_YOY_PCT = 0.0
MIN_BETA_1Y = 1.0
MIN_MONTHLY_DOLLAR_VOLUME = 900_000_000.0


@dataclass(frozen=True)
class OneYearWinnersSnapshot:
    matched: bool
    current_price: float
    market_cap: float | None
    one_week_return_pct: float
    one_month_return_pct: float
    one_year_return_pct: float
    revenue_growth_ttm_yoy_pct: float | None
    beta_1y: float | None
    avg_volume_1m: float
    monthly_dollar_volume: float
    ema21: float
    sma50: float
    ema100: float
    criteria_passed: int
    criteria_total: int
    criteria: dict[str, bool]
    reasons: list[str]

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True)
class OneYearWinnersHit:
    ticker: str
    sector: str | None
    industry: str | None
    exchange: str | None
    signal_date: str
    current_price: float
    market_cap: float | None
    one_week_return_pct: float
    one_month_return_pct: float
    one_year_return_pct: float
    revenue_growth_ttm_yoy_pct: float | None
    beta_1y: float | None
    avg_volume_1m: float
    monthly_dollar_volume: float
    ema21: float
    sma50: float
    ema100: float
    criteria_passed: int
    criteria_total: int
    reasons: list[str]

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True)
class OneYearWinnersScreenResult:
    run_date: str
    benchmark_ticker: str
    total_tickers: int
    passed_tickers: int
    failed_tickers: list[dict[str, str]]
    hits: list[OneYearWinnersHit]

    def to_dict(self) -> dict[str, object]:
        return {
            "run_date": self.run_date,
            "benchmark_ticker": self.benchmark_ticker,
            "total_tickers": self.total_tickers,
            "passed_tickers": self.passed_tickers,
            "failed_tickers": self.failed_tickers,
            "hits": [item.to_dict() for item in self.hits],
        }


def _log(message: str) -> None:
    print(message, flush=True)


def _normalize_price_frame(frame: pd.DataFrame) -> pd.DataFrame:
    required = ["Close", "Volume"]
    available = {str(column).lower(): column for column in frame.columns}
    if any(column.lower() not in available for column in required):
        return pd.DataFrame()
    normalized = frame[[available[column.lower()] for column in required]].copy()
    normalized.columns = required
    normalized = normalized.dropna(subset=required).sort_index()
    if not isinstance(normalized.index, pd.DatetimeIndex):
        normalized.index = pd.to_datetime(normalized.index)
    return normalized


def _build_price_frame(financials: object) -> pd.DataFrame:
    rows = financials._get_clean_price_data()  # type: ignore[attr-defined]
    if not rows:
        return pd.DataFrame()
    frame = pd.DataFrame(
        {
            "Date": pd.to_datetime([row.get("formatted_date") for row in rows]),
            "Close": [row.get("close") for row in rows],
            "Volume": [row.get("volume") for row in rows],
        }
    )
    return frame.dropna(subset=["Date", "Close", "Volume"]).set_index("Date").sort_index()


def _period_return_pct(close: pd.Series, sessions: int) -> float:
    prior = float(close.iloc[-(sessions + 1)])
    return ((float(close.iloc[-1]) / prior) - 1.0) * 100.0 if prior > 0 else 0.0


def _compute_beta_1y(stock_close: pd.Series, benchmark_frame: pd.DataFrame | None) -> float | None:
    if benchmark_frame is None:
        return None
    benchmark_bars = _normalize_price_frame(benchmark_frame)
    if benchmark_bars.empty:
        return None
    aligned = pd.concat(
        [
            stock_close.pct_change().rename("stock"),
            benchmark_bars["Close"].astype(float).pct_change().rename("benchmark"),
        ],
        axis=1,
        join="inner",
    ).dropna()
    aligned = aligned.tail(ONE_YEAR_SESSIONS)
    if len(aligned) < BETA_MIN_OBSERVATIONS:
        return None
    benchmark_variance = float(aligned["benchmark"].var())
    if benchmark_variance <= 0:
        return None
    return float(aligned["stock"].cov(aligned["benchmark"]) / benchmark_variance)


def evaluate_one_year_winners(
    frame: pd.DataFrame,
    benchmark_frame: pd.DataFrame | None,
    *,
    market_cap: float | None,
    revenue_growth_ttm_yoy_pct: float | None,
) -> OneYearWinnersSnapshot | None:
    bars = _normalize_price_frame(frame)
    if bars.empty or len(bars) < ONE_YEAR_SESSIONS + 1:
        return None

    close = bars["Close"].astype(float)
    volume = bars["Volume"].astype(float)
    current_price = float(close.iloc[-1])
    ema21 = float(close.ewm(span=21, adjust=False).mean().iloc[-1])
    sma50 = float(close.rolling(50).mean().iloc[-1])
    ema100 = float(close.ewm(span=100, adjust=False).mean().iloc[-1])
    avg_volume_1m = float(volume.tail(ONE_MONTH_SESSIONS).mean())
    monthly_dollar_volume = current_price * avg_volume_1m
    one_week_return_pct = _period_return_pct(close, ONE_WEEK_SESSIONS)
    one_month_return_pct = _period_return_pct(close, ONE_MONTH_SESSIONS)
    one_year_return_pct = _period_return_pct(close, ONE_YEAR_SESSIONS)
    beta_1y = _compute_beta_1y(close, benchmark_frame)

    if any(pd.isna(value) for value in (ema21, sma50, ema100, avg_volume_1m)):
        return None

    criteria = {
        "price_gt_20": current_price > MIN_PRICE,
        "price_gt_ema100": current_price > ema100,
        "ema21_gt_sma50": ema21 > sma50,
        "market_cap_gt_10b": market_cap is not None and market_cap > MIN_MARKET_CAP,
        "one_year_return_gt_30pct": one_year_return_pct > MIN_ONE_YEAR_RETURN_PCT,
        "revenue_growth_ttm_yoy_gt_0": (
            revenue_growth_ttm_yoy_pct is not None
            and revenue_growth_ttm_yoy_pct > MIN_REVENUE_GROWTH_TTM_YOY_PCT
        ),
        "beta_1y_gt_1": beta_1y is not None and beta_1y > MIN_BETA_1Y,
        "monthly_dollar_volume_gt_900m": monthly_dollar_volume > MIN_MONTHLY_DOLLAR_VOLUME,
    }
    reasons = [
        f"close ${current_price:.2f} above $20 and 100 EMA ${ema100:.2f}",
        f"21 EMA ${ema21:.2f} above 50 SMA ${sma50:.2f}",
        f"market cap ${(market_cap or 0.0) / 1_000_000_000:.2f}B above $10B"
        if market_cap is not None
        else "market cap unavailable",
        f"rolling 1Y return {one_year_return_pct:.1f}% above 30%",
        f"1W / 1M returns {one_week_return_pct:.1f}% / {one_month_return_pct:.1f}%",
        f"revenue growth TTM YoY {revenue_growth_ttm_yoy_pct:.1f}% above 0%"
        if revenue_growth_ttm_yoy_pct is not None
        else "revenue growth TTM YoY unavailable",
        f"1Y beta {beta_1y:.2f} above 1" if beta_1y is not None else "1Y beta unavailable",
        f"close x 1M average volume ${monthly_dollar_volume / 1_000_000:.1f}M above $900M",
        "Leadership candidate only: review the chart and wait for a valid entry setup.",
    ]
    return OneYearWinnersSnapshot(
        matched=all(criteria.values()),
        current_price=current_price,
        market_cap=market_cap,
        one_week_return_pct=one_week_return_pct,
        one_month_return_pct=one_month_return_pct,
        one_year_return_pct=one_year_return_pct,
        revenue_growth_ttm_yoy_pct=revenue_growth_ttm_yoy_pct,
        beta_1y=beta_1y,
        avg_volume_1m=avg_volume_1m,
        monthly_dollar_volume=monthly_dollar_volume,
        ema21=ema21,
        sma50=sma50,
        ema100=ema100,
        criteria_passed=sum(criteria.values()),
        criteria_total=len(criteria),
        criteria=criteria,
        reasons=reasons,
    )


def find_one_year_winners_hit(
    frame: pd.DataFrame,
    benchmark_frame: pd.DataFrame | None,
    *,
    ticker: UniverseTicker,
    market_cap: float | None,
    revenue_growth_ttm_yoy_pct: float | None,
    signal_date: dt.date,
) -> OneYearWinnersHit | None:
    snapshot = evaluate_one_year_winners(
        frame,
        benchmark_frame,
        market_cap=market_cap,
        revenue_growth_ttm_yoy_pct=revenue_growth_ttm_yoy_pct,
    )
    if snapshot is None or not snapshot.matched:
        return None
    return OneYearWinnersHit(
        ticker=ticker.symbol,
        sector=ticker.sector,
        industry=ticker.industry,
        exchange=ticker.exchange,
        signal_date=signal_date.isoformat(),
        current_price=snapshot.current_price,
        market_cap=snapshot.market_cap,
        one_week_return_pct=snapshot.one_week_return_pct,
        one_month_return_pct=snapshot.one_month_return_pct,
        one_year_return_pct=snapshot.one_year_return_pct,
        revenue_growth_ttm_yoy_pct=snapshot.revenue_growth_ttm_yoy_pct,
        beta_1y=snapshot.beta_1y,
        avg_volume_1m=snapshot.avg_volume_1m,
        monthly_dollar_volume=snapshot.monthly_dollar_volume,
        ema21=snapshot.ema21,
        sma50=snapshot.sma50,
        ema100=snapshot.ema100,
        criteria_passed=snapshot.criteria_passed,
        criteria_total=snapshot.criteria_total,
        reasons=snapshot.reasons,
    )


def _load_benchmark_frame(config: AppConfig, run_date: dt.date, database_url: str) -> pd.DataFrame | None:
    benchmark_frame = load_ticker_window(
        config.benchmark_ticker,
        run_date,
        ONE_YEAR_WINNERS_HISTORY_DAYS,
        database_url=database_url,
    )
    if benchmark_frame is not None and db_frame_has_recent_coverage(benchmark_frame, run_date):
        return benchmark_frame
    cookstock = load_configured_cookstock(config)
    with freeze_cookstock_today(cookstock, run_date):
        financials = cookstock.cookFinancials(
            config.benchmark_ticker,
            benchmarkTicker=config.benchmark_ticker,
            historyLookbackDays=ONE_YEAR_WINNERS_HISTORY_DAYS,
        )
    fallback_frame = _build_price_frame(financials)
    return fallback_frame if not fallback_frame.empty else None


def run_one_year_winners_screen(
    config: AppConfig,
    tickers: list[UniverseTicker],
    *,
    as_of_date: dt.date | None = None,
    database_url: str | None = None,
) -> OneYearWinnersScreenResult:
    run_date = as_of_date or dt.date.today()
    resolved_database_url = resolve_database_url(database_url)
    symbols = [ticker.symbol.upper() for ticker in tickers]
    hits: list[OneYearWinnersHit] = []
    failures: list[dict[str, str]] = []

    _log(
        "starting one-year winners screen: "
        f"total={len(tickers)}, return_1y>{MIN_ONE_YEAR_RETURN_PCT:.0f}%, "
        f"market_cap>{MIN_MARKET_CAP / 1_000_000_000:.0f}B, beta>{MIN_BETA_1Y:.0f}, "
        f"dollar_volume_1m>{MIN_MONTHLY_DOLLAR_VOLUME / 1_000_000:.0f}M"
    )

    frame_map = load_many_ticker_windows(
        symbols,
        run_date,
        ONE_YEAR_WINNERS_HISTORY_DAYS,
        database_url=resolved_database_url,
    )
    metadata_map = load_ticker_metadata_map(symbols, database_url=resolved_database_url)
    fundamentals_map = RatingsRepository(resolved_database_url).load_latest_fundamentals_snapshots_for_tickers(
        symbols,
        as_of_date=run_date,
    )
    benchmark_frame = _load_benchmark_frame(config, run_date, resolved_database_url)
    fallback_tickers: list[tuple[int, UniverseTicker]] = []

    def evaluate_ticker(position: int, ticker: UniverseTicker, frame: pd.DataFrame, source: str) -> None:
        metadata = metadata_map.get(ticker.symbol.upper(), {})
        fundamentals = fundamentals_map.get(ticker.symbol.upper(), {})
        runtime_ticker = UniverseTicker(
            symbol=ticker.symbol.upper(),
            sector=ticker.sector or str(metadata.get("sector") or fundamentals.get("sector") or "") or None,
            industry=ticker.industry or str(metadata.get("industry") or fundamentals.get("industry") or "") or None,
            exchange=ticker.exchange or str(metadata.get("exchange") or "") or None,
        )
        _log(f"[{position}/{len(tickers)}] screening {runtime_ticker.symbol} from {source} | passed={len(hits)}")
        hit = find_one_year_winners_hit(
            frame,
            benchmark_frame,
            ticker=runtime_ticker,
            market_cap=fundamentals.get("market_cap"),
            revenue_growth_ttm_yoy_pct=fundamentals.get("sales_yoy_ttm_pct"),
            signal_date=run_date,
        )
        if hit is None:
            _log(f"[{position}/{len(tickers)}] {runtime_ticker.symbol} filtered: one-year winners failed | passed={len(hits)}")
            return
        hits.append(hit)
        _log(
            f"[{position}/{len(tickers)}] {runtime_ticker.symbol} passed: "
            f"return_1y={hit.one_year_return_pct:.1f}% cap={(hit.market_cap or 0.0) / 1_000_000_000:.1f}B | passed={len(hits)}"
        )

    for position, ticker in enumerate(tickers, start=1):
        frame = frame_map.get(ticker.symbol.upper())
        if frame is None or not db_frame_has_recent_coverage(frame, run_date) or len(frame) < ONE_YEAR_SESSIONS + 1:
            fallback_tickers.append((position, ticker))
            continue
        try:
            evaluate_ticker(position, ticker, frame, "DB")
        except Exception as exc:
            failures.append({"ticker": ticker.symbol, "error": str(exc)})
            _log(f"[{position}/{len(tickers)}] {ticker.symbol} error: {exc} | passed={len(hits)}")

    if fallback_tickers:
        cookstock = load_configured_cookstock(config)
        with freeze_cookstock_today(cookstock, as_of_date):
            for position, ticker in fallback_tickers:
                try:
                    financials = cookstock.cookFinancials(
                        ticker.symbol,
                        benchmarkTicker=config.benchmark_ticker,
                        historyLookbackDays=ONE_YEAR_WINNERS_HISTORY_DAYS,
                    )
                    evaluate_ticker(position, ticker, _build_price_frame(financials), "internet fallback")
                except Exception as exc:
                    failures.append({"ticker": ticker.symbol, "error": str(exc)})
                    _log(f"[{position}/{len(tickers)}] {ticker.symbol} error: {exc} | passed={len(hits)}")

    hits.sort(
        key=lambda item: (
            -item.one_year_return_pct,
            -item.one_month_return_pct,
            -(item.market_cap or 0.0),
            item.ticker,
        )
    )
    return OneYearWinnersScreenResult(
        run_date=run_date.isoformat(),
        benchmark_ticker=config.benchmark_ticker,
        total_tickers=len(tickers),
        passed_tickers=len(hits),
        failed_tickers=failures,
        hits=hits,
    )


def load_one_year_winners_universe(
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
