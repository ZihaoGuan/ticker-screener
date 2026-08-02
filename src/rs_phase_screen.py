from __future__ import annotations

from dataclasses import asdict, dataclass
import datetime as dt

import pandas as pd

from .config import AppConfig
from .cookstock_bridge import freeze_cookstock_today, iter_prefetched_cookstock_batches, load_configured_cookstock
from .rs_screen import _compute_latest_rs_rating, _compute_rs_new_high_flags
from .universe import UniverseTicker


RS_PHASE_HISTORY_DAYS = 320
RS_PHASE_EMA_PERIOD = 21
RS_PHASE_MIN_ACTIVE_DAYS = 3
RS_PHASE_NEW_HIGH_LOOKBACK = 250


@dataclass(frozen=True)
class RsPhaseHit:
    ticker: str
    sector: str | None
    industry: str | None
    exchange: str | None
    signal_date: str
    benchmark_ticker: str
    current_price: float
    current_high: float
    current_rs_line: float
    current_rs_ema21: float
    rs_phase_active_days: int
    rs_phase_new_reclaim: bool
    rs_phase_recent_reclaim_days_ago: int | None
    daily_rs_new_high: bool
    daily_rs_new_high_before_price: bool
    daily_price_high: float
    daily_rs_line_high: float
    rs_score: float
    rs_rating: float
    reasons: list[str]

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True)
class RsPhaseScreenResult:
    run_date: str
    benchmark_ticker: str
    total_tickers: int
    passed_tickers: int
    failed_tickers: list[dict[str, str]]
    hits: list[RsPhaseHit]

    def to_dict(self) -> dict[str, object]:
        return {
            "run_date": self.run_date,
            "benchmark_ticker": self.benchmark_ticker,
            "total_tickers": self.total_tickers,
            "passed_tickers": self.passed_tickers,
            "failed_tickers": self.failed_tickers,
            "hits": [item.to_dict() for item in self.hits],
        }


def _build_price_frame_from_rows(rows: list[dict[str, object]]) -> pd.DataFrame:
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
    return frame.dropna(subset=["Date", "High", "Close"]).set_index("Date").sort_index()


def _build_close_frame_from_rows(rows: list[dict[str, object]]) -> pd.DataFrame:
    if not rows:
        return pd.DataFrame()
    frame = pd.DataFrame(
        {
            "Date": pd.to_datetime([row.get("formatted_date") for row in rows]),
            "Close": [row.get("close") for row in rows],
        }
    )
    return frame.dropna(subset=["Date", "Close"]).set_index("Date").sort_index()


def compute_rs_phase_context(
    stock_frame: pd.DataFrame,
    benchmark_frame: pd.DataFrame,
    *,
    ema_period: int = RS_PHASE_EMA_PERIOD,
    new_high_lookback: int = RS_PHASE_NEW_HIGH_LOOKBACK,
) -> dict[str, object] | None:
    if stock_frame.empty or benchmark_frame.empty:
        return None
    stock = stock_frame.copy().sort_index()
    benchmark = benchmark_frame.copy().sort_index()
    if "Close" not in stock.columns or "Close" not in benchmark.columns or "High" not in stock.columns:
        return None
    aligned = stock[["Close", "High"]].join(benchmark[["Close"]].rename(columns={"Close": "BenchmarkClose"}), how="inner").dropna()
    if len(aligned) < max(2, int(ema_period)):
        return None

    rs_line = aligned["Close"] / aligned["BenchmarkClose"]
    rs_ema = rs_line.ewm(span=max(1, int(ema_period)), adjust=False).mean()
    phase_active = rs_line > rs_ema
    reclaim = phase_active & ~phase_active.shift(1, fill_value=False)
    loss = ~phase_active & phase_active.shift(1, fill_value=False)
    active_days = 0
    for value in reversed([bool(item) for item in phase_active.tolist()]):
        if not value:
            break
        active_days += 1

    recent_reclaim_days_ago = None
    if bool(reclaim.any()):
        recent_reclaim_index = reclaim[reclaim].index[-1]
        recent_reclaim_days_ago = len(reclaim.loc[recent_reclaim_index:]) - 1

    rs_new_high, rs_new_high_before_price = _compute_rs_new_high_flags(
        rs_line,
        aligned["High"],
        lookback=max(1, int(new_high_lookback)),
    )
    latest_index = aligned.index[-1]
    rolling_rs_high = rs_line.rolling(window=max(1, int(new_high_lookback)), min_periods=1).max()
    rolling_price_high = aligned["High"].rolling(window=max(1, int(new_high_lookback)), min_periods=1).max()

    return {
        "signal_date": pd.Timestamp(latest_index).date().isoformat(),
        "current_price": float(aligned["Close"].iloc[-1]),
        "current_high": float(aligned["High"].iloc[-1]),
        "current_rs_line": float(rs_line.iloc[-1]),
        "current_rs_ema21": float(rs_ema.iloc[-1]),
        "rs_phase_active": bool(phase_active.iloc[-1]),
        "rs_phase_active_days": int(active_days),
        "rs_phase_new_reclaim": bool(reclaim.iloc[-1]),
        "rs_phase_recent_reclaim_days_ago": int(recent_reclaim_days_ago) if recent_reclaim_days_ago is not None else None,
        "rs_phase_lost": bool(loss.iloc[-1]),
        "daily_rs_new_high": bool(rs_new_high.loc[latest_index]),
        "daily_rs_new_high_before_price": bool(rs_new_high_before_price.loc[latest_index]),
        "daily_price_high": float(rolling_price_high.iloc[-1]),
        "daily_rs_line_high": float(rolling_rs_high.iloc[-1]),
    }


def find_recent_rs_phase_hit(
    stock_frame: pd.DataFrame,
    benchmark_frame: pd.DataFrame,
    *,
    ticker: UniverseTicker,
    benchmark_ticker: str,
    min_active_days: int = RS_PHASE_MIN_ACTIVE_DAYS,
) -> RsPhaseHit | None:
    context = compute_rs_phase_context(stock_frame, benchmark_frame)
    if context is None:
        return None
    if not bool(context["rs_phase_active"]):
        return None
    if int(context["rs_phase_active_days"]) < int(min_active_days):
        return None

    stock_rows = [
        {"formatted_date": pd.Timestamp(index).date().isoformat(), "close": row["Close"]}
        for index, row in stock_frame.iterrows()
        if pd.notna(row.get("Close"))
    ]
    benchmark_rows = [
        {"formatted_date": pd.Timestamp(index).date().isoformat(), "close": row["Close"]}
        for index, row in benchmark_frame.iterrows()
        if pd.notna(row.get("Close"))
    ]
    rs_metrics = _compute_latest_rs_rating(stock_rows, benchmark_rows)
    if rs_metrics is None:
        return None
    rs_score, rs_rating = rs_metrics

    reasons = [
        f"RS line above 21 EMA for {int(context['rs_phase_active_days'])} session(s)",
        f"RS line {float(context['current_rs_line']):.6f} > RS EMA21 {float(context['current_rs_ema21']):.6f}",
        f"RS rating {float(rs_rating):.1f}",
    ]
    if bool(context["rs_phase_new_reclaim"]):
        reasons.append("RS phase reclaimed today")
    elif context["rs_phase_recent_reclaim_days_ago"] is not None:
        reasons.append(f"RS phase reclaimed {int(context['rs_phase_recent_reclaim_days_ago'])} session(s) ago")
    if bool(context["daily_rs_new_high_before_price"]):
        reasons.append("RS new high before price")
    elif bool(context["daily_rs_new_high"]):
        reasons.append("RS new high")

    return RsPhaseHit(
        ticker=ticker.symbol,
        sector=ticker.sector,
        industry=ticker.industry,
        exchange=ticker.exchange,
        signal_date=str(context["signal_date"]),
        benchmark_ticker=benchmark_ticker,
        current_price=float(context["current_price"]),
        current_high=float(context["current_high"]),
        current_rs_line=float(context["current_rs_line"]),
        current_rs_ema21=float(context["current_rs_ema21"]),
        rs_phase_active_days=int(context["rs_phase_active_days"]),
        rs_phase_new_reclaim=bool(context["rs_phase_new_reclaim"]),
        rs_phase_recent_reclaim_days_ago=context["rs_phase_recent_reclaim_days_ago"],
        daily_rs_new_high=bool(context["daily_rs_new_high"]),
        daily_rs_new_high_before_price=bool(context["daily_rs_new_high_before_price"]),
        daily_price_high=float(context["daily_price_high"]),
        daily_rs_line_high=float(context["daily_rs_line_high"]),
        rs_score=float(rs_score),
        rs_rating=float(rs_rating),
        reasons=reasons,
    )


def run_rs_phase_screen(
    config: AppConfig,
    tickers: list[UniverseTicker],
    *,
    as_of_date: dt.date | None = None,
    min_active_days: int = RS_PHASE_MIN_ACTIVE_DAYS,
) -> RsPhaseScreenResult:
    cookstock = load_configured_cookstock(config)
    hits: list[RsPhaseHit] = []
    failures: list[dict[str, str]] = []
    run_date = as_of_date or dt.date.today()
    total_tickers = len(tickers)

    with freeze_cookstock_today(cookstock, as_of_date):
        position = 0
        for ticker_batch in iter_prefetched_cookstock_batches(
            config,
            tickers,
            as_of_date=as_of_date,
            history_lookback_days=RS_PHASE_HISTORY_DAYS,
            benchmark_ticker=config.benchmark_ticker,
        ):
            for ticker in ticker_batch:
                position += 1
                print(f"[{position}/{total_tickers}] screening {ticker.symbol} | passed={len(hits)}")
                try:
                    financials = cookstock.cookFinancials(
                        ticker.symbol,
                        benchmarkTicker=config.benchmark_ticker,
                        historyLookbackDays=RS_PHASE_HISTORY_DAYS,
                    )
                    stock_rows = [item for item in financials._get_clean_price_data() if isinstance(item, dict)]
                    benchmark_rows = [
                        item
                        for item in financials._get_benchmark_price_data(config.benchmark_ticker)
                        if isinstance(item, dict)
                    ]
                    hit = find_recent_rs_phase_hit(
                        _build_price_frame_from_rows(stock_rows),
                        _build_close_frame_from_rows(benchmark_rows),
                        ticker=ticker,
                        benchmark_ticker=config.benchmark_ticker,
                        min_active_days=min_active_days,
                    )
                    if hit is not None:
                        hits.append(hit)
                except Exception as exc:
                    failures.append({"ticker": ticker.symbol, "error": str(exc)})
                    print(f"[{position}/{total_tickers}] {ticker.symbol} error: {exc} | passed={len(hits)}")

    hits.sort(key=lambda item: (-item.rs_phase_active_days, -item.rs_rating, item.ticker))
    print(f"screen complete: passed={len(hits)}, failed={len(failures)}, total={total_tickers}")
    return RsPhaseScreenResult(
        run_date=run_date.isoformat(),
        benchmark_ticker=config.benchmark_ticker,
        total_tickers=total_tickers,
        passed_tickers=len(hits),
        failed_tickers=failures,
        hits=hits,
    )
