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
RS_PHASE_NEW_HIGH_LOOKBACK = 50
RS_PHASE_NEW_MAX_DAYS = 3
RS_PHASE_ESTABLISHED_MAX_DAYS = 20
RS_PHASE_QUICK_RECLAIM_MAX_BELOW_DAYS = 3
RS_PHASE_CONFIRMED_LOSS_MIN_DAYS = 2


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
    rs_phase_state: str
    rs_phase_badge_label: str
    rs_phase_quick_reclaim: bool
    rs_phase_below_days_before_reclaim: int | None
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


def _trailing_run_length(values: list[bool], target: bool) -> int:
    count = 0
    for value in reversed(values):
        if value is not target:
            break
        count += 1
    return count


def _rs_phase_badge_label(state: str, active_days: int) -> str:
    if state == "quick_reclaim":
        return "RS Quick Reclaim"
    if state == "new":
        return f"RS New · {active_days}D"
    if state == "established":
        return f"RS Established · {active_days}D"
    if state == "mature":
        return f"RS Mature · {active_days}D"
    if state == "lost":
        return "RS Lost"
    return "RS Inactive"


def classify_rs_phase_lifecycle(phase_active: pd.Series) -> dict[str, object]:
    """Classify the latest RS-line/EMA21 relationship without treating a one-day dip as a loss."""
    values = [bool(value) for value in phase_active.dropna().tolist()]
    if not values:
        return {
            "rs_phase_state": "inactive",
            "rs_phase_badge_label": "RS Inactive",
            "rs_phase_active_days": 0,
            "rs_phase_inactive_days": 0,
            "rs_phase_quick_reclaim": False,
            "rs_phase_below_days_before_reclaim": None,
            "rs_phase_prior_active_days": None,
            "rs_phase_new_reclaim": False,
            "rs_phase_recent_reclaim_days_ago": None,
            "rs_phase_lost": False,
            "rs_phase_loss_confirmed": False,
            "rs_phase_recent_loss_days_ago": None,
        }

    active_days = _trailing_run_length(values, True)
    inactive_days = _trailing_run_length(values, False)
    reclaim_positions = [index for index in range(1, len(values)) if values[index] and not values[index - 1]]
    loss_positions = [index for index in range(1, len(values)) if not values[index] and values[index -1]]
    latest_reclaim = reclaim_positions[-1] if reclaim_positions else None
    latest_loss = loss_positions[-1] if loss_positions else None
    recent_reclaim_days_ago = len(values) - latest_reclaim - 1 if latest_reclaim is not None else None
    recent_loss_days_ago = len(values) - latest_loss - 1 if latest_loss is not None else None

    below_days_before_reclaim = None
    prior_active_days = None
    quick_reclaim = False
    if values[-1] and latest_reclaim is not None:
        before_reclaim = values[:latest_reclaim]
        below_days_before_reclaim = _trailing_run_length(before_reclaim, False)
        before_dip = before_reclaim[: len(before_reclaim) - below_days_before_reclaim]
        prior_active_days = _trailing_run_length(before_dip, True)
        quick_reclaim = (
            active_days <= RS_PHASE_NEW_MAX_DAYS
            and 1 <= below_days_before_reclaim <= RS_PHASE_QUICK_RECLAIM_MAX_BELOW_DAYS
            and prior_active_days >= RS_PHASE_MIN_ACTIVE_DAYS
        )

    loss_confirmed = bool(
        not values[-1]
        and inactive_days >= RS_PHASE_CONFIRMED_LOSS_MIN_DAYS
        and _trailing_run_length(values[: len(values) - inactive_days], True) >= RS_PHASE_MIN_ACTIVE_DAYS
    )
    if values[-1]:
        state = "quick_reclaim" if quick_reclaim else (
            "new" if active_days <= RS_PHASE_NEW_MAX_DAYS else "established" if active_days <= RS_PHASE_ESTABLISHED_MAX_DAYS else "mature"
        )
    else:
        state = "lost" if loss_confirmed else "inactive"

    return {
        "rs_phase_state": state,
        "rs_phase_badge_label": _rs_phase_badge_label(state, active_days),
        "rs_phase_active_days": active_days,
        "rs_phase_inactive_days": inactive_days,
        "rs_phase_quick_reclaim": quick_reclaim,
        "rs_phase_below_days_before_reclaim": below_days_before_reclaim,
        "rs_phase_prior_active_days": prior_active_days,
        "rs_phase_new_reclaim": bool(values[-1] and latest_reclaim == len(values) - 1),
        "rs_phase_recent_reclaim_days_ago": recent_reclaim_days_ago,
        "rs_phase_lost": bool(not values[-1] and latest_loss == len(values) - 1),
        "rs_phase_loss_confirmed": loss_confirmed,
        "rs_phase_recent_loss_days_ago": recent_loss_days_ago,
    }


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
    lifecycle = classify_rs_phase_lifecycle(phase_active)

    rs_new_high, rs_new_high_before_price = _compute_rs_new_high_flags(
        rs_line,
        aligned["Close"],
        lookback=max(1, int(new_high_lookback)),
    )
    latest_index = aligned.index[-1]
    rolling_rs_high = rs_line.rolling(window=max(1, int(new_high_lookback)), min_periods=1).max().shift(1)
    rolling_price_high = aligned["Close"].rolling(window=max(1, int(new_high_lookback)), min_periods=1).max().shift(1)

    return {
        "signal_date": pd.Timestamp(latest_index).date().isoformat(),
        "current_price": float(aligned["Close"].iloc[-1]),
        "current_high": float(aligned["High"].iloc[-1]),
        "current_rs_line": float(rs_line.iloc[-1]),
        "current_rs_ema21": float(rs_ema.iloc[-1]),
        "rs_phase_active": bool(phase_active.iloc[-1]),
        **lifecycle,
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
    reasons.append(f"RS lifecycle: {str(context['rs_phase_badge_label'])}")
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
        rs_phase_state=str(context["rs_phase_state"]),
        rs_phase_badge_label=str(context["rs_phase_badge_label"]),
        rs_phase_quick_reclaim=bool(context["rs_phase_quick_reclaim"]),
        rs_phase_below_days_before_reclaim=context["rs_phase_below_days_before_reclaim"],
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
