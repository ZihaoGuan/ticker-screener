from __future__ import annotations

from dataclasses import asdict, dataclass
import datetime as dt

from .config import AppConfig
from .cookstock_bridge import load_configured_cookstock


@dataclass(frozen=True)
class EarningsEvent:
    ticker: str
    earnings_date: str | None = None
    summary: str | None = None
    sector: str | None = None
    exchange: str | None = None


@dataclass(frozen=True)
class PegHit:
    ticker: str
    earnings_date: str | None
    earnings_summary: str | None
    sector: str | None
    exchange: str | None
    benchmark_ticker: str
    setup_type: str
    peg_date: str
    peg_open: float
    peg_high: float
    peg_low: float
    peg_close: float
    previous_close: float
    gap_pct: float
    open_gap_pct: float
    volume_ratio: float
    close_position_ratio: float
    entry_distance_pct: float
    current_price: float
    hvc: float
    hvc5: float
    gdh: float
    gdl: float
    earnings_actual_eps: float | None
    earnings_estimated_eps: float | None
    earnings_surprise_pct: float | None
    primary_entry_label: str | None
    primary_entry: float | None
    secondary_entry_label: str | None
    secondary_entry_fast_ema_length: int | None
    secondary_entry_slow_ema_length: int | None
    secondary_entry_fast_ema: float | None
    secondary_entry_slow_ema: float | None
    secondary_entry_low: float | None
    secondary_entry_high: float | None
    distribution_warning: bool
    distribution_days_count: int
    latest_distribution_date: str | None
    latest_distribution_volume_ratio: float | None
    distribution_volume_ratio_threshold: float | None

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True)
class PegScreenResult:
    run_date: str
    benchmark_ticker: str
    total_tickers: int
    passed_tickers: int
    failed_tickers: list[dict[str, str]]
    hits: list[PegHit]

    def to_dict(self) -> dict[str, object]:
        return {
            "run_date": self.run_date,
            "benchmark_ticker": self.benchmark_ticker,
            "total_tickers": self.total_tickers,
            "passed_tickers": self.passed_tickers,
            "failed_tickers": self.failed_tickers,
            "hits": [item.to_dict() for item in self.hits],
        }


def _optional_float(value: object) -> float | None:
    if value in (None, "", "NA", "n/a"):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _optional_int(value: object) -> int | None:
    if value in (None, "", "NA", "n/a"):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _to_hit(event: EarningsEvent, benchmark_ticker: str, peg_setup: dict[str, object], trade_plan: dict[str, object] | None) -> PegHit:
    return PegHit(
        ticker=event.ticker,
        earnings_date=event.earnings_date,
        earnings_summary=event.summary,
        sector=event.sector,
        exchange=event.exchange,
        benchmark_ticker=benchmark_ticker,
        setup_type=str(peg_setup["setup_type"]),
        peg_date=str(peg_setup["peg_date"]),
        peg_open=float(peg_setup["peg_open"]),
        peg_high=float(peg_setup["peg_high"]),
        peg_low=float(peg_setup["peg_low"]),
        peg_close=float(peg_setup["peg_close"]),
        previous_close=float(peg_setup["previous_close"]),
        gap_pct=float(peg_setup["gap_pct"]),
        open_gap_pct=float(peg_setup["open_gap_pct"]),
        volume_ratio=float(peg_setup["volume_ratio"]),
        close_position_ratio=float(peg_setup["close_position_ratio"]),
        entry_distance_pct=float(peg_setup["entry_distance_pct"]),
        current_price=float(peg_setup["current_price"]),
        hvc=float(peg_setup["hvc"]),
        hvc5=float(peg_setup["hvc5"]),
        gdh=float(peg_setup["gdh"]),
        gdl=float(peg_setup["gdl"]),
        earnings_actual_eps=_optional_float(peg_setup.get("earnings_actual_eps")),
        earnings_estimated_eps=_optional_float(peg_setup.get("earnings_estimated_eps")),
        earnings_surprise_pct=_optional_float(peg_setup.get("earnings_surprise_pct")),
        primary_entry_label=str(trade_plan["primary_entry_label"]) if trade_plan and trade_plan.get("primary_entry_label") is not None else None,
        primary_entry=_optional_float(trade_plan.get("primary_entry")) if trade_plan else None,
        secondary_entry_label=str(trade_plan["secondary_entry_label"]) if trade_plan and trade_plan.get("secondary_entry_label") is not None else None,
        secondary_entry_fast_ema_length=_optional_int(trade_plan.get("secondary_entry_fast_ema_length")) if trade_plan else None,
        secondary_entry_slow_ema_length=_optional_int(trade_plan.get("secondary_entry_slow_ema_length")) if trade_plan else None,
        secondary_entry_fast_ema=_optional_float(trade_plan.get("secondary_entry_fast_ema")) if trade_plan else None,
        secondary_entry_slow_ema=_optional_float(trade_plan.get("secondary_entry_slow_ema")) if trade_plan else None,
        secondary_entry_low=_optional_float(trade_plan.get("secondary_entry_low")) if trade_plan else None,
        secondary_entry_high=_optional_float(trade_plan.get("secondary_entry_high")) if trade_plan else None,
        distribution_warning=bool(trade_plan.get("distribution_warning")) if trade_plan else False,
        distribution_days_count=int(trade_plan.get("distribution_days_count", 0)) if trade_plan else 0,
        latest_distribution_date=str(trade_plan["latest_distribution_date"]) if trade_plan and trade_plan.get("latest_distribution_date") else None,
        latest_distribution_volume_ratio=_optional_float(trade_plan.get("latest_distribution_volume_ratio")) if trade_plan else None,
        distribution_volume_ratio_threshold=_optional_float(trade_plan.get("distribution_volume_ratio_threshold")) if trade_plan else None,
    )


def run_peg_screen(config: AppConfig, earnings_events: list[EarningsEvent]) -> PegScreenResult:
    cookstock = load_configured_cookstock(config)
    hits: list[PegHit] = []
    failures: list[dict[str, str]] = []

    for position, event in enumerate(earnings_events, start=1):
        print(f"[{position}/{len(earnings_events)}] screening {event.ticker}")
        try:
            financials = cookstock.cookFinancials(
                event.ticker,
                benchmarkTicker=config.benchmark_ticker,
            )
            peg_setup = financials.find_recent_power_earnings_gap()
            if not peg_setup:
                continue
            trade_plan = financials.get_peg_trade_plan(peg_setup)
            hits.append(_to_hit(event, config.benchmark_ticker, peg_setup, trade_plan))
        except Exception as exc:
            failures.append({"ticker": event.ticker, "error": str(exc)})
            print(f"screening failed for {event.ticker}: {exc}")

    return PegScreenResult(
        run_date=dt.date.today().isoformat(),
        benchmark_ticker=config.benchmark_ticker,
        total_tickers=len(earnings_events),
        passed_tickers=len(hits),
        failed_tickers=failures,
        hits=hits,
    )
