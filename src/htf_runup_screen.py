from __future__ import annotations

from dataclasses import asdict, dataclass
import datetime as dt

from .config import AppConfig
from .cookstock_bridge import load_configured_cookstock
from .universe import UniverseTicker


@dataclass(frozen=True)
class HtfRunupHit:
    ticker: str
    sector: str | None
    exchange: str | None
    benchmark_ticker: str
    current_price: float
    ema_21: float
    price_above_ema21: bool
    runup_window_days: int
    runup_pct: float
    pullback_from_high_pct: float
    runup_low: float
    runup_high: float
    runup_low_date: str
    runup_high_date: str
    reasons: list[str]

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True)
class HtfRunupScreenResult:
    run_date: str
    benchmark_ticker: str
    total_tickers: int
    passed_tickers: int
    failed_tickers: list[dict[str, str]]
    hits: list[HtfRunupHit]

    def to_dict(self) -> dict[str, object]:
        return {
            "run_date": self.run_date,
            "benchmark_ticker": self.benchmark_ticker,
            "total_tickers": self.total_tickers,
            "passed_tickers": self.passed_tickers,
            "failed_tickers": self.failed_tickers,
            "hits": [item.to_dict() for item in self.hits],
        }


def _to_hit(ticker: UniverseTicker, benchmark_ticker: str, summary: dict[str, object]) -> HtfRunupHit:
    runup_pct = float(summary["runup_pct"])
    pullback_pct = float(summary["pullback_from_high_pct"])
    current_price = float(summary["current_price"])
    ema_21 = float(summary["ema_21"])
    reasons = [
        f"{runup_pct:.1f}% runup in {int(summary['window_days'])} sessions",
        f"{pullback_pct:.1f}% off the runup high",
        f"holding above 21 EMA ({ema_21:.2f})",
        "monitor for HTF setup",
    ]
    return HtfRunupHit(
        ticker=ticker.symbol,
        sector=ticker.sector,
        exchange=ticker.exchange,
        benchmark_ticker=benchmark_ticker,
        current_price=current_price,
        ema_21=ema_21,
        price_above_ema21=bool(summary["price_above_ema21"]),
        runup_window_days=int(summary["window_days"]),
        runup_pct=runup_pct,
        pullback_from_high_pct=pullback_pct,
        runup_low=float(summary["runup_low"]),
        runup_high=float(summary["runup_high"]),
        runup_low_date=str(summary.get("runup_low_date") or "NA"),
        runup_high_date=str(summary.get("runup_high_date") or "NA"),
        reasons=reasons,
    )


def run_htf_runup_screen(config: AppConfig, tickers: list[UniverseTicker]) -> HtfRunupScreenResult:
    cookstock = load_configured_cookstock(config)
    hits: list[HtfRunupHit] = []
    failures: list[dict[str, str]] = []
    history_days = max(int(config.htf_history_days), int(config.htf_runup_window_days), 90)
    min_runup_pct = float(config.htf_min_runup_pct)
    total_tickers = len(tickers)

    print(
        "starting 8W 100% runup screen: "
        f"total={total_tickers}, "
        f"window={config.htf_runup_window_days}, "
        f"min_runup={min_runup_pct:.1f}%, "
        "require_price_above_ema21=true"
    )

    for position, ticker in enumerate(tickers, start=1):
        print(f"[{position}/{total_tickers}] screening {ticker.symbol} | passed={len(hits)}")
        try:
            financials = cookstock.cookFinancials(
                ticker.symbol,
                benchmarkTicker=config.benchmark_ticker,
                historyLookbackDays=history_days,
            )
            runup_summary = financials._get_htf_runup_summary(config.htf_runup_window_days)
            if not runup_summary:
                print(f"[{position}/{total_tickers}] {ticker.symbol} filtered: no 8W runup summary | passed={len(hits)}")
                continue
            ema_21 = financials._get_latest_ema_value(21)
            current_price = float(runup_summary["current_price"])
            if ema_21 is None:
                print(f"[{position}/{total_tickers}] {ticker.symbol} filtered: missing 21 EMA | passed={len(hits)}")
                continue
            if current_price <= float(ema_21):
                print(
                    f"[{position}/{total_tickers}] {ticker.symbol} filtered: "
                    f"current {current_price:.2f} <= 21 EMA {float(ema_21):.2f} | passed={len(hits)}"
                )
                continue
            runup_pct = float(runup_summary["runup_pct"])
            if runup_pct < min_runup_pct:
                print(
                    f"[{position}/{total_tickers}] {ticker.symbol} filtered: "
                    f"runup {runup_pct:.1f}% < {min_runup_pct:.1f}% | passed={len(hits)}"
                )
                continue
            runup_summary["ema_21"] = float(ema_21)
            runup_summary["price_above_ema21"] = True
            hits.append(_to_hit(ticker, config.benchmark_ticker, runup_summary))
            latest_hit = hits[-1]
            print(
                f"[{position}/{total_tickers}] {ticker.symbol} passed: "
                f"runup {latest_hit.runup_pct:.1f}% pullback {latest_hit.pullback_from_high_pct:.1f}% "
                f"above 21 EMA {latest_hit.ema_21:.2f} | passed={len(hits)}"
            )
        except Exception as exc:
            failures.append({"ticker": ticker.symbol, "error": str(exc)})
            print(f"[{position}/{total_tickers}] {ticker.symbol} error: {exc} | passed={len(hits)}")

    hits.sort(
        key=lambda hit: (
            -hit.runup_pct,
            hit.pullback_from_high_pct,
            hit.ticker,
        )
    )

    print(
        "finished 8W 100% runup screen: "
        f"passed={len(hits)}, failed={len(failures)}, total={total_tickers}"
    )

    return HtfRunupScreenResult(
        run_date=dt.date.today().isoformat(),
        benchmark_ticker=config.benchmark_ticker,
        total_tickers=total_tickers,
        passed_tickers=len(hits),
        failed_tickers=failures,
        hits=hits,
    )
