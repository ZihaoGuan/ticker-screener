from __future__ import annotations

from dataclasses import asdict, dataclass
import datetime as dt

from .config import AppConfig
from .cookstock_bridge import freeze_cookstock_today, iter_prefetched_cookstock_batches, load_configured_cookstock
from .high_tight_flag_setup_screen import find_high_tight_flag_setup_hit
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
    has_htf_setup: bool
    htf_setup_pivot_price: float | None
    htf_setup_distance_to_pivot_pct: float | None
    htf_setup_flag_days: int | None
    htf_setup_pole_gain_ratio: float | None
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
    has_htf_setup = bool(summary.get("has_htf_setup"))
    htf_setup_pivot_price = summary.get("htf_setup_pivot_price")
    htf_setup_distance_to_pivot_pct = summary.get("htf_setup_distance_to_pivot_pct")
    htf_setup_flag_days = summary.get("htf_setup_flag_days")
    htf_setup_pole_gain_ratio = summary.get("htf_setup_pole_gain_ratio")
    reasons = [
        f"{runup_pct:.1f}% runup in {int(summary['window_days'])} sessions",
        f"{pullback_pct:.1f}% off the runup high",
        f"holding above 21 EMA ({ema_21:.2f})",
        "monitor for HTF setup",
    ]
    if has_htf_setup:
        distance_text = ""
        if htf_setup_distance_to_pivot_pct is not None:
            distance_text = f" {float(htf_setup_distance_to_pivot_pct) * 100.0:.1f}% below pivot"
        reasons.append(f"current HTF setup detected{distance_text}")
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
        has_htf_setup=has_htf_setup,
        htf_setup_pivot_price=float(htf_setup_pivot_price) if htf_setup_pivot_price is not None else None,
        htf_setup_distance_to_pivot_pct=float(htf_setup_distance_to_pivot_pct)
        if htf_setup_distance_to_pivot_pct is not None
        else None,
        htf_setup_flag_days=int(htf_setup_flag_days) if htf_setup_flag_days is not None else None,
        htf_setup_pole_gain_ratio=float(htf_setup_pole_gain_ratio) if htf_setup_pole_gain_ratio is not None else None,
        reasons=reasons,
    )


def run_htf_runup_screen(
    config: AppConfig,
    tickers: list[UniverseTicker],
    *,
    as_of_date: dt.date | None = None,
) -> HtfRunupScreenResult:
    cookstock = load_configured_cookstock(config)
    hits: list[HtfRunupHit] = []
    failures: list[dict[str, str]] = []
    history_days = max(int(config.htf_history_days), int(config.htf_runup_window_days), 90)
    min_runup_pct = float(config.htf_min_runup_pct)
    total_tickers = len(tickers)
    run_date = as_of_date or dt.date.today()

    print(
        "starting 8W 100% runup screen: "
        f"total={total_tickers}, "
        f"window={config.htf_runup_window_days}, "
        f"min_runup={min_runup_pct:.1f}%, "
        "require_price_above_ema21=true"
    )

    with freeze_cookstock_today(cookstock, as_of_date):
        position = 0
        for ticker_batch in iter_prefetched_cookstock_batches(
            config,
            tickers,
            as_of_date=as_of_date,
            history_lookback_days=history_days,
            benchmark_ticker=config.benchmark_ticker,
        ):
            for ticker in ticker_batch:
                position += 1
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
                    frame = financials._get_clean_price_data()
                    setup_frame = None
                    if frame:
                        import pandas as pd

                        setup_frame = pd.DataFrame(
                            {
                                "Date": pd.to_datetime([row.get("formatted_date") for row in frame]),
                                "Open": [row.get("open") for row in frame],
                                "High": [row.get("high") for row in frame],
                                "Low": [row.get("low") for row in frame],
                                "Close": [row.get("close") for row in frame],
                                "Volume": [row.get("volume") for row in frame],
                            }
                        ).dropna(subset=["Date", "Open", "High", "Low", "Close", "Volume"]).set_index("Date").sort_index()
                    setup_hit = find_high_tight_flag_setup_hit(setup_frame, ticker=ticker) if setup_frame is not None else None
                    runup_summary["has_htf_setup"] = setup_hit is not None
                    runup_summary["htf_setup_pivot_price"] = setup_hit.pivot_price if setup_hit is not None else None
                    runup_summary["htf_setup_distance_to_pivot_pct"] = (
                        setup_hit.distance_to_pivot_pct if setup_hit is not None else None
                    )
                    runup_summary["htf_setup_flag_days"] = setup_hit.flag_days if setup_hit is not None else None
                    runup_summary["htf_setup_pole_gain_ratio"] = setup_hit.pole_gain_ratio if setup_hit is not None else None
                    runup_summary["ema_21"] = float(ema_21)
                    runup_summary["price_above_ema21"] = True
                    hits.append(_to_hit(ticker, config.benchmark_ticker, runup_summary))
                    latest_hit = hits[-1]
                    htf_suffix = ""
                    if latest_hit.has_htf_setup and latest_hit.htf_setup_distance_to_pivot_pct is not None:
                        htf_suffix = (
                            f" | HTF setup pivot gap {latest_hit.htf_setup_distance_to_pivot_pct * 100.0:.1f}%"
                        )
                    print(
                        f"[{position}/{total_tickers}] {ticker.symbol} passed: "
                        f"runup {latest_hit.runup_pct:.1f}% pullback {latest_hit.pullback_from_high_pct:.1f}% "
                        f"above 21 EMA {latest_hit.ema_21:.2f}{htf_suffix} | passed={len(hits)}"
                    )
                except Exception as exc:
                    failures.append({"ticker": ticker.symbol, "error": str(exc)})
                    print(f"[{position}/{total_tickers}] {ticker.symbol} error: {exc} | passed={len(hits)}")

    hits.sort(
        key=lambda hit: (
            0 if hit.has_htf_setup else 1,
            hit.htf_setup_distance_to_pivot_pct if hit.htf_setup_distance_to_pivot_pct is not None else 999.0,
            -(hit.htf_setup_pole_gain_ratio if hit.htf_setup_pole_gain_ratio is not None else -1.0),
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
        run_date=run_date.isoformat(),
        benchmark_ticker=config.benchmark_ticker,
        total_tickers=total_tickers,
        passed_tickers=len(hits),
        failed_tickers=failures,
        hits=hits,
    )
