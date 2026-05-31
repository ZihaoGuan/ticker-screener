from __future__ import annotations

from dataclasses import asdict, dataclass
import datetime as dt

import pandas as pd

from .config import AppConfig
from .cookstock_bridge import freeze_cookstock_today, iter_prefetched_cookstock_batches, load_configured_cookstock
from .universe import UniverseTicker


@dataclass(frozen=True)
class WeeklyHtfPullbackHit:
    ticker: str
    sector: str | None
    exchange: str | None
    benchmark_ticker: str
    current_price: float
    weekly_ema8: float
    weekly_ema8_distance_pct: float
    weekly_ema8_distance_abs_pct: float
    is_above_weekly_ema8: bool
    weekly_rs_new_high: bool
    weekly_rs_new_high_recent: bool
    weekly_signal_weeks_ago: int | None
    weekly_recent_signal_weeks: int
    current_rs_line: float
    rs_line_high: float
    htf_score: float
    htf_grade: str
    htf_trade_plan: str
    htf_runup_pct: float
    htf_pullback_from_high_pct: float
    htf_runup_low: float
    htf_runup_high: float
    htf_runup_low_date: str
    htf_runup_high_date: str
    year_high: float
    distance_from_year_high_pct: float
    is_near_year_high: bool
    is_strong_rs: bool
    is_sector_etf_strong: bool
    sector_etf: str
    reasons: list[str]

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True)
class WeeklyHtfPullbackScreenResult:
    run_date: str
    benchmark_ticker: str
    total_tickers: int
    passed_tickers: int
    failed_tickers: list[dict[str, str]]
    hits: list[WeeklyHtfPullbackHit]

    def to_dict(self) -> dict[str, object]:
        return {
            "run_date": self.run_date,
            "benchmark_ticker": self.benchmark_ticker,
            "total_tickers": self.total_tickers,
            "passed_tickers": self.passed_tickers,
            "failed_tickers": self.failed_tickers,
            "hits": [item.to_dict() for item in self.hits],
        }


def _build_price_frame(financials) -> pd.DataFrame:
    rows = financials._get_clean_price_data()
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
    frame = frame.dropna(subset=["Date", "Close"]).set_index("Date").sort_index()
    return frame


def _latest_weekly_snapshot(financials) -> dict[str, float] | None:
    frame = _build_price_frame(financials)
    if frame.empty:
        return None
    weekly = frame.resample("W-FRI").agg(
        {
            "Open": "first",
            "High": "max",
            "Low": "min",
            "Close": "last",
            "Volume": "sum",
        }
    ).dropna(subset=["Open", "High", "Low", "Close"])
    if len(weekly) < 8:
        return None
    weekly["ema8"] = weekly["Close"].ewm(span=8, adjust=False).mean()
    latest = weekly.iloc[-1]
    ema8 = float(latest["ema8"])
    if ema8 <= 0:
        return None
    current_price = float(frame["Close"].iloc[-1])
    distance_ratio = (current_price - ema8) / ema8
    return {
        "weekly_ema8": ema8,
        "current_price": current_price,
        "weekly_ema8_distance_pct": distance_ratio * 100.0,
        "weekly_ema8_distance_abs_pct": abs(distance_ratio) * 100.0,
        "is_above_weekly_ema8": current_price >= ema8,
    }


def _build_hit(
    ticker: UniverseTicker,
    rs_summary: dict[str, object],
    htf_summary: dict[str, object],
    weekly_snapshot: dict[str, float],
) -> WeeklyHtfPullbackHit:
    recency_weeks = rs_summary.get("weekly_signal_weeks_ago")
    reasons = list(htf_summary.get("reasons", []))
    if rs_summary.get("weekly_rs_new_high_recent"):
        if recency_weeks is None:
            reasons.append("recent weekly RS new high")
        elif int(recency_weeks) == 0:
            reasons.append("weekly RS new high this week")
        else:
            reasons.append(f"weekly RS new high {int(recency_weeks)} week(s) ago")
    if weekly_snapshot["is_above_weekly_ema8"]:
        reasons.append("holding above 8-week EMA")
    else:
        reasons.append("slightly under 8-week EMA")

    return WeeklyHtfPullbackHit(
        ticker=ticker.symbol,
        sector=ticker.sector,
        exchange=ticker.exchange,
        benchmark_ticker=str(rs_summary["benchmark_ticker"]),
        current_price=float(weekly_snapshot["current_price"]),
        weekly_ema8=float(weekly_snapshot["weekly_ema8"]),
        weekly_ema8_distance_pct=float(weekly_snapshot["weekly_ema8_distance_pct"]),
        weekly_ema8_distance_abs_pct=float(weekly_snapshot["weekly_ema8_distance_abs_pct"]),
        is_above_weekly_ema8=bool(weekly_snapshot["is_above_weekly_ema8"]),
        weekly_rs_new_high=bool(rs_summary["weekly_rs_new_high"]),
        weekly_rs_new_high_recent=bool(rs_summary["weekly_rs_new_high_recent"]),
        weekly_signal_weeks_ago=int(recency_weeks) if recency_weeks is not None else None,
        weekly_recent_signal_weeks=int(rs_summary["weekly_recent_signal_weeks"]),
        current_rs_line=float(rs_summary["current_rs_line"]),
        rs_line_high=float(rs_summary["rs_line_high"]),
        htf_score=float(htf_summary["htf_score"]),
        htf_grade=str(htf_summary["htf_grade"]),
        htf_trade_plan=str(htf_summary["trade_plan"]),
        htf_runup_pct=float(htf_summary["htf_runup_pct"]),
        htf_pullback_from_high_pct=float(htf_summary["htf_pullback_from_high_pct"]),
        htf_runup_low=float(htf_summary["htf_runup_low"]),
        htf_runup_high=float(htf_summary["htf_runup_high"]),
        htf_runup_low_date=str(htf_summary["htf_runup_low_date"]),
        htf_runup_high_date=str(htf_summary["htf_runup_high_date"]),
        year_high=float(htf_summary["year_high"]),
        distance_from_year_high_pct=float(htf_summary["distance_from_year_high_pct"]),
        is_near_year_high=bool(htf_summary["is_near_year_high"]),
        is_strong_rs=bool(htf_summary["is_strong_rs"]),
        is_sector_etf_strong=bool(htf_summary["is_sector_etf_strong"]),
        sector_etf=str(htf_summary["sector_etf"]),
        reasons=reasons,
    )


def run_weekly_htf_pullback_screen(
    config: AppConfig,
    tickers: list[UniverseTicker],
    *,
    as_of_date: dt.date | None = None,
) -> WeeklyHtfPullbackScreenResult:
    cookstock = load_configured_cookstock(config)
    hits: list[WeeklyHtfPullbackHit] = []
    failures: list[dict[str, str]] = []
    history_days = max(config.rs_new_high_history_days, config.htf_history_days, 365)
    total_tickers = len(tickers)
    run_date = as_of_date or dt.date.today()

    print(
        "starting weekly HTF 8W pullback screen: "
        f"total={total_tickers}, "
        f"weekly_rs_recent_window={config.rs_weekly_recent_signal_weeks}w, "
        f"htf_min_runup={config.htf_min_runup_pct:.1f}%, "
        f"htf_max_pullback={config.htf_max_correction_pct:.1f}%, "
        f"ema8_breach_tolerance={config.weekly_htf_ema8_breach_tolerance_pct * 100:.1f}%"
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
                    rs_summary = financials.get_rs_new_high_before_price_summary(
                        sectorName=ticker.sector,
                        benchmarkTicker=config.benchmark_ticker,
                        signalProfile="weekly",
                    )
                    if not rs_summary or not bool(rs_summary.get("weekly_rs_new_high_recent")):
                        print(f"[{position}/{total_tickers}] {ticker.symbol} filtered: no recent weekly RS new high | passed={len(hits)}")
                        continue

                    htf_summary = financials.get_htf_leader_summary(
                        sectorName=ticker.sector,
                        benchmarkTicker=config.benchmark_ticker,
                    )
                    if not htf_summary:
                        print(f"[{position}/{total_tickers}] {ticker.symbol} filtered: no HTF summary | passed={len(hits)}")
                        continue
                    if str(htf_summary.get("htf_grade", "")).upper() not in {"A", "B"}:
                        print(
                            f"[{position}/{total_tickers}] {ticker.symbol} filtered: "
                            f"HTF grade {htf_summary.get('htf_grade')} | passed={len(hits)}"
                        )
                        continue

                    weekly_snapshot = _latest_weekly_snapshot(financials)
                    if not weekly_snapshot:
                        print(f"[{position}/{total_tickers}] {ticker.symbol} filtered: missing weekly snapshot | passed={len(hits)}")
                        continue

                    distance_ratio = float(weekly_snapshot["weekly_ema8_distance_pct"]) / 100.0
                    if distance_ratio < -float(config.weekly_htf_ema8_breach_tolerance_pct):
                        print(
                            f"[{position}/{total_tickers}] {ticker.symbol} filtered: "
                            f"{weekly_snapshot['weekly_ema8_distance_pct']:.2f}% vs 8W EMA | passed={len(hits)}"
                        )
                        continue

                    hits.append(_build_hit(ticker, rs_summary, htf_summary, weekly_snapshot))
                    latest_hit = hits[-1]
                    print(
                        f"[{position}/{total_tickers}] {ticker.symbol} passed: "
                        f"HTF {latest_hit.htf_grade} {latest_hit.htf_score:.1f}, "
                        f"8W EMA distance {latest_hit.weekly_ema8_distance_pct:+.2f}% | passed={len(hits)}"
                    )
                except Exception as exc:
                    failures.append({"ticker": ticker.symbol, "error": str(exc)})
                    print(f"[{position}/{total_tickers}] {ticker.symbol} error: {exc} | passed={len(hits)}")

    hits.sort(
        key=lambda hit: (
            0 if hit.is_above_weekly_ema8 else 1,
            hit.weekly_ema8_distance_abs_pct,
            hit.weekly_signal_weeks_ago if hit.weekly_signal_weeks_ago is not None else 99,
            -hit.htf_score,
            hit.ticker,
        )
    )

    print(
        "finished weekly HTF 8W pullback screen: "
        f"passed={len(hits)}, failed={len(failures)}, total={total_tickers}"
    )

    return WeeklyHtfPullbackScreenResult(
        run_date=run_date.isoformat(),
        benchmark_ticker=config.benchmark_ticker,
        total_tickers=total_tickers,
        passed_tickers=len(hits),
        failed_tickers=failures,
        hits=hits,
    )
