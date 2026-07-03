from __future__ import annotations

from dataclasses import asdict, dataclass
import datetime as dt

import numpy as np

from .config import AppConfig
from .cookstock_bridge import freeze_cookstock_today, iter_prefetched_cookstock_batches, load_configured_cookstock
from .universe import UniverseTicker


@dataclass(frozen=True)
class GapFillHit:
    ticker: str
    sector: str | None
    exchange: str | None
    benchmark_ticker: str
    current_price: float
    avg_volume_20: float
    avg_dollar_volume_20: float
    ema_21: float
    ema_50: float
    price_above_ema21: bool
    price_above_ema50: bool
    inside_day: bool
    recent_range_pct: float | None
    gap_date: str
    gap_top: float
    gap_bottom: float
    gap_size_pct: float
    distance_to_gap_bottom_pct: float
    distance_to_gap_top_pct: float
    gap_reclaimed: bool
    recent_low: float
    reasons: list[str]

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True)
class GapFillScreenResult:
    run_date: str
    benchmark_ticker: str
    total_tickers: int
    passed_tickers: int
    failed_tickers: list[dict[str, str]]
    hits: list[GapFillHit]

    def to_dict(self) -> dict[str, object]:
        return {
            "run_date": self.run_date,
            "benchmark_ticker": self.benchmark_ticker,
            "total_tickers": self.total_tickers,
            "passed_tickers": self.passed_tickers,
            "failed_tickers": self.failed_tickers,
            "hits": [item.to_dict() for item in self.hits],
        }


def _is_inside_day(price_data: list[dict[str, object]]) -> bool:
    if len(price_data) < 2:
        return False
    latest_bar = price_data[-1]
    previous_bar = price_data[-2]
    latest_high = latest_bar.get("high")
    latest_low = latest_bar.get("low")
    previous_high = previous_bar.get("high")
    previous_low = previous_bar.get("low")
    if None in (latest_high, latest_low, previous_high, previous_low):
        return False
    return bool(float(latest_high) < float(previous_high) and float(latest_low) > float(previous_low))


def _scan_open_overhead_gap(
    price_data: list[dict[str, object]],
    *,
    lookback_days: int,
    min_gap_pct: float,
    current_price: float,
) -> dict[str, object] | None:
    if len(price_data) < 3:
        return None
    start_index = max(1, len(price_data) - int(lookback_days))
    candidates: list[dict[str, object]] = []
    for idx in range(start_index, len(price_data)):
        gap_bar = price_data[idx]
        previous_bar = price_data[idx - 1]
        previous_low = previous_bar.get("low")
        gap_high = gap_bar.get("high")
        if previous_low is None or gap_high is None:
            continue
        previous_low = float(previous_low)
        gap_high = float(gap_high)
        if previous_low <= 0 or not gap_high < previous_low:
            continue
        gap_size_pct = (previous_low - gap_high) / previous_low
        if gap_size_pct < float(min_gap_pct):
            continue
        future_highs = [
            float(item["high"])
            for item in price_data[idx + 1:]
            if item.get("high") is not None
        ]
        max_future_high = max(future_highs) if future_highs else None
        if max_future_high is not None and max_future_high >= previous_low:
            continue
        if current_price >= previous_low:
            continue
        # Treat the tradable gap entry as the remaining unfilled zone after any
        # later rallies partially reclaim the original overhead gap.
        gap_bottom = max(gap_high, max_future_high or gap_high)
        gap_top = previous_low
        distance_to_gap_bottom_pct = (gap_bottom - current_price) / current_price
        distance_to_gap_top_pct = (gap_top - current_price) / current_price
        candidates.append(
            {
                "gap_date": str(gap_bar.get("formatted_date") or "NA"),
                "gap_top": gap_top,
                "gap_bottom": gap_bottom,
                "gap_size_pct": gap_size_pct * 100.0,
                "distance_to_gap_bottom_pct": distance_to_gap_bottom_pct * 100.0,
                "distance_to_gap_top_pct": distance_to_gap_top_pct * 100.0,
                "gap_reclaimed": current_price >= gap_bottom,
            }
        )
    if not candidates:
        return None
    candidates.sort(
        key=lambda item: (
            abs(float(item["distance_to_gap_bottom_pct"])),
            float(item["distance_to_gap_top_pct"]),
            str(item["gap_date"]),
        )
    )
    return candidates[0]


def _to_hit(
    ticker: UniverseTicker,
    benchmark_ticker: str,
    *,
    current_price: float,
    avg_volume_20: float,
    avg_dollar_volume_20: float,
    ema_21: float,
    ema_50: float,
    price_above_ema21: bool,
    price_above_ema50: bool,
    inside_day: bool,
    recent_range_pct: float | None,
    gap_summary: dict[str, object],
    recent_low: float,
) -> GapFillHit:
    gap_top = float(gap_summary["gap_top"])
    gap_bottom = float(gap_summary["gap_bottom"])
    reasons = [
        f"open overhead gap from {gap_summary['gap_date']}",
        f"gap size {float(gap_summary['gap_size_pct']):.1f}%",
        f"{float(gap_summary['distance_to_gap_bottom_pct']):+.1f}% vs gap entry",
        f"{float(gap_summary['distance_to_gap_top_pct']):+.1f}% to fill target",
        f"above 21 EMA {ema_21:.2f} and 50 EMA {ema_50:.2f}",
    ]
    if inside_day:
        reasons.append("inside day")
    if recent_range_pct is not None:
        reasons.append(f"{recent_range_pct * 100:.1f}% 10D range")
    if current_price >= gap_bottom:
        reasons.append("already trading inside the gap")
    return GapFillHit(
        ticker=ticker.symbol,
        sector=ticker.sector,
        exchange=ticker.exchange,
        benchmark_ticker=benchmark_ticker,
        current_price=current_price,
        avg_volume_20=avg_volume_20,
        avg_dollar_volume_20=avg_dollar_volume_20,
        ema_21=ema_21,
        ema_50=ema_50,
        price_above_ema21=price_above_ema21,
        price_above_ema50=price_above_ema50,
        inside_day=inside_day,
        recent_range_pct=recent_range_pct,
        gap_date=str(gap_summary["gap_date"]),
        gap_top=gap_top,
        gap_bottom=gap_bottom,
        gap_size_pct=float(gap_summary["gap_size_pct"]),
        distance_to_gap_bottom_pct=float(gap_summary["distance_to_gap_bottom_pct"]),
        distance_to_gap_top_pct=float(gap_summary["distance_to_gap_top_pct"]),
        gap_reclaimed=bool(gap_summary["gap_reclaimed"]),
        recent_low=recent_low,
        reasons=reasons,
    )


def run_gap_fill_screen(
    config: AppConfig,
    tickers: list[UniverseTicker],
    *,
    as_of_date: dt.date | None = None,
) -> GapFillScreenResult:
    cookstock = load_configured_cookstock(config)
    hits: list[GapFillHit] = []
    failures: list[dict[str, str]] = []
    history_days = max(int(config.gap_fill_history_days), int(config.gap_fill_lookback_days) + 30, 120)
    total_tickers = len(tickers)
    run_date = as_of_date or dt.date.today()

    print(
        "starting gap fill screen: "
        f"total={total_tickers}, "
        f"lookback={config.gap_fill_lookback_days}, "
        f"min_gap={float(config.gap_fill_min_gap_pct) * 100.0:.1f}%, "
        f"max_gap_entry_distance={float(config.gap_fill_max_distance_to_gap_bottom_pct) * 100.0:.1f}%"
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
                    price_data = financials._get_clean_price_data()
                    if len(price_data) < 40:
                        print(f"[{position}/{total_tickers}] {ticker.symbol} filtered: insufficient price history | passed={len(hits)}")
                        continue

                    current_price = float(price_data[-1]["close"])
                    avg_volume_20 = float(financials._get_average_volume(20))
                    if avg_volume_20 < int(config.gap_fill_min_avg_volume):
                        print(
                            f"[{position}/{total_tickers}] {ticker.symbol} filtered: "
                            f"avg vol20 {avg_volume_20:,.0f} < {int(config.gap_fill_min_avg_volume):,} | passed={len(hits)}"
                        )
                        continue
                    avg_dollar_volume_20 = float(financials._get_average_dollar_volume(20))
                    if avg_dollar_volume_20 < float(config.gap_fill_min_avg_dollar_volume):
                        print(
                            f"[{position}/{total_tickers}] {ticker.symbol} filtered: "
                            f"avg $ vol20 {avg_dollar_volume_20:,.0f} < {float(config.gap_fill_min_avg_dollar_volume):,.0f} | passed={len(hits)}"
                        )
                        continue

                    ema_21 = financials._get_latest_ema_value(21)
                    ema_50 = financials._get_latest_ema_value(50)
                    if ema_21 is None or ema_50 is None:
                        print(f"[{position}/{total_tickers}] {ticker.symbol} filtered: missing EMA values | passed={len(hits)}")
                        continue
                    if current_price <= float(ema_21):
                        print(
                            f"[{position}/{total_tickers}] {ticker.symbol} filtered: "
                            f"current {current_price:.2f} <= 21 EMA {float(ema_21):.2f} | passed={len(hits)}"
                        )
                        continue
                    if current_price <= float(ema_50):
                        print(
                            f"[{position}/{total_tickers}] {ticker.symbol} filtered: "
                            f"current {current_price:.2f} <= 50 EMA {float(ema_50):.2f} | passed={len(hits)}"
                        )
                        continue

                    gap_summary = _scan_open_overhead_gap(
                        price_data,
                        lookback_days=int(config.gap_fill_lookback_days),
                        min_gap_pct=float(config.gap_fill_min_gap_pct),
                        current_price=current_price,
                    )
                    if not gap_summary:
                        print(f"[{position}/{total_tickers}] {ticker.symbol} filtered: no open overhead gap | passed={len(hits)}")
                        continue

                    distance_to_gap_bottom_pct = float(gap_summary["distance_to_gap_bottom_pct"]) / 100.0
                    distance_to_gap_top_pct = float(gap_summary["distance_to_gap_top_pct"]) / 100.0
                    if distance_to_gap_bottom_pct < float(config.gap_fill_min_distance_to_gap_bottom_pct):
                        print(
                            f"[{position}/{total_tickers}] {ticker.symbol} filtered: "
                            f"too deep inside gap {distance_to_gap_bottom_pct * 100.0:+.1f}% | passed={len(hits)}"
                        )
                        continue
                    if distance_to_gap_bottom_pct > float(config.gap_fill_max_distance_to_gap_bottom_pct):
                        print(
                            f"[{position}/{total_tickers}] {ticker.symbol} filtered: "
                            f"too far below gap entry {distance_to_gap_bottom_pct * 100.0:+.1f}% | passed={len(hits)}"
                        )
                        continue
                    if distance_to_gap_top_pct < float(config.gap_fill_min_distance_to_gap_top_pct):
                        print(
                            f"[{position}/{total_tickers}] {ticker.symbol} filtered: "
                            f"not enough fill room {distance_to_gap_top_pct * 100.0:+.1f}% | passed={len(hits)}"
                        )
                        continue
                    if distance_to_gap_top_pct > float(config.gap_fill_max_distance_to_gap_top_pct):
                        print(
                            f"[{position}/{total_tickers}] {ticker.symbol} filtered: "
                            f"gap target too far {distance_to_gap_top_pct * 100.0:+.1f}% | passed={len(hits)}"
                        )
                        continue

                    inside_day = _is_inside_day(price_data)
                    recent_range_pct = financials._get_recent_range_pct(int(config.gap_fill_tight_range_lookback_days))
                    recent_window = price_data[-max(5, int(config.gap_fill_tight_range_lookback_days)) :]
                    recent_lows = [float(item["low"]) for item in recent_window if item.get("low") is not None]
                    if not recent_lows:
                        print(f"[{position}/{total_tickers}] {ticker.symbol} filtered: missing recent lows | passed={len(hits)}")
                        continue
                    recent_low = min(recent_lows)
                    hit = _to_hit(
                        ticker,
                        config.benchmark_ticker,
                        current_price=current_price,
                        avg_volume_20=avg_volume_20,
                        avg_dollar_volume_20=avg_dollar_volume_20,
                        ema_21=float(ema_21),
                        ema_50=float(ema_50),
                        price_above_ema21=True,
                        price_above_ema50=True,
                        inside_day=inside_day,
                        recent_range_pct=recent_range_pct,
                        gap_summary=gap_summary,
                        recent_low=recent_low,
                    )
                    hits.append(hit)
                    print(
                        f"[{position}/{total_tickers}] {ticker.symbol} passed: "
                        f"gap entry {hit.distance_to_gap_bottom_pct:+.1f}% fill target {hit.distance_to_gap_top_pct:+.1f}% "
                        f"inside_day={hit.inside_day} | passed={len(hits)}"
                    )
                except Exception as exc:
                    failures.append({"ticker": ticker.symbol, "error": str(exc)})
                    print(f"[{position}/{total_tickers}] {ticker.symbol} error: {exc} | passed={len(hits)}")

    hits.sort(
        key=lambda hit: (
            not hit.gap_reclaimed,
            abs(hit.distance_to_gap_bottom_pct),
            not hit.inside_day,
            hit.recent_range_pct if hit.recent_range_pct is not None else float("inf"),
            hit.distance_to_gap_top_pct,
            -hit.avg_dollar_volume_20,
            hit.ticker,
        )
    )

    print(
        "finished gap fill screen: "
        f"passed={len(hits)}, failed={len(failures)}, total={total_tickers}"
    )

    return GapFillScreenResult(
        run_date=run_date.isoformat(),
        benchmark_ticker=config.benchmark_ticker,
        total_tickers=total_tickers,
        passed_tickers=len(hits),
        failed_tickers=failures,
        hits=hits,
    )
