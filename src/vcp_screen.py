from __future__ import annotations

from dataclasses import asdict, dataclass
import datetime as dt

from .config import AppConfig
from .cookstock_bridge import load_configured_cookstock
from .universe import UniverseTicker


@dataclass(frozen=True)
class VcpHit:
    ticker: str
    sector: str | None
    exchange: str | None
    signal_date: str
    benchmark_ticker: str
    screen_profile: str
    current_price: float
    support_price: float
    pivot_price: float
    vcp_contractions_count: int
    vcp_record: list[list[object]]
    footprint: list[list[object]]
    is_vcp_structure_valid: bool
    is_good_pivot: bool
    is_deep_correction: bool
    is_demand_dry: bool
    demand_dry_start_date: str | None
    demand_dry_end_date: str | None
    demand_dry_volume_slope: float | None
    demand_dry_recent_volume_slope: float | None
    is_breakout_volume_confirmed: bool
    breakout_day_volume: float
    breakout_avg_volume_50: float
    is_near_year_high: bool | None
    year_high: float | None
    distance_from_year_high_pct: float | None
    is_strong_rs: bool | None
    stock_return_vs_rs_window_pct: float | None
    benchmark_return_vs_rs_window_pct: float | None
    current_rs_line: float | None
    rs_line_high: float | None
    is_sector_etf_strong: bool | None
    sector_etf: str | None
    sector_etf_near_year_high: bool | None
    sector_etf_distance_from_year_high_pct: float | None
    sector_etf_return_vs_rs_window_pct: float | None
    sector_benchmark_return_vs_rs_window_pct: float | None
    reasons: list[str]

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True)
class VcpScreenResult:
    run_date: str
    benchmark_ticker: str
    total_tickers: int
    passed_tickers: int
    failed_tickers: list[dict[str, str]]
    hits: list[VcpHit]

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
    if value in (None, "", "N/A", "NA", "n/a"):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _optional_bool(value: object) -> bool | None:
    if value in (None, "", "N/A", "NA", "n/a"):
        return None
    return bool(value)


def _as_serializable_records(records: list[list[object]]) -> list[list[object]]:
    serialized: list[list[object]] = []
    for record in records:
        serialized.append([item.isoformat() if hasattr(item, "isoformat") else item for item in record])
    return serialized


def _build_reasons(
    hit: dict[str, object],
) -> list[str]:
    reasons: list[str] = []
    count = int(hit["vcp_contractions_count"])
    reasons.append(f"{count} contraction{'s' if count != 1 else ''}")
    if bool(hit["is_vcp_structure_valid"]):
        reasons.append("tightening VCP structure")
    if bool(hit["is_good_pivot"]):
        reasons.append("pivot breakout in range")
    if bool(hit["is_demand_dry"]):
        reasons.append("demand drying up")
    if bool(hit["is_breakout_volume_confirmed"]):
        reasons.append("breakout volume confirmed")

    is_near_year_high = _optional_bool(hit.get("is_near_year_high"))
    distance = _optional_float(hit.get("distance_from_year_high_pct"))
    if is_near_year_high and distance is not None:
        reasons.append(f"within {distance * 100:.1f}% of year high")

    is_strong_rs = _optional_bool(hit.get("is_strong_rs"))
    if is_strong_rs:
        reasons.append("strong relative strength")

    is_sector_etf_strong = _optional_bool(hit.get("is_sector_etf_strong"))
    sector_etf = hit.get("sector_etf")
    if is_sector_etf_strong and sector_etf:
        reasons.append(f"sector ETF {sector_etf} acting well")
    return reasons


def _to_hit(
    ticker: UniverseTicker,
    benchmark_ticker: str,
    screen_profile: str,
    payload: dict[str, object],
) -> VcpHit:
    normalized = dict(payload)
    normalized["reasons"] = _build_reasons(normalized)
    return VcpHit(
        ticker=ticker.symbol,
        sector=ticker.sector,
        exchange=ticker.exchange,
        signal_date=dt.date.today().isoformat(),
        benchmark_ticker=benchmark_ticker,
        screen_profile=screen_profile,
        current_price=float(normalized["current_price"]),
        support_price=float(normalized["support_price"]),
        pivot_price=float(normalized["pivot_price"]),
        vcp_contractions_count=int(normalized["vcp_contractions_count"]),
        vcp_record=_as_serializable_records(list(normalized["vcp_record"])),
        footprint=_as_serializable_records(list(normalized["footprint"])),
        is_vcp_structure_valid=bool(normalized["is_vcp_structure_valid"]),
        is_good_pivot=bool(normalized["is_good_pivot"]),
        is_deep_correction=bool(normalized["is_deep_correction"]),
        is_demand_dry=bool(normalized["is_demand_dry"]),
        demand_dry_start_date=str(normalized["demand_dry_start_date"]) if normalized.get("demand_dry_start_date") else None,
        demand_dry_end_date=str(normalized["demand_dry_end_date"]) if normalized.get("demand_dry_end_date") else None,
        demand_dry_volume_slope=_optional_float(normalized.get("demand_dry_volume_slope")),
        demand_dry_recent_volume_slope=_optional_float(normalized.get("demand_dry_recent_volume_slope")),
        is_breakout_volume_confirmed=bool(normalized["is_breakout_volume_confirmed"]),
        breakout_day_volume=float(normalized["breakout_day_volume"]),
        breakout_avg_volume_50=float(normalized["breakout_avg_volume_50"]),
        is_near_year_high=_optional_bool(normalized.get("is_near_year_high")),
        year_high=_optional_float(normalized.get("year_high")),
        distance_from_year_high_pct=_optional_float(normalized.get("distance_from_year_high_pct")),
        is_strong_rs=_optional_bool(normalized.get("is_strong_rs")),
        stock_return_vs_rs_window_pct=_optional_float(normalized.get("stock_return_vs_rs_window_pct")),
        benchmark_return_vs_rs_window_pct=_optional_float(normalized.get("benchmark_return_vs_rs_window_pct")),
        current_rs_line=_optional_float(normalized.get("current_rs_line")),
        rs_line_high=_optional_float(normalized.get("rs_line_high")),
        is_sector_etf_strong=_optional_bool(normalized.get("is_sector_etf_strong")),
        sector_etf=str(normalized["sector_etf"]) if normalized.get("sector_etf") not in (None, "", "N/A", "NA", "n/a") else None,
        sector_etf_near_year_high=_optional_bool(normalized.get("sector_etf_near_year_high")),
        sector_etf_distance_from_year_high_pct=_optional_float(normalized.get("sector_etf_distance_from_year_high_pct")),
        sector_etf_return_vs_rs_window_pct=_optional_float(normalized.get("sector_etf_return_vs_rs_window_pct")),
        sector_benchmark_return_vs_rs_window_pct=_optional_float(normalized.get("sector_benchmark_return_vs_rs_window_pct")),
        reasons=list(normalized["reasons"]),
    )


def run_vcp_screen(config: AppConfig, tickers: list[UniverseTicker]) -> VcpScreenResult:
    cookstock = load_configured_cookstock(config)
    screen_profile = str(getattr(cookstock.algoParas, "SCREEN_PROFILE", "strict")).strip().lower() or "strict"
    hits: list[VcpHit] = []
    failures: list[dict[str, str]] = []
    date_from = dt.date.today() - dt.timedelta(days=100)

    for position, ticker in enumerate(tickers, start=1):
        print(f"[{position}/{len(tickers)}] screening {ticker.symbol}")
        try:
            financials = cookstock.cookFinancials(
                ticker.symbol,
                benchmarkTicker=config.benchmark_ticker,
                historyLookbackDays=max(config.rs_new_high_history_days, 365),
            )
            passed = financials.combined_best_strategy(
                sectorName=ticker.sector,
                benchmarkTicker=config.benchmark_ticker,
                screenProfile=screen_profile,
            )
            if not passed:
                continue

            vcp_contractions_count, vcp_record = financials.find_volatility_contraction_pattern(date_from)
            footprint = financials.get_footPrint()
            is_good_pivot, current_price, support_price, pivot_price = financials.is_pivot_good()
            is_deep_correction = financials.is_correction_deep()
            is_demand_dry, start_date, end_date, _, slope, _, _, _, _, slope_recent, _ = financials.is_demand_dry()
            is_breakout_volume_confirmed, breakout_day_volume, breakout_avg_volume_50 = financials.is_breakout_volume_confirmed()

            payload: dict[str, object] = {
                "current_price": current_price,
                "support_price": support_price,
                "pivot_price": pivot_price,
                "vcp_contractions_count": vcp_contractions_count,
                "vcp_record": vcp_record,
                "footprint": footprint,
                "is_vcp_structure_valid": financials.is_vcp_structure_valid(),
                "is_good_pivot": is_good_pivot,
                "is_deep_correction": is_deep_correction,
                "is_demand_dry": is_demand_dry,
                "demand_dry_start_date": start_date if start_date not in (-1, None) else None,
                "demand_dry_end_date": end_date if end_date not in (-1, None) else None,
                "demand_dry_volume_slope": slope,
                "demand_dry_recent_volume_slope": slope_recent,
                "is_breakout_volume_confirmed": is_breakout_volume_confirmed,
                "breakout_day_volume": breakout_day_volume,
                "breakout_avg_volume_50": breakout_avg_volume_50,
                "is_near_year_high": None,
                "year_high": None,
                "distance_from_year_high_pct": None,
                "is_strong_rs": None,
                "stock_return_vs_rs_window_pct": None,
                "benchmark_return_vs_rs_window_pct": None,
                "current_rs_line": None,
                "rs_line_high": None,
                "is_sector_etf_strong": None,
                "sector_etf": None,
                "sector_etf_near_year_high": None,
                "sector_etf_distance_from_year_high_pct": None,
                "sector_etf_return_vs_rs_window_pct": None,
                "sector_benchmark_return_vs_rs_window_pct": None,
            }

            if screen_profile != "legacy":
                is_near_year_high, _, year_high, year_high_distance = financials.is_near_year_high()
                is_strong_rs, stock_return, benchmark_return, current_rs_line, rs_line_high = financials.is_relative_strength_strong(
                    config.benchmark_ticker
                )
                (
                    is_sector_etf_strong,
                    sector_etf,
                    sector_etf_near_year_high,
                    _sector_etf_current,
                    _sector_etf_year_high,
                    sector_etf_distance_from_year_high_pct,
                    sector_etf_return_vs_rs_window_pct,
                    sector_benchmark_return_vs_rs_window_pct,
                ) = financials.is_sector_etf_strong(ticker.sector, config.benchmark_ticker)
                payload.update(
                    {
                        "is_near_year_high": is_near_year_high,
                        "year_high": year_high,
                        "distance_from_year_high_pct": year_high_distance,
                        "is_strong_rs": is_strong_rs,
                        "stock_return_vs_rs_window_pct": stock_return,
                        "benchmark_return_vs_rs_window_pct": benchmark_return,
                        "current_rs_line": current_rs_line,
                        "rs_line_high": rs_line_high,
                        "is_sector_etf_strong": is_sector_etf_strong,
                        "sector_etf": sector_etf,
                        "sector_etf_near_year_high": sector_etf_near_year_high,
                        "sector_etf_distance_from_year_high_pct": sector_etf_distance_from_year_high_pct,
                        "sector_etf_return_vs_rs_window_pct": sector_etf_return_vs_rs_window_pct,
                        "sector_benchmark_return_vs_rs_window_pct": sector_benchmark_return_vs_rs_window_pct,
                    }
                )

            hits.append(_to_hit(ticker, config.benchmark_ticker, screen_profile, payload))
        except Exception as exc:
            failures.append({"ticker": ticker.symbol, "error": str(exc)})
            print(f"screening failed for {ticker.symbol}: {exc}")

    return VcpScreenResult(
        run_date=dt.date.today().isoformat(),
        benchmark_ticker=config.benchmark_ticker,
        total_tickers=len(tickers),
        passed_tickers=len(hits),
        failed_tickers=failures,
        hits=hits,
    )
