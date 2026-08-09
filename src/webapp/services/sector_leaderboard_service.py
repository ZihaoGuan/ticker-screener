from __future__ import annotations

import datetime as dt
from pathlib import Path
import math
from dataclasses import dataclass
from typing import Any

import pandas as pd

from ...market_data_access import load_many_ticker_windows
from ...ratings.repository import RatingsRepository
from ...ssga_holdings import load_holdings_cache


@dataclass(frozen=True)
class SectorHolding:
    ticker: str
    weight: float
    name: str = ""
    shares_held: float | None = None


@dataclass(frozen=True)
class SectorEtf:
    ticker: str
    description: str
    provider: str
    source_url: str
    holdings: tuple[SectorHolding, ...]


_SSGA_BASE_URL = "https://www.ssga.com/us/en/intermediary/etfs"
_BENCHMARK_TICKER = "SPY"


DEFAULT_SECTOR_ETFS: tuple[SectorEtf, ...] = (
    SectorEtf(
        ticker="XLC",
        description="Comm. Services",
        provider="State Street",
        source_url=f"{_SSGA_BASE_URL}/state-street-communication-services-select-sector-spdr-etf-xlc",
        holdings=(
            SectorHolding("META", 18.03),
            SectorHolding("GOOGL", 10.04),
            SectorHolding("GOOG", 8.07),
            SectorHolding("T", 5.06),
            SectorHolding("VZ", 4.81),
        ),
    ),
    SectorEtf(
        ticker="XLY",
        description="Consumer Disc.",
        provider="State Street",
        source_url=f"{_SSGA_BASE_URL}/state-street-consumer-discretionary-select-sector-spdr-etf-xly",
        holdings=(
            SectorHolding("AMZN", 23.20),
            SectorHolding("TSLA", 15.67),
            SectorHolding("HD", 5.90),
            SectorHolding("MCD", 4.36),
            SectorHolding("TJX", 4.28),
        ),
    ),
    SectorEtf(
        ticker="XLP",
        description="Consumer Staples",
        provider="State Street",
        source_url=f"{_SSGA_BASE_URL}/state-street-consumer-staples-select-sector-spdr-etf-xlp",
        holdings=(
            SectorHolding("WMT", 10.35),
            SectorHolding("COST", 8.95),
            SectorHolding("PG", 7.40),
            SectorHolding("KO", 6.87),
            SectorHolding("PM", 6.49),
        ),
    ),
    SectorEtf(
        ticker="XLE",
        description="Energy",
        provider="State Street",
        source_url=f"{_SSGA_BASE_URL}/state-street-energy-select-sector-spdr-etf-xle",
        holdings=(
            SectorHolding("XOM", 20.93),
            SectorHolding("CVX", 15.04),
            SectorHolding("COP", 5.95),
            SectorHolding("MPC", 4.97),
            SectorHolding("PSX", 4.92),
        ),
    ),
    SectorEtf(
        ticker="XLF",
        description="Financials",
        provider="State Street",
        source_url=f"{_SSGA_BASE_URL}/state-street-financial-select-sector-spdr-etf-xlf",
        holdings=(
            SectorHolding("JPM", 11.89),
            SectorHolding("BRK-B", 11.40),
            SectorHolding("V", 7.42),
            SectorHolding("MA", 5.47),
            SectorHolding("BAC", 5.09),
        ),
    ),
    SectorEtf(
        ticker="XLV",
        description="Healthcare",
        provider="State Street",
        source_url=f"{_SSGA_BASE_URL}/state-street-health-care-select-sector-spdr-etf-xlv",
        holdings=(
            SectorHolding("LLY", 16.11),
            SectorHolding("JNJ", 10.80),
            SectorHolding("ABBV", 7.80),
            SectorHolding("UNH", 6.51),
            SectorHolding("MRK", 5.51),
        ),
    ),
    SectorEtf(
        ticker="XLI",
        description="Industrials",
        provider="State Street",
        source_url=f"{_SSGA_BASE_URL}/state-street-industrial-select-sector-spdr-etf-xli",
        holdings=(
            SectorHolding("GE", 5.15),
            SectorHolding("RTX", 4.55),
            SectorHolding("CAT", 4.50),
            SectorHolding("UBER", 4.20),
            SectorHolding("BA", 3.60),
        ),
    ),
    SectorEtf(
        ticker="XLB",
        description="Materials",
        provider="State Street",
        source_url=f"{_SSGA_BASE_URL}/state-street-materials-select-sector-spdr-etf-xlb",
        holdings=(
            SectorHolding("LIN", 16.20),
            SectorHolding("SHW", 7.10),
            SectorHolding("ECL", 6.40),
            SectorHolding("APD", 5.20),
            SectorHolding("NEM", 4.80),
        ),
    ),
    SectorEtf(
        ticker="XLRE",
        description="Real Estate",
        provider="State Street",
        source_url=f"{_SSGA_BASE_URL}/state-street-real-estate-select-sector-spdr-etf-xlre",
        holdings=(
            SectorHolding("PLD", 12.20),
            SectorHolding("AMT", 8.90),
            SectorHolding("EQIX", 7.80),
            SectorHolding("WELL", 6.80),
            SectorHolding("SPG", 5.40),
        ),
    ),
    SectorEtf(
        ticker="XLK",
        description="Technology",
        provider="State Street",
        source_url=f"{_SSGA_BASE_URL}/state-street-technology-select-sector-spdr-etf-xlk",
        holdings=(
            SectorHolding("NVDA", 14.17),
            SectorHolding("AAPL", 13.84),
            SectorHolding("MSFT", 8.02),
            SectorHolding("AVGO", 5.12),
            SectorHolding("AMD", 4.59),
        ),
    ),
    SectorEtf(
        ticker="XLU",
        description="Utilities",
        provider="State Street",
        source_url=f"{_SSGA_BASE_URL}/state-street-utilities-select-sector-spdr-etf-xlu",
        holdings=(
            SectorHolding("NEE", 13.10),
            SectorHolding("SO", 8.30),
            SectorHolding("DUK", 7.80),
            SectorHolding("CEG", 6.90),
            SectorHolding("SRE", 4.80),
        ),
    ),
    SectorEtf(
        ticker="XAR",
        description="Aerospace & Defense",
        provider="State Street",
        source_url=f"{_SSGA_BASE_URL}/spdr-sp-aerospace-defense-etf-xar",
        holdings=(SectorHolding("RTX", 4.0), SectorHolding("LMT", 4.0), SectorHolding("NOC", 4.0), SectorHolding("GD", 4.0), SectorHolding("AXON", 4.0)),
    ),
    SectorEtf(
        ticker="XBI",
        description="Biotechnology",
        provider="State Street",
        source_url=f"{_SSGA_BASE_URL}/spdr-sp-biotech-etf-xbi",
        holdings=(SectorHolding("MRNA", 2.0), SectorHolding("TWST", 2.0), SectorHolding("CRSP", 2.0), SectorHolding("IONS", 2.0), SectorHolding("EXEL", 2.0)),
    ),
    SectorEtf(
        ticker="XES",
        description="Oil & Gas Equip.",
        provider="State Street",
        source_url=f"{_SSGA_BASE_URL}/spdr-sp-oil-gas-equipment-services-etf-xes",
        holdings=(SectorHolding("SLB", 5.0), SectorHolding("HAL", 5.0), SectorHolding("BKR", 5.0), SectorHolding("FTI", 5.0), SectorHolding("NOV", 5.0)),
    ),
    SectorEtf(
        ticker="XHB",
        description="Homebuilders",
        provider="State Street",
        source_url=f"{_SSGA_BASE_URL}/spdr-sp-homebuilders-etf-xhb",
        holdings=(SectorHolding("PHM", 4.0), SectorHolding("DHI", 4.0), SectorHolding("LEN", 4.0), SectorHolding("NVR", 4.0), SectorHolding("TOL", 4.0)),
    ),
    SectorEtf(
        ticker="XHE",
        description="Health Care Equip.",
        provider="State Street",
        source_url=f"{_SSGA_BASE_URL}/spdr-sp-health-care-equipment-etf-xhe",
        holdings=(SectorHolding("ISRG", 4.0), SectorHolding("BSX", 4.0), SectorHolding("SYK", 4.0), SectorHolding("MDT", 4.0), SectorHolding("EW", 4.0)),
    ),
    SectorEtf(
        ticker="XHS",
        description="Health Care Services",
        provider="State Street",
        source_url=f"{_SSGA_BASE_URL}/spdr-sp-health-care-services-etf-xhs",
        holdings=(SectorHolding("HCA", 4.0), SectorHolding("UHS", 4.0), SectorHolding("DGX", 4.0), SectorHolding("LH", 4.0), SectorHolding("MOH", 4.0)),
    ),
    SectorEtf(
        ticker="XME",
        description="Metals & Mining",
        provider="State Street",
        source_url=f"{_SSGA_BASE_URL}/spdr-sp-metals-mining-etf-xme",
        holdings=(SectorHolding("NEM", 5.0), SectorHolding("FCX", 5.0), SectorHolding("CLF", 5.0), SectorHolding("STLD", 5.0), SectorHolding("NUE", 5.0)),
    ),
    SectorEtf(
        ticker="XOP",
        description="Oil & Gas",
        provider="State Street",
        source_url=f"{_SSGA_BASE_URL}/spdr-sp-oil-gas-exploration-production-etf-xop",
        holdings=(SectorHolding("TPL", 3.0), SectorHolding("EOG", 3.0), SectorHolding("FANG", 3.0), SectorHolding("DVN", 3.0), SectorHolding("CNX", 3.0)),
    ),
    SectorEtf(
        ticker="XPH",
        description="Pharmaceuticals",
        provider="State Street",
        source_url=f"{_SSGA_BASE_URL}/spdr-sp-pharmaceuticals-etf-xph",
        holdings=(SectorHolding("LLY", 5.0), SectorHolding("MRK", 5.0), SectorHolding("PFE", 5.0), SectorHolding("BMY", 5.0), SectorHolding("VTRS", 5.0)),
    ),
    SectorEtf(
        ticker="XRT",
        description="Retail",
        provider="State Street",
        source_url=f"{_SSGA_BASE_URL}/spdr-sp-retail-etf-xrt",
        holdings=(SectorHolding("GRPN", 2.0), SectorHolding("REAL", 2.0), SectorHolding("BBWI", 2.0), SectorHolding("WRBY", 2.0), SectorHolding("UPBD", 2.0)),
    ),
    SectorEtf(
        ticker="XSD",
        description="Semiconductors",
        provider="State Street",
        source_url=f"{_SSGA_BASE_URL}/spdr-sp-semiconductor-etf-xsd",
        holdings=(SectorHolding("NVDA", 4.0), SectorHolding("AMD", 4.0), SectorHolding("AVGO", 4.0), SectorHolding("MU", 4.0), SectorHolding("LRCX", 4.0)),
    ),
    SectorEtf(
        ticker="XSW",
        description="Software & Services",
        provider="State Street",
        source_url=f"{_SSGA_BASE_URL}/spdr-sp-software-services-etf-xsw",
        holdings=(SectorHolding("MSFT", 4.0), SectorHolding("ORCL", 4.0), SectorHolding("ADBE", 4.0), SectorHolding("CRM", 4.0), SectorHolding("PANW", 4.0)),
    ),
    SectorEtf(
        ticker="XTN",
        description="Transports",
        provider="State Street",
        source_url=f"{_SSGA_BASE_URL}/spdr-sp-transportation-etf-xtn",
        holdings=(SectorHolding("UNP", 4.0), SectorHolding("UBER", 4.0), SectorHolding("CSX", 4.0), SectorHolding("UPS", 4.0), SectorHolding("UAL", 4.0)),
    ),
    SectorEtf(
        ticker="XTL",
        description="Telecom",
        provider="State Street",
        source_url=f"{_SSGA_BASE_URL}/spdr-sp-telecom-etf-xtl",
        holdings=(SectorHolding("CSCO", 4.0), SectorHolding("MSI", 4.0), SectorHolding("ANET", 4.0), SectorHolding("JNPR", 4.0), SectorHolding("CIEN", 4.0)),
    ),
    SectorEtf(
        ticker="KBE",
        description="Banks",
        provider="State Street",
        source_url=f"{_SSGA_BASE_URL}/spdr-sp-bank-etf-kbe",
        holdings=(SectorHolding("JPM", 3.0), SectorHolding("BAC", 3.0), SectorHolding("WFC", 3.0), SectorHolding("C", 3.0), SectorHolding("USB", 3.0)),
    ),
    SectorEtf(
        ticker="KCE",
        description="Capital Markets",
        provider="State Street",
        source_url=f"{_SSGA_BASE_URL}/spdr-sp-capital-markets-etf-kce",
        holdings=(SectorHolding("GS", 4.0), SectorHolding("MS", 4.0), SectorHolding("SCHW", 4.0), SectorHolding("BLK", 4.0), SectorHolding("IBKR", 4.0)),
    ),
    SectorEtf(
        ticker="KIE",
        description="Insurance",
        provider="State Street",
        source_url=f"{_SSGA_BASE_URL}/spdr-sp-insurance-etf-kie",
        holdings=(SectorHolding("CB", 4.0), SectorHolding("PGR", 4.0), SectorHolding("TRV", 4.0), SectorHolding("AFL", 4.0), SectorHolding("ALL", 4.0)),
    ),
    SectorEtf(
        ticker="KRE",
        description="Regional Banks",
        provider="State Street",
        source_url=f"{_SSGA_BASE_URL}/spdr-sp-regional-banking-etf-kre",
        holdings=(SectorHolding("FITB", 3.0), SectorHolding("HBAN", 3.0), SectorHolding("RF", 3.0), SectorHolding("CFG", 3.0), SectorHolding("KEY", 3.0)),
    ),
    SectorEtf(
        ticker="XITK",
        description="Innovative Tech",
        provider="State Street",
        source_url=f"{_SSGA_BASE_URL}/spdr-factset-innovative-technology-etf-xitk",
        holdings=(SectorHolding("PLTR", 4.0), SectorHolding("CRWD", 4.0), SectorHolding("DDOG", 4.0), SectorHolding("SNOW", 4.0), SectorHolding("NET", 4.0)),
    ),
    SectorEtf(
        ticker="XNTK",
        description="NYSE Technology",
        provider="State Street",
        source_url=f"{_SSGA_BASE_URL}/spdr-nyse-technology-etf-xntk",
        holdings=(SectorHolding("NVDA", 4.0), SectorHolding("AAPL", 4.0), SectorHolding("MSFT", 4.0), SectorHolding("AVGO", 4.0), SectorHolding("ORCL", 4.0)),
    ),
)


class SectorLeaderboardService:
    def __init__(self, *, database_url: str | None = None, artifacts_dir: Path | None = None, etfs: tuple[SectorEtf, ...] = DEFAULT_SECTOR_ETFS):
        self.database_url = database_url
        self.artifacts_dir = artifacts_dir
        self.etfs = etfs

    def get_payload(self, *, as_of_date: dt.date | None = None) -> dict[str, Any]:
        resolved_as_of = as_of_date or dt.date.today()
        etf_tickers = [item.ticker for item in self.etfs]
        cache = load_holdings_cache(self.artifacts_dir) if self.artifacts_dir is not None else None
        holdings_by_etf = self._resolve_holdings_by_etf(cache)
        holding_tickers = sorted({holding.ticker for holdings in holdings_by_etf.values() for holding in holdings})
        etf_frames = load_many_ticker_windows([*etf_tickers, _BENCHMARK_TICKER], resolved_as_of, 270, database_url=self.database_url)
        benchmark_frame = etf_frames.get(_BENCHMARK_TICKER)
        holding_frames = load_many_ticker_windows(holding_tickers, resolved_as_of, 270, database_url=self.database_url)
        technical_map = self._load_technical_rating_map(holding_tickers, as_of_date=resolved_as_of)

        rows = [
            self._build_row(
                item,
                etf_frames.get(item.ticker),
                benchmark_frame,
                holdings_by_etf.get(item.ticker, item.holdings),
                holding_frames,
                technical_map,
            )
            for item in self.etfs
        ]
        rows.sort(key=lambda row: _sort_value(row.get("day_change_pct")), reverse=True)
        latest_dates = [row["latest_date"] for row in rows if row.get("latest_date")]
        latest_update = max(latest_dates) if latest_dates else None
        return {
            "generated_at": dt.datetime.now(dt.timezone.utc).isoformat(),
            "as_of_date": resolved_as_of.isoformat(),
            "latest_data_date": latest_update,
            "source": {
                "provider": "State Street Global Advisors",
                "fund_finder_url": "https://www.ssga.com/us/en/intermediary/fund-finder?g=assetclass%3Aequity!noLabel*Sectors---Industries&type=etfs",
                "note": "Default catalog uses Select Sector SPDR equity sector ETFs and excludes premium-income variants.",
                "benchmark_ticker": _BENCHMARK_TICKER,
                "holdings_cache_generated_at": str(cache.get("generated_at") or "") if isinstance(cache, dict) else "",
            },
            "rows": rows,
        }

    def _build_row(
        self,
        etf: SectorEtf,
        frame: Any,
        benchmark_frame: Any,
        holdings_source: tuple[SectorHolding, ...],
        holding_frames: dict[str, Any],
        technical_map: dict[str, dict[str, Any]],
    ) -> dict[str, Any]:
        metrics = _compute_metrics(frame)
        relative_metrics = _compute_relative_metrics(frame, benchmark_frame)
        holdings = []
        for holding in holdings_source:
            holding_frame = holding_frames.get(holding.ticker)
            holding_metrics = _compute_metrics(holding_frame)
            holding_relative_metrics = _compute_relative_metrics(holding_frame, benchmark_frame)
            technical = technical_map.get(holding.ticker, {})
            holdings.append(
                {
                    "ticker": holding.ticker,
                    "name": holding.name,
                    "weight": holding.weight,
                    "shares_held": holding.shares_held,
                    "day_change_pct": holding_metrics["day_change_pct"],
                    "avg_volume_20d": holding_metrics["avg_volume_20d"],
                    "relative_volume_20d": holding_metrics["relative_volume_20d"],
                    "rs_days_21d": holding_relative_metrics["rs_days_21d"],
                    "rs_days_21d_pct": holding_relative_metrics["rs_days_21d_pct"],
                    "red_rs_days_21d": holding_relative_metrics["red_rs_days_21d"],
                    "red_rs_days_21d_pct": holding_relative_metrics["red_rs_days_21d_pct"],
                    "rs_new_high_63d": holding_relative_metrics["rs_new_high_63d"],
                    "avg_dcr_21d": holding_metrics["avg_dcr_21d"],
                    "strong_close_days_21d": holding_metrics["strong_close_days_21d"],
                    "hv63": holding_metrics["hv63"],
                    "volume_confirmation": holding_metrics["volume_confirmation"],
                    "daily_rs_rating": _finite_float(technical.get("daily_rs_rating")),
                    "weekly_rs_rating": _finite_float(technical.get("weekly_rs_rating")),
                    "rs_rating_3m": _finite_float(technical.get("rs_rating_3m")),
                    "rs_rating_6m": _finite_float(technical.get("rs_rating_6m")),
                    "leadership_score": _finite_float(technical.get("leadership_score")),
                }
            )

        return {
            "ticker": etf.ticker,
            "description": etf.description,
            "provider": etf.provider,
            "source_url": etf.source_url,
            "price": metrics["price"],
            "day_change_pct": metrics["day_change_pct"],
            "week_change_pct": metrics["week_change_pct"],
            "month_change_pct": metrics["month_change_pct"],
            "year_change_pct": metrics["year_change_pct"],
            "rs_vs_spy_1m_pct": relative_metrics["rs_1m_pct"],
            "rs_vs_spy_3m_pct": relative_metrics["rs_3m_pct"],
            "rs_momentum_score": relative_metrics["rs_momentum_score"],
            "rs_days_21d": relative_metrics["rs_days_21d"],
            "rs_days_21d_pct": relative_metrics["rs_days_21d_pct"],
            "red_rs_days_21d": relative_metrics["red_rs_days_21d"],
            "red_rs_days_21d_pct": relative_metrics["red_rs_days_21d_pct"],
            "rs_new_high_63d": relative_metrics["rs_new_high_63d"],
            "atr_pct": metrics["atr_pct"],
            "volume": metrics["volume"],
            "avg_volume_20d": metrics["avg_volume_20d"],
            "relative_volume_20d": metrics["relative_volume_20d"],
            "avg_dcr_21d": metrics["avg_dcr_21d"],
            "strong_close_days_21d": metrics["strong_close_days_21d"],
            "hv63": metrics["hv63"],
            "volume_confirmation": metrics["volume_confirmation"],
            "latest_date": metrics["latest_date"],
            "top_holdings": holdings,
        }

    def _resolve_holdings_by_etf(self, cache: dict[str, Any] | None) -> dict[str, tuple[SectorHolding, ...]]:
        fallback = {item.ticker: item.holdings for item in self.etfs}
        if not isinstance(cache, dict):
            return fallback
        results = cache.get("results")
        if not isinstance(results, dict):
            return fallback

        resolved: dict[str, tuple[SectorHolding, ...]] = {}
        for etf in self.etfs:
            result = results.get(etf.ticker)
            raw_holdings = result.get("holdings") if isinstance(result, dict) else None
            if not isinstance(raw_holdings, list):
                resolved[etf.ticker] = etf.holdings
                continue
            holdings: list[SectorHolding] = []
            for item in raw_holdings:
                if not isinstance(item, dict):
                    continue
                ticker = str(item.get("ticker") or "").strip().upper()
                weight = _finite_float(item.get("weight"))
                if not ticker or weight is None:
                    continue
                holdings.append(
                    SectorHolding(
                        ticker=ticker,
                        weight=weight,
                        name=str(item.get("name") or ""),
                        shares_held=_finite_float(item.get("shares_held")),
                    )
                )
            resolved[etf.ticker] = tuple(holdings) if holdings else etf.holdings
        return resolved

    def _load_technical_rating_map(self, tickers: list[str], *, as_of_date: dt.date) -> dict[str, dict[str, Any]]:
        if not self.database_url or not tickers:
            return {}
        try:
            return RatingsRepository(self.database_url).load_latest_technical_rating_snapshots_for_tickers(
                tickers,
                as_of_date=as_of_date,
                allow_older_as_of_date=True,
            )
        except Exception:
            return {}


def _compute_metrics(frame: Any) -> dict[str, Any]:
    if frame is None or frame.empty:
        return {
            "price": None,
            "day_change_pct": None,
            "week_change_pct": None,
            "month_change_pct": None,
            "year_change_pct": None,
            "atr_pct": None,
            "volume": None,
            "avg_volume_20d": None,
            "relative_volume_20d": None,
            "avg_dcr_21d": None,
            "strong_close_days_21d": None,
            "hv63": None,
            "volume_confirmation": None,
            "latest_date": None,
        }
    ordered = frame.sort_index()
    latest = ordered.iloc[-1]
    close = _finite_float(latest.get("Close"))
    latest_date = pd.Timestamp(ordered.index[-1]).date().isoformat()
    return {
        "price": close,
        "day_change_pct": _change_pct(ordered, 1),
        "week_change_pct": _change_pct(ordered, 5),
        "month_change_pct": _change_pct(ordered, 21),
        "year_change_pct": _change_pct(ordered, 252),
        "atr_pct": _atr_pct(ordered, 14),
        "volume": _finite_int(latest.get("Volume")),
        "avg_volume_20d": _avg_volume(ordered, 20),
        "relative_volume_20d": _relative_volume(ordered, 20),
        "avg_dcr_21d": _avg_dcr(ordered, 21),
        "strong_close_days_21d": _strong_close_days(ordered, 21),
        "hv63": _highest_volume_flag(ordered, 63),
        "volume_confirmation": _volume_confirmation(ordered),
        "latest_date": latest_date,
    }


def _compute_relative_metrics(frame: Any, benchmark_frame: Any) -> dict[str, float | int | bool | None]:
    if frame is None or benchmark_frame is None or frame.empty or benchmark_frame.empty:
        return _empty_relative_metrics()

    pair = pd.concat([frame["Close"], benchmark_frame["Close"]], axis=1, join="inner").dropna()
    pair.columns = ["sector_close", "benchmark_close"]
    pair = pair[(pair["sector_close"] > 0) & (pair["benchmark_close"] > 0)]
    if pair.empty:
        return _empty_relative_metrics()

    ratio = pair["sector_close"] / pair["benchmark_close"]
    rs_1m = _ratio_change_pct(ratio, 21)
    rs_3m = _ratio_change_pct(ratio, 63)
    score = _relative_momentum_score(ratio)
    rs_days_count, rs_days_pct = _rs_days(pair["sector_close"], pair["benchmark_close"], 21)
    red_rs_days_count, red_rs_days_pct = _red_market_rs_days(pair["sector_close"], pair["benchmark_close"], 21)
    return {
        "rs_1m_pct": rs_1m,
        "rs_3m_pct": rs_3m,
        "rs_momentum_score": score,
        "rs_days_21d": rs_days_count,
        "rs_days_21d_pct": rs_days_pct,
        "red_rs_days_21d": red_rs_days_count,
        "red_rs_days_21d_pct": red_rs_days_pct,
        "rs_new_high_63d": _rs_new_high(ratio, 63),
    }


def _empty_relative_metrics() -> dict[str, float | int | bool | None]:
    return {
        "rs_1m_pct": None,
        "rs_3m_pct": None,
        "rs_momentum_score": None,
        "rs_days_21d": None,
        "rs_days_21d_pct": None,
        "red_rs_days_21d": None,
        "red_rs_days_21d_pct": None,
        "rs_new_high_63d": None,
    }


def _ratio_change_pct(ratio: pd.Series, lookback: int) -> float | None:
    if len(ratio) <= lookback:
        return None
    latest = _finite_float(ratio.iloc[-1])
    prior = _finite_float(ratio.iloc[-(lookback + 1)])
    if latest is None or prior is None or prior <= 0:
        return None
    return round(((latest / prior) - 1) * 100, 2)


def _relative_momentum_score(ratio: pd.Series) -> float | None:
    log_ratio = ratio.apply(math.log)
    momentum_21 = _log_momentum_pct(log_ratio, 21)
    momentum_63 = _log_momentum_pct(log_ratio, 63)
    if momentum_21 is None and momentum_63 is None:
        return None
    raw_score = 50.0
    if momentum_21 is not None:
        raw_score += momentum_21 * 4.0
    if momentum_63 is not None:
        raw_score += momentum_63 * 1.5
    return round(min(100.0, max(0.0, raw_score)), 1)


def _log_momentum_pct(log_ratio: pd.Series, lookback: int) -> float | None:
    if len(log_ratio) <= lookback:
        return None
    latest = _finite_float(log_ratio.iloc[-1])
    prior = _finite_float(log_ratio.iloc[-(lookback + 1)])
    if latest is None or prior is None:
        return None
    return (latest - prior) * 100


def _rs_days(stock_close: pd.Series, benchmark_close: pd.Series, lookback: int) -> tuple[int | None, float | None]:
    aligned = pd.concat([stock_close, benchmark_close], axis=1, join="inner").dropna()
    aligned.columns = ["stock", "benchmark"]
    if len(aligned) <= 1:
        return None, None
    stock_returns = aligned["stock"].pct_change()
    benchmark_returns = aligned["benchmark"].pct_change()
    comparison = pd.concat([stock_returns, benchmark_returns], axis=1, join="inner").dropna()
    comparison.columns = ["stock_return", "benchmark_return"]
    window = comparison.tail(max(1, int(lookback)))
    if window.empty:
        return None, None
    rs_days = int((window["stock_return"] > window["benchmark_return"]).sum())
    return rs_days, round((rs_days / len(window)) * 100, 1)


def _red_market_rs_days(stock_close: pd.Series, benchmark_close: pd.Series, lookback: int) -> tuple[int | None, float | None]:
    aligned = pd.concat([stock_close, benchmark_close], axis=1, join="inner").dropna()
    aligned.columns = ["stock", "benchmark"]
    if len(aligned) <= 1:
        return None, None
    stock_returns = aligned["stock"].pct_change()
    benchmark_returns = aligned["benchmark"].pct_change()
    comparison = pd.concat([stock_returns, benchmark_returns], axis=1, join="inner").dropna()
    comparison.columns = ["stock_return", "benchmark_return"]
    red_market_window = comparison.tail(max(1, int(lookback)))
    red_market_window = red_market_window[red_market_window["benchmark_return"] < 0]
    if red_market_window.empty:
        return 0, 0.0
    red_rs_days = int((red_market_window["stock_return"] >= 0).sum())
    return red_rs_days, round((red_rs_days / len(red_market_window)) * 100, 1)


def _rs_new_high(ratio: pd.Series, lookback: int) -> bool | None:
    if ratio.empty:
        return None
    window = ratio.dropna().tail(max(1, int(lookback)))
    if window.empty:
        return None
    latest = _finite_float(window.iloc[-1])
    highest = _finite_float(window.max())
    if latest is None or highest is None:
        return None
    return latest >= highest


def _change_pct(frame: Any, lookback: int) -> float | None:
    if len(frame) <= lookback:
        return None
    latest_close = _finite_float(frame.iloc[-1].get("Close"))
    prior_close = _finite_float(frame.iloc[-(lookback + 1)].get("Close"))
    if latest_close is None or prior_close is None or prior_close == 0:
        return None
    return round(((latest_close / prior_close) - 1) * 100, 2)


def _atr_pct(frame: Any, lookback: int) -> float | None:
    if len(frame) < lookback + 1:
        return None
    window = frame.tail(lookback + 1)
    ranges: list[float] = []
    previous_close: float | None = None
    for _, row in window.iterrows():
        high = _finite_float(row.get("High"))
        low = _finite_float(row.get("Low"))
        close = _finite_float(row.get("Close"))
        if high is None or low is None or close is None:
            previous_close = close
            continue
        if previous_close is None:
            previous_close = close
            continue
        ranges.append(max(high - low, abs(high - previous_close), abs(low - previous_close)))
        previous_close = close
    latest_close = _finite_float(frame.iloc[-1].get("Close"))
    if not ranges or latest_close is None or latest_close <= 0:
        return None
    return round((sum(ranges[-lookback:]) / len(ranges[-lookback:]) / latest_close) * 100, 2)


def _avg_volume(frame: Any, lookback: int) -> int | None:
    if frame is None or frame.empty or "Volume" not in frame.columns:
        return None
    volumes = pd.to_numeric(frame["Volume"], errors="coerce").dropna().tail(max(1, int(lookback)))
    if volumes.empty:
        return None
    return int(round(float(volumes.mean())))


def _relative_volume(frame: Any, lookback: int) -> float | None:
    if frame is None or frame.empty:
        return None
    latest_volume = _finite_float(frame.iloc[-1].get("Volume"))
    avg_volume = _avg_volume(frame, lookback)
    if latest_volume is None or avg_volume is None or avg_volume <= 0:
        return None
    return round(latest_volume / avg_volume, 2)


def _daily_close_range(frame: Any) -> pd.Series:
    if frame is None or frame.empty:
        return pd.Series(dtype="float64")
    required = {"High", "Low", "Close"}
    if not required.issubset(set(frame.columns)):
        return pd.Series(dtype="float64")
    high = pd.to_numeric(frame["High"], errors="coerce")
    low = pd.to_numeric(frame["Low"], errors="coerce")
    close = pd.to_numeric(frame["Close"], errors="coerce")
    spread = high - low
    dcr = ((close - low) / spread) * 100
    return dcr.where(spread > 0).dropna()


def _latest_dcr(frame: Any) -> float | None:
    dcr = _daily_close_range(frame)
    if dcr.empty:
        return None
    return _finite_float(dcr.iloc[-1])


def _avg_dcr(frame: Any, lookback: int) -> float | None:
    dcr = _daily_close_range(frame).tail(max(1, int(lookback)))
    if dcr.empty:
        return None
    return round(float(dcr.mean()), 1)


def _strong_close_days(frame: Any, lookback: int) -> int | None:
    dcr = _daily_close_range(frame).tail(max(1, int(lookback)))
    if dcr.empty:
        return None
    return int((dcr >= 60.0).sum())


def _highest_volume_flag(frame: Any, lookback: int) -> bool | None:
    if frame is None or frame.empty or "Volume" not in frame.columns:
        return None
    volumes = pd.to_numeric(frame["Volume"], errors="coerce").dropna().tail(max(1, int(lookback)))
    if volumes.empty:
        return None
    latest = _finite_float(volumes.iloc[-1])
    highest = _finite_float(volumes.max())
    if latest is None or highest is None:
        return None
    return latest >= highest


def _volume_confirmation(frame: Any) -> bool | None:
    latest_dcr = _latest_dcr(frame)
    if latest_dcr is None:
        return None
    relative_volume = _relative_volume(frame, 20)
    hv63 = _highest_volume_flag(frame, 63)
    strong_close = latest_dcr >= 60.0
    if relative_volume is None and hv63 is None:
        return None
    return strong_close and ((relative_volume is not None and relative_volume >= 1.2) or hv63 is True)


def _finite_float(value: object) -> float | None:
    try:
        number = float(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _finite_int(value: object) -> int | None:
    number = _finite_float(value)
    return int(number) if number is not None else None


def _sort_value(value: object) -> float:
    number = _finite_float(value)
    return number if number is not None else -9999.0
