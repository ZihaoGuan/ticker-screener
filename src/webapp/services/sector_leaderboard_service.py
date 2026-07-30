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
    SectorEtf(
        ticker="XWEB",
        description="Internet",
        provider="State Street",
        source_url=f"{_SSGA_BASE_URL}/spdr-sp-internet-etf-xweb",
        holdings=(SectorHolding("META", 4.0), SectorHolding("GOOGL", 4.0), SectorHolding("NFLX", 4.0), SectorHolding("DASH", 4.0), SectorHolding("SHOP", 4.0)),
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
        etf_frames = load_many_ticker_windows(etf_tickers, resolved_as_of, 270, database_url=self.database_url)
        holding_frames = load_many_ticker_windows(holding_tickers, resolved_as_of, 3, database_url=self.database_url)
        technical_map = self._load_technical_rating_map(holding_tickers, as_of_date=resolved_as_of)

        rows = [self._build_row(item, etf_frames.get(item.ticker), holdings_by_etf.get(item.ticker, item.holdings), holding_frames, technical_map) for item in self.etfs]
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
                "holdings_cache_generated_at": str(cache.get("generated_at") or "") if isinstance(cache, dict) else "",
            },
            "rows": rows,
        }

    def _build_row(
        self,
        etf: SectorEtf,
        frame: Any,
        holdings_source: tuple[SectorHolding, ...],
        holding_frames: dict[str, Any],
        technical_map: dict[str, dict[str, Any]],
    ) -> dict[str, Any]:
        metrics = _compute_metrics(frame)
        holdings = []
        for holding in holdings_source:
            holding_metrics = _compute_metrics(holding_frames.get(holding.ticker))
            technical = technical_map.get(holding.ticker, {})
            holdings.append(
                {
                    "ticker": holding.ticker,
                    "name": holding.name,
                    "weight": holding.weight,
                    "shares_held": holding.shares_held,
                    "day_change_pct": holding_metrics["day_change_pct"],
                    "daily_rs_rating": _finite_float(technical.get("daily_rs_rating")),
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
            "atr_pct": metrics["atr_pct"],
            "volume": metrics["volume"],
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
        "latest_date": latest_date,
    }


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
