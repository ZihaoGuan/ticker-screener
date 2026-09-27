from __future__ import annotations

import datetime as dt
import json
import re
from dataclasses import dataclass
from io import StringIO
from pathlib import Path
from typing import Any, Callable

import pandas as pd
import requests


@dataclass(frozen=True)
class MomentumEtf:
    ticker: str
    name: str
    provider: str
    source_url: str
    fallback_url: str = ""
    official_holdings_url: str = ""
    full_fallback_url: str = ""


MOMENTUM_ETFS: tuple[MomentumEtf, ...] = (
    MomentumEtf("FMTM", "MarketDesk Focused U.S. Momentum ETF", "MarketDesk", "https://www.marketdeskindices.com/fmtm", "https://stockanalysis.com/etf/fmtm/holdings/", full_fallback_url="https://www.etfchannel.com/lists/?a=stockholdings&issuer=&reverse=&rpp=20&sortby=&start={page}&symbol=FMTM"),
    MomentumEtf("SPMO", "Invesco S&P 500 Momentum ETF", "Invesco", "https://www.invesco.com/us/en/financial-products/etfs/invesco-sp-500-momentum-etf.html", "https://stockanalysis.com/etf/spmo/holdings/", official_holdings_url="https://dng-api.invesco.com/cache/v1/accounts/en_US/shareclasses/46138E339/holdings/fund?idType=cusip&productType=ETF", full_fallback_url="https://www.etfchannel.com/lists/?a=stockholdings&issuer=&reverse=&rpp=20&sortby=&start={page}&symbol=SPMO"),
    MomentumEtf("PTF", "Invesco Dorsey Wright Technology Momentum ETF", "Invesco", "https://www.invesco.com/us/en/financial-products/etfs/invesco-dorsey-wright-technology-momentum-etf.html", "https://stockanalysis.com/etf/ptf/holdings/", official_holdings_url="https://dng-api.invesco.com/cache/v1/accounts/en_US/shareclasses/46137V811/holdings/fund?idType=cusip&productType=ETF", full_fallback_url="https://www.etfchannel.com/lists/?a=stockholdings&issuer=&reverse=&rpp=20&sortby=&start={page}&symbol=PTF"),
    MomentumEtf("FFTY", "CapForce IBD 50 ETF", "CapForce", "https://www.capforceetf.com/ffty/details"),
)

_TICKER_PATTERN = re.compile(r"^[A-Z][A-Z0-9.\-]{0,11}$")
_NON_EQUITY_TICKERS = {"CASH", "CASH&OTHER", "USD", "FXFXX"}
_NON_HOLDING_MARKERS = ("CASH", "MONEY MARKET", "TREASURY", "GOVERNMENT OBLIGATIONS", "OTHER")


def momentum_etf_holdings_cache_dir(artifacts_dir: Path) -> Path:
    return Path(artifacts_dir) / "momentum_etf_holdings"


def momentum_etf_holdings_cache_path(artifacts_dir: Path) -> Path:
    return momentum_etf_holdings_cache_dir(artifacts_dir) / "latest.json"


def load_momentum_etf_holdings_cache(artifacts_dir: Path) -> dict[str, Any] | None:
    path = momentum_etf_holdings_cache_path(artifacts_dir)
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    return payload if isinstance(payload, dict) else None


def refresh_momentum_etf_holdings_cache(
    *,
    artifacts_dir: Path,
    etfs: tuple[MomentumEtf, ...] = MOMENTUM_ETFS,
    timeout_seconds: float = 30.0,
    get: Callable[..., Any] = requests.get,
) -> dict[str, Any]:
    """Fetch issuer-published portfolio pages and retain the last good cache on total failure."""
    generated_at = dt.datetime.now(dt.timezone.utc).replace(microsecond=0).isoformat()
    results: dict[str, Any] = {}
    errors: dict[str, str] = {}
    for etf in etfs:
        failures: list[str] = []
        candidates: list[dict[str, Any]] = []
        if etf.official_holdings_url:
            try:
                response = _get_holdings_response(get, etf.official_holdings_url, timeout_seconds)
                result = parse_invesco_holdings_json(response.text, etf=etf, fetched_at=generated_at)
                result.update(source_url=etf.official_holdings_url, issuer_source_url=etf.source_url, source_kind="issuer")
                candidates.append(result)
            except Exception as exc:
                failures.append(str(exc))
        for source_url, source_kind in ((etf.source_url, "issuer"), (etf.fallback_url, "fallback")):
            if not source_url:
                continue
            try:
                response = _get_holdings_response(get, source_url, timeout_seconds)
                result = parse_momentum_etf_holdings_html(response.text, etf=etf, fetched_at=generated_at)
                result["source_url"] = source_url
                result["issuer_source_url"] = etf.source_url
                result["source_kind"] = source_kind
                candidates.append(result)
            except Exception as exc:
                failures.append(str(exc))
        if etf.full_fallback_url:
            try:
                result = fetch_paginated_holdings(etf=etf, url_template=etf.full_fallback_url, fetched_at=generated_at, timeout_seconds=timeout_seconds, get=get)
                result.update(issuer_source_url=etf.source_url, source_kind="full_fallback")
                candidates.append(result)
            except Exception as exc:
                failures.append(str(exc))
        if candidates:
            results[etf.ticker] = max(candidates, key=_holdings_candidate_rank)
        else:
            errors[etf.ticker] = "; ".join(failures)

    output_dir = momentum_etf_holdings_cache_dir(artifacts_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    latest_path = momentum_etf_holdings_cache_path(artifacts_dir)
    dated_path = output_dir / f"momentum_etf_holdings_{dt.datetime.now(dt.timezone.utc).strftime('%Y-%m-%d')}.json"
    payload = {
        "generated_at": generated_at,
        "source": "issuer-holdings-pages",
        "requested_etfs": [etf.ticker for etf in etfs],
        "etf_count": len(results),
        "holding_tickers": sorted({holding["ticker"] for result in results.values() for holding in result["holdings"]}),
        "results": results,
        "errors": errors,
    }
    if results:
        serialized = json.dumps(payload, indent=2, sort_keys=True)
        latest_path.write_text(serialized, encoding="utf-8")
        dated_path.write_text(serialized, encoding="utf-8")
    return {**payload, "output_file": str(latest_path), "dated_output_file": str(dated_path), "updated_cache": bool(results)}


def _get_holdings_response(get: Callable[..., Any], url: str, timeout_seconds: float) -> Any:
    response = get(url, timeout=timeout_seconds, headers={
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/124 Safari/537.36",
        "Accept": "application/json,*/*",
        "Referer": "https://www.invesco.com/",
    })
    response.raise_for_status()
    return response


def _holdings_candidate_rank(result: dict[str, Any]) -> tuple[int, int, int]:
    source_priority = {"issuer": 2, "full_fallback": 1, "fallback": 0}
    return (
        int(bool(result.get("is_complete"))),
        int(result.get("holding_count") or 0),
        source_priority.get(str(result.get("source_kind") or ""), 0),
    )


def parse_invesco_holdings_json(raw: str, *, etf: MomentumEtf, fetched_at: str = "") -> dict[str, Any]:
    payload = json.loads(raw)
    rows = payload.get("holdings") if isinstance(payload, dict) else None
    if not isinstance(rows, list):
        raise ValueError(f"No recognizable Invesco holdings payload found for {etf.ticker}.")
    normalized_rows = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        ticker = _clean_ticker(row.get("ticker"))
        name = _clean_text(row.get("issuerName"))
        weight = _coerce_percent(row.get("percentageOfTotalNetAssets") if row.get("percentageOfTotalNetAssets") is not None else row.get("weight"))
        if _is_equity_holding(ticker=ticker, name=name) and weight is not None and weight > 0:
            normalized_rows.append({"ticker": ticker, "name": name, "weight": weight, "sector": _clean_text(row.get("sectorName"))})
    holdings = _dedupe_holdings(normalized_rows)
    if not holdings:
        raise ValueError(f"No equity holdings found in Invesco payload for {etf.ticker}.")
    reported_count = int(payload.get("totalNumberOfHoldings") or len(rows))
    return {
        "etf_ticker": etf.ticker,
        "fund_name": etf.name,
        "provider": etf.provider,
        "source_url": etf.source_url,
        "as_of_date": _normalize_date(payload.get("effectiveDate")),
        "fetched_at": fetched_at,
        "holding_count": len(holdings),
        "reported_holding_count": reported_count,
        "is_complete": True,
        "holdings": holdings,
    }


def fetch_paginated_holdings(*, etf: MomentumEtf, url_template: str, fetched_at: str, timeout_seconds: float, get: Callable[..., Any]) -> dict[str, Any]:
    first_url = url_template.format(page=0)
    first_response = _get_holdings_response(get, first_url, timeout_seconds)
    first_rows, page_count = parse_etf_channel_holdings_html(first_response.text)
    rows = list(first_rows)
    for page in range(1, page_count):
        response = _get_holdings_response(get, url_template.format(page=page), timeout_seconds)
        page_rows, _ = parse_etf_channel_holdings_html(response.text)
        rows.extend(page_rows)
    holdings = _dedupe_holdings(rows)
    if not holdings:
        raise ValueError(f"No paginated holdings found for {etf.ticker}.")
    return {
        "etf_ticker": etf.ticker,
        "fund_name": etf.name,
        "provider": etf.provider,
        "source_url": first_url,
        "as_of_date": "",
        "fetched_at": fetched_at,
        "holding_count": len(holdings),
        "reported_holding_count": len(holdings),
        "is_complete": True,
        "holdings": holdings,
    }


def parse_etf_channel_holdings_html(html: str) -> tuple[list[dict[str, Any]], int]:
    page_match = re.search(r"Stock Holdings Page\s+\d+\s+of\s+(\d+)", html, flags=re.IGNORECASE)
    page_count = int(page_match.group(1)) if page_match else 1
    pattern = re.compile(
        r'<a\s+href="/symbol/([^/\"]+)/">([^<]+)</a>.*?</td>\s*'
        r'<td[^>]*>.*?([0-9]+(?:\.[0-9]+)?)%.*?</td>',
        flags=re.IGNORECASE | re.DOTALL,
    )
    rows = []
    for ticker, name, weight in pattern.findall(html):
        normalized_ticker = _clean_ticker(ticker)
        normalized_name = _clean_text(name)
        if _is_equity_holding(ticker=normalized_ticker, name=normalized_name):
            rows.append({"ticker": normalized_ticker, "name": normalized_name, "weight": float(weight), "sector": ""})
    if not rows:
        raise ValueError("No recognizable paginated holdings table found.")
    return rows, page_count


def _normalize_date(value: object) -> str:
    text = _clean_text(value)
    if not text:
        return ""
    for candidate in (text, text[:10]):
        for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%Y/%m/%d"):
            try:
                return dt.datetime.strptime(candidate, fmt).date().isoformat()
            except ValueError:
                pass
    return ""


def parse_momentum_etf_holdings_html(html: str, *, etf: MomentumEtf, fetched_at: str = "") -> dict[str, Any]:
    """Normalize issuer table variants without coupling the board to a provider's page format."""
    rows: list[dict[str, Any]] = []
    as_of_date = _extract_as_of_date(html)
    try:
        tables = pd.read_html(StringIO(html))
    except ValueError:
        tables = []
    for table in tables:
        normalized = {_normalize_column(column): column for column in table.columns}
        ticker_column = _first_column(normalized, "ticker", "symbol")
        weight_column = _first_column(normalized, "weight", "weightfund", "percentoffund", "portfolioweight")
        name_column = _first_column(normalized, "name", "company", "description", "security")
        sector_column = _first_column(normalized, "sector", "gicssector")
        if ticker_column is None or weight_column is None:
            continue
        for _, raw in table.iterrows():
            ticker = _clean_ticker(raw.get(ticker_column))
            name = _clean_text(raw.get(name_column)) if name_column else ""
            weight = _coerce_percent(raw.get(weight_column))
            if not _is_equity_holding(ticker=ticker, name=name) or weight is None or weight <= 0:
                continue
            rows.append({
                "ticker": ticker,
                "name": name,
                "weight": weight,
                "sector": _clean_text(raw.get(sector_column)) if sector_column else "",
            })
    holdings = _dedupe_holdings(rows)
    if not holdings:
        raise ValueError(f"No recognizable issuer holdings table found for {etf.ticker}.")
    return {
        "etf_ticker": etf.ticker,
        "fund_name": etf.name,
        "provider": etf.provider,
        "source_url": etf.source_url,
        "as_of_date": as_of_date,
        "fetched_at": fetched_at,
        "holding_count": len(holdings),
        "reported_holding_count": _extract_reported_holding_count(html) or len(holdings),
        "is_complete": len(holdings) >= (_extract_reported_holding_count(html) or len(holdings)),
        "holdings": holdings,
    }


def _normalize_column(value: object) -> str:
    return re.sub(r"[^a-z0-9]", "", str(value or "").lower())


def _first_column(columns: dict[str, object], *candidates: str) -> object | None:
    for candidate in candidates:
        if candidate in columns:
            return columns[candidate]
    return None


def _clean_text(value: object) -> str:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return ""
    return str(value).strip()


def _clean_ticker(value: object) -> str:
    ticker = _clean_text(value).upper().replace(" ", "")
    return ticker if _TICKER_PATTERN.match(ticker) else ""


def _coerce_percent(value: object) -> float | None:
    text = _clean_text(value).replace("%", "").replace(",", "")
    try:
        return float(text)
    except ValueError:
        return None


def _extract_as_of_date(html: str) -> str:
    values = re.findall(r"(?:as\s+of)\s*[:\-]?\s*([A-Z][a-z]+\s+\d{1,2},\s+\d{4}|\d{1,2}/\d{1,2}/\d{4}|\d{4}-\d{2}-\d{2})", html, flags=re.IGNORECASE)
    dates: list[dt.date] = []
    for value in values:
        for fmt in ("%B %d, %Y", "%b %d, %Y", "%m/%d/%Y", "%Y-%m-%d"):
            try:
                dates.append(dt.datetime.strptime(value, fmt).date())
                break
            except ValueError:
                pass
    return max(dates).isoformat() if dates else ""


def _extract_reported_holding_count(html: str) -> int | None:
    patterns = (r"(?:total of|show all \()\s*(\d+)\s*(?:individual )?holdings", r"show all \((\d+)\)")
    for pattern in patterns:
        match = re.search(pattern, html, flags=re.IGNORECASE)
        if match:
            return int(match.group(1))
    return None


def _is_equity_holding(*, ticker: str, name: str) -> bool:
    if not ticker or ticker in _NON_EQUITY_TICKERS:
        return False
    return not any(marker in name.upper() for marker in _NON_HOLDING_MARKERS)


def _dedupe_holdings(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    by_ticker: dict[str, dict[str, Any]] = {}
    for row in rows:
        ticker = str(row["ticker"])
        current = by_ticker.get(ticker)
        if current is None or float(row["weight"]) > float(current["weight"]):
            by_ticker[ticker] = row
    return sorted(by_ticker.values(), key=lambda item: (-float(item["weight"]), str(item["ticker"])))
