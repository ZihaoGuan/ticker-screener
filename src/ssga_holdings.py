from __future__ import annotations

import datetime as dt
import json
import re
from io import BytesIO
from pathlib import Path
from typing import Any
from zipfile import ZipFile
import xml.etree.ElementTree as ET

import requests


SSGA_HOLDINGS_URL_TEMPLATE = "https://www.ssga.com/library-content/products/fund-data/etfs/us/holdings-daily-us-en-{ticker}.xlsx"
_XLSX_NS = {"a": "http://schemas.openxmlformats.org/spreadsheetml/2006/main"}
_NON_HOLDING_NAME_MARKERS = ("US DOLLAR", "MONEY MARKET", "EMINI", "E-MINI", "FUTURE")
_TICKER_PATTERN = re.compile(r"^[A-Z][A-Z0-9.\-]{0,9}$")


def holdings_url_for_ticker(ticker: str) -> str:
    return SSGA_HOLDINGS_URL_TEMPLATE.format(ticker=str(ticker or "").strip().lower())


def holdings_cache_dir(artifacts_dir: Path) -> Path:
    return Path(artifacts_dir) / "sector_etf_holdings"


def holdings_cache_path(artifacts_dir: Path) -> Path:
    return holdings_cache_dir(artifacts_dir) / "latest.json"


def load_holdings_cache(artifacts_dir: Path) -> dict[str, Any] | None:
    path = holdings_cache_path(artifacts_dir)
    if not path.exists():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    return payload if isinstance(payload, dict) else None


def refresh_ssga_holdings_cache(
    *,
    etf_tickers: list[str],
    artifacts_dir: Path,
    timeout_seconds: float = 30.0,
) -> dict[str, Any]:
    normalized = [ticker.strip().upper() for ticker in etf_tickers if ticker.strip()]
    fetched_at = dt.datetime.now(dt.timezone.utc).replace(microsecond=0).isoformat()
    results: dict[str, Any] = {}
    errors: dict[str, str] = {}

    for ticker in normalized:
        source_url = holdings_url_for_ticker(ticker)
        try:
            response = requests.get(source_url, timeout=timeout_seconds)
            response.raise_for_status()
            results[ticker] = parse_ssga_holdings_xlsx(response.content, etf_ticker=ticker, source_url=source_url)
        except Exception as exc:
            errors[ticker] = str(exc)

    all_holding_tickers = sorted(
        {
            str(holding.get("ticker") or "").upper()
            for result in results.values()
            for holding in result.get("holdings", [])
            if str(holding.get("ticker") or "").strip()
        }
    )
    payload = {
        "provider": "State Street Global Advisors",
        "generated_at": fetched_at,
        "source": "ssga-daily-xlsx",
        "etf_count": len(results),
        "requested_etfs": normalized,
        "holding_tickers": all_holding_tickers,
        "results": results,
        "errors": errors,
    }
    output_dir = holdings_cache_dir(artifacts_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    latest_path = holdings_cache_path(artifacts_dir)
    latest_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    date_label = dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%d")
    dated_path = output_dir / f"sector_etf_holdings_{date_label}.json"
    dated_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    return {**payload, "output_file": str(latest_path), "dated_output_file": str(dated_path)}


def parse_ssga_holdings_xlsx(content: bytes, *, etf_ticker: str, source_url: str) -> dict[str, Any]:
    rows = _read_first_sheet_rows(content)
    fund_name = _cell(rows, 0, 1)
    as_of_date = _parse_as_of_date(_cell(rows, 2, 1))
    header_index = _find_header_row(rows)
    if header_index is None:
        raise ValueError(f"Could not find holdings header row for {etf_ticker}.")
    header = [str(value).strip().lower() for value in rows[header_index]]
    column_map = {name: index for index, name in enumerate(header)}
    required = {"name", "ticker", "weight"}
    missing = sorted(required - set(column_map))
    if missing:
        raise ValueError(f"Missing SSGA holdings columns for {etf_ticker}: {', '.join(missing)}")

    holdings: list[dict[str, Any]] = []
    for row in rows[header_index + 1 :]:
        name = _row_value(row, column_map.get("name")).strip()
        ticker = _row_value(row, column_map.get("ticker")).strip().upper()
        if not _is_equity_holding(name=name, ticker=ticker):
            continue
        weight = _coerce_float(_row_value(row, column_map.get("weight")))
        if weight is None or weight <= 0:
            continue
        shares_held = _coerce_float(_row_value(row, column_map.get("shares held")))
        holdings.append(
            {
                "name": name,
                "ticker": ticker,
                "identifier": _row_value(row, column_map.get("identifier")).strip(),
                "sedol": _row_value(row, column_map.get("sedol")).strip(),
                "weight": weight,
                "sector": _row_value(row, column_map.get("sector")).strip(),
                "shares_held": shares_held,
                "local_currency": _row_value(row, column_map.get("local currency")).strip(),
            }
        )

    holdings.sort(key=lambda item: float(item.get("weight") or 0.0), reverse=True)
    return {
        "etf_ticker": etf_ticker.strip().upper(),
        "fund_name": fund_name,
        "as_of_date": as_of_date,
        "source_url": source_url,
        "holding_count": len(holdings),
        "holdings": holdings,
    }


def _read_first_sheet_rows(content: bytes) -> list[list[str]]:
    with ZipFile(BytesIO(content)) as workbook:
        shared_strings = _read_shared_strings(workbook)
        sheet_name = "xl/worksheets/sheet1.xml"
        root = ET.fromstring(workbook.read(sheet_name))
        rows: list[list[str]] = []
        for row in root.findall(".//a:row", _XLSX_NS):
            values: list[str] = []
            for cell in row.findall("a:c", _XLSX_NS):
                ref = str(cell.get("r") or "")
                column_index = _column_index_from_ref(ref)
                while len(values) < column_index:
                    values.append("")
                values.append(_read_cell_value(cell, shared_strings))
            rows.append(values)
    return rows


def _read_shared_strings(workbook: ZipFile) -> list[str]:
    if "xl/sharedStrings.xml" not in workbook.namelist():
        return []
    root = ET.fromstring(workbook.read("xl/sharedStrings.xml"))
    strings: list[str] = []
    for item in root.findall("a:si", _XLSX_NS):
        strings.append("".join(text.text or "" for text in item.findall(".//a:t", _XLSX_NS)))
    return strings


def _read_cell_value(cell: ET.Element, shared_strings: list[str]) -> str:
    value = cell.find("a:v", _XLSX_NS)
    raw = value.text if value is not None and value.text is not None else ""
    if cell.get("t") == "s" and raw:
        return shared_strings[int(raw)]
    return raw


def _column_index_from_ref(ref: str) -> int:
    letters = "".join(char for char in ref if char.isalpha()).upper()
    index = 0
    for char in letters:
        index = index * 26 + (ord(char) - ord("A") + 1)
    return max(0, index - 1)


def _find_header_row(rows: list[list[str]]) -> int | None:
    for index, row in enumerate(rows):
        normalized = {str(value).strip().lower() for value in row}
        if {"name", "ticker", "weight"}.issubset(normalized):
            return index
    return None


def _cell(rows: list[list[str]], row_index: int, column_index: int) -> str:
    if row_index >= len(rows) or column_index >= len(rows[row_index]):
        return ""
    return str(rows[row_index][column_index] or "").strip()


def _row_value(row: list[str], index: int | None) -> str:
    if index is None or index >= len(row):
        return ""
    return str(row[index] or "")


def _parse_as_of_date(value: str) -> str:
    cleaned = value.replace("As of", "").strip()
    for fmt in ("%d-%b-%Y", "%d-%B-%Y", "%Y-%m-%d"):
        try:
            return dt.datetime.strptime(cleaned, fmt).date().isoformat()
        except ValueError:
            continue
    return cleaned


def _is_equity_holding(*, name: str, ticker: str) -> bool:
    if not ticker or ticker == "-":
        return False
    if not _TICKER_PATTERN.match(ticker):
        return False
    normalized_name = name.upper()
    return not any(marker in normalized_name for marker in _NON_HOLDING_NAME_MARKERS)


def _coerce_float(value: str) -> float | None:
    try:
        return float(str(value).replace(",", "").strip())
    except (TypeError, ValueError):
        return None
