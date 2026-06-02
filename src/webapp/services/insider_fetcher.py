from __future__ import annotations

import datetime as dt
import json
from pathlib import Path
from typing import Any
import xml.etree.ElementTree as ET

import requests


SEC_ARCHIVES_BASE = "https://www.sec.gov/Archives/edgar/data"
SEC_SUBMISSIONS_BASE = "https://data.sec.gov/submissions"
SEC_TICKER_URL = "https://www.sec.gov/include/ticker.txt"
DEFAULT_USER_AGENT = "ticker-screener insider fetcher contact@example.com"
REQUEST_TIMEOUT = (10, 30)


def fetch_insider_trades_window(
    *,
    tickers: list[str],
    as_of_date: dt.date,
    lookback_days: int,
    min_gross_amount: float = 0.0,
    user_agent: str = DEFAULT_USER_AGENT,
    session: requests.Session | None = None,
) -> dict[str, Any]:
    normalized_tickers = [normalize_ticker(raw) for raw in tickers if normalize_ticker(raw)]
    if not normalized_tickers:
        return {
            "generated_at": dt.datetime.now(dt.timezone.utc).replace(microsecond=0).isoformat(),
            "source": "sec_form4_submissions",
            "requested_tickers": [],
            "lookback_days": max(1, int(lookback_days)),
            "as_of_date": as_of_date.isoformat(),
            "entries": [],
        }

    active_session = session or requests.Session()
    active_session.headers.update(
        {
            "User-Agent": user_agent,
            "Accept-Encoding": "gzip, deflate",
        }
    )

    ticker_map = download_ticker_map(active_session)
    entries: list[dict[str, Any]] = []
    for ticker in normalized_tickers:
        cik = ticker_map.get(ticker)
        if cik is None:
            continue
        entries.extend(
            fetch_ticker_entries(
                session=active_session,
                ticker=ticker,
                cik=cik,
                as_of_date=as_of_date,
                lookback_days=max(1, int(lookback_days)),
                min_gross_amount=max(0.0, float(min_gross_amount)),
            )
        )

    return {
        "generated_at": dt.datetime.now(dt.timezone.utc).replace(microsecond=0).isoformat(),
        "source": "sec_form4_submissions",
        "requested_tickers": normalized_tickers,
        "lookback_days": max(1, int(lookback_days)),
        "as_of_date": as_of_date.isoformat(),
        "entries": sorted(
            entries,
            key=lambda item: (
                item.get("ticker", ""),
                item.get("transaction_date", ""),
                item.get("filing_date", ""),
                float(item.get("gross_amount") or 0.0),
            ),
            reverse=True,
        ),
    }


def write_insider_window_artifact(
    *,
    payload: dict[str, Any],
    output_path: Path,
) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def fetch_ticker_entries(
    *,
    session: requests.Session,
    ticker: str,
    cik: str,
    as_of_date: dt.date,
    lookback_days: int,
    min_gross_amount: float,
) -> list[dict[str, Any]]:
    submissions = session.get(
        f"{SEC_SUBMISSIONS_BASE}/CIK{cik.zfill(10)}.json",
        timeout=REQUEST_TIMEOUT,
    )
    submissions.raise_for_status()
    payload = submissions.json()
    recent = payload.get("filings", {}).get("recent", {})
    forms = recent.get("form", [])
    filing_dates = recent.get("filingDate", [])
    accession_numbers = recent.get("accessionNumber", [])
    primary_documents = recent.get("primaryDocument", [])
    if not all(isinstance(column, list) for column in (forms, filing_dates, accession_numbers, primary_documents)):
        return []

    start_date = as_of_date - dt.timedelta(days=lookback_days)
    raw_entries: list[dict[str, Any]] = []
    cik_numeric = cik.lstrip("0") or "0"
    for index, form in enumerate(forms):
        if str(form).strip() not in {"4", "4/A"}:
            continue
        filing_date_text = str(filing_dates[index]).strip()
        filing_date = parse_date(filing_date_text)
        if filing_date < start_date or filing_date > as_of_date:
            continue
        accession_number = str(accession_numbers[index]).strip()
        primary_document = str(primary_documents[index]).strip()
        if not accession_number or not primary_document:
            continue
        accession_path = accession_number.replace("-", "")
        xml_url = f"{SEC_ARCHIVES_BASE}/{cik_numeric}/{accession_path}/{primary_document}"
        xml_response = session.get(xml_url, timeout=REQUEST_TIMEOUT)
        xml_response.raise_for_status()
        raw_entries.extend(
            parse_form4(
                xml_text=xml_response.text,
                ticker=ticker,
                filing_date=filing_date_text,
                source_url=xml_url,
                min_gross_amount=min_gross_amount,
            )
        )

    return aggregate_entries(raw_entries)


def parse_form4(
    *,
    xml_text: str,
    ticker: str,
    filing_date: str,
    source_url: str,
    min_gross_amount: float,
) -> list[dict[str, Any]]:
    root = ET.fromstring(xml_text)
    footnotes = build_footnote_map(root)
    transactions = iter_non_derivative_transactions(root)
    entries: list[dict[str, Any]] = []

    for owner in find_children(root, "reportingOwner"):
        relation = find_child(owner, "reportingOwnerRelationship")
        if relation is None:
            continue
        if not is_truthy(text_of(find_child(relation, "isDirector"))) and not is_truthy(text_of(find_child(relation, "isOfficer"))):
            continue
        owner_name = text_of(find_descendant(owner, "rptOwnerName")) or "Unknown Owner"
        position = build_position(relation)

        for tx in transactions:
            if owner is not tx["owner"]:
                continue
            node = tx["transaction"]
            code = text_of(find_descendant(node, "transactionCode"))
            if code not in {"P", "S"}:
                continue
            shares = parse_float(text_of(find_descendant(node, "transactionShares")))
            price = parse_float(text_of(find_descendant(node, "transactionPricePerShare")))
            if shares <= 0 or price <= 0:
                continue
            gross_amount = shares * price
            if gross_amount < min_gross_amount:
                continue
            transaction_date = (text_of(find_descendant(node, "transactionDate")) or "")[:10]
            shares_owned_after = int(round(parse_float(text_of(find_descendant(node, "sharesOwnedFollowingTransaction")))))
            is_10b5_1 = detect_10b5_1(node, footnotes)
            trade_type = "BUY" if code == "P" else "SELL"
            entries.append(
                {
                    "ticker": ticker,
                    "filing_date": filing_date,
                    "transaction_date": transaction_date,
                    "owner_name": owner_name,
                    "position": position,
                    "type": trade_type,
                    "shares": int(round(shares)),
                    "price": round(price, 4),
                    "gross_amount": round(gross_amount, 2),
                    "net_amount": round(gross_amount if trade_type == "BUY" else -gross_amount, 2),
                    "shares_owned_after": shares_owned_after,
                    "is_10b5_1": is_10b5_1,
                    "source_url": source_url,
                }
            )
    return entries


def iter_non_derivative_transactions(root: ET.Element) -> list[dict[str, ET.Element]]:
    rows: list[dict[str, ET.Element]] = []
    for owner in find_children(root, "reportingOwner"):
        non_derivative = find_child(owner, "nonDerivativeTable")
        if non_derivative is None:
            continue
        for transaction in find_children(non_derivative, "nonDerivativeTransaction"):
            rows.append({"owner": owner, "transaction": transaction})
    if rows:
        return rows

    owners = find_children(root, "reportingOwner")
    owner = owners[0] if owners else root
    non_derivative = find_child(root, "nonDerivativeTable")
    if non_derivative is None:
        return []
    return [{"owner": owner, "transaction": transaction} for transaction in find_children(non_derivative, "nonDerivativeTransaction")]


def aggregate_entries(entries: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, str, str, str], list[dict[str, Any]]] = {}
    for entry in entries:
        key = (
            str(entry.get("ticker") or ""),
            str(entry.get("transaction_date") or ""),
            str(entry.get("owner_name") or ""),
            str(entry.get("type") or ""),
        )
        grouped.setdefault(key, []).append(entry)

    aggregated: list[dict[str, Any]] = []
    for rows in grouped.values():
        rows = sorted(rows, key=lambda item: str(item.get("filing_date") or ""))
        first = rows[0]
        gross_amount = round(sum(float(item.get("gross_amount") or 0.0) for item in rows), 2)
        shares = int(round(sum(float(item.get("shares") or 0.0) for item in rows)))
        weighted_price_numerator = sum(float(item.get("price") or 0.0) * float(item.get("shares") or 0.0) for item in rows)
        weighted_price = round(weighted_price_numerator / shares, 4) if shares > 0 else 0.0
        aggregated.append(
            {
                **first,
                "shares": shares,
                "price": weighted_price,
                "gross_amount": gross_amount,
                "net_amount": gross_amount if first.get("type") == "BUY" else -gross_amount,
                "shares_owned_after": int(rows[-1].get("shares_owned_after") or 0),
                "is_10b5_1": all(bool(item.get("is_10b5_1")) for item in rows),
            }
        )
    return aggregated


def download_ticker_map(session: requests.Session) -> dict[str, str]:
    response = session.get(SEC_TICKER_URL, timeout=REQUEST_TIMEOUT)
    response.raise_for_status()
    mapping: dict[str, str] = {}
    for raw_line in response.text.splitlines():
        parts = raw_line.strip().split("\t")
        if len(parts) != 2:
            continue
        symbol = normalize_ticker(parts[0])
        cik = str(parts[1]).strip()
        if symbol and cik:
            mapping[symbol] = cik
    return mapping


def normalize_ticker(value: str) -> str:
    return value.strip().upper().replace(".", "-")


def parse_date(value: str) -> dt.date:
    return dt.date.fromisoformat(value.strip()[:10])


def build_footnote_map(root: ET.Element) -> dict[str, str]:
    mapping: dict[str, str] = {}
    footnotes = find_child(root, "footnotes")
    if footnotes is None:
        return mapping
    for footnote in find_children(footnotes, "footnote"):
        footnote_id = footnote.attrib.get("id", "").strip()
        text = " ".join(part.strip() for part in footnote.itertext() if part.strip())
        if footnote_id and text:
            mapping[footnote_id] = text
    return mapping


def detect_10b5_1(node: ET.Element, footnotes: dict[str, str]) -> bool:
    field_value = text_of(find_descendant(node, "is10b5-1Transaction"))
    if is_truthy(field_value):
        return True
    for element in node.iter():
        for footnote_id in extract_footnote_ids(element):
            text = footnotes.get(footnote_id, "").lower()
            if "10b5-1" in text or "rule 10b5" in text:
                return True
    return False


def extract_footnote_ids(element: ET.Element) -> list[str]:
    ids: list[str] = []
    if local_name(element.tag) == "footnoteId":
        footnote_id = element.attrib.get("id", "").strip()
        if footnote_id:
            ids.append(footnote_id)
    for child in element:
        if local_name(child.tag) == "footnoteId":
            footnote_id = child.attrib.get("id", "").strip()
            if footnote_id:
                ids.append(footnote_id)
    return ids


def build_position(relation: ET.Element) -> str:
    parts: list[str] = []
    if is_truthy(text_of(find_child(relation, "isDirector"))):
        parts.append("Director")
    if is_truthy(text_of(find_child(relation, "isOfficer"))):
        parts.append("Officer")
    officer_title = text_of(find_child(relation, "officerTitle"))
    if officer_title:
        parts.append(officer_title)
    return ", ".join(dict.fromkeys(parts)) or "Insider"


def find_child(node: ET.Element, name: str) -> ET.Element | None:
    for child in node:
        if local_name(child.tag) == name:
            return child
    return None


def find_children(node: ET.Element, name: str) -> list[ET.Element]:
    return [child for child in node if local_name(child.tag) == name]


def find_descendant(node: ET.Element, name: str) -> ET.Element | None:
    for child in node.iter():
        if local_name(child.tag) == name:
            return child
    return None


def text_of(node: ET.Element | None) -> str:
    if node is None:
        return ""
    return "".join(part.strip() for part in node.itertext() if part and part.strip())


def local_name(tag: str) -> str:
    return tag.rsplit("}", 1)[-1]


def is_truthy(value: str) -> bool:
    return value.strip().lower() in {"1", "true", "y", "yes"}


def parse_float(value: str) -> float:
    try:
        return float(value.replace(",", "").strip())
    except ValueError:
        return 0.0
