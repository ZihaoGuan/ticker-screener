from __future__ import annotations

import datetime as dt
import re

from .constants import METRIC_LABEL_TO_FIELD, RATING_STATUS_SCRAPE_FAILED
from .models import FinvizProbeResult, FundamentalsSnapshot

_MULTIPLIERS = {"K": 1_000.0, "M": 1_000_000.0, "B": 1_000_000_000.0, "T": 1_000_000_000_000.0}


def _coerce_number(value: str | None) -> float | None:
    text = str(value or "").strip().replace(",", "")
    if not text or text in {"-", "N/A"}:
        return None
    sign = -1.0 if text.startswith("-") else 1.0
    text = text.lstrip("+-")
    if text.endswith("%"):
        text = text[:-1]
    suffix = text[-1:] if text else ""
    multiplier = _MULTIPLIERS.get(suffix, 1.0)
    if multiplier != 1.0:
        text = text[:-1]
    try:
        return sign * float(text) * multiplier
    except ValueError:
        return None


def _parse_eps_sales_surprise(value: str | None) -> tuple[float | None, float | None]:
    text = str(value or "").strip()
    if not text:
        return None, None
    parts = [part for part in re.split(r"\s+", text) if part]
    if len(parts) >= 2:
        return _coerce_number(parts[0]), _coerce_number(parts[1])
    number = _coerce_number(text)
    return number, None


def _parse_volatility(value: str | None) -> tuple[float | None, float | None]:
    text = str(value or "").strip()
    if not text:
        return None, None
    parts = [part for part in re.split(r"\s+", text) if part]
    if len(parts) >= 2:
        return _coerce_number(parts[0]), _coerce_number(parts[-1])
    number = _coerce_number(text)
    return number, number


def _clean_meta(value: str | None) -> str | None:
    normalized = str(value or "").strip()
    if not normalized:
        return None
    if re.match(r"^[+-]?\d", normalized):
        return None
    if normalized in {"•", "|"}:
        return None
    return normalized or None


def _parse_date(value: str | None) -> dt.date | None:
    text = str(value or "").strip()
    if not text or text in {"-", "N/A"}:
        return None
    for fmt in ("%b %d, %Y", "%B %d, %Y", "%Y-%m-%d", "%m/%d/%Y", "%b-%d-%Y", "%b-%d-%y"):
        try:
            return dt.datetime.strptime(text, fmt).date()
        except ValueError:
            continue
    return None


def _fallback_sector_from_body(body_excerpt: str) -> tuple[str | None, str | None]:
    lines = [line.strip() for line in str(body_excerpt or "").splitlines() if line.strip()]
    for index, line in enumerate(lines):
        if re.fullmatch(r"[A-Z]{1,6}", line):
            for offset in range(index + 1, min(len(lines), index + 12)):
                parts = [part.strip() for part in lines[offset].split("•") if part.strip()]
                if len(parts) >= 2:
                    return parts[0], parts[1]
    return None, None


def parse_finviz_probe(
    probe: FinvizProbeResult,
    *,
    as_of_date: dt.date,
    fallback_sector: str | None = None,
    fallback_industry: str | None = None,
) -> FundamentalsSnapshot:
    snapshot = FundamentalsSnapshot(
        ticker=probe.ticker,
        as_of_date=as_of_date,
        sector=_clean_meta(fallback_sector),
        industry=_clean_meta(fallback_industry),
        source_url=probe.final_url or probe.source_url,
        parse_status="ok",
    )
    if probe.status_code != 200:
        snapshot.parse_status = RATING_STATUS_SCRAPE_FAILED
        snapshot.parse_error = f"Unexpected HTTP status: {probe.status_code}"
        return snapshot

    for label, raw_value in probe.metric_pairs:
        if label == "EPS next Y":
            numeric_value = _coerce_number(raw_value)
            if raw_value.endswith("%"):
                snapshot.eps_next_y_pct = numeric_value
            continue
        if label in {"IPO", "IPO Date"}:
            snapshot.ipo_date = _parse_date(raw_value)
            continue
        if label == "Volatility":
            week_value, month_value = _parse_volatility(raw_value)
            if week_value is not None:
                snapshot.volatility_week_pct = week_value
            if month_value is not None:
                snapshot.volatility_month_pct = month_value
            continue
        if label == "EPS/Sales Surpr.":
            eps_surprise, sales_surprise = _parse_eps_sales_surprise(raw_value)
            snapshot.eps_surprise_pct = eps_surprise
            snapshot.sales_surprise_pct = sales_surprise
            continue
        field_name = METRIC_LABEL_TO_FIELD.get(label)
        if not field_name:
            continue
        parsed_value = _coerce_number(raw_value)
        if parsed_value is None and getattr(snapshot, field_name) is not None:
            continue
        setattr(snapshot, field_name, parsed_value)

    sector = _clean_meta(probe.sector)
    industry = _clean_meta(probe.industry)
    if not sector or not industry:
        body_sector, body_industry = _fallback_sector_from_body(probe.body_excerpt)
        sector = sector or body_sector
        industry = industry or body_industry
    snapshot.sector = sector or snapshot.sector
    snapshot.industry = industry or snapshot.industry
    if snapshot.source_url == "":
        snapshot.source_url = probe.source_url
    return snapshot
