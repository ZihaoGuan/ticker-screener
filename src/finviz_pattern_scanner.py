from __future__ import annotations

import datetime as dt
from typing import Any, Sequence


FINVIZ_PATTERN_OPTIONS: tuple[tuple[str, str], ...] = (
    ("horizontal", "Horizontal S/R"),
    ("horizontal2", "Horizontal S/R (Strong)"),
    ("tlresistance", "TL Resistance"),
    ("tlresistance2", "TL Resistance (Strong)"),
    ("tlsupport", "TL Support"),
    ("tlsupport2", "TL Support (Strong)"),
    ("wedgeup", "Wedge Up"),
    ("wedgeup2", "Wedge Up (Strong)"),
    ("wedgedown", "Wedge Down"),
    ("wedgedown2", "Wedge Down (Strong)"),
    ("wedgeresistance", "Triangle Ascending"),
    ("wedgeresistance2", "Triangle Ascending (Strong)"),
    ("wedgesupport", "Triangle Descending"),
    ("wedgesupport2", "Triangle Descending (Strong)"),
    ("wedge", "Wedge"),
    ("wedge2", "Wedge (Strong)"),
    ("channelup", "Channel Up"),
    ("channelup2", "Channel Up (Strong)"),
    ("channeldown", "Channel Down"),
    ("channeldown2", "Channel Down (Strong)"),
    ("channel", "Channel"),
    ("channel2", "Channel (Strong)"),
    ("doubletop", "Double Top"),
    ("doublebottom", "Double Bottom"),
    ("multipletop", "Multiple Top"),
    ("multiplebottom", "Multiple Bottom"),
    ("headandshoulders", "Head & Shoulders"),
    ("headandshouldersinv", "Head & Shoulders Inverse"),
)
FINVIZ_PATTERN_LABELS: dict[str, str] = dict(FINVIZ_PATTERN_OPTIONS)


def _load_finviz_screener() -> type[Any]:
    try:
        from finviz.screener import Screener
    except ImportError as exc:
        raise RuntimeError(
            "finviz dependency missing. Install requirements-finviz.txt or requirements.txt before running Finviz pattern scanner."
        ) from exc
    return Screener


def _normalize_ticker_list(tickers: Sequence[str] | None) -> set[str]:
    normalized: set[str] = set()
    for item in tickers or ():
        ticker = str(item or "").strip().upper()
        if ticker:
            normalized.add(ticker)
    return normalized


def build_finviz_pattern_strategy_id(pattern: str) -> str:
    normalized = str(pattern or "").strip().lower()
    return f"finviz_pattern_{normalized}"


def resolve_finviz_pattern_label(pattern: str) -> str:
    normalized = str(pattern or "").strip().lower()
    label = FINVIZ_PATTERN_LABELS.get(normalized)
    if not label:
        raise ValueError(f"Unknown Finviz pattern: {pattern}")
    return label


def resolve_finviz_pattern_filter(pattern: str) -> str:
    normalized = str(pattern or "").strip().lower()
    if normalized not in FINVIZ_PATTERN_LABELS:
        raise ValueError(f"Unknown Finviz pattern: {pattern}")
    return f"ta_pattern_{normalized}"


def _normalize_hit(row: dict[str, Any], *, pattern: str, pattern_label: str, strategy_id: str) -> dict[str, Any]:
    ticker = str(row.get("Ticker") or "").strip().upper()
    payload = dict(row)
    payload["ticker"] = ticker
    payload["company_name"] = str(row.get("Company") or "").strip()
    payload["strategy_id"] = strategy_id
    payload["source"] = "finviz"
    payload["pattern"] = pattern
    payload["pattern_label"] = pattern_label
    payload["event_label"] = pattern_label
    return payload


def run_finviz_pattern_scanner(
    *,
    pattern: str,
    limit: int | None = None,
    tickers: Sequence[str] | None = None,
) -> dict[str, Any]:
    normalized_pattern = str(pattern or "").strip().lower()
    pattern_label = resolve_finviz_pattern_label(normalized_pattern)
    filter_token = resolve_finviz_pattern_filter(normalized_pattern)
    strategy_id = build_finviz_pattern_strategy_id(normalized_pattern)

    screener_cls = _load_finviz_screener()
    screener = screener_cls(filters=[filter_token], table="Overview", order="ticker")
    requested_tickers = _normalize_ticker_list(tickers)
    hits: list[dict[str, Any]] = []

    for row in screener:
        normalized = _normalize_hit(
            dict(row),
            pattern=normalized_pattern,
            pattern_label=pattern_label,
            strategy_id=strategy_id,
        )
        ticker = str(normalized.get("ticker") or "")
        if requested_tickers and ticker not in requested_tickers:
            continue
        hits.append(normalized)
        if limit is not None and len(hits) >= limit:
            break

    return {
        "strategy_id": strategy_id,
        "screen_name": f"Finviz Pattern: {pattern_label}",
        "source": "finviz.screener",
        "pattern": normalized_pattern,
        "pattern_label": pattern_label,
        "finviz_filter": filter_token,
        "fetched_at": dt.datetime.now(dt.UTC).isoformat(),
        "total_candidates": len(screener),
        "returned_candidates": len(hits),
        "requested_tickers": sorted(requested_tickers),
        "hits": hits,
    }
