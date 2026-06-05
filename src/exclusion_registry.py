from __future__ import annotations

import datetime as dt
import json
from pathlib import Path
from typing import Any

from .config import AppConfig, project_root
from .ticker_filters import (
    auto_excluded_tickers_dir,
    excluded_tickers_path,
    manual_included_tickers_path,
    manual_excluded_tickers_path,
    normalize_ticker_symbol,
    special_security_tickers_path,
)


def list_exclusion_entries(config: AppConfig) -> list[dict[str, Any]]:
    aggregated: dict[str, dict[str, Any]] = {}

    for path, source_kind, source_label in (
        (excluded_tickers_path(config), "configured", "Configured exclusions"),
        (manual_excluded_tickers_path(config), "manual", "Manual exclusions"),
    ):
        for record in _read_user_file_records(path):
            _merge_entry(
                aggregated,
                ticker=record["ticker"],
                source_kind=source_kind,
                source_label=source_label,
                reason=record["reason"] or source_label,
                removable=True,
            )

    auto_dir = auto_excluded_tickers_dir(config)
    if auto_dir.exists():
        for path in sorted(auto_dir.glob("*.txt")):
            label = _humanize_auto_source(path.stem)
            for record in _read_user_file_records(path):
                _merge_entry(
                    aggregated,
                    ticker=record["ticker"],
                    source_kind="auto",
                    source_label=label,
                    reason=record["reason"] or f"Auto rule from {label}",
                    removable=False,
                )

    for record in _read_special_security_records(special_security_tickers_path(config)):
        _merge_entry(
            aggregated,
            ticker=record["ticker"],
            source_kind="special",
            source_label="Special security filter",
            reason=record["reason"] or "Special security filter",
            removable=False,
        )

    return sorted(aggregated.values(), key=lambda item: item["ticker"])


def list_inclusion_entries(config: AppConfig) -> list[dict[str, Any]]:
    return sorted(_read_user_file_records(manual_included_tickers_path(config)), key=lambda item: item["ticker"])


def add_manual_exclusion(config: AppConfig, *, ticker: str, reason: str) -> dict[str, Any]:
    normalized = normalize_ticker_symbol(ticker)
    if not normalized:
        raise ValueError("Ticker is required.")
    records = _read_user_file_records(manual_excluded_tickers_path(config))
    record_map = {record["ticker"]: record for record in records}
    record_map[normalized] = {"ticker": normalized, "reason": reason.strip()}
    _write_user_file_records(manual_excluded_tickers_path(config), list(record_map.values()))
    _append_audit_log(action="add", ticker=normalized, reason=reason.strip(), source="manual")
    return {"ticker": normalized, "reason": reason.strip()}


def add_manual_inclusion(config: AppConfig, *, ticker: str, reason: str) -> dict[str, Any]:
    normalized = normalize_ticker_symbol(ticker)
    if not normalized:
        raise ValueError("Ticker is required.")
    records = _read_user_file_records(manual_included_tickers_path(config))
    record_map = {record["ticker"]: record for record in records}
    record_map[normalized] = {"ticker": normalized, "reason": reason.strip()}
    _write_user_file_records(manual_included_tickers_path(config), list(record_map.values()))
    _append_audit_log(action="include", ticker=normalized, reason=reason.strip(), source="manual_include")
    return {"ticker": normalized, "reason": reason.strip()}


def remove_user_exclusion(config: AppConfig, *, ticker: str, reason: str) -> dict[str, Any]:
    normalized = normalize_ticker_symbol(ticker)
    if not normalized:
        raise ValueError("Ticker is required.")

    removed_from: list[str] = []
    for path, source_label in (
        (manual_excluded_tickers_path(config), "Manual exclusions"),
        (excluded_tickers_path(config), "Configured exclusions"),
    ):
        records = _read_user_file_records(path)
        if not any(record["ticker"] == normalized for record in records):
            continue
        next_records = [record for record in records if record["ticker"] != normalized]
        _write_user_file_records(path, next_records)
        removed_from.append(source_label)

    if not removed_from:
        raise ValueError(f"{normalized} is not in a removable exclusion list.")

    _append_audit_log(action="remove", ticker=normalized, reason=reason.strip(), source=",".join(removed_from))
    return {"ticker": normalized, "removed_from": removed_from, "reason": reason.strip()}


def remove_manual_inclusion(config: AppConfig, *, ticker: str, reason: str) -> dict[str, Any]:
    normalized = normalize_ticker_symbol(ticker)
    if not normalized:
        raise ValueError("Ticker is required.")

    records = _read_user_file_records(manual_included_tickers_path(config))
    if not any(record["ticker"] == normalized for record in records):
        raise ValueError(f"{normalized} is not in manual inclusion list.")
    next_records = [record for record in records if record["ticker"] != normalized]
    _write_user_file_records(manual_included_tickers_path(config), next_records)
    _append_audit_log(action="uninclude", ticker=normalized, reason=reason.strip(), source="manual_include")
    return {"ticker": normalized, "reason": reason.strip()}


def _merge_entry(
    aggregated: dict[str, dict[str, Any]],
    *,
    ticker: str,
    source_kind: str,
    source_label: str,
    reason: str,
    removable: bool,
) -> None:
    entry = aggregated.setdefault(
        ticker,
        {
            "ticker": ticker,
            "reason": "",
            "reasons": [],
            "sources": [],
            "source_kinds": [],
            "removable": False,
        },
    )
    clean_reason = reason.strip()
    if clean_reason and clean_reason not in entry["reasons"]:
        entry["reasons"].append(clean_reason)
    if source_label not in entry["sources"]:
        entry["sources"].append(source_label)
    if source_kind not in entry["source_kinds"]:
        entry["source_kinds"].append(source_kind)
    if removable:
        entry["removable"] = True
    if not entry["reason"]:
        entry["reason"] = clean_reason or source_label


def _read_user_file_records(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    records: list[dict[str, str]] = []
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        tickers, reason = _parse_ticker_line(raw_line)
        for ticker in tickers:
            records.append({"ticker": ticker, "reason": reason})
    return records


def _read_special_security_records(path: Path) -> list[dict[str, str]]:
    import csv

    if not path.exists():
        return []
    records: list[dict[str, str]] = []
    with path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            ticker = normalize_ticker_symbol(str(row.get("ticker", "")))
            if not ticker or ticker == "DXYZ":
                continue
            filter_reason = str(row.get("filter_reason", "")).strip()
            if filter_reason == "share_class_or_structured_suffix" and _is_share_class_ticker(ticker):
                continue
            records.append(
                {
                    "ticker": ticker,
                    "reason": filter_reason,
                }
            )
    return records


def _parse_ticker_line(raw_line: str) -> tuple[list[str], str]:
    body, _, comment = raw_line.partition("#")
    reason = comment.strip()
    tickers = [
        normalize_ticker_symbol(part)
        for part in body.replace(",", " ").split()
        if normalize_ticker_symbol(part)
    ]
    return tickers, reason


def _write_user_file_records(path: Path, records: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    header_lines = _read_header_lines(path)
    unique_records = {
        record["ticker"]: {"ticker": record["ticker"], "reason": record["reason"].strip()}
        for record in records
        if record["ticker"]
    }
    lines = list(header_lines)
    if lines and lines[-1].strip():
        lines.append("")
    for ticker in sorted(unique_records):
        reason = unique_records[ticker]["reason"]
        lines.append(f"{ticker}  # {reason}" if reason else ticker)
    path.write_text("\n".join(lines).rstrip() + "\n", encoding="utf-8")


def _read_header_lines(path: Path) -> list[str]:
    if not path.exists():
        return []
    lines = path.read_text(encoding="utf-8").splitlines()
    header: list[str] = []
    for line in lines:
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            header.append(line)
            continue
        break
    return header


def _humanize_auto_source(stem: str) -> str:
    return f"Auto {stem.replace('-', ' ')}"


def _append_audit_log(*, action: str, ticker: str, reason: str, source: str) -> None:
    log_path = project_root() / "config" / "exclusion_change_log.jsonl"
    payload = {
        "timestamp": dt.datetime.now(dt.timezone.utc).replace(microsecond=0).isoformat(),
        "action": action,
        "ticker": ticker,
        "reason": reason,
        "source": source,
    }
    with log_path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload, ensure_ascii=True) + "\n")


def _is_share_class_ticker(ticker: str) -> bool:
    root, dot, suffix = ticker.partition(".")
    if not root or dot != ".":
        return False
    return len(suffix) == 1 and suffix.isalpha()
