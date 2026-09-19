from __future__ import annotations

import datetime as dt
import json
import os
from pathlib import Path
import re
import tempfile
from typing import Any


_TICKER_PATTERN = re.compile(r"^[A-Z][A-Z0-9.-]{0,9}$")
_AGENT_ID_PATTERN = re.compile(r"^[a-z0-9][a-z0-9_-]{0,63}$")
_VALID_GROUPS = {"A", "B", "C", "D", "E", "U"}


class DailyReportService:
    def __init__(self, *, artifacts_dir: Path) -> None:
        self.reports_dir = artifacts_dir / "daily_reports"

    def list_reports(self, *, limit: int = 90) -> list[dict[str, Any]]:
        if not self.reports_dir.exists():
            return []
        paths = list(self.reports_dir.glob("*/*.json")) + list(self.reports_dir.glob("*.json"))
        reports: list[dict[str, Any]] = []
        for path in paths:
            payload = self._load_payload(path)
            if payload is None:
                continue
            fallback_date = path.stem if path.parent == self.reports_dir else path.parent.name
            fallback_agent = "default" if path.parent == self.reports_dir else path.stem
            candidates = payload.get("candidates") if isinstance(payload.get("candidates"), list) else []
            group_counts = {group: 0 for group in ("A", "B", "C", "D", "E", "U")}
            for candidate in candidates:
                if isinstance(candidate, dict) and candidate.get("group") in group_counts:
                    group_counts[str(candidate["group"])] += 1
            reports.append(
                {
                    "report_date": str(payload.get("report_date") or fallback_date),
                    "agent_id": str(payload.get("agent_id") or fallback_agent),
                    "agent_name": str(payload.get("agent_name") or payload.get("agent_id") or "Default agent"),
                    "model": str(payload.get("model") or ""),
                    "target_trading_date": str(payload.get("target_trading_date") or ""),
                    "generated_at": str(payload.get("generated_at") or ""),
                    "title": str(payload.get("title") or "Daily VCP Report"),
                    "candidate_count": _coerce_int(payload.get("candidate_count"), len(candidates)),
                    "analyzed_count": _coerce_int(payload.get("analyzed_count"), len(candidates)),
                    "group_counts": group_counts,
                    "top_tickers": [
                        str(item.get("ticker") or "")
                        for item in candidates
                        if isinstance(item, dict) and str(item.get("group") or "") in {"A", "B"}
                    ][:5],
                }
            )
        reports.sort(key=lambda item: (item["report_date"], item["generated_at"], item["agent_id"]), reverse=True)
        return reports[: max(1, min(int(limit), 365))]

    def get_report(self, report_date: str, agent_id: str | None = None) -> dict[str, Any]:
        normalized_date = _parse_iso_date(report_date).isoformat()
        normalized_agent = _normalize_agent_id(agent_id) if agent_id else ""
        if normalized_agent:
            payload = self._load_payload(self.reports_dir / normalized_date / f"{normalized_agent}.json")
        else:
            date_payloads = [
                item
                for item in (
                    self._load_payload(path)
                    for path in (self.reports_dir / normalized_date).glob("*.json")
                )
                if item is not None
            ]
            date_payloads.sort(
                key=lambda item: (str(item.get("generated_at") or ""), str(item.get("agent_id") or "")),
                reverse=True,
            )
            payload = date_payloads[0] if date_payloads else self._load_payload(self.reports_dir / f"{normalized_date}.json")
        if payload is None:
            suffix = f"/{normalized_agent}" if normalized_agent else ""
            raise ValueError(f"Unknown daily report: {normalized_date}{suffix}")
        return payload

    def upsert_report(self, payload: dict[str, Any]) -> dict[str, Any]:
        normalized = self._normalize_report(payload)
        self.reports_dir.mkdir(parents=True, exist_ok=True)
        date_dir = self.reports_dir / normalized["report_date"]
        date_dir.mkdir(parents=True, exist_ok=True)
        target = date_dir / f"{normalized['agent_id']}.json"
        descriptor, temporary_name = tempfile.mkstemp(
            dir=date_dir,
            prefix=f".{normalized['agent_id']}.",
            suffix=".tmp",
        )
        try:
            with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
                json.dump(normalized, handle, indent=2, sort_keys=False)
                handle.write("\n")
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temporary_name, target)
        except Exception:
            try:
                os.unlink(temporary_name)
            except OSError:
                pass
            raise
        return normalized

    def _normalize_report(self, payload: dict[str, Any]) -> dict[str, Any]:
        if not isinstance(payload, dict):
            raise ValueError("Daily report must be a JSON object.")
        report_date = _parse_iso_date(payload.get("report_date")).isoformat()
        agent_id = _normalize_agent_id(payload.get("agent_id"))
        candidates_raw = payload.get("candidates")
        if not isinstance(candidates_raw, list):
            raise ValueError("Daily report candidates must be a list.")
        if len(candidates_raw) > 500:
            raise ValueError("Daily report cannot contain more than 500 candidates.")

        candidates: list[dict[str, Any]] = []
        seen_tickers: set[str] = set()
        for index, item in enumerate(candidates_raw):
            if not isinstance(item, dict):
                raise ValueError(f"Candidate {index + 1} must be a JSON object.")
            ticker = str(item.get("ticker") or "").strip().upper()
            if not _TICKER_PATTERN.fullmatch(ticker):
                raise ValueError(f"Candidate {index + 1} has an invalid ticker.")
            if ticker in seen_tickers:
                raise ValueError(f"Duplicate candidate ticker: {ticker}")
            seen_tickers.add(ticker)
            group = str(item.get("group") or "U").strip().upper()
            if group not in _VALID_GROUPS:
                raise ValueError(f"Candidate {ticker} has an invalid group: {group}")
            score = _coerce_optional_float(item.get("score"))
            if score is not None and not 0 <= score <= 100:
                raise ValueError(f"Candidate {ticker} score must be between 0 and 100.")
            normalized_candidate = dict(item)
            normalized_candidate.update({"ticker": ticker, "group": group, "score": score})
            candidates.append(normalized_candidate)

        generated_at = str(payload.get("generated_at") or "").strip()
        if not generated_at:
            generated_at = dt.datetime.now(dt.timezone.utc).isoformat()
        watch_plan_raw = payload.get("watch_plan")
        watch_plan = [str(item).strip() for item in watch_plan_raw or [] if str(item).strip()] if isinstance(watch_plan_raw, list) else []
        normalized = dict(payload)
        normalized.update(
            {
                "schema_version": 1,
                "report_date": report_date,
                "agent_id": agent_id,
                "agent_name": str(payload.get("agent_name") or agent_id).strip() or agent_id,
                "model": str(payload.get("model") or "").strip(),
                "target_trading_date": str(payload.get("target_trading_date") or report_date),
                "generated_at": generated_at,
                "title": str(payload.get("title") or "Daily VCP Report").strip() or "Daily VCP Report",
                "market_context": str(payload.get("market_context") or "").strip(),
                "candidate_count": _coerce_int(payload.get("candidate_count"), len(candidates)),
                "analyzed_count": _coerce_int(payload.get("analyzed_count"), len(candidates)),
                "candidates": candidates,
                "watch_plan": watch_plan,
            }
        )
        return normalized

    @staticmethod
    def _load_payload(path: Path) -> dict[str, Any] | None:
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return None
        return payload if isinstance(payload, dict) else None


def _parse_iso_date(value: Any) -> dt.date:
    text = str(value or "").strip()
    try:
        return dt.date.fromisoformat(text)
    except ValueError as exc:
        raise ValueError("Daily report report_date must use YYYY-MM-DD.") from exc


def _normalize_agent_id(value: Any) -> str:
    text = str(value or "").strip().lower()
    if not _AGENT_ID_PATTERN.fullmatch(text):
        raise ValueError("Daily report agent_id must use lowercase letters, numbers, underscores, or hyphens.")
    return text


def _coerce_optional_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"Invalid numeric value: {value}") from exc


def _coerce_int(value: Any, default: int) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default
