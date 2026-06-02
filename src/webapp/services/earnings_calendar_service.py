from __future__ import annotations

import datetime as dt
from pathlib import Path
from typing import Any

from ...config import AppConfig, load_app_config
from ...cookstock_bridge import load_configured_cookstock
from ...earnings_enrichment import _parse_session_from_summary
from ...universe import UniverseTicker, load_universe
from .screener_history_service import ScreenerHistoryService


CRITERIA_STRATEGY_ID = "earnings_weekly_criteria"


class EarningsCalendarService:
    def __init__(
        self,
        *,
        project_root: Path,
        app_config: AppConfig | None = None,
        database_url: str = "",
        artifacts_dir: Path | None = None,
    ) -> None:
        self.project_root = project_root
        self.app_config = app_config or load_app_config()
        self.history_service = ScreenerHistoryService(database_url=database_url, artifacts_dir=artifacts_dir)
        self._universe_index: dict[str, UniverseTicker] | None = None

    def get_next_week_calendar(
        self,
        *,
        reference_date: dt.date | None = None,
        exclude_sectors: list[str] | None = None,
        exclude_industries: list[str] | None = None,
        only_criteria: bool = False,
    ) -> dict[str, Any]:
        anchor_date = reference_date or dt.date.today()
        week_start = anchor_date - dt.timedelta(days=anchor_date.weekday() + 1) if anchor_date.weekday() != 6 else anchor_date
        next_week_start = week_start + dt.timedelta(days=7)
        next_week_end = next_week_start + dt.timedelta(days=6)
        criteria_meta = self._load_latest_criteria_meta()
        matched_tickers = {str(value).upper() for value in criteria_meta.get("matched_tickers", [])}
        criteria_by_ticker = criteria_meta.get("ticker_details", {})

        excluded_sector_keys = {_normalize_filter_value(value) for value in (exclude_sectors or []) if _normalize_filter_value(value)}
        excluded_industry_keys = {_normalize_filter_value(value) for value in (exclude_industries or []) if _normalize_filter_value(value)}
        universe_index = self._get_universe_index()
        cookstock = load_configured_cookstock(self.app_config)
        raw_events = cookstock.fetch_earnings_calendar_watchlist(next_week_start, next_week_end)

        grouped_days: dict[str, dict[str, Any]] = {}
        available_sector_labels: set[str] = set()
        available_industry_labels: set[str] = set()
        seen_by_day: dict[str, set[str]] = {}
        for offset in range(7):
            current_date = next_week_start + dt.timedelta(days=offset)
            date_key = current_date.isoformat()
            grouped_days[date_key] = {
                "date": date_key,
                "weekday": current_date.strftime("%a"),
                "before_market": [],
                "after_market": [],
                "during_market": [],
                "unknown": [],
            }
            seen_by_day[date_key] = set()

        for item in raw_events:
            ticker = str(item.get("ticker") or "").strip().upper()
            event_date = item.get("event_date")
            if not ticker or not isinstance(event_date, dt.date):
                continue
            if event_date < next_week_start or event_date > next_week_end:
                continue

            metadata = universe_index.get(ticker)
            sector = metadata.sector if metadata else None
            industry = metadata.industry if metadata else None
            exchange = metadata.exchange if metadata else None
            if sector:
                available_sector_labels.add(sector)
            if industry:
                available_industry_labels.add(industry)

            if _normalize_filter_value(sector) in excluded_sector_keys:
                continue
            if _normalize_filter_value(industry) in excluded_industry_keys:
                continue
            if only_criteria and ticker not in matched_tickers:
                continue

            date_key = event_date.isoformat()
            if ticker in seen_by_day[date_key]:
                continue
            seen_by_day[date_key].add(ticker)

            summary = str(item.get("summary") or "").strip() or None
            session = _parse_session_from_summary(summary)
            bucket_key = session if session in {"before_market", "after_market", "during_market"} else "unknown"
            grouped_days[date_key][bucket_key].append(
                {
                    "ticker": ticker,
                    "date": date_key,
                    "session": session,
                    "summary": summary,
                    "sector": sector,
                    "industry": industry,
                    "exchange": exchange,
                    "criteria": criteria_by_ticker.get(ticker),
                    "implied_move_signal": criteria_by_ticker.get(ticker, {}).get("implied_move_signal")
                    if isinstance(criteria_by_ticker.get(ticker), dict)
                    else None,
                }
            )

        for day in grouped_days.values():
            for bucket_key in ("before_market", "after_market", "during_market", "unknown"):
                day[bucket_key].sort(key=lambda entry: str(entry.get("ticker") or ""))

        return {
            "week_start": next_week_start.isoformat(),
            "week_end": next_week_end.isoformat(),
            "reference_date": anchor_date.isoformat(),
            "days": [grouped_days[(next_week_start + dt.timedelta(days=offset)).isoformat()] for offset in range(7)],
            "filters": {
                "exclude_sectors": sorted(value for value in (exclude_sectors or []) if value),
                "exclude_industries": sorted(value for value in (exclude_industries or []) if value),
                "only_criteria": bool(only_criteria),
            },
            "available_sectors": sorted(available_sector_labels),
            "available_industries": sorted(available_industry_labels),
            "criteria_filter": {
                "enabled": True,
                "available": bool(criteria_meta.get("available")),
                "strategy_id": criteria_meta.get("strategy_id"),
                "run_id": criteria_meta.get("run_id"),
                "run_date": criteria_meta.get("run_date"),
                "matched_count": len(matched_tickers),
            },
        }

    def _get_universe_index(self) -> dict[str, UniverseTicker]:
        if self._universe_index is not None:
            return self._universe_index
        universe = load_universe(self.app_config)
        self._universe_index = {item.symbol.upper(): item for item in universe if item.symbol}
        return self._universe_index

    def _load_latest_criteria_meta(self) -> dict[str, Any]:
        payload = {
            "available": False,
            "strategy_id": CRITERIA_STRATEGY_ID,
            "run_id": None,
            "run_date": None,
            "matched_tickers": [],
            "ticker_details": {},
        }
        if not self.history_service.is_configured():
            return payload
        runs = self.history_service.list_runs(strategy_id=CRITERIA_STRATEGY_ID, limit=1)
        if not runs:
            return payload
        latest = runs[0]
        run_id = latest.get("id")
        if not isinstance(run_id, int):
            return payload
        detail = self.history_service.get_run(run_id, include_hits=True, hit_limit=1000)
        if not isinstance(detail, dict):
            return payload
        tickers: list[str] = []
        ticker_details: dict[str, dict[str, Any]] = {}
        for hit in detail.get("hits", []):
            if not isinstance(hit, dict):
                continue
            ticker = str(hit.get("ticker") or "").strip().upper()
            if ticker:
                raw_payload = hit.get("hit_payload_json")
                payload_json = raw_payload if isinstance(raw_payload, dict) else {}
                criteria = payload_json.get("criteria") if isinstance(payload_json.get("criteria"), dict) else {}
                passed = bool(hit.get("passed"))
                matched = [key for key, value in criteria.items() if value]
                not_matched = [key for key, value in criteria.items() if not value]
                ticker_details[ticker] = {
                    "passed": passed,
                    "criteria": criteria,
                    "matched_criteria": matched,
                    "not_matched_criteria": not_matched,
                    "pass_mode": payload_json.get("pass_mode") or "",
                    "error": payload_json.get("error") or "",
                    "implied_move_signal": payload_json.get("implied_move_signal")
                    if isinstance(payload_json.get("implied_move_signal"), dict)
                    else None,
                }
                if passed:
                    tickers.append(ticker)
        payload["available"] = bool(ticker_details)
        payload["run_id"] = run_id
        run_date = latest.get("run_date")
        payload["run_date"] = run_date.isoformat() if hasattr(run_date, "isoformat") else str(run_date or "")
        payload["matched_tickers"] = sorted(set(tickers))
        payload["ticker_details"] = ticker_details
        return payload


def _normalize_filter_value(value: object) -> str:
    return str(value or "").strip().lower()
