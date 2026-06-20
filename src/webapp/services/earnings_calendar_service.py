from __future__ import annotations

import datetime as dt
from pathlib import Path
from typing import Any

from ...config import AppConfig, load_app_config
from ...cookstock_bridge import load_configured_cookstock
from ...earnings_enrichment import _parse_session_from_summary
from ...ratings.repository import RatingsRepository
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
        self.ratings_repository = RatingsRepository(database_url)
        self._universe_index: dict[str, UniverseTicker] | None = None

    def get_next_week_calendar(
        self,
        *,
        reference_date: dt.date | None = None,
        week_offset: int = 0,
        exclude_sectors: list[str] | None = None,
        exclude_industries: list[str] | None = None,
        only_criteria: bool = False,
    ) -> dict[str, Any]:
        anchor_date = reference_date or dt.date.today()
        normalized_week_offset = max(0, min(2, int(week_offset)))
        week_start = anchor_date - dt.timedelta(days=anchor_date.weekday() + 1) if anchor_date.weekday() != 6 else anchor_date
        selected_week_start = week_start + dt.timedelta(days=normalized_week_offset * 7)
        selected_week_end = selected_week_start + dt.timedelta(days=6)
        criteria_meta = self._load_criteria_meta_for_week(selected_week_start, selected_week_end)
        matched_tickers = {str(value).upper() for value in criteria_meta.get("matched_tickers", [])}
        criteria_by_ticker = criteria_meta.get("ticker_details", {})

        excluded_sector_keys = {_normalize_filter_value(value) for value in (exclude_sectors or []) if _normalize_filter_value(value)}
        excluded_industry_keys = {_normalize_filter_value(value) for value in (exclude_industries or []) if _normalize_filter_value(value)}
        universe_index = self._get_universe_index()
        cookstock = load_configured_cookstock(self.app_config)
        raw_events = cookstock.fetch_earnings_calendar_watchlist(selected_week_start, selected_week_end)
        ticker_list = sorted(
            {
                str(item.get("ticker") or "").strip().upper()
                for item in raw_events
                if str(item.get("ticker") or "").strip()
            }
        )
        rating_snapshots = self.ratings_repository.load_latest_rating_snapshots_for_tickers(ticker_list)
        technical_rating_snapshots = self.ratings_repository.load_latest_technical_rating_snapshots_for_tickers(ticker_list)
        technical_indicator_ratings = self.ratings_repository.load_latest_technical_indicator_ratings_for_tickers(ticker_list)

        grouped_days: dict[str, dict[str, Any]] = {}
        available_sector_labels: set[str] = set()
        available_industry_labels: set[str] = set()
        seen_by_day: dict[str, set[str]] = {}
        for offset in range(7):
            current_date = selected_week_start + dt.timedelta(days=offset)
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
            if event_date < selected_week_start or event_date > selected_week_end:
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
                    "fundamental_rating": rating_snapshots.get(ticker),
                    "technical_rating": technical_rating_snapshots.get(ticker),
                    "technical_indicator_ratings": technical_indicator_ratings.get(ticker, {}),
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
            "week_start": selected_week_start.isoformat(),
            "week_end": selected_week_end.isoformat(),
            "reference_date": anchor_date.isoformat(),
            "week_offset": normalized_week_offset,
            "days": [grouped_days[(selected_week_start + dt.timedelta(days=offset)).isoformat()] for offset in range(7)],
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

    def _empty_criteria_meta(self) -> dict[str, Any]:
        return {
            "available": False,
            "strategy_id": CRITERIA_STRATEGY_ID,
            "run_id": None,
            "run_date": None,
            "matched_tickers": [],
            "ticker_details": {},
        }

    def _load_criteria_meta_for_week(self, week_start: dt.date, week_end: dt.date) -> dict[str, Any]:
        payload = {
            **self._empty_criteria_meta(),
            "week_start": week_start.isoformat(),
            "week_end": week_end.isoformat(),
        }
        if not self.history_service.is_configured():
            return payload
        runs = self.history_service.list_runs(strategy_id=CRITERIA_STRATEGY_ID, limit=12)
        if not runs:
            return payload
        direct_match_runs: list[dict[str, Any]] = []
        for run in runs:
            run_date = run.get("run_date")
            if not isinstance(run_date, dt.date):
                continue
            target_week_start, target_week_end = _criteria_target_week_for_run_date(run_date)
            if target_week_start == week_start and target_week_end == week_end:
                direct_match_runs.append(run)
        for run in direct_match_runs:
            run_id = run.get("id")
            if not isinstance(run_id, int):
                continue
            detail = self.history_service.get_run(run_id, include_hits=True, hit_limit=1000)
            if not isinstance(detail, dict):
                continue
            candidate = self._build_criteria_meta_from_run(detail, run)
            if candidate.get("ticker_details"):
                payload.update(candidate)
                return payload
        best_payload: dict[str, Any] | None = None
        best_match_count = -1
        for run in runs:
            run_id = run.get("id")
            if not isinstance(run_id, int):
                continue
            detail = self.history_service.get_run(run_id, include_hits=True, hit_limit=1000)
            if not isinstance(detail, dict):
                continue
            candidate = self._build_criteria_meta_from_run(detail, run, week_start=week_start, week_end=week_end)
            candidate_match_count = len(candidate.get("ticker_details", {}))
            if candidate_match_count <= 0:
                continue
            if candidate_match_count > best_match_count:
                best_payload = candidate
                best_match_count = candidate_match_count
        if best_payload is None:
            return payload
        payload.update(best_payload)
        return payload

    def _build_criteria_meta_from_run(
        self,
        detail: dict[str, Any],
        run_summary: dict[str, Any],
        *,
        week_start: dt.date | None = None,
        week_end: dt.date | None = None,
    ) -> dict[str, Any]:
        tickers: list[str] = []
        ticker_details: dict[str, dict[str, Any]] = {}
        for hit in detail.get("hits", []):
            if not isinstance(hit, dict):
                continue
            raw_payload = hit.get("hit_payload_json")
            payload_json = raw_payload if isinstance(raw_payload, dict) else {}
            if week_start is not None and week_end is not None:
                earnings_date = _parse_iso_date(payload_json.get("earnings_date"))
                if earnings_date is not None and (earnings_date < week_start or earnings_date > week_end):
                    continue
            ticker = str(hit.get("ticker") or "").strip().upper()
            if not ticker:
                continue
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
        run_date = run_summary.get("run_date")
        return {
            "available": bool(ticker_details),
            "strategy_id": CRITERIA_STRATEGY_ID,
            "run_id": detail.get("id"),
            "run_date": run_date.isoformat() if hasattr(run_date, "isoformat") else str(run_date or ""),
            "matched_tickers": sorted(set(tickers)),
            "ticker_details": ticker_details,
        }


def _normalize_filter_value(value: object) -> str:
    return str(value or "").strip().lower()


def _parse_iso_date(value: object) -> dt.date | None:
    if not isinstance(value, str) or not value.strip():
        return None
    try:
        return dt.date.fromisoformat(value.strip())
    except ValueError:
        return None


def _criteria_target_week_for_run_date(run_date: dt.date) -> tuple[dt.date, dt.date]:
    week_start = run_date - dt.timedelta(days=run_date.weekday() + 1) if run_date.weekday() != 6 else run_date
    target_week_start = week_start + dt.timedelta(days=7)
    target_week_end = target_week_start + dt.timedelta(days=6)
    return target_week_start, target_week_end
