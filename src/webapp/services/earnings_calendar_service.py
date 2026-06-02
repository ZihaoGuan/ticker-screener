from __future__ import annotations

import datetime as dt
import json
import logging
from pathlib import Path
from typing import Any

from ...config import AppConfig, load_app_config
from ...cookstock_bridge import load_configured_cookstock
from ...earnings_enrichment import _parse_session_from_summary
from ...universe import UniverseTicker, load_universe
from .screener_history_service import ScreenerHistoryService
from .watchlist_service import _load_yahoo_implied_move_playwright


CRITERIA_STRATEGY_ID = "earnings_weekly_criteria"
IMPLIED_MOVE_CRITERIA_KEY = "implied_move_ge_7_near_earnings"
IMPLIED_MOVE_THRESHOLD_PCT = 7.0
IMPLIED_MOVE_CACHE_TTL_HOURS = 12

logger = logging.getLogger(__name__)


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
        self.artifacts_dir = artifacts_dir or (project_root / "artifacts")
        self.history_service = ScreenerHistoryService(database_url=database_url, artifacts_dir=self.artifacts_dir)
        self._universe_index: dict[str, UniverseTicker] | None = None
        self._implied_move_cache_path = self.artifacts_dir / "raw" / "earnings" / "implied_move_cache.json"

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

        pending_entries: list[dict[str, Any]] = []
        tickers_to_enrich: set[str] = set()
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

            date_key = event_date.isoformat()
            if ticker in seen_by_day[date_key]:
                continue
            seen_by_day[date_key].add(ticker)

            summary = str(item.get("summary") or "").strip() or None
            session = _parse_session_from_summary(summary)
            bucket_key = session if session in {"before_market", "after_market", "during_market"} else "unknown"
            pending_entries.append(
                {
                    "bucket_key": bucket_key,
                    "date_key": date_key,
                    "entry": {
                        "ticker": ticker,
                        "date": date_key,
                        "session": session,
                        "summary": summary,
                        "sector": sector,
                        "industry": industry,
                        "exchange": exchange,
                    },
                    "base_criteria": criteria_by_ticker.get(ticker),
                }
            )
            tickers_to_enrich.add(ticker)

        implied_move_by_ticker = self._load_implied_move_signals(sorted(tickers_to_enrich))
        matched_tickers: set[str] = set()
        for item in pending_entries:
            entry = dict(item["entry"])
            ticker = str(entry.get("ticker") or "").upper()
            implied_move_signal = implied_move_by_ticker.get(ticker)
            combined_criteria = _merge_earnings_entry_criteria(
                item.get("base_criteria"),
                implied_move_signal=implied_move_signal,
            )
            entry["criteria"] = combined_criteria
            entry["implied_move_signal"] = implied_move_signal
            if only_criteria and not (combined_criteria and combined_criteria.get("passed")):
                continue
            if combined_criteria and combined_criteria.get("passed"):
                matched_tickers.add(ticker)
            grouped_days[str(item["date_key"])][str(item["bucket_key"])].append(entry)

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

    def _load_implied_move_signals(self, tickers: list[str]) -> dict[str, dict[str, Any]]:
        cache_payload = self._read_implied_move_cache()
        cache_entries = cache_payload.get("entries", {}) if isinstance(cache_payload.get("entries"), dict) else {}
        now = dt.datetime.now(dt.timezone.utc)
        results: dict[str, dict[str, Any]] = {}
        mutated = False
        for raw_ticker in tickers:
            ticker = str(raw_ticker or "").strip().upper()
            if not ticker:
                continue
            cache_entry = cache_entries.get(ticker)
            if isinstance(cache_entry, dict) and _is_implied_move_cache_fresh(cache_entry, now=now):
                signal = _build_implied_move_signal(cache_entry)
                if signal:
                    results[ticker] = signal
                    continue
            implied_move, diagnostics = _load_yahoo_implied_move_playwright(ticker)
            percent_move = implied_move.get("percent_move") if isinstance(implied_move, dict) else None
            status = str(diagnostics.get("status") or "empty")
            cache_entries[ticker] = {
                "ticker": ticker,
                "refreshed_at": now.replace(microsecond=0).isoformat(),
                "percent_move": percent_move,
                "status": status,
            }
            mutated = True
            signal = _build_implied_move_signal(cache_entries[ticker])
            if signal:
                results[ticker] = signal
            else:
                results[ticker] = {
                    "threshold_pct": IMPLIED_MOVE_THRESHOLD_PCT,
                    "near_earnings": True,
                    "matched": False,
                    "percent_move": None,
                    "status": status,
                }
        if mutated:
            self._write_implied_move_cache({"entries": cache_entries})
        return results

    def _read_implied_move_cache(self) -> dict[str, Any]:
        if not self._implied_move_cache_path.exists():
            return {"entries": {}}
        try:
            payload = json.loads(self._implied_move_cache_path.read_text(encoding="utf-8"))
        except Exception as exc:
            logger.warning("Unable to read implied move cache: %s", exc)
            return {"entries": {}}
        return payload if isinstance(payload, dict) else {"entries": {}}

    def _write_implied_move_cache(self, payload: dict[str, Any]) -> None:
        try:
            self._implied_move_cache_path.parent.mkdir(parents=True, exist_ok=True)
            self._implied_move_cache_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        except Exception as exc:
            logger.warning("Unable to write implied move cache: %s", exc)


def _normalize_filter_value(value: object) -> str:
    return str(value or "").strip().lower()


def _merge_earnings_entry_criteria(
    base_criteria: object,
    *,
    implied_move_signal: dict[str, Any] | None,
) -> dict[str, Any] | None:
    if not isinstance(base_criteria, dict):
        return None
    criteria_values = dict(base_criteria.get("criteria", {})) if isinstance(base_criteria.get("criteria"), dict) else {}
    implied_move_matched = bool(implied_move_signal and implied_move_signal.get("matched"))
    criteria_values[IMPLIED_MOVE_CRITERIA_KEY] = implied_move_matched
    matched = [key for key, value in criteria_values.items() if value]
    not_matched = [key for key, value in criteria_values.items() if not value]
    return {
        "passed": bool(not not_matched),
        "criteria": criteria_values,
        "matched_criteria": matched,
        "not_matched_criteria": not_matched,
        "pass_mode": base_criteria.get("pass_mode") or "",
        "error": base_criteria.get("error") or ("" if not not_matched else "criteria_not_met"),
    }


def _is_implied_move_cache_fresh(cache_entry: dict[str, Any], *, now: dt.datetime) -> bool:
    refreshed_at = str(cache_entry.get("refreshed_at") or "").strip()
    if not refreshed_at:
        return False
    try:
        refreshed = dt.datetime.fromisoformat(refreshed_at.replace("Z", "+00:00"))
    except ValueError:
        return False
    if refreshed.tzinfo is None:
        refreshed = refreshed.replace(tzinfo=dt.timezone.utc)
    return now - refreshed.astimezone(dt.timezone.utc) <= dt.timedelta(hours=IMPLIED_MOVE_CACHE_TTL_HOURS)


def _build_implied_move_signal(cache_entry: dict[str, Any]) -> dict[str, Any] | None:
    percent_move_raw = cache_entry.get("percent_move")
    try:
        percent_move = float(percent_move_raw) if percent_move_raw is not None else None
    except (TypeError, ValueError):
        percent_move = None
    status = str(cache_entry.get("status") or ("ok" if percent_move is not None else "empty"))
    return {
        "threshold_pct": IMPLIED_MOVE_THRESHOLD_PCT,
        "near_earnings": True,
        "matched": percent_move is not None and percent_move > IMPLIED_MOVE_THRESHOLD_PCT,
        "percent_move": percent_move,
        "status": status,
    }
