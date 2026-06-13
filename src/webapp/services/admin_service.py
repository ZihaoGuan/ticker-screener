from __future__ import annotations

import datetime as dt
from collections import Counter
import json
from pathlib import Path
from typing import Any

from src.config import load_app_config, today_label
from src.exclusion_registry import (
    add_manual_exclusion,
    add_manual_inclusion,
    list_exclusion_entries,
    list_inclusion_entries,
    remove_manual_inclusion,
    remove_user_exclusion,
)
from src.market_data_access import resolve_database_url
from src.ticker_filters import normalize_ticker_symbol
from src.universe import load_universe


class AdminService:
    def __init__(self, database_url: str = "", *, artifacts_dir: Path | None = None) -> None:
        self.database_url = resolve_database_url(database_url)
        self.artifacts_dir = artifacts_dir or (Path(__file__).resolve().parents[3] / "artifacts")

    def get_context(self, *, coverage_start: str = "2020-01-01") -> dict[str, Any]:
        excluded = self._load_exclusions()
        included = self._load_inclusions()
        status = self._history_status(coverage_start=coverage_start)
        return {
            "excluded_tickers": excluded,
            "excluded_count": len(excluded),
            "included_tickers": included,
            "included_count": len(included),
            "database_status": status,
        }

    def get_ratings_status(self) -> dict[str, Any]:
        status: dict[str, Any] = {
            "database_configured": bool(self.database_url),
            "target_universe_count": 0,
            "latest_fundamentals_as_of_date": None,
            "latest_fundamentals_updated_at": None,
            "latest_baselines_as_of_date": None,
            "latest_baselines_updated_at": None,
            "latest_ratings_as_of_date": None,
            "latest_ratings_updated_at": None,
            "latest_fundamentals_snapshot_count": 0,
            "latest_rating_snapshot_count": 0,
            "latest_fundamentals_parse_status_counts": {},
            "latest_rating_status_counts": {},
            "tickers_with_any_fundamentals": 0,
            "tickers_with_latest_ok_rating": 0,
            "diagnostics_count": 0,
            "diagnostic_category_counts": {},
            "diagnostics": [],
            "notes": [],
        }
        if not self.database_url:
            status["notes"] = ["TICKER_SCREENER_DATABASE_URL not set."]
            return status

        target_tickers = self._load_target_tickers()
        status["target_universe_count"] = len(target_tickers)

        try:
            query_payload = self._query_ratings_status(target_tickers=target_tickers)
        except Exception as exc:
            status["notes"] = [f"Ratings status query failed: {exc}"]
            return status

        status.update(query_payload)
        if not target_tickers:
            status["notes"] = ["Universe loader returned 0 tickers."]
        elif not status["diagnostics"]:
            status["notes"] = ["All target tickers currently have an OK latest rating."]
        else:
            status["notes"] = [
                f"{status['tickers_with_latest_ok_rating']}/{len(target_tickers)} target tickers have an OK latest rating."
            ]
        return status

    def add_exclusion(self, *, ticker: str, reason: str) -> dict[str, Any]:
        config = load_app_config()
        clean_reason = reason.strip()
        if not clean_reason:
            raise ValueError("Reason is required.")
        return add_manual_exclusion(config, ticker=ticker, reason=clean_reason)

    def remove_exclusion(self, *, ticker: str, reason: str) -> dict[str, Any]:
        config = load_app_config()
        clean_reason = reason.strip()
        if not clean_reason:
            raise ValueError("Reason is required.")
        return remove_user_exclusion(config, ticker=ticker, reason=clean_reason)

    def add_inclusion(self, *, ticker: str, reason: str, remove_from_exclusions: bool = True) -> dict[str, Any]:
        config = load_app_config()
        clean_reason = reason.strip()
        if not clean_reason:
            raise ValueError("Reason is required.")
        added = add_manual_inclusion(config, ticker=ticker, reason=clean_reason)
        removed_from: list[str] = []
        if remove_from_exclusions:
            try:
                removed = remove_user_exclusion(config, ticker=ticker, reason=f"Moved to inclusion list. {clean_reason}")
                removed_from = list(removed.get("removed_from") or [])
            except ValueError:
                removed_from = []
        return {**added, "removed_from": removed_from}

    def remove_inclusion(self, *, ticker: str, reason: str) -> dict[str, Any]:
        config = load_app_config()
        clean_reason = reason.strip()
        if not clean_reason:
            raise ValueError("Reason is required.")
        return remove_manual_inclusion(config, ticker=ticker, reason=clean_reason)

    def get_ticker_list_status(self, *, ticker: str) -> dict[str, Any]:
        normalized = normalize_ticker_symbol(ticker)
        if not normalized:
            raise ValueError("Ticker is required.")
        exclusion_entries = self._load_exclusions()
        inclusion_entries = self._load_inclusions()
        exclusion_entry = next((entry for entry in exclusion_entries if str(entry.get("ticker") or "") == normalized), None)
        inclusion_entry = next((entry for entry in inclusion_entries if str(entry.get("ticker") or "") == normalized), None)
        return {
            "ticker": normalized,
            "is_excluded": exclusion_entry is not None,
            "is_included": inclusion_entry is not None,
            "exclusion_entry": exclusion_entry,
            "inclusion_entry": inclusion_entry,
        }

    def get_partial_ticker_detail(self, *, ticker: str, coverage_start: str = "2020-01-01") -> dict[str, Any]:
        normalized = normalize_ticker_symbol(ticker)
        if not normalized:
            raise ValueError("Ticker is required.")
        if not self.database_url:
            raise ValueError("TICKER_SCREENER_DATABASE_URL not set.")
        try:
            coverage_start_date = dt.date.fromisoformat(coverage_start)
        except ValueError as exc:
            raise ValueError(f"Invalid coverage start date: {coverage_start}") from exc
        coverage_end_date = dt.date.today()
        try:
            return self._query_partial_ticker_detail(
                ticker=normalized,
                coverage_start=coverage_start_date,
                coverage_end=coverage_end_date,
            )
        except Exception as exc:
            raise ValueError(f"Database query failed: {exc}") from exc

    def list_scheduled_jobs(self) -> list[dict[str, Any]]:
        status_dir = self.artifacts_dir / "status"
        if not status_dir.exists():
            return []
        jobs: list[dict[str, Any]] = []
        for path in sorted(status_dir.glob("*.json")):
            try:
                payload = json.loads(path.read_text(encoding="utf-8"))
            except Exception:
                continue
            if not isinstance(payload, dict):
                continue
            job_id = str(payload.get("job_id") or path.stem).strip() or path.stem
            jobs.append(
                {
                    "job_id": job_id,
                    "job_label": str(payload.get("job_label") or job_id),
                    "status": str(payload.get("status") or "unknown"),
                    "last_started_at": self._to_iso(payload.get("last_started_at")),
                    "last_finished_at": self._to_iso(payload.get("last_finished_at")),
                    "exit_code": _coerce_int(payload.get("exit_code")),
                    "log_file": str(payload.get("log_file") or ""),
                    "artifact_file": str(payload.get("artifact_file") or ""),
                    "message": str(payload.get("message") or ""),
                    "status_file": str(path),
                }
            )
        jobs.sort(key=lambda item: (str(item.get("job_label") or item.get("job_id") or "")))
        return jobs

    def _load_exclusions(self) -> list[dict[str, Any]]:
        config = load_app_config()
        return list_exclusion_entries(config)

    def _load_inclusions(self) -> list[dict[str, Any]]:
        config = load_app_config()
        return list_inclusion_entries(config)

    def _history_status(self, *, coverage_start: str) -> dict[str, Any]:
        status: dict[str, Any] = {
            "database_configured": bool(self.database_url),
            "coverage_start": coverage_start,
            "coverage_end": today_label(),
            "target_universe_count": 0,
            "db_ticker_count": 0,
            "covered_ticker_count": 0,
            "partial_ticker_count": 0,
            "missing_ticker_count": 0,
            "total_bar_rows": 0,
            "overall_first_trade_date": None,
            "overall_last_trade_date": None,
            "latest_metadata_update_at": None,
            "stale_ticker_count": 0,
            "coverage_percent": 0.0,
            "sample_missing_tickers": [],
            "sample_partial_tickers": [],
            "notes": [],
        }
        if not self.database_url:
            status["notes"] = ["TICKER_SCREENER_DATABASE_URL not set."]
            return status

        try:
            coverage_start_date = dt.date.fromisoformat(coverage_start)
        except ValueError:
            status["notes"] = [f"Invalid coverage start date: {coverage_start}"]
            return status

        target_tickers = self._load_target_tickers()
        status["target_universe_count"] = len(target_tickers)

        try:
            ticker_stats, overall_stats = self._query_db_stats()
        except Exception as exc:
            status["notes"] = [f"Database query failed: {exc}"]
            return status

        status["db_ticker_count"] = len(ticker_stats)
        status["overall_first_trade_date"] = overall_stats.get("overall_first_trade_date")
        status["overall_last_trade_date"] = overall_stats.get("overall_last_trade_date")
        status["latest_metadata_update_at"] = overall_stats.get("latest_metadata_update_at")
        status["total_bar_rows"] = int(overall_stats.get("total_bar_rows") or 0)

        latest_expected = dt.date.today() - dt.timedelta(days=7)
        missing: list[str] = []
        partial: list[str] = []
        stale_count = 0
        covered_count = 0

        for ticker in target_tickers:
            entry = ticker_stats.get(ticker)
            if entry is None:
                missing.append(ticker)
                continue
            first_trade_date = self._to_date(entry.get("first_trade_date"))
            last_trade_date = self._to_date(entry.get("last_trade_date"))
            if last_trade_date is None or last_trade_date < latest_expected:
                stale_count += 1
            if first_trade_date is None:
                missing.append(ticker)
            elif first_trade_date > coverage_start_date:
                partial.append(ticker)
            else:
                covered_count += 1

        target_count = len(target_tickers)
        status["covered_ticker_count"] = covered_count
        status["partial_ticker_count"] = len(partial)
        status["missing_ticker_count"] = len(missing)
        status["stale_ticker_count"] = stale_count
        status["sample_missing_tickers"] = [{"ticker": item} for item in missing[:20]]
        status["sample_partial_tickers"] = [{"ticker": item} for item in partial[:20]]
        status["coverage_percent"] = round((covered_count / target_count) * 100.0, 1) if target_count else 0.0
        if target_count == 0:
            status["notes"] = ["Universe loader returned 0 tickers."]
        elif not missing and not partial:
            status["notes"] = [f"All target tickers have bars from {coverage_start_date.isoformat()} forward."]
        else:
            status["notes"] = [
                f"{covered_count}/{target_count} tickers fully covered from {coverage_start_date.isoformat()}."
            ]
        return status

    def _load_target_tickers(self) -> list[str]:
        config = load_app_config()
        universe = load_universe(config)
        return sorted({item.symbol.upper() for item in universe if getattr(item, "symbol", "")})

    def _query_db_stats(self) -> tuple[dict[str, dict[str, Any]], dict[str, Any]]:
        import psycopg

        ticker_stats: dict[str, dict[str, Any]] = {}
        with psycopg.connect(self.database_url) as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT
                      ticker,
                      MIN(trade_date) AS first_trade_date,
                      MAX(trade_date) AS last_trade_date,
                      COUNT(*) AS bar_count
                    FROM daily_bars
                    GROUP BY ticker
                    """
                )
                for ticker, first_trade_date, last_trade_date, bar_count in cursor.fetchall():
                    ticker_stats[str(ticker).upper()] = {
                        "first_trade_date": first_trade_date,
                        "last_trade_date": last_trade_date,
                        "bar_count": int(bar_count or 0),
                    }

                cursor.execute(
                    """
                    SELECT
                      MIN(trade_date) AS overall_first_trade_date,
                      MAX(trade_date) AS overall_last_trade_date,
                      COUNT(*) AS total_bar_rows
                    FROM daily_bars
                    """
                )
                overall_first_trade_date, overall_last_trade_date, total_bar_rows = cursor.fetchone() or (None, None, 0)

                cursor.execute("SELECT MAX(updated_at) FROM ticker_metadata")
                latest_metadata_update_at = cursor.fetchone()
        return ticker_stats, {
            "overall_first_trade_date": self._to_iso(overall_first_trade_date),
            "overall_last_trade_date": self._to_iso(overall_last_trade_date),
            "total_bar_rows": int(total_bar_rows or 0),
            "latest_metadata_update_at": self._to_iso(latest_metadata_update_at[0] if latest_metadata_update_at else None),
        }

    def _query_partial_ticker_detail(
        self,
        *,
        ticker: str,
        coverage_start: dt.date,
        coverage_end: dt.date,
    ) -> dict[str, Any]:
        import psycopg

        benchmark_ticker = load_app_config().benchmark_ticker.upper()
        with psycopg.connect(self.database_url) as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT
                      MIN(trade_date) AS first_trade_date,
                      MAX(trade_date) AS last_trade_date,
                      COUNT(*) AS bar_count
                    FROM daily_bars
                    WHERE ticker = %s
                    """,
                    (ticker,),
                )
                first_trade_date, last_trade_date, bar_count = cursor.fetchone() or (None, None, 0)
                cursor.execute(
                    """
                    WITH benchmark_calendar AS (
                      SELECT trade_date
                      FROM daily_bars
                      WHERE ticker = %s
                        AND trade_date BETWEEN %s AND %s
                    ),
                    ticker_dates AS (
                      SELECT trade_date
                      FROM daily_bars
                      WHERE ticker = %s
                        AND trade_date BETWEEN %s AND %s
                    )
                    SELECT benchmark_calendar.trade_date
                    FROM benchmark_calendar
                    LEFT JOIN ticker_dates USING (trade_date)
                    WHERE ticker_dates.trade_date IS NULL
                    ORDER BY benchmark_calendar.trade_date
                    """,
                    (benchmark_ticker, coverage_start, coverage_end, ticker, coverage_start, coverage_end),
                )
                missing_dates = [row[0] for row in cursor.fetchall()]

        ranges = _build_missing_ranges(
            coverage_start=coverage_start,
            coverage_end=coverage_end,
            first_trade_date=self._to_date(first_trade_date),
            last_trade_date=self._to_date(last_trade_date),
            missing_dates=[self._to_date(item) for item in missing_dates if self._to_date(item) is not None],
        )
        return {
            "ticker": ticker,
            "coverage_start": coverage_start.isoformat(),
            "coverage_end": coverage_end.isoformat(),
            "first_trade_date": self._to_iso(first_trade_date),
            "last_trade_date": self._to_iso(last_trade_date),
            "bar_count": int(bar_count or 0),
            "missing_ranges": ranges,
            "missing_date_count": len(missing_dates),
            "sample_missing_dates": [self._to_iso(item) for item in missing_dates[:20]],
        }

    def _query_ratings_status(self, *, target_tickers: list[str]) -> dict[str, Any]:
        import psycopg

        latest_fundamentals_date: dt.date | None = None
        latest_fundamentals_updated_at: object = None
        latest_baselines_date: dt.date | None = None
        latest_baselines_updated_at: object = None
        latest_ratings_date: dt.date | None = None
        latest_ratings_updated_at: object = None
        latest_fundamentals_snapshot_count = 0
        latest_rating_snapshot_count = 0
        latest_fundamentals_parse_status_counts: dict[str, int] = {}
        latest_rating_status_counts: dict[str, int] = {}
        latest_fundamentals_by_ticker: dict[str, dict[str, Any]] = {}
        latest_ratings_by_ticker: dict[str, dict[str, Any]] = {}

        with psycopg.connect(self.database_url) as connection:
            with connection.cursor() as cursor:
                cursor.execute("SELECT MAX(as_of_date), MAX(updated_at) FROM ticker_fundamentals_snapshots")
                latest_fundamentals_date, latest_fundamentals_updated_at = cursor.fetchone() or (None, None)

                cursor.execute("SELECT MAX(as_of_date), MAX(updated_at) FROM sector_metric_baselines")
                latest_baselines_date, latest_baselines_updated_at = cursor.fetchone() or (None, None)

                cursor.execute("SELECT MAX(as_of_date), MAX(updated_at) FROM ticker_rating_snapshots")
                latest_ratings_date, latest_ratings_updated_at = cursor.fetchone() or (None, None)

                if latest_fundamentals_date is not None:
                    cursor.execute(
                        """
                        SELECT parse_status, COUNT(*)
                        FROM ticker_fundamentals_snapshots
                        WHERE as_of_date = %s
                        GROUP BY parse_status
                        """,
                        (latest_fundamentals_date,),
                    )
                    latest_fundamentals_parse_status_counts = {
                        str(parse_status or "unknown"): int(count or 0)
                        for parse_status, count in cursor.fetchall()
                    }
                    latest_fundamentals_snapshot_count = int(sum(latest_fundamentals_parse_status_counts.values()))

                if latest_ratings_date is not None:
                    cursor.execute(
                        """
                        SELECT rating_status, COUNT(*)
                        FROM ticker_rating_snapshots
                        WHERE as_of_date = %s
                        GROUP BY rating_status
                        """,
                        (latest_ratings_date,),
                    )
                    latest_rating_status_counts = {
                        str(rating_status or "unknown"): int(count or 0)
                        for rating_status, count in cursor.fetchall()
                    }
                    latest_rating_snapshot_count = int(sum(latest_rating_status_counts.values()))

                cursor.execute(
                    """
                    WITH latest_fundamentals AS (
                      SELECT DISTINCT ON (ticker)
                        ticker,
                        as_of_date,
                        parse_status,
                        parse_error,
                        sector,
                        industry,
                        updated_at
                      FROM ticker_fundamentals_snapshots
                      ORDER BY ticker, as_of_date DESC
                    )
                    SELECT ticker, as_of_date, parse_status, parse_error, sector, industry, updated_at
                    FROM latest_fundamentals
                    """
                )
                for ticker, as_of_date, parse_status, parse_error, sector, industry, updated_at in cursor.fetchall():
                    latest_fundamentals_by_ticker[str(ticker).upper()] = {
                        "as_of_date": self._to_iso(as_of_date),
                        "parse_status": str(parse_status or ""),
                        "parse_error": str(parse_error or "") or None,
                        "sector": str(sector or "") or None,
                        "industry": str(industry or "") or None,
                        "updated_at": self._to_iso(updated_at),
                    }

                cursor.execute(
                    """
                    WITH latest_ratings AS (
                      SELECT DISTINCT ON (ticker)
                        ticker,
                        as_of_date,
                        rating_status,
                        rating_status_reason,
                        missing_metric_names,
                        insufficient_baseline_metrics,
                        overall_rating,
                        updated_at
                      FROM ticker_rating_snapshots
                      ORDER BY ticker, as_of_date DESC
                    )
                    SELECT
                      ticker,
                      as_of_date,
                      rating_status,
                      rating_status_reason,
                      missing_metric_names,
                      insufficient_baseline_metrics,
                      overall_rating,
                      updated_at
                    FROM latest_ratings
                    """
                )
                for (
                    ticker,
                    as_of_date,
                    rating_status,
                    rating_status_reason,
                    missing_metric_names,
                    insufficient_baseline_metrics,
                    overall_rating,
                    updated_at,
                ) in cursor.fetchall():
                    latest_ratings_by_ticker[str(ticker).upper()] = {
                        "as_of_date": self._to_iso(as_of_date),
                        "rating_status": str(rating_status or ""),
                        "rating_status_reason": str(rating_status_reason or "") or None,
                        "missing_metric_names": list(missing_metric_names or []),
                        "insufficient_baseline_metrics": list(insufficient_baseline_metrics or []),
                        "overall_rating": float(overall_rating) if overall_rating is not None else None,
                        "updated_at": self._to_iso(updated_at),
                    }

        diagnostics: list[dict[str, Any]] = []
        tickers_with_ok_rating = 0
        tickers_with_any_fundamentals = 0
        target_set = {item.upper() for item in target_tickers}

        for ticker in sorted(target_set):
            fundamentals = latest_fundamentals_by_ticker.get(ticker)
            rating = latest_ratings_by_ticker.get(ticker)
            if fundamentals is not None:
                tickers_with_any_fundamentals += 1
            category = ""
            reason = ""
            if fundamentals is None:
                category = "missing_fundamentals"
                reason = "No fundamentals snapshot found for this ticker."
            elif str(fundamentals.get("parse_status") or "") != "ok":
                category = str(fundamentals.get("parse_status") or "scrape_failed")
                reason = str(fundamentals.get("parse_error") or "Fundamentals sync did not complete successfully.")
            elif rating is None:
                category = "missing_rating"
                reason = "No rating snapshot found for the latest fundamentals snapshot."
            elif str(rating.get("as_of_date") or "") != str(fundamentals.get("as_of_date") or ""):
                category = "stale_rating"
                reason = "Latest rating snapshot date does not match latest fundamentals snapshot date."
            elif str(rating.get("rating_status") or "") != "ok":
                category = str(rating.get("rating_status") or "missing_rating")
                reason = str(rating.get("rating_status_reason") or "Rating snapshot is not OK.")
            else:
                tickers_with_ok_rating += 1

            if not category:
                continue
            diagnostics.append(
                {
                    "ticker": ticker,
                    "category": category,
                    "reason": reason,
                    "fundamentals_as_of_date": fundamentals.get("as_of_date") if fundamentals else None,
                    "rating_as_of_date": rating.get("as_of_date") if rating else None,
                    "parse_status": fundamentals.get("parse_status") if fundamentals else None,
                    "rating_status": rating.get("rating_status") if rating else None,
                    "sector": fundamentals.get("sector") if fundamentals else None,
                    "industry": fundamentals.get("industry") if fundamentals else None,
                    "overall_rating": rating.get("overall_rating") if rating else None,
                    "missing_metric_names": list(rating.get("missing_metric_names") or []) if rating else [],
                    "insufficient_baseline_metrics": list(rating.get("insufficient_baseline_metrics") or []) if rating else [],
                }
            )

        diagnostics.sort(key=lambda item: (str(item.get("category") or ""), str(item.get("ticker") or "")))
        diagnostic_category_counts = dict(sorted(Counter(str(item.get("category") or "unknown") for item in diagnostics).items()))

        return {
            "latest_fundamentals_as_of_date": self._to_iso(latest_fundamentals_date),
            "latest_fundamentals_updated_at": self._to_iso(latest_fundamentals_updated_at),
            "latest_baselines_as_of_date": self._to_iso(latest_baselines_date),
            "latest_baselines_updated_at": self._to_iso(latest_baselines_updated_at),
            "latest_ratings_as_of_date": self._to_iso(latest_ratings_date),
            "latest_ratings_updated_at": self._to_iso(latest_ratings_updated_at),
            "latest_fundamentals_snapshot_count": latest_fundamentals_snapshot_count,
            "latest_rating_snapshot_count": latest_rating_snapshot_count,
            "latest_fundamentals_parse_status_counts": dict(sorted(latest_fundamentals_parse_status_counts.items())),
            "latest_rating_status_counts": dict(sorted(latest_rating_status_counts.items())),
            "tickers_with_any_fundamentals": tickers_with_any_fundamentals,
            "tickers_with_latest_ok_rating": tickers_with_ok_rating,
            "diagnostics_count": len(diagnostics),
            "diagnostic_category_counts": diagnostic_category_counts,
            "diagnostics": diagnostics,
        }

    def _to_date(self, value: object) -> dt.date | None:
        if value is None:
            return None
        if isinstance(value, dt.date):
            return value
        try:
            return dt.date.fromisoformat(str(value))
        except ValueError:
            return None

    def _to_iso(self, value: object) -> str | None:
        if value is None:
            return None
        if isinstance(value, (dt.date, dt.datetime)):
            return value.isoformat()
        text = str(value).strip()
        return text or None


def _build_missing_ranges(
    *,
    coverage_start: dt.date,
    coverage_end: dt.date,
    first_trade_date: dt.date | None,
    last_trade_date: dt.date | None,
    missing_dates: list[dt.date],
) -> list[dict[str, str | int]]:
    ranges: list[tuple[dt.date, dt.date]] = []
    if first_trade_date is None:
        ranges.append((coverage_start, coverage_end))
    else:
        if first_trade_date > coverage_start:
            ranges.append((coverage_start, first_trade_date - dt.timedelta(days=1)))
        if last_trade_date is not None and last_trade_date < coverage_end:
            ranges.append((last_trade_date + dt.timedelta(days=1), coverage_end))
        ranges.extend(_collapse_dates_to_ranges(missing_dates))

    normalized: list[dict[str, str | int]] = []
    seen: set[tuple[str, str]] = set()
    for start, end in ranges:
        start_iso = start.isoformat()
        end_iso = end.isoformat()
        if (start_iso, end_iso) in seen or start > end:
            continue
        seen.add((start_iso, end_iso))
        normalized.append(
            {
                "start": start_iso,
                "end": end_iso,
                "days": (end - start).days + 1,
            }
        )
    return normalized


def _collapse_dates_to_ranges(dates: list[dt.date]) -> list[tuple[dt.date, dt.date]]:
    if not dates:
        return []
    ordered = sorted(set(dates))
    start = ordered[0]
    previous = ordered[0]
    ranges: list[tuple[dt.date, dt.date]] = []
    for current in ordered[1:]:
        if (current - previous).days <= 3:
            previous = current
            continue
        ranges.append((start, previous))
        start = current
        previous = current
    ranges.append((start, previous))
    return ranges


def _coerce_int(value: object) -> int | None:
    if value is None or value == "":
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None
