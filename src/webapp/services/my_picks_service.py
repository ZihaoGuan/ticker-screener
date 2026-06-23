from __future__ import annotations

import datetime as dt
from typing import Any

from src.ratings.repository import RatingsRepository
from src.ticker_filters import normalize_ticker_symbol
from src.webapp.repositories.my_picks_repository import MyPicksRepository


class MyPicksService:
    def __init__(self, *, repository: MyPicksRepository | None = None, database_url: str = "") -> None:
        self.repository = repository or MyPicksRepository(database_url=database_url)
        self.database_url = self.repository.database_url
        self.ratings_repository = RatingsRepository(self.database_url)

    def get_context(self) -> dict[str, Any]:
        picks = [self._serialize_pick(row) for row in self.repository.list_picks()]
        self._attach_rating_context(picks)
        self._attach_signal_context(picks)
        return {
            "database_configured": self.repository.is_configured(),
            "total_count": len(picks),
            "rows": picks,
            "available_added_dates": list(dict.fromkeys([str(row.get("added_date") or "") for row in picks if row.get("added_date")])),
        }

    def create_pick(
        self,
        *,
        ticker: str,
        notes: str = "",
        actor_user_id: int | None = None,
    ) -> dict[str, Any]:
        self._require_configured()
        normalized_ticker = self._normalize_ticker(ticker)
        self.ratings_repository.ensure_ticker_metadata_stub(normalized_ticker, source="my-picks")
        created = self.repository.create_pick(
            ticker=normalized_ticker,
            notes=str(notes or "").strip(),
            created_by_user_id=actor_user_id,
        )
        if created is None:
            raise ValueError("Failed to add pick.")
        row = self._serialize_pick(created)
        self._attach_rating_context([row])
        self._attach_signal_context([row])
        return row

    def delete_pick(self, pick_id: int) -> None:
        self._require_configured()
        if not self.repository.delete_pick(int(pick_id)):
            raise ValueError("Pick not found.")

    def _attach_rating_context(self, rows: list[dict[str, Any]]) -> None:
        tickers = sorted({str(row.get("ticker") or "").upper() for row in rows if str(row.get("ticker") or "").strip()})
        if not tickers:
            return
        fundamental_map = self.ratings_repository.load_latest_rating_snapshots_for_tickers(tickers)
        technical_map = self.ratings_repository.load_latest_technical_rating_snapshots_for_tickers(tickers, allow_older_as_of_date=True)
        technical_indicator_map = self.ratings_repository.load_latest_technical_indicator_ratings_for_tickers(tickers)
        for row in rows:
            ticker = str(row.get("ticker") or "").upper()
            fundamental = fundamental_map.get(ticker) or {}
            technical = technical_map.get(ticker) or {}
            indicators = technical_indicator_map.get(ticker) or {}
            fa_rating = _safe_float(fundamental.get("overall_rating"))
            ta_rating = _safe_float(technical.get("overall_rating"))
            leadership_score = _safe_float(technical.get("leadership_score"))
            row["sector"] = row.get("sector") or fundamental.get("sector") or technical.get("sector")
            row["industry"] = row.get("industry") or fundamental.get("industry") or technical.get("industry")
            row["ratings_as_of_date"] = str(fundamental.get("as_of_date") or technical.get("as_of_date") or "")
            row["perf_year_pct"] = _safe_float(fundamental.get("perf_year_pct"))
            row["perf_ytd_pct"] = _safe_float(fundamental.get("perf_ytd_pct"))
            row["fundamental_rating"] = fa_rating
            row["fundamental_rank"] = _safe_int(fundamental.get("current_rank"))
            row["fundamental_status"] = str(fundamental.get("rating_status") or "") or None
            row["technical_rating"] = ta_rating
            row["leadership_score"] = leadership_score
            row["technical_band"] = str(technical.get("rating_band") or "") or None
            row["technical_status"] = str(technical.get("technical_status") or "") or None
            row["technical_indicator_ratings"] = indicators
            row["als_score"] = _average_present([fa_rating, ta_rating, leadership_score])

    def _attach_signal_context(self, rows: list[dict[str, Any]]) -> None:
        tickers = sorted({str(row.get("ticker") or "").upper() for row in rows if str(row.get("ticker") or "").strip()})
        if not tickers:
            return
        summary_map = self.repository.list_recent_signal_summary(tickers)
        for row in rows:
            summary = summary_map.get(str(row.get("ticker") or "").upper()) or {}
            row["recent_signal_count"] = int(summary.get("signal_count") or 0)
            row["latest_signal_date"] = summary.get("latest_signal_date")
            row["recent_signals"] = list(summary.get("recent_signals") or [])

    def _serialize_pick(self, row: dict[str, Any]) -> dict[str, Any]:
        added_at = _to_iso_datetime(row.get("created_at"))
        added_date = added_at.split("T", 1)[0] if added_at else None
        return {
            "id": int(row.get("id") or 0),
            "ticker": str(row.get("ticker") or "").upper(),
            "notes": str(row.get("notes") or ""),
            "created_by_user_id": _safe_int(row.get("created_by_user_id")),
            "added_at": added_at,
            "added_date": added_date,
            "sector": None,
            "industry": None,
            "ratings_as_of_date": None,
            "perf_year_pct": None,
            "perf_ytd_pct": None,
            "fundamental_rating": None,
            "fundamental_rank": None,
            "fundamental_status": None,
            "technical_rating": None,
            "leadership_score": None,
            "technical_band": None,
            "technical_status": None,
            "technical_indicator_ratings": {},
            "als_score": None,
            "recent_signal_count": 0,
            "latest_signal_date": None,
            "recent_signals": [],
        }

    def _normalize_ticker(self, ticker: str) -> str:
        normalized = normalize_ticker_symbol(ticker)
        if not normalized:
            raise ValueError("Ticker is required.")
        return normalized

    def _require_configured(self) -> None:
        if not self.repository.is_configured():
            raise ValueError("TICKER_SCREENER_DATABASE_URL not set.")


def _to_iso_datetime(value: object) -> str | None:
    if hasattr(value, "isoformat"):
        return value.isoformat()
    text = str(value or "").strip()
    return text or None


def _safe_float(value: object) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _safe_int(value: object) -> int | None:
    if value in (None, ""):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _average_present(values: list[float | None]) -> float | None:
    numbers = [value for value in values if isinstance(value, (int, float))]
    if not numbers:
        return None
    return sum(float(value) for value in numbers) / len(numbers)
