from __future__ import annotations

import datetime as dt
from typing import Any

from src.market_data_access import load_many_ticker_windows_for_range
from src.ratings.repository import RatingsRepository
from src.ticker_filters import normalize_ticker_symbol
from src.trendline_snapshots import load_latest_trendline_snapshot_map
from src.webapp.config import load_webapp_config
from src.webapp.repositories.watchlist_repository import WatchlistRepository
from src.webapp.repositories.my_picks_repository import MyPicksRepository


FUNDAMENTAL_CHECKLIST: tuple[dict[str, str], ...] = (
    {
        "key": "revenue_growth",
        "label": "Revenue Growth",
        "short_label": "Rev",
        "description": "Revenue growth is strong, ideally 20% to 30%+ and not flattening or declining.",
    },
    {
        "key": "eps_growth",
        "label": "EPS Growth",
        "short_label": "EPS",
        "description": "EPS growth is strong and still improving year over year.",
    },
    {
        "key": "eps_inflection",
        "label": "EPS Inflection",
        "short_label": "EPS Turn",
        "description": "EPS is turning from negative to positive, or showing a major profitability inflection.",
    },
    {
        "key": "margins_profitability",
        "label": "Margins / Profitability",
        "short_label": "Margin",
        "description": "Margins and profitability look healthy or are improving.",
    },
    {
        "key": "cashflow_net_income",
        "label": "FCF / Net Income",
        "short_label": "FCF",
        "description": "Free cash flow and net income are positive or trending in the right direction.",
    },
    {
        "key": "debt_equity",
        "label": "Debt / Equity",
        "short_label": "D/E",
        "description": "Debt to equity looks reasonable and balance-sheet risk is acceptable.",
    },
    {
        "key": "estimates_guidance",
        "label": "Estimates / Guidance",
        "short_label": "Guide",
        "description": "Forward estimates, earnings beats, and guidance support the growth story.",
    },
    {
        "key": "technical_confirmation",
        "label": "Technical Confirmation",
        "short_label": "TA",
        "description": "Chart shows confirmation such as a constructive base, cup, or breakout setup.",
    },
    {
        "key": "avoid_decliners",
        "label": "Avoid Declining Names",
        "short_label": "No Decline",
        "description": "Revenue and EPS are not in a flat or declining trend.",
    },
)

_CHECKLIST_KEYS = {item["key"] for item in FUNDAMENTAL_CHECKLIST}


class MyPicksService:
    def __init__(self, *, repository: MyPicksRepository | None = None, database_url: str = "") -> None:
        self.repository = repository or MyPicksRepository(database_url=database_url)
        self.database_url = self.repository.database_url
        self.ratings_repository = RatingsRepository(self.database_url)
        self.watchlist_repository = WatchlistRepository(artifacts_dir=load_webapp_config().artifacts_dir, database_url=self.database_url)

    def get_context(self) -> dict[str, Any]:
        picks = [self._serialize_pick(row) for row in self.repository.list_picks()]
        self._attach_rating_context(picks)
        self._attach_signal_context(picks)
        self._attach_trendline_context(picks)
        self._attach_price_change_context(picks)
        return {
            "database_configured": self.repository.is_configured(),
            "total_count": len(picks),
            "rows": picks,
            "available_added_dates": list(dict.fromkeys([str(row.get("added_date") or "") for row in picks if row.get("added_date")])),
            "fundamental_checklist": list(FUNDAMENTAL_CHECKLIST),
            "fundamental_summary": [
                "Prioritize stocks with strong revenue growth, usually 20% to 30%+ and ideally accelerating.",
                "EPS growth matters most; pay extra attention when EPS flips from negative to positive.",
                "Check margins, profitability, free cash flow, net income, and debt/equity before trusting the story.",
                "Use forward estimates, earnings beats, and guidance to confirm growth is likely to continue.",
                "Avoid companies with flat or declining revenue and EPS, even if the brand is well known.",
                "Only keep names that also have technical confirmation such as a strong base or breakout setup.",
            ],
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
            checklist=self._empty_checklist(),
            created_by_user_id=actor_user_id,
        )
        if created is None:
            raise ValueError("Failed to add pick.")
        row = self._serialize_pick(created)
        self._attach_rating_context([row])
        self._attach_signal_context([row])
        self._attach_trendline_context([row])
        self._attach_price_change_context([row])
        return row

    def delete_pick(self, pick_id: int) -> None:
        self._require_configured()
        if not self.repository.delete_pick(int(pick_id)):
            raise ValueError("Pick not found.")

    def update_pick_checklist_item(self, *, pick_id: int, key: str, checked: bool) -> dict[str, Any]:
        self._require_configured()
        normalized_key = str(key or "").strip()
        if normalized_key not in _CHECKLIST_KEYS:
            raise ValueError("Unknown checklist item.")
        rows = [self._serialize_pick(row) for row in self.repository.list_picks()]
        target = next((row for row in rows if int(row.get("id") or 0) == int(pick_id)), None)
        if target is None:
            raise ValueError("Pick not found.")
        checklist = dict(target.get("checklist") or {})
        checklist[normalized_key] = bool(checked)
        updated = self.repository.update_pick_checklist(int(pick_id), checklist)
        if updated is None:
            raise ValueError("Pick not found.")
        row = self._serialize_pick(updated)
        self._attach_rating_context([row])
        self._attach_signal_context([row])
        self._attach_trendline_context([row])
        self._attach_price_change_context([row])
        return row

    def _attach_rating_context(self, rows: list[dict[str, Any]]) -> None:
        tickers = sorted({str(row.get("ticker") or "").upper() for row in rows if str(row.get("ticker") or "").strip()})
        if not tickers:
            return
        fundamental_map = self.ratings_repository.load_latest_rating_snapshots_for_tickers(tickers)
        technical_map = self.ratings_repository.load_latest_technical_rating_snapshots_for_tickers(tickers, allow_older_as_of_date=True)
        technical_indicator_map = self.ratings_repository.load_latest_technical_indicator_ratings_for_tickers(tickers)
        canslim_map = self.watchlist_repository.load_latest_stored_canslim_score_map(tickers)
        vcp_map = self.watchlist_repository.load_latest_stored_vcp_score_map(tickers)
        for row in rows:
            ticker = str(row.get("ticker") or "").upper()
            fundamental = fundamental_map.get(ticker) or {}
            technical = technical_map.get(ticker) or {}
            indicators = technical_indicator_map.get(ticker) or {}
            canslim = canslim_map.get(ticker) or {}
            vcp = vcp_map.get(ticker) or {}
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
            row["canslim_score"] = _safe_int(canslim.get("canslim_score"))
            row["canslim_max_score"] = _safe_int(canslim.get("canslim_max_score"))
            row["canslim_rank"] = _safe_int(canslim.get("canslim_rank"))
            row["vcp_score"] = _safe_float(vcp.get("vcp_score"))
            row["vcp_rating"] = str(vcp.get("vcp_rating") or "") or None
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

    def _attach_trendline_context(self, rows: list[dict[str, Any]]) -> None:
        tickers = sorted({str(row.get("ticker") or "").upper() for row in rows if str(row.get("ticker") or "").strip()})
        if not tickers:
            return
        snapshot_map = load_latest_trendline_snapshot_map(
            tickers,
            as_of_date=dt.date.today(),
            database_url=self.database_url,
        )
        for row in rows:
            snapshot = snapshot_map.get(str(row.get("ticker") or "").upper()) or {}
            close = _safe_float(snapshot.get("close"))
            daily_ema9 = _safe_float(snapshot.get("daily_ema9"))
            daily_ema21 = _safe_float(snapshot.get("daily_ema21"))
            row["trendline_as_of_date"] = _to_iso_date(snapshot.get("trade_date"))
            row["latest_close"] = close
            row["daily_ema9"] = daily_ema9
            row["daily_ema21"] = daily_ema21
            row["distance_to_ema9_pct"] = _percent_distance(close, daily_ema9)
            row["distance_to_ema21_pct"] = _percent_distance(close, daily_ema21)

    def _attach_price_change_context(self, rows: list[dict[str, Any]]) -> None:
        tickers = sorted({str(row.get("ticker") or "").upper() for row in rows if str(row.get("ticker") or "").strip()})
        if not tickers:
            return
        today = dt.date.today()
        current_year_start = dt.date(today.year, 1, 1)
        added_dates = [_to_date(row.get("added_at")) for row in rows]
        baseline_dates = [item for item in [current_year_start, *added_dates] if item is not None]
        if not baseline_dates:
            return
        frames = load_many_ticker_windows_for_range(
            tickers,
            start_date=min(baseline_dates),
            end_date=today,
            trading_days_needed=30,
            database_url=self.database_url,
        )
        for row in rows:
            ticker = str(row.get("ticker") or "").upper()
            frame = frames.get(ticker)
            row["change_1d_pct"] = None
            row["change_since_added_pct"] = None
            row["ema9_tested_since_added"] = None
            row["ema21_tested_since_added"] = None
            if frame is None or frame.empty or "Close" not in frame:
                continue
            close_series = frame["Close"].dropna()
            if close_series.empty:
                continue
            latest_close = _safe_float(close_series.iloc[-1])
            if row.get("latest_close") is None:
                row["latest_close"] = latest_close
            if len(close_series) >= 2:
                prev_close = _safe_float(close_series.iloc[-2])
                row["change_1d_pct"] = _percent_change(latest_close, prev_close)
            ytd_baseline = _close_on_or_before(frame, current_year_start)
            if row.get("perf_ytd_pct") is None:
                row["perf_ytd_pct"] = _percent_change(latest_close, ytd_baseline)
            added_date = _to_date(row.get("added_at"))
            added_baseline = _close_on_or_after(frame, added_date) if added_date is not None else None
            if added_baseline is None and added_date is not None:
                added_baseline = _close_on_or_before(frame, added_date)
            row["change_since_added_pct"] = _percent_change(latest_close, added_baseline)
            row["ema9_tested_since_added"] = _was_ema_tested_since_date(frame, added_date, 9)
            row["ema21_tested_since_added"] = _was_ema_tested_since_date(frame, added_date, 21)

    def _serialize_pick(self, row: dict[str, Any]) -> dict[str, Any]:
        added_at = _to_iso_datetime(row.get("created_at"))
        added_date = added_at.split("T", 1)[0] if added_at else None
        return {
            "id": int(row.get("id") or 0),
            "ticker": str(row.get("ticker") or "").upper(),
            "notes": str(row.get("notes") or ""),
            "checklist": self._normalize_checklist(row.get("checklist_json")),
            "created_by_user_id": _safe_int(row.get("created_by_user_id")),
            "added_at": added_at,
            "added_date": added_date,
            "sector": None,
            "industry": None,
            "ratings_as_of_date": None,
            "perf_year_pct": None,
            "perf_ytd_pct": None,
            "change_1d_pct": None,
            "change_since_added_pct": None,
            "ema9_tested_since_added": None,
            "ema21_tested_since_added": None,
            "fundamental_rating": None,
            "fundamental_rank": None,
            "fundamental_status": None,
            "technical_rating": None,
            "leadership_score": None,
            "technical_band": None,
            "technical_status": None,
            "technical_indicator_ratings": {},
            "canslim_score": None,
            "canslim_max_score": None,
            "canslim_rank": None,
            "vcp_score": None,
            "vcp_rating": None,
            "als_score": None,
            "recent_signal_count": 0,
            "latest_signal_date": None,
            "recent_signals": [],
            "trendline_as_of_date": None,
            "latest_close": None,
            "daily_ema9": None,
            "daily_ema21": None,
            "distance_to_ema9_pct": None,
            "distance_to_ema21_pct": None,
        }

    def _normalize_ticker(self, ticker: str) -> str:
        normalized = normalize_ticker_symbol(ticker)
        if not normalized:
            raise ValueError("Ticker is required.")
        return normalized

    def _normalize_checklist(self, value: object) -> dict[str, bool]:
        raw = value if isinstance(value, dict) else {}
        return {key: bool(raw.get(key)) for key in _CHECKLIST_KEYS}

    def _empty_checklist(self) -> dict[str, bool]:
        return {key: False for key in _CHECKLIST_KEYS}

    def _require_configured(self) -> None:
        if not self.repository.is_configured():
            raise ValueError("TICKER_SCREENER_DATABASE_URL not set.")


def _to_iso_datetime(value: object) -> str | None:
    if hasattr(value, "isoformat"):
        return value.isoformat()
    text = str(value or "").strip()
    return text or None


def _to_iso_date(value: object) -> str | None:
    if isinstance(value, dt.datetime):
        return value.date().isoformat()
    if isinstance(value, dt.date):
        return value.isoformat()
    text = str(value or "").strip()
    return text or None


def _to_date(value: object) -> dt.date | None:
    if isinstance(value, dt.datetime):
        return value.date()
    if isinstance(value, dt.date):
        return value
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return dt.date.fromisoformat(text.split("T", 1)[0])
    except ValueError:
        return None


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


def _percent_distance(value: float | None, baseline: float | None) -> float | None:
    if value is None or baseline is None or baseline == 0:
        return None
    return ((value - baseline) / baseline) * 100.0


def _percent_change(value: float | None, baseline: float | None) -> float | None:
    return _percent_distance(value, baseline)


def _close_on_or_before(frame: Any, target_date: dt.date) -> float | None:
    if target_date is None or frame is None or frame.empty or "Close" not in frame:
        return None
    history = frame.loc[frame.index.date <= target_date, "Close"].dropna()
    if history.empty:
        return None
    return _safe_float(history.iloc[-1])


def _close_on_or_after(frame: Any, target_date: dt.date) -> float | None:
    if target_date is None or frame is None or frame.empty or "Close" not in frame:
        return None
    history = frame.loc[frame.index.date >= target_date, "Close"].dropna()
    if history.empty:
        return None
    return _safe_float(history.iloc[0])


def _was_ema_tested_since_date(frame: Any, target_date: dt.date | None, length: int) -> bool | None:
    if target_date is None or frame is None or frame.empty or "Close" not in frame:
        return None
    try:
        ema_series = frame["Close"].ewm(span=length, adjust=False).mean()
    except Exception:
        return None
    probe_series = frame["Low"] if "Low" in frame else frame["Close"]
    if probe_series is None:
        return None
    history = frame.loc[frame.index.date >= target_date].copy()
    if history.empty:
        return None
    history["ema"] = ema_series.loc[history.index]
    history["probe"] = probe_series.loc[history.index]
    history = history.dropna(subset=["ema", "probe"])
    if history.empty:
        return None
    return bool((history["probe"] <= history["ema"]).any())
