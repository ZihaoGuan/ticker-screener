from __future__ import annotations

from dataclasses import asdict, dataclass
import datetime as dt
import os
from pathlib import Path
from typing import Any

import requests

from .config import AppConfig
from .cookstock_bridge import load_configured_cookstock


AIVEST_BASE_URL = "https://openapi.ainvest.com/open"
FMP_STABLE_BASE_URL = "https://financialmodelingprep.com/stable"


@dataclass(frozen=True)
class EarningsAnnotation:
    ticker: str
    release_date: str | None
    release_session: str | None
    release_label: str | None
    status: str
    status_label: str
    is_reported: bool
    eps_actual: float | None
    eps_estimate: float | None
    eps_surprise_pct: float | None
    revenue_actual: float | None
    revenue_estimate: float | None
    revenue_surprise_pct: float | None
    provider: str
    source_summary: str | None = None

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


def _safe_float(value: object) -> float | None:
    if value in (None, "", "NA", "n/a", "None"):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _normalize_eps_surprise_pct(
    actual: float | None,
    estimate: float | None,
    provider_value: float | None,
) -> float | None:
    if actual is not None and estimate not in (None, 0):
        try:
            return ((float(actual) - float(estimate)) / abs(float(estimate))) * 100.0
        except (TypeError, ValueError, ZeroDivisionError):
            pass
    if provider_value is None:
        return None
    if abs(float(provider_value)) <= 2.0:
        return float(provider_value) * 100.0
    return float(provider_value)


def _parse_date(value: object) -> dt.date | None:
    if not value:
        return None
    try:
        return dt.date.fromisoformat(str(value))
    except ValueError:
        return None


def _format_release_label(release_date: dt.date | None, release_session: str | None) -> str | None:
    if release_date is None:
        return None
    date_text = f"{release_date.month}/{release_date.day}"
    if release_session == "before_market":
        return f"{date_text} 盘前"
    if release_session == "after_market":
        return f"{date_text} 盘后"
    if release_session == "during_market":
        return f"{date_text} 盘中"
    return date_text


def _classify_status(
    release_date: dt.date | None,
    release_session: str | None,
    eps_actual: float | None,
    eps_estimate: float | None,
    as_of_date: dt.date,
) -> tuple[str, str]:
    release_label = _format_release_label(release_date, release_session)
    if eps_actual is not None and eps_estimate is not None:
        if eps_actual > eps_estimate:
            return "beat", "高于预期"
        if eps_actual < eps_estimate:
            return "miss", "低于预期"
        return "inline", "符合预期"
    if release_date is not None and release_date >= as_of_date:
        if release_label:
            return "upcoming", release_label
        return "upcoming", str(release_date)
    if release_date is not None:
        if release_label:
            return "reported_unknown", f"已发布 {release_label}"
        return "reported_unknown", f"已发布 {release_date.isoformat()}"
    return "unknown", "未知"


def _parse_session_from_summary(summary: str | None) -> str | None:
    if not summary:
        return None
    normalized = summary.strip().lower()
    if any(token in normalized for token in ("after close", "after market", "after-hours", "after hours", "post-market", "post market", "amc", "盘后")):
        return "after_market"
    if any(token in normalized for token in ("before open", "before market", "pre-market", "pre market", "bmo", "盘前")):
        return "before_market"
    if any(token in normalized for token in ("during market", "盘中")):
        return "during_market"
    return None


def _parse_session(value: object) -> str | None:
    if not value:
        return None
    normalized = str(value).strip().lower().replace("-", "_").replace(" ", "_")
    if normalized in {"amc", "after_market", "after_close", "after_market_close"}:
        return "after_market"
    if normalized in {"bmo", "before_market", "before_open", "before_market_open"}:
        return "before_market"
    if normalized in {"during_market", "market", "during_open_market"}:
        return "during_market"
    return _parse_session_from_summary(str(value))


def _watchlist_earnings_prefix(annotation: EarningsAnnotation) -> str:
    if annotation.status in {"beat", "miss", "inline"}:
        pieces = [f"Earnings {annotation.status_label}"]
        if annotation.eps_actual is not None and annotation.eps_estimate is not None:
            pieces.append(f"EPS {annotation.eps_actual:.2f} vs {annotation.eps_estimate:.2f}")
        return ". ".join(pieces) + "."
    return f"Earnings {annotation.status_label}."


class AInvestEarningsClient:
    def __init__(self, api_key: str, timeout_seconds: int = 15) -> None:
        self.api_key = api_key.strip()
        self.timeout_seconds = max(1, int(timeout_seconds))
        self.session = requests.Session()
        self.session.headers.update(
            {
                "Authorization": f"Bearer {self.api_key}",
                "Accept": "application/json",
                "User-Agent": "ticker-screener/1.0",
            }
        )

    def _get(self, path: str, params: dict[str, object]) -> dict[str, Any]:
        response = self.session.get(
            f"{AIVEST_BASE_URL}{path}",
            params=params,
            timeout=self.timeout_seconds,
        )
        response.raise_for_status()
        payload = response.json()
        if payload.get("status_code") not in (0, None):
            raise RuntimeError(f"AInvest error: {payload.get('status_msg', 'unknown error')}")
        return payload

    def fetch_history(self, ticker: str, size: int = 5) -> list[dict[str, Any]]:
        payload = self._get(
            "/securities/stock/financials/earnings",
            {"ticker": ticker, "size": size},
        )
        data = payload.get("data", [])
        return data if isinstance(data, list) else []

    def fetch_calendar_for_date(self, date_value: dt.date) -> list[dict[str, Any]]:
        payload = self._get("/calendar/earnings", {"date": date_value.isoformat()})
        data = payload.get("data", {})
        rows = data.get("data", []) if isinstance(data, dict) else []
        return rows if isinstance(rows, list) else []


class FMPEarningsClient:
    def __init__(self, api_key: str, timeout_seconds: int = 15) -> None:
        self.api_key = api_key.strip()
        self.timeout_seconds = max(1, int(timeout_seconds))
        self.session = requests.Session()
        self.session.headers.update(
            {
                "Accept": "application/json",
                "User-Agent": "ticker-screener/1.0",
            }
        )

    def _get(self, path: str, params: dict[str, object]) -> list[dict[str, Any]]:
        full_params = dict(params)
        full_params["apikey"] = self.api_key
        response = self.session.get(
            f"{FMP_STABLE_BASE_URL}{path}",
            params=full_params,
            timeout=self.timeout_seconds,
        )
        response.raise_for_status()
        payload = response.json()
        if isinstance(payload, list):
            return [row for row in payload if isinstance(row, dict)]
        raise RuntimeError("FMP returned an unexpected response payload.")

    def fetch_calendar_range(self, start_date: dt.date, end_date: dt.date) -> list[dict[str, Any]]:
        return self._get(
            "/earnings-calendar",
            {"from": start_date.isoformat(), "to": end_date.isoformat()},
        )

    def fetch_confirmed_range(self, start_date: dt.date, end_date: dt.date) -> list[dict[str, Any]]:
        return self._get(
            "/earnings-calendar-confirmed",
            {"from": start_date.isoformat(), "to": end_date.isoformat()},
        )


class YFinanceEarningsClient:
    def __init__(self) -> None:
        try:
            import yfinance as yf
        except ImportError as exc:
            raise RuntimeError("yfinance is not installed.") from exc
        self.yf = yf

    def fetch_latest(self, ticker: str) -> dict[str, Any]:
        stock = self.yf.Ticker(ticker)
        reported_release_date: dt.date | None = None
        upcoming_release_date: dt.date | None = None
        upcoming_release_session: str | None = None
        eps_actual: float | None = None
        eps_estimate: float | None = None
        eps_surprise_pct: float | None = None

        try:
            history = stock.get_earnings_history()
        except Exception:
            history = None
        if history is not None and not history.empty:
            row = history.iloc[0]
            index_value = history.index[0]
            if hasattr(index_value, "date"):
                reported_release_date = index_value.date()
            eps_actual = _safe_float(row.get("epsActual"))
            eps_estimate = _safe_float(row.get("epsEstimate"))
            eps_surprise_pct = _normalize_eps_surprise_pct(
                eps_actual,
                eps_estimate,
                _safe_float(row.get("surprisePercent")),
            )

        try:
            earnings_dates = stock.get_earnings_dates(limit=1)
        except Exception:
            earnings_dates = None
        if earnings_dates is not None and not earnings_dates.empty:
            index_value = earnings_dates.index[0]
            if hasattr(index_value, "date"):
                upcoming_release_date = index_value.date()
            hour = getattr(index_value, "hour", None)
            if hour is not None:
                if hour < 12:
                    upcoming_release_session = "before_market"
                elif hour >= 16:
                    upcoming_release_session = "after_market"
                else:
                    upcoming_release_session = "during_market"

        return {
            "reported_release_date": reported_release_date,
            "upcoming_release_date": upcoming_release_date,
            "upcoming_release_session": upcoming_release_session,
            "eps_actual": eps_actual,
            "eps_estimate": eps_estimate,
            "eps_surprise_pct": eps_surprise_pct,
            "revenue_actual": None,
            "revenue_estimate": None,
            "revenue_surprise_pct": None,
        }


def _next_week_session_map(config: AppConfig, start_date: dt.date, end_date: dt.date) -> dict[str, tuple[dt.date, str | None, str | None]]:
    cookstock = load_configured_cookstock(config)
    events = cookstock.fetch_earnings_calendar_watchlist(start_date, end_date)
    session_by_ticker: dict[str, tuple[dt.date, str | None, str | None]] = {}
    for event in events:
        ticker = str(event.get("ticker", "")).upper().strip()
        event_date = event.get("event_date")
        if not ticker or not isinstance(event_date, dt.date):
            continue
        summary = str(event.get("summary", "")).strip() or None
        session_by_ticker[ticker] = (
            event_date,
            _parse_session_from_summary(summary),
            summary,
        )
    return session_by_ticker


def _value_from(row: dict[str, Any], *keys: str) -> object:
    for key in keys:
        if key in row and row[key] not in (None, ""):
            return row[key]
    return None


def _date_from_row(row: dict[str, Any], *keys: str) -> dt.date | None:
    return _parse_date(_value_from(row, *keys))


def _select_fmp_calendar_row(rows: list[dict[str, Any]], as_of_date: dt.date) -> dict[str, Any] | None:
    if not rows:
        return None

    def _row_date(row: dict[str, Any]) -> dt.date:
        return _date_from_row(row, "date", "reportDate", "reportedDate") or dt.date.min

    reported_rows = [
        row
        for row in rows
        if _row_date(row) <= as_of_date
        and (
            _safe_float(_value_from(row, "eps", "epsActual", "actualEps")) is not None
            or _safe_float(_value_from(row, "epsEstimated", "epsEstimate", "estimatedEps")) is not None
        )
    ]
    if reported_rows:
        return max(reported_rows, key=_row_date)

    upcoming_rows = [row for row in rows if _row_date(row) >= as_of_date]
    if upcoming_rows:
        return min(upcoming_rows, key=_row_date)

    return max(rows, key=_row_date)


def _select_fmp_confirmed_row(rows: list[dict[str, Any]], as_of_date: dt.date) -> dict[str, Any] | None:
    if not rows:
        return None

    def _row_date(row: dict[str, Any]) -> dt.date:
        return _date_from_row(row, "date", "reportDate", "reportedDate") or dt.date.max

    future_rows = [row for row in rows if _row_date(row) >= as_of_date]
    if future_rows:
        return min(future_rows, key=_row_date)
    return min(rows, key=_row_date)


def _build_fmp_annotation(
    ticker: str,
    calendar_row: dict[str, Any] | None,
    confirmed_row: dict[str, Any] | None,
    next_week_map: dict[str, tuple[dt.date, str | None, str | None]],
    as_of_date: dt.date,
) -> EarningsAnnotation:
    reported_release_date = _date_from_row(calendar_row or {}, "date", "reportDate", "reportedDate")
    reported_release_session = _parse_session(
        _value_from(confirmed_row or {}, "time", "publishTime", "session")
        or _value_from(calendar_row or {}, "time", "publishTime", "session")
    )
    source_summary = None

    eps_actual = _safe_float(_value_from(calendar_row or {}, "eps", "epsActual", "actualEps"))
    eps_estimate = _safe_float(_value_from(calendar_row or {}, "epsEstimated", "epsEstimate", "estimatedEps"))
    revenue_actual = _safe_float(_value_from(calendar_row or {}, "revenue", "revenueActual", "actualRevenue"))
    revenue_estimate = _safe_float(_value_from(calendar_row or {}, "revenueEstimated", "revenueEstimate", "estimatedRevenue"))
    eps_surprise_pct = _normalize_eps_surprise_pct(
        eps_actual,
        eps_estimate,
        _safe_float(_value_from(calendar_row or {}, "epsSurprisePercent", "epsSurprise", "surprisePercent")),
    )
    revenue_surprise_pct = _safe_float(_value_from(calendar_row or {}, "revenueSurprisePercent", "revenueSurprise"))

    release_date = reported_release_date
    release_session = reported_release_session
    if ticker in next_week_map:
        mapped_date, mapped_session, mapped_summary = next_week_map[ticker]
        if eps_actual is None and eps_estimate is None:
            release_date = mapped_date
            release_session = release_session or mapped_session
            source_summary = mapped_summary or source_summary
        elif release_session is None and release_date == mapped_date:
            release_session = mapped_session
            source_summary = mapped_summary or source_summary

    status, status_label = _classify_status(
        release_date,
        release_session,
        eps_actual,
        eps_estimate,
        as_of_date,
    )

    return EarningsAnnotation(
        ticker=ticker,
        release_date=release_date.isoformat() if release_date else None,
        release_session=release_session,
        release_label=_format_release_label(release_date, release_session),
        status=status,
        status_label=status_label,
        is_reported=status in {"beat", "miss", "inline", "reported_unknown"},
        eps_actual=eps_actual,
        eps_estimate=eps_estimate,
        eps_surprise_pct=eps_surprise_pct,
        revenue_actual=revenue_actual,
        revenue_estimate=revenue_estimate,
        revenue_surprise_pct=revenue_surprise_pct,
        provider="fmp",
        source_summary=source_summary,
    )


def _build_ainvest_annotation(
    ticker: str,
    history: list[dict[str, Any]],
    upcoming: dict[str, Any] | None,
    next_week_map: dict[str, tuple[dt.date, str | None, str | None]],
    as_of_date: dt.date,
) -> EarningsAnnotation:
    reported_release_date: dt.date | None = None
    release_session: str | None = None
    source_summary: str | None = None
    eps_actual: float | None = None
    eps_estimate: float | None = None
    eps_surprise_pct: float | None = None
    revenue_actual: float | None = None
    revenue_estimate: float | None = None
    revenue_surprise_pct: float | None = None
    upcoming_release_date: dt.date | None = None

    if history:
        latest = history[0]
        reported_release_date = _parse_date(latest.get("release_date"))
        eps_actual = _safe_float(latest.get("eps_actual"))
        eps_estimate = _safe_float(latest.get("eps_forecast"))
        eps_surprise_pct = _normalize_eps_surprise_pct(
            eps_actual,
            eps_estimate,
            _safe_float(latest.get("eps_surprise")),
        )
        revenue_actual = _safe_float(latest.get("revenue_actual"))
        revenue_estimate = _safe_float(latest.get("revenue_forecast"))
        revenue_surprise_pct = _safe_float(latest.get("revenue_surprise"))

    if upcoming is not None:
        upcoming_release_date = _parse_date(upcoming.get("date"))

    release_date = reported_release_date
    if release_date is None and upcoming_release_date is not None:
        release_date = upcoming_release_date
        eps_actual = _safe_float(upcoming.get("eps_actual"))
        eps_estimate = _safe_float(upcoming.get("eps_forecast"))
        eps_surprise_pct = _normalize_eps_surprise_pct(
            eps_actual,
            eps_estimate,
            _safe_float(upcoming.get("eps_surprise")),
        )
        revenue_actual = _safe_float(upcoming.get("revenue_actual"))
        revenue_estimate = _safe_float(upcoming.get("revenue_forecast"))
        revenue_surprise_pct = _safe_float(upcoming.get("revenue_surprise"))

    if ticker in next_week_map:
        mapped_date, mapped_session, mapped_summary = next_week_map[ticker]
        if release_date is None:
            release_date = mapped_date
            release_session = mapped_session or release_session
            source_summary = mapped_summary or source_summary
        elif release_session is None and release_date == mapped_date:
            release_session = mapped_session
            source_summary = mapped_summary or source_summary

    status, status_label = _classify_status(
        release_date,
        release_session,
        eps_actual,
        eps_estimate,
        as_of_date,
    )

    return EarningsAnnotation(
        ticker=ticker,
        release_date=release_date.isoformat() if release_date else None,
        release_session=release_session,
        release_label=_format_release_label(release_date, release_session),
        status=status,
        status_label=status_label,
        is_reported=status in {"beat", "miss", "inline", "reported_unknown"},
        eps_actual=eps_actual,
        eps_estimate=eps_estimate,
        eps_surprise_pct=eps_surprise_pct,
        revenue_actual=revenue_actual,
        revenue_estimate=revenue_estimate,
        revenue_surprise_pct=revenue_surprise_pct,
        provider="ainvest+ics",
        source_summary=source_summary,
    )


def _build_yfinance_annotation(
    ticker: str,
    payload: dict[str, Any],
    next_week_map: dict[str, tuple[dt.date, str | None, str | None]],
    as_of_date: dt.date,
) -> EarningsAnnotation:
    release_date = payload.get("reported_release_date")
    release_session = None
    source_summary: str | None = None
    if release_date is None:
        release_date = payload.get("upcoming_release_date")
        release_session = payload.get("upcoming_release_session")
    if ticker in next_week_map:
        mapped_date, mapped_session, mapped_summary = next_week_map[ticker]
        if release_date is None:
            release_date = mapped_date
            release_session = release_session or mapped_session
            source_summary = mapped_summary or source_summary
        elif release_session is None and release_date == mapped_date:
            release_session = mapped_session
            source_summary = mapped_summary or source_summary
    status, status_label = _classify_status(
        release_date,
        release_session,
        payload.get("eps_actual"),
        payload.get("eps_estimate"),
        as_of_date,
    )
    return EarningsAnnotation(
        ticker=ticker,
        release_date=release_date.isoformat() if isinstance(release_date, dt.date) else None,
        release_session=release_session,
        release_label=_format_release_label(release_date if isinstance(release_date, dt.date) else None, release_session),
        status=status,
        status_label=status_label,
        is_reported=status in {"beat", "miss", "inline", "reported_unknown"},
        eps_actual=payload.get("eps_actual"),
        eps_estimate=payload.get("eps_estimate"),
        eps_surprise_pct=payload.get("eps_surprise_pct"),
        revenue_actual=payload.get("revenue_actual"),
        revenue_estimate=payload.get("revenue_estimate"),
        revenue_surprise_pct=payload.get("revenue_surprise_pct"),
        provider="yfinance+ics",
        source_summary=source_summary,
    )


def build_earnings_annotations(
    tickers: list[str],
    config: AppConfig,
    *,
    as_of_date: dt.date | None = None,
    upcoming_days: int = 14,
    provider: str | None = None,
    ainvest_api_key: str | None = None,
    fmp_api_key: str | None = None,
) -> dict[str, EarningsAnnotation]:
    as_of_date = as_of_date or dt.date.today()
    normalized_tickers = []
    seen: set[str] = set()
    for ticker in tickers:
        normalized = str(ticker).upper().strip()
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        normalized_tickers.append(normalized)

    annotations: dict[str, EarningsAnnotation] = {}
    if not normalized_tickers:
        return annotations

    upcoming_end = as_of_date + dt.timedelta(days=max(0, int(upcoming_days)))
    try:
        next_week_map = _next_week_session_map(config, as_of_date, upcoming_end)
    except Exception as exc:
        print(f"warning: unable to load ICS earnings sessions: {exc}")
        next_week_map = {}

    selected_provider = (provider or config.earnings_enrichment_provider or "yfinance").strip().lower()
    fmp_key = (fmp_api_key or os.getenv("FMP_API_KEY") or "").strip()
    ainvest_key = (ainvest_api_key or os.getenv("AINVEST_API_KEY") or "").strip()

    if selected_provider == "auto":
        if ainvest_key:
            selected_provider = "ainvest"
        elif fmp_key:
            selected_provider = "fmp"
        else:
            selected_provider = "yfinance"

    if selected_provider == "yfinance":
        client = YFinanceEarningsClient()
        for ticker in normalized_tickers:
            try:
                payload = client.fetch_latest(ticker)
            except Exception as exc:
                print(f"warning: unable to load yfinance earnings for {ticker}: {exc}")
                payload = {
                    "reported_release_date": None,
                    "upcoming_release_date": None,
                    "upcoming_release_session": None,
                    "eps_actual": None,
                    "eps_estimate": None,
                    "eps_surprise_pct": None,
                    "revenue_actual": None,
                    "revenue_estimate": None,
                    "revenue_surprise_pct": None,
                }
            annotations[ticker] = _build_yfinance_annotation(
                ticker,
                payload,
                next_week_map,
                as_of_date,
            )
        return annotations

    if selected_provider == "fmp":
        if not fmp_key:
            print("warning: FMP_API_KEY is not set; falling back to yfinance for earnings enrichment.")
            return build_earnings_annotations(
                normalized_tickers,
                config,
                as_of_date=as_of_date,
                upcoming_days=upcoming_days,
                provider="yfinance",
                ainvest_api_key=ainvest_key,
                fmp_api_key=fmp_api_key,
            )
        client = FMPEarningsClient(fmp_key, timeout_seconds=config.request_timeout_seconds)
        calendar_rows: list[dict[str, Any]]
        confirmed_rows: list[dict[str, Any]]
        try:
            calendar_rows = client.fetch_calendar_range(as_of_date - dt.timedelta(days=120), upcoming_end)
        except Exception as exc:
            print(f"warning: unable to load FMP earnings calendar: {exc}")
            calendar_rows = []
        try:
            confirmed_rows = client.fetch_confirmed_range(as_of_date, upcoming_end)
        except Exception as exc:
            print(f"warning: unable to load FMP confirmed earnings calendar: {exc}")
            confirmed_rows = []
        calendar_by_ticker: dict[str, list[dict[str, Any]]] = {}
        for row in calendar_rows:
            ticker = str(_value_from(row, "symbol", "ticker", "companySymbol") or "").upper().strip()
            if ticker:
                calendar_by_ticker.setdefault(ticker, []).append(row)
        confirmed_by_ticker: dict[str, list[dict[str, Any]]] = {}
        for row in confirmed_rows:
            ticker = str(_value_from(row, "symbol", "ticker", "companySymbol") or "").upper().strip()
            if ticker:
                confirmed_by_ticker.setdefault(ticker, []).append(row)
        for ticker in normalized_tickers:
            annotations[ticker] = _build_fmp_annotation(
                ticker,
                _select_fmp_calendar_row(calendar_by_ticker.get(ticker, []), as_of_date),
                _select_fmp_confirmed_row(confirmed_by_ticker.get(ticker, []), as_of_date),
                next_week_map,
                as_of_date,
            )
        return annotations

    if selected_provider == "ainvest":
        if not ainvest_key:
            print("warning: AINVEST_API_KEY is not set; falling back to yfinance for earnings enrichment.")
            return build_earnings_annotations(
                normalized_tickers,
                config,
                as_of_date=as_of_date,
                upcoming_days=upcoming_days,
                provider="yfinance",
                ainvest_api_key=ainvest_key,
                fmp_api_key=fmp_api_key,
            )
        client = AInvestEarningsClient(
            ainvest_key,
            timeout_seconds=config.request_timeout_seconds,
        )
        upcoming_by_ticker: dict[str, dict[str, Any]] = {}
        for offset in range(max(0, int(upcoming_days)) + 1):
            date_value = as_of_date + dt.timedelta(days=offset)
            try:
                rows = client.fetch_calendar_for_date(date_value)
            except Exception as exc:
                print(f"warning: unable to load AInvest earnings calendar for {date_value}: {exc}")
                continue
            for row in rows:
                ticker = str(row.get("ticker", "")).upper().strip()
                if ticker and ticker not in upcoming_by_ticker:
                    upcoming_by_ticker[ticker] = row

        for ticker in normalized_tickers:
            try:
                history = client.fetch_history(ticker, size=5)
            except Exception as exc:
                print(f"warning: unable to load AInvest earnings history for {ticker}: {exc}")
                history = []
            annotations[ticker] = _build_ainvest_annotation(
                ticker,
                history,
                upcoming_by_ticker.get(ticker),
                next_week_map,
                as_of_date,
            )
        return annotations

    raise ValueError(f"Unsupported earnings enrichment provider: {selected_provider}")

    return annotations


def enrich_watchlist_entries(
    entries: list[dict[str, Any]],
    annotations: dict[str, EarningsAnnotation],
    *,
    append_summary: bool = True,
) -> list[dict[str, Any]]:
    enriched: list[dict[str, Any]] = []
    for entry in entries:
        row = dict(entry)
        ticker = str(row.get("ticker", "")).upper().strip()
        annotation = annotations.get(ticker)
        if annotation is None:
            enriched.append(row)
            continue
        row["earnings_context"] = annotation.to_dict()
        row["earnings_release_date"] = annotation.release_date
        row["earnings_release_session"] = annotation.release_session
        row["earnings_status"] = annotation.status
        row["earnings_status_label"] = annotation.status_label
        row["earnings_eps_actual"] = annotation.eps_actual
        row["earnings_eps_estimate"] = annotation.eps_estimate
        row["earnings_eps_surprise_pct"] = annotation.eps_surprise_pct
        if append_summary:
            prefix = _watchlist_earnings_prefix(annotation)
            summary = str(row.get("summary", "")).strip()
            if summary:
                row["summary"] = f"{prefix} {summary}"
            else:
                row["summary"] = prefix
        enriched.append(row)
    return enriched


def enrich_raw_hits(payload: dict[str, Any], annotations: dict[str, EarningsAnnotation]) -> dict[str, Any]:
    result = dict(payload)
    hits = result.get("hits")
    if not isinstance(hits, list):
        return result
    enriched_hits: list[dict[str, Any]] = []
    for hit in hits:
        if not isinstance(hit, dict):
            enriched_hits.append(hit)
            continue
        row = dict(hit)
        ticker = str(row.get("ticker", "")).upper().strip()
        annotation = annotations.get(ticker)
        if annotation is not None:
            row["earnings_context"] = annotation.to_dict()
            row["earnings_release_date"] = annotation.release_date
            row["earnings_release_session"] = annotation.release_session
            row["earnings_status"] = annotation.status
            row["earnings_status_label"] = annotation.status_label
            row["earnings_eps_actual"] = annotation.eps_actual
            row["earnings_eps_estimate"] = annotation.eps_estimate
            row["earnings_eps_surprise_pct"] = annotation.eps_surprise_pct
        enriched_hits.append(row)
    result["hits"] = enriched_hits
    return result


def infer_output_path(input_path: Path, suffix: str = "_earnings") -> Path:
    return input_path.with_name(f"{input_path.stem}{suffix}{input_path.suffix}")
