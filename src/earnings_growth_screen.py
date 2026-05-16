from __future__ import annotations

from dataclasses import asdict, dataclass
import datetime as dt
import os
from statistics import median
from typing import Any

import pandas as pd
import requests

from .config import AppConfig
from .cookstock_bridge import load_configured_cookstock
from .pre_earnings_screen import PreEarningsEvent


FMP_STABLE_BASE_URL = "https://financialmodelingprep.com/stable"
AIVEST_BASE_URL = "https://openapi.ainvest.com/open"


@dataclass(frozen=True)
class EarningsGrowthHit:
    ticker: str
    earnings_date: str | None
    earnings_summary: str | None
    sector: str | None
    exchange: str | None
    benchmark_ticker: str
    current_price: float
    ma_short: float
    ma_medium: float
    ma_long: float
    ma_short_length: int
    ma_medium_length: int
    ma_long_length: int
    ma_stack_bullish: bool
    latest_quarter_revenue: float
    revenue_yoy_pct: float
    latest_eps_actual: float
    eps_improving_quarters: int
    eps_series: list[float]
    institutional_ownership_pct: float
    historical_earnings_moves_pct: list[float]
    large_move_occurrences: int
    median_post_earnings_move_pct: float
    next_earnings_session: str | None
    reasons: list[str]

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True)
class EarningsGrowthScreenResult:
    run_date: str
    benchmark_ticker: str
    earnings_provider: str
    financials_provider: str
    total_tickers: int
    passed_tickers: int
    failed_tickers: list[dict[str, str]]
    hits: list[EarningsGrowthHit]

    def to_dict(self) -> dict[str, object]:
        return {
            "run_date": self.run_date,
            "benchmark_ticker": self.benchmark_ticker,
            "earnings_provider": self.earnings_provider,
            "financials_provider": self.financials_provider,
            "total_tickers": self.total_tickers,
            "passed_tickers": self.passed_tickers,
            "failed_tickers": self.failed_tickers,
            "hits": [item.to_dict() for item in self.hits],
        }


def _safe_float(value: object) -> float | None:
    if value in (None, "", "NA", "n/a", "None"):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _parse_date(value: object) -> dt.date | None:
    if not value:
        return None
    try:
        return dt.date.fromisoformat(str(value))
    except ValueError:
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
    return None


def _normalize_pct(value: float | None) -> float | None:
    if value is None:
        return None
    if abs(value) <= 1.0:
        return value * 100.0
    return value


class FMPClient:
    def __init__(self, api_key: str, timeout_seconds: int = 20) -> None:
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
        query = dict(params)
        query["apikey"] = self.api_key
        response = self.session.get(
            f"{FMP_STABLE_BASE_URL}{path}",
            params=query,
            timeout=self.timeout_seconds,
        )
        response.raise_for_status()
        payload = response.json()
        if isinstance(payload, list):
            return [item for item in payload if isinstance(item, dict)]
        raise RuntimeError(f"Unexpected FMP response for {path}")

    def get_earnings(self, ticker: str, limit: int = 12) -> list[dict[str, Any]]:
        return self._get("/earnings", {"symbol": ticker, "limit": limit})

    def get_income_statements(self, ticker: str, limit: int = 8) -> list[dict[str, Any]]:
        return self._get("/income-statement", {"symbol": ticker, "period": "quarter", "limit": limit})

    def get_latest_institutional_ownership_pct(self, ticker: str) -> float | None:
        current = dt.date.today()
        quarter = ((current.month - 1) // 3) + 1
        attempts: list[tuple[int, int]] = []
        year = current.year
        q = quarter
        for _ in range(8):
            attempts.append((year, q))
            q -= 1
            if q == 0:
                q = 4
                year -= 1

        for year_value, quarter_value in attempts:
            try:
                rows = self._get(
                    "/institutional-ownership/symbol-positions-summary",
                    {"symbol": ticker, "year": year_value, "quarter": quarter_value},
                )
            except Exception:
                continue
            if not rows:
                continue
            row = rows[0]
            pct = _safe_float(
                row.get("ownershipPercentage")
                or row.get("ownershipPercent")
                or row.get("institutionalOwnershipPercentage")
                or row.get("institutionalOwnershipPercent")
            )
            pct = _normalize_pct(pct)
            if pct is not None:
                return pct
        return None


class YFinanceGrowthClient:
    def __init__(self) -> None:
        try:
            import yfinance as yf
        except ImportError as exc:
            raise RuntimeError("yfinance is not installed.") from exc
        self.yf = yf

    def get_earnings(self, ticker: str, limit: int = 12) -> list[dict[str, Any]]:
        stock = self.yf.Ticker(ticker)
        rows: list[dict[str, Any]] = []

        try:
            earnings_dates = stock.get_earnings_dates(limit=limit)
        except Exception:
            earnings_dates = None
        if earnings_dates is not None and not earnings_dates.empty:
            for idx, row in earnings_dates.iterrows():
                date_value = idx.date() if hasattr(idx, "date") else None
                if date_value is None:
                    continue
                time_label = None
                hour = getattr(idx, "hour", None)
                if hour is not None:
                    if hour < 12:
                        time_label = "bmo"
                    elif hour >= 16:
                        time_label = "amc"
                rows.append(
                    {
                        "date": date_value.isoformat(),
                        "time": time_label,
                        "epsEstimated": _safe_float(row.get("EPS Estimate")),
                        "eps": _safe_float(row.get("Reported EPS")),
                    }
                )

        if rows:
            return rows[:limit]

        try:
            history = stock.get_earnings_history()
        except Exception:
            history = None
        if history is not None and not history.empty:
            for idx, row in history.iterrows():
                date_value = idx.date() if hasattr(idx, "date") else None
                if date_value is None:
                    continue
                rows.append(
                    {
                        "date": date_value.isoformat(),
                        "epsEstimated": _safe_float(row.get("epsEstimate")),
                        "eps": _safe_float(row.get("epsActual")),
                    }
                )
        return rows[:limit]

    def get_income_statements(self, ticker: str, limit: int = 8) -> list[dict[str, Any]]:
        stock = self.yf.Ticker(ticker)
        frame = getattr(stock, "quarterly_income_stmt", None)
        if frame is None or frame.empty:
            return []

        revenue_row_name = None
        for candidate in ("Total Revenue", "Operating Revenue", "Revenue"):
            if candidate in frame.index:
                revenue_row_name = candidate
                break
        if revenue_row_name is None:
            return []

        rows: list[dict[str, Any]] = []
        for column in list(frame.columns)[:limit]:
            revenue_value = _safe_float(frame.at[revenue_row_name, column])
            if revenue_value is None:
                continue
            date_value = column.date() if hasattr(column, "date") else None
            if date_value is None:
                continue
            rows.append(
                {
                    "date": date_value.isoformat(),
                    "revenue": revenue_value,
                }
            )
        return rows

    def get_latest_institutional_ownership_pct(self, ticker: str) -> float | None:
        stock = self.yf.Ticker(ticker)
        info = None
        try:
            info = stock.get_info()
        except Exception:
            try:
                info = stock.info
            except Exception:
                info = None
        if not isinstance(info, dict):
            return None
        pct = _safe_float(
            info.get("heldPercentInstitutions")
            or info.get("institutionsPercentHeld")
        )
        return _normalize_pct(pct)


class AInvestGrowthClient:
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

    def get_earnings(self, ticker: str, limit: int = 12) -> list[dict[str, Any]]:
        payload = self._get(
            "/securities/stock/financials/earnings",
            {"ticker": ticker, "size": limit},
        )
        data = payload.get("data", [])
        if not isinstance(data, list):
            return []
        rows: list[dict[str, Any]] = []
        for row in data:
            if not isinstance(row, dict):
                continue
            rows.append(
                {
                    "date": row.get("release_date"),
                    "epsEstimated": _safe_float(row.get("eps_forecast")),
                    "eps": _safe_float(row.get("eps_actual")),
                    "revenue": _safe_float(row.get("revenue_actual")),
                    "revenueEstimated": _safe_float(row.get("revenue_forecast")),
                }
            )
        return rows


class OpenBBGrowthClient:
    def __init__(self, timeout_seconds: int = 20, providers: tuple[str | None, ...] = ("nasdaq", None)) -> None:
        try:
            from openbb import obb
        except ImportError as exc:
            raise RuntimeError("openbb is not installed.") from exc
        self.obb = obb
        self.timeout_seconds = max(1, int(timeout_seconds))
        self.providers = providers

    def get_earnings(self, ticker: str, limit: int = 12) -> list[dict[str, Any]]:
        start_date = (dt.date.today() - dt.timedelta(days=365 * 5)).isoformat()
        end_date = (dt.date.today() + dt.timedelta(days=365)).isoformat()
        last_error: Exception | None = None

        for provider in self.providers:
            query: dict[str, Any] = {
                "symbol": ticker,
                "start_date": start_date,
                "end_date": end_date,
            }
            if provider is not None:
                query["provider"] = provider
            try:
                response = self.obb.equity.calendar.earnings(**query)
            except Exception as exc:
                last_error = exc
                continue

            rows = self._normalize_response(ticker, response)
            if rows:
                rows.sort(key=lambda item: str(item.get("date", "")), reverse=True)
                return rows[:limit]

        if last_error is not None:
            raise last_error
        return []

    def _normalize_response(self, ticker: str, response: Any) -> list[dict[str, Any]]:
        if hasattr(response, "results"):
            results = response.results
        elif isinstance(response, dict):
            results = response.get("results", [])
        else:
            results = []

        rows: list[dict[str, Any]] = []
        for item in results or []:
            if hasattr(item, "model_dump"):
                data = item.model_dump()
            elif isinstance(item, dict):
                data = item
            else:
                continue

            symbol = str(data.get("symbol") or "").upper().strip()
            if symbol and symbol != ticker.upper():
                continue

            report_date = data.get("report_date") or data.get("date")
            session = data.get("reporting_time") or data.get("time")
            rows.append(
                {
                    "date": str(report_date) if report_date else None,
                    "time": session,
                    "epsEstimated": _safe_float(data.get("eps_consensus")),
                    "eps": _safe_float(data.get("eps_actual")),
                    "revenue": _safe_float(data.get("revenue_actual")),
                    "revenueEstimated": _safe_float(data.get("revenue_consensus")),
                }
            )
        return rows


class AKShareGrowthClient:
    def __init__(self) -> None:
        try:
            import akshare as ak
        except ImportError as exc:
            raise RuntimeError("akshare is not installed.") from exc
        self.ak = ak

    def get_income_statements(self, ticker: str, limit: int = 8) -> list[dict[str, Any]]:
        candidates: list[dict[str, Any]] = []
        for loader in (
            self._load_analysis_indicator_rows,
            self._load_report_rows,
        ):
            try:
                rows = loader(ticker, limit)
            except Exception:
                rows = []
            if rows:
                return rows[:limit]
        return candidates

    def _load_analysis_indicator_rows(self, ticker: str, limit: int) -> list[dict[str, Any]]:
        frame = self.ak.stock_financial_us_analysis_indicator_em(symbol=ticker)
        return self._normalize_income_frame(frame, limit)

    def _load_report_rows(self, ticker: str, limit: int) -> list[dict[str, Any]]:
        for report_name in ("利润表", "综合损益表"):
            try:
                frame = self.ak.stock_financial_us_report_em(symbol=ticker, indicator=report_name)
            except TypeError:
                frame = self.ak.stock_financial_us_report_em(symbol=ticker)
            rows = self._normalize_income_frame(frame, limit)
            if rows:
                return rows
        return []

    def _normalize_income_frame(self, frame: Any, limit: int) -> list[dict[str, Any]]:
        if frame is None or getattr(frame, "empty", True):
            return []
        date_column = None
        revenue_column = None
        for column in frame.columns:
            text = str(column)
            normalized = text.lower()
            if date_column is None and any(token in normalized for token in ("date", "report", "period", "日期")):
                date_column = column
            if revenue_column is None and any(
                token in normalized
                for token in (
                    "revenue",
                    "营业总收入",
                    "营业收入",
                    "总营收",
                    "total revenue",
                )
            ):
                revenue_column = column
        if date_column is None or revenue_column is None:
            return []
        rows: list[dict[str, Any]] = []
        for _, row in frame.head(limit).iterrows():
            date_value = _parse_date(row.get(date_column))
            revenue_value = _safe_float(row.get(revenue_column))
            if date_value is None or revenue_value is None:
                continue
            rows.append({"date": date_value.isoformat(), "revenue": revenue_value})
        return rows


def _sma(closes: list[float], length: int) -> float | None:
    if len(closes) < length:
        return None
    series = pd.Series(closes, dtype="float64")
    value = series.rolling(length).mean().iloc[-1]
    if pd.isna(value):
        return None
    return float(value)


def _build_price_context(cookstock: Any, config: AppConfig, ticker: str) -> tuple[float, float, float, float, list[dict[str, Any]]]:
    financials = cookstock.cookFinancials(
        ticker,
        benchmarkTicker=config.benchmark_ticker,
    )
    price_rows = financials._get_clean_price_data()
    closes = [float(item.get("close") or 0.0) for item in price_rows if item.get("close") is not None]
    if len(closes) < config.earnings_growth_ma_long:
        raise RuntimeError("not enough price history for MA stack")
    ma_short = _sma(closes, config.earnings_growth_ma_short)
    ma_medium = _sma(closes, config.earnings_growth_ma_medium)
    ma_long = _sma(closes, config.earnings_growth_ma_long)
    if ma_short is None or ma_medium is None or ma_long is None:
        raise RuntimeError("unable to compute MA stack")
    return closes[-1], ma_short, ma_medium, ma_long, price_rows


def _historical_earnings_rows(earnings_rows: list[dict[str, Any]], as_of_date: dt.date) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for row in earnings_rows:
        date_value = _parse_date(row.get("date"))
        if date_value is None or date_value >= as_of_date:
            continue
        rows.append(row)
    rows.sort(key=lambda item: str(item.get("date", "")), reverse=True)
    return rows


def _next_upcoming_earnings_row(earnings_rows: list[dict[str, Any]], as_of_date: dt.date) -> dict[str, Any] | None:
    upcoming: list[dict[str, Any]] = []
    for row in earnings_rows:
        date_value = _parse_date(row.get("date"))
        if date_value is None or date_value < as_of_date:
            continue
        upcoming.append(row)
    upcoming.sort(key=lambda item: str(item.get("date", "")))
    return upcoming[0] if upcoming else None


def _compute_post_earnings_moves(
    price_rows: list[dict[str, Any]],
    earnings_rows: list[dict[str, Any]],
    lookback_quarters: int,
) -> list[float]:
    date_to_index: dict[dt.date, int] = {}
    parsed_dates: list[dt.date] = []
    for idx, row in enumerate(price_rows):
        date_value = _parse_date(row.get("formatted_date") or row.get("date"))
        parsed_dates.append(date_value or dt.date.min)
        if date_value is not None:
            date_to_index[date_value] = idx

    moves: list[float] = []
    for row in earnings_rows[:lookback_quarters]:
        event_date = _parse_date(row.get("date"))
        if event_date is None:
            continue
        event_idx = date_to_index.get(event_date)
        if event_idx is None:
            continue
        if event_idx + 1 >= len(price_rows):
            continue
        event_close = _safe_float(price_rows[event_idx].get("close"))
        next_high = _safe_float(price_rows[event_idx + 1].get("high"))
        next_low = _safe_float(price_rows[event_idx + 1].get("low"))
        if not event_close or next_high is None or next_low is None:
            continue
        move = max(abs(next_high / event_close - 1.0), abs(next_low / event_close - 1.0)) * 100.0
        moves.append(move)
    return moves


def _latest_revenue_context(income_rows: list[dict[str, Any]]) -> tuple[float | None, float | None]:
    if len(income_rows) < 5:
        return None, None
    latest_revenue = _safe_float(income_rows[0].get("revenue"))
    year_ago_revenue = _safe_float(income_rows[4].get("revenue"))
    if latest_revenue is None or year_ago_revenue in (None, 0):
        return latest_revenue, None
    revenue_yoy_pct = ((latest_revenue - year_ago_revenue) / abs(year_ago_revenue)) * 100.0
    return latest_revenue, revenue_yoy_pct


def _eps_series_from_earnings(earnings_rows: list[dict[str, Any]], count: int) -> list[float]:
    values: list[float] = []
    for row in earnings_rows:
        eps_value = _safe_float(
            row.get("eps")
            or row.get("epsActual")
            or row.get("actualEps")
            or row.get("actualEPS")
        )
        if eps_value is None:
            continue
        values.append(eps_value)
        if len(values) >= count:
            break
    return values


def _eps_is_improving(eps_series: list[float], min_quarters: int) -> bool:
    if len(eps_series) < min_quarters:
        return False
    latest = eps_series[:min_quarters]
    for idx in range(len(latest) - 1):
        if latest[idx] <= latest[idx + 1]:
            return False
    return True


def _to_hit(
    config: AppConfig,
    event: PreEarningsEvent,
    price_context: tuple[float, float, float, float, list[dict[str, Any]]],
    latest_revenue: float,
    revenue_yoy_pct: float,
    latest_eps_actual: float,
    eps_series: list[float],
    institutional_ownership_pct: float,
    post_earnings_moves: list[float],
    next_earnings_session: str | None,
) -> EarningsGrowthHit:
    current_price, ma_short, ma_medium, ma_long, _ = price_context
    large_moves = [value for value in post_earnings_moves if value >= config.earnings_growth_min_move_pct]
    reasons = [
        f"{len(large_moves)} / {len(post_earnings_moves)} earnings reactions >= {config.earnings_growth_min_move_pct:.1f}%",
        f"Revenue YoY {revenue_yoy_pct:.1f}%",
        f"Latest EPS {latest_eps_actual:.2f} with improving trend",
        f"Institutional ownership {institutional_ownership_pct:.1f}%",
        f"MA stack {config.earnings_growth_ma_short}>{config.earnings_growth_ma_medium}>{config.earnings_growth_ma_long}",
    ]
    return EarningsGrowthHit(
        ticker=event.ticker,
        earnings_date=event.earnings_date,
        earnings_summary=event.summary,
        sector=event.sector,
        exchange=event.exchange,
        benchmark_ticker=config.benchmark_ticker,
        current_price=current_price,
        ma_short=ma_short,
        ma_medium=ma_medium,
        ma_long=ma_long,
        ma_short_length=config.earnings_growth_ma_short,
        ma_medium_length=config.earnings_growth_ma_medium,
        ma_long_length=config.earnings_growth_ma_long,
        ma_stack_bullish=current_price > ma_short > ma_medium > ma_long,
        latest_quarter_revenue=latest_revenue,
        revenue_yoy_pct=revenue_yoy_pct,
        latest_eps_actual=latest_eps_actual,
        eps_improving_quarters=config.earnings_growth_eps_improving_quarters,
        eps_series=eps_series,
        institutional_ownership_pct=institutional_ownership_pct,
        historical_earnings_moves_pct=post_earnings_moves,
        large_move_occurrences=len(large_moves),
        median_post_earnings_move_pct=float(median(post_earnings_moves)) if post_earnings_moves else 0.0,
        next_earnings_session=next_earnings_session,
        reasons=reasons,
    )


def run_earnings_growth_screen(
    config: AppConfig,
    events: list[PreEarningsEvent],
    *,
    as_of_date: dt.date | None = None,
) -> EarningsGrowthScreenResult:
    as_of_date = as_of_date or dt.date.today()
    ainvest_api_key = (os.getenv("AINVEST_API_KEY") or "").strip()
    earnings_sources: list[tuple[str, Any]] = []
    earnings_providers_used: list[str] = []
    try:
        earnings_sources.append(("openbb", OpenBBGrowthClient(timeout_seconds=config.request_timeout_seconds)))
    except Exception:
        print("warning: OpenBB is not available; falling back to downstream earnings providers.")

    if ainvest_api_key:
        earnings_sources.append(
            ("ainvest", AInvestGrowthClient(ainvest_api_key, timeout_seconds=config.request_timeout_seconds))
        )
    else:
        print("warning: AINVEST_API_KEY is not set; skipping AInvest earnings provider.")

    yfinance_client = YFinanceGrowthClient()
    earnings_sources.append(("yfinance", yfinance_client))

    financial_client = yfinance_client
    financials_providers_used = ["yfinance"]
    akshare_client: Any | None = None
    try:
        akshare_client = AKShareGrowthClient()
    except Exception:
        akshare_client = None

    cookstock = load_configured_cookstock(config)
    hits: list[EarningsGrowthHit] = []
    failures: list[dict[str, str]] = []

    for position, event in enumerate(events, start=1):
        print(f"[{position}/{len(events)}] screening {event.ticker}")
        try:
            earnings_rows: list[dict[str, Any]] = []
            last_earnings_error: Exception | None = None
            for provider_name, provider_client in earnings_sources:
                try:
                    earnings_rows = provider_client.get_earnings(
                        event.ticker,
                        limit=max(12, config.earnings_growth_move_lookback_quarters + 4),
                    )
                except Exception as exc:
                    last_earnings_error = exc
                    continue
                if provider_name not in earnings_providers_used:
                    earnings_providers_used.append(provider_name)
                if earnings_rows:
                    break
            if not earnings_rows:
                if last_earnings_error is not None:
                    raise last_earnings_error
                continue

            historical_earnings = _historical_earnings_rows(earnings_rows, as_of_date)
            if len(historical_earnings) < config.earnings_growth_move_lookback_quarters:
                continue

            income_rows = financial_client.get_income_statements(event.ticker, limit=8)
            if not income_rows and akshare_client is not None:
                try:
                    income_rows = akshare_client.get_income_statements(event.ticker, limit=8)
                    if income_rows:
                        if "akshare" not in financials_providers_used:
                            financials_providers_used.append("akshare")
                except Exception:
                    income_rows = []
            latest_revenue, revenue_yoy_pct = _latest_revenue_context(income_rows)
            if latest_revenue is None or revenue_yoy_pct is None:
                continue
            if latest_revenue < config.earnings_growth_min_quarter_revenue:
                continue
            if revenue_yoy_pct < config.earnings_growth_min_revenue_yoy_pct:
                continue

            eps_series = _eps_series_from_earnings(historical_earnings, config.earnings_growth_eps_improving_quarters)
            if len(eps_series) < config.earnings_growth_eps_improving_quarters:
                continue
            latest_eps_actual = eps_series[0]
            if latest_eps_actual >= 0:
                continue
            if not _eps_is_improving(eps_series, config.earnings_growth_eps_improving_quarters):
                continue

            institutional_ownership_pct = financial_client.get_latest_institutional_ownership_pct(event.ticker)
            if institutional_ownership_pct is None:
                continue
            if institutional_ownership_pct < config.earnings_growth_min_institutional_ownership_pct:
                continue

            price_context = _build_price_context(cookstock, config, event.ticker)
            current_price, ma_short, ma_medium, ma_long, price_rows = price_context
            if not (current_price > ma_short > ma_medium > ma_long):
                continue

            post_earnings_moves = _compute_post_earnings_moves(
                price_rows,
                historical_earnings,
                config.earnings_growth_move_lookback_quarters,
            )
            large_move_occurrences = len(
                [value for value in post_earnings_moves if value >= config.earnings_growth_min_move_pct]
            )
            if large_move_occurrences < config.earnings_growth_min_move_occurrences:
                continue

            next_row = _next_upcoming_earnings_row(earnings_rows, as_of_date)
            next_session = _parse_session(next_row.get("time")) if next_row else None
            hits.append(
                _to_hit(
                    config,
                    event,
                    price_context,
                    latest_revenue,
                    revenue_yoy_pct,
                    latest_eps_actual,
                    eps_series,
                    institutional_ownership_pct,
                    post_earnings_moves,
                    next_session,
                )
            )
        except Exception as exc:
            failures.append({"ticker": event.ticker, "error": str(exc)})
            earnings_label = "+".join(earnings_providers_used) if earnings_providers_used else "configured-chain"
            financials_label = "+".join(financials_providers_used) if financials_providers_used else "configured-chain"
            print(
                f"earnings growth screening failed for {event.ticker} "
                f"(earnings={earnings_label}, financials={financials_label}): {exc}"
            )

    hits.sort(
        key=lambda item: (
            item.revenue_yoy_pct,
            item.median_post_earnings_move_pct,
            item.institutional_ownership_pct,
        ),
        reverse=True,
    )
    return EarningsGrowthScreenResult(
        run_date=as_of_date.isoformat(),
        benchmark_ticker=config.benchmark_ticker,
        earnings_provider="+".join(earnings_providers_used) if earnings_providers_used else "none",
        financials_provider="+".join(financials_providers_used) if financials_providers_used else "none",
        total_tickers=len(events),
        passed_tickers=len(hits),
        failed_tickers=failures,
        hits=hits,
    )
