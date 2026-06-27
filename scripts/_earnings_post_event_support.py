from __future__ import annotations

import datetime as dt
from dataclasses import dataclass
from pathlib import Path

from src.config import load_app_config
from src.cookstock_bridge import load_configured_cookstock
from src.ticker_filters import filter_pre_earnings_events, load_excluded_tickers
from src.universe import load_universe
from src.pre_earnings_screen import PreEarningsEvent


@dataclass(frozen=True)
class WeeklyEarningsEvent:
    ticker: str
    event_date: dt.date
    summary: str | None
    session: str | None
    sector: str | None
    exchange: str | None

    @property
    def eligible_on(self) -> dt.date:
        return next_trading_day(self.event_date)

    def to_dict(self) -> dict[str, object]:
        return {
            "ticker": self.ticker,
            "event_date": self.event_date.isoformat(),
            "summary": self.summary,
            "session": self.session,
            "sector": self.sector,
            "exchange": self.exchange,
            "eligible_on": self.eligible_on.isoformat(),
        }


def resolve_selected_week(
    reference_date: dt.date | None,
    *,
    week_offset: int = 0,
) -> tuple[dt.date, dt.date, dt.date]:
    anchor_date = reference_date or dt.date.today()
    normalized_week_offset = max(0, int(week_offset))
    week_start = (
        anchor_date - dt.timedelta(days=anchor_date.weekday() + 1)
        if anchor_date.weekday() != 6
        else anchor_date
    )
    selected_week_start = week_start + dt.timedelta(days=normalized_week_offset * 7)
    selected_week_end = selected_week_start + dt.timedelta(days=6)
    return anchor_date, selected_week_start, selected_week_end


def next_trading_day(date_value: dt.date) -> dt.date:
    candidate = date_value + dt.timedelta(days=1)
    while candidate.weekday() >= 5:
        candidate += dt.timedelta(days=1)
    return candidate


def load_weekly_earnings_events(
    *,
    config_path: str,
    reference_date: dt.date | None,
    week_offset: int = 0,
    ignore_exclusions: bool = False,
    limit: int | None = None,
) -> list[WeeklyEarningsEvent]:
    config = load_app_config(config_path)
    excluded = load_excluded_tickers(config)
    universe = load_universe(config)
    sector_map = {item.symbol.upper(): (item.sector, item.exchange) for item in universe if item.symbol}
    _, week_start, week_end = resolve_selected_week(reference_date, week_offset=week_offset)
    cookstock = load_configured_cookstock(config)
    raw_events = cookstock.fetch_earnings_calendar_watchlist(week_start, week_end)

    pre_earnings_events: list[PreEarningsEvent] = []
    seen: set[str] = set()
    for item in raw_events:
        ticker = str(item.get("ticker") or "").strip().upper()
        event_date = item.get("event_date")
        if not ticker or not isinstance(event_date, dt.date):
            continue
        if event_date < week_start or event_date > week_end:
            continue
        if ticker in seen:
            continue
        seen.add(ticker)
        sector, exchange = sector_map.get(ticker, (None, None))
        pre_earnings_events.append(
            PreEarningsEvent(
                ticker=ticker,
                earnings_date=event_date.isoformat(),
                summary=str(item.get("summary")) if item.get("summary") else None,
                sector=sector,
                exchange=exchange,
            )
        )

    if not ignore_exclusions:
        pre_earnings_events = filter_pre_earnings_events(pre_earnings_events, excluded)
    if limit is not None:
        pre_earnings_events = pre_earnings_events[:limit]

    events: list[WeeklyEarningsEvent] = []
    for event in pre_earnings_events:
        event_date = dt.date.fromisoformat(event.earnings_date) if event.earnings_date else None
        if event_date is None:
            continue
        events.append(
            WeeklyEarningsEvent(
                ticker=event.ticker.upper(),
                event_date=event_date,
                summary=event.summary,
                session=_session_from_summary(event.summary),
                sector=event.sector,
                exchange=event.exchange,
            )
        )
    return events


def _session_from_summary(summary: str | None) -> str | None:
    text = str(summary or "").strip().lower()
    if not text:
        return None
    if "before market" in text or "pre-market" in text or "bmo" in text:
        return "before_market"
    if "after market" in text or "after-hours" in text or "amc" in text:
        return "after_market"
    if "during market" in text or "during-market" in text:
        return "during_market"
    return None
