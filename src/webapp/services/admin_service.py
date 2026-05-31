from __future__ import annotations

import datetime as dt
from typing import Any

from src.config import load_app_config, today_label
from src.market_data_access import resolve_database_url
from src.universe import load_universe


class AdminService:
    def __init__(self, database_url: str = "") -> None:
        self.database_url = resolve_database_url(database_url)

    def get_context(self, *, coverage_start: str = "2020-01-01") -> dict[str, Any]:
        excluded = self._load_exclusions()
        status = self._history_status(coverage_start=coverage_start)
        return {
            "excluded_tickers": excluded[:500],
            "excluded_count": len(excluded),
            "database_status": status,
        }

    def _load_exclusions(self) -> list[str]:
        from src.ticker_filters import load_excluded_tickers

        config = load_app_config()
        return sorted(load_excluded_tickers(config))

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
        status["sample_missing_tickers"] = missing[:20]
        status["sample_partial_tickers"] = partial[:20]
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
