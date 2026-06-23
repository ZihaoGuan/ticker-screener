from __future__ import annotations

import datetime as dt
from pathlib import Path
from typing import Any

from src.market_data_access import resolve_database_url


class MyPicksRepository:
    def __init__(self, *, database_url: str = "") -> None:
        self.database_url = resolve_database_url(database_url)
        self._schema_ready = False

    def is_configured(self) -> bool:
        return bool(self.database_url)

    def ensure_schema(self) -> None:
        if self._schema_ready or not self.database_url:
            return
        try:
            import psycopg
        except ImportError:
            return
        schema_path = Path(__file__).resolve().parents[3] / "sql" / "postgres_app_schema.sql"
        with psycopg.connect(self.database_url) as connection:
            with connection.cursor() as cursor:
                cursor.execute(schema_path.read_text(encoding="utf-8"))
            connection.commit()
        self._schema_ready = True

    def _connect(self):
        if not self.database_url:
            return None
        self.ensure_schema()
        try:
            import psycopg
        except ImportError:
            return None
        return psycopg.connect(self.database_url)

    def _rows_to_dicts(self, cursor: Any, rows: list[tuple[Any, ...]]) -> list[dict[str, Any]]:
        columns = [item.name if hasattr(item, "name") else item[0] for item in cursor.description or []]
        return [dict(zip(columns, row)) for row in rows]

    def list_picks(self) -> list[dict[str, Any]]:
        connection = self._connect()
        if connection is None:
            return []
        sql = """
            SELECT id, ticker, notes, created_by_user_id, created_at
            FROM my_picks
            ORDER BY created_at DESC, id DESC
        """
        with connection:
            with connection.cursor() as cursor:
                cursor.execute(sql)
                return self._rows_to_dicts(cursor, cursor.fetchall())

    def create_pick(
        self,
        *,
        ticker: str,
        notes: str = "",
        created_by_user_id: int | None = None,
    ) -> dict[str, Any] | None:
        connection = self._connect()
        if connection is None:
            return None
        sql = """
            INSERT INTO my_picks (ticker, notes, created_by_user_id)
            VALUES (%s, %s, %s)
            RETURNING id, ticker, notes, created_by_user_id, created_at
        """
        with connection:
            with connection.cursor() as cursor:
                cursor.execute(sql, (ticker, notes, created_by_user_id))
                rows = self._rows_to_dicts(cursor, cursor.fetchall())
            connection.commit()
        return rows[0] if rows else None

    def delete_pick(self, pick_id: int) -> bool:
        connection = self._connect()
        if connection is None:
            return False
        sql = "DELETE FROM my_picks WHERE id = %s"
        with connection:
            with connection.cursor() as cursor:
                cursor.execute(sql, (pick_id,))
                deleted = cursor.rowcount > 0
            connection.commit()
        return deleted

    def list_recent_signal_summary(self, tickers: list[str], *, lookback_days: int = 45) -> dict[str, dict[str, Any]]:
        connection = self._connect()
        if connection is None:
            return {}
        normalized = sorted({str(item).strip().upper() for item in tickers if str(item).strip()})
        if not normalized:
            return {}
        sql = """
            SELECT
              hits.ticker,
              COUNT(*)::int AS signal_count,
              MAX(hits.signal_date) AS latest_signal_date,
              ARRAY_AGG(
                DISTINCT CONCAT(hits.strategy_id, '|', hits.signal_date::text)
                ORDER BY CONCAT(hits.strategy_id, '|', hits.signal_date::text) DESC
              ) AS signal_keys
            FROM screen_run_hits hits
            JOIN screen_runs runs
              ON runs.id = hits.screen_run_id
            WHERE hits.ticker = ANY(%s)
              AND hits.passed = TRUE
              AND runs.deleted_at IS NULL
              AND hits.signal_date >= CURRENT_DATE - (%s * INTERVAL '1 day')
            GROUP BY hits.ticker
        """
        with connection:
            with connection.cursor() as cursor:
                cursor.execute(sql, (normalized, max(1, int(lookback_days))))
                rows = cursor.fetchall()
        result: dict[str, dict[str, Any]] = {}
        for ticker, signal_count, latest_signal_date, signal_keys in rows:
            recent_signals: list[dict[str, Any]] = []
            for raw in list(signal_keys or [])[:6]:
                text = str(raw or "")
                strategy_id, _, signal_date = text.partition("|")
                if not strategy_id:
                    continue
                recent_signals.append({"strategy_id": strategy_id, "signal_date": signal_date or None})
            result[str(ticker).upper()] = {
                "signal_count": int(signal_count or 0),
                "latest_signal_date": latest_signal_date.isoformat() if isinstance(latest_signal_date, dt.date) else None,
                "recent_signals": recent_signals,
            }
        return result
