from __future__ import annotations

import datetime as dt
import json
from pathlib import Path
from typing import Any

from src.market_data_access import resolve_database_url


class TigerPositionsRepository:
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
        try:
            self.ensure_schema()
        except Exception:
            return None
        try:
            import psycopg
        except ImportError:
            return None
        try:
            return psycopg.connect(self.database_url)
        except Exception:
            return None

    def _rows_to_dicts(self, cursor: Any, rows: list[tuple[Any, ...]]) -> list[dict[str, Any]]:
        columns = [item.name if hasattr(item, "name") else item[0] for item in cursor.description or []]
        return [dict(zip(columns, row)) for row in rows]

    def get_user_settings(self, user_id: int) -> dict[str, Any] | None:
        connection = self._connect()
        if connection is None:
            return None
        sql = """
            SELECT
              user_id,
              display_name,
              tiger_id,
              account,
              private_key_env_var,
              is_enabled,
              last_synced_at,
              last_sync_error,
              created_at,
              updated_at
            FROM tiger_account_settings
            WHERE user_id = %s
        """
        with connection:
            with connection.cursor() as cursor:
                cursor.execute(sql, (user_id,))
                rows = self._rows_to_dicts(cursor, cursor.fetchall())
        return rows[0] if rows else None

    def list_enabled_settings(self) -> list[dict[str, Any]]:
        connection = self._connect()
        if connection is None:
            return []
        sql = """
            SELECT
              user_id,
              display_name,
              tiger_id,
              account,
              private_key_env_var,
              is_enabled,
              last_synced_at,
              last_sync_error,
              created_at,
              updated_at
            FROM tiger_account_settings
            WHERE is_enabled = TRUE
            ORDER BY updated_at DESC, user_id ASC
        """
        with connection:
            with connection.cursor() as cursor:
                cursor.execute(sql)
                return self._rows_to_dicts(cursor, cursor.fetchall())

    def upsert_user_settings(
        self,
        *,
        user_id: int,
        display_name: str,
        tiger_id: str,
        account: str,
        private_key_env_var: str,
        is_enabled: bool,
    ) -> dict[str, Any] | None:
        connection = self._connect()
        if connection is None:
            return None
        sql = """
            INSERT INTO tiger_account_settings (
              user_id, display_name, tiger_id, account, private_key_env_var, is_enabled, updated_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, NOW())
            ON CONFLICT (user_id)
            DO UPDATE SET
              display_name = EXCLUDED.display_name,
              tiger_id = EXCLUDED.tiger_id,
              account = EXCLUDED.account,
              private_key_env_var = EXCLUDED.private_key_env_var,
              is_enabled = EXCLUDED.is_enabled,
              updated_at = NOW()
            RETURNING
              user_id,
              display_name,
              tiger_id,
              account,
              private_key_env_var,
              is_enabled,
              last_synced_at,
              last_sync_error,
              created_at,
              updated_at
        """
        with connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    sql,
                    (user_id, display_name, tiger_id, account, private_key_env_var, is_enabled),
                )
                rows = self._rows_to_dicts(cursor, cursor.fetchall())
            connection.commit()
        return rows[0] if rows else None

    def record_sync_status(
        self,
        *,
        user_id: int,
        synced_at: dt.datetime | None,
        error_text: str = "",
    ) -> None:
        connection = self._connect()
        if connection is None:
            return
        sql = """
            UPDATE tiger_account_settings
            SET last_synced_at = %s,
                last_sync_error = %s,
                updated_at = NOW()
            WHERE user_id = %s
        """
        with connection:
            with connection.cursor() as cursor:
                cursor.execute(sql, (synced_at, error_text or None, user_id))
            connection.commit()

    def ensure_ticker_metadata_stub(self, ticker: str) -> None:
        connection = self._connect()
        if connection is None:
            return
        sql = """
            INSERT INTO ticker_metadata (ticker, source)
            VALUES (%s, %s)
            ON CONFLICT (ticker) DO NOTHING
        """
        with connection:
            with connection.cursor() as cursor:
                cursor.execute(sql, (ticker.upper(), "tiger_positions"))
            connection.commit()

    def insert_position_batch(
        self,
        *,
        user_id: int,
        tiger_account: str,
        captured_at: dt.datetime,
        as_of_date: dt.date | None,
        positions: list[dict[str, Any]],
    ) -> int:
        if not positions:
            return 0
        connection = self._connect()
        if connection is None:
            return 0
        sql = """
            INSERT INTO tiger_position_snapshots (
              user_id,
              tiger_account,
              ticker,
              quantity,
              average_cost,
              market_price,
              market_value,
              unrealized_pl,
              currency,
              as_of_date,
              captured_at,
              raw_json
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb)
        """
        values: list[tuple[Any, ...]] = []
        for item in positions:
            ticker = str(item.get("ticker") or "").strip().upper()
            if not ticker:
                continue
            self.ensure_ticker_metadata_stub(ticker)
            values.append(
                (
                    user_id,
                    tiger_account,
                    ticker,
                    item.get("quantity"),
                    item.get("average_cost"),
                    item.get("market_price"),
                    item.get("market_value"),
                    item.get("unrealized_pl"),
                    item.get("currency"),
                    as_of_date,
                    captured_at,
                    json.dumps(item.get("raw_json") or {}),
                )
            )
        if not values:
            return 0
        with connection:
            with connection.cursor() as cursor:
                cursor.executemany(sql, values)
            connection.commit()
        return len(values)

    def list_latest_positions(self, user_id: int) -> list[dict[str, Any]]:
        connection = self._connect()
        if connection is None:
            return []
        sql = """
            WITH latest_batch AS (
              SELECT MAX(captured_at) AS captured_at
              FROM tiger_position_snapshots
              WHERE user_id = %s
            )
            SELECT
              snapshots.id,
              snapshots.user_id,
              snapshots.tiger_account,
              snapshots.ticker,
              snapshots.quantity,
              snapshots.average_cost,
              snapshots.market_price,
              snapshots.market_value,
              snapshots.unrealized_pl,
              snapshots.currency,
              snapshots.as_of_date,
              snapshots.captured_at,
              snapshots.raw_json
            FROM tiger_position_snapshots snapshots
            JOIN latest_batch ON latest_batch.captured_at = snapshots.captured_at
            WHERE snapshots.user_id = %s
            ORDER BY snapshots.market_value DESC NULLS LAST, snapshots.ticker ASC
        """
        with connection:
            with connection.cursor() as cursor:
                cursor.execute(sql, (user_id, user_id))
                return self._rows_to_dicts(cursor, cursor.fetchall())
