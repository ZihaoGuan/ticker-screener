from __future__ import annotations

import datetime as dt
import json
from pathlib import Path
from typing import Any

from src.market_data_access import resolve_database_url


class PortfolioRepository:
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

    def list_portfolios(self) -> list[dict[str, Any]]:
        connection = self._connect()
        if connection is None:
            return []
        sql = """
            SELECT id, name, created_by_user_id, created_at, updated_at
            FROM portfolios
            ORDER BY name ASC
        """
        with connection:
            with connection.cursor() as cursor:
                cursor.execute(sql)
                return self._rows_to_dicts(cursor, cursor.fetchall())

    def get_or_create_portfolio(self, *, name: str, created_by_user_id: int | None = None) -> dict[str, Any] | None:
        connection = self._connect()
        if connection is None:
            return None
        sql = """
            INSERT INTO portfolios (name, created_by_user_id, updated_at)
            VALUES (%s, %s, NOW())
            ON CONFLICT (name)
            DO UPDATE SET updated_at = NOW()
            RETURNING id, name, created_by_user_id, created_at, updated_at
        """
        with connection:
            with connection.cursor() as cursor:
                cursor.execute(sql, (name, created_by_user_id))
                rows = self._rows_to_dicts(cursor, cursor.fetchall())
            connection.commit()
        return rows[0] if rows else None

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
                cursor.execute(sql, (ticker.upper(), "portfolio_manual"))
            connection.commit()

    def list_positions(self) -> list[dict[str, Any]]:
        connection = self._connect()
        if connection is None:
            return []
        sql = """
            SELECT
              positions.id,
              positions.portfolio_id,
              portfolios.name AS portfolio_name,
              positions.ticker,
              positions.shares,
              positions.entry_price,
              positions.opened_at,
              positions.notes,
              positions.created_by_user_id,
              positions.updated_by_user_id,
              positions.created_at,
              positions.updated_at,
              advice.as_of_date,
              advice.latest_trade_date,
              advice.market_data_status,
              advice.close_price,
              advice.signal_status,
              advice.stop_loss_price,
              advice.tp1_price,
              advice.tp2_price,
              advice.tp1_sell_fraction,
              advice.tp2_sell_fraction,
              advice.average_up_price,
              advice.average_up_share_fraction,
              advice.blended_entry_after_average_up,
              advice.net_cost_after_tp1,
              advice.remaining_cost_basis_after_tp1,
              advice.explanation,
              advice.data_source,
              advice.signal_context_json,
              advice.refreshed_at
            FROM portfolio_positions positions
            JOIN portfolios ON portfolios.id = positions.portfolio_id
            LEFT JOIN portfolio_advice_snapshots advice ON advice.position_id = positions.id
            ORDER BY portfolios.name ASC, positions.ticker ASC, positions.opened_at DESC, positions.id DESC
        """
        with connection:
            with connection.cursor() as cursor:
                cursor.execute(sql)
                return self._rows_to_dicts(cursor, cursor.fetchall())

    def get_position(self, position_id: int) -> dict[str, Any] | None:
        connection = self._connect()
        if connection is None:
            return None
        sql = """
            SELECT id, portfolio_id, ticker, shares, entry_price, opened_at, notes,
                   created_by_user_id, updated_by_user_id, created_at, updated_at
            FROM portfolio_positions
            WHERE id = %s
        """
        with connection:
            with connection.cursor() as cursor:
                cursor.execute(sql, (position_id,))
                rows = self._rows_to_dicts(cursor, cursor.fetchall())
        return rows[0] if rows else None

    def create_position(
        self,
        *,
        portfolio_id: int,
        ticker: str,
        shares: float,
        entry_price: float,
        opened_at: dt.date,
        notes: str = "",
        created_by_user_id: int | None = None,
    ) -> dict[str, Any] | None:
        connection = self._connect()
        if connection is None:
            return None
        sql = """
            INSERT INTO portfolio_positions (
              portfolio_id, ticker, shares, entry_price, opened_at, notes, created_by_user_id, updated_by_user_id
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id, portfolio_id, ticker, shares, entry_price, opened_at, notes,
                      created_by_user_id, updated_by_user_id, created_at, updated_at
        """
        with connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    sql,
                    (portfolio_id, ticker, shares, entry_price, opened_at, notes, created_by_user_id, created_by_user_id),
                )
                rows = self._rows_to_dicts(cursor, cursor.fetchall())
            connection.commit()
        return rows[0] if rows else None

    def update_position(
        self,
        position_id: int,
        *,
        portfolio_id: int,
        ticker: str,
        shares: float,
        entry_price: float,
        opened_at: dt.date,
        notes: str = "",
        updated_by_user_id: int | None = None,
    ) -> dict[str, Any] | None:
        connection = self._connect()
        if connection is None:
            return None
        sql = """
            UPDATE portfolio_positions
            SET portfolio_id = %s,
                ticker = %s,
                shares = %s,
                entry_price = %s,
                opened_at = %s,
                notes = %s,
                updated_by_user_id = %s,
                updated_at = NOW()
            WHERE id = %s
            RETURNING id, portfolio_id, ticker, shares, entry_price, opened_at, notes,
                      created_by_user_id, updated_by_user_id, created_at, updated_at
        """
        with connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    sql,
                    (portfolio_id, ticker, shares, entry_price, opened_at, notes, updated_by_user_id, position_id),
                )
                rows = self._rows_to_dicts(cursor, cursor.fetchall())
            connection.commit()
        return rows[0] if rows else None

    def delete_position(self, position_id: int) -> bool:
        connection = self._connect()
        if connection is None:
            return False
        sql = "DELETE FROM portfolio_positions WHERE id = %s"
        with connection:
            with connection.cursor() as cursor:
                cursor.execute(sql, (position_id,))
                deleted = cursor.rowcount > 0
            connection.commit()
        return deleted

    def upsert_advice_snapshot(
        self,
        position_id: int,
        *,
        as_of_date: dt.date | None,
        latest_trade_date: dt.date | None,
        market_data_status: str,
        close_price: float | None,
        signal_status: str,
        stop_loss_price: float | None,
        tp1_price: float | None,
        tp2_price: float | None,
        tp1_sell_fraction: float | None,
        tp2_sell_fraction: float | None,
        average_up_price: float | None,
        average_up_share_fraction: float | None,
        blended_entry_after_average_up: float | None,
        net_cost_after_tp1: float | None,
        remaining_cost_basis_after_tp1: float | None,
        explanation: str,
        data_source: str,
        signal_context_json: dict[str, Any] | None = None,
    ) -> None:
        connection = self._connect()
        if connection is None:
            return
        sql = """
            INSERT INTO portfolio_advice_snapshots (
              position_id, as_of_date, latest_trade_date, market_data_status, close_price, signal_status,
              stop_loss_price, tp1_price, tp2_price, tp1_sell_fraction, tp2_sell_fraction,
              average_up_price, average_up_share_fraction, blended_entry_after_average_up,
              net_cost_after_tp1, remaining_cost_basis_after_tp1, explanation, data_source, signal_context_json,
              refreshed_at, updated_at
            )
            VALUES (
              %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, NOW(), NOW()
            )
            ON CONFLICT (position_id)
            DO UPDATE SET
              as_of_date = EXCLUDED.as_of_date,
              latest_trade_date = EXCLUDED.latest_trade_date,
              market_data_status = EXCLUDED.market_data_status,
              close_price = EXCLUDED.close_price,
              signal_status = EXCLUDED.signal_status,
              stop_loss_price = EXCLUDED.stop_loss_price,
              tp1_price = EXCLUDED.tp1_price,
              tp2_price = EXCLUDED.tp2_price,
              tp1_sell_fraction = EXCLUDED.tp1_sell_fraction,
              tp2_sell_fraction = EXCLUDED.tp2_sell_fraction,
              average_up_price = EXCLUDED.average_up_price,
              average_up_share_fraction = EXCLUDED.average_up_share_fraction,
              blended_entry_after_average_up = EXCLUDED.blended_entry_after_average_up,
              net_cost_after_tp1 = EXCLUDED.net_cost_after_tp1,
              remaining_cost_basis_after_tp1 = EXCLUDED.remaining_cost_basis_after_tp1,
              explanation = EXCLUDED.explanation,
              data_source = EXCLUDED.data_source,
              signal_context_json = EXCLUDED.signal_context_json,
              refreshed_at = NOW(),
              updated_at = NOW()
        """
        with connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    sql,
                    (
                        position_id,
                        as_of_date,
                        latest_trade_date,
                        market_data_status,
                        close_price,
                        signal_status,
                        stop_loss_price,
                        tp1_price,
                        tp2_price,
                        tp1_sell_fraction,
                        tp2_sell_fraction,
                        average_up_price,
                        average_up_share_fraction,
                        blended_entry_after_average_up,
                        net_cost_after_tp1,
                        remaining_cost_basis_after_tp1,
                        explanation,
                        data_source,
                        json.dumps(signal_context_json or {}),
                    ),
                )
            connection.commit()

    def create_import_batch(
        self,
        *,
        portfolio_id: int | None,
        source_name: str,
        imported_by_user_id: int | None,
        row_count: int,
        accepted_count: int,
        error_count: int,
        raw_csv_text: str,
        summary_json: dict[str, Any],
    ) -> int | None:
        connection = self._connect()
        if connection is None:
            return None
        sql = """
            INSERT INTO portfolio_import_batches (
              portfolio_id, source_name, imported_by_user_id, row_count, accepted_count, error_count, raw_csv_text, summary_json
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s::jsonb)
            RETURNING id
        """
        with connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    sql,
                    (
                        portfolio_id,
                        source_name,
                        imported_by_user_id,
                        row_count,
                        accepted_count,
                        error_count,
                        raw_csv_text,
                        json.dumps(summary_json),
                    ),
                )
                row = cursor.fetchone()
            connection.commit()
        return int(row[0]) if row else None

    def list_recent_signal_hits(self, tickers: list[str], *, lookback_days: int = 45) -> dict[str, list[dict[str, Any]]]:
        connection = self._connect()
        if connection is None:
            return {}
        normalized = sorted({str(item).strip().upper() for item in tickers if str(item).strip()})
        if not normalized:
            return {}
        sql = """
            SELECT hits.ticker, hits.strategy_id, hits.signal_date, hits.reasons_json, hits.metrics_json
            FROM screen_run_hits hits
            JOIN screen_runs runs ON runs.id = hits.screen_run_id
            WHERE hits.ticker = ANY(%s)
              AND hits.passed = TRUE
              AND runs.deleted_at IS NULL
              AND hits.signal_date >= CURRENT_DATE - (%s * INTERVAL '1 day')
            ORDER BY hits.signal_date DESC, hits.strategy_id ASC
        """
        with connection:
            with connection.cursor() as cursor:
                cursor.execute(sql, (normalized, max(1, int(lookback_days))))
                rows = self._rows_to_dicts(cursor, cursor.fetchall())
        grouped: dict[str, list[dict[str, Any]]] = {}
        for row in rows:
            ticker = str(row.get("ticker") or "").upper()
            grouped.setdefault(ticker, [])
            if len(grouped[ticker]) >= 6:
                continue
            grouped[ticker].append(row)
        return grouped
