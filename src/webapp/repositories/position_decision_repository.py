from __future__ import annotations

import datetime as dt
import json
from pathlib import Path
from typing import Any

from src.market_data_access import resolve_database_url


class PositionDecisionRepository:
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

    def upsert_daily_decisions(self, rows: list[dict[str, Any]]) -> int:
        if not rows:
            return 0
        connection = self._connect()
        if connection is None:
            return 0
        sql = """
            INSERT INTO daily_position_decisions (
              as_of_date,
              ticker,
              action,
              action_score,
              regime_state,
              trend_state,
              extension_state,
              support_reference,
              atr_dist_21,
              atr_dist_10w,
              atr_pct,
              daily_atr_ratio,
              close_price,
              ema21,
              sma50,
              sma10w,
              danger_signal_count,
              reason_summary,
              evidence_json,
              updated_at
            )
            VALUES (
              %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, NOW()
            )
            ON CONFLICT (as_of_date, ticker)
            DO UPDATE SET
              action = EXCLUDED.action,
              action_score = EXCLUDED.action_score,
              regime_state = EXCLUDED.regime_state,
              trend_state = EXCLUDED.trend_state,
              extension_state = EXCLUDED.extension_state,
              support_reference = EXCLUDED.support_reference,
              atr_dist_21 = EXCLUDED.atr_dist_21,
              atr_dist_10w = EXCLUDED.atr_dist_10w,
              atr_pct = EXCLUDED.atr_pct,
              daily_atr_ratio = EXCLUDED.daily_atr_ratio,
              close_price = EXCLUDED.close_price,
              ema21 = EXCLUDED.ema21,
              sma50 = EXCLUDED.sma50,
              sma10w = EXCLUDED.sma10w,
              danger_signal_count = EXCLUDED.danger_signal_count,
              reason_summary = EXCLUDED.reason_summary,
              evidence_json = EXCLUDED.evidence_json,
              updated_at = NOW()
        """
        values = [
            (
                row["as_of_date"],
                str(row.get("ticker") or "").strip().upper(),
                str(row.get("action") or "").strip(),
                float(row.get("action_score") or 0.0),
                row.get("regime_state"),
                row.get("trend_state"),
                row.get("extension_state"),
                row.get("support_reference"),
                row.get("atr_dist_21"),
                row.get("atr_dist_10w"),
                row.get("atr_pct"),
                row.get("daily_atr_ratio"),
                row.get("close_price"),
                row.get("ema21"),
                row.get("sma50"),
                row.get("sma10w"),
                int(row.get("danger_signal_count") or 0),
                row.get("reason_summary"),
                json.dumps(row.get("evidence_json") or {}),
            )
            for row in rows
        ]
        with connection:
            with connection.cursor() as cursor:
                cursor.executemany(sql, values)
            connection.commit()
        return len(values)

    def load_latest_decision_map(
        self,
        tickers: list[str],
        *,
        as_of_date: dt.date | None = None,
    ) -> dict[str, dict[str, Any]]:
        normalized = sorted({str(item or "").strip().upper() for item in tickers if str(item or "").strip()})
        if not normalized:
            return {}
        connection = self._connect()
        if connection is None:
            return {}
        if as_of_date is None:
            sql = """
                SELECT DISTINCT ON (ticker)
                  ticker,
                  as_of_date,
                  action,
                  action_score,
                  regime_state,
                  trend_state,
                  extension_state,
                  support_reference,
                  atr_dist_21,
                  atr_dist_10w,
                  atr_pct,
                  daily_atr_ratio,
                  close_price,
                  ema21,
                  sma50,
                  sma10w,
                  danger_signal_count,
                  reason_summary,
                  evidence_json,
                  created_at,
                  updated_at
                FROM daily_position_decisions
                WHERE ticker = ANY(%s)
                ORDER BY ticker ASC, as_of_date DESC, updated_at DESC, id DESC
            """
            params = (normalized,)
        else:
            sql = """
                SELECT DISTINCT ON (ticker)
                  ticker,
                  as_of_date,
                  action,
                  action_score,
                  regime_state,
                  trend_state,
                  extension_state,
                  support_reference,
                  atr_dist_21,
                  atr_dist_10w,
                  atr_pct,
                  daily_atr_ratio,
                  close_price,
                  ema21,
                  sma50,
                  sma10w,
                  danger_signal_count,
                  reason_summary,
                  evidence_json,
                  created_at,
                  updated_at
                FROM daily_position_decisions
                WHERE ticker = ANY(%s)
                  AND as_of_date <= %s
                ORDER BY ticker ASC, as_of_date DESC, updated_at DESC, id DESC
            """
            params = (normalized, as_of_date)
        with connection:
            with connection.cursor() as cursor:
                cursor.execute(sql, params)
                rows = self._rows_to_dicts(cursor, cursor.fetchall())
        payload: dict[str, dict[str, Any]] = {}
        for row in rows:
            ticker = str(row.get("ticker") or "").upper()
            if not ticker:
                continue
            payload[ticker] = row
        return payload
