from __future__ import annotations

import datetime as dt
import json
from pathlib import Path
from typing import Any

from src.market_data_access import resolve_database_url


class RelativeTrendStrengthRepository:
    def __init__(self, *, database_url: str = "") -> None:
        self.database_url = resolve_database_url(database_url)
        self._schema_ready = False

    def _connect(self):
        if not self.database_url:
            return None
        try:
            import psycopg
            if not self._schema_ready:
                schema_path = Path(__file__).resolve().parents[3] / "sql" / "postgres_app_schema.sql"
                with psycopg.connect(self.database_url) as connection:
                    with connection.cursor() as cursor:
                        cursor.execute(schema_path.read_text(encoding="utf-8"))
                    connection.commit()
                self._schema_ready = True
            return psycopg.connect(self.database_url)
        except Exception:
            return None

    def upsert_snapshots(self, rows: list[dict[str, object]]) -> int:
        if not rows:
            return 0
        connection = self._connect()
        if connection is None:
            return 0
        columns = (
            "ticker, as_of_date, sector, sector_etf, rts_score, rts_state, confidence, close_price, "
            "stock_vs_spy_21d_pct, stock_vs_spy_63d_pct, stock_vs_sector_63d_pct, alpha_acceleration_pct, "
            "market_relative_score, sector_relative_score, acceleration_score, structure_score, evidence_json"
        )
        sql = f"""
          INSERT INTO ticker_relative_trend_strength_snapshots ({columns})
          VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb)
          ON CONFLICT (ticker, as_of_date) DO UPDATE SET
            sector = EXCLUDED.sector, sector_etf = EXCLUDED.sector_etf, rts_score = EXCLUDED.rts_score,
            rts_state = EXCLUDED.rts_state, confidence = EXCLUDED.confidence,
            close_price = EXCLUDED.close_price,
            stock_vs_spy_21d_pct = EXCLUDED.stock_vs_spy_21d_pct, stock_vs_spy_63d_pct = EXCLUDED.stock_vs_spy_63d_pct,
            stock_vs_sector_63d_pct = EXCLUDED.stock_vs_sector_63d_pct, alpha_acceleration_pct = EXCLUDED.alpha_acceleration_pct,
            market_relative_score = EXCLUDED.market_relative_score, sector_relative_score = EXCLUDED.sector_relative_score,
            acceleration_score = EXCLUDED.acceleration_score, structure_score = EXCLUDED.structure_score,
            evidence_json = EXCLUDED.evidence_json, updated_at = NOW()
        """
        values = [tuple(row.get(key) if key != "evidence" else json.dumps(row.get("evidence") or {}) for key in (
            "ticker", "as_of_date", "sector", "sector_etf", "rts_score", "rts_state", "confidence", "close_price",
            "stock_vs_spy_21d_pct", "stock_vs_spy_63d_pct", "stock_vs_sector_63d_pct", "alpha_acceleration_pct",
            "market_relative_score", "sector_relative_score", "acceleration_score", "structure_score", "evidence",
        )) for row in rows]
        with connection:
            with connection.cursor() as cursor:
                cursor.executemany(sql, values)
            connection.commit()
        return len(values)

    def load_latest_snapshot_map(self, tickers: list[str], *, as_of_date: dt.date | None = None) -> dict[str, dict[str, Any]]:
        normalized = sorted({str(item or "").strip().upper() for item in tickers if str(item or "").strip()})
        if not normalized:
            return {}
        connection = self._connect()
        if connection is None:
            return {}
        predicate, params = ("ticker = ANY(%s)", (normalized,)) if as_of_date is None else ("ticker = ANY(%s) AND as_of_date <= %s", (normalized, as_of_date))
        sql = f"SELECT DISTINCT ON (ticker) * FROM ticker_relative_trend_strength_snapshots WHERE {predicate} ORDER BY ticker, as_of_date DESC, updated_at DESC"
        try:
            with connection:
                with connection.cursor() as cursor:
                    cursor.execute(sql, params)
                    columns = [item.name if hasattr(item, "name") else item[0] for item in cursor.description or []]
                    rows = [dict(zip(columns, row)) for row in cursor.fetchall()]
        except Exception:
            return {}
        return {str(row.get("ticker") or "").upper(): row for row in rows}

    def load_recent_snapshot_map(
        self,
        tickers: list[str],
        *,
        as_of_date: dt.date | None = None,
        limit_per_ticker: int = 20,
    ) -> dict[str, list[dict[str, Any]]]:
        normalized = sorted({str(item or "").strip().upper() for item in tickers if str(item or "").strip()})
        if not normalized:
            return {}
        connection = self._connect()
        if connection is None:
            return {}
        date_clause = "" if as_of_date is None else "AND as_of_date <= %s"
        params: tuple[object, ...] = (normalized, int(limit_per_ticker)) if as_of_date is None else (normalized, as_of_date, int(limit_per_ticker))
        sql = f"""
          SELECT * FROM (
            SELECT *, ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY as_of_date DESC, updated_at DESC) AS row_number
            FROM ticker_relative_trend_strength_snapshots
            WHERE ticker = ANY(%s) {date_clause}
          ) ranked
          WHERE row_number <= %s
          ORDER BY ticker, as_of_date
        """
        try:
            with connection:
                with connection.cursor() as cursor:
                    cursor.execute(sql, params)
                    columns = [item.name if hasattr(item, "name") else item[0] for item in cursor.description or []]
                    rows = [dict(zip(columns, row)) for row in cursor.fetchall()]
        except Exception:
            return {}
        result: dict[str, list[dict[str, Any]]] = {}
        for row in rows:
            result.setdefault(str(row.get("ticker") or "").upper(), []).append(row)
        return result

    def load_snapshots_for_validation(
        self,
        *,
        start_date: dt.date,
        end_date: dt.date,
    ) -> list[dict[str, Any]]:
        connection = self._connect()
        if connection is None:
            return []
        sql = """
          SELECT * FROM ticker_relative_trend_strength_snapshots
          WHERE as_of_date BETWEEN %s AND %s
          ORDER BY as_of_date, ticker
        """
        try:
            with connection:
                with connection.cursor() as cursor:
                    cursor.execute(sql, (start_date, end_date))
                    columns = [item.name if hasattr(item, "name") else item[0] for item in cursor.description or []]
                    return [dict(zip(columns, row)) for row in cursor.fetchall()]
        except Exception:
            return []
