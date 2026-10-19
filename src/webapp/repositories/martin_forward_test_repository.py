from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from src.market_data_access import resolve_database_url


class MartinForwardTestRepository:
    def __init__(self, *, database_url: str = "") -> None:
        self.database_url = resolve_database_url(database_url)
        self._schema_ready = False

    def _connect(self):
        if not self.database_url:
            return None
        try:
            import psycopg
        except ImportError:
            return None
        if not self._schema_ready:
            schema = Path(__file__).resolve().parents[3] / "sql" / "postgres_app_schema.sql"
            with psycopg.connect(self.database_url) as connection:
                with connection.cursor() as cursor:
                    cursor.execute(schema.read_text(encoding="utf-8"))
            self._schema_ready = True
        return psycopg.connect(self.database_url)

    def save_session(self, *, session_date: Any, source_run_id: int, fingerprint: str, candidates: list[dict[str, Any]], summary: dict[str, Any]) -> int | None:
        connection = self._connect()
        if connection is None:
            return None
        with connection:
            with connection.cursor() as cursor:
                cursor.execute("""INSERT INTO martin_forward_runs(strategy_id,session_date,source_run_id,source_fingerprint,summary_json)
                    VALUES ('martin_daily_v1',%s,%s,%s,%s::jsonb) ON CONFLICT(strategy_id,session_date,source_fingerprint)
                    DO UPDATE SET summary_json=EXCLUDED.summary_json RETURNING id""", (session_date, source_run_id, fingerprint, json.dumps(summary)))
                run_id = cursor.fetchone()[0]
                for item in candidates:
                    cursor.execute("""INSERT INTO martin_forward_candidates(run_id,ticker,rank,status,reasons_json,payload_json)
                        VALUES (%s,%s,%s,%s,%s::jsonb,%s::jsonb) ON CONFLICT(run_id,ticker) DO NOTHING""",
                        (run_id, item["ticker"], item["rank"], item["status"], json.dumps(item["reasons"]), json.dumps(item)))
        return int(run_id)

    def pending_candidates(self, *, before: Any) -> list[dict[str, Any]]:
        return self._rows("""SELECT c.id,c.run_id,c.ticker,c.rank,c.status,c.payload_json,r.session_date
            FROM martin_forward_candidates c JOIN martin_forward_runs r ON r.id=c.run_id
            WHERE c.status='pending' AND r.session_date < %s ORDER BY r.session_date,c.rank,c.id""", (before,))

    def pending_orders(self) -> list[dict[str, Any]]:
        return self._rows("""SELECT c.ticker,c.payload_json FROM martin_forward_candidates c
            WHERE c.status='pending' ORDER BY c.rank,c.id""")

    def open_positions(self) -> list[dict[str, Any]]:
        return self._rows("SELECT id,ticker,payload_json,opened_on FROM martin_forward_positions WHERE status='open' ORDER BY opened_on,id")

    def has_open_position(self, ticker: str) -> bool:
        rows = self._rows("SELECT id FROM martin_forward_positions WHERE ticker=%s AND status='open' LIMIT 1", (ticker,))
        return bool(rows)

    def has_pending_order(self, ticker: str) -> bool:
        return bool(self._rows("""SELECT c.id FROM martin_forward_candidates c JOIN martin_forward_runs r ON r.id=c.run_id
            WHERE c.ticker=%s AND c.status='pending' LIMIT 1""", (ticker,)))

    def candidate_status(self, run_id: int, ticker: str) -> str | None:
        rows = self._rows("SELECT status FROM martin_forward_candidates WHERE run_id=%s AND ticker=%s", (run_id, ticker))
        return str(rows[0]["status"]) if rows else None

    def update_candidate(self, run_id: int, ticker: str, payload: dict[str, Any]) -> None:
        connection = self._connect()
        if connection is None:
            return
        with connection:
            with connection.cursor() as cursor:
                cursor.execute("""UPDATE martin_forward_candidates SET status=%s,reasons_json=%s::jsonb,payload_json=%s::jsonb
                    WHERE run_id=%s AND ticker=%s""", (payload["status"], json.dumps(payload.get("reasons") or []), json.dumps(payload), run_id, ticker))

    def fill_order(self, *, candidate_id: int, run_id: int, ticker: str, event_date: Any,
                   position_payload: dict[str, Any], event_payload: dict[str, Any]) -> int | None:
        """Atomically reserve the unique open slot, append the fill and settle its candidate."""
        connection = self._connect()
        if connection is None:
            return None
        with connection:
            with connection.cursor() as cursor:
                cursor.execute("SELECT status FROM martin_forward_candidates WHERE id=%s FOR UPDATE", (candidate_id,))
                candidate = cursor.fetchone()
                if not candidate or candidate[0] != "pending":
                    return None
                cursor.execute("""INSERT INTO martin_forward_positions(ticker,status,opened_on,payload_json)
                    VALUES (%s,'open',%s,%s::jsonb) ON CONFLICT DO NOTHING RETURNING id""", (ticker, event_date, json.dumps(position_payload)))
                row = cursor.fetchone()
                if row is None:
                    blocked = {"status": "blocked", "reason": "open_position_already_exists"}
                    cursor.execute("""INSERT INTO martin_forward_events(run_id,ticker,event_type,event_date,payload_json)
                        VALUES (%s,%s,'order_blocked',%s,%s::jsonb) ON CONFLICT DO NOTHING""", (run_id, ticker, event_date, json.dumps(blocked)))
                    cursor.execute("UPDATE martin_forward_candidates SET status='blocked',payload_json=payload_json || %s::jsonb WHERE id=%s",
                                   (json.dumps(blocked), candidate_id))
                    return None
                position_id = int(row[0])
                cursor.execute("""INSERT INTO martin_forward_events(run_id,position_id,ticker,event_type,event_date,payload_json)
                    VALUES (%s,%s,%s,'order_filled',%s,%s::jsonb) ON CONFLICT DO NOTHING""",
                    (run_id, position_id, ticker, event_date, json.dumps(event_payload)))
                cursor.execute("UPDATE martin_forward_candidates SET status='filled',payload_json=payload_json || %s::jsonb WHERE id=%s",
                               (json.dumps(event_payload), candidate_id))
                return position_id

    def apply_event(self, *, run_id: int | None, position_id: int | None, ticker: str, event_type: str, event_date: Any,
                    payload: dict[str, Any], candidate_id: int | None = None, candidate_status: str | None = None,
                    position_payload: dict[str, Any] | None = None, close_position: bool = False) -> bool:
        connection = self._connect()
        if connection is None:
            return False
        with connection:
            with connection.cursor() as cursor:
                cursor.execute("""INSERT INTO martin_forward_events(run_id,position_id,ticker,event_type,event_date,payload_json)
                    VALUES (%s,%s,%s,%s,%s,%s::jsonb) ON CONFLICT DO NOTHING RETURNING id""",
                    (run_id, position_id, ticker, event_type, event_date, json.dumps(payload)))
                if cursor.fetchone() is None:
                    return False
                if candidate_id is not None and candidate_status:
                    cursor.execute("UPDATE martin_forward_candidates SET status=%s,payload_json=payload_json || %s::jsonb WHERE id=%s",
                                   (candidate_status, json.dumps(payload), candidate_id))
                if position_id is not None and position_payload is not None:
                    cursor.execute("UPDATE martin_forward_positions SET payload_json=%s::jsonb,status=%s,closed_on=%s WHERE id=%s",
                                   (json.dumps(position_payload), "closed" if close_position else "open", event_date if close_position else None, position_id))
                return True

    def set_position_payload(self, position_id: int, payload: dict[str, Any]) -> None:
        connection = self._connect()
        if connection is None:
            return
        with connection:
            with connection.cursor() as cursor:
                cursor.execute("UPDATE martin_forward_positions SET payload_json=%s::jsonb WHERE id=%s AND status='open'", (json.dumps(payload), position_id))

    def latest_context(self) -> dict[str, Any]:
        connection = self._connect()
        if connection is None:
            return {"configured": False, "runs": [], "candidates": [], "positions": [], "events": []}
        with connection:
            with connection.cursor() as cursor:
                cursor.execute("SELECT id,session_date FROM martin_forward_runs ORDER BY session_date DESC,id DESC LIMIT 1")
                latest = cursor.fetchone()
                if latest:
                    run_id = int(latest[0])
                    runs = self._fetch(cursor, "SELECT * FROM martin_forward_runs WHERE id=%s", (run_id,))
                    candidates = self._fetch(cursor, "SELECT * FROM martin_forward_candidates WHERE run_id=%s ORDER BY rank", (run_id,))
                else:
                    runs, candidates = [], []
                positions = self._fetch(cursor, "SELECT * FROM martin_forward_positions WHERE status='open' ORDER BY opened_on,id")
                events = self._fetch(cursor, "SELECT * FROM martin_forward_events ORDER BY event_date DESC,id DESC LIMIT 100")
        return {"configured": True, "runs": runs, "candidates": candidates, "positions": positions, "events": events}

    def _rows(self, sql: str, params: tuple[Any, ...] = ()) -> list[dict[str, Any]]:
        connection = self._connect()
        if connection is None:
            return []
        with connection:
            with connection.cursor() as cursor:
                return self._fetch(cursor, sql, params)

    @staticmethod
    def _fetch(cursor: Any, sql: str, params: tuple[Any, ...] = ()) -> list[dict[str, Any]]:
        cursor.execute(sql, params)
        columns = [col.name for col in cursor.description]
        return [dict(zip(columns, row)) for row in cursor.fetchall()]

    def context(self) -> dict[str, Any]:
        return self.latest_context()
