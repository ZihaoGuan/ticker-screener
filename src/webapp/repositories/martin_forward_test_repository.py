from __future__ import annotations

import datetime as dt
import json
from contextlib import contextmanager
from pathlib import Path
from typing import Any

from src.market_data_access import resolve_database_url
from src.martin_daily_gap_retest import REPLAY_STRATEGY_ID, REPLAY_V2_STRATEGY_ID, REPLAY_V3_STRATEGY_ID, REPLAY_V4_STRATEGY_ID
from src.webapp.services.martin_rotation import ROTATION_STRATEGY_ID, ROTATION_POLICY_VERSION, ROTATION_LIMITS, account_budget

GAP_RETEST_STRATEGY_ID = "martin_daily_gap_retest_v1"
GAP_RETEST_REPLAY_STRATEGY_ID = REPLAY_STRATEGY_ID
GAP_RETEST_REPLAY_V2_STRATEGY_ID = REPLAY_V2_STRATEGY_ID
GAP_RETEST_REPLAY_V3_STRATEGY_ID = REPLAY_V3_STRATEGY_ID
GAP_RETEST_REPLAY_V4_STRATEGY_ID = REPLAY_V4_STRATEGY_ID
ACCOUNT_STRATEGIES = {"martin_daily_v1", "martin_rs_expanded_v1", ROTATION_STRATEGY_ID, GAP_RETEST_STRATEGY_ID,
                      GAP_RETEST_REPLAY_STRATEGY_ID, GAP_RETEST_REPLAY_V2_STRATEGY_ID, GAP_RETEST_REPLAY_V3_STRATEGY_ID,
                      GAP_RETEST_REPLAY_V4_STRATEGY_ID}


def _json_default(value: Any) -> str:
    if isinstance(value, (dt.date, dt.datetime)):
        return value.isoformat()
    raise TypeError(f"Object of type {value.__class__.__name__} is not JSON serializable")


class _SessionConnection:
    def __init__(self, connection): self.connection = connection
    def __enter__(self): return self
    def __exit__(self, *_args): return False
    def cursor(self): return self.connection.cursor()


class MartinForwardTestRepository:
    def __init__(self, *, database_url: str = "", read_only: bool = False) -> None:
        self.database_url = resolve_database_url(database_url)
        self.read_only = read_only
        self._schema_ready = False
        self._session_connection = None

    @contextmanager
    def atomic_session(self, strategy_id: str):
        """The rotation book's linked exits and entries commit or roll back together."""
        if self._session_connection is not None:
            yield
            return
        connection = self._connect()
        if connection is None:
            raise RuntimeError("Rotation strategy requires a configured database")
        with connection:
            with connection.cursor() as cursor:
                cursor.execute("SELECT pg_try_advisory_xact_lock(hashtext(%s))", (f"martin-forward:{strategy_id}",))
                if not cursor.fetchone()[0]:
                    raise RuntimeError("Another process is working on the rotation book; retry later")
            self._session_connection = _SessionConnection(connection)
            try:
                yield
            finally:
                self._session_connection = None

    def latest_session_date(self, strategy_id: str):
        rows = self._rows("SELECT max(session_date) AS latest_date FROM martin_forward_runs WHERE strategy_id=%s", (strategy_id,))
        return rows[0]["latest_date"] if rows else None

    def session_run_id(self, strategy_id: str, session_date: Any) -> int | None:
        rows = self._rows("SELECT id FROM martin_forward_runs WHERE strategy_id=%s AND session_date=%s ORDER BY id DESC LIMIT 1", (strategy_id, session_date))
        return int(rows[0]["id"]) if rows else None

    def latest_source_run_id(self, strategy_id: str) -> int | None:
        rows = self._rows("SELECT source_run_id FROM martin_forward_runs WHERE strategy_id=%s ORDER BY session_date DESC,id DESC LIMIT 1", (strategy_id,))
        return int(rows[0]["source_run_id"]) if rows and rows[0].get("source_run_id") is not None else None

    def latest_gap_watch_states(self, strategy_id: str = GAP_RETEST_STRATEGY_ID) -> dict[str, dict[str, Any]]:
        rows = self._rows("""SELECT DISTINCT ON (c.ticker) c.ticker,c.payload_json
            FROM martin_forward_candidates c JOIN martin_forward_runs r ON r.id=c.run_id
            WHERE r.strategy_id=%s ORDER BY c.ticker,r.session_date DESC,r.id DESC,c.id DESC""", (strategy_id,))
        return {str(row["ticker"]).upper(): row.get("payload_json") or {} for row in rows}

    def gap_events(self, strategy_id: str = GAP_RETEST_STRATEGY_ID) -> list[dict[str, Any]]:
        return self._rows("SELECT * FROM martin_forward_events WHERE strategy_id=%s ORDER BY event_date,id", (strategy_id,))

    def has_gap_event_fill(self, strategy_id: str, event_identity: Any) -> bool:
        identity = {"event_identity": event_identity}
        rows = self._rows("""SELECT id FROM martin_forward_positions WHERE strategy_id=%s
            AND payload_json @> %s::jsonb LIMIT 1""", (strategy_id, json.dumps(identity)))
        if rows:
            return True
        # Events keep the identity after a position is closed or otherwise retired.
        rows = self._rows("""SELECT id FROM martin_forward_events WHERE strategy_id=%s
            AND event_type='order_filled' AND payload_json @> %s::jsonb LIMIT 1""", (strategy_id, json.dumps(identity)))
        return bool(rows)

    def account_state(self, strategy_id: str, *, exclude_order_ticker: str | None = None) -> dict[str, float]:
        rows = self._rows("SELECT status,payload_json FROM martin_forward_positions WHERE strategy_id=%s", (strategy_id,))
        realized = sum(float((row.get("payload_json") or {}).get("realized_pnl") or 0) for row in rows)
        orders = [row for row in self.pending_orders(strategy_id=strategy_id) if row["ticker"] != exclude_order_ticker]
        return account_budget([row for row in rows if row["status"] == "open"], orders, realized)

    def set_run_metadata(self, run_id: int, metadata: dict) -> None:
        with self._connect() as connection:
            with connection.cursor() as cursor:
                cursor.execute("UPDATE martin_forward_runs SET summary_json=summary_json || %s::jsonb WHERE id=%s", (json.dumps(metadata), run_id))

    def last_rotation_exit(self, strategy_id: str):
        rows = self._rows("SELECT max(event_date) AS latest_date FROM martin_forward_events WHERE strategy_id=%s AND event_type='rotation_exit'", (strategy_id,))
        return rows[0]["latest_date"] if rows else None

    @contextmanager
    def execution_lock(self, strategy_id: str):
        """Share one book lock between scheduled runs and explicit historical replay."""
        if not self.database_url:
            yield
            return
        import psycopg
        with psycopg.connect(self.database_url, autocommit=True) as connection:
            with connection.cursor() as cursor:
                key = f"martin-forward:{strategy_id}"
                cursor.execute("SELECT pg_try_advisory_lock(hashtext(%s))", (key,))
                if not cursor.fetchone()[0]:
                    raise RuntimeError(f"Another process is working on {strategy_id}; retry later.")
                try:
                    yield
                finally:
                    cursor.execute("SELECT pg_advisory_unlock(hashtext(%s))", (key,))

    def _connect(self):
        if self._session_connection is not None:
            return self._session_connection
        if not self.database_url:
            return None
        try:
            import psycopg
        except ImportError:
            return None
        if not self._schema_ready and not self.read_only:
            schema = Path(__file__).resolve().parents[3] / "sql" / "postgres_app_schema.sql"
            with psycopg.connect(self.database_url) as connection:
                with connection.cursor() as cursor:
                    cursor.execute(schema.read_text(encoding="utf-8"))
            self._schema_ready = True
        return psycopg.connect(self.database_url, options="-c default_transaction_read_only=on") if self.read_only else psycopg.connect(self.database_url)

    def save_session(self, *, session_date: Any, source_run_id: int, fingerprint: str, candidates: list[dict[str, Any]], summary: dict[str, Any], strategy_id: str = "martin_daily_v1") -> int | None:
        connection = self._connect()
        if connection is None:
            return None
        with connection:
            with connection.cursor() as cursor:
                cursor.execute("""INSERT INTO martin_forward_runs(strategy_id,session_date,source_run_id,source_fingerprint,summary_json)
                    VALUES (%s,%s,%s,%s,%s::jsonb) ON CONFLICT(strategy_id,session_date,source_fingerprint)
                    DO UPDATE SET summary_json=EXCLUDED.summary_json RETURNING id""", (strategy_id, session_date, source_run_id, fingerprint, json.dumps(summary, default=_json_default)))
                run_id = cursor.fetchone()[0]
                for item in candidates:
                    cursor.execute("""INSERT INTO martin_forward_candidates(run_id,ticker,rank,status,reasons_json,payload_json)
                        VALUES (%s,%s,%s,%s,%s::jsonb,%s::jsonb) ON CONFLICT(run_id,ticker) DO NOTHING""",
                        (run_id, item["ticker"], item["rank"], item["status"], json.dumps(item["reasons"], default=_json_default), json.dumps(item, default=_json_default)))
        return int(run_id)

    def replay_covers_date(self, session_date: Any, strategy_id: str) -> bool:
        return bool(self._rows("""SELECT id FROM martin_forward_runs WHERE strategy_id=%s AND session_date >= %s
            AND summary_json->'replay_provenance'->>'origin'='historical_replay' LIMIT 1""", (strategy_id, session_date)))

    def pending_candidates(self, *, before: Any, strategy_id: str = "martin_daily_v1") -> list[dict[str, Any]]:
        return self._rows("""SELECT c.id,c.run_id,c.ticker,c.rank,c.status,c.payload_json,r.session_date
            FROM martin_forward_candidates c JOIN martin_forward_runs r ON r.id=c.run_id
            WHERE c.status='pending' AND r.session_date < %s AND r.strategy_id=%s ORDER BY r.session_date,c.rank,c.id""", (before, strategy_id))

    def pending_orders(self, *, strategy_id: str = "martin_daily_v1", before: Any = None) -> list[dict[str, Any]]:
        if before is not None:
            return self._rows("""SELECT c.ticker,c.payload_json FROM martin_forward_candidates c
                JOIN martin_forward_runs r ON r.id=c.run_id WHERE c.status='pending' AND r.strategy_id=%s
                AND r.session_date < %s ORDER BY c.rank,c.id""", (strategy_id, before))
        return self._rows("""SELECT c.ticker,c.payload_json FROM martin_forward_candidates c
            JOIN martin_forward_runs r ON r.id=c.run_id WHERE c.status='pending' AND r.strategy_id=%s ORDER BY c.rank,c.id""", (strategy_id,))

    def submitted_order_count(self, *, session_date: Any, strategy_id: str) -> int:
        rows = self._rows("""SELECT COUNT(*) AS count FROM martin_forward_candidates c
            JOIN martin_forward_runs r ON r.id=c.run_id
            WHERE r.strategy_id=%s AND r.session_date=%s AND c.payload_json ? 'order_date'""", (strategy_id, session_date))
        return int(rows[0]["count"]) if rows else 0

    def daily_top5_selected_tickers(self, *, session_date: Any, book_id: str = "martin_daily_top5_v1") -> list[str]:
        rows = self._rows("""SELECT DISTINCT c.ticker FROM martin_forward_candidates c
            JOIN martin_forward_runs r ON r.id=c.run_id
            WHERE r.strategy_id=%s AND r.session_date=%s
              AND c.payload_json->>'selected_for_book'='true' ORDER BY c.ticker""", (book_id, session_date))
        return [str(row["ticker"]).upper() for row in rows]

    def open_positions(self, *, strategy_id: str = "martin_daily_v1") -> list[dict[str, Any]]:
        return self._rows("SELECT id,ticker,payload_json,opened_on FROM martin_forward_positions WHERE status='open' AND strategy_id=%s ORDER BY opened_on,id", (strategy_id,))

    def has_open_position(self, ticker: str, *, strategy_id: str = "martin_daily_v1") -> bool:
        rows = self._rows("SELECT id FROM martin_forward_positions WHERE ticker=%s AND status='open' AND strategy_id=%s LIMIT 1", (ticker, strategy_id))
        return bool(rows)

    def has_pending_order(self, ticker: str, *, strategy_id: str = "martin_daily_v1") -> bool:
        return bool(self._rows("""SELECT c.id FROM martin_forward_candidates c JOIN martin_forward_runs r ON r.id=c.run_id
            WHERE c.ticker=%s AND c.status='pending' AND r.strategy_id=%s LIMIT 1""", (ticker, strategy_id)))

    def candidate_status(self, run_id: int, ticker: str) -> str | None:
        rows = self._rows("SELECT status FROM martin_forward_candidates WHERE run_id=%s AND ticker=%s", (run_id, ticker))
        return str(rows[0]["status"]) if rows else None

    def candidate_payload(self, run_id: int, ticker: str) -> dict[str, Any]:
        rows = self._rows("SELECT payload_json FROM martin_forward_candidates WHERE run_id=%s AND ticker=%s", (run_id, ticker))
        return rows[0].get("payload_json") or {} if rows else {}

    def update_run_summary(self, *, run_id: int, strategy_id: str, mark_prices: dict[str, float], action_counts: dict[str, int], policy_id: str | None = None) -> None:
        connection = self._connect()
        if connection is None:
            return
        with connection:
            with connection.cursor() as cursor:
                cursor.execute("SELECT session_date,summary_json FROM martin_forward_runs WHERE id=%s AND strategy_id=%s", (run_id, strategy_id))
                run = cursor.fetchone()
                if not run:
                    return
                session_date, summary = run
                cursor.execute("SELECT status,payload_json FROM martin_forward_candidates WHERE run_id=%s", (run_id,))
                candidates = [{"status": row[0], "payload_json": row[1] or {}} for row in cursor.fetchall()]
                cursor.execute("SELECT status,payload_json FROM martin_forward_positions WHERE strategy_id=%s", (strategy_id,))
                positions = [{"status": row[0], "payload_json": row[1] or {}} for row in cursor.fetchall()]
                cursor.execute("SELECT summary_json FROM martin_forward_runs WHERE strategy_id=%s AND session_date<%s ORDER BY session_date,id", (strategy_id, session_date))
                prior_summaries = [row[0] or {} for row in cursor.fetchall()]
                closed = [row["payload_json"] for row in positions if row["status"] == "closed"]
                opened = [row["payload_json"] for row in positions if row["status"] == "open"]
                realized_positions = closed + opened
                realized_r = sum(float(row.get("realized_r") or 0) for row in realized_positions)
                realized_pnl = sum(float(row.get("realized_pnl") or 0) for row in realized_positions)
                unrealized_r = unrealized_pnl = 0.0
                for position in opened:
                    ticker = str(position.get("ticker") or "").upper()
                    mark = mark_prices.get(ticker)
                    if mark is None:
                        continue
                    shares = int(position.get("remaining_shares") or 0)
                    entry = float(position.get("entry_price") or 0)
                    risk_per_share = float(position.get("initial_r_per_share") or 0)
                    pnl = (mark - entry) * shares
                    unrealized_pnl += pnl
                    denominator = risk_per_share * int(position.get("initial_shares") or position.get("shares") or 1)
                    unrealized_r += pnl / denominator if denominator else 0
                cumulative_r = realized_r + unrealized_r
                metric_policy = policy_id or strategy_id
                equity = 100_000.0 + realized_pnl + unrealized_pnl if metric_policy in ACCOUNT_STRATEGIES else None
                prior_equities = [float(item["account_equity"]) for item in prior_summaries if item.get("account_equity") is not None]
                prior_rs = [float(item["cumulative_r"]) for item in prior_summaries if item.get("cumulative_r") is not None]
                equity_peak = max([100_000.0, *prior_equities])
                r_peak = max([0.0, *prior_rs])
                sectors = {}
                for candidate in candidates:
                    payload = candidate["payload_json"]
                    sector = str(payload.get("sector") or "Unknown")
                    sectors[sector] = sectors.get(sector, 0) + int(bool(payload.get("selected_for_book", candidate["status"] in {"eligible", "pending", "filled"})))
                summary = {**(summary or {}), "action_counts": action_counts,
                    "partial": action_counts.get("missing_bars", 0) > 0,
                    "selected_count": sum(bool(item["payload_json"].get("selected_for_book", item["payload_json"].get("selection_tier") or item["status"] in {"eligible", "pending", "filled"})) for item in candidates),
                    "pending_count": sum(item["status"] == "pending" for item in candidates),
                    "waiting_capacity_count": sum(item["status"] == "waiting_capacity" for item in candidates),
                    "open_count": len(opened), "closed_count": len(closed), "sector_counts": sectors,
                    "realized_r": realized_r, "unrealized_r": unrealized_r, "cumulative_r": cumulative_r,
                    "realized_pnl": realized_pnl, "unrealized_pnl": unrealized_pnl,
                    "gross_wins": sum(float(row.get("realized_pnl") or 0) for row in closed if float(row.get("realized_pnl") or 0) > 0),
                    "gross_losses": sum(abs(float(row.get("realized_pnl") or 0)) for row in closed if float(row.get("realized_pnl") or 0) < 0),
                    **_closed_trade_metrics(closed),
                    "account_equity": equity, "equity_drawdown_pct": (equity / equity_peak - 1) * 100 if equity is not None else None,
                    "r_drawdown": cumulative_r - r_peak}
                if metric_policy == ROTATION_STRATEGY_ID:
                    summary.update(self.account_state(strategy_id))
                cursor.execute("UPDATE martin_forward_runs SET summary_json=%s::jsonb WHERE id=%s", (json.dumps(summary), run_id))

    def update_candidate(self, run_id: int, ticker: str, payload: dict[str, Any]) -> None:
        connection = self._connect()
        if connection is None:
            return
        with connection:
            with connection.cursor() as cursor:
                cursor.execute("""UPDATE martin_forward_candidates SET status=%s,reasons_json=%s::jsonb,payload_json=%s::jsonb
                    WHERE run_id=%s AND ticker=%s""", (payload["status"], json.dumps(payload.get("reasons") or []), json.dumps(payload), run_id, ticker))

    def fill_order(self, *, candidate_id: int, run_id: int, ticker: str, event_date: Any,
                   position_payload: dict[str, Any], event_payload: dict[str, Any], strategy_id: str = "martin_daily_v1") -> int | None:
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
                cursor.execute("""INSERT INTO martin_forward_positions(strategy_id,ticker,status,opened_on,payload_json)
                    VALUES (%s,%s,'open',%s,%s::jsonb) ON CONFLICT DO NOTHING RETURNING id""", (strategy_id, ticker, event_date, json.dumps(position_payload)))
                row = cursor.fetchone()
                if row is None:
                    blocked = {"status": "blocked", "reason": "open_position_already_exists"}
                    cursor.execute("""INSERT INTO martin_forward_events(strategy_id,run_id,ticker,event_type,event_date,payload_json)
                        VALUES (%s,%s,%s,'order_blocked',%s,%s::jsonb) ON CONFLICT DO NOTHING""", (strategy_id, run_id, ticker, event_date, json.dumps(blocked)))
                    cursor.execute("UPDATE martin_forward_candidates SET status='blocked',payload_json=payload_json || %s::jsonb WHERE id=%s",
                                   (json.dumps(blocked), candidate_id))
                    return None
                position_id = int(row[0])
                cursor.execute("""INSERT INTO martin_forward_events(strategy_id,run_id,position_id,ticker,event_type,event_date,payload_json)
                    VALUES (%s,%s,%s,%s,'order_filled',%s,%s::jsonb) ON CONFLICT DO NOTHING""",
                    (strategy_id, run_id, position_id, ticker, event_date, json.dumps(event_payload)))
                cursor.execute("UPDATE martin_forward_candidates SET status='filled',payload_json=payload_json || %s::jsonb WHERE id=%s",
                               (json.dumps(event_payload), candidate_id))
                return position_id

    def apply_event(self, *, run_id: int | None, position_id: int | None, ticker: str, event_type: str, event_date: Any,
                    payload: dict[str, Any], candidate_id: int | None = None, candidate_status: str | None = None,
                    position_payload: dict[str, Any] | None = None, close_position: bool = False,
                    strategy_id: str = "martin_daily_v1") -> bool:
        connection = self._connect()
        if connection is None:
            return False
        if position_payload is not None and close_position:
            position_payload = {
                **position_payload,
                "status": "closed",
                "closed_on": event_date.isoformat() if hasattr(event_date, "isoformat") else str(event_date),
            }
        with connection:
            with connection.cursor() as cursor:
                cursor.execute("""INSERT INTO martin_forward_events(strategy_id,run_id,position_id,ticker,event_type,event_date,payload_json)
                    VALUES (%s,%s,%s,%s,%s,%s,%s::jsonb) ON CONFLICT DO NOTHING RETURNING id""",
                    (strategy_id, run_id, position_id, ticker, event_type, event_date, json.dumps(payload)))
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

    def context(self) -> dict[str, Any]:
        connection = self._connect()
        if connection is None:
            return {"configured": False, "books": {}, "comparison": {}}
        with connection:
            with connection.cursor() as cursor:
                books = {}
                for strategy_id, key in (("martin_daily_v1", "account"), ("martin_sector_book_v1", "sector_opportunity"),
                                         ("martin_daily_top5_v1", "daily_top5"), ("martin_rs_expanded_v1", "rs_expanded"),
                                         (ROTATION_STRATEGY_ID, "rs_expanded_rotation"),
                                         (GAP_RETEST_STRATEGY_ID, "gap_retest"),
                                         (GAP_RETEST_REPLAY_STRATEGY_ID, "gap_retest_replay"),
                                         (GAP_RETEST_REPLAY_V2_STRATEGY_ID, "gap_retest_replay_v2"),
                                         (GAP_RETEST_REPLAY_V3_STRATEGY_ID, "gap_retest_replay_v3"),
                                         (GAP_RETEST_REPLAY_V4_STRATEGY_ID, "gap_retest_replay_v4")):
                    runs = self._fetch(cursor, "SELECT * FROM martin_forward_runs WHERE strategy_id=%s ORDER BY session_date DESC,id DESC LIMIT 200", (strategy_id,))
                    latest_id = runs[0]["id"] if runs else None
                    candidates = self._fetch(cursor, "SELECT * FROM martin_forward_candidates WHERE run_id=%s ORDER BY rank", (latest_id,)) if latest_id else []
                    outstanding_orders = self._fetch(cursor, """SELECT c.id,c.run_id,c.ticker,c.rank,c.status,c.reasons_json,c.payload_json,r.session_date
                        FROM martin_forward_candidates c JOIN martin_forward_runs r ON r.id=c.run_id
                        WHERE c.status='pending' AND r.strategy_id=%s ORDER BY r.session_date,c.rank,c.id""", (strategy_id,))
                    positions = self._fetch(cursor, "SELECT * FROM martin_forward_positions WHERE strategy_id=%s ORDER BY opened_on DESC,id DESC", (strategy_id,))
                    events = self._fetch(cursor, "SELECT * FROM martin_forward_events WHERE strategy_id=%s ORDER BY event_date DESC,id DESC LIMIT 200", (strategy_id,))
                    books[key] = {**_performance_context(strategy_id, runs, candidates, positions, events),
                                  "outstanding_orders": outstanding_orders}
                    if strategy_id == ROTATION_STRATEGY_ID:
                        books[key]["rotation_policy_version"] = ROTATION_POLICY_VERSION
                        books[key]["rotation_limits"] = ROTATION_LIMITS
                execution_books = self._fetch(cursor, """SELECT DISTINCT strategy_id FROM martin_forward_runs
                    WHERE (position(%s in strategy_id)=1
                      AND summary_json->>'execution_book_version' IN (%s,%s,%s,%s))
                       OR (position(%s in strategy_id)=1
                      AND summary_json->>'execution_book_version'=%s)
                    ORDER BY strategy_id""", ("martin_range_bar_retest_", "martin_bar_retest_execution_v1", "martin_bar_retest_execution_v2", "martin_bar_retest_execution_v3", "martin_bar_retest_execution_v4",
                    "rs_leader_30m_pivot_replay_", "rs_leader_30m_pivot_execution_v1"))
                for item in execution_books:
                    strategy_id = str(item["strategy_id"])
                    runs = self._fetch(cursor, "SELECT * FROM martin_forward_runs WHERE strategy_id=%s ORDER BY session_date DESC,id DESC LIMIT 200", (strategy_id,))
                    latest_id = runs[0]["id"] if runs else None
                    candidates = self._fetch(cursor, "SELECT * FROM martin_forward_candidates WHERE run_id=%s ORDER BY rank", (latest_id,)) if latest_id else []
                    outstanding_orders = self._fetch(cursor, """SELECT c.id,c.run_id,c.ticker,c.rank,c.status,c.reasons_json,c.payload_json,r.session_date
                        FROM martin_forward_candidates c JOIN martin_forward_runs r ON r.id=c.run_id
                        WHERE c.status='pending' AND r.strategy_id=%s ORDER BY r.session_date,c.rank,c.id""", (strategy_id,))
                    positions = self._fetch(cursor, "SELECT * FROM martin_forward_positions WHERE strategy_id=%s ORDER BY opened_on DESC,id DESC", (strategy_id,))
                    events = self._fetch(cursor, "SELECT * FROM martin_forward_events WHERE strategy_id=%s ORDER BY event_date DESC,id DESC LIMIT 200", (strategy_id,))
                    book = _performance_context(strategy_id, runs, candidates, positions, events)
                    book["outstanding_orders"] = outstanding_orders
                    book["execution_book_version"] = ((runs[0].get("summary_json") or {}).get("execution_book_version")
                        if runs else "martin_bar_retest_execution_v1")
                    books[strategy_id] = book
                account = books.get("account")
                for account in (account, books.get("rs_expanded_rotation"), books.get("gap_retest"),
                                books.get("gap_retest_replay"), books.get("gap_retest_replay_v2"),
                                books.get("gap_retest_replay_v3"), books.get("gap_retest_replay_v4"),
                                *[book for key, book in books.items() if key.startswith(("martin_range_bar_retest_", "rs_leader_30m_pivot_replay_"))]):
                    if not account or not account.get("history"):
                        continue
                    dates = [str(point.get("date") or "") for point in account["history"] if point.get("date")]
                    start, end = min(dates), max(dates)
                    latest_run_by_date = {}
                    for run in reversed(account.get("runs", [])):
                        latest_run_by_date[str(run["session_date"])] = run
                    partial_dates = {day for day, run in latest_run_by_date.items() if (run.get("summary_json") or {}).get("partial")}
                    # Intraday fallback is provisional evidence, but it does not
                    # invalidate an otherwise persisted daily equity mark.
                    provisional_dates = _provisional_fallback_dates(
                        latest_run_by_date, account.get("execution_book_version"))
                    try:
                        with connection.transaction():
                            price_rows = self._fetch(cursor, """SELECT ticker,trade_date,close FROM daily_bars
                                WHERE ticker IN ('SPY','QQQ') AND trade_date >= %s AND trade_date <= %s
                                ORDER BY ticker,trade_date""", (start, end))
                        if account.get("history_source") != "stored_session_marks":
                            account["benchmark_comparison"] = {"status": "unavailable", "reason": "Stored daily account equity marks are unavailable.", "basis": "price_return_close"}
                        else:
                            account["benchmark_comparison"] = _benchmark_comparison(
                                account["history"], price_rows, partial_dates - provisional_dates,
                                provisional_dates=provisional_dates)
                    except Exception:
                        account["benchmark_comparison"] = {"status": "unavailable", "reason": "Stored benchmark bars could not be read from the database.", "basis": "price_return_close"}
        return {"configured": True, "books": books, "comparison": _comparison_series(books)}

    def gap_retest_context(self) -> dict[str, Any]:
        context = self.context()
        return context.get("books", {}).get("gap_retest") or {"history": [], "runs": [], "candidates": [], "positions": [], "events": [], "outstanding_orders": []}

    def gap_retest_decision_snapshot(self, session_date: Any) -> dict[str, Any]:
        rows = self._rows("""SELECT r.id,r.session_date,c.ticker,c.status,c.payload_json
            FROM martin_forward_runs r LEFT JOIN martin_forward_candidates c ON c.run_id=r.id
            WHERE r.strategy_id=%s AND r.session_date=%s ORDER BY c.rank,c.ticker""", (GAP_RETEST_STRATEGY_ID, session_date))
        return {"decision_id": rows[0]["id"] if rows else None,
                "decisions": {str(row["ticker"]).upper(): {**(row.get("payload_json") or {}), "status": row.get("status")}
                              for row in rows if row.get("ticker")}}

    def daily_bar_provenance(self, ticker: str, start_date: Any, end_date: Any) -> list[dict[str, Any]]:
        return self._rows("""SELECT source,split_factor,COUNT(*) AS rows,MIN(trade_date) AS first_date,MAX(trade_date) AS last_date
            FROM daily_bars WHERE ticker=%s AND trade_date BETWEEN %s AND %s
            GROUP BY source,split_factor ORDER BY source,split_factor""", (ticker.upper(), start_date, end_date))

    def daily_bar_replay_provenance(self, ticker: str, start_date: Any, end_date: Any) -> list[dict[str, Any]]:
        return self._rows("""SELECT source,split_factor,trade_date,1 AS rows,trade_date AS first_date,trade_date AS last_date,
                (SELECT MAX(trade_date) FROM daily_bars WHERE ticker=%s AND trade_date<=%s
                 AND split_factor IS NOT NULL AND split_factor<>1) AS latest_split_date,
                (SELECT MIN(trade_date) FROM daily_bars WHERE ticker=%s AND trade_date<=%s) AS history_first_date,
                (SELECT COUNT(*) FROM daily_bars WHERE ticker=%s AND trade_date<=%s
                 AND (split_factor IS NULL OR split_factor<=0)) AS unknown_split_rows,
                (SELECT COUNT(*) FROM daily_bars WHERE ticker=%s AND trade_date<=%s
                 AND (source IS NULL OR source NOT IN ('tradingview','tradingview_screener','massive','massive_api',
                     'yahoo','yahoo_finance','yfinance','yfinance+nasdaq','yfinance_historical_repair'))) AS unknown_source_rows
            FROM daily_bars WHERE ticker=%s AND trade_date BETWEEN %s AND %s
            ORDER BY trade_date""", (ticker.upper(), end_date, ticker.upper(), end_date, ticker.upper(), end_date,
                                      ticker.upper(), end_date, ticker.upper(), start_date, end_date))

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

def _performance_context(strategy_id: str, runs: list[dict[str, Any]], candidates: list[dict[str, Any]],
                         positions: list[dict[str, Any]], events: list[dict[str, Any]]) -> dict[str, Any]:
    account_sized = strategy_id in ACCOUNT_STRATEGIES or strategy_id.startswith(("martin_range_bar_retest_", "rs_leader_30m_pivot_replay_"))
    closed = [row for row in positions if row.get("status") == "closed"]
    grouped: dict[str, dict[str, float]] = {}
    for row in closed:
        payload = row.get("payload_json") or {}
        day = str(row.get("closed_on") or payload.get("closed_on") or "")
        if not day:
            continue
        item = grouped.setdefault(day, {"realized_r": 0.0, "realized_pnl": 0.0})
        item["realized_r"] += float(payload.get("realized_r") or 0)
        item["realized_pnl"] += float(payload.get("realized_pnl") or 0)
    history, cumulative_r, cumulative_pnl, peak_r, peak_equity, max_r = [], 0.0, 0.0, 0.0, 100_000.0, 0.0
    for day in sorted(grouped):
        cumulative_r += grouped[day]["realized_r"]
        cumulative_pnl += grouped[day]["realized_pnl"]
        equity = 100_000.0 + cumulative_pnl
        peak_r = max(peak_r, cumulative_r)
        peak_equity = max(peak_equity, equity)
        max_r = min(max_r, cumulative_r - peak_r)
        history.append({"date": day, "cumulative_r": cumulative_r, "account_equity": equity if account_sized else None,
                        "equity_drawdown_pct": (equity / peak_equity - 1) * 100 if peak_equity and account_sized else None,
                        "r_drawdown": cumulative_r - peak_r})
    realized = [float((row.get("payload_json") or {}).get("realized_pnl") or 0) for row in closed]
    realized_positions = [row.get("payload_json") or {} for row in positions]
    realized_total_r = sum(float(row.get("realized_r") or 0) for row in realized_positions)
    realized_total_pnl = sum(float(row.get("realized_pnl") or 0) for row in realized_positions)
    attribution: dict[str, dict[str, dict[str, float]]] = {}
    for row in closed:
        payload = row.get("payload_json") or {}
        dimensions = (("sector", payload.get("sector") or "Unknown"),
                                 ("quadrant", payload.get("sector_quadrant") or "Unknown"),
                                 ("selection_tier", payload.get("selection_tier") or "Unknown"),
                                 ("adr_suitability", payload.get("adr_suitability") or "Unknown"),
                                 ("exit_reason", payload.get("exit_reason") or payload.get("exit_type") or "Unknown"))
        if strategy_id.startswith(("martin_range_bar_retest_", "rs_leader_30m_pivot_replay_")):
            dimensions += (("discovery_category", payload.get("event_category") or payload.get("category") or "Unknown"),)
        for dimension, value in dimensions:
            stats = attribution.setdefault(dimension, {}).setdefault(str(value), {"count": 0, "realized_r": 0.0, "realized_pnl": 0.0})
            stats["count"] += 1
            stats["realized_r"] += float(payload.get("realized_r") or 0)
            stats["realized_pnl"] += float(payload.get("realized_pnl") or 0)
    marks = []
    for run in reversed(runs):
        summary = run.get("summary_json") or {}
        if summary.get("cumulative_r") is not None or (strategy_id.startswith(("martin_range_bar_retest_", "rs_leader_30m_pivot_replay_")) and summary.get("mark_date")):
            equity = summary.get("account_equity")
            marks.append({"date": str(run["session_date"]), "cumulative_r": summary.get("cumulative_r"),
                          "account_equity": equity,
                          "equity_drawdown_pct": summary.get("equity_drawdown_pct"), "r_drawdown": summary.get("r_drawdown")})
    has_account_marks = account_sized and bool(marks) and any(point.get("account_equity") is not None for point in marks)
    execution_book = strategy_id.startswith(("martin_range_bar_retest_", "rs_leader_30m_pivot_replay_"))
    has_execution_marks = execution_book and any((run.get("summary_json") or {}).get("mark_date") for run in runs)
    if marks:
        history = marks
    elif execution_book:
        history = []
    latest_mark = marks[-1] if marks else {}
    latest_summary = (runs[0].get("summary_json") or {}) if runs else {}
    return {"strategy_id": strategy_id, "runs": runs, "candidates": candidates, "positions": positions,
            "events": events, "history": history, "history_source": "stored_session_marks" if has_account_marks else "stored_session_marks_partial" if has_execution_marks else "unavailable_legacy_no_daily_marks" if execution_book else "realized_close_fallback", "attribution": attribution,
            **({"rs_tier_metrics": _rs_tier_metrics(closed),
                "cost_model": "10 bps adverse slippage on stop and market-open exits; no commissions. Cohort drawdown is closed-trade R, not portfolio drawdown."}
               if strategy_id in {"martin_rs_expanded_v1", ROTATION_STRATEGY_ID} else {}),
            "metrics": {"realized_r": latest_summary.get("realized_r", sum(float(row.get("realized_r") or 0) for row in realized_positions)),
                        "unrealized_r": latest_summary.get("unrealized_r", None if execution_book and not has_account_marks else 0.0),
                        "cumulative_r": latest_mark.get("cumulative_r", None if execution_book else realized_total_r),
                        "realized_pnl": latest_summary.get("realized_pnl", realized_total_pnl),
                        "unrealized_pnl": latest_summary.get("unrealized_pnl", None if execution_book and not has_account_marks else 0.0),
                        "closed_count": len(closed), "gross_wins": sum(x for x in realized if x > 0),
                        "gross_losses": sum(abs(x) for x in realized if x < 0), "max_r_drawdown": None if execution_book and not has_account_marks else min([max_r, *(point.get("r_drawdown", 0) for point in history)]),
                        "account_equity": latest_mark.get("account_equity", 100_000.0 + realized_total_pnl) if account_sized and (has_account_marks or not execution_book) else None,
                        "equity_drawdown_pct": latest_mark.get("equity_drawdown_pct") if account_sized else None,
                        "mark_coverage": latest_summary.get("mark_coverage") if execution_book else None,
                        **_closed_trade_metrics([row.get("payload_json") or {} for row in closed])}}


def _rs_tier_metrics(closed: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    result = {}
    for tier in ("preferred", "emerging"):
        rows = [row for row in closed if (row.get("payload_json") or {}).get("rs_tier") == tier]
        payloads = [row.get("payload_json") or {} for row in rows]
        by_date: dict[str, float] = {}
        for row, payload in zip(rows, payloads):
            day = str(row.get("closed_on") or payload.get("closed_on") or "")
            by_date[day] = by_date.get(day, 0) + float(payload.get("realized_r") or 0)
        total = peak = drawdown = 0.0
        for day in sorted(by_date):
            total += by_date[day]
            peak = max(peak, total)
            drawdown = min(drawdown, total - peak)
        result[tier] = {"closed_count": len(rows), "realized_r": total,
                        "realized_pnl": sum(float(row.get("realized_pnl") or 0) for row in payloads),
                        **_closed_trade_metrics(payloads), "max_closed_drawdown_r": drawdown,
                        "stop_out_rate_pct": sum((row.get("exit_type") or row.get("exit_reason")) in {"stop", "gap_stop"} for row in payloads) / len(rows) * 100 if rows else None}
    return result


def _closed_trade_metrics(closed: list[dict[str, Any]]) -> dict[str, float | None]:
    trade_rs = [float(row.get("realized_r") or 0) for row in closed]
    winners_r = sum(value for value in trade_rs if value > 0)
    losers_r = sum(value for value in trade_rs if value < 0)
    return {"expectancy_r": sum(trade_rs) / len(trade_rs) if trade_rs else None,
            "win_rate_pct": sum(value > 0 for value in trade_rs) / len(trade_rs) * 100 if trade_rs else None,
            "profit_factor_r": winners_r / abs(losers_r) if losers_r else None,
            "average_holding_sessions": sum(float(row.get("holding_sessions") or 0) for row in closed) / len(closed) if closed else None}


def _comparison_series(books: dict[str, dict[str, Any]]) -> dict[str, Any]:
    return {key: {"history": value.get("history", []), "attribution": value.get("attribution", {}),
                  "cumulative_r": [{"date": point["date"], "value": point["cumulative_r"]} for point in value.get("history", [])],
                  "account_equity": [{"date": point["date"], "value": point["account_equity"]} for point in value.get("history", []) if point.get("account_equity") is not None],
                  "drawdowns": [{"date": point["date"], "equity_pct": point.get("equity_drawdown_pct"), "r": point.get("r_drawdown")} for point in value.get("history", [])]}
            for key, value in books.items()}


def _benchmark_comparison(history: list[dict[str, Any]], price_rows: list[dict[str, Any]],
                          partial_dates: set[str] | None = None,
                          provisional_dates: set[str] | None = None,
                          include_strict: bool = True,
                          excluded_dates: set[str] | None = None) -> dict[str, Any]:
    """Align account equity and stored ETF closes without filling missing sessions."""
    import math

    def positive(value: Any) -> float | None:
        try:
            result = float(value)
            return result if math.isfinite(result) and result > 0 else None
        except (TypeError, ValueError):
            return None

    by_date: dict[str, dict[str, float]] = {}
    for row in price_rows:
        ticker, day, close = str(row.get("ticker") or "").upper(), str(row.get("trade_date") or "")[:10], positive(row.get("close"))
        if ticker in {"SPY", "QQQ"} and day and close is not None:
            by_date.setdefault(day, {})[ticker] = close
    account_by_date = {str(point.get("date")): positive(point.get("account_equity")) for point in history if point.get("date")}
    account_dates = sorted(account_by_date)
    if not account_dates:
        return {"status": "unavailable", "reason": "Account history is unavailable.", "basis": "price_return_close"}
    excluded = excluded_dates or set()
    eligible_account_dates = [day for day in account_dates if day not in excluded]
    if not eligible_account_dates:
        return {"status": "unavailable", "reason": "No strict account sessions remain after excluding provisional dates.", "basis": "price_return_close"}
    baseline = eligible_account_dates[0]
    dates = sorted((set(account_dates) | {day for day in by_date if baseline <= day <= account_dates[-1]}) - excluded)
    partial = partial_dates or set()
    provisional = provisional_dates or set()
    baseline_equity = account_by_date.get(baseline)
    baseline_prices = by_date.get(baseline, {})
    missing = [ticker for ticker in ("SPY", "QQQ") if ticker not in baseline_prices]
    if baseline in partial:
        missing.append("Account session is partial")
    if baseline_equity is None:
        missing.append("Account equity is missing or non-positive")
    if missing:
        return {"status": "unavailable", "reason": f"Common baseline {baseline} is unavailable: {', '.join(missing)}.",
                "basis": "price_return_close", "baseline_date": baseline, "account_history_start": account_dates[0], "account_history_end": account_dates[-1],
                "coverage": {"expected_sessions": len(dates), "included_sessions": 0, "missing_dates": [baseline]}}
    base = {"Account": baseline_equity, "SPY": baseline_prices["SPY"], "QQQ": baseline_prices["QQQ"]}
    peaks = {name: 100.0 for name in base}
    series = {name: [] for name in base}
    missing_dates = []
    for day in dates:
        prices = by_date.get(day, {})
        values = {"Account": account_by_date.get(day), "SPY": prices.get("SPY"), "QQQ": prices.get("QQQ")}
        if day in partial:
            values["Account"] = None
        if any(value is None or positive(value) is None for value in values.values()):
            missing_dates.append(day)
            for name in values:
                series[name].append({"date": day, "value": None, "return_pct": None, "drawdown_pct": None})
            continue
        for name, value in values.items():
            rebased = value / base[name] * 100
            peaks[name] = max(peaks[name], rebased)
            series[name].append({"date": day, "value": rebased, "return_pct": rebased - 100, "drawdown_pct": (rebased / peaks[name] - 1) * 100})
    common_dates = [point["date"] for point in series["Account"] if point["value"] is not None]
    result = {"status": "available", "basis": "price_return_close", "baseline_date": baseline,
            "account_history_start": account_dates[0], "account_history_end": account_dates[-1], "included_sessions": len(dates) - len(missing_dates),
            "expected_sessions": len(dates), "missing_dates": missing_dates,
            "provisional_dates": sorted(day for day in provisional if day in dates and day not in missing_dates),
            "comparison_date": common_dates[-1] if common_dates else baseline, "series": series}
    if include_strict and provisional:
        strict_history = [point for point in history if str(point.get("date") or "") not in provisional]
        result["strict_comparison"] = _benchmark_comparison(
            strict_history, price_rows, partial - provisional, include_strict=False,
            excluded_dates=provisional)
    return result


def _provisional_fallback_dates(latest_runs: dict[str, dict[str, Any]], execution_book_version: str | None) -> set[str]:
    """Find v3/v4 runs partial only because execution used daily fallback."""
    if execution_book_version not in {"martin_bar_retest_execution_v3", "martin_bar_retest_execution_v4"}:
        return set()
    provisional: set[str] = set()
    for day, run in latest_runs.items():
        summary = run.get("summary_json") or {}
        actions = summary.get("execution_actions") or {}
        fallback_count = summary.get("intraday_fallback_count", actions.get("intraday_fallbacks", 0))
        missing_bars = actions.get("missing_bars", summary.get("missing_bars", 0))
        try:
            fallback_count, missing_bars = int(fallback_count or 0), int(missing_bars or 0)
        except (TypeError, ValueError):
            continue
        if summary.get("partial") and missing_bars == 0 and fallback_count > 0:
            provisional.add(day)
    return provisional
