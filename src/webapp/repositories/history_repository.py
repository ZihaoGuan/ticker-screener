from __future__ import annotations

import datetime as dt
import json
from pathlib import Path
from typing import Any

from src.market_data_access import resolve_database_url


class HistoryRepository:
    def __init__(self, database_url: str = "", artifacts_dir: Path | None = None) -> None:
        self.database_url = resolve_database_url(database_url)
        self.artifacts_dir = artifacts_dir
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

    def create_job_run(
        self,
        *,
        job_type: str,
        job_name: str,
        status: str,
        trigger_source: str,
        request_payload: dict[str, Any],
    ) -> int | None:
        connection = self._connect()
        if connection is None:
            return None
        sql = """
            INSERT INTO job_runs (job_type, job_name, status, trigger_source, request_payload)
            VALUES (%s, %s, %s, %s, %s::jsonb)
            RETURNING id
        """
        with connection:
            with connection.cursor() as cursor:
                cursor.execute(sql, (job_type, job_name, status, trigger_source, json.dumps(request_payload)))
                row = cursor.fetchone()
            connection.commit()
        return int(row[0]) if row else None

    def update_job_run(
        self,
        job_run_id: int | None,
        *,
        status: str,
        result_payload: dict[str, Any] | None = None,
        artifact_path: str | None = None,
        finished_at: str | None = None,
    ) -> None:
        if job_run_id is None:
            return
        connection = self._connect()
        if connection is None:
            return
        sql = """
            UPDATE job_runs
            SET status = %s,
                result_payload = COALESCE(%s::jsonb, result_payload),
                artifact_path = COALESCE(%s, artifact_path),
                finished_at = COALESCE(%s::timestamptz, finished_at)
            WHERE id = %s
        """
        with connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    sql,
                    (
                        status,
                        json.dumps(result_payload) if result_payload is not None else None,
                        artifact_path,
                        finished_at,
                        job_run_id,
                    ),
                )
            connection.commit()

    def upsert_screen_run(
        self,
        *,
        strategy_id: str,
        run_date: dt.date,
        job_run_id: int | None,
        config_json: dict[str, Any],
        config_hash: str,
        scope_json: dict[str, Any],
        scope_hash: str,
        market_data_mode: str,
        source_kind: str,
        hit_count: int,
        failure_count: int,
        result_summary_json: dict[str, Any],
        raw_artifact_path: str,
        watchlist_artifact_path: str,
        report_artifact_path: str = "",
        notes: str = "",
    ) -> int | None:
        connection = self._connect()
        if connection is None:
            return None
        sql = """
            INSERT INTO screen_runs (
                strategy_id,
                run_date,
                job_run_id,
                config_json,
                config_hash,
                scope_json,
                scope_hash,
                market_data_mode,
                source_kind,
                hit_count,
                failure_count,
                result_summary_json,
                raw_artifact_path,
                watchlist_artifact_path,
                report_artifact_path,
                notes
            )
            VALUES (
                %s, %s, %s, %s::jsonb, %s, %s::jsonb, %s, %s, %s,
                %s, %s, %s::jsonb, %s, %s, %s, %s
            )
            ON CONFLICT (strategy_id, run_date, config_hash, scope_hash)
            DO UPDATE SET
                job_run_id = EXCLUDED.job_run_id,
                market_data_mode = EXCLUDED.market_data_mode,
                source_kind = EXCLUDED.source_kind,
                hit_count = EXCLUDED.hit_count,
                failure_count = EXCLUDED.failure_count,
                result_summary_json = EXCLUDED.result_summary_json,
                raw_artifact_path = EXCLUDED.raw_artifact_path,
                watchlist_artifact_path = EXCLUDED.watchlist_artifact_path,
                report_artifact_path = EXCLUDED.report_artifact_path,
                notes = EXCLUDED.notes,
                deleted_at = NULL,
                deleted_reason = NULL
            RETURNING id
        """
        with connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    sql,
                    (
                        strategy_id,
                        run_date,
                        job_run_id,
                        json.dumps(config_json),
                        config_hash,
                        json.dumps(scope_json),
                        scope_hash,
                        market_data_mode,
                        source_kind,
                        hit_count,
                        failure_count,
                        json.dumps(result_summary_json),
                        raw_artifact_path,
                        watchlist_artifact_path,
                        report_artifact_path,
                        notes,
                    ),
                )
                row = cursor.fetchone()
            connection.commit()
        return int(row[0]) if row else None

    def replace_screen_run_hits(self, screen_run_id: int | None, rows: list[dict[str, Any]]) -> None:
        if screen_run_id is None:
            return
        connection = self._connect()
        if connection is None:
            return
        delete_sql = "DELETE FROM screen_run_hits WHERE screen_run_id = %s"
        insert_sql = """
            INSERT INTO screen_run_hits (
                screen_run_id, strategy_id, signal_date, ticker, passed, rank,
                metrics_json, reasons_json, hit_payload_json
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s::jsonb, %s::jsonb, %s::jsonb)
        """
        with connection:
            with connection.cursor() as cursor:
                cursor.execute(delete_sql, (screen_run_id,))
                for row in rows:
                    cursor.execute(
                        insert_sql,
                        (
                            screen_run_id,
                            row["strategy_id"],
                            row["signal_date"],
                            row["ticker"],
                            bool(row.get("passed")),
                            row.get("rank"),
                            json.dumps(row.get("metrics_json") or {}),
                            json.dumps(row.get("reasons_json") or []),
                            json.dumps(row.get("hit_payload_json") or {}),
                        ),
                    )
            connection.commit()

    def list_screen_runs(
        self,
        *,
        strategy_id: str = "",
        start_date: dt.date | None = None,
        end_date: dt.date | None = None,
        include_deleted: bool = False,
        config_hash: str = "",
        has_hits: bool | None = None,
        limit: int = 50,
        offset: int = 0,
    ) -> list[dict[str, Any]]:
        connection = self._connect()
        if connection is None:
            return []
        where = ["1=1"]
        params: list[Any] = []
        if strategy_id:
            where.append("strategy_id = %s")
            params.append(strategy_id)
        if start_date is not None:
            where.append("run_date >= %s")
            params.append(start_date)
        if end_date is not None:
            where.append("run_date <= %s")
            params.append(end_date)
        if not include_deleted:
            where.append("deleted_at IS NULL")
        if config_hash:
            where.append("config_hash = %s")
            params.append(config_hash)
        if has_hits is True:
            where.append("hit_count > 0")
        elif has_hits is False:
            where.append("hit_count = 0")
        sql = f"""
            SELECT id, strategy_id, run_date, config_hash, market_data_mode, source_kind,
                   hit_count, failure_count, raw_artifact_path, watchlist_artifact_path,
                   report_artifact_path, result_summary_json, deleted_at, deleted_reason,
                   created_at
            FROM screen_runs
            WHERE {" AND ".join(where)}
            ORDER BY run_date DESC, id DESC
            LIMIT %s OFFSET %s
        """
        params.extend([max(1, int(limit)), max(0, int(offset))])
        with connection:
            with connection.cursor() as cursor:
                cursor.execute(sql, tuple(params))
                rows = self._rows_to_dicts(cursor, cursor.fetchall())
        return rows

    def get_screen_run(self, run_id: int, *, include_hits: bool = False, hit_limit: int = 200, hit_offset: int = 0) -> dict[str, Any] | None:
        connection = self._connect()
        if connection is None:
            return None
        sql = """
            SELECT id, strategy_id, run_date, job_run_id, config_json, config_hash, scope_json, scope_hash,
                   market_data_mode, source_kind, hit_count, failure_count, result_summary_json,
                   raw_artifact_path, watchlist_artifact_path, report_artifact_path, notes,
                   deleted_at, deleted_reason, created_at
            FROM screen_runs
            WHERE id = %s
        """
        with connection:
            with connection.cursor() as cursor:
                cursor.execute(sql, (run_id,))
                rows = self._rows_to_dicts(cursor, cursor.fetchall())
                if not rows:
                    return None
                payload = rows[0]
                if include_hits:
                    cursor.execute(
                        """
                        SELECT id, strategy_id, signal_date, ticker, passed, rank,
                               metrics_json, reasons_json, hit_payload_json, created_at
                        FROM screen_run_hits
                        WHERE screen_run_id = %s
                        ORDER BY signal_date DESC, rank NULLS LAST, ticker ASC
                        LIMIT %s OFFSET %s
                        """,
                        (run_id, max(1, int(hit_limit)), max(0, int(hit_offset))),
                    )
                    payload["hits"] = self._rows_to_dicts(cursor, cursor.fetchall())
        return payload

    def soft_delete_screen_run(self, run_id: int, *, reason: str) -> bool:
        connection = self._connect()
        if connection is None:
            return False
        sql = """
            UPDATE screen_runs
            SET deleted_at = NOW(), deleted_reason = %s
            WHERE id = %s
              AND deleted_at IS NULL
        """
        with connection:
            with connection.cursor() as cursor:
                cursor.execute(sql, (reason, run_id))
                updated = cursor.rowcount
            connection.commit()
        return bool(updated)

    def list_signal_cache_summary(
        self,
        *,
        strategy_ids: list[str] | None = None,
        start_date: dt.date | None = None,
        end_date: dt.date | None = None,
    ) -> list[dict[str, Any]]:
        connection = self._connect()
        if connection is None:
            return []
        where = ["deleted_at IS NULL"]
        params: list[Any] = []
        if strategy_ids:
            where.append("strategy_id = ANY(%s)")
            params.append(strategy_ids)
        if start_date is not None:
            where.append("run_date >= %s")
            params.append(start_date)
        if end_date is not None:
            where.append("run_date <= %s")
            params.append(end_date)
        sql = f"""
            SELECT strategy_id,
                   COUNT(*) AS run_count,
                   COUNT(*) FILTER (WHERE hit_count > 0) AS run_with_hits_count,
                   MIN(run_date) AS first_run_date,
                   MAX(run_date) AS last_run_date
            FROM screen_runs
            WHERE {" AND ".join(where)}
            GROUP BY strategy_id
            ORDER BY strategy_id ASC
        """
        with connection:
            with connection.cursor() as cursor:
                cursor.execute(sql, tuple(params))
                return self._rows_to_dicts(cursor, cursor.fetchall())

    def list_signal_cache_calendar(
        self,
        *,
        strategy_ids: list[str] | None = None,
        start_date: dt.date,
        end_date: dt.date,
        include_deleted: bool = False,
    ) -> list[dict[str, Any]]:
        connection = self._connect()
        if connection is None:
            return []
        where = ["run_date >= %s", "run_date <= %s"]
        params: list[Any] = [start_date, end_date]
        if strategy_ids:
            where.append("strategy_id = ANY(%s)")
            params.append(strategy_ids)
        if not include_deleted:
            where.append("deleted_at IS NULL")
        sql = f"""
            SELECT id, strategy_id, run_date, market_data_mode, source_kind,
                   hit_count, failure_count, deleted_at, deleted_reason, created_at
            FROM screen_runs
            WHERE {" AND ".join(where)}
            ORDER BY run_date ASC, strategy_id ASC, created_at DESC, id DESC
        """
        with connection:
            with connection.cursor() as cursor:
                cursor.execute(sql, tuple(params))
                return self._rows_to_dicts(cursor, cursor.fetchall())

    def load_cached_signals(
        self,
        *,
        screener_ids: list[str],
        start_date: dt.date,
        end_date: dt.date,
        include_deleted: bool = False,
    ) -> list[dict[str, Any]]:
        connection = self._connect()
        if connection is None or not screener_ids:
            return []
        sql = """
            SELECT hits.screen_run_id, hits.strategy_id, hits.signal_date, hits.ticker, hits.passed,
                   hits.rank, hits.metrics_json, hits.reasons_json, hits.hit_payload_json
            FROM screen_run_hits hits
            JOIN screen_runs runs ON runs.id = hits.screen_run_id
            WHERE hits.strategy_id = ANY(%s)
              AND hits.signal_date >= %s
              AND hits.signal_date <= %s
              AND hits.passed = TRUE
        """
        params: list[Any] = [screener_ids, start_date, end_date]
        if not include_deleted:
            sql += " AND runs.deleted_at IS NULL"
        sql += " ORDER BY hits.signal_date ASC, hits.ticker ASC, hits.strategy_id ASC"
        with connection:
            with connection.cursor() as cursor:
                cursor.execute(sql, tuple(params))
                return self._rows_to_dicts(cursor, cursor.fetchall())

    def list_backtest_runs(self, *, limit: int = 20) -> list[dict[str, Any]]:
        connection = self._connect()
        if connection is None:
            return []
        sql = """
            SELECT id, strategy_id, start_date, end_date, parameters, summary,
                   html_report_path, json_report_path, job_run_id, created_at
            FROM backtest_runs
            ORDER BY created_at DESC, id DESC
            LIMIT %s
        """
        with connection:
            with connection.cursor() as cursor:
                cursor.execute(sql, (max(1, int(limit)),))
                return self._rows_to_dicts(cursor, cursor.fetchall())

    def create_backtest_run(
        self,
        *,
        strategy_id: str,
        start_date: dt.date,
        end_date: dt.date,
        parameters: dict[str, Any],
        summary: dict[str, Any],
        html_report_path: str,
        json_report_path: str,
        job_run_id: int | None,
    ) -> int | None:
        connection = self._connect()
        if connection is None:
            return None
        sql = """
            INSERT INTO backtest_runs (
                strategy_id, start_date, end_date, parameters, summary,
                html_report_path, json_report_path, job_run_id
            )
            VALUES (%s, %s, %s, %s::jsonb, %s::jsonb, %s, %s, %s)
            RETURNING id
        """
        with connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    sql,
                    (
                        strategy_id,
                        start_date,
                        end_date,
                        json.dumps(parameters),
                        json.dumps(summary),
                        html_report_path,
                        json_report_path,
                        job_run_id,
                    ),
                )
                row = cursor.fetchone()
            connection.commit()
        return int(row[0]) if row else None
