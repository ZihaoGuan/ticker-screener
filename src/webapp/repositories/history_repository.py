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
        parent_job_run_id: int | None = None,
    ) -> int | None:
        connection = self._connect()
        if connection is None:
            return None
        sql = """
            INSERT INTO job_runs (parent_job_run_id, job_type, job_name, status, trigger_source, request_payload)
            VALUES (%s, %s, %s, %s, %s, %s::jsonb)
            RETURNING id
        """
        with connection:
            with connection.cursor() as cursor:
                cursor.execute(sql, (parent_job_run_id, job_type, job_name, status, trigger_source, json.dumps(request_payload)))
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

    def list_child_job_runs(self, parent_job_run_ids: list[int]) -> list[dict[str, Any]]:
        if not parent_job_run_ids:
            return []
        connection = self._connect()
        if connection is None:
            return []
        sql = """
            SELECT id, parent_job_run_id, job_type, job_name, status, trigger_source,
                   request_payload, result_payload, artifact_path, started_at, finished_at, created_at
            FROM job_runs
            WHERE parent_job_run_id = ANY(%s)
            ORDER BY started_at ASC NULLS LAST, id ASC
        """
        with connection:
            with connection.cursor() as cursor:
                cursor.execute(sql, (parent_job_run_ids,))
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

    def rewrite_screen_run_artifact_paths(self, path_map: dict[str, str]) -> int:
        if not path_map:
            return 0
        connection = self._connect()
        if connection is None:
            return 0
        updated = 0
        sql = """
            UPDATE screen_runs
            SET raw_artifact_path = CASE WHEN raw_artifact_path = %s THEN %s ELSE raw_artifact_path END,
                watchlist_artifact_path = CASE WHEN watchlist_artifact_path = %s THEN %s ELSE watchlist_artifact_path END
            WHERE raw_artifact_path = %s
               OR watchlist_artifact_path = %s
        """
        with connection:
            with connection.cursor() as cursor:
                for old_path, new_path in path_map.items():
                    cursor.execute(sql, (old_path, new_path, old_path, new_path, old_path, old_path))
                    updated += int(cursor.rowcount or 0)
            connection.commit()
        return updated

    def upsert_overlap_run(
        self,
        *,
        run_date: dt.date,
        strategy_set_key: str,
        strategy_ids: list[str],
        market_data_mode: str,
        candidate_threshold: int,
        source_job_run_id: int | None,
        artifact_path: str,
        summary_json: dict[str, Any],
    ) -> int | None:
        connection = self._connect()
        if connection is None:
            return None
        sql = """
            INSERT INTO overlap_runs (
                run_date,
                strategy_set_key,
                strategy_ids_json,
                market_data_mode,
                candidate_threshold,
                source_job_run_id,
                artifact_path,
                summary_json
            )
            VALUES (%s, %s, %s::jsonb, %s, %s, %s, %s, %s::jsonb)
            ON CONFLICT (run_date, strategy_set_key, candidate_threshold)
            DO UPDATE SET
                strategy_ids_json = EXCLUDED.strategy_ids_json,
                market_data_mode = EXCLUDED.market_data_mode,
                source_job_run_id = EXCLUDED.source_job_run_id,
                artifact_path = EXCLUDED.artifact_path,
                summary_json = EXCLUDED.summary_json
            RETURNING id
        """
        with connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    sql,
                    (
                        run_date,
                        strategy_set_key,
                        json.dumps(strategy_ids),
                        market_data_mode,
                        candidate_threshold,
                        source_job_run_id,
                        artifact_path,
                        json.dumps(summary_json),
                    ),
                )
                row = cursor.fetchone()
            connection.commit()
        return int(row[0]) if row else None

    def replace_overlap_run_members(self, overlap_run_id: int | None, rows: list[dict[str, Any]]) -> None:
        if overlap_run_id is None:
            return
        connection = self._connect()
        if connection is None:
            return
        delete_sql = "DELETE FROM overlap_run_members WHERE overlap_run_id = %s"
        insert_sql = """
            INSERT INTO overlap_run_members (
                overlap_run_id, run_date, ticker, signal_count, contributing_strategies_json, metadata_json
            )
            VALUES (%s, %s, %s, %s, %s::jsonb, %s::jsonb)
        """
        with connection:
            with connection.cursor() as cursor:
                cursor.execute(delete_sql, (overlap_run_id,))
                for row in rows:
                    cursor.execute(
                        insert_sql,
                        (
                            overlap_run_id,
                            row["run_date"],
                            row["ticker"],
                            row["signal_count"],
                            json.dumps(row.get("contributing_strategies_json") or []),
                            json.dumps(row.get("metadata_json") or {}),
                        ),
                    )
            connection.commit()

    def list_overlap_runs(
        self,
        *,
        strategy_set_key: str = "",
        start_date: dt.date | None = None,
        end_date: dt.date | None = None,
        candidate_threshold: int | None = None,
        limit: int = 200,
    ) -> list[dict[str, Any]]:
        connection = self._connect()
        if connection is None:
            return []
        where = ["1=1"]
        params: list[Any] = []
        if strategy_set_key:
            where.append("strategy_set_key = %s")
            params.append(strategy_set_key)
        if start_date is not None:
            where.append("run_date >= %s")
            params.append(start_date)
        if end_date is not None:
            where.append("run_date <= %s")
            params.append(end_date)
        if candidate_threshold is not None:
            where.append("candidate_threshold = %s")
            params.append(candidate_threshold)
        sql = f"""
            SELECT id, run_date, strategy_set_key, strategy_ids_json, market_data_mode,
                   candidate_threshold, source_job_run_id, artifact_path, summary_json, created_at
            FROM overlap_runs
            WHERE {" AND ".join(where)}
            ORDER BY run_date DESC, id DESC
            LIMIT %s
        """
        params.append(max(1, int(limit)))
        with connection:
            with connection.cursor() as cursor:
                cursor.execute(sql, tuple(params))
                return self._rows_to_dicts(cursor, cursor.fetchall())

    def list_overlap_run_members(
        self,
        *,
        overlap_run_id: int | None = None,
        start_date: dt.date | None = None,
        end_date: dt.date | None = None,
        min_signal_count: int | None = None,
        strategy_set_key: str = "",
    ) -> list[dict[str, Any]]:
        connection = self._connect()
        if connection is None:
            return []
        where = ["1=1"]
        params: list[Any] = []
        join = ""
        if overlap_run_id is not None:
            where.append("members.overlap_run_id = %s")
            params.append(overlap_run_id)
        if start_date is not None:
            where.append("members.run_date >= %s")
            params.append(start_date)
        if end_date is not None:
            where.append("members.run_date <= %s")
            params.append(end_date)
        if min_signal_count is not None:
            where.append("members.signal_count >= %s")
            params.append(min_signal_count)
        if strategy_set_key:
            join = "JOIN overlap_runs runs ON runs.id = members.overlap_run_id"
            where.append("runs.strategy_set_key = %s")
            params.append(strategy_set_key)
        sql = f"""
            SELECT members.id, members.overlap_run_id, members.run_date, members.ticker,
                   members.signal_count, members.contributing_strategies_json, members.metadata_json,
                   members.created_at
            FROM overlap_run_members members
            {join}
            WHERE {" AND ".join(where)}
            ORDER BY members.run_date DESC, members.signal_count DESC, members.ticker ASC
        """
        with connection:
            with connection.cursor() as cursor:
                cursor.execute(sql, tuple(params))
                return self._rows_to_dicts(cursor, cursor.fetchall())

    def create_backtest_run(
        self,
        *,
        strategy_id: str,
        strategy_set_key: str,
        strategy_ids: list[str],
        start_date: dt.date,
        end_date: dt.date,
        parameters: dict[str, Any],
        summary: dict[str, Any],
        job_run_id: int | None,
        artifact_path: str = "",
        html_report_path: str = "",
        json_report_path: str = "",
    ) -> int | None:
        connection = self._connect()
        if connection is None:
            return None
        sql = """
            INSERT INTO backtest_runs (
                strategy_id,
                strategy_set_key,
                strategy_ids_json,
                start_date,
                end_date,
                parameters,
                summary,
                html_report_path,
                json_report_path,
                job_run_id,
                hold_periods_json,
                entry_signal_threshold,
                artifact_path
            )
            VALUES (
                %s, %s, %s::jsonb, %s, %s, %s::jsonb, %s::jsonb, %s, %s, %s, %s::jsonb, %s, %s
            )
            RETURNING id
        """
        hold_periods = parameters.get("hold_periods") if isinstance(parameters.get("hold_periods"), list) else [5, 10]
        threshold = int(parameters.get("entry_signal_threshold") or 4)
        with connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    sql,
                    (
                        strategy_id,
                        strategy_set_key,
                        json.dumps(strategy_ids),
                        start_date,
                        end_date,
                        json.dumps(parameters),
                        json.dumps(summary),
                        html_report_path,
                        json_report_path,
                        job_run_id,
                        json.dumps(hold_periods),
                        threshold,
                        artifact_path,
                    ),
                )
                row = cursor.fetchone()
            connection.commit()
        return int(row[0]) if row else None

    def replace_backtest_run_trades(self, backtest_run_id: int | None, rows: list[dict[str, Any]]) -> None:
        if backtest_run_id is None:
            return
        connection = self._connect()
        if connection is None:
            return
        delete_sql = "DELETE FROM backtest_run_trades WHERE backtest_run_id = %s"
        insert_sql = """
            INSERT INTO backtest_run_trades (
                backtest_run_id,
                signal_date,
                ticker,
                signal_count,
                contributing_strategies_json,
                entry_date,
                entry_price,
                hold_results_json,
                metadata_json
            )
            VALUES (%s, %s, %s, %s, %s::jsonb, %s, %s, %s::jsonb, %s::jsonb)
        """
        with connection:
            with connection.cursor() as cursor:
                cursor.execute(delete_sql, (backtest_run_id,))
                for row in rows:
                    cursor.execute(
                        insert_sql,
                        (
                            backtest_run_id,
                            row["signal_date"],
                            row["ticker"],
                            row["signal_count"],
                            json.dumps(row.get("contributing_strategies_json") or []),
                            row["entry_date"],
                            row["entry_price"],
                            json.dumps(row.get("hold_results_json") or {}),
                            json.dumps(row.get("metadata_json") or {}),
                        ),
                    )
            connection.commit()

    def list_backtest_runs_v2(self, *, limit: int = 30) -> list[dict[str, Any]]:
        connection = self._connect()
        if connection is None:
            return []
        sql = """
            SELECT id, strategy_id, strategy_set_key, strategy_ids_json, start_date, end_date,
                   parameters, summary, html_report_path, json_report_path, job_run_id,
                   hold_periods_json, entry_signal_threshold, artifact_path, created_at
            FROM backtest_runs
            ORDER BY created_at DESC, id DESC
            LIMIT %s
        """
        with connection:
            with connection.cursor() as cursor:
                cursor.execute(sql, (max(1, int(limit)),))
                return self._rows_to_dicts(cursor, cursor.fetchall())

    def get_backtest_run_v2(self, run_id: int) -> dict[str, Any] | None:
        connection = self._connect()
        if connection is None:
            return None
        sql = """
            SELECT id, strategy_id, strategy_set_key, strategy_ids_json, start_date, end_date,
                   parameters, summary, html_report_path, json_report_path, job_run_id,
                   hold_periods_json, entry_signal_threshold, artifact_path, created_at
            FROM backtest_runs
            WHERE id = %s
        """
        trades_sql = """
            SELECT id, signal_date, ticker, signal_count, contributing_strategies_json,
                   entry_date, entry_price, hold_results_json, metadata_json, created_at
            FROM backtest_run_trades
            WHERE backtest_run_id = %s
            ORDER BY signal_date DESC, ticker ASC
        """
        with connection:
            with connection.cursor() as cursor:
                cursor.execute(sql, (run_id,))
                rows = self._rows_to_dicts(cursor, cursor.fetchall())
                if not rows:
                    return None
                payload = rows[0]
                cursor.execute(trades_sql, (run_id,))
                payload["trades"] = self._rows_to_dicts(cursor, cursor.fetchall())
        return payload
