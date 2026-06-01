from __future__ import annotations

from pathlib import Path
from typing import Any

from src.market_data_access import resolve_database_url


class AuditRepository:
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

    def create_event(
        self,
        *,
        actor_user_id: int | None,
        actor_email: str,
        actor_role: str,
        request_ip: str,
        request_user_agent: str,
        action: str,
        resource_type: str,
        resource_id: str,
        resource_label: str,
        status: str,
        message: str,
        metadata_json: dict[str, Any],
    ) -> dict[str, Any] | None:
        connection = self._connect()
        if connection is None:
            return None
        sql = """
            INSERT INTO app_audit_events (
              actor_user_id,
              actor_email,
              actor_role,
              request_ip,
              request_user_agent,
              action,
              resource_type,
              resource_id,
              resource_label,
              status,
              message,
              metadata_json
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb)
            RETURNING id, event_at, actor_user_id, actor_email, actor_role, request_ip, request_user_agent,
                      action, resource_type, resource_id, resource_label, status, message, metadata_json
        """
        import json

        with connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    sql,
                    (
                        actor_user_id,
                        actor_email or None,
                        actor_role or None,
                        request_ip or None,
                        request_user_agent or None,
                        action,
                        resource_type,
                        resource_id or None,
                        resource_label or None,
                        status,
                        message or None,
                        json.dumps(metadata_json or {}),
                    ),
                )
                rows = self._rows_to_dicts(cursor, cursor.fetchall())
            connection.commit()
        return rows[0] if rows else None

    def list_events(
        self,
        *,
        actor_email: str = "",
        action: str = "",
        resource_type: str = "",
        from_date: object | None = None,
        to_date: object | None = None,
        limit: int = 50,
        offset: int = 0,
    ) -> list[dict[str, Any]]:
        connection = self._connect()
        if connection is None:
            return []
        where_parts: list[str] = []
        params: list[object] = []
        if actor_email.strip():
            where_parts.append("LOWER(COALESCE(actor_email, '')) = %s")
            params.append(actor_email.strip().lower())
        if action.strip():
            where_parts.append("action = %s")
            params.append(action.strip())
        if resource_type.strip():
            where_parts.append("resource_type = %s")
            params.append(resource_type.strip())
        if from_date is not None:
            where_parts.append("event_at >= %s")
            params.append(from_date)
        if to_date is not None:
            where_parts.append("event_at < %s")
            params.append(to_date)
        where_sql = f"WHERE {' AND '.join(where_parts)}" if where_parts else ""
        sql = f"""
            SELECT id, event_at, actor_user_id, actor_email, actor_role, request_ip, request_user_agent,
                   action, resource_type, resource_id, resource_label, status, message, metadata_json
            FROM app_audit_events
            {where_sql}
            ORDER BY event_at DESC, id DESC
            LIMIT %s
            OFFSET %s
        """
        params.extend([limit, offset])
        with connection:
            with connection.cursor() as cursor:
                cursor.execute(sql, tuple(params))
                return self._rows_to_dicts(cursor, cursor.fetchall())
