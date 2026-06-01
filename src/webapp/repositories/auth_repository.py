from __future__ import annotations

import datetime as dt
from pathlib import Path
from typing import Any

from src.market_data_access import resolve_database_url


class AuthRepository:
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

    def upsert_user(self, *, email: str, role: str, is_active: bool = True) -> dict[str, Any] | None:
        connection = self._connect()
        if connection is None:
            return None
        sql = """
            INSERT INTO app_users (email, role, is_active)
            VALUES (%s, %s, %s)
            ON CONFLICT (email)
            DO UPDATE SET
                role = EXCLUDED.role,
                is_active = EXCLUDED.is_active,
                updated_at = NOW()
            RETURNING id, email, role, is_active, created_at, updated_at, last_login_at
        """
        with connection:
            with connection.cursor() as cursor:
                cursor.execute(sql, (email.lower(), role, is_active))
                rows = self._rows_to_dicts(cursor, cursor.fetchall())
            connection.commit()
        return rows[0] if rows else None

    def get_user_by_email(self, email: str) -> dict[str, Any] | None:
        return self._fetch_one(
            """
            SELECT id, email, role, is_active, created_at, updated_at, last_login_at
            FROM app_users
            WHERE email = %s
            """,
            (email.lower(),),
        )

    def get_user_by_id(self, user_id: int) -> dict[str, Any] | None:
        return self._fetch_one(
            """
            SELECT id, email, role, is_active, created_at, updated_at, last_login_at
            FROM app_users
            WHERE id = %s
            """,
            (user_id,),
        )

    def list_users(self) -> list[dict[str, Any]]:
        connection = self._connect()
        if connection is None:
            return []
        sql = """
            SELECT id, email, role, is_active, created_at, updated_at, last_login_at
            FROM app_users
            ORDER BY email ASC
        """
        with connection:
            with connection.cursor() as cursor:
                cursor.execute(sql)
                return self._rows_to_dicts(cursor, cursor.fetchall())

    def update_user_role(self, *, user_id: int, role: str) -> dict[str, Any] | None:
        return self._update_user(user_id=user_id, field_sql="role = %s", value=role)

    def update_user_active(self, *, user_id: int, is_active: bool) -> dict[str, Any] | None:
        return self._update_user(user_id=user_id, field_sql="is_active = %s", value=is_active)

    def create_magic_link(
        self,
        *,
        user_id: int,
        token_hash: str,
        expires_at: dt.datetime,
        request_ip: str,
        request_user_agent: str,
    ) -> int | None:
        connection = self._connect()
        if connection is None:
            return None
        sql = """
            INSERT INTO app_magic_links (user_id, token_hash, expires_at, request_ip, request_user_agent)
            VALUES (%s, %s, %s, %s, %s)
            RETURNING id
        """
        with connection:
            with connection.cursor() as cursor:
                cursor.execute(sql, (user_id, token_hash, expires_at, request_ip, request_user_agent))
                row = cursor.fetchone()
            connection.commit()
        return int(row[0]) if row else None

    def get_magic_link_by_hash(self, token_hash: str) -> dict[str, Any] | None:
        return self._fetch_one(
            """
            SELECT links.id, links.user_id, links.token_hash, links.expires_at, links.used_at, links.revoked_at,
                   links.request_ip, links.request_user_agent, links.created_at,
                   users.email, users.role, users.is_active, users.last_login_at
            FROM app_magic_links links
            JOIN app_users users ON users.id = links.user_id
            WHERE links.token_hash = %s
            """,
            (token_hash,),
        )

    def mark_magic_link_used(self, *, magic_link_id: int) -> None:
        self._execute("UPDATE app_magic_links SET used_at = NOW() WHERE id = %s", (magic_link_id,))

    def revoke_magic_links_for_user(self, *, user_id: int) -> None:
        self._execute(
            """
            UPDATE app_magic_links
            SET revoked_at = NOW()
            WHERE user_id = %s
              AND used_at IS NULL
              AND revoked_at IS NULL
            """,
            (user_id,),
        )

    def create_session(
        self,
        *,
        user_id: int,
        session_id: str,
        expires_at: dt.datetime,
        created_ip: str,
        created_user_agent: str,
    ) -> int | None:
        connection = self._connect()
        if connection is None:
            return None
        sql = """
            INSERT INTO app_sessions (user_id, session_id, expires_at, created_ip, created_user_agent)
            VALUES (%s, %s, %s, %s, %s)
            RETURNING id
        """
        with connection:
            with connection.cursor() as cursor:
                cursor.execute(sql, (user_id, session_id, expires_at, created_ip, created_user_agent))
                row = cursor.fetchone()
            connection.commit()
        return int(row[0]) if row else None

    def get_session(self, session_id: str) -> dict[str, Any] | None:
        return self._fetch_one(
            """
            SELECT sessions.id, sessions.user_id, sessions.session_id, sessions.expires_at, sessions.revoked_at,
                   sessions.created_at, sessions.last_seen_at, users.email, users.role, users.is_active
            FROM app_sessions sessions
            JOIN app_users users ON users.id = sessions.user_id
            WHERE sessions.session_id = %s
            """,
            (session_id,),
        )

    def touch_session(self, *, session_id: str) -> None:
        self._execute("UPDATE app_sessions SET last_seen_at = NOW() WHERE session_id = %s", (session_id,))

    def revoke_session(self, *, session_id: str) -> None:
        self._execute("UPDATE app_sessions SET revoked_at = NOW() WHERE session_id = %s AND revoked_at IS NULL", (session_id,))

    def update_last_login(self, *, user_id: int) -> None:
        self._execute("UPDATE app_users SET last_login_at = NOW(), updated_at = NOW() WHERE id = %s", (user_id,))

    def _update_user(self, *, user_id: int, field_sql: str, value: object) -> dict[str, Any] | None:
        connection = self._connect()
        if connection is None:
            return None
        sql = f"""
            UPDATE app_users
            SET {field_sql},
                updated_at = NOW()
            WHERE id = %s
            RETURNING id, email, role, is_active, created_at, updated_at, last_login_at
        """
        with connection:
            with connection.cursor() as cursor:
                cursor.execute(sql, (value, user_id))
                rows = self._rows_to_dicts(cursor, cursor.fetchall())
            connection.commit()
        return rows[0] if rows else None

    def _fetch_one(self, sql: str, params: tuple[object, ...]) -> dict[str, Any] | None:
        connection = self._connect()
        if connection is None:
            return None
        with connection:
            with connection.cursor() as cursor:
                cursor.execute(sql, params)
                rows = self._rows_to_dicts(cursor, cursor.fetchall())
        return rows[0] if rows else None

    def _execute(self, sql: str, params: tuple[object, ...]) -> None:
        connection = self._connect()
        if connection is None:
            return
        with connection:
            with connection.cursor() as cursor:
                cursor.execute(sql, params)
            connection.commit()
