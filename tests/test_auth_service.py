from __future__ import annotations

import datetime as dt
import unittest

from src.webapp.config import WebAppConfig
from src.webapp.services.auth_service import AuthService, UserAdminService


class _FakeAuthRepository:
    def __init__(self) -> None:
        self.users = {
            "admin@example.com": {
                "id": 1,
                "email": "admin@example.com",
                "role": "admin",
                "is_active": True,
                "created_at": None,
                "updated_at": None,
                "last_login_at": None,
            }
        }
        self.magic_links: dict[str, dict[str, object]] = {}
        self.sessions: dict[str, dict[str, object]] = {}
        self.access_requests: dict[int, dict[str, object]] = {}

    def is_configured(self) -> bool:
        return True

    def upsert_user(self, *, email: str, role: str, is_active: bool = True):
        existing = self.users.get(email)
        if existing:
            existing["role"] = role
            existing["is_active"] = is_active
            return dict(existing)
        next_id = len(self.users) + 1
        self.users[email] = {
            "id": next_id,
            "email": email,
            "role": role,
            "is_active": is_active,
            "created_at": None,
            "updated_at": None,
            "last_login_at": None,
        }
        return dict(self.users[email])

    def get_user_by_email(self, email: str):
        user = self.users.get(email)
        return dict(user) if user else None

    def get_user_by_id(self, user_id: int):
        for user in self.users.values():
            if user["id"] == user_id:
                return dict(user)
        return None

    def list_users(self):
        return [dict(user) for user in sorted(self.users.values(), key=lambda item: item["email"])]

    def update_user_role(self, *, user_id: int, role: str):
        user = self.get_user_by_id(user_id)
        if not user:
            return None
        user["role"] = role
        self.users[user["email"]] = user
        return dict(user)

    def update_user_active(self, *, user_id: int, is_active: bool):
        user = self.get_user_by_id(user_id)
        if not user:
            return None
        user["is_active"] = is_active
        self.users[user["email"]] = user
        return dict(user)

    def revoke_magic_links_for_user(self, *, user_id: int):
        for item in self.magic_links.values():
            if item["user_id"] == user_id and item.get("used_at") is None:
                item["revoked_at"] = dt.datetime.now(dt.timezone.utc)

    def create_magic_link(self, *, user_id: int, token_hash: str, expires_at: dt.datetime, request_ip: str, request_user_agent: str):
        self.magic_links[token_hash] = {
            "id": len(self.magic_links) + 1,
            "user_id": user_id,
            "token_hash": token_hash,
            "expires_at": expires_at,
            "used_at": None,
            "revoked_at": None,
            "request_ip": request_ip,
            "request_user_agent": request_user_agent,
            "created_at": dt.datetime.now(dt.timezone.utc),
        }
        return len(self.magic_links)

    def get_magic_link_by_hash(self, token_hash: str):
        item = self.magic_links.get(token_hash)
        if not item:
            return None
        user = self.get_user_by_id(int(item["user_id"]))
        return {**item, **user}

    def mark_magic_link_used(self, *, magic_link_id: int):
        for item in self.magic_links.values():
            if item["id"] == magic_link_id:
                item["used_at"] = dt.datetime.now(dt.timezone.utc)

    def create_session(self, *, user_id: int, session_id: str, expires_at: dt.datetime, created_ip: str, created_user_agent: str):
        self.sessions[session_id] = {
            "id": len(self.sessions) + 1,
            "user_id": user_id,
            "session_id": session_id,
            "expires_at": expires_at,
            "revoked_at": None,
            "created_ip": created_ip,
            "created_user_agent": created_user_agent,
            "created_at": dt.datetime.now(dt.timezone.utc),
            "last_seen_at": dt.datetime.now(dt.timezone.utc),
        }
        return len(self.sessions)

    def get_session(self, session_id: str):
        session = self.sessions.get(session_id)
        if not session:
            return None
        user = self.get_user_by_id(int(session["user_id"]))
        return {**session, **user}

    def touch_session(self, *, session_id: str):
        if session_id in self.sessions:
            self.sessions[session_id]["last_seen_at"] = dt.datetime.now(dt.timezone.utc)

    def revoke_session(self, *, session_id: str):
        if session_id in self.sessions:
            self.sessions[session_id]["revoked_at"] = dt.datetime.now(dt.timezone.utc)

    def update_last_login(self, *, user_id: int):
        user = self.get_user_by_id(user_id)
        if user:
            user["last_login_at"] = dt.datetime.now(dt.timezone.utc)
            self.users[user["email"]] = user

    def get_pending_access_request_by_email(self, email: str):
        matches = [
            dict(item)
            for item in self.access_requests.values()
            if item["email"] == email and item["status"] == "pending"
        ]
        matches.sort(key=lambda item: item["requested_at"], reverse=True)
        return matches[0] if matches else None

    def create_access_request(self, *, email: str, requested_role: str = "premium"):
        existing = self.get_pending_access_request_by_email(email)
        if existing:
            return existing
        request_id = len(self.access_requests) + 1
        item = {
            "id": request_id,
            "email": email,
            "requested_role": requested_role,
            "status": "pending",
            "requested_at": dt.datetime.now(dt.timezone.utc),
            "reviewed_at": None,
            "reviewed_by_user_id": None,
            "reviewed_by_email": None,
            "deny_reason": "",
            "invited_user_id": None,
            "invited_user_email": None,
            "created_at": dt.datetime.now(dt.timezone.utc),
        }
        self.access_requests[request_id] = item
        return dict(item)

    def list_access_requests(self, *, status: str | None = None, limit: int = 200):
        items = [dict(item) for item in self.access_requests.values()]
        if status:
            items = [item for item in items if item["status"] == status]
        items.sort(key=lambda item: (0 if item["status"] == "pending" else 1, -item["id"]))
        return items[:limit]

    def get_access_request_by_id(self, request_id: int):
        item = self.access_requests.get(request_id)
        return dict(item) if item else None

    def resolve_access_request(self, *, request_id: int, status: str, reviewed_by_user_id: int, deny_reason: str = "", invited_user_id: int | None = None):
        item = self.access_requests.get(request_id)
        if not item:
            return None
        reviewer = self.get_user_by_id(reviewed_by_user_id)
        invited = self.get_user_by_id(invited_user_id) if invited_user_id is not None else None
        item.update(
            {
                "status": status,
                "reviewed_at": dt.datetime.now(dt.timezone.utc),
                "reviewed_by_user_id": reviewed_by_user_id,
                "reviewed_by_email": reviewer["email"] if reviewer else None,
                "deny_reason": deny_reason,
                "invited_user_id": invited_user_id,
                "invited_user_email": invited["email"] if invited else None,
            }
        )
        return dict(item)


class AuthServiceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.repo = _FakeAuthRepository()
        self.config = WebAppConfig(
            app_base_url="https://app.example.com",
            auth_secret_key="secret-key",
            smtp_host="smtp.example.com",
            smtp_port=587,
            smtp_from_address="noreply@example.com",
            smtp_use_tls=False,
            smtp_use_ssl=False,
            auth_bootstrap_admin_emails_raw="admin@example.com",
        )
        self._original_send_email = AuthService._send_email
        AuthService._send_email = lambda *_, **__: None  # type: ignore[method-assign]
        self.service = AuthService(config=self.config, repository=self.repo)  # type: ignore[arg-type]
        self.user_admin = UserAdminService(repository=self.repo, config=self.config)  # type: ignore[arg-type]

    def tearDown(self) -> None:
        AuthService._send_email = self._original_send_email  # type: ignore[assignment]

    def test_request_and_verify_magic_link_creates_authenticated_principal(self) -> None:
        request_result = self.service.request_magic_link(
            email="admin@example.com",
            request_ip="127.0.0.1",
            request_user_agent="unit-test",
        )

        self.assertTrue(request_result["ok"])
        token_hash = next(iter(self.repo.magic_links.keys()))
        raw_token = "missing"
        for candidate in ("x",):
            _ = candidate
        # recreate from stored hash impossible, so verify through direct service helpers
        # by generating a fresh token path deterministically.
        self.repo.magic_links.clear()
        raw_token = "test-token"
        self.repo.create_magic_link(
            user_id=1,
            token_hash=self.service._hash_token(raw_token),
            expires_at=dt.datetime.now(dt.timezone.utc) + dt.timedelta(minutes=10),
            request_ip="127.0.0.1",
            request_user_agent="unit-test",
        )

        verified = self.service.verify_magic_link(
            token=raw_token,
            request_ip="127.0.0.1",
            request_user_agent="unit-test",
        )

        self.assertIn("session_cookie_value", verified)
        self.assertEqual(verified["principal"]["role"], "admin")
        principal = self.service.principal_from_signed_session(verified["session_cookie_value"])
        self.assertTrue(principal.authenticated)
        self.assertTrue(principal.can("manage_users"))

    def test_logout_revokes_session(self) -> None:
        self.repo.create_session(
            user_id=1,
            session_id="session-1",
            expires_at=dt.datetime.now(dt.timezone.utc) + dt.timedelta(hours=1),
            created_ip="127.0.0.1",
            created_user_agent="ua",
        )
        signed = self.service.sign_session_cookie("session-1")

        self.service.logout(signed_session=signed)

        principal = self.service.principal_from_signed_session(signed)
        self.assertFalse(principal.authenticated)

    def test_user_admin_service_updates_roles(self) -> None:
        created = self.user_admin.invite_or_create_user(email="premium@example.com", role="premium")
        updated = self.user_admin.update_role(user_id=created["id"], role="admin")

        self.assertEqual(updated["role"], "admin")
        users = self.user_admin.list_users()
        self.assertEqual(len(users), 2)

    def test_request_premium_access_creates_pending_request(self) -> None:
        first = self.service.request_premium_access(email="visitor@example.com")
        second = self.service.request_premium_access(email="visitor@example.com")

        self.assertEqual(first["status"], "pending")
        self.assertEqual(second["status"], "already_pending")
        self.assertEqual(len(self.repo.access_requests), 1)

    def test_approve_access_request_grants_premium_and_resolves_request(self) -> None:
        self.service.request_premium_access(email="visitor@example.com")

        approved = self.user_admin.approve_access_request(request_id=1, reviewed_by_user_id=1)

        self.assertEqual(approved["status"], "approved")
        user = self.repo.get_user_by_email("visitor@example.com")
        self.assertIsNotNone(user)
        self.assertEqual(user["role"], "premium")
        self.assertTrue(self.repo.magic_links)


if __name__ == "__main__":
    unittest.main()
