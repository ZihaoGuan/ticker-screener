from __future__ import annotations

import datetime as dt
import unittest
from unittest.mock import patch

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
                "trial_ends_at": None,
                "created_at": None,
                "updated_at": None,
                "last_login_at": None,
            }
        }
        self.magic_links: dict[str, dict[str, object]] = {}
        self.sessions: dict[str, dict[str, object]] = {}
        self.identities: dict[tuple[str, str], dict[str, object]] = {}
        self.access_requests: dict[int, dict[str, object]] = {}

    def is_configured(self) -> bool:
        return True

    def upsert_user(self, *, email: str, role: str, is_active: bool = True, trial_ends_at: dt.datetime | None = None):
        existing = self.users.get(email)
        if existing:
            existing["role"] = role
            existing["is_active"] = is_active
            existing["trial_ends_at"] = trial_ends_at
            return dict(existing)
        next_id = len(self.users) + 1
        self.users[email] = {
            "id": next_id,
            "email": email,
            "role": role,
            "is_active": is_active,
            "trial_ends_at": trial_ends_at,
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

    def update_user_email(self, *, user_id: int, email: str):
        user = self.get_user_by_id(user_id)
        if not user:
            return None
        self.users.pop(user["email"])
        user["email"] = email
        self.users[email] = user
        return dict(user)

    def update_user_role(self, *, user_id: int, role: str):
        user = self.get_user_by_id(user_id)
        if not user:
            return None
        user["role"] = role
        user["trial_ends_at"] = None
        self.users[user["email"]] = user
        return dict(user)

    def update_user_active(self, *, user_id: int, is_active: bool):
        user = self.get_user_by_id(user_id)
        if not user:
            return None
        user["is_active"] = is_active
        self.users[user["email"]] = user
        return dict(user)

    def get_user_identity(self, *, provider: str, provider_subject: str):
        identity = self.identities.get((provider, provider_subject))
        return dict(identity) if identity else None

    def upsert_user_identity(self, *, user_id: int, provider: str, provider_subject: str, provider_email: str):
        identity = {
            "id": len(self.identities) + 1,
            "user_id": user_id,
            "provider": provider,
            "provider_subject": provider_subject,
            "provider_email": provider_email,
        }
        self.identities[(provider, provider_subject)] = identity
        return dict(identity)

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
            google_client_id="google-client",
            google_client_secret="google-secret",
            google_redirect_uri="https://app.example.com/api/auth/google/callback",
            smtp_host="smtp.example.com",
            smtp_port=587,
            smtp_from_address="noreply@example.com",
            smtp_use_tls=False,
            smtp_use_ssl=False,
            auth_bootstrap_admin_emails_raw="admin@example.com",
        )
        self.service = AuthService(config=self.config, repository=self.repo)  # type: ignore[arg-type]
        self.user_admin = UserAdminService(repository=self.repo, config=self.config)  # type: ignore[arg-type]

    def test_signed_session_creates_authenticated_principal(self) -> None:
        self.repo.create_session(
            user_id=1,
            session_id="admin-session",
            expires_at=dt.datetime.now(dt.timezone.utc) + dt.timedelta(hours=1),
            created_ip="127.0.0.1",
            created_user_agent="unit-test",
        )

        principal = self.service.principal_from_signed_session(self.service.sign_session_cookie("admin-session"))
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
        self.assertIsNone(user["trial_ends_at"])

    def test_first_google_login_starts_trial(self) -> None:
        self.service._verify_google_oauth_state = lambda **_: {"next_path": "/"}  # type: ignore[method-assign]
        self.service._exchange_google_auth_code = lambda _: {"id_token": "token"}  # type: ignore[method-assign]

        with patch(
            "src.webapp.services.auth_service.google_id_token.verify_oauth2_token",
            return_value={"email": "new@example.com", "sub": "google-1", "email_verified": True},
        ):
            result = self.service.complete_google_oauth(
                code="code",
                state="state",
                signed_state_cookie="cookie",
                request_ip="127.0.0.1",
                request_user_agent="unit-test",
            )

        user = self.repo.get_user_by_email("new@example.com")
        self.assertEqual(result["principal"]["role"], "premium")
        self.assertTrue(result["principal"]["trial_ends_at"])
        self.assertGreater(user["trial_ends_at"], dt.datetime.now(dt.timezone.utc) + dt.timedelta(days=13))

    def test_expired_trial_keeps_session_but_loses_run_access(self) -> None:
        user = self.repo.upsert_user(
            email="expired@example.com",
            role="premium",
            trial_ends_at=dt.datetime.now(dt.timezone.utc) - dt.timedelta(seconds=1),
        )
        self.repo.create_session(
            user_id=user["id"],
            session_id="expired-trial",
            expires_at=dt.datetime.now(dt.timezone.utc) + dt.timedelta(hours=1),
            created_ip="127.0.0.1",
            created_user_agent="unit-test",
        )

        principal = self.service.principal_from_signed_session(self.service.sign_session_cookie("expired-trial"))

        self.assertTrue(principal.authenticated)
        self.assertEqual(principal.role, "visitor")
        self.assertFalse(principal.can("run_screeners"))

    def test_expired_trial_can_request_full_access(self) -> None:
        self.repo.upsert_user(
            email="expired@example.com",
            role="premium",
            trial_ends_at=dt.datetime.now(dt.timezone.utc) - dt.timedelta(seconds=1),
        )

        result = self.service.request_premium_access(email="expired@example.com")

        self.assertEqual(result["status"], "pending")

    def test_manual_role_update_clears_trial_expiry(self) -> None:
        user = self.repo.upsert_user(
            email="trial@example.com",
            role="premium",
            trial_ends_at=dt.datetime.now(dt.timezone.utc) + dt.timedelta(days=7),
        )

        updated = self.user_admin.update_role(user_id=user["id"], role="premium")

        self.assertIsNone(updated["trial_ends_at"])


if __name__ == "__main__":
    unittest.main()
