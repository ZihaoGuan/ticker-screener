from __future__ import annotations

import base64
import datetime as dt
import email.message
import hashlib
import hmac
import secrets
import smtplib
from typing import Any

from src.webapp.access_control import Principal, anonymous_principal, normalize_role, principal_for_user
from src.webapp.config import WebAppConfig
from src.webapp.repositories.auth_repository import AuthRepository


class AuthService:
    def __init__(self, *, config: WebAppConfig, repository: AuthRepository | None = None) -> None:
        self.config = config
        self.repository = repository or AuthRepository(database_url=config.database_url)

    def ensure_bootstrap_admins(self) -> None:
        if not self.repository.is_configured():
            return
        for email in self.config.auth_bootstrap_admin_emails:
            self.repository.upsert_user(email=email, role="admin", is_active=True)

    def request_magic_link(self, *, email: str, request_ip: str, request_user_agent: str) -> dict[str, Any]:
        if not self.repository.is_configured():
            raise ValueError("Authentication requires TICKER_SCREENER_DATABASE_URL.")
        self.ensure_bootstrap_admins()
        clean_email = str(email).strip().lower()
        if not clean_email:
            raise ValueError("email is required")
        user = self.repository.get_user_by_email(clean_email)
        if user is None:
            raise ValueError("Unknown account.")
        if not bool(user.get("is_active")):
            raise ValueError("Account is inactive.")
        self._ensure_email_delivery_configured()
        self.repository.revoke_magic_links_for_user(user_id=int(user["id"]))
        raw_token = secrets.token_urlsafe(32)
        token_hash = self._hash_token(raw_token)
        expires_at = self._now() + dt.timedelta(minutes=max(1, int(self.config.auth_magic_link_ttl_minutes)))
        self.repository.create_magic_link(
            user_id=int(user["id"]),
            token_hash=token_hash,
            expires_at=expires_at,
            request_ip=request_ip,
            request_user_agent=request_user_agent,
        )
        link = self._build_magic_link(raw_token)
        self._send_email(
            to_address=clean_email,
            subject=f"{self.config.app_title} sign-in link",
            body=(
                f"Use this sign-in link for {self.config.app_title}:\n\n"
                f"{link}\n\n"
                f"This link expires at {expires_at.isoformat()} UTC."
            ),
        )
        return {"ok": True, "email": clean_email, "expires_at": expires_at.isoformat()}

    def request_premium_access(self, *, email: str) -> dict[str, Any]:
        if not self.repository.is_configured():
            raise ValueError("Authentication requires TICKER_SCREENER_DATABASE_URL.")
        self.ensure_bootstrap_admins()
        clean_email = str(email).strip().lower()
        if not clean_email:
            raise ValueError("email is required")
        user = self.repository.get_user_by_email(clean_email)
        if user is not None:
            if not bool(user.get("is_active")):
                raise ValueError("Account is inactive.")
            role = normalize_role(user.get("role"))
            if role in {"premium", "admin"}:
                return {"ok": True, "email": clean_email, "status": "already_granted", "message": "This email already has access."}
        existing = self.repository.get_pending_access_request_by_email(clean_email)
        if existing is not None:
            return {"ok": True, "email": clean_email, "status": "already_pending", "message": "A premium access request is already pending."}
        request_record = self.repository.create_access_request(email=clean_email, requested_role="premium")
        if request_record is None:
            raise ValueError("Unable to create access request.")
        return {"ok": True, "email": clean_email, "status": "pending", "message": "Premium access request submitted."}

    def verify_magic_link(self, *, token: str, request_ip: str, request_user_agent: str) -> dict[str, Any]:
        if not self.repository.is_configured():
            raise ValueError("Authentication requires TICKER_SCREENER_DATABASE_URL.")
        clean_token = str(token).strip()
        if not clean_token:
            raise ValueError("token is required")
        record = self.repository.get_magic_link_by_hash(self._hash_token(clean_token))
        if record is None:
            raise ValueError("Invalid magic link.")
        if record.get("revoked_at") is not None:
            raise ValueError("Magic link has been revoked.")
        if record.get("used_at") is not None:
            raise ValueError("Magic link has already been used.")
        expires_at = record.get("expires_at")
        if not isinstance(expires_at, dt.datetime) or expires_at <= self._now():
            raise ValueError("Magic link has expired.")
        if not bool(record.get("is_active")):
            raise ValueError("Account is inactive.")
        session_id = secrets.token_urlsafe(32)
        self.repository.mark_magic_link_used(magic_link_id=int(record["id"]))
        self.repository.create_session(
            user_id=int(record["user_id"]),
            session_id=session_id,
            expires_at=self._now() + dt.timedelta(hours=max(1, int(self.config.auth_session_ttl_hours))),
            created_ip=request_ip,
            created_user_agent=request_user_agent,
        )
        self.repository.update_last_login(user_id=int(record["user_id"]))
        principal = principal_for_user(
            user_id=int(record["user_id"]),
            email=str(record["email"]),
            role=str(record["role"]),
            is_active=bool(record["is_active"]),
        )
        return {
            "session_cookie_value": self.sign_session_cookie(session_id),
            "principal": principal.to_dict(),
        }

    def logout(self, *, signed_session: str | None) -> None:
        session_id = self.verify_session_cookie(signed_session)
        if session_id:
            self.repository.revoke_session(session_id=session_id)

    def principal_from_signed_session(self, signed_session: str | None) -> Principal:
        self.ensure_bootstrap_admins()
        session_id = self.verify_session_cookie(signed_session)
        if not session_id:
            return anonymous_principal()
        record = self.repository.get_session(session_id)
        if record is None:
            return anonymous_principal()
        expires_at = record.get("expires_at")
        if record.get("revoked_at") is not None:
            return anonymous_principal()
        if not isinstance(expires_at, dt.datetime) or expires_at <= self._now():
            return anonymous_principal()
        if not bool(record.get("is_active")):
            return anonymous_principal()
        self.repository.touch_session(session_id=session_id)
        return principal_for_user(
            user_id=int(record["user_id"]),
            email=str(record["email"]),
            role=str(record["role"]),
            is_active=bool(record["is_active"]),
        )

    def sign_session_cookie(self, session_id: str) -> str:
        signature = self._sign_value(session_id)
        payload = f"{session_id}.{signature}"
        return base64.urlsafe_b64encode(payload.encode("utf-8")).decode("utf-8")

    def verify_session_cookie(self, signed_session: str | None) -> str | None:
        if not signed_session:
            return None
        try:
            decoded = base64.urlsafe_b64decode(str(signed_session).encode("utf-8")).decode("utf-8")
        except Exception:
            return None
        session_id, sep, signature = decoded.partition(".")
        if not sep or not session_id or not signature:
            return None
        expected = self._sign_value(session_id)
        if not hmac.compare_digest(signature, expected):
            return None
        return session_id

    def _build_magic_link(self, raw_token: str) -> str:
        base_url = self.config.app_base_url.strip().rstrip("/")
        if not base_url:
            raise ValueError("TICKER_SCREENER_APP_BASE_URL is required for auth.")
        return f"{base_url}/login?token={raw_token}"

    def _hash_token(self, raw_token: str) -> str:
        return hashlib.sha256(raw_token.encode("utf-8")).hexdigest()

    def _sign_value(self, value: str) -> str:
        secret = self.config.auth_secret_key.strip()
        if not secret:
            raise ValueError("TICKER_SCREENER_AUTH_SECRET_KEY is required for auth.")
        return hmac.new(secret.encode("utf-8"), value.encode("utf-8"), hashlib.sha256).hexdigest()

    def _ensure_email_delivery_configured(self) -> None:
        if not self.config.smtp_host.strip() or not self.config.smtp_from_address.strip():
            raise ValueError("SMTP is not configured. Set SMTP host and from address.")

    def _send_email(self, *, to_address: str, subject: str, body: str) -> None:
        message = email.message.EmailMessage()
        message["From"] = self.config.smtp_from_address
        message["To"] = to_address
        message["Subject"] = subject
        message.set_content(body)

        if self.config.smtp_use_ssl:
            server: smtplib.SMTP = smtplib.SMTP_SSL(self.config.smtp_host, self.config.smtp_port, timeout=20)
        else:
            server = smtplib.SMTP(self.config.smtp_host, self.config.smtp_port, timeout=20)
        with server:
            if self.config.smtp_use_tls and not self.config.smtp_use_ssl:
                server.starttls()
            if self.config.smtp_username.strip():
                server.login(self.config.smtp_username, self.config.smtp_password)
            server.send_message(message)

    def _deliver_role_grant_link(self, *, user_id: int, email: str, subject: str, intro: str) -> dict[str, Any]:
        self.repository.revoke_magic_links_for_user(user_id=user_id)
        raw_token = secrets.token_urlsafe(32)
        token_hash = self._hash_token(raw_token)
        expires_at = self._now() + dt.timedelta(minutes=max(1, int(self.config.auth_magic_link_ttl_minutes)))
        self.repository.create_magic_link(
            user_id=user_id,
            token_hash=token_hash,
            expires_at=expires_at,
            request_ip="admin-grant",
            request_user_agent="system",
        )
        link = self._build_magic_link(raw_token)
        self._send_email(
            to_address=email,
            subject=subject,
            body=(
                f"{intro}\n\n"
                f"{link}\n\n"
                f"This link expires at {expires_at.isoformat()} UTC."
            ),
        )
        return {"ok": True, "email": email, "expires_at": expires_at.isoformat()}

    def _now(self) -> dt.datetime:
        return dt.datetime.now(dt.timezone.utc)


class UserAdminService:
    def __init__(self, *, repository: AuthRepository, config: WebAppConfig) -> None:
        self.repository = repository
        self.config = config

    def list_users(self) -> list[dict[str, Any]]:
        if not self.repository.is_configured():
            return []
        AuthService(config=self.config, repository=self.repository).ensure_bootstrap_admins()
        users = self.repository.list_users()
        return [self._normalize_user(item) for item in users]

    def list_access_requests(self, *, status: str | None = None) -> list[dict[str, Any]]:
        if not self.repository.is_configured():
            return []
        return [self._normalize_access_request(item) for item in self.repository.list_access_requests(status=status)]

    def invite_or_create_user(self, *, email: str, role: str) -> dict[str, Any]:
        if not self.repository.is_configured():
            raise ValueError("Authentication requires TICKER_SCREENER_DATABASE_URL.")
        clean_email = str(email).strip().lower()
        if not clean_email:
            raise ValueError("email is required")
        user = self.repository.upsert_user(email=clean_email, role=normalize_role(role), is_active=True)
        if user is None:
            raise ValueError("Unable to create user.")
        auth_service = AuthService(config=self.config, repository=self.repository)
        auth_service._ensure_email_delivery_configured()
        auth_service._deliver_role_grant_link(
            user_id=int(user["id"]),
            email=clean_email,
            subject=f"{self.config.app_title} invitation",
            intro=(
                f"You have been granted {normalize_role(role)} access to {self.config.app_title}.\n\n"
                "Use the sign-in link below to access the app."
            ),
        )
        return self._normalize_user(user)

    def approve_access_request(self, *, request_id: int, reviewed_by_user_id: int) -> dict[str, Any]:
        if not self.repository.is_configured():
            raise ValueError("Authentication requires TICKER_SCREENER_DATABASE_URL.")
        request_record = self.repository.get_access_request_by_id(request_id)
        if request_record is None:
            raise ValueError("Unknown access request.")
        if str(request_record.get("status")) != "pending":
            raise ValueError("Access request is not pending.")
        email = str(request_record["email"]).strip().lower()
        user = self.repository.get_user_by_email(email)
        if user is not None and not bool(user.get("is_active")):
            user = self.repository.update_user_active(user_id=int(user["id"]), is_active=True)
        if user is None:
            user = self.repository.upsert_user(email=email, role="premium", is_active=True)
        else:
            user = self.repository.update_user_role(user_id=int(user["id"]), role="premium")
        if user is None:
            raise ValueError("Unable to grant premium access.")
        auth_service = AuthService(config=self.config, repository=self.repository)
        auth_service._ensure_email_delivery_configured()
        auth_service._deliver_role_grant_link(
            user_id=int(user["id"]),
            email=email,
            subject=f"{self.config.app_title} premium access approved",
            intro=(
                f"Your premium access request for {self.config.app_title} has been approved.\n\n"
                "Use the sign-in link below to access the app."
            ),
        )
        resolved = self.repository.resolve_access_request(
            request_id=request_id,
            status="approved",
            reviewed_by_user_id=reviewed_by_user_id,
            invited_user_id=int(user["id"]),
        )
        if resolved is None:
            raise ValueError("Unable to update access request.")
        return self._normalize_access_request({**resolved, "invited_user_email": email})

    def deny_access_request(self, *, request_id: int, reviewed_by_user_id: int, deny_reason: str = "") -> dict[str, Any]:
        if not self.repository.is_configured():
            raise ValueError("Authentication requires TICKER_SCREENER_DATABASE_URL.")
        request_record = self.repository.get_access_request_by_id(request_id)
        if request_record is None:
            raise ValueError("Unknown access request.")
        if str(request_record.get("status")) != "pending":
            raise ValueError("Access request is not pending.")
        resolved = self.repository.resolve_access_request(
            request_id=request_id,
            status="denied",
            reviewed_by_user_id=reviewed_by_user_id,
            deny_reason=deny_reason,
        )
        if resolved is None:
            raise ValueError("Unable to update access request.")
        return self._normalize_access_request(resolved)

    def update_role(self, *, user_id: int, role: str) -> dict[str, Any]:
        if not self.repository.is_configured():
            raise ValueError("Authentication requires TICKER_SCREENER_DATABASE_URL.")
        user = self.repository.update_user_role(user_id=user_id, role=normalize_role(role))
        if user is None:
            raise ValueError("Unknown user.")
        return self._normalize_user(user)

    def deactivate(self, *, user_id: int) -> dict[str, Any]:
        if not self.repository.is_configured():
            raise ValueError("Authentication requires TICKER_SCREENER_DATABASE_URL.")
        user = self.repository.update_user_active(user_id=user_id, is_active=False)
        if user is None:
            raise ValueError("Unknown user.")
        return self._normalize_user(user)

    def reactivate(self, *, user_id: int) -> dict[str, Any]:
        if not self.repository.is_configured():
            raise ValueError("Authentication requires TICKER_SCREENER_DATABASE_URL.")
        user = self.repository.update_user_active(user_id=user_id, is_active=True)
        if user is None:
            raise ValueError("Unknown user.")
        return self._normalize_user(user)

    def _normalize_user(self, user: dict[str, Any]) -> dict[str, Any]:
        return {
            "id": int(user["id"]),
            "email": str(user["email"]),
            "role": normalize_role(user["role"]),
            "is_active": bool(user["is_active"]),
            "created_at": user.get("created_at"),
            "updated_at": user.get("updated_at"),
            "last_login_at": user.get("last_login_at"),
        }

    def _normalize_access_request(self, item: dict[str, Any]) -> dict[str, Any]:
        return {
            "id": int(item["id"]),
            "email": str(item["email"]),
            "requested_role": normalize_role(item.get("requested_role")),
            "status": str(item.get("status") or "pending"),
            "requested_at": item.get("requested_at"),
            "reviewed_at": item.get("reviewed_at"),
            "reviewed_by_user_id": item.get("reviewed_by_user_id"),
            "reviewed_by_email": item.get("reviewed_by_email"),
            "deny_reason": item.get("deny_reason") or "",
            "invited_user_id": item.get("invited_user_id"),
            "invited_user_email": item.get("invited_user_email"),
            "created_at": item.get("created_at"),
        }
