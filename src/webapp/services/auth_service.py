from __future__ import annotations

import base64
import datetime as dt
import hashlib
import hmac
import json
import secrets
from typing import Any
from urllib.parse import urlencode

import requests
from google.auth.transport.requests import Request as GoogleRequest
from google.oauth2 import id_token as google_id_token

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
        _ = (email, request_ip, request_user_agent)
        raise ValueError("Magic link login has been removed. Use Google sign-in.")

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
        _ = (token, request_ip, request_user_agent)
        raise ValueError("Magic link login has been removed. Use Google sign-in.")

    def begin_google_oauth(self, *, next_path: str) -> dict[str, str]:
        self._ensure_google_oauth_configured()
        normalized_next = self._normalize_next_path(next_path)
        issued_at = int(self._now().timestamp())
        state = secrets.token_urlsafe(24)
        payload = {
            "state": state,
            "next_path": normalized_next,
            "issued_at": issued_at,
        }
        state_cookie_value = self._sign_structured_payload(payload)
        query = {
            "client_id": self.config.google_client_id.strip(),
            "redirect_uri": self.config.google_redirect_uri.strip(),
            "response_type": "code",
            "scope": "openid email",
            "state": state,
            "access_type": "offline",
            "include_granted_scopes": "true",
            "prompt": "select_account",
        }
        hosted_domain = self.config.google_hosted_domain.strip()
        if hosted_domain:
            query["hd"] = hosted_domain
        authorization_url = f"https://accounts.google.com/o/oauth2/v2/auth?{urlencode(query)}"
        return {"authorization_url": authorization_url, "state_cookie_value": state_cookie_value}

    def complete_google_oauth(
        self,
        *,
        code: str,
        state: str,
        signed_state_cookie: str | None,
        request_ip: str,
        request_user_agent: str,
    ) -> dict[str, Any]:
        if not self.repository.is_configured():
            raise ValueError("Authentication requires TICKER_SCREENER_DATABASE_URL.")
        self.ensure_bootstrap_admins()
        self._ensure_google_oauth_configured()
        if not code.strip():
            raise ValueError("Missing Google authorization code.")
        state_payload = self._verify_google_oauth_state(state=state, signed_state_cookie=signed_state_cookie)
        token_payload = self._exchange_google_auth_code(code.strip())
        raw_id_token = str(token_payload.get("id_token") or "").strip()
        if not raw_id_token:
            raise ValueError("Google response did not include an ID token.")
        claims = google_id_token.verify_oauth2_token(raw_id_token, GoogleRequest(), self.config.google_client_id.strip())
        email = str(claims.get("email") or "").strip().lower()
        provider_subject = str(claims.get("sub") or "").strip()
        email_verified = bool(claims.get("email_verified"))
        hosted_domain = str(claims.get("hd") or "").strip().lower()
        required_domain = self.config.google_hosted_domain.strip().lower()
        if not provider_subject:
            raise ValueError("Google response did not include a stable subject identifier.")
        if not email:
            raise ValueError("Google response did not include an email address.")
        if not email_verified:
            raise ValueError("Google account email is not verified.")
        if required_domain and hosted_domain != required_domain:
            raise ValueError(f"Google account must belong to {required_domain}.")

        identity = self.repository.get_user_identity(provider="google", provider_subject=provider_subject)
        user = None
        if identity is not None:
            user = self.repository.get_user_by_id(int(identity["user_id"]))
            if user is None:
                raise ValueError("Linked Google identity points to an unknown app user.")
        if user is None:
            user = self.repository.get_user_by_email(email)
        if user is None:
            raise ValueError("No approved account found for this Google email. Ask an admin to add your email or request premium access first.")
        if not bool(user.get("is_active")):
            raise ValueError("Account is inactive.")

        app_user_id = int(user["id"])
        current_email = str(user.get("email") or "").strip().lower()
        if current_email and current_email != email:
            existing_email_user = self.repository.get_user_by_email(email)
            if existing_email_user is None or int(existing_email_user["id"]) == app_user_id:
                updated_user = self.repository.update_user_email(user_id=app_user_id, email=email)
                if updated_user is not None:
                    user = updated_user
        self.repository.upsert_user_identity(
            user_id=app_user_id,
            provider="google",
            provider_subject=provider_subject,
            provider_email=email,
        )

        session_id = secrets.token_urlsafe(32)
        self.repository.create_session(
            user_id=app_user_id,
            session_id=session_id,
            expires_at=self._now() + dt.timedelta(hours=max(1, int(self.config.auth_session_ttl_hours))),
            created_ip=request_ip,
            created_user_agent=request_user_agent,
        )
        self.repository.update_last_login(user_id=app_user_id)
        principal = principal_for_user(
            user_id=app_user_id,
            email=str(user["email"]),
            role=str(user["role"]),
            is_active=bool(user["is_active"]),
        )
        return {
            "session_cookie_value": self.sign_session_cookie(session_id),
            "principal": principal.to_dict(),
            "next_path": str(state_payload.get("next_path") or "/"),
            "email": email,
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

    def _sign_value(self, value: str) -> str:
        secret = self.config.auth_secret_key.strip()
        if not secret:
            raise ValueError("TICKER_SCREENER_AUTH_SECRET_KEY is required for auth.")
        return hmac.new(secret.encode("utf-8"), value.encode("utf-8"), hashlib.sha256).hexdigest()

    def _sign_structured_payload(self, payload: dict[str, Any]) -> str:
        encoded_payload = base64.urlsafe_b64encode(json.dumps(payload, separators=(",", ":")).encode("utf-8")).decode("utf-8")
        signature = self._sign_value(encoded_payload)
        return f"{encoded_payload}.{signature}"

    def _verify_structured_payload(self, raw_value: str | None) -> dict[str, Any] | None:
        if not raw_value:
            return None
        try:
            encoded_payload, signature = str(raw_value).split(".", 1)
        except ValueError:
            return None
        expected = self._sign_value(encoded_payload)
        if not hmac.compare_digest(signature, expected):
            return None
        try:
            decoded = base64.urlsafe_b64decode(encoded_payload.encode("utf-8")).decode("utf-8")
            payload = json.loads(decoded)
        except Exception:
            return None
        return payload if isinstance(payload, dict) else None

    def _verify_google_oauth_state(self, *, state: str, signed_state_cookie: str | None) -> dict[str, Any]:
        payload = self._verify_structured_payload(signed_state_cookie)
        if payload is None:
            raise ValueError("Missing or invalid Google sign-in state.")
        expected_state = str(payload.get("state") or "")
        if not expected_state or expected_state != str(state or ""):
            raise ValueError("Google sign-in state mismatch.")
        issued_at = int(payload.get("issued_at") or 0)
        age_seconds = int(self._now().timestamp()) - issued_at
        if issued_at <= 0 or age_seconds < 0 or age_seconds > 600:
            raise ValueError("Google sign-in attempt expired. Start again.")
        payload["next_path"] = self._normalize_next_path(str(payload.get("next_path") or "/"))
        return payload

    def _ensure_google_oauth_configured(self) -> None:
        if not self.config.google_client_id.strip():
            raise ValueError("WEBAPP_GOOGLE_CLIENT_ID is required for Google sign-in.")
        if not self.config.google_client_secret.strip():
            raise ValueError("WEBAPP_GOOGLE_CLIENT_SECRET is required for Google sign-in.")
        if not self.config.google_redirect_uri.strip():
            raise ValueError("WEBAPP_GOOGLE_REDIRECT_URI is required for Google sign-in.")
        if not self.config.auth_secret_key.strip():
            raise ValueError("TICKER_SCREENER_AUTH_SECRET_KEY is required for auth.")

    def _exchange_google_auth_code(self, code: str) -> dict[str, Any]:
        try:
            response = requests.post(
                "https://oauth2.googleapis.com/token",
                data={
                    "code": code,
                    "client_id": self.config.google_client_id.strip(),
                    "client_secret": self.config.google_client_secret.strip(),
                    "redirect_uri": self.config.google_redirect_uri.strip(),
                    "grant_type": "authorization_code",
                },
                timeout=20,
            )
        except requests.RequestException as exc:
            raise ValueError(f"Unable to reach Google token endpoint: {exc}") from exc
        if not response.ok:
            detail = ""
            try:
                payload = response.json()
                if isinstance(payload, dict):
                    detail = str(payload.get("error_description") or payload.get("error") or "").strip()
            except ValueError:
                detail = response.text.strip()
            raise ValueError(f"Google token exchange failed: {detail or response.status_code}")
        payload = response.json()
        if not isinstance(payload, dict):
            raise ValueError("Google token exchange returned an invalid response.")
        return payload

    def _normalize_next_path(self, next_path: str) -> str:
        candidate = str(next_path or "/").strip()
        if not candidate.startswith("/") or candidate.startswith("//"):
            return "/"
        return candidate

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

    def get_user(self, *, user_id: int) -> dict[str, Any] | None:
        if not self.repository.is_configured():
            return None
        user = self.repository.get_user_by_id(user_id)
        return self._normalize_user(user) if user is not None else None

    def get_access_request(self, *, request_id: int) -> dict[str, Any] | None:
        if not self.repository.is_configured():
            return None
        item = self.repository.get_access_request_by_id(request_id)
        return self._normalize_access_request(item) if item is not None else None

    def invite_or_create_user(self, *, email: str, role: str) -> dict[str, Any]:
        if not self.repository.is_configured():
            raise ValueError("Authentication requires TICKER_SCREENER_DATABASE_URL.")
        clean_email = str(email).strip().lower()
        if not clean_email:
            raise ValueError("email is required")
        user = self.repository.upsert_user(email=clean_email, role=normalize_role(role), is_active=True)
        if user is None:
            raise ValueError("Unable to create user.")
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
