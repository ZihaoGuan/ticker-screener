from __future__ import annotations

import datetime as dt
from typing import Any, TYPE_CHECKING

if TYPE_CHECKING:
    from fastapi import Request

from src.webapp.access_control import Principal
from src.webapp.repositories.audit_repository import AuditRepository


class AuditService:
    def __init__(self, *, repository: AuditRepository) -> None:
        self.repository = repository

    def is_configured(self) -> bool:
        return self.repository.is_configured()

    def record_event(
        self,
        *,
        principal: Principal | None,
        request: "Request | None",
        action: str,
        resource_type: str,
        resource_id: str = "",
        resource_label: str = "",
        message: str = "",
        metadata: dict[str, Any] | None = None,
        actor_email_override: str = "",
        actor_role_override: str = "",
        actor_user_id_override: int | None = None,
    ) -> dict[str, Any] | None:
        actor_user_id = actor_user_id_override
        actor_email = actor_email_override.strip()
        actor_role = actor_role_override.strip()
        if principal is not None:
            if actor_user_id is None:
                actor_user_id = principal.user_id
            if not actor_email:
                actor_email = str(principal.email or "")
            if not actor_role:
                actor_role = str(principal.role or "")
        request_ip = ""
        request_user_agent = ""
        if request is not None:
            request_ip = request.client.host if request.client else ""
            request_user_agent = request.headers.get("user-agent", "")
        return self.repository.create_event(
            actor_user_id=actor_user_id,
            actor_email=actor_email,
            actor_role=actor_role,
            request_ip=request_ip,
            request_user_agent=request_user_agent,
            action=action,
            resource_type=resource_type,
            resource_id=resource_id,
            resource_label=resource_label,
            status="success",
            message=message,
            metadata_json=metadata or {},
        )

    def list_events(
        self,
        *,
        actor_email: str = "",
        action: str = "",
        resource_type: str = "",
        from_date: str = "",
        to_date: str = "",
        limit: int = 50,
        offset: int = 0,
    ) -> dict[str, Any]:
        start_dt = self._parse_date_start(from_date)
        end_dt = self._parse_date_end_exclusive(to_date)
        events = [self._normalize_event(item) for item in self.repository.list_events(
            actor_email=actor_email,
            action=action,
            resource_type=resource_type,
            from_date=start_dt,
            to_date=end_dt,
            limit=limit + 1,
            offset=offset,
        )]
        has_more = len(events) > limit
        return {
            "events": events[:limit],
            "filters": {
                "actorEmail": actor_email,
                "action": action,
                "resourceType": resource_type,
                "from": from_date,
                "to": to_date,
                "limit": limit,
                "offset": offset,
            },
            "limit": limit,
            "offset": offset,
            "has_more": has_more,
        }

    def _normalize_event(self, item: dict[str, Any]) -> dict[str, Any]:
        return {
            "id": int(item["id"]),
            "event_at": item.get("event_at"),
            "actor_user_id": item.get("actor_user_id"),
            "actor_email": item.get("actor_email"),
            "actor_role": item.get("actor_role"),
            "request_ip": item.get("request_ip"),
            "request_user_agent": item.get("request_user_agent"),
            "action": str(item.get("action") or ""),
            "resource_type": str(item.get("resource_type") or ""),
            "resource_id": item.get("resource_id"),
            "resource_label": item.get("resource_label"),
            "status": str(item.get("status") or "success"),
            "message": item.get("message") or "",
            "metadata_json": item.get("metadata_json") or {},
        }

    def _parse_date_start(self, value: str) -> dt.datetime | None:
        text = value.strip()
        if not text:
            return None
        try:
            day = dt.date.fromisoformat(text)
        except ValueError as exc:
            raise ValueError(f"Invalid from date: {text}") from exc
        return dt.datetime.combine(day, dt.time.min, tzinfo=dt.timezone.utc)

    def _parse_date_end_exclusive(self, value: str) -> dt.datetime | None:
        text = value.strip()
        if not text:
            return None
        try:
            day = dt.date.fromisoformat(text)
        except ValueError as exc:
            raise ValueError(f"Invalid to date: {text}") from exc
        return dt.datetime.combine(day + dt.timedelta(days=1), dt.time.min, tzinfo=dt.timezone.utc)
