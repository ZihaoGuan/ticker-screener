from __future__ import annotations

import unittest

from src.webapp.access_control import anonymous_principal, principal_for_user
from src.webapp.services.audit_service import AuditService


class _FakeAuditRepository:
    def __init__(self) -> None:
        self.events: list[dict[str, object]] = []

    def is_configured(self) -> bool:
        return True

    def create_event(self, **kwargs: object):
        event = {"id": len(self.events) + 1, "event_at": "2026-06-01T00:00:00+00:00", **kwargs}
        self.events.append(event)
        return event

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
    ):
        _ = (from_date, to_date)
        items = list(self.events)
        if actor_email:
            items = [item for item in items if str(item.get("actor_email") or "").lower() == actor_email.lower()]
        if action:
            items = [item for item in items if item.get("action") == action]
        if resource_type:
            items = [item for item in items if item.get("resource_type") == resource_type]
        return items[offset : offset + limit]


class AuditServiceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.repository = _FakeAuditRepository()
        self.service = AuditService(repository=self.repository)  # type: ignore[arg-type]

    def test_record_event_uses_principal_defaults(self) -> None:
        principal = principal_for_user(user_id=1, email="admin@example.com", role="admin", is_active=True)

        result = self.service.record_event(
            principal=principal,
            request=None,
            action="admin.user.invite",
            resource_type="user",
            resource_id="7",
            resource_label="user@example.com",
            message="Invited user.",
            metadata={"role": "premium"},
        )

        self.assertIsNotNone(result)
        self.assertEqual(self.repository.events[0]["actor_email"], "admin@example.com")
        self.assertEqual(self.repository.events[0]["actor_role"], "admin")

    def test_record_event_allows_override_for_visitor_action(self) -> None:
        result = self.service.record_event(
            principal=anonymous_principal(),
            request=None,
            action="auth.request_premium",
            resource_type="access_request",
            resource_id="visitor@example.com",
            resource_label="visitor@example.com",
            message="Premium access request submitted.",
            metadata={"requested_role": "premium"},
            actor_email_override="visitor@example.com",
            actor_role_override="visitor",
        )

        self.assertIsNotNone(result)
        self.assertEqual(self.repository.events[0]["actor_email"], "visitor@example.com")
        self.assertEqual(self.repository.events[0]["actor_role"], "visitor")

    def test_list_events_filters_and_has_more(self) -> None:
        self.repository.create_event(
            actor_user_id=1,
            actor_email="admin@example.com",
            actor_role="admin",
            request_ip="",
            request_user_agent="",
            action="admin.user.invite",
            resource_type="user",
            resource_id="7",
            resource_label="a@example.com",
            status="success",
            message="Invited A.",
            metadata_json={},
        )
        self.repository.create_event(
            actor_user_id=1,
            actor_email="admin@example.com",
            actor_role="admin",
            request_ip="",
            request_user_agent="",
            action="admin.user.invite",
            resource_type="user",
            resource_id="8",
            resource_label="b@example.com",
            status="success",
            message="Invited B.",
            metadata_json={},
        )

        payload = self.service.list_events(actor_email="admin@example.com", action="admin.user.invite", limit=1, offset=0)

        self.assertEqual(len(payload["events"]), 1)
        self.assertTrue(payload["has_more"])


if __name__ == "__main__":
    unittest.main()
