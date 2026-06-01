from __future__ import annotations

import datetime as dt
import unittest

try:
    from fastapi.testclient import TestClient
except ModuleNotFoundError:  # pragma: no cover - local env may not have web deps installed
    TestClient = None

if TestClient is not None:
    from web.app import app
    from web.dependencies import (
        get_ad_hoc_screen_service,
        get_auth_service,
        get_audit_service,
        get_backtest_service,
        get_current_principal,
        get_run_service,
        get_screener_history_service,
        get_user_admin_service,
        get_watchlist_service,
    )
    from src.webapp.access_control import principal_for_user, anonymous_principal


class _FakeAdHocService:
    def run(self, *, ticker: str, as_of_date: dt.date, screener_ids: list[str]):
        return {
            "ticker": ticker.upper(),
            "as_of_date": as_of_date.isoformat(),
            "screeners": [{"id": screener_ids[0], "passed": True, "error": None, "timing_ms": 1.2, "metrics": {}, "reasons": [], "hit": None}],
            "timing": {"total_ms": 1.2},
            "summary": {"requested_screener_count": len(screener_ids), "passed_screener_count": 1, "failed_screener_count": 0},
        }


class _FakeRunService:
    def launch(self, action_id: str, *, options: dict[str, object] | None = None):
        _ = options
        return f"{action_id}-job"

    def list_actions(self):
        return [{"id": "rs", "label": "Run RS"}]


class _FakeScreenerHistoryService:
    def is_configured(self):
        return True

    def list_runs(self, **_: object):
        return []

    def list_signal_cache_summary(self, **_: object):
        return []

    def get_run(self, *_: object, **__: object):
        return None

    def soft_delete(self, *_: object, **__: object):
        return True


class _FakeBacktestService:
    def default_templates(self):
        return []

    def list_runs(self, *, limit: int = 20):
        _ = limit
        return []


class _FakeAuthService:
    def request_magic_link(self, *, email: str, request_ip: str, request_user_agent: str):
        _ = (request_ip, request_user_agent)
        if not email:
            raise ValueError("Email is required.")
        return {"ok": True, "message": f"Sent sign-in link to {email.lower()}."}

    def request_premium_access(self, *, email: str):
        if not email:
            raise ValueError("Email is required.")
        return {"ok": True, "email": email.lower(), "message": "Premium access request submitted."}

    def verify_magic_link(self, *, token: str, request_ip: str, request_user_agent: str):
        _ = (request_ip, request_user_agent)
        if token != "valid-token":
            raise ValueError("Invalid or expired sign-in link.")
        principal = principal_for_user(user_id=7, email="admin@example.com", role="admin", is_active=True).to_dict()
        return {"principal": principal, "session_cookie_value": "signed-session"}


class _FakeAuditService:
    def is_configured(self):
        return True

    def record_event(self, **_: object):
        return {"id": 1}

    def list_events(self, **_: object):
        return {
            "events": [
                {
                    "id": 1,
                    "event_at": "2026-06-01T00:00:00+00:00",
                    "actor_email": "admin@example.com",
                    "actor_role": "admin",
                    "action": "admin.user.invite",
                    "resource_type": "user",
                    "resource_id": "9",
                    "resource_label": "user@example.com",
                    "status": "success",
                    "message": "Invited or updated user user@example.com.",
                    "metadata_json": {},
                }
            ],
            "filters": {"actorEmail": "", "action": "", "resourceType": "", "from": "", "to": "", "limit": 50, "offset": 0},
            "limit": 50,
            "offset": 0,
            "has_more": False,
        }


class _FakeUserAdminService:
    def list_users(self):
        return [{"id": 7, "email": "admin@example.com", "role": "admin", "is_active": True, "last_login_at": None}]

    def list_access_requests(self, *, status: str | None = None):
        _ = status
        return [{"id": 1, "email": "visitor@example.com", "requested_role": "premium", "status": "pending"}]

    def invite_or_create_user(self, *, email: str, role: str):
        return {"id": 9, "email": email.lower(), "role": role, "is_active": True}

    def update_role(self, *, user_id: int, role: str):
        return {"id": user_id, "email": "user@example.com", "role": role, "is_active": True}

    def deactivate(self, *, user_id: int):
        return {"id": user_id, "email": "user@example.com", "role": "premium", "is_active": False}

    def reactivate(self, *, user_id: int):
        return {"id": user_id, "email": "user@example.com", "role": "premium", "is_active": True}

    def approve_access_request(self, *, request_id: int, reviewed_by_user_id: int):
        _ = reviewed_by_user_id
        return {"id": request_id, "email": "visitor@example.com", "requested_role": "premium", "status": "approved"}

    def deny_access_request(self, *, request_id: int, reviewed_by_user_id: int, deny_reason: str = ""):
        _ = (reviewed_by_user_id, deny_reason)
        return {"id": request_id, "email": "visitor@example.com", "requested_role": "premium", "status": "denied"}


class _FakeWatchlistService:
    def list_recent(self):
        return []

    def get_watchlist_detail(self, stem: str):
        return {"stem": stem, "entry_count": 0, "entries": []}

    def get_chart_payload(self, ticker: str, period: str = "18mo", *, as_of_date: dt.date | None = None):
        return {
            "ticker": ticker.upper(),
            "benchmark_ticker": "SPY",
            "period": period,
            "requested_as_of_date": as_of_date.isoformat() if as_of_date else None,
            "resolved_as_of_date": "2026-05-30",
            "latest_available_date": "2026-05-30",
            "data_source": "internet",
            "candles": [{"time": "2026-05-30", "open": 10.0, "high": 11.0, "low": 9.5, "close": 10.5}],
            "volume": [{"time": "2026-05-30", "value": 1000}],
            "ma20": [],
            "ma50": [],
            "ma200": [],
            "ema8": [],
            "ema21": [],
            "weekly_ema8": [],
            "ipo_vwap": [],
            "rs_line": [],
            "rs_markers": [],
            "fearzone_panel": {"rows": [], "signals": []},
        }


@unittest.skipIf(TestClient is None, "fastapi test dependencies are not installed")
class ApiAdHocScreenTests(unittest.TestCase):
    def setUp(self) -> None:
        app.dependency_overrides[get_ad_hoc_screen_service] = lambda: _FakeAdHocService()
        app.dependency_overrides[get_run_service] = lambda: _FakeRunService()
        app.dependency_overrides[get_screener_history_service] = lambda: _FakeScreenerHistoryService()
        app.dependency_overrides[get_backtest_service] = lambda: _FakeBacktestService()
        app.dependency_overrides[get_auth_service] = lambda: _FakeAuthService()
        app.dependency_overrides[get_audit_service] = lambda: _FakeAuditService()
        app.dependency_overrides[get_user_admin_service] = lambda: _FakeUserAdminService()
        app.dependency_overrides[get_watchlist_service] = lambda: _FakeWatchlistService()
        app.dependency_overrides[get_current_principal] = anonymous_principal
        self.client = TestClient(app)

    def tearDown(self) -> None:
        app.dependency_overrides.clear()

    def test_post_ad_hoc_screen(self) -> None:
        response = self.client.post(
            "/api/ad-hoc-screen",
            json={"ticker": "aapl", "as_of_date": "2026-02-27", "screeners": ["rs"]},
        )
        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["ticker"], "AAPL")
        self.assertEqual(payload["screeners"][0]["id"], "rs")

    def test_post_ad_hoc_screen_requires_fields(self) -> None:
        response = self.client.post("/api/ad-hoc-screen", json={"ticker": "aapl"})
        self.assertEqual(response.status_code, 400)
        self.assertIn("as_of_date", response.json()["detail"])

    def test_get_standalone_chart(self) -> None:
        response = self.client.get("/api/charts/nvda?asOfDate=2026-05-31")
        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["ticker"], "NVDA")
        self.assertEqual(payload["requested_as_of_date"], "2026-05-31")
        self.assertEqual(payload["resolved_as_of_date"], "2026-05-30")

    def test_post_screener_runs_batch_requires_strategy_ids(self) -> None:
        response = self.client.post("/api/screener-runs/batch", json={"start_date": "2026-01-01", "end_date": "2026-01-31"})
        self.assertEqual(response.status_code, 401)

    def test_post_backtests_queues_job(self) -> None:
        app.dependency_overrides[get_current_principal] = lambda: principal_for_user(
            user_id=1,
            email="admin@example.com",
            role="admin",
            is_active=True,
        )
        response = self.client.post(
            "/api/backtests",
            json={
                "entry_rule": {"mode": "min_count_same_day", "screener_ids": ["rs"], "min_count": 1},
                "date_range": {"start_date": "2026-01-01", "end_date": "2026-01-31"},
                "exit_rules": [{"kind": "fixed_hold", "trading_days": 5}],
                "position_rules": {},
                "signal_cache_policy": "reuse_only",
                "market_data_mode": "database_only",
            },
        )
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["job_id"], "backtest_v1-job")

    def test_auth_me_is_anonymous_by_default(self) -> None:
        response = self.client.get("/api/auth/me")
        self.assertEqual(response.status_code, 200)
        self.assertFalse(response.json()["authenticated"])
        self.assertEqual(response.json()["role"], "visitor")

    def test_request_magic_link(self) -> None:
        response = self.client.post("/api/auth/request-link", json={"email": "Admin@Example.com"})
        self.assertEqual(response.status_code, 200)
        self.assertTrue(response.json()["ok"])

    def test_request_premium_access(self) -> None:
        response = self.client.post("/api/auth/request-premium", json={"email": "visitor@example.com"})
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["message"], "Premium access request submitted.")

    def test_verify_magic_link_sets_cookie(self) -> None:
        response = self.client.post("/api/auth/verify-link", json={"token": "valid-token"})
        self.assertEqual(response.status_code, 200)
        self.assertTrue(response.json()["authenticated"])
        self.assertIn("set-cookie", response.headers)

    def test_anonymous_cannot_launch_run(self) -> None:
        response = self.client.post("/api/runs/rs", json={})
        self.assertEqual(response.status_code, 401)

    def test_premium_can_launch_run(self) -> None:
        app.dependency_overrides[get_current_principal] = lambda: principal_for_user(
            user_id=2,
            email="premium@example.com",
            role="premium",
            is_active=True,
        )
        response = self.client.post("/api/runs/rs", json={})
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["job_id"], "rs-job")

    def test_premium_cannot_access_admin_users(self) -> None:
        app.dependency_overrides[get_current_principal] = lambda: principal_for_user(
            user_id=2,
            email="premium@example.com",
            role="premium",
            is_active=True,
        )
        response = self.client.get("/api/admin/users")
        self.assertEqual(response.status_code, 403)

    def test_admin_can_access_admin_users(self) -> None:
        app.dependency_overrides[get_current_principal] = lambda: principal_for_user(
            user_id=1,
            email="admin@example.com",
            role="admin",
            is_active=True,
        )
        response = self.client.get("/api/admin/users")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["users"][0]["email"], "admin@example.com")
        self.assertEqual(response.json()["access_requests"][0]["email"], "visitor@example.com")

    def test_admin_can_access_audit_events(self) -> None:
        app.dependency_overrides[get_current_principal] = lambda: principal_for_user(
            user_id=1,
            email="admin@example.com",
            role="admin",
            is_active=True,
        )
        response = self.client.get("/api/admin/audit-events")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["events"][0]["action"], "admin.user.invite")
