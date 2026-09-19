from __future__ import annotations

from pathlib import Path
import tempfile
import unittest

try:
    from fastapi.testclient import TestClient
except ModuleNotFoundError:  # pragma: no cover
    TestClient = None

if TestClient is not None:
    from src.webapp.access_control import anonymous_principal, principal_for_user
    from src.webapp.services.daily_report_service import DailyReportService
    from web.app import app
    from web.dependencies import get_current_principal, get_daily_report_service


@unittest.skipIf(TestClient is None, "FastAPI test dependencies are unavailable")
class DailyReportApiTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.service = DailyReportService(artifacts_dir=Path(self.temp_dir.name))
        app.dependency_overrides[get_daily_report_service] = lambda: self.service
        app.dependency_overrides[get_current_principal] = lambda: principal_for_user(
            user_id=7,
            email="admin@example.com",
            role="admin",
            is_active=True,
        )
        self.client = TestClient(app)

    def tearDown(self) -> None:
        app.dependency_overrides.clear()
        self.temp_dir.cleanup()

    def test_admin_can_ingest_and_member_can_read_dated_report(self) -> None:
        response = self.client.post(
            "/api/daily-reports",
            json={
                "report_date": "2026-09-18",
                "agent_id": "vcp-analyst",
                "target_trading_date": "2026-09-17",
                "candidates": [{"ticker": "NVDA", "group": "B", "score": 88}],
            },
        )
        self.assertEqual(response.status_code, 201)

        app.dependency_overrides[get_current_principal] = lambda: principal_for_user(
            user_id=8,
            email="premium@example.com",
            role="premium",
            is_active=True,
        )
        listing = self.client.get("/api/daily-reports")
        detail = self.client.get("/api/daily-reports/2026-09-18/vcp-analyst")
        self.assertEqual(listing.status_code, 200)
        self.assertEqual(listing.json()["reports"][0]["top_tickers"], ["NVDA"])
        self.assertEqual(detail.status_code, 200)
        self.assertEqual(detail.json()["candidates"][0]["ticker"], "NVDA")

    def test_anonymous_cannot_read_or_ingest(self) -> None:
        app.dependency_overrides[get_current_principal] = anonymous_principal
        self.assertEqual(self.client.get("/api/daily-reports").status_code, 401)
        self.assertEqual(
            self.client.post(
                "/api/daily-reports",
                json={"report_date": "2026-09-18", "agent_id": "vcp-analyst", "candidates": []},
            ).status_code,
            401,
        )


if __name__ == "__main__":
    unittest.main()
