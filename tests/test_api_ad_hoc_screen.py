from __future__ import annotations

import datetime as dt
import unittest

try:
    from fastapi.testclient import TestClient
except ModuleNotFoundError:  # pragma: no cover - local env may not have web deps installed
    TestClient = None

if TestClient is not None:
    from web.app import app
    from web.dependencies import get_ad_hoc_screen_service, get_backtest_service, get_run_service, get_screener_history_service


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


@unittest.skipIf(TestClient is None, "fastapi test dependencies are not installed")
class ApiAdHocScreenTests(unittest.TestCase):
    def setUp(self) -> None:
        app.dependency_overrides[get_ad_hoc_screen_service] = lambda: _FakeAdHocService()
        app.dependency_overrides[get_run_service] = lambda: _FakeRunService()
        app.dependency_overrides[get_screener_history_service] = lambda: _FakeScreenerHistoryService()
        app.dependency_overrides[get_backtest_service] = lambda: _FakeBacktestService()
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

    def test_post_screener_runs_batch_requires_strategy_ids(self) -> None:
        response = self.client.post("/api/screener-runs/batch", json={"start_date": "2026-01-01", "end_date": "2026-01-31"})
        self.assertEqual(response.status_code, 400)
        self.assertIn("strategy_ids", response.json()["detail"])

    def test_post_backtests_queues_job(self) -> None:
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
