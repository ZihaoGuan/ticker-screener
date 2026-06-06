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
        get_earnings_calendar_service,
        get_portfolio_service,
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

    def get_chart_insider_payload(self, ticker: str, *, lookback_days: int = 14, as_of_date: dt.date | None = None):
        return {
            "ticker": ticker.upper(),
            "requested_as_of_date": as_of_date.isoformat() if as_of_date else None,
            "resolved_as_of_date": (as_of_date or dt.date(2026, 5, 30)).isoformat(),
            "lookback_days": lookback_days,
            "window_start_date": "2026-05-16",
            "window_end_date": (as_of_date or dt.date(2026, 5, 30)).isoformat(),
            "generated_at": "2026-06-02T00:00:00+00:00",
            "cache_status": "miss",
            "fetch_status": "fetched",
            "notice": None,
            "entries": [
                {
                    "ticker": ticker.upper(),
                    "filing_date": "2026-05-29",
                    "transaction_date": "2026-05-28",
                    "owner_name": "Jane Insider",
                    "position": "Officer, CEO",
                    "type": "BUY",
                    "shares": 1000,
                    "price": 10.25,
                    "gross_amount": 10250.0,
                    "net_amount": 10250.0,
                    "shares_owned_after": 15000,
                    "is_10b5_1": False,
                    "source_url": "https://www.sec.gov/Archives/example.xml",
                }
            ],
            "summary": {
                "total_count": 1,
                "buy_count": 1,
                "sell_count": 0,
                "total_buy_amount": 10250.0,
                "total_sell_amount": 0.0,
                "net_amount": 10250.0,
            },
        }


class _FakeEarningsCalendarService:
    def get_next_week_calendar(
        self,
        *,
        reference_date: dt.date | None = None,
        exclude_sectors: list[str] | None = None,
        exclude_industries: list[str] | None = None,
        only_criteria: bool = False,
    ):
        _ = (reference_date, exclude_sectors, exclude_industries, only_criteria)
        return {
            "week_start": "2026-06-07",
            "week_end": "2026-06-13",
            "reference_date": "2026-06-02",
            "days": [
                {"date": "2026-06-07", "weekday": "Sun", "before_market": [], "after_market": [], "during_market": [], "unknown": []},
                {
                    "date": "2026-06-08",
                    "weekday": "Mon",
                    "before_market": [{
                        "ticker": "AAA",
                        "date": "2026-06-08",
                        "session": "before_market",
                        "summary": "Before market open",
                        "sector": "Tech",
                        "industry": "Software",
                        "exchange": "NASDAQ",
                        "criteria": {
                            "passed": True,
                            "criteria": {"bullish_ma_stack": True, "revenue_yoy_ge_100": True, "eps_improving_last_4": True},
                            "matched_criteria": ["bullish_ma_stack", "revenue_yoy_ge_100", "eps_improving_last_4"],
                            "not_matched_criteria": [],
                            "pass_mode": "loose",
                            "error": "",
                        },
                    }],
                    "after_market": [{
                        "ticker": "BBB",
                        "date": "2026-06-08",
                        "session": "after_market",
                        "summary": "After market close",
                        "sector": "Health Care",
                        "industry": "Biotech",
                        "exchange": "NASDAQ",
                        "criteria": {
                            "passed": False,
                            "criteria": {"bullish_ma_stack": True, "revenue_yoy_ge_100": False, "eps_improving_last_4": True},
                            "matched_criteria": ["bullish_ma_stack", "eps_improving_last_4"],
                            "not_matched_criteria": ["revenue_yoy_ge_100"],
                            "pass_mode": "loose",
                            "error": "criteria_not_met",
                        },
                    }],
                    "during_market": [],
                    "unknown": [],
                },
                {"date": "2026-06-09", "weekday": "Tue", "before_market": [], "after_market": [], "during_market": [], "unknown": []},
                {"date": "2026-06-10", "weekday": "Wed", "before_market": [], "after_market": [], "during_market": [], "unknown": []},
                {"date": "2026-06-11", "weekday": "Thu", "before_market": [], "after_market": [], "during_market": [], "unknown": []},
                {"date": "2026-06-12", "weekday": "Fri", "before_market": [], "after_market": [], "during_market": [], "unknown": []},
                {"date": "2026-06-13", "weekday": "Sat", "before_market": [], "after_market": [], "during_market": [], "unknown": []},
            ],
            "filters": {"exclude_sectors": [], "exclude_industries": [], "only_criteria": only_criteria},
            "available_sectors": ["Health Care", "Tech"],
            "available_industries": ["Biotech", "Software"],
            "criteria_filter": {
                "enabled": only_criteria,
                "available": True,
                "strategy_id": "earnings_weekly_criteria",
                "run_id": 11,
                "run_date": "2026-06-01",
                "matched_count": 2,
            },
        }

    def get_chart_fundamentals_payload(self, ticker: str, *, earnings_limit: int = 4):
        return {
            "ticker": ticker.upper(),
            "earnings_eps_history": [
                {
                    "date": "2026-05-28",
                    "eps_estimate": 1.23,
                    "reported_eps": 1.45,
                    "surprise_pct": 17.89,
                }
            ][:earnings_limit],
            "holders_float_held_by_institutions_pct": 79.25,
            "revenue_yoy_pct": 85.2,
            "earnings_yoy_pct": 210.6,
            "implied_move": {
                "strike": 225.0,
                "straddle_mid": 6.70,
                "dollar_move": 6.70,
                "percent_move": 2.99,
            },
            "diagnostics": {
                "earnings": {"status": "ok", "attempts": [{"url": "https://finance.yahoo.com/calendar/earnings?symbol=NVDA"}]},
                "holders": {"status": "ok", "attempts": [{"url": "https://finance.yahoo.com/quote/NVDA/holders"}]},
                "statistics": {"status": "ok", "attempts": [{"url": "https://nz.finance.yahoo.com/quote/NVDA/key-statistics/"}]},
                "options": {"status": "ok", "attempts": [{"url": "https://nz.finance.yahoo.com/quote/NVDA/options/"}]},
            },
        }


class _FakePortfolioService:
    def get_context(self):
        return {
            "database_configured": True,
            "summary": {
                "position_count": 1,
                "total_market_value": 1400.0,
                "total_cost_basis": 1000.0,
                "total_unrealized_pl": 400.0,
                "total_unrealized_pl_pct": 40.0,
                "stale_advice_count": 0,
                "missing_advice_count": 0,
                "last_refreshed_at": "2026-06-06T02:00:00+00:00",
            },
            "positions": [
                {
                    "id": 1,
                    "portfolio_id": 1,
                    "portfolio_name": "Main",
                    "ticker": "NVDA",
                    "shares": 10,
                    "entry_price": 100,
                    "opened_at": "2026-05-01",
                    "notes": "",
                    "created_at": "2026-06-06T00:00:00+00:00",
                    "updated_at": "2026-06-06T00:00:00+00:00",
                    "seed_shares": 10,
                    "seed_entry_price": 100,
                    "realized_pl": 95.0,
                    "is_closed": False,
                    "market_value": 1400.0,
                    "unrealized_pl": 400.0,
                    "unrealized_pl_pct": 40.0,
                    "transactions": [
                        {
                            "id": 77,
                            "position_id": 1,
                            "trade_date": "2026-05-20",
                            "side": "buy",
                            "shares": 2,
                            "price": 120.0,
                            "fees": 0.0,
                            "notes": "added",
                            "created_at": "2026-06-06T00:00:00+00:00",
                        }
                    ],
                    "advice": {
                        "as_of_date": "2026-06-06",
                        "latest_trade_date": "2026-06-06",
                        "market_data_status": "ready",
                        "close_price": 140.0,
                        "signal_status": "hold",
                        "stop_loss_price": 128.0,
                        "tp1_price": 152.0,
                        "tp2_price": 166.0,
                        "tp1_sell_fraction": 0.4,
                        "tp2_sell_fraction": 0.6,
                        "net_cost_after_tp1": 65.33,
                        "remaining_cost_basis_after_tp1": 392.0,
                        "explanation": "Trend context available.",
                        "data_source": "database",
                        "signal_context": {},
                        "refreshed_at": "2026-06-06T02:00:00+00:00",
                    },
                }
            ],
            "portfolios": [{"id": 1, "name": "Main", "created_by_user_id": 1, "created_at": "2026-06-06T00:00:00+00:00", "updated_at": "2026-06-06T00:00:00+00:00"}],
            "market_regime": {"title": "Market Regime Placeholder", "status": "deferred", "description": "placeholder"},
        }

    def create_position(self, **kwargs: object):
        return {
            "id": 2,
            "portfolio_id": 1,
            "portfolio_name": str(kwargs.get("portfolio_name") or "Main"),
            "ticker": "AAPL",
            "shares": 5.0,
            "entry_price": 200.0,
            "opened_at": "2026-05-20",
            "notes": str(kwargs.get("notes") or ""),
            "created_at": "2026-06-06T00:00:00+00:00",
            "updated_at": "2026-06-06T00:00:00+00:00",
            "market_value": None,
            "unrealized_pl": None,
            "unrealized_pl_pct": None,
            "advice": {"market_data_status": "pending"},
        }

    def import_csv(self, **kwargs: object):
        _ = kwargs
        return {
            "ok": True,
            "portfolio_name": "Main",
            "import_batch_id": 9,
            "accepted_count": 1,
            "error_count": 1,
            "accepted": [{"row": 2, "position": {"ticker": "MSFT"}}],
            "errors": [{"row": 3, "message": "shares must be greater than 0."}],
        }

    def refresh_advice(self, **kwargs: object):
        _ = kwargs
        return {"ok": True, "refreshed_count": 1, "positions": [{"position_id": 1, "ticker": "NVDA"}]}

    def update_position(self, position_id: int, **kwargs: object):
        _ = kwargs
        return {
            "id": position_id,
            "portfolio_id": 1,
            "portfolio_name": "Main",
            "ticker": "NVDA",
            "shares": 12.0,
            "entry_price": 110.0,
            "opened_at": "2026-05-01",
            "notes": "",
            "created_at": "2026-06-06T00:00:00+00:00",
            "updated_at": "2026-06-06T01:00:00+00:00",
            "market_value": None,
            "unrealized_pl": None,
            "unrealized_pl_pct": None,
            "advice": {"market_data_status": "pending"},
        }

    def delete_position(self, position_id: int):
        _ = position_id
        return None

    def record_transaction(self, position_id: int, **kwargs: object):
        return {
            "id": 88,
            "position_id": position_id,
            "trade_date": str(kwargs.get("trade_date") or "2026-06-06"),
            "side": str(kwargs.get("side") or "buy"),
            "shares": float(kwargs.get("shares") or 1),
            "price": float(kwargs.get("price") or 100),
            "fees": float(kwargs.get("fees") or 0),
            "notes": str(kwargs.get("notes") or ""),
            "created_at": "2026-06-06T00:00:00+00:00",
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
        app.dependency_overrides[get_earnings_calendar_service] = lambda: _FakeEarningsCalendarService()
        app.dependency_overrides[get_portfolio_service] = lambda: _FakePortfolioService()
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

    def test_get_chart_fundamentals(self) -> None:
        response = self.client.get("/api/chart-fundamentals/nvda?earningsLimit=1")
        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["ticker"], "NVDA")
        self.assertEqual(len(payload["earnings_eps_history"]), 1)
        self.assertEqual(payload["holders_float_held_by_institutions_pct"], 79.25)
        self.assertEqual(payload["revenue_yoy_pct"], 85.2)
        self.assertEqual(payload["earnings_yoy_pct"], 210.6)
        self.assertEqual(payload["diagnostics"]["earnings"]["status"], "ok")
        self.assertEqual(payload["diagnostics"]["statistics"]["status"], "ok")
        self.assertEqual(payload["implied_move"]["percent_move"], 2.99)
        self.assertEqual(payload["diagnostics"]["options"]["status"], "ok")

    def test_get_chart_insider(self) -> None:
        response = self.client.get("/api/chart-insider/nvda?lookbackDays=14&asOfDate=2026-05-30")
        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["ticker"], "NVDA")
        self.assertEqual(payload["lookback_days"], 14)
        self.assertEqual(payload["requested_as_of_date"], "2026-05-30")
        self.assertEqual(payload["entries"][0]["owner_name"], "Jane Insider")
        self.assertEqual(payload["summary"]["net_amount"], 10250.0)

    def test_get_earnings_calendar(self) -> None:
        response = self.client.get("/api/earnings-calendar?excludeSector=Energy&onlyCriteria=true")
        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["week_start"], "2026-06-07")
        self.assertEqual(len(payload["days"]), 7)
        self.assertEqual(payload["days"][1]["before_market"][0]["ticker"], "AAA")
        self.assertEqual(payload["days"][1]["after_market"][0]["ticker"], "BBB")
        self.assertTrue(payload["filters"]["only_criteria"])
        self.assertEqual(payload["criteria_filter"]["strategy_id"], "earnings_weekly_criteria")

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

    def test_premium_cannot_access_portfolio(self) -> None:
        app.dependency_overrides[get_current_principal] = lambda: principal_for_user(
            user_id=2,
            email="premium@example.com",
            role="premium",
            is_active=True,
        )
        response = self.client.get("/api/admin/portfolio")
        self.assertEqual(response.status_code, 403)

    def test_admin_can_access_portfolio(self) -> None:
        app.dependency_overrides[get_current_principal] = lambda: principal_for_user(
            user_id=1,
            email="admin@example.com",
            role="admin",
            is_active=True,
        )
        response = self.client.get("/api/admin/portfolio")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["positions"][0]["ticker"], "NVDA")

    def test_admin_can_import_portfolio_positions(self) -> None:
        app.dependency_overrides[get_current_principal] = lambda: principal_for_user(
            user_id=1,
            email="admin@example.com",
            role="admin",
            is_active=True,
        )
        response = self.client.post(
            "/api/admin/portfolio/positions/import",
            json={"csv_text": "ticker,shares,entry_price,opened_at\nMSFT,4,400,2026-05-01\n", "portfolio_name": "Main"},
        )
        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["accepted_count"], 1)
        self.assertEqual(payload["error_count"], 1)

    def test_admin_can_record_portfolio_transaction(self) -> None:
        app.dependency_overrides[get_current_principal] = lambda: principal_for_user(
            user_id=1,
            email="admin@example.com",
            role="admin",
            is_active=True,
        )
        response = self.client.post(
            "/api/admin/portfolio/positions/1/transactions",
            json={"side": "sell", "shares": 2, "price": 145.5, "trade_date": "2026-06-06", "fees": 1.25},
        )
        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["transaction"]["side"], "sell")
        self.assertEqual(payload["transaction"]["position_id"], 1)
