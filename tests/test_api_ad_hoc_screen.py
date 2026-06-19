from __future__ import annotations

import datetime as dt
from pathlib import Path
import tempfile
import unittest

try:
    from fastapi.testclient import TestClient
except ModuleNotFoundError:  # pragma: no cover - local env may not have web deps installed
    TestClient = None

if TestClient is not None:
    from web.app import app
    from web.dependencies import (
        get_ad_hoc_screen_service,
        get_admin_service,
        get_auth_service,
        get_audit_service,
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
    def __init__(self) -> None:
        self._temp_dir = tempfile.TemporaryDirectory()
        self.project_root = Path(self._temp_dir.name)
        log_dir = self.project_root / "artifacts" / "status"
        log_dir.mkdir(parents=True, exist_ok=True)
        self.log_path = log_dir / "rs-job.log"
        self.log_path.write_text("line one\nline two\n", encoding="utf-8")

    def launch(self, action_id: str, *, options: dict[str, object] | None = None):
        _ = options
        return f"{action_id}-job"

    def list_actions(self):
        return [{"id": "rs", "label": "Run RS"}]

    def list_jobs(self):
        return [
            {
                "job_id": "rs-job",
                "action_id": "rs",
                "label": "Run RS",
                "status": "success",
                "command": "python scripts/run_rs_screen.py",
                "started_at": "2026-06-15T00:00:00+00:00",
                "finished_at": "2026-06-15T00:01:00+00:00",
                "return_code": 0,
                "log_tail": "line one\nline two",
                "progress_current": 2,
                "progress_total": 2,
                "progress_percent": 100,
                "progress_label": "Completed",
                "success_count": 3,
                "watchlist_file": "",
                "watchlist_stem": "",
                "watchlist_url": "",
                "summary_file": "",
                "raw_results_file": "",
                "scan_target": "",
                "job_run_id": None,
                "screen_run_id": None,
                "backtest_run_id": None,
                "cancel_requested": False,
                "execution_mode": "local",
                "worker_name": "",
                "target_worker": "",
                "duration_seconds": 60,
                "child_jobs": [],
                "child_job_summary": {"total": 0, "running": 0, "success": 0, "failed": 0, "cancelled": 0},
            }
        ]

    def get_job(self, job_id: str):
        return {
            "job_id": job_id,
            "action_id": "rs",
            "label": "Run RS",
            "status": "success",
            "command": "python scripts/run_rs_screen.py",
            "started_at": "2026-06-15T00:00:00+00:00",
            "finished_at": "2026-06-15T00:01:00+00:00",
            "return_code": 0,
            "log_tail": "line one\nline two",
            "progress_current": 2,
            "progress_total": 2,
            "progress_percent": 100,
            "progress_label": "Completed",
            "success_count": 3,
            "watchlist_file": "",
            "watchlist_stem": "",
            "watchlist_url": "",
            "summary_file": "",
            "raw_results_file": "",
            "scan_target": "",
            "job_run_id": None,
            "screen_run_id": None,
            "backtest_run_id": None,
            "cancel_requested": False,
            "execution_mode": "local",
            "worker_name": "",
            "target_worker": "",
            "duration_seconds": 60,
            "child_jobs": [],
            "child_job_summary": {"total": 0, "running": 0, "success": 0, "failed": 0, "cancelled": 0},
            "log_file": str(self.log_path),
        }

    def get_child_job(self, child_job_run_id: int):
        return {
            "job_run_id": child_job_run_id,
            "parent_job_run_id": 1,
            "job_type": "screen_run",
            "label": "Run RS (2026-06-15)",
            "status": "success",
            "started_at": "2026-06-15T00:00:00+00:00",
            "finished_at": "2026-06-15T00:01:00+00:00",
            "artifact_path": "",
            "command": "python scripts/run_rs_screen.py",
            "strategy_id": "rs",
            "run_date": "2026-06-15",
            "screen_run_id": 77,
            "success_count": 3,
            "summary_file": "",
            "watchlist_file": "",
            "raw_results_file": "",
            "log_tail": "line one\nline two",
            "log_file": str(self.log_path),
            "message": "Done",
            "skipped": False,
            "progress_current": 2,
            "progress_total": 2,
            "progress_percent": 100,
            "progress_label": "Completed",
            "duration_seconds": 60,
        }


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


class _FakeAdminService:
    def get_context(self, *, coverage_start: str = "2020-01-01"):
        return {
            "excluded_tickers": [],
            "excluded_count": 0,
            "included_tickers": [],
            "included_count": 0,
            "database_status": {
                "database_configured": True,
                "coverage_start": coverage_start,
                "coverage_end": "2026-06-14",
                "target_universe_count": 2,
                "db_ticker_count": 2,
                "covered_ticker_count": 2,
                "partial_ticker_count": 0,
                "missing_ticker_count": 0,
                "total_bar_rows": 10,
                "overall_first_trade_date": "2020-01-01",
                "overall_last_trade_date": "2026-06-14",
                "latest_metadata_update_at": "2026-06-14T00:00:00+00:00",
                "stale_ticker_count": 0,
                "coverage_percent": 100,
                "sample_missing_tickers": [],
                "sample_partial_tickers": [],
                "notes": [],
            },
        }

    def get_ratings_status(self):
        return {
            "database_configured": True,
            "target_universe_count": 2,
            "latest_fundamentals_as_of_date": "2026-06-13",
            "latest_fundamentals_updated_at": "2026-06-13T00:00:00+00:00",
            "latest_baselines_as_of_date": "2026-06-13",
            "latest_baselines_updated_at": "2026-06-13T00:00:00+00:00",
            "latest_ratings_as_of_date": "2026-06-13",
            "latest_ratings_updated_at": "2026-06-13T00:00:00+00:00",
            "latest_fundamentals_snapshot_count": 2,
            "latest_rating_snapshot_count": 2,
            "latest_fundamentals_parse_status_counts": {"ok": 2},
            "latest_rating_status_counts": {"ok": 2},
            "tickers_with_any_fundamentals": 2,
            "tickers_with_latest_ok_rating": 2,
            "diagnostics_count": 0,
            "diagnostic_category_counts": {},
            "diagnostics": [],
            "healthy_remote_worker_count": 0,
            "remote_workers": [],
            "notes": [],
        }

    def get_partial_ticker_detail(self, *, ticker: str, coverage_start: str = "2020-01-01"):
        return {
            "ticker": ticker.upper(),
            "coverage_start": coverage_start,
            "coverage_end": "2026-06-14",
            "first_trade_date": "2020-01-01",
            "last_trade_date": "2026-06-14",
            "bar_count": 10,
            "missing_ranges": [],
            "missing_date_count": 0,
            "sample_missing_dates": [],
        }

    def get_ticker_list_status(self, *, ticker: str):
        return {
            "ticker": ticker.upper(),
            "is_excluded": False,
            "is_included": False,
            "exclusion_entry": None,
            "inclusion_entry": None,
        }

    def get_missing_sector_context(self):
        return {
            "database_configured": True,
            "missing_count": 1,
            "tickers": [
                {
                    "ticker": "NVDA",
                    "exchange": "NASDAQ",
                    "industry": "Semiconductors",
                    "source": "finviz",
                    "updated_at": "2026-06-13T00:00:00+00:00",
                    "suggested_sector": "Technology",
                    "suggested_industry": "Semiconductors",
                }
            ],
            "available_sectors": ["Finance", "Technology"],
            "notes": ["1 tickers still need sector assignment."],
        }

    def update_ticker_sector(self, *, ticker: str, sector: str):
        return {"ticker": ticker.upper(), "sector": sector, "source": "finviz", "updated_at": "2026-06-14T00:00:00+00:00"}


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

    def get_scanner_board(self):
        return {
            "generated_at": "2026-06-13T03:00:00Z",
            "reference_now_new_york": "2026-06-12T23:00:00-04:00",
            "target_trading_date": "2026-06-12",
            "cutoff_time_label": "20:30 America/New_York",
            "latest_update_at": "2026-06-13T01:00:00+00:00",
            "latest_signal_date": "2026-06-12",
            "cards": [
                {
                    "id": "rs",
                    "strategy_id": "rs",
                    "label": "RS New High Before Price",
                    "description": "Daily RS leaders",
                    "timeframe": "Daily",
                    "accent": "cyan",
                    "available": True,
                    "stem": "rs_new_high_before_price_2026-06-12",
                    "group_label": "RS",
                    "captured_at": "2026-06-13T00:55:00+00:00",
                    "sort_date": "2026-06-12",
                    "entry_count": 4,
                    "preview_tickers": ["AAPL", "CRWD"],
                    "list_href": "/watchlists?stem=rs_new_high_before_price_2026-06-12",
                },
                {
                    "id": "weekly_rs",
                    "strategy_id": "weekly_rs",
                    "label": "Weekly RS New High",
                    "description": "Leaders",
                    "timeframe": "Weekly",
                    "accent": "violet",
                    "available": True,
                    "stem": "weekly_rs_new_high_2026-06-12",
                    "group_label": "Weekly RS",
                    "captured_at": "2026-06-13T01:00:00+00:00",
                    "sort_date": "2026-06-12",
                    "entry_count": 6,
                    "preview_tickers": ["NVDA", "MSFT"],
                    "list_href": "/watchlists?stem=weekly_rs_new_high_2026-06-12",
                }
            ],
        }

    def get_watchlist_detail(self, stem: str):
        return {"stem": stem, "entry_count": 0, "entries": []}

    def get_chart_payload(
        self,
        ticker: str,
        period: str = "18mo",
        *,
        as_of_date: dt.date | None = None,
        include_setup_markers: bool = False,
    ):
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
            "market_extension": {"config": {"timeframe": "weekly", "ma_type": "sma", "length": 10, "warning_pct": 11.0, "extreme_pct": 15.0, "label": "10W SMA"}, "line": [], "signals": [], "latest": None},
            "rs_line": [],
            "daily_rs_rating": [],
            "weekly_rs_rating": [],
            "rs_markers": [],
            "setup_markers": [],
            "fearzone_panel": {"rows": [], "signals": []},
            "vcs": None,
            "sepa_dashboard": None,
        }

    def get_chart_overlays_payload(
        self,
        ticker: str,
        period: str = "18mo",
        *,
        as_of_date: dt.date | None = None,
        include_setup_markers: bool = False,
    ):
        return {
            "ticker": ticker.upper(),
            "benchmark_ticker": "SPY",
            "period": period,
            "requested_as_of_date": as_of_date.isoformat() if as_of_date else None,
            "resolved_as_of_date": "2026-05-30",
            "latest_available_date": "2026-05-30",
            "data_source": "database",
            "market_extension": {"config": {"timeframe": "weekly", "ma_type": "sma", "length": 10, "warning_pct": 11.0, "extreme_pct": 15.0, "label": "10W SMA"}, "line": [], "signals": [], "latest": None},
            "rs_line": [],
            "daily_rs_rating": [],
            "weekly_rs_rating": [],
            "rs_markers": [],
            "setup_markers": [{"time": "2026-05-30", "kind": "ftd_sweep_breakout", "label": "FTD Sweep"}] if include_setup_markers else [],
            "fearzone_panel": {"rows": [], "signals": []},
            "vcs": None,
            "sepa_dashboard": None,
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
        app.dependency_overrides[get_admin_service] = lambda: _FakeAdminService()
        app.dependency_overrides[get_run_service] = lambda: _FakeRunService()
        app.dependency_overrides[get_screener_history_service] = lambda: _FakeScreenerHistoryService()
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

    def test_get_chart_overlays(self) -> None:
        response = self.client.get("/api/chart-overlays/nvda?asOfDate=2026-05-31&includeSetupMarkers=true")
        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["ticker"], "NVDA")
        self.assertEqual(payload["setup_markers"][0]["kind"], "ftd_sweep_breakout")

    def test_get_scanner_board(self) -> None:
        app.dependency_overrides[get_current_principal] = lambda: premium_principal()

        response = self.client.get("/api/scanner-board")
        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["target_trading_date"], "2026-06-12")
        cards = {item["id"]: item for item in payload["cards"]}
        self.assertEqual(cards["weekly_rs"]["entry_count"], 6)
        self.assertEqual(cards["rs"]["stem"], "rs_new_high_before_price_2026-06-12")

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

    def test_premium_can_stream_job_log(self) -> None:
        app.dependency_overrides[get_current_principal] = lambda: principal_for_user(
            user_id=2,
            email="premium@example.com",
            role="premium",
            is_active=True,
        )

        with self.client.stream("GET", "/api/jobs/rs-job/stream?cursor=0") as response:
            body = response.text

        self.assertEqual(response.status_code, 200)
        self.assertIn("event: snapshot", body)
        self.assertIn("event: log", body)
        self.assertIn('"line":"line one"', body)
        self.assertIn('"line":"line two"', body)
        self.assertIn("event: eof", body)

    def test_premium_can_stream_jobs_snapshot(self) -> None:
        app.dependency_overrides[get_current_principal] = lambda: principal_for_user(
            user_id=2,
            email="premium@example.com",
            role="premium",
            is_active=True,
        )

        with self.client.stream("GET", "/api/jobs/stream") as response:
            body = response.text

        self.assertEqual(response.status_code, 200)
        self.assertIn("event: snapshot", body)
        self.assertIn('"job_id":"rs-job"', body)
        self.assertIn('"id":"rs"', body)

    def test_premium_can_stream_child_job_log(self) -> None:
        app.dependency_overrides[get_current_principal] = lambda: principal_for_user(
            user_id=2,
            email="premium@example.com",
            role="premium",
            is_active=True,
        )

        with self.client.stream("GET", "/api/child-jobs/501/stream?cursor=0") as response:
            body = response.text

        self.assertEqual(response.status_code, 200)
        self.assertIn("event: snapshot", body)
        self.assertIn("event: log", body)
        self.assertIn('"line":"line one"', body)
        self.assertIn('"line":"line two"', body)
        self.assertIn("event: eof", body)

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

    def test_admin_can_access_missing_sector_list(self) -> None:
        app.dependency_overrides[get_current_principal] = lambda: principal_for_user(
            user_id=1,
            email="admin@example.com",
            role="admin",
            is_active=True,
        )
        response = self.client.get("/api/admin/missing-sectors")
        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["missing_count"], 1)
        self.assertEqual(payload["tickers"][0]["ticker"], "NVDA")

    def test_admin_can_update_ticker_sector(self) -> None:
        app.dependency_overrides[get_current_principal] = lambda: principal_for_user(
            user_id=1,
            email="admin@example.com",
            role="admin",
            is_active=True,
        )
        response = self.client.post("/api/admin/ticker-sectors/NVDA", json={"sector": "Technology"})
        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertTrue(payload["ok"])
        self.assertEqual(payload["entry"]["ticker"], "NVDA")
        self.assertEqual(payload["entry"]["sector"], "Technology")

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
