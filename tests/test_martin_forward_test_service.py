from __future__ import annotations

import unittest
import datetime as dt

import pandas as pd

from src.webapp.services.martin_forward_test_service import MartinForwardTestService, classify_candidate, order_outcome, position_exit


class MartinForwardTestRulesTest(unittest.TestCase):
    def setUp(self) -> None:
        self.row = {"ticker": "ABC", "scanner_ids": ["rs"], "scanners": [{"id": "rs"}], "market_cap": 2_000_000_000, "day_close": 20,
                    "daily_rs_rating": 95, "trade_readiness": "defined_risk", "adr14_pct": 4,
                    "trade_plan": {"trigger_price": 20, "stop_price": 19, "max_entry_price": 21}}

    def test_filters_and_adr_adjusted_max_entry(self) -> None:
        item = classify_candidate(self.row, 1)
        self.assertEqual(item["status"], "pending")
        self.assertAlmostEqual(item["limit_price"], 19 / 0.98)
        self.assertEqual(classify_candidate({**self.row, "daily_rs_rating": 80}, 1)["status"], "blocked")
        self.assertEqual(classify_candidate({**self.row, "trade_readiness": "leader"}, 1)["status"], "watch")
        self.assertEqual(classify_candidate({**self.row, "trade_readiness": "avoid"}, 1)["status"], "avoid")
        self.assertEqual(classify_candidate({**self.row, "adr14_pct": None}, 1)["status"], "blocked_data")
        self.assertEqual(classify_candidate({**self.row, "scanner_ids": ["unrelated"], "scanners": []}, 1)["status"], "blocked")

    def test_order_fill_cancel_expiry_and_missing_bar(self) -> None:
        order = {"stop": 19, "limit_price": 20}
        self.assertEqual(order_outcome(order, {"open": 19, "low": 18}), ("cancelled", None, "opened_at_or_below_stop"))
        self.assertEqual(order_outcome(order, {"open": 19.5, "low": 19}), ("filled", 19.5, None))
        self.assertEqual(order_outcome(order, {"open": 21, "low": 20}), ("filled", 20, None))
        self.assertEqual(order_outcome(order, {"open": 21, "low": 20.1})[0], "expired")
        self.assertEqual(order_outcome(order, {})[0], "pending")

    def test_stop_first_trim_and_ema9_next_open(self) -> None:
        position = {"entry_price": 20, "stop": 19, "initial_r": 1}
        self.assertEqual(position_exit(position, {"open": 20, "low": 18.9, "high": 23}), ("stop", 18.981))
        self.assertEqual(position_exit({**position, "stop": 18}, {"open": 20, "low": 19, "high": 23}), ("trim_3r", 23))
        self.assertEqual(position_exit({**position, "exit_next_open": True}, {"open": 22, "low": 21}), ("ema9_exit", 21.978))


class _ForwardRepo:
    def __init__(self):
        self.runs, self.candidates, self.positions, self.events = {}, [], [], {}
        self.next_id = 1

    def save_session(self, *, session_date, source_run_id, fingerprint, candidates, summary):
        _ = summary
        key = (session_date, source_run_id, fingerprint)
        if key not in self.runs:
            self.runs[key] = self.next_id
            run_id = self.next_id
            self.next_id += 1
            for item in candidates:
                self.candidates.append({"id": self.next_id, "run_id": run_id, "ticker": item["ticker"], "rank": item["rank"],
                                        "status": item["status"], "payload_json": dict(item), "session_date": session_date})
                self.next_id += 1
        return self.runs[key]

    def pending_candidates(self, *, before):
        return [dict(row) for row in self.candidates if row["status"] == "pending" and row["session_date"] < before]

    def pending_orders(self):
        return [dict(ticker=row["ticker"], payload_json=row["payload_json"]) for row in self.candidates if row["status"] == "pending"]

    def open_positions(self):
        return [dict(row) for row in self.positions if row["status"] == "open"]

    def candidate_status(self, run_id, ticker):
        row = next((row for row in self.candidates if row["run_id"] == run_id and row["ticker"] == ticker), None)
        return row["status"] if row else None

    def update_candidate(self, run_id, ticker, payload):
        row = next(row for row in self.candidates if row["run_id"] == run_id and row["ticker"] == ticker)
        row["status"], row["payload_json"] = payload["status"], dict(payload)

    def has_pending_order(self, ticker):
        return any(row["ticker"] == ticker and row["status"] == "pending" for row in self.candidates)

    def reserve_position(self, *, ticker, opened_on, payload):
        if any(row["ticker"] == ticker and row["status"] == "open" for row in self.positions):
            return None
        position_id = self.next_id
        self.next_id += 1
        self.positions.append({"id": position_id, "ticker": ticker, "opened_on": opened_on, "status": "open", "payload_json": dict(payload)})
        return position_id

    def fill_order(self, *, candidate_id, run_id, ticker, event_date, position_payload, event_payload):
        _ = run_id
        candidate = next(row for row in self.candidates if row["id"] == candidate_id)
        if candidate["status"] != "pending":
            return None
        position_id = self.reserve_position(ticker=ticker, opened_on=event_date, payload=position_payload)
        if position_id is None:
            candidate["status"] = "blocked"
            return None
        candidate["status"] = "filled"
        self.events[(ticker, "order_filled", event_date, position_id)] = dict(event_payload)
        return position_id

    def apply_event(self, *, run_id, position_id, ticker, event_type, event_date, payload, candidate_id=None,
                    candidate_status=None, position_payload=None, close_position=False):
        _ = run_id
        key = (ticker, event_type, event_date, position_id)
        if key in self.events:
            return False
        self.events[key] = dict(payload)
        if candidate_id is not None:
            row = next(item for item in self.candidates if item["id"] == candidate_id)
            row["status"] = candidate_status
        if position_id is not None and position_payload is not None:
            row = next(item for item in self.positions if item["id"] == position_id)
            row["payload_json"] = dict(position_payload)
            if close_position:
                row["status"] = "closed"
        return True


class MartinForwardTestLifecycleTest(unittest.TestCase):
    def test_session_reserves_then_fills_same_day_stops_and_retries_idempotently(self) -> None:
        repo = _ForwardRepo()
        date1, date2 = dt.date(2026, 1, 2), dt.date(2026, 1, 5)
        dates = pd.date_range("2025-12-19", periods=12, freq="B")
        frame = pd.DataFrame({"Open": [20.0] * 11 + [19.3], "High": [20.5] * 11 + [23.0],
                              "Low": [19.5] * 11 + [18.8], "Close": [20.0] * 11 + [19.0]}, index=dates)
        def load_bars(tickers, as_of, days, **kwargs):
            _ = tickers, days, kwargs
            return {"ABC": frame.loc[frame.index.date <= as_of]}
        service = MartinForwardTestService(repo, bar_loader=load_bars)
        row = {"ticker": "ABC", "scanner_ids": ["rs"], "market_cap": 2e9, "close": 20, "daily_rs_rating": 95,
               "adr14_pct": 4, "trade_readiness": "defined_risk", "trade_plan": {"trigger_price": 20, "stop_price": 19, "max_entry_price": 21}}
        snap1 = {"target_trading_date": date1.isoformat(), "_snapshot_run_id": 11, "source_fingerprint": "fp1", "guru_board": {"rows": [row]}}
        snap2 = {"target_trading_date": date2.isoformat(), "_snapshot_run_id": 12, "source_fingerprint": "fp2", "guru_board": {"rows": [row]}}
        service.run_session(as_of_date=date1, snapshot=snap1)
        self.assertEqual(repo.candidates[0]["status"], "pending")
        self.assertIsInstance(repo.candidates[0]["payload_json"]["shares"], int)
        self.assertLessEqual(repo.candidates[0]["payload_json"]["risk_dollars"], 500)
        self.assertLessEqual(repo.candidates[0]["payload_json"]["allocation_dollars"], 20_000)
        service.run_session(as_of_date=date2, snapshot=snap2)
        self.assertEqual(repo.candidates[0]["status"], "filled")
        self.assertEqual(repo.positions[0]["status"], "closed")
        self.assertIn(("ABC", "stop", date2, repo.positions[0]["id"]), repo.events)
        event_count, position_count = len(repo.events), len(repo.positions)
        service.run_session(as_of_date=date2, snapshot=snap2)
        self.assertEqual(len(repo.events), event_count)
        self.assertEqual(len(repo.positions), position_count)

    def test_position_trim_ema9_schedule_and_next_open_exit(self) -> None:
        repo = _ForwardRepo()
        ticker = "ABC"
        date1, date2, date3 = dt.date(2026, 1, 2), dt.date(2026, 1, 5), dt.date(2026, 1, 6)
        dates = pd.bdate_range("2025-12-19", "2026-01-06")
        frame = pd.DataFrame({"Open": [20.0] * len(dates), "High": [20.5] * len(dates),
                              "Low": [19.5] * len(dates), "Close": [20.0] * len(dates)}, index=dates)
        frame.loc[pd.Timestamp(date1), ["High", "Close"]] = [23.0, 20.0]
        frame.loc[pd.Timestamp(date2), ["Close"]] = 19.0
        frame.loc[pd.Timestamp(date3), ["Open", "High", "Low", "Close"]] = [18.5, 19.0, 18.0, 18.8]
        repo.positions.append({"id": 77, "ticker": ticker, "status": "open", "opened_on": date1,
                               "payload_json": {"entry_price": 20.0, "stop": 19.0, "initial_r": 1.0,
                                                "shares": 100, "remaining_shares": 100, "trimmed": False,
                                                "exit_next_open": False}})
        def load_bars(tickers, as_of, days, **kwargs):
            _ = tickers, days, kwargs
            return {ticker: frame.loc[frame.index.date <= as_of]}
        service = MartinForwardTestService(repo, bar_loader=load_bars)
        def empty_snapshot(day, source_id):
            return {"target_trading_date": day.isoformat(), "_snapshot_run_id": source_id,
                    "source_fingerprint": f"fp{source_id}", "guru_board": {"rows": []}}
        service.run_session(as_of_date=date1, snapshot=empty_snapshot(date1, 21))
        self.assertEqual(repo.positions[0]["payload_json"]["remaining_shares"], 90)
        self.assertIn((ticker, "trim_3r", date1, 77), repo.events)
        service.run_session(as_of_date=date2, snapshot=empty_snapshot(date2, 22))
        self.assertTrue(repo.positions[0]["payload_json"]["exit_next_open"])
        self.assertIn((ticker, "ema9_exit_scheduled", date2, 77), repo.events)
        service.run_session(as_of_date=date3, snapshot=empty_snapshot(date3, 23))
        self.assertEqual(repo.positions[0]["status"], "closed")
        self.assertAlmostEqual(repo.positions[0]["payload_json"]["exit_price"], 18.5 * .999)
        self.assertIn((ticker, "ema9_exit", date3, 77), repo.events)

    def test_missing_bar_leaves_pending_order_intact(self) -> None:
        repo = _ForwardRepo()
        date1, date2 = dt.date(2026, 1, 2), dt.date(2026, 1, 5)
        service = MartinForwardTestService(repo, bar_loader=lambda *_args, **_kwargs: {})
        row = {"ticker": "ABC", "scanner_ids": ["rs"], "market_cap": 2e9, "close": 20, "daily_rs_rating": 95,
               "adr14_pct": 4, "trade_readiness": "defined_risk", "trade_plan": {"trigger_price": 20, "stop_price": 19, "max_entry_price": 21}}
        snap = lambda day, source: {"target_trading_date": day.isoformat(), "_snapshot_run_id": source,
                                    "source_fingerprint": f"fp{source}", "guru_board": {"rows": [row]}}
        service.run_session(as_of_date=date1, snapshot=snap(date1, 31))
        service.run_session(as_of_date=date2, snapshot=None)
        self.assertEqual(repo.candidates[0]["status"], "pending")
        self.assertEqual(repo.positions, [])

    def test_next_bar_cancels_gap_below_stop_and_expires_unreached_limit(self) -> None:
        repo = _ForwardRepo()
        date1, date2 = dt.date(2026, 1, 2), dt.date(2026, 1, 5)
        dates = pd.to_datetime([date1, date2])
        frames = {
            "AAA": pd.DataFrame({"Open": [20, 18], "High": [20.5, 19], "Low": [19.5, 17], "Close": [20, 18]}, index=dates),
            "BBB": pd.DataFrame({"Open": [20, 22], "High": [20.5, 23], "Low": [19.5, 20], "Close": [20, 22]}, index=dates),
        }
        def load_bars(tickers, as_of, days, **kwargs):
            _ = days, kwargs
            return {ticker: frames[ticker].loc[frames[ticker].index.date <= as_of] for ticker in tickers}
        service = MartinForwardTestService(repo, bar_loader=load_bars)
        row = lambda ticker: {"ticker": ticker, "scanner_ids": ["rs"], "market_cap": 2e9, "close": 20,
                              "daily_rs_rating": 95, "adr14_pct": 4, "trade_readiness": "defined_risk",
                              "trade_plan": {"trigger_price": 20, "stop_price": 19, "max_entry_price": 21}}
        snapshot = {"target_trading_date": date1.isoformat(), "_snapshot_run_id": 41,
                    "source_fingerprint": "gap-test", "guru_board": {"rows": [row("AAA"), row("BBB")]}}
        service.run_session(as_of_date=date1, snapshot=snapshot)
        service.run_session(as_of_date=date2, snapshot=None)
        self.assertEqual({item["ticker"]: item["status"] for item in repo.candidates}, {"AAA": "cancelled", "BBB": "expired"})
        self.assertIn(("AAA", "order_cancelled", date2, None), repo.events)
        self.assertIn(("BBB", "order_expired", date2, None), repo.events)

    def test_entry_day_three_r_does_not_trim(self) -> None:
        repo = _ForwardRepo()
        date1, date2 = dt.date(2026, 1, 2), dt.date(2026, 1, 5)
        dates = pd.to_datetime([date1, date2])
        frame = pd.DataFrame({"Open": [20, 19.3], "High": [20.5, 24], "Low": [19.5, 19.2], "Close": [20, 22]}, index=dates)
        service = MartinForwardTestService(repo, bar_loader=lambda *_args, **_kwargs: {"ABC": frame})
        row = {"ticker": "ABC", "scanner_ids": ["rs"], "market_cap": 2e9, "close": 20, "daily_rs_rating": 95,
               "adr14_pct": 4, "trade_readiness": "defined_risk", "trade_plan": {"trigger_price": 20, "stop_price": 19, "max_entry_price": 21}}
        snapshot = {"target_trading_date": date1.isoformat(), "_snapshot_run_id": 51,
                    "source_fingerprint": "no-entry-trim", "guru_board": {"rows": [row]}}
        service.run_session(as_of_date=date1, snapshot=snapshot)
        service.run_session(as_of_date=date2, snapshot=None)
        self.assertEqual(repo.positions[0]["payload_json"]["remaining_shares"], repo.positions[0]["payload_json"]["shares"])
        self.assertFalse(repo.positions[0]["payload_json"]["trimmed"])
        self.assertFalse(any(key[1] == "trim_3r" for key in repo.events))


if __name__ == "__main__":
    unittest.main()
