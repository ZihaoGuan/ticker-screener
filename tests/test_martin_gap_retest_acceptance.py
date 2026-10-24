from __future__ import annotations

import copy
import json
import unittest
from contextlib import contextmanager
from datetime import date
from unittest.mock import patch

import pandas as pd

from src.martin_daily_gap_retest import STRATEGY_ID
from src.webapp.services.martin_gap_retest_service import MartinGapRetestService


class MemoryBookRepository:
    """Small transactional stand-in for the existing forward-test repository."""

    def __init__(self):
        self.runs: list[dict] = []
        self.candidates: list[dict] = []
        self.positions: list[dict] = []
        self.events: list[dict] = []
        self.next_run = 1
        self.next_candidate = 1
        self.next_position = 1

    @contextmanager
    def atomic_session(self, strategy_id):
        before = copy.deepcopy((self.runs, self.candidates, self.positions, self.events,
                                self.next_run, self.next_candidate, self.next_position))
        try:
            yield
        except Exception:
            (self.runs, self.candidates, self.positions, self.events,
             self.next_run, self.next_candidate, self.next_position) = before
            raise

    def session_run_id(self, strategy_id, session_date):
        row = next((r for r in self.runs if r["strategy_id"] == strategy_id and r["session_date"] == session_date), None)
        return row["id"] if row else None

    def latest_session_date(self, strategy_id):
        rows = [r["session_date"] for r in self.runs if r["strategy_id"] == strategy_id]
        return max(rows) if rows else None

    def latest_source_run_id(self, strategy_id):
        rows = [r for r in self.runs if r["strategy_id"] == strategy_id]
        return max(rows, key=lambda r: r["session_date"])["source_run_id"] if rows else None

    def latest_gap_watch_states(self, strategy_id):
        latest = {}
        for candidate in self.candidates:
            run = next(r for r in self.runs if r["id"] == candidate["run_id"])
            if run["strategy_id"] == strategy_id and (candidate["ticker"] not in latest or run["session_date"] > latest[candidate["ticker"]][0]):
                latest[candidate["ticker"]] = (run["session_date"], copy.deepcopy(candidate["payload_json"]))
        return {ticker: payload for ticker, (_day, payload) in latest.items()}

    def open_positions(self, *, strategy_id):
        return copy.deepcopy([p for p in self.positions if p["strategy_id"] == strategy_id and p["status"] == "open"])

    def pending_candidates(self, *, before, strategy_id):
        return copy.deepcopy([c for c in self.candidates if c["strategy_id"] == strategy_id and c["status"] == "pending"
                              and next(r for r in self.runs if r["id"] == c["run_id"])["session_date"] < before])

    def account_state(self, strategy_id):
        open_positions = self.open_positions(strategy_id=strategy_id)
        pending = self.pending_candidates(before=date.max, strategy_id=strategy_id)
        realized = sum(float(p["payload_json"].get("realized_pnl", 0)) for p in self.positions if p["strategy_id"] == strategy_id)
        cash = 100_000.0 + realized - sum(float(p["payload_json"].get("entry_price", 0)) * int(p["payload_json"].get("remaining_shares", 0)) for p in open_positions)
        risk = sum(max(float(p["payload_json"].get("entry_price", 0)) - float(p["payload_json"].get("stop", 0)), 0) * int(p["payload_json"].get("remaining_shares", 0)) for p in open_positions)
        cash -= sum(float(c["payload_json"].get("reservation_cash", 0)) for c in pending)
        risk += sum(float(c["payload_json"].get("planned_stop_risk", 0)) for c in pending)
        return {"available_cash": cash, "committed_stop_risk": risk}

    def has_gap_event_fill(self, strategy_id, event_identity):
        return any(e["strategy_id"] == strategy_id and e["event_type"] == "order_filled"
                   and e["payload"].get("event_identity") == event_identity for e in self.events)

    def save_session(self, *, session_date, source_run_id, fingerprint, candidates, summary, strategy_id):
        json.dumps(summary)
        json.dumps(candidates)
        for payload in candidates:
            payload["ticker"]
            payload["rank"]
            payload["status"]
            payload["reasons"]
        run_id = self.next_run
        self.next_run += 1
        self.runs.append({"id": run_id, "strategy_id": strategy_id, "session_date": session_date,
                          "source_run_id": source_run_id, "fingerprint": fingerprint, "summary": copy.deepcopy(summary)})
        for payload in candidates:
            row = {"id": self.next_candidate, "run_id": run_id, "strategy_id": strategy_id,
                   "ticker": payload["ticker"], "status": payload.get("status"), "payload_json": copy.deepcopy(payload)}
            self.next_candidate += 1
            self.candidates.append(row)
        return run_id

    def fill_order(self, *, candidate_id, run_id, ticker, event_date, position_payload, event_payload, strategy_id):
        candidate = next(c for c in self.candidates if c["id"] == candidate_id)
        candidate["status"] = "filled"
        position_id = self.next_position
        self.next_position += 1
        self.positions.append({"id": position_id, "strategy_id": strategy_id, "ticker": ticker,
                               "status": "open", "opened_on": event_date, "payload_json": copy.deepcopy(position_payload)})
        self.events.append({"strategy_id": strategy_id, "event_type": "order_filled", "event_date": event_date,
                            "ticker": ticker, "payload": copy.deepcopy(event_payload)})
        return position_id

    def apply_event(self, *, run_id, position_id, ticker, event_type, event_date, payload,
                    position_payload=None, close_position=False, candidate_id=None, candidate_status=None, strategy_id):
        if candidate_id is not None:
            candidate = next((c for c in self.candidates if c["id"] == candidate_id), None)
            if candidate is None:
                return False
            candidate["status"] = candidate_status
        if position_id is not None:
            position = next(p for p in self.positions if p["id"] == position_id)
            if position_payload is not None:
                position["payload_json"] = copy.deepcopy(position_payload)
            if close_position:
                position["status"] = "closed"
        self.events.append({"strategy_id": strategy_id, "event_type": event_type, "event_date": event_date,
                            "ticker": ticker, "payload": copy.deepcopy(payload)})
        return True

    def set_position_payload(self, position_id, payload):
        next(p for p in self.positions if p["id"] == position_id)["payload_json"] = copy.deepcopy(payload)

    def update_candidate(self, run_id, ticker, payload):
        row = next(c for c in self.candidates if c["run_id"] == run_id and c["ticker"] == ticker)
        row.update(status=payload["status"], payload_json=copy.deepcopy(payload))

    def set_run_metadata(self, run_id, metadata):
        next(r for r in self.runs if r["id"] == run_id)["summary"].update(copy.deepcopy(metadata))

    def update_run_summary(self, *, run_id, strategy_id, mark_prices, action_counts):
        next(r for r in self.runs if r["id"] == run_id)["summary"].update(mark_prices=mark_prices, action_counts=action_counts)

    def seed_position(self, *, run_date, source_run_id, ticker="HELD", payload=None):
        run_id = self.next_run
        self.next_run += 1
        self.runs.append({"id": run_id, "strategy_id": STRATEGY_ID, "session_date": run_date,
                          "source_run_id": source_run_id, "fingerprint": "seed", "summary": {}})
        position_id = self.next_position
        self.next_position += 1
        seeded = {"entry_price": 100.0, "stop": 97.0, "initial_stop": 97.0,
                  "initial_r_per_share": 3.0, "initial_shares": 100, "remaining_shares": 100,
                  "shares": 100, "realized_pnl": 0.0, "realized_r": 0.0,
                  "opened_on": run_date.isoformat(), "trimmed": False, "exit_next_open": False,
                  "holding_sessions": 1, "last_close": 100.0}
        seeded.update(payload or {})
        self.positions.append({"id": position_id, "strategy_id": STRATEGY_ID, "ticker": ticker,
                               "status": "open", "opened_on": run_date, "payload_json": seeded})
        return position_id


class GapRetestOrchestratorAcceptanceTests(unittest.TestCase):
    start = date(2026, 1, 23)
    session_dates = []

    @classmethod
    def setUpClass(cls):
        # Inject a deterministic exchange-session calendar with a full indicator warmup.
        cls.session_dates = [day for day in pd.bdate_range(end="2026-01-23", periods=326).date if day != date(2026, 1, 19)]
        cls.session_dates += [date(2026, 1, 26), date(2026, 1, 27), date(2026, 1, 28)]

    def calendar(self, through):
        return [day for day in self.session_dates if day <= through]

    def source(self, day, run_id, tickers=("XYZ",)):
        published = "2026-01-24T17:00:00Z" if day == date(2026, 1, 23) else f"{day.isoformat()}T22:00:00Z"
        return {"id": run_id, "run_date": day.isoformat(), "published_at": published,
                "source_fingerprint": f"source-{run_id}", "hits": [{"ticker": ticker, "hit_payload_json": {
                    "ticker": ticker, "power_gap_volume_context": {"active_signals": ["gap"],
                        "rules_version": "power_gap_volume_watch_v1",
                        "gap_event": {"date": self.session_dates[-10].isoformat(), "low": 90.0}}}} for ticker in tickers]}

    def frames(self, execution_bars=None, omit_ticker=None):
        execution_bars = execution_bars or {}

        def load(tickers, as_of, _window, **_kwargs):
            frames = {}
            for ticker in tickers:
                if ticker == omit_ticker:
                    continue
                rows = []
                for day in self.calendar(as_of):
                    opening, high, low, close, volume = (100.0, 101.0, 99.0, 100.0, 2_000_000.0)
                    override = execution_bars.get(day)
                    if isinstance(override, dict):
                        override = override.get(ticker)
                    elif ticker != "XYZ":
                        override = None
                    if override is not None:
                        opening, high, low, close = override
                    rows.append({"Open": opening, "High": max(high, opening, close), "Low": min(low, opening, close),
                                 "Close": close, "Volume": volume})
                frame = pd.DataFrame(rows, index=pd.to_datetime(self.calendar(as_of)))
                frame.attrs["adjustment_basis"] = "split_adjusted_regular_session"
                frames[ticker] = frame
            return frames
        return load

    @staticmethod
    def confirmed(_frame, event, _prior=None, *, ticker=None, **_kwargs):
        return {"ticker": ticker or event["ticker"], "status": "confirmed", "gap_state": "confirmed", "reasons": [],
                "event_identity": event["event_identity"], "original_gap_date": event["original_gap_date"],
                "reference_entry": 100.0, "entry": 100.0, "stop": 97.0, "initial_stop": 97.0,
                "max_entry": 100.0, "adv20_dollars": 30_000_000.0, "adr14_pct": 5.0,
                "reference_risk_pct": 3.0, "stop_to_adr": .6, "plan": {"reference_entry": 100.0, "initial_stop": 97.0, "max_entry": 100.0}}

    @staticmethod
    def varied_confirmed(frame, event, prior=None, *, ticker=None, **kwargs):
        decision = GapRetestOrchestratorAcceptanceTests.confirmed(frame, event, prior, ticker=ticker, **kwargs)
        if (ticker or event["ticker"]) in {"A", "B", "C", "D", "E"}:
            decision.update(stop=98.8, initial_stop=98.8, reference_risk_pct=1.2, stop_to_adr=.24,
                            plan={"reference_entry": 100.0, "initial_stop": 98.8, "max_entry": 100.0})
        return decision

    @staticmethod
    def flat_watch(_frame, event, _prior=None, *, ticker=None, **_kwargs):
        return {"ticker": ticker or event["ticker"], "status": "watch", "gap_state": "watch",
                "reasons": ["awaiting_first_retest"], "event_identity": event["event_identity"],
                "original_gap_date": event["original_gap_date"], "source_run_id": event["source_run_id"]}

    def service(self, repo, bar_loader):
        return MartinGapRetestService(repo, bar_loader=bar_loader, calendar=self.calendar)

    def test_dry_run_matches_allocation_without_writes_then_fill_stop_and_no_rebuy(self):
        repo = MemoryBookRepository()
        day0, day1, day2 = self.session_dates[-4:-1]
        source0 = self.source(day0, 71)
        service = self.service(repo, self.frames())
        with patch("src.webapp.services.martin_gap_retest_service.require_completed_session"), \
             patch("src.webapp.services.martin_gap_retest_service.evaluate_gap_retest", side_effect=self.confirmed):
            preview = service.run_session(as_of_date=day0, source_record=source0, dry_run=True)
            self.assertEqual("dry_run_complete", preview["status"])
            self.assertEqual("pending", preview["candidates"][0]["status"])
            self.assertEqual(166, preview["candidates"][0]["shares"])
            planned = preview["candidates"][0]
            self.assertEqual(100.0, planned["entry"])
            self.assertEqual(100.0, planned["limit_price"])
            self.assertEqual(100.0, planned["reference_entry"])
            self.assertEqual(97.0, planned["stop"])
            self.assertEqual(97.0, planned["plan"]["initial_stop"])
            self.assertEqual(100.0, planned["plan"]["max_entry"])
            self.assertEqual(([], [], [], [], 1, 1, 1),
                             (repo.runs, repo.candidates, repo.positions, repo.events,
                              repo.next_run, repo.next_candidate, repo.next_position))
            applied = service.run_session(as_of_date=day0, source_record=source0, dry_run=False)
            self.assertEqual(preview["candidates"], applied["candidates"])
            self.assertEqual("pending", repo.candidates[0]["status"])
            self.assertEqual(day1.isoformat(), repo.candidates[0]["payload_json"]["awaiting_session_date"])
            before_noop = copy.deepcopy((repo.runs, repo.candidates, repo.positions, repo.events))
            rerun = service.run_session(as_of_date=day0, source_record=self.source(day0, 999), dry_run=False)
            self.assertTrue(rerun["skipped"])
            self.assertEqual("session_already_processed", rerun["reason"])
            self.assertEqual(before_noop, (repo.runs, repo.candidates, repo.positions, repo.events))

            service.bar_loader = self.frames({day1: (99.0, 100.0, 96.5, 99.0)})
            next_day = service.run_session(as_of_date=day1, source_record=self.source(day1, 72), dry_run=False)
            self.assertEqual(1, next_day["fills"])
            self.assertEqual(1, next_day["stops"])
            position = repo.positions[0]
            self.assertEqual("closed", position["status"])
            self.assertEqual("stop", position["payload_json"]["exit_type"])
            self.assertAlmostEqual(96.903, position["payload_json"]["exit_price"])

            service.bar_loader = self.frames()
            final = service.run_session(as_of_date=day2, source_record=self.source(day2, 73), dry_run=False)
            candidate = next(item for item in final["candidates"] if item["ticker"] == "XYZ")
            self.assertEqual("retired", candidate["status"])
            self.assertIn("event_already_traded", candidate["reasons"])
            self.assertEqual(1, len([e for e in repo.events if e["event_type"] == "order_filled"]))
            self.assertFalse(any(c["status"] == "pending" and c["run_id"] == final["run_id"] for c in repo.candidates))

    def test_missing_execution_bar_rolls_back_and_skipped_session_is_rejected(self):
        repo = MemoryBookRepository()
        day0, day1, day2 = self.session_dates[-4:-1]
        service = self.service(repo, self.frames())
        with patch("src.webapp.services.martin_gap_retest_service.require_completed_session"), \
             patch("src.webapp.services.martin_gap_retest_service.evaluate_gap_retest", side_effect=self.confirmed):
            service.run_session(as_of_date=day0, source_record=self.source(day0, 81), dry_run=False)
            before = copy.deepcopy((repo.runs, repo.candidates, repo.positions, repo.events))
            service.bar_loader = self.frames(omit_ticker="XYZ")
            with self.assertRaisesRegex(ValueError, "execution history.*rolled back"):
                service.run_session(as_of_date=day1, source_record=self.source(day1, 82), dry_run=False)
            self.assertEqual(before, (repo.runs, repo.candidates, repo.positions, repo.events))
            service.bar_loader = self.frames()
            with self.assertRaisesRegex(ValueError, "missed intermediate XNYS sessions"):
                service.run_session(as_of_date=day2, source_record=self.source(day2, 83), dry_run=False)
            self.assertEqual(before, (repo.runs, repo.candidates, repo.positions, repo.events))

    def test_entry_day_stops_release_budget_before_fresh_confirmation(self):
        repo = MemoryBookRepository()
        day0, day1 = self.session_dates[-4:-2]
        service = self.service(repo, self.frames())
        execution = {ticker: (99.9, 100.0, 98.7, 99.5) for ticker in "ABCDE"}
        with patch("src.webapp.services.martin_gap_retest_service.require_completed_session"), \
             patch("src.webapp.services.martin_gap_retest_service.evaluate_gap_retest", side_effect=self.varied_confirmed):
            initial = service.run_session(as_of_date=day0, source_record=self.source(day0, 91, tuple("ABCDE")), dry_run=False)
            self.assertEqual(5, sum(candidate["status"] == "pending" for candidate in initial["candidates"]))
            service.bar_loader = self.frames({day1: execution})
            second_source = self.source(day1, 92, tuple("ABCDEF"))
            preview = service.run_session(as_of_date=day1, source_record=second_source, dry_run=True)
            self.assertEqual("pending", next(c for c in preview["candidates"] if c["ticker"] == "F")["status"])
            self.assertEqual(166, next(c for c in preview["candidates"] if c["ticker"] == "F")["shares"])
            applied = service.run_session(as_of_date=day1, source_record=second_source, dry_run=False)
            self.assertEqual(preview["candidates"], applied["candidates"])
            for candidate in applied["candidates"]:
                if candidate["ticker"] in "ABCDE":
                    self.assertEqual("filled", next(c for c in repo.candidates if c["ticker"] == candidate["ticker"] and c["run_id"] == initial["run_id"])["status"])
            fresh = next(c for c in repo.candidates if c["run_id"] == applied["run_id"] and c["ticker"] == "F")
            self.assertEqual("pending", fresh["status"])
            self.assertEqual(166, fresh["payload_json"]["shares"])
            closed = [p for p in repo.positions if p["status"] == "closed"]
            self.assertEqual(5, len(closed))
            self.assertTrue(all(p["payload_json"]["exit_type"] == "stop" for p in closed))

    def test_source_missing_still_trims_schedules_then_executes_ema9_exit(self):
        repo = MemoryBookRepository()
        day0, day1, day2 = self.session_dates[-4:-1]
        repo.seed_position(run_date=self.session_dates[-5], source_run_id=501)
        service = self.service(repo, self.frames({day0: {"HELD": (100.0, 110.0, 99.0, 98.5)}, day1: {"HELD": (99.5, 100.0, 98.0, 99.5)}}))
        with patch("src.webapp.services.martin_gap_retest_service.require_completed_session"), \
             patch("src.webapp.services.martin_gap_retest_service.evaluate_gap_retest", side_effect=self.confirmed):
            trimmed = service.run_session(as_of_date=day0, source_record=None, dry_run=False)
            position = repo.positions[0]
            self.assertEqual(1, trimmed["trims"])
            self.assertEqual([], trimmed["candidates"])
            self.assertEqual("open", position["status"])
            self.assertEqual(90, position["payload_json"]["remaining_shares"])
            self.assertTrue(position["payload_json"]["trimmed"])
            self.assertTrue(position["payload_json"]["exit_next_open"])
            self.assertEqual(1, len([e for e in repo.events if e["event_type"] == "trim_3r"]))

            service.bar_loader = self.frames({day1: {"HELD": (99.5, 100.0, 98.0, 99.5)}})
            exited = service.run_session(as_of_date=day1, source_record=None, dry_run=False)
            self.assertEqual(1, exited["trail_exits"])
            self.assertEqual([], exited["candidates"])
            self.assertEqual("closed", position["status"])
            self.assertEqual("ema9_exit", position["payload_json"]["exit_type"])
            self.assertEqual(0, position["payload_json"]["remaining_shares"])
            self.assertEqual(1, len([e for e in repo.events if e["event_type"] == "trim_3r"]))

    def test_stop_precedes_ambiguous_three_r_target(self):
        repo = MemoryBookRepository()
        day0 = self.session_dates[-4]
        repo.seed_position(run_date=self.session_dates[-5], source_run_id=601)
        service = self.service(repo, self.frames({day0: {"HELD": (100.0, 110.0, 96.0, 100.0)}}))
        with patch("src.webapp.services.martin_gap_retest_service.require_completed_session"):
            result = service.run_session(as_of_date=day0, source_record=None, dry_run=False)
        self.assertEqual(1, result["stops"])
        self.assertEqual(0, result["trims"])
        self.assertEqual("closed", repo.positions[0]["status"])
        self.assertEqual("stop", repo.positions[0]["payload_json"]["exit_type"])
        self.assertFalse(any(e["event_type"] == "trim_3r" for e in repo.events))

    def test_confirmed_source_flat_watch_is_json_persistable_with_rank(self):
        repo = MemoryBookRepository()
        day0 = self.session_dates[-4]
        service = self.service(repo, self.frames())
        with patch("src.webapp.services.martin_gap_retest_service.require_completed_session"), \
             patch("src.webapp.services.martin_gap_retest_service.evaluate_gap_retest", side_effect=self.flat_watch):
            result = service.run_session(as_of_date=day0, source_record=self.source(day0, 701), dry_run=False)
        self.assertTrue(result["discovery_complete"])
        self.assertEqual(1, result["candidate_count"])
        watch = next(candidate for candidate in repo.candidates if candidate["run_id"] == result["run_id"])
        self.assertEqual("watch", watch["status"])
        self.assertIsInstance(watch["payload_json"]["rank"], int)
        self.assertEqual(["awaiting_first_retest"], watch["payload_json"]["reasons"])

if __name__ == "__main__":
    unittest.main()
