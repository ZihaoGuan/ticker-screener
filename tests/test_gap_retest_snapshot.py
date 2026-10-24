from __future__ import annotations

import datetime as dt
import sys
import tempfile
import types
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

from scripts.run_martin_gap_retest import _load_source
from src.webapp.repositories.history_repository import HistoryRepository
from src.webapp.repositories.martin_forward_test_repository import MartinForwardTestRepository
from src.webapp.services.scanner_top_hits_refresh import refresh_scanner_top_hits
from src.webapp.services.watchlist_service import (
    GAP_RETEST_STRATEGY_ID,
    WatchlistService,
    _gap_retest_preview,
    _project_scanner_top_hit_list_row,
)


class GapRetestSnapshotTests(unittest.TestCase):
    day = dt.date(2026, 6, 12)

    def test_flat_saved_decision_maps_to_optional_preview_and_compact_projection(self):
        for state in ("confirmed", "pending", "waiting_risk", "waiting_capacity"):
            with self.subTest(state=state):
                preview = _gap_retest_preview({
                    "gap_state": state, "config_version": "martin_daily_gap_retest_v1",
                    "stop_policy_version": "gap_reclaim_candle_low_v1", "entry_policy_version": "gap_daily_reclaim_next_open_v1",
                    "management_policy_version": "gap_trim10_3r_ema9_v1", "reference_entry": 100.0,
                    "stop": 97.0, "max_entry": 100.0, "reference_risk_pct": 3.0, "adr14_pct": 6.0, "stop_to_adr": .5,
                    "reasons": ["capacity_full"] if state == "waiting_capacity" else [],
                }, decision_id=88, decision_date=self.day)
                self.assertEqual(preview["status"], "available")
                self.assertEqual(preview["strategy_id"], GAP_RETEST_STRATEGY_ID)
                self.assertEqual((preview["entry_policy_version"], preview["stop_policy_version"], preview["management_policy_version"]),
                                 ("gap_daily_reclaim_next_open_v1", "gap_reclaim_candle_low_v1", "gap_trim10_3r_ema9_v1"))
                self.assertEqual(preview["plan"], {"reference_entry": 100.0, "initial_stop": 97.0, "max_entry": 100.0,
                                                     "reference_risk_pct": 3.0, "adr14_pct": 6.0, "stop_to_adr": .5})
                row = _project_scanner_top_hit_list_row({"ticker": "ABC", "power_gap_volume_context": {}, "gap_retest_context": preview})
                self.assertEqual(row["gap_retest_context"], preview)
                self.assertEqual(preview["decision_id"], 88)
        unavailable = _gap_retest_preview({"gap_state": "watch", "reasons": ["awaiting_first_retest"]}, decision_id=88, decision_date=self.day)
        self.assertEqual(unavailable["status"], "unavailable")
        self.assertIsNone(unavailable["plan"])

    def test_snapshot_optional_book_read_failure_preserves_regular_rows_and_persists(self):
        with tempfile.TemporaryDirectory() as folder:
            service = WatchlistService(artifacts_dir=Path(folder), database_url="")
            board = {"target_trading_date": self.day.isoformat(), "cards": []}
            payload = {"target_trading_date": self.day.isoformat(), "rows": [{"ticker": "ABC", "scanner_count": 1,
                       "power_gap_volume_context": {"active_signals": ["gap"]}}], "cards": []}
            with patch.object(service.screener_history_service, "is_configured", return_value=True), \
                 patch.object(service, "_build_scanner_top_hits_payload_live", return_value=payload), \
                 patch("src.webapp.services.watchlist_service.MartinForwardTestRepository", side_effect=RuntimeError("read failed")), \
                 patch.object(service.screener_history_service, "persist_snapshot_run") as persist:
                result = service.persist_scanner_top_hits_snapshot(board_payload=board)
            self.assertEqual(result["rows"][0]["ticker"], "ABC")
            self.assertEqual(result["rows"][0]["power_gap_volume_context"]["active_signals"], ["gap"])
            self.assertNotIn("gap_retest_context", result["rows"][0])
            persist.assert_called_once()

    def test_book_decision_completion_refreshes_current_snapshot_without_source_change(self):
        with tempfile.TemporaryDirectory() as folder:
            service = WatchlistService(artifacts_dir=Path(folder), database_url="")
            service.repository.artifacts_dir.mkdir(parents=True, exist_ok=True)
            now = dt.datetime(2026, 6, 13, 1, 0, tzinfo=dt.timezone.utc)
            board = {"target_trading_date": self.day.isoformat(), "cards": [{"timeframe": "Daily", "sort_date": self.day.isoformat(),
                      "captured_at": "2026-06-13T00:45:00Z", "available": True}]}
            with patch("src.webapp.services.scanner_top_hits_refresh._configured_options", return_value={"required_job_ids": ["src"], "skip_if_current": True}), \
                 patch("src.webapp.services.scanner_top_hits_refresh.check_required_scheduled_jobs", return_value=SimpleNamespace(ready=True, pending_job_ids=[])), \
                 patch("src.webapp.services.scanner_top_hits_refresh.load_daily_bars_frame_from_db", return_value=SimpleNamespace(empty=False)), \
                 patch.object(service, "get_scanner_board", return_value=board), \
                 patch.object(service, "get_scanner_top_hits_snapshot_refresh_state", return_value={"has_current_snapshot": True,
                      "source_changed": False, "gap_retest_decision_changed": True, "latest_source_at": ""}), \
                 patch.object(service, "persist_scanner_top_hits_snapshot", return_value={"rows": [{"ticker": "ABC"}]}) as persist:
                result = refresh_scanner_top_hits(service, project_root=Path(folder), now=now)
            self.assertEqual(result["status"], "rebuilt")
            persist.assert_called_once()

    def test_zero_candidate_applied_session_has_decision_fingerprint(self):
        repo = MartinForwardTestRepository(database_url="")
        queries = []
        repo._rows = lambda sql, *_args: (queries.append(sql) or [{"id": 121, "session_date": self.day, "ticker": None, "status": None, "payload_json": None}])
        snapshot = repo.gap_retest_decision_snapshot(self.day)
        self.assertEqual(snapshot, {"decision_id": 121, "decisions": {}})
        self.assertIn("LEFT JOIN", queries[0])

    def test_empty_exact_date_source_is_present_and_large_hit_set_is_paginated(self):
        history = Mock()
        history.list_runs.return_value = [{"id": 9, "run_date": self.day.isoformat()}]
        history.get_run.return_value = {"id": 9, "run_date": self.day.isoformat(), "hit_count": 0}
        empty = _load_source(history, self.day)
        self.assertEqual(empty["hits"], [])
        self.assertEqual(empty["source_run_id"], 9)

        history.get_run.side_effect = [
            {"id": 9, "run_date": self.day.isoformat(), "hit_count": 5001},
            {"hits": [{"ticker": str(i)} for i in range(5000)]},
            {"hits": [{"ticker": "LAST"}]},
        ]
        source = _load_source(history, self.day)
        self.assertEqual(len(source["hits"]), 5001)
        self.assertEqual([call.kwargs["hit_offset"] for call in history.get_run.call_args_list[-2:]], [0, 5000])

        history.get_run.side_effect = [
            {"id": 9, "run_date": self.day.isoformat(), "hit_count": 5001}, {"hits": []},
        ]
        with self.assertRaisesRegex(RuntimeError, "only 0 of 5001"):
            _load_source(history, self.day)

    def test_read_only_repositories_skip_schema_mutation_and_use_readonly_connections(self):
        connections = []

        class Connection:
            def __enter__(self): return self
            def __exit__(self, *_args): return False
            def cursor(self): return object()

        def connect(*args, **kwargs):
            connections.append((args, kwargs))
            return Connection()

        fake_pg = types.SimpleNamespace(connect=connect)
        with patch.dict(sys.modules, {"psycopg": fake_pg}):
            repo = MartinForwardTestRepository(database_url="postgres://example", read_only=True)
            with patch.object(Path, "read_text", side_effect=AssertionError("DDL file must not be read")):
                repo._connect()
            history = HistoryRepository(database_url="postgres://example", read_only=True)
            with patch.object(history, "_history_schema_is_ready", return_value=True):
                history._connect()
        self.assertEqual(len(connections), 3)
        self.assertTrue(all("default_transaction_read_only=on" in call[1]["options"] for call in connections))


if __name__ == "__main__":
    unittest.main()
