import datetime as dt
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import MagicMock, patch

from src.webapp.services.scanner_top_hits_refresh import refresh_scanner_top_hits


class ScannerTopHitsRefreshTests(unittest.TestCase):
    def setUp(self):
        bars = patch("src.webapp.services.scanner_top_hits_refresh.load_daily_bars_frame_from_db", return_value=MagicMock(empty=False))
        self.bars = bars.start()
        self.addCleanup(bars.stop)
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        (self.root / "config").mkdir()
        self.status = self.root / "artifacts" / "status"
        self.status.mkdir(parents=True)
        (self.root / "config" / "scheduled_jobs.json").write_text(json.dumps({"jobs": [{
            "action_id": "build_scanner_top_hits_snapshot", "options": {
                "required_job_ids": ["scanner_a"], "skip_if_current": True,
                "allow_incremental_refresh": True}}]}))
        self.set_status("success", "2026-10-20T23:00:00Z")
        self.service = MagicMock()
        self.service.repository.artifacts_dir = self.root / "artifacts"
        self.service.get_scanner_board.return_value = {"target_trading_date": "2026-10-20", "cards": [{
            "timeframe": "Daily", "sort_date": "2026-10-20", "has_run": True,
            "captured_at": "2026-10-20T23:00:00Z"}]}
        self.service.get_scanner_top_hits_snapshot_refresh_state.return_value = {
            "has_current_snapshot": False, "source_changed": True, "latest_source_at": "2026-10-20T23:00:00Z"}
        self.service.persist_scanner_top_hits_snapshot.return_value = {"rows": [{"ticker": "ABC"}]}

    def set_status(self, status, finished):
        (self.status / "scanner_a.json").write_text(json.dumps({"status": status, "last_finished_at": finished}))

    def refresh(self, now=None, **kwargs):
        return refresh_scanner_top_hits(self.service, project_root=self.root,
            now=now or dt.datetime(2026, 10, 20, 23, 30, tzinfo=dt.timezone.utc), **kwargs)

    def test_completed_batch_promotes_today_before_2030_cutoff(self):
        result = self.refresh()
        self.assertEqual(result["status"], "rebuilt")
        self.assertEqual(result["target_date"], "2026-10-20")
        self.assertIn("2026-10-20", result["message"])
        self.assertEqual(self.service.get_scanner_board.call_args.kwargs["target_date"], dt.date(2026, 10, 20))
        self.service.persist_scanner_top_hits_snapshot.assert_called_once()

    def test_incomplete_batch_waits_even_with_unchanged_yesterday_snapshot(self):
        self.set_status("failed", "2026-10-20T23:00:00Z")
        self.service.get_scanner_top_hits_snapshot_refresh_state.return_value = {
            "has_current_snapshot": True, "source_changed": False, "latest_source_at": "2026-10-19T23:00:00Z"}
        result = self.refresh()
        self.assertEqual(result["status"], "waiting")
        self.assertIn("scanner_a", result["message"])
        self.service.persist_scanner_top_hits_snapshot.assert_not_called()

    def test_incremental_flag_does_not_bypass_active_or_failed_prerequisites(self):
        for state in ("running", "failed"):
            with self.subTest(state=state):
                self.set_status(state, "2026-10-20T23:00:00Z")
                self.assertEqual(self.refresh()["status"], "waiting")
        self.service.persist_scanner_top_hits_snapshot.assert_not_called()

    def test_preclose_and_weekend_do_not_publish_daily_snapshot(self):
        for now in (dt.datetime(2026,10,20,19,30,tzinfo=dt.timezone.utc),
                    dt.datetime(2026,10,24,23,30,tzinfo=dt.timezone.utc)):
            with self.subTest(now=now):
                self.assertEqual(self.refresh(now)["status"], "waiting")
        self.service.persist_scanner_top_hits_snapshot.assert_not_called()

    def test_holiday_or_stale_scanner_dates_do_not_publish_new_day(self):
        self.service.get_scanner_board.return_value["cards"][0]["sort_date"] = "2026-10-19"
        self.assertEqual(self.refresh()["status"], "waiting")
        self.service.persist_scanner_top_hits_snapshot.assert_not_called()

    def test_preclose_source_run_is_not_completed_daily_bar_evidence(self):
        self.service.get_scanner_board.return_value["cards"][0]["captured_at"] = "2026-10-20T18:00:00Z"
        self.assertEqual(self.refresh()["status"], "waiting")
        self.service.persist_scanner_top_hits_snapshot.assert_not_called()

    def test_changed_source_waits_for_debounce(self):
        self.service.get_scanner_top_hits_snapshot_refresh_state.return_value["latest_source_at"] = "2026-10-20T23:29:00Z"
        self.assertEqual(self.refresh()["status"], "waiting")
        self.service.persist_scanner_top_hits_snapshot.assert_not_called()

    def test_unchanged_today_has_explicit_current_outcome(self):
        self.service.get_scanner_top_hits_snapshot_refresh_state.return_value.update(has_current_snapshot=True, source_changed=False)
        result = self.refresh()
        self.assertEqual(result["status"], "current")
        self.assertIn("2026-10-20", result["message"])
        self.service.persist_scanner_top_hits_snapshot.assert_not_called()

    def test_unconfigured_prerequisites_fail_closed(self):
        (self.root / "config" / "scheduled_jobs.json").unlink()
        self.assertEqual(self.refresh()["status"], "waiting")
        self.service.persist_scanner_top_hits_snapshot.assert_not_called()

    def test_missing_benchmark_session_bar_does_not_publish_holiday(self):
        self.bars.return_value = None
        self.assertEqual(self.refresh()["status"], "waiting")
        self.service.persist_scanner_top_hits_snapshot.assert_not_called()

    def test_cli_cannot_weaken_configured_prerequisites(self):
        self.set_status("failed", "2026-10-20T23:00:00Z")
        self.assertEqual(self.refresh(options={"required_job_ids": []})["status"], "waiting")
        self.service.persist_scanner_top_hits_snapshot.assert_not_called()

    def test_manual_path_shares_completed_and_incomplete_batch_rules(self):
        from src.webapp.services.watchlist_service import WatchlistService

        service = WatchlistService(artifacts_dir=self.root / "artifacts", database_url="")
        with patch("src.webapp.services.scanner_top_hits_refresh._configured_options", return_value={"required_job_ids": ["scanner_a"]}), \
             patch.object(service, "get_scanner_board", return_value=self.service.get_scanner_board.return_value) as board, \
             patch.object(service, "get_scanner_top_hits_snapshot_refresh_state", return_value=self.service.get_scanner_top_hits_snapshot_refresh_state.return_value), \
             patch.object(service, "persist_scanner_top_hits_snapshot", return_value={"rows": []}) as persist:
            now = dt.datetime(2026,10,20,23,30,tzinfo=dt.timezone.utc)
            self.assertEqual(service.force_scanner_board_refresh(now=now)["snapshot_refresh"]["status"], "rebuilt")
            persist.assert_called_once()
            self.assertEqual(board.call_args_list[0].kwargs["target_date"], dt.date(2026,10,20))
            self.set_status("running", "2026-10-20T23:00:00Z")
            self.assertEqual(service.force_scanner_board_refresh(now=now)["snapshot_refresh"]["status"], "waiting")
            persist.assert_called_once()
        self.assertFalse(service._scanner_board_override_path.exists())

    def test_board_reads_latest_published_date_before_legacy_cutoff_and_ignores_override(self):
        from src.webapp.services.watchlist_service import WatchlistService

        service = WatchlistService(artifacts_dir=self.root / "artifacts", database_url="")
        service._scanner_board_override_path.write_text(json.dumps({"target_trading_date":"2026-10-21"}))
        now = dt.datetime(2026,10,20,23,30,tzinfo=dt.timezone.utc)
        with patch.object(service.screener_history_service, "is_configured", return_value=True), \
             patch.object(service.screener_history_service, "list_runs", return_value=[{"run_date":"2026-10-20"}]) as latest, \
             patch.object(service, "_get_scanner_board_from_database", return_value={"cards":[]}) as board:
            service.get_scanner_board(now=now)
            self.assertEqual(board.call_args.kwargs["target_trading_date"], dt.date(2026,10,20))
            self.assertFalse(board.call_args.kwargs["manual_override_active"])
            latest.assert_called_once_with(strategy_id="scanner_top_hits_snapshot", limit=1)

    def test_new_publication_is_fresh_before_legacy_cutoff(self):
        from src.webapp.services.watchlist_service import WatchlistService

        service = WatchlistService(artifacts_dir=self.root / "artifacts", database_url="")
        with patch.object(service, "_load_latest_persisted_scanner_top_hits_payload", return_value={
            "target_trading_date":"2026-10-20", "generated_at":"2026-10-20T23:20:00Z", "rows": [], "cards": []}):
            result = service.get_scanner_top_hits_snapshot_payload(now=dt.datetime(2026,10,20,23,30,tzinfo=dt.timezone.utc))
        self.assertEqual(result["snapshot"]["freshness"], "fresh")
        self.assertEqual(result["snapshot"]["age_seconds"], 600)

    def test_waiting_reason_exposed_without_first_snapshot_or_worker_status(self):
        from src.webapp.services.watchlist_service import WatchlistService

        self.set_status("failed", "2026-10-20T23:00:00Z")
        self.refresh()
        service = WatchlistService(artifacts_dir=self.root / "artifacts", database_url="")
        with patch.object(service, "_load_latest_persisted_scanner_top_hits_payload", return_value=None):
            result = service.get_scanner_top_hits_snapshot_payload(now=dt.datetime(2026,10,20,23,30,tzinfo=dt.timezone.utc))
        self.assertEqual(result["snapshot"]["refresh_status"], "waiting")
        self.assertIn("scanner_a", result["snapshot"]["refresh_message"])

    def test_malformed_decision_does_not_break_snapshot_reads(self):
        from src.webapp.services.watchlist_service import _load_scanner_top_hits_refresh_state

        for payload in ([], None, "broken"):
            with self.subTest(payload=payload):
                (self.status / "scanner_top_hits_refresh.json").write_text(json.dumps(payload))
                self.assertEqual(_load_scanner_top_hits_refresh_state(self.status)["refresh_status"], "idle")

    def test_shared_lock_prevents_overlapping_manual_or_scheduled_build(self):
        from src.webapp.services.scanner_top_hits_refresh import _snapshot_build_lock

        with _snapshot_build_lock(self.status / "build_scanner_top_hits_snapshot.lock") as acquired:
            self.assertTrue(acquired)
            result = self.refresh()
        self.assertEqual(result["status"], "waiting")
        self.assertIn("another Top Hits build", result["message"])
        self.service.persist_scanner_top_hits_snapshot.assert_not_called()

    def test_persistence_failure_is_not_reported_as_rebuilt(self):
        self.service.persist_scanner_top_hits_snapshot.side_effect = RuntimeError("database unavailable")
        with self.assertRaisesRegex(RuntimeError, "database unavailable"):
            self.refresh()
        decision = json.loads((self.status / "scanner_top_hits_refresh.json").read_text())
        self.assertEqual(decision["status"], "failed")

    def test_newer_manual_outcome_replaces_stale_failed_worker_status(self):
        from src.webapp.services.watchlist_service import _load_scanner_top_hits_refresh_state

        (self.status / "scanner_top_hits_refresh.json").write_text(json.dumps({"status":"rebuilt",
            "message":"Rebuilt for 2026-10-20", "checked_at":"2026-10-20T23:30:00Z"}))
        for worker_status, finished, expected in (("failed","2026-10-20T23:00:00Z","rebuilt"),
                ("waiting","2026-10-20T23:00:00Z","rebuilt"),
                ("failed","2026-10-20T23:35:00Z","failed"),
                ("failed",None,"rebuilt"),
                ("running","2026-10-20T23:00:00Z","running")):
            with self.subTest(worker_status=worker_status, finished=finished):
                (self.status / "build_scanner_top_hits_snapshot.json").write_text(json.dumps({
                    "status":worker_status, "last_finished_at":finished, "message":"Worker status"}))
                self.assertEqual(_load_scanner_top_hits_refresh_state(self.status)["refresh_status"], expected)
