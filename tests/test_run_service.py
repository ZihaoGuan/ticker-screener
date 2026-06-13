from __future__ import annotations

import datetime as dt
import json
from pathlib import Path
import tempfile
import unittest

from src.webapp.services import run_service as run_service_module
from src.webapp.services.run_service import RunService


class _DummyProcess:
    def __init__(self) -> None:
        self.terminated = False

    def terminate(self) -> None:
        self.terminated = True


class RunServiceTests(unittest.TestCase):
    def setUp(self) -> None:
        RunService._jobs = []
        RunService._jobs_by_id = {}
        self.temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp_dir.cleanup)
        self.project_root = Path(self.temp_dir.name)
        self.service = RunService(project_root=self.project_root)

    def test_update_progress_tracks_percent_and_success_count(self) -> None:
        job = {
            "success_count": 0,
            "progress_current": None,
            "progress_total": None,
            "progress_percent": None,
            "progress_label": None,
        }

        self.service._update_progress(
            job,
            [
                "[1/10] screening AAPL | passed=0",
                "[4/10] screening NVDA | passed=2",
            ],
        )

        self.assertEqual(job["success_count"], 2)
        self.assertEqual(job["progress_current"], 4)
        self.assertEqual(job["progress_total"], 10)
        self.assertEqual(job["progress_percent"], 40)
        self.assertEqual(job["progress_label"], "4/10 screening")

    def test_update_progress_uses_stage_markers_when_batch_progress_not_available(self) -> None:
        job = {
            "success_count": 0,
            "progress_current": None,
            "progress_total": None,
            "progress_percent": None,
            "progress_label": None,
        }

        self.service._update_progress(
            job,
            [
                "running: /opt/python scripts/sync_finviz_fundamentals.py --as-of-date 2026-06-13",
                "Stage 2/3: Build Sector Rating Baselines",
            ],
        )

        self.assertIsNone(job["progress_current"])
        self.assertIsNone(job["progress_total"])
        self.assertEqual(job["progress_percent"], 33)
        self.assertEqual(job["progress_label"], "Stage 2/3 · Build Sector Rating Baselines")

    def test_load_summary_metadata_reads_passed_tickers_and_watchlist_file(self) -> None:
        summary_path = self.project_root / "artifacts" / "raw" / "summary.json"
        summary_path.parent.mkdir(parents=True, exist_ok=True)
        summary_path.write_text(
            json.dumps({"passed_tickers": 7, "watchlist_file": "/tmp/watchlist.json"}),
            encoding="utf-8",
        )
        job = {"summary_file": str(summary_path), "success_count": 0, "watchlist_file": ""}

        self.service._load_summary_metadata(job)

        self.assertEqual(job["success_count"], 7)
        self.assertEqual(job["watchlist_file"], "/tmp/watchlist.json")

    def test_serialize_job_exposes_watchlist_navigation_fields(self) -> None:
        payload = self.service._serialize_job(
            {
                "job_id": "job-1",
                "action_id": "rs",
                "label": "Run RS",
                "status": "success",
                "command": "python scripts/run_rs_screen.py",
                "started_at": "2026-05-31T00:00:00+00:00",
                "finished_at": "2026-05-31T00:02:00+00:00",
                "watchlist_file": "/tmp/rs_new_high_2026-05-31.json",
                "_started_monotonic": 1.0,
                "_finished_monotonic": 2.0,
            }
        )

        self.assertEqual(payload["watchlist_stem"], "rs_new_high_2026-05-31")
        self.assertEqual(payload["watchlist_url"], "/watchlists?stem=rs_new_high_2026-05-31")

    def test_serialize_job_resolves_stem_from_dated_watchlist_path(self) -> None:
        watchlist_path = self.project_root / "artifacts" / "screeners" / "2026-05-31" / "weekly_rs" / "watchlist.json"
        watchlist_path.parent.mkdir(parents=True, exist_ok=True)
        summary_path = watchlist_path.parent / "run_summary.json"
        summary_path.write_text(
            json.dumps(
                {
                    "strategy_id": "weekly_rs",
                    "date_label": "dell-2026-05-31",
                    "watchlist_file": str(watchlist_path),
                }
            ),
            encoding="utf-8",
        )

        payload = self.service._serialize_job(
            {
                "job_id": "job-2",
                "action_id": "weekly_rs",
                "label": "Run Weekly RS New High Before Price",
                "status": "success",
                "command": "python scripts/run_weekly_rs_screen.py",
                "started_at": "2026-05-31T00:00:00+00:00",
                "finished_at": "2026-05-31T00:02:00+00:00",
                "watchlist_file": str(watchlist_path),
                "_started_monotonic": 1.0,
                "_finished_monotonic": 2.0,
            }
        )

        self.assertEqual(payload["watchlist_stem"], "weekly_rs_new_high_dell-2026-05-31")
        self.assertEqual(payload["watchlist_url"], "/watchlists?stem=weekly_rs_new_high_dell-2026-05-31")

    def test_cancel_marks_job_and_terminates_process(self) -> None:
        process = _DummyProcess()
        job = {
            "job_id": "job-1",
            "action_id": "rs",
            "label": "Run RS",
            "status": "running",
            "command": "python scripts/run_rs_screen.py",
            "started_at": "2026-05-31T00:00:00+00:00",
            "finished_at": "",
            "return_code": None,
            "log_tail": "Starting...\n",
            "progress_current": None,
            "progress_total": None,
            "progress_percent": None,
            "progress_label": "Starting…",
            "success_count": 0,
            "watchlist_file": "",
            "summary_file": "",
            "cancel_requested": False,
            "_started_monotonic": 10.0,
            "_process": process,
        }
        RunService._jobs = [job]
        RunService._jobs_by_id = {"job-1": job}

        payload = self.service.cancel("job-1")

        self.assertTrue(process.terminated)
        self.assertTrue(job["cancel_requested"])
        self.assertTrue(payload["cancel_requested"])
        self.assertEqual(payload["job_id"], "job-1")
        self.assertIn("Cancellation requested", job["log_tail"])

    def test_launch_sync_job_applies_start_date_end_date_chunk_size_and_tickers(self) -> None:
        captured: dict[str, object] = {}

        def fake_run_job(job_id: str, command: list[str], env: dict[str, str]) -> None:
            captured["job_id"] = job_id
            captured["command"] = command
            captured["env"] = env

        self.service._run_job = fake_run_job  # type: ignore[method-assign]

        job_id = self.service.launch(
            "sync_postgres_market_data",
            options={
                "start_date": "2020-01-01",
                "end_date": "2026-05-31",
                "chunk_size": "55",
                "tickers": "AAPL NVDA",
            },
        )

        self.assertEqual(job_id, captured["job_id"])
        command = captured["command"]
        self.assertIn("scripts/sync_postgres_market_data.py", command)
        self.assertIn("--start-date", command)
        self.assertIn("2020-01-01", command)
        self.assertIn("--end-date", command)
        self.assertIn("2026-05-31", command)
        self.assertIn("--chunk-size", command)
        self.assertIn("55", command)
        tickers_index = command.index("--tickers")
        self.assertEqual(command[tickers_index + 1 : tickers_index + 3], ["AAPL", "NVDA"])

    def test_list_actions_includes_fearzone(self) -> None:
        action_ids = {item["id"] for item in self.service.list_actions()}
        self.assertIn("fearzone", action_ids)
        self.assertIn("fearzone_zeiierman", action_ids)
        self.assertIn("td9_bullish", action_ids)
        self.assertIn("td9_bearish", action_ids)
        self.assertIn("macd_golden_cross", action_ids)
        self.assertIn("macd_dead_cross", action_ids)
        self.assertIn("rsi_ma_bb_bullish", action_ids)
        self.assertIn("rsi_ma_bb_bearish", action_ids)
        self.assertIn("bb_squeeze", action_ids)
        self.assertIn("rti", action_ids)
        self.assertIn("vcs_setup_stage", action_ids)
        self.assertIn("vcs_critical_tightness", action_ids)
        self.assertIn("base_detection", action_ids)
        self.assertIn("cup_detection", action_ids)
        self.assertIn("double_bottom_detection", action_ids)
        self.assertIn("weekly_tight_close", action_ids)
        self.assertIn("weekly_tight_close_breakout", action_ids)
        self.assertIn("three_weeks_tight", action_ids)
        self.assertIn("hve", action_ids)
        self.assertIn("inside_dryup", action_ids)
        self.assertIn("weekly_rs_before_price", action_ids)
        self.assertNotIn("weekly_rs", action_ids)
        self.assertIn("earnings_weekly_criteria", action_ids)
        self.assertIn("sync_finviz_fundamentals", action_ids)
        self.assertIn("build_sector_rating_baselines", action_ids)
        self.assertIn("build_ticker_ratings", action_ids)
        self.assertIn("run_finviz_ratings_pipeline", action_ids)

    def test_launch_finviz_ratings_pipeline_includes_custom_options(self) -> None:
        captured: dict[str, object] = {}

        def fake_run_job(job_id: str, command: list[str], env: dict[str, str]) -> None:
            captured["job_id"] = job_id
            captured["command"] = command
            captured["env"] = env

        self.service._run_job = fake_run_job  # type: ignore[method-assign]

        job_id = self.service.launch(
            "run_finviz_ratings_pipeline",
            options={
                "as_of_date": "2026-06-13",
                "tickers": "NVDA",
                "delay_min_seconds": "4",
                "delay_max_seconds": "7",
                "batch_size_before_rest": "80",
                "rest_seconds": "50",
                "min_sector_peers": "25",
            },
        )

        self.assertEqual(job_id, captured["job_id"])
        command = captured["command"]
        self.assertIn("scripts/run_finviz_ratings_pipeline.py", command)
        self.assertIn("--as-of-date", command)
        self.assertIn("2026-06-13", command)
        self.assertIn("--tickers", command)
        self.assertIn("NVDA", command)
        self.assertIn("--delay-min-seconds", command)
        self.assertIn("4.0", command)
        self.assertIn("--delay-max-seconds", command)
        self.assertIn("7.0", command)
        self.assertIn("--batch-size-before-rest", command)
        self.assertIn("80", command)
        self.assertIn("--rest-seconds", command)
        self.assertIn("50.0", command)
        self.assertIn("--min-sector-peers", command)
        self.assertIn("25", command)

    def test_launch_earnings_weekly_criteria_includes_reference_date(self) -> None:
        captured: dict[str, object] = {}

        def fake_run_job(job_id: str, command: list[str], env: dict[str, str]) -> None:
            captured["job_id"] = job_id
            captured["command"] = command
            captured["env"] = env

        self.service._run_job = fake_run_job  # type: ignore[method-assign]

        job_id = self.service.launch(
            "earnings_weekly_criteria",
            options={
                "reference_date": "2026-06-06",
                "date_label": "2026-06-06",
                "limit": "25",
            },
        )

        self.assertEqual(job_id, captured["job_id"])
        command = captured["command"]
        self.assertIn("scripts/run_earnings_weekly_criteria_screen.py", command)
        self.assertIn("--reference-date", command)
        self.assertIn("2026-06-06", command)
        self.assertIn("--date-label", command)
        self.assertIn("--limit", command)

    def test_launch_signal_warm_batch_includes_parent_job_run_id_and_market_data_source(self) -> None:
        captured: dict[str, object] = {}

        def fake_run_job(job_id: str, command: list[str], env: dict[str, str]) -> None:
            captured["job_id"] = job_id
            captured["command"] = command
            captured["env"] = env

        self.service._run_job = fake_run_job  # type: ignore[method-assign]
        self.service.history_repository.create_job_run = lambda **kwargs: 321  # type: ignore[method-assign]

        job_id = self.service.launch(
            "signal_warm_batch",
            options={
                "strategy_ids": ["rs", "vcp"],
                "start_date": "2026-06-01",
                "end_date": "2026-06-05",
                "market_data_source": "internet",
                "overwrite_policy": "skip_existing",
                "candidate_threshold": "4",
                "max_parallel": "3",
            },
        )

        self.assertEqual(job_id, captured["job_id"])
        command = captured["command"]
        self.assertIn("scripts/run_signal_warm_batch.py", command)
        self.assertIn("--market-data-source", command)
        self.assertIn("internet", command)
        self.assertIn("--job-run-id", command)
        self.assertTrue(any(str(item).isdigit() for item in command[command.index("--job-run-id") + 1 : command.index("--job-run-id") + 2]))

    def test_launch_overlap_backtest_includes_parent_job_run_id(self) -> None:
        captured: dict[str, object] = {}

        def fake_run_job(job_id: str, command: list[str], env: dict[str, str]) -> None:
            captured["job_id"] = job_id
            captured["command"] = command
            captured["env"] = env

        self.service._run_job = fake_run_job  # type: ignore[method-assign]
        self.service.history_repository.create_job_run = lambda **kwargs: 654  # type: ignore[method-assign]

        job_id = self.service.launch(
            "overlap_backtest_v1",
            options={
                "strategy_ids": ["rs", "vcp"],
                "start_date": "2026-06-01",
                "end_date": "2026-06-05",
                "entry_signal_threshold": "4",
                "hold_periods_json": "[5, 10]",
            },
        )

        self.assertEqual(job_id, captured["job_id"])
        command = captured["command"]
        self.assertIn("scripts/run_overlap_backtest_v1.py", command)
        self.assertIn("--job-run-id", command)
        self.assertTrue(any(str(item).isdigit() for item in command[command.index("--job-run-id") + 1 : command.index("--job-run-id") + 2]))

    def test_list_jobs_attaches_batch_child_job_logs(self) -> None:
        RunService._jobs = [
            {
                "job_id": "job-1",
                "action_id": "screener_history_batch",
                "job_run_id": 101,
                "label": "Batch Screener History Cache",
                "status": "running",
                "command": "python scripts/run_screener_history_batch.py",
                "started_at": "2026-06-01T00:00:00+00:00",
                "finished_at": "",
                "return_code": None,
                "log_tail": "Starting...\n",
                "progress_current": None,
                "progress_total": None,
                "progress_percent": None,
                "progress_label": "Starting…",
                "success_count": 0,
                "watchlist_file": "",
                "summary_file": "",
                "cancel_requested": False,
                "_started_monotonic": 10.0,
            }
        ]
        RunService._jobs_by_id = {"job-1": RunService._jobs[0]}
        self.service.history_repository.list_child_job_runs = lambda parent_ids: [  # type: ignore[method-assign]
            {
                "id": 501,
                "parent_job_run_id": 101,
                "job_type": "screen_run",
                "job_name": "Run RS (2026-05-31)",
                "status": "success",
                "request_payload": {"strategy_id": "rs", "run_date": "2026-05-31", "command": "python scripts/run_rs_screen.py"},
                "result_payload": {
                    "strategy_id": "rs",
                    "run_date": "2026-05-31",
                    "screen_run_id": 77,
                    "success_count": 3,
                    "log_tail": "line one\n[2/10] screening MSFT | passed=3\nline two",
                    "message": "Persisted cached screener result",
                },
                "artifact_path": "/tmp/summary.json",
                "started_at": dt.datetime(2026, 6, 1, 0, 0, tzinfo=dt.timezone.utc),
                "finished_at": dt.datetime(2026, 6, 1, 0, 1, tzinfo=dt.timezone.utc),
                "created_at": dt.datetime(2026, 6, 1, 0, 0, tzinfo=dt.timezone.utc),
            }
        ]

        payload = self.service.list_jobs(limit=10)

        self.assertEqual(len(payload), 1)
        self.assertEqual(payload[0]["child_job_summary"]["total"], 1)
        self.assertEqual(payload[0]["child_job_summary"]["success"], 1)
        self.assertEqual(payload[0]["child_jobs"][0]["strategy_id"], "rs")
        self.assertEqual(payload[0]["child_jobs"][0]["screen_run_id"], 77)
        self.assertIn("line two", payload[0]["child_jobs"][0]["log_tail"])
        self.assertEqual(payload[0]["child_jobs"][0]["progress_current"], 2)
        self.assertEqual(payload[0]["child_jobs"][0]["progress_total"], 10)
        self.assertEqual(payload[0]["child_jobs"][0]["progress_percent"], 20)

    def test_precheck_reports_db_ready_vs_fallback(self) -> None:
        self.service.history_repository.is_configured = lambda: True  # type: ignore[method-assign]
        original_load_app_config = run_service_module.load_app_config
        original_build_screener_catalog = run_service_module.build_screener_catalog
        original_load_universe = run_service_module.load_universe
        original_load_many_ticker_windows = run_service_module.load_many_ticker_windows
        original_db_frame_has_recent_coverage = run_service_module.db_frame_has_recent_coverage
        self.addCleanup(setattr, run_service_module, "load_app_config", original_load_app_config)
        self.addCleanup(setattr, run_service_module, "build_screener_catalog", original_build_screener_catalog)
        self.addCleanup(setattr, run_service_module, "load_universe", original_load_universe)
        self.addCleanup(setattr, run_service_module, "load_many_ticker_windows", original_load_many_ticker_windows)
        self.addCleanup(setattr, run_service_module, "db_frame_has_recent_coverage", original_db_frame_has_recent_coverage)

        class _Frame:
            def __init__(self, count: int) -> None:
                self.count = count

            def __len__(self) -> int:
                return self.count

        run_service_module.load_app_config = lambda: original_load_app_config()  # type: ignore[assignment]
        run_service_module.build_screener_catalog = lambda config: {"rs": type("Spec", (), {"lookback_trading_days": 100, "warmup_trading_days": 20, "required_inputs": ("daily_bars", "benchmark_bars", "metadata")})()}  # type: ignore[assignment]
        run_service_module.load_universe = lambda config, limit=None: [run_service_module.UniverseTicker(symbol="AAPL"), run_service_module.UniverseTicker(symbol="MSFT")]  # type: ignore[assignment]
        run_service_module.load_many_ticker_windows = lambda tickers, as_of_date, trading_days_needed, database_url=None: {  # type: ignore[assignment]
            "AAPL": _Frame(120),
            "MSFT": _Frame(50),
            "SPY": _Frame(120),
        }
        run_service_module.db_frame_has_recent_coverage = lambda frame, end_date, tolerance_days=7: True  # type: ignore[assignment]

        result = self.service.precheck("rs", options={"market_data_source": "database-first"})

        self.assertTrue(result["applicable"])
        self.assertEqual(result["db_ready_tickers"], 1)
        self.assertEqual(result["fallback_tickers"], 1)
        self.assertEqual(result["sample_fallback_tickers"], ["MSFT"])


if __name__ == "__main__":
    unittest.main()
