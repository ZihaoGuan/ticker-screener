from __future__ import annotations

import datetime as dt
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import MagicMock, patch

from src.webapp.services import run_service as run_service_module
from src.webapp.services.discord_notification_service import DiscordNotificationService
from src.webapp.services.run_service import RunService


class _DummyProcess:
    def __init__(self, *, pid: int | None = None) -> None:
        self.terminated = False
        self.pid = pid

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

    def test_resolve_as_of_date_accepts_template_token(self) -> None:
        resolved = self.service._resolve_as_of_date({"as_of_date": "{{local_date}}"})

        self.assertEqual(resolved, dt.date.today())

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

    def test_update_progress_combines_stage_markers_with_rating_loop_progress(self) -> None:
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
                "Stage 3/3: Build Ticker Ratings",
                "[125/3621] rating ARM status=ok",
            ],
        )

        self.assertEqual(job["progress_current"], 125)
        self.assertEqual(job["progress_total"], 3621)
        self.assertEqual(job["progress_percent"], 3)
        self.assertEqual(job["progress_label"], "Stage 3/3 · 125/3621 build ticker ratings")

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

    def test_persist_completed_job_patches_screen_run_id_and_notifies(self) -> None:
        summary_path = self.project_root / "artifacts" / "output" / "run_summary.json"
        raw_path = self.project_root / "artifacts" / "output" / "raw.json"
        summary_path.parent.mkdir(parents=True, exist_ok=True)
        summary_path.write_text(
            json.dumps(
                {
                    "strategy_id": "weekly_rs",
                    "raw_results_file": str(raw_path),
                    "watchlist_file": "/tmp/weekly_rs_new_high_2026-06-20.json",
                }
            ),
            encoding="utf-8",
        )
        raw_path.write_text(json.dumps({"rows": []}), encoding="utf-8")
        notifier = MagicMock(spec=DiscordNotificationService)
        service = RunService(project_root=self.project_root, discord_notification_service=notifier)
        service.history_repository.update_job_run = MagicMock()  # type: ignore[method-assign]
        service.history_repository.patch_job_run_result = MagicMock()  # type: ignore[method-assign]
        service.screener_history_service.persist_screen_run = MagicMock(return_value=321)  # type: ignore[method-assign]
        job = {
            "job_id": "job-3",
            "job_run_id": 55,
            "action_id": "weekly_rs",
            "label": "Run Weekly RS",
            "status": "success",
            "finished_at": "2026-06-20T00:01:00+00:00",
            "return_code": 0,
            "summary_file": str(summary_path),
            "watchlist_file": "/tmp/weekly_rs_new_high_2026-06-20.json",
            "raw_results_file": str(raw_path),
            "success_count": 8,
            "options": {},
            "trigger_source": "manual",
        }
        RunService._jobs = [job]
        RunService._jobs_by_id = {"job-3": job}

        service._persist_completed_job("job-3")

        service.history_repository.patch_job_run_result.assert_called_with(55, result_payload_patch={"screen_run_id": 321})
        notifier.notify_job_completion.assert_called_once()
        self.assertEqual(job["screen_run_id"], 321)

    def test_build_command_supports_trendline_snapshot_backfill(self) -> None:
        command = self.service.build_command(
            "backfill_trendline_snapshots",
            {
                "tickers": ["AAPL", "NVDA"],
                "start_date": "2026-06-01",
                "end_date": "2026-06-13",
            },
        )

        self.assertEqual(command[1], "scripts/backfill_trendline_snapshots.py")
        self.assertEqual(
            command[2:],
            [
                "--tickers",
                "AAPL",
                "NVDA",
                "--start-date",
                "2026-06-01",
                "--end-date",
                "2026-06-13",
            ],
        )

    def test_list_actions_prefers_new_8w_runup_name(self) -> None:
        actions = {item["id"]: item for item in self.service.list_actions()}

        self.assertIn("eight_week_100_runup", actions)
        self.assertNotIn("htf_8w_runup", actions)
        self.assertEqual(actions["eight_week_100_runup"]["label"], "Run 8W 100% Runup")

    def test_list_actions_includes_venu_scanner(self) -> None:
        actions = {item["id"]: item for item in self.service.list_actions()}

        self.assertIn("venu_scanner", actions)
        self.assertEqual(actions["venu_scanner"]["label"], "Run Venu Scanner")
        self.assertIn("scripts/run_venu_scanner.py", actions["venu_scanner"]["command"])

    def test_list_actions_includes_vcp_scored(self) -> None:
        actions = {item["id"]: item for item in self.service.list_actions()}

        self.assertIn("vcp_scored", actions)
        self.assertEqual(actions["vcp_scored"]["label"], "Run VCP Scored")
        self.assertIn("scripts/run_vcp_scored_screen.py", actions["vcp_scored"]["command"])

    def test_list_actions_includes_canslim_v2(self) -> None:
        actions = {item["id"]: item for item in self.service.list_actions()}

        self.assertIn("canslim_v2", actions)
        self.assertEqual(actions["canslim_v2"]["label"], "Run CANSLIM V2")
        self.assertIn("scripts/run_canslim_v2_screen.py", actions["canslim_v2"]["command"])

    def test_list_actions_includes_gamma_squeeze(self) -> None:
        actions = {item["id"]: item for item in self.service.list_actions()}

        self.assertIn("gamma_squeeze", actions)
        self.assertEqual(actions["gamma_squeeze"]["label"], "Run Gamma Squeeze")
        self.assertIn("scripts/run_gamma_squeeze_screen.py", actions["gamma_squeeze"]["command"])

    def test_list_actions_includes_stockbee_momentum_burst(self) -> None:
        actions = {item["id"]: item for item in self.service.list_actions()}

        self.assertIn("stockbee_momentum_burst", actions)
        self.assertEqual(actions["stockbee_momentum_burst"]["label"], "Run Stockbee Momentum Burst")
        self.assertIn("scripts/run_stockbee_momentum_burst_screen.py", actions["stockbee_momentum_burst"]["command"])

    def test_list_actions_includes_market_breadth(self) -> None:
        actions = {item["id"]: item for item in self.service.list_actions()}

        self.assertIn("market_breadth", actions)
        self.assertEqual(actions["market_breadth"]["label"], "Run Market Breadth")
        self.assertIn("scripts/run_market_breadth_analysis.py", actions["market_breadth"]["command"])

    def test_build_command_supports_market_breadth_date_label(self) -> None:
        command = self.service.build_command(
            "market_breadth",
            {
                "date_label": "after-close-2026-06-26",
            },
        )

        self.assertEqual(
            command,
            [
                run_service_module.sys.executable,
                "scripts/run_market_breadth_analysis.py",
                "--date-label",
                "after-close-2026-06-26",
            ],
        )

    def test_list_actions_includes_uptrend_analysis(self) -> None:
        actions = {item["id"]: item for item in self.service.list_actions()}

        self.assertIn("uptrend_analysis", actions)
        self.assertEqual(actions["uptrend_analysis"]["label"], "Run Uptrend Analyzer")
        self.assertIn("scripts/run_uptrend_analysis.py", actions["uptrend_analysis"]["command"])

    def test_build_command_supports_uptrend_analysis_date_label(self) -> None:
        command = self.service.build_command(
            "uptrend_analysis",
            {
                "date_label": "after-close-2026-06-26",
            },
        )

        self.assertEqual(
            command,
            [
                run_service_module.sys.executable,
                "scripts/run_uptrend_analysis.py",
                "--date-label",
                "after-close-2026-06-26",
            ],
        )

    def test_list_actions_includes_ibd_distribution_day_monitor(self) -> None:
        actions = {item["id"]: item for item in self.service.list_actions()}

        self.assertIn("ibd_distribution_day_monitor", actions)
        self.assertEqual(actions["ibd_distribution_day_monitor"]["label"], "Run IBD Distribution Day Monitor")
        self.assertIn("scripts/run_ibd_distribution_day_monitor.py", actions["ibd_distribution_day_monitor"]["command"])

    def test_build_command_supports_ibd_distribution_day_monitor_as_of_date(self) -> None:
        command = self.service.build_command(
            "ibd_distribution_day_monitor",
            {
                "as_of_date": "2026-06-26",
            },
        )

        self.assertEqual(
            command,
            [
                run_service_module.sys.executable,
                "scripts/run_ibd_distribution_day_monitor.py",
                "--as-of-date",
                "2026-06-26",
            ],
        )

    def test_list_actions_includes_exposure_coach(self) -> None:
        actions = {item["id"]: item for item in self.service.list_actions()}

        self.assertIn("exposure_coach", actions)
        self.assertEqual(actions["exposure_coach"]["label"], "Run Exposure Coach")
        self.assertIn("scripts/run_exposure_coach.py", actions["exposure_coach"]["command"])

    def test_build_command_supports_exposure_coach_date_label(self) -> None:
        command = self.service.build_command(
            "exposure_coach",
            {
                "date_label": "after-close-2026-06-26",
            },
        )

        self.assertEqual(
            command,
            [
                run_service_module.sys.executable,
                "scripts/run_exposure_coach.py",
                "--date-label",
                "after-close-2026-06-26",
            ],
        )

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

    def test_cancel_terminates_process_group_when_pid_available(self) -> None:
        process = _DummyProcess(pid=43210)
        job = {
            "job_id": "job-2",
            "action_id": "run_finviz_ratings_pipeline",
            "label": "Run Finviz Ratings Pipeline",
            "status": "running",
            "command": "python scripts/run_finviz_ratings_pipeline.py",
            "started_at": "2026-06-13T00:00:00+00:00",
            "finished_at": "",
            "return_code": None,
            "log_tail": "",
            "progress_current": None,
            "progress_total": None,
            "progress_percent": None,
            "progress_label": "Stage 1/3",
            "success_count": 0,
            "watchlist_file": "",
            "summary_file": "",
            "cancel_requested": False,
            "_started_monotonic": 10.0,
            "_process": process,
        }
        RunService._jobs = [job]
        RunService._jobs_by_id = {"job-2": job}

        with patch.object(run_service_module.os, "getpgid", return_value=43210), patch.object(run_service_module.os, "killpg") as killpg:
            payload = self.service.cancel("job-2")

        killpg.assert_called_once_with(43210, run_service_module.signal.SIGTERM)
        self.assertFalse(process.terminated)
        self.assertTrue(job["cancel_requested"])
        self.assertTrue(payload["cancel_requested"])

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

    def test_build_command_supports_reload_postgres_market_data_date(self) -> None:
        command = self.service.build_command(
            "reload_postgres_market_data_date",
            {
                "trade_date": "2026-06-13",
                "chunk_size": "25",
                "max_retries": "5",
                "retry_base_seconds": "3",
                "chunk_sleep_seconds": "2",
                "single_ticker_sleep_seconds": "1",
                "batch_size": "4000",
                "ensure_schema": True,
            },
        )

        self.assertEqual(command[1], "scripts/reload_postgres_market_data_date.py")
        self.assertEqual(command[2], "2026-06-13")
        self.assertIn("--chunk-size", command)
        self.assertIn("25", command)
        self.assertIn("--max-retries", command)
        self.assertIn("5", command)
        self.assertIn("--retry-base-seconds", command)
        self.assertIn("3.0", command)
        self.assertIn("--chunk-sleep-seconds", command)
        self.assertIn("2.0", command)
        self.assertIn("--single-ticker-sleep-seconds", command)
        self.assertIn("1.0", command)
        self.assertIn("--batch-size", command)
        self.assertIn("4000", command)
        self.assertIn("--ensure-schema", command)

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
        self.assertIn("ema21_pullback_buy", action_ids)
        self.assertIn("sma200_pullback_buy", action_ids)
        self.assertIn("high_tight_flag", action_ids)
        self.assertIn("high_tight_flag_setup", action_ids)
        self.assertIn("leif_high_tight_flag", action_ids)
        self.assertIn("sepa_vcp", action_ids)
        self.assertIn("rti", action_ids)
        self.assertIn("sean_breakout", action_ids)
        self.assertIn("sean_gap_up", action_ids)
        self.assertIn("trend_template", action_ids)
        self.assertIn("canslim", action_ids)
        self.assertIn("vcs_setup_stage", action_ids)
        self.assertIn("vcs_critical_tightness", action_ids)
        self.assertIn("base_detection", action_ids)
        self.assertIn("cup_detection", action_ids)
        self.assertIn("double_bottom_detection", action_ids)
        self.assertIn("weekly_tight_close", action_ids)
        self.assertIn("weinstein_stage2_early", action_ids)
        self.assertIn("weekly_tight_close_breakout", action_ids)
        self.assertIn("three_weeks_tight", action_ids)
        self.assertIn("inside_dryup", action_ids)
        self.assertIn("inside_dryup_v2", action_ids)
        self.assertIn("wyckoff_buy_signal", action_ids)
        self.assertIn("wyckoff_sell_signal", action_ids)
        self.assertIn("daily_rs_new_high", action_ids)
        self.assertIn("weekly_rs_new_high", action_ids)
        self.assertIn("weekly_rs_before_price", action_ids)
        self.assertNotIn("weekly_rs", action_ids)
        self.assertNotIn("sean_peg", action_ids)
        self.assertIn("earnings_weekly_criteria", action_ids)
        self.assertIn("sync_finviz_fundamentals", action_ids)
        self.assertIn("build_sector_rating_baselines", action_ids)
        self.assertIn("build_ticker_ratings", action_ids)
        self.assertIn("build_technical_ratings", action_ids)
        self.assertIn("run_finviz_ratings_pipeline", action_ids)
        self.assertIn("sync_chart_fundamentals_cache", action_ids)
        self.assertIn("flashalpha_gex_close", action_ids)
        self.assertIn("gamma_squeeze", action_ids)
        self.assertIn("backfill_trendline_snapshots", action_ids)
        self.assertIn("reload_postgres_market_data_date", action_ids)

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
                "include_sectors": ["Technology"],
                "delay_min_seconds": "4",
                "delay_max_seconds": "7",
                "batch_size_before_rest": "80",
                "rest_seconds": "50",
                "retry_failed_from_manifest": True,
                "circuit_breaker_consecutive_503": "12",
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
        self.assertIn("--include-sectors", command)
        self.assertIn("Technology", command)
        self.assertIn("--delay-min-seconds", command)
        self.assertIn("4.0", command)
        self.assertIn("--delay-max-seconds", command)
        self.assertIn("7.0", command)
        self.assertIn("--batch-size-before-rest", command)
        self.assertIn("80", command)
        self.assertIn("--rest-seconds", command)
        self.assertIn("50.0", command)
        self.assertIn("--retry-failed-from-manifest", command)
        self.assertIn("--circuit-breaker-consecutive-503", command)
        self.assertIn("12", command)
        self.assertIn("--min-sector-peers", command)
        self.assertIn("25", command)

    def test_launch_finviz_ratings_pipeline_defaults_to_skip_existing(self) -> None:
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
            },
        )

        self.assertEqual(job_id, captured["job_id"])
        command = captured["command"]
        overwrite_index = command.index("--overwrite-policy")
        self.assertEqual(command[overwrite_index + 1], "skip-existing")

    def test_launch_chart_fundamentals_cache_sync_includes_focused_options(self) -> None:
        captured: dict[str, object] = {}

        def fake_run_job(job_id: str, command: list[str], env: dict[str, str]) -> None:
            captured["job_id"] = job_id
            captured["command"] = command
            captured["env"] = env

        self.service._run_job = fake_run_job  # type: ignore[method-assign]

        job_id = self.service.launch(
            "sync_chart_fundamentals_cache",
            options={
                "as_of_date": "2026-06-15",
                "fundamental_limit": "220",
                "technical_limit": "180",
                "upcoming_weeks": "2",
                "earnings_limit": "10",
                "overwrite_policy": "replace-date",
            },
        )

        self.assertEqual(job_id, captured["job_id"])
        command = captured["command"]
        self.assertIn("scripts/sync_chart_fundamentals_cache.py", command)
        self.assertIn("--as-of-date", command)
        self.assertIn("2026-06-15", command)
        self.assertIn("--fundamental-limit", command)
        self.assertIn("220", command)
        self.assertIn("--technical-limit", command)
        self.assertIn("180", command)
        self.assertIn("--upcoming-weeks", command)
        self.assertIn("2", command)
        self.assertIn("--earnings-limit", command)
        self.assertIn("10", command)
        self.assertIn("--overwrite-policy", command)
        self.assertIn("replace-date", command)

    def test_launch_remote_finviz_pipeline_queues_db_job_and_skips_local_runner(self) -> None:
        captured_patch: dict[str, object] = {}

        def fake_patch_job_run_result(job_run_id: int | None, **kwargs: object) -> None:
            captured_patch["job_run_id"] = job_run_id
            captured_patch["kwargs"] = kwargs

        self.service.history_repository.create_job_run = lambda **kwargs: 901  # type: ignore[method-assign]
        self.service.history_repository.patch_job_run_result = fake_patch_job_run_result  # type: ignore[method-assign]
        self.service.history_repository.healthy_remote_worker_count = lambda stale_after_seconds=None: 1  # type: ignore[method-assign]

        job_id = self.service.launch(
            "run_finviz_ratings_pipeline",
            options={
                "as_of_date": "2026-06-13",
                "execution_mode": "remote",
                "target_worker": "worker-a",
            },
        )

        self.assertEqual(job_id, "remote-901")
        self.assertEqual(captured_patch["job_run_id"], 901)
        kwargs = captured_patch["kwargs"]
        self.assertEqual(kwargs["status"], "queued")
        self.assertEqual(kwargs["result_payload_patch"]["target_worker"], "worker-a")
        self.assertEqual(kwargs["result_payload_patch"]["execution_mode"], "remote")

    def test_launch_remote_falls_back_to_local_when_no_healthy_workers(self) -> None:
        captured: dict[str, object] = {}

        def fake_run_job(job_id: str, command: list[str], env: dict[str, str]) -> None:
            captured["job_id"] = job_id
            captured["command"] = command
            captured["env"] = env

        self.service._run_job = fake_run_job  # type: ignore[method-assign]
        self.service.history_repository.create_job_run = lambda **kwargs: 777  # type: ignore[method-assign]
        self.service.history_repository.healthy_remote_worker_count = lambda stale_after_seconds=None: 0  # type: ignore[method-assign]

        job_id = self.service.launch(
            "run_finviz_ratings_pipeline",
            options={
                "as_of_date": "2026-06-13",
                "execution_mode": "remote",
            },
        )

        self.assertEqual(job_id, captured["job_id"])
        job = self.service.get_job(job_id)
        self.assertEqual(job["job_run_id"], 777)
        self.assertEqual(job["execution_mode"], "local")
        self.assertIn("No healthy remote workers detected", job["log_tail"])

    def test_launch_remote_rejects_unsupported_action(self) -> None:
        with self.assertRaisesRegex(ValueError, "Remote worker execution"):
            self.service.launch("rs", options={"execution_mode": "remote"})

    def test_cancel_remote_job_uses_repository_cancel(self) -> None:
        remote_row = {
            "id": 915,
            "parent_job_run_id": None,
            "job_type": "admin_sync",
            "job_name": "Run Finviz Ratings Pipeline",
            "status": "running",
            "trigger_source": "manual",
            "request_payload": {
                "action_id": "run_finviz_ratings_pipeline",
                "options": {"as_of_date": "2026-06-13", "execution_mode": "remote"},
            },
            "result_payload": {
                "command": "python scripts/run_finviz_ratings_pipeline.py --as-of-date 2026-06-13",
                "progress_label": "Stage 1/3",
                "cancel_requested": True,
            },
            "artifact_path": "",
            "started_at": dt.datetime(2026, 6, 13, 0, 0, tzinfo=dt.timezone.utc),
            "finished_at": None,
            "created_at": dt.datetime(2026, 6, 13, 0, 0, tzinfo=dt.timezone.utc),
        }
        self.service.history_repository.get_job_run = lambda job_run_id: remote_row if job_run_id == 915 else None  # type: ignore[method-assign]
        self.service.history_repository.request_remote_job_cancel = lambda job_run_id: remote_row if job_run_id == 915 else None  # type: ignore[method-assign]

        payload = self.service.cancel("remote-915")

        self.assertEqual(payload["job_id"], "remote-915")
        self.assertTrue(payload["cancel_requested"])
        self.assertEqual(payload["execution_mode"], "remote")

    def test_list_jobs_includes_remote_queue_jobs(self) -> None:
        self.service.history_repository.list_remote_job_runs = lambda limit=20: [  # type: ignore[method-assign]
            {
                "id": 930,
                "parent_job_run_id": None,
                "job_type": "admin_sync",
                "job_name": "Sync Finviz Fundamentals",
                "status": "queued",
                "trigger_source": "scheduler",
                "request_payload": {
                    "action_id": "sync_finviz_fundamentals",
                    "options": {"as_of_date": "2026-06-13", "execution_mode": "remote", "target_worker": "worker-b"},
                },
                "result_payload": {"progress_label": "Queued for remote worker", "worker_name": ""},
                "artifact_path": "",
                "started_at": dt.datetime(2026, 6, 13, 0, 0, tzinfo=dt.timezone.utc),
                "finished_at": None,
                "created_at": dt.datetime(2026, 6, 13, 0, 0, tzinfo=dt.timezone.utc),
            }
        ]

        payload = self.service.list_jobs(limit=10)

        self.assertEqual(payload[0]["job_id"], "remote-930")
        self.assertEqual(payload[0]["status"], "queued")
        self.assertEqual(payload[0]["target_worker"], "worker-b")
        self.assertEqual(payload[0]["execution_mode"], "remote")

    def test_list_jobs_includes_persisted_local_jobs_when_memory_is_empty(self) -> None:
        self.service.history_repository.list_local_job_runs = lambda limit=20: [  # type: ignore[method-assign]
            {
                "id": 941,
                "parent_job_run_id": None,
                "job_type": "admin_sync",
                "job_name": "Run Finviz Ratings Pipeline",
                "status": "running",
                "trigger_source": "manual",
                "request_payload": {
                    "action_id": "run_finviz_ratings_pipeline",
                    "execution_mode": "local",
                    "options": {"as_of_date": "2026-06-13"},
                },
                "result_payload": {
                    "job_id": "job-local-941",
                    "command": "python scripts/run_finviz_ratings_pipeline.py --as-of-date 2026-06-13",
                    "progress_label": "Stage 1/3",
                    "log_tail": "[9/3621] ABBV fundamentals_ok sector=Health Care",
                },
                "artifact_path": "",
                "started_at": dt.datetime(2026, 6, 13, 0, 0, tzinfo=dt.timezone.utc),
                "finished_at": None,
                "created_at": dt.datetime(2026, 6, 13, 0, 0, tzinfo=dt.timezone.utc),
            }
        ]

        payload = self.service.list_jobs(limit=10)

        self.assertEqual(payload[0]["job_id"], "job-local-941")
        self.assertEqual(payload[0]["status"], "running")
        self.assertEqual(payload[0]["execution_mode"], "local")

    def test_recover_remote_jobs_starts_local_fallback_when_workers_are_down(self) -> None:
        captured: dict[str, object] = {}

        def fake_run_job(job_id: str, command: list[str], env: dict[str, str]) -> None:
            captured["job_id"] = job_id
            captured["command"] = command

        self.service._run_job = fake_run_job  # type: ignore[method-assign]
        self.service.history_repository.requeue_stale_remote_job_runs = lambda stale_after_seconds=None: [  # type: ignore[method-assign]
            {"id": 901}
        ]
        self.service.history_repository.healthy_remote_worker_count = lambda stale_after_seconds=None: 0  # type: ignore[method-assign]
        fallback_row = {
            "id": 915,
            "parent_job_run_id": None,
            "job_type": "admin_sync",
            "job_name": "Run Finviz Ratings Pipeline",
            "status": "running",
            "trigger_source": "scheduler",
            "request_payload": {
                "action_id": "run_finviz_ratings_pipeline",
                "options": {"as_of_date": "2026-06-13", "execution_mode": "remote"},
            },
            "result_payload": {
                "message": "No healthy remote workers detected. Falling back to local execution.",
            },
            "artifact_path": "",
            "started_at": dt.datetime(2026, 6, 13, 0, 0, tzinfo=dt.timezone.utc),
            "finished_at": None,
            "created_at": dt.datetime(2026, 6, 13, 0, 0, tzinfo=dt.timezone.utc),
        }
        claims = [fallback_row, None]
        self.service.history_repository.claim_remote_job_run_for_local_fallback = lambda: claims.pop(0)  # type: ignore[method-assign]

        result = self.service.recover_remote_jobs()

        self.assertEqual(result["requeued"], 1)
        self.assertEqual(result["local_fallback_started"], 1)
        self.assertEqual(captured["job_id"], "remote-915")

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

    def test_get_child_job_serializes_child_run(self) -> None:
        self.service.history_repository.get_job_run = lambda job_run_id: {  # type: ignore[method-assign]
            "id": job_run_id,
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
                "log_file": "/tmp/rs.log",
            },
            "artifact_path": "/tmp/summary.json",
            "started_at": dt.datetime(2026, 6, 1, 0, 0, tzinfo=dt.timezone.utc),
            "finished_at": dt.datetime(2026, 6, 1, 0, 1, tzinfo=dt.timezone.utc),
            "created_at": dt.datetime(2026, 6, 1, 0, 0, tzinfo=dt.timezone.utc),
        }

        payload = self.service.get_child_job(501)

        self.assertEqual(payload["job_run_id"], 501)
        self.assertEqual(payload["strategy_id"], "rs")
        self.assertEqual(payload["screen_run_id"], 77)
        self.assertEqual(payload["log_file"], "/tmp/rs.log")

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
