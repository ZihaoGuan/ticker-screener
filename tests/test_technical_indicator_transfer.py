from __future__ import annotations

import datetime as dt
import gzip
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import MagicMock, patch

from scripts import build_technical_indicator_ratings as builder
from scripts.run_external_screener import run as run_external
from src.ratings.models import TechnicalIndicatorRatingSnapshot
from src.ratings.repository import RatingsRepository
from src.ratings.technical_indicator_transfer import read_export, write_export
from src.webapp.repositories.history_repository import HistoryRepository
from src.webapp.services.external_job_result_service import ExternalJobResultService
from src.webapp.services.run_service import RunService


class TechnicalIndicatorTransferTests(unittest.TestCase):
    def setUp(self) -> None:
        self.date = dt.date(2026, 10, 16)
        self.rows = [TechnicalIndicatorRatingSnapshot(
            ticker="NVDA", as_of_date=self.date, timeframe=timeframe,
            technical_status="missing_metrics", technical_status_reason="No OHLCV bars available for timeframe.",
            missing_metric_names=["ohlcv"],
        ) for timeframe in ("1d", "1w", "1m")]

    def test_github_eligibility_and_export_round_trip(self) -> None:
        self.assertIn("github", RunService(project_root=Path("/tmp")).execution_modes_for_action("build_technical_indicator_ratings"))
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "ratings.json.gz"
            write_export(path, as_of_date=self.date, tickers=["NVDA"], ratings=self.rows)
            date, tickers, rows = read_export(path.read_bytes(), expected_date="2026-10-16")
            self.assertEqual((date, tickers, len(rows)), (self.date, ["NVDA"], 3))

    def test_rejects_tampered_scope_date_and_score(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "ratings.json.gz"
            write_export(path, as_of_date=self.date, tickers=["NVDA"], ratings=self.rows)
            payload = json.loads(gzip.decompress(path.read_bytes()))
            for change in (
                lambda p: p["rows"][0].update(ticker="AAPL"),
                lambda p: p.update(row_count=2),
                lambda p: p["rows"][0].update(as_of_date="2026-10-15"),
                lambda p: p["rows"][0].update(overall_score=0.5),
            ):
                candidate = json.loads(json.dumps(payload))
                change(candidate)
                with self.assertRaises(ValueError):
                    read_export(gzip.compress(json.dumps(candidate).encode()))

    def test_builder_export_mode_never_writes_database(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            export_path = Path(directory) / "ratings.json.gz"
            args = MagicMock(as_of_date="2026-10-16", database_url="postgresql://readonly", tickers=None, include_sectors=None, limit=None, export_file=export_path)
            repository = MagicMock()
            repository.list_active_tickers.return_value = ["NVDA"]
            with patch.object(builder, "parse_args", return_value=args), patch.object(builder, "RatingsRepository", return_value=repository), patch.object(builder, "load_many_ticker_windows", return_value={}), patch.object(builder, "load_excluded_tickers", return_value=set()), patch.object(builder, "build_multi_timeframe_technical_indicator_ratings", return_value=self.rows):
                self.assertEqual(builder.main(), 0)
            self.assertEqual(len(read_export(export_path.read_bytes())[2]), 3)
            repository.replace_technical_indicator_rating_snapshots.assert_not_called()

    def test_external_runner_requires_readonly_tunnel_and_export_switch(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            artifacts_file = Path(directory) / "external-artifacts.json"
            service = MagicMock()
            service.execution_modes_for_action.return_value = ("remote", "github")
            service.build_command.return_value = ["python", "scripts/build_technical_indicator_ratings.py"]
            with patch("scripts.run_external_screener.RunService", return_value=service), patch.dict("os.environ", {"TICKER_SCREENER_DATABASE_URL": ""}):
                with self.assertRaisesRegex(ValueError, "read-only Postgres tunnel"):
                    run_external(action_id="build_technical_indicator_ratings", options={}, artifacts_file=artifacts_file)
            export_path = artifacts_file.with_name("technical-ratings.json.gz")
            export_path.write_bytes(b"export")
            with patch("scripts.run_external_screener.RunService", return_value=service), patch.dict("os.environ", {"TICKER_SCREENER_DATABASE_URL": "postgresql://readonly"}), patch("scripts.run_external_screener.subprocess.run", return_value=MagicMock(returncode=0, stdout="", stderr="")) as process:
                self.assertEqual(run_external(action_id="build_technical_indicator_ratings", options={}, artifacts_file=artifacts_file), {"ratings": str(export_path)})
            self.assertEqual(process.call_args.args[0][-2:], ["--export-file", str(export_path)])

    def test_completion_checks_manifest_and_persists_once(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "ratings.json.gz"
            write_export(path, as_of_date=self.date, tickers=["NVDA"], ratings=self.rows)
            data = path.read_bytes()
            base = "https://r2.example/external-jobs/44/attempt-1/"
            repository = MagicMock(database_url="postgresql://oracle")
            repository.get_job_run.return_value = {
                "id": 44, "status": "running",
                "request_payload": {"action_id": "build_technical_indicator_ratings", "execution_mode": "github", "options": {"as_of_date": "2026-10-16"}},
                "result_payload": {"executor": "github_actions", "github_run_id": 77, "github_attempt": 1},
            }
            service = ExternalJobResultService(repository=repository, history=MagicMock(), artifacts_dir=Path(directory), artifact_base_url="https://r2.example")
            manifest = {
                "job_run_id": 44, "github_run_id": 77, "action_id": "build_technical_indicator_ratings",
                "files": {"ratings": {"url": base + "ratings.json.gz", "sha256": __import__("hashlib").sha256(data).hexdigest(), "size": len(data)}},
            }
            payload = {"github_run_id": 77, "status": "success", "manifest_url": base + "manifest.json"}
            with patch.object(service, "_read_json_url", return_value=manifest), patch.object(service, "_download", return_value=data), patch("src.webapp.services.external_job_result_service.RatingsRepository") as ratings_repository:
                ratings_repository.return_value.replace_technical_indicator_rating_snapshots.return_value = 3
                self.assertEqual(service.complete(job_run_id=44, payload=payload), {"status": "success", "technical_rating_count": 3})
                ratings_repository.return_value.replace_technical_indicator_rating_snapshots.assert_called_once()
                repository.patch_job_run_result.assert_called_once()
                repository.get_job_run.return_value["status"] = "success"
                repository.get_job_run.return_value["result_payload"]["technical_rating_count"] = 3
                self.assertEqual(service.complete(job_run_id=44, payload=payload), {"status": "success", "technical_rating_count": 3})
                ratings_repository.return_value.replace_technical_indicator_rating_snapshots.assert_called_once()
                with self.assertRaisesRegex(ValueError, "manifest URL"):
                    repository.get_job_run.return_value["status"] = "running"
                    repository.get_job_run.return_value["result_payload"].pop("technical_rating_count")
                    service.complete(job_run_id=44, payload={**payload, "manifest_url": "https://r2.example/elsewhere/manifest.json"})

    def test_github_fallback_lowers_priority_without_replacing_dependencies(self) -> None:
        cursor = MagicMock(rowcount=1)
        connection = MagicMock()
        connection.__enter__.return_value = connection
        connection.cursor.return_value.__enter__.return_value = cursor
        repository = HistoryRepository(database_url="")
        with patch.object(repository, "_connect", return_value=connection):
            self.assertTrue(repository.fallback_github_job_run(job_run_id=44, github_run_id=77, message="failed"))
        sql = cursor.execute.call_args.args[0]
        self.assertIn("'{queue_priority}', '10'::jsonb", sql)
        self.assertIn("jsonb_set(request_payload, '{execution_mode}'", sql)
        self.assertNotIn("dependency_job_ids", sql)

    def test_repository_replaces_scope_in_one_transaction_and_does_not_commit_failed_insert(self) -> None:
        connection = MagicMock()
        connection.__enter__.return_value = connection
        cursor = MagicMock()
        connection.cursor.return_value.__enter__.return_value = cursor
        repository = RatingsRepository("")
        with patch.object(repository, "_connect", return_value=connection):
            self.assertEqual(repository.replace_technical_indicator_rating_snapshots(self.date, self.rows, tickers=["NVDA"]), 3)
        self.assertIn("DELETE FROM ticker_technical_indicator_rating_snapshots", cursor.execute.call_args.args[0])
        self.assertIn("INSERT INTO ticker_technical_indicator_rating_snapshots", cursor.executemany.call_args.args[0])
        connection.commit.assert_called_once()
        cursor.executemany.side_effect = RuntimeError("insert failed")
        connection.commit.reset_mock()
        with patch.object(repository, "_connect", return_value=connection):
            with self.assertRaisesRegex(RuntimeError, "insert failed"):
                repository.replace_technical_indicator_rating_snapshots(self.date, self.rows, tickers=["NVDA"])
        connection.commit.assert_not_called()
