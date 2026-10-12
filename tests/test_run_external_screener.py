from __future__ import annotations

import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import MagicMock, patch

from scripts.run_external_screener import _artifact_paths, run


class RunExternalScreenerTests(unittest.TestCase):
    def test_artifact_paths_requires_standard_screening_outputs(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            raw = root / "raw.json"
            watchlist = root / "watchlist.json"
            summary = root / "summary.json"
            raw.write_text("{}", encoding="utf-8")
            watchlist.write_text("{}", encoding="utf-8")
            summary.write_text(
                json.dumps({"raw_results_file": str(raw), "watchlist_file": str(watchlist)}),
                encoding="utf-8",
            )

            self.assertEqual(
                _artifact_paths(summary),
                {"raw": str(raw), "watchlist": str(watchlist), "summary": str(summary)},
            )

    def test_artifact_paths_rejects_missing_watchlist(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            summary = root / "summary.json"
            summary.write_text(json.dumps({"raw_results_file": str(root / "raw.json")}), encoding="utf-8")

            with self.assertRaisesRegex(ValueError, "required artifacts"):
                _artifact_paths(summary)

    def test_run_rejects_database_first_without_database_connection(self) -> None:
        with patch.dict(os.environ, {"TICKER_SCREENER_DATABASE_URL": ""}, clear=False):
            with patch("scripts.run_external_screener.subprocess.run") as run_process:
                with self.assertRaisesRegex(ValueError, "requires TICKER_SCREENER_DATABASE_URL"):
                    run(
                        action_id="vcp_spec",
                        options={"market_data_source": "database-first"},
                        artifacts_file=Path("/tmp/external-artifacts.json"),
                    )
        run_process.assert_not_called()

    def test_run_rejects_empty_database_universe(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            raw = root / "raw.json"
            watchlist = root / "watchlist.json"
            summary = root / "run_summary.json"
            raw.write_text("{}", encoding="utf-8")
            watchlist.write_text("[]", encoding="utf-8")
            summary.write_text(
                json.dumps(
                    {
                        "source": "database-universe",
                        "total_tickers": 0,
                        "raw_results_file": str(raw),
                        "watchlist_file": str(watchlist),
                    }
                ),
                encoding="utf-8",
            )
            completed = MagicMock(returncode=0, stdout=f"Wrote run summary to {summary}\n", stderr="")
            service = MagicMock()
            service.execution_modes_for_action.return_value = ("github",)
            service.build_command.return_value = ["python", "fake_screener.py"]

            with patch("scripts.run_external_screener.RunService", return_value=service):
                with patch("scripts.run_external_screener.subprocess.run", return_value=completed):
                    with self.assertRaisesRegex(ValueError, "empty database universe"):
                        run(
                            action_id="vcp_spec",
                            options={},
                            artifacts_file=root / "external-artifacts.json",
                        )

    def test_run_passes_database_first_setting_to_screener_process(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            raw = root / "raw.json"
            watchlist = root / "watchlist.json"
            summary = root / "run_summary.json"
            raw.write_text("{}", encoding="utf-8")
            watchlist.write_text("[]", encoding="utf-8")
            summary.write_text(
                json.dumps(
                    {
                        "source": "database-universe",
                        "total_tickers": 1,
                        "raw_results_file": str(raw),
                        "watchlist_file": str(watchlist),
                    }
                ),
                encoding="utf-8",
            )
            completed = MagicMock(returncode=0, stdout=f"Wrote run summary to {summary}\n", stderr="")
            service = MagicMock()
            service.execution_modes_for_action.return_value = ("github",)
            service.build_command.return_value = ["python", "fake_screener.py"]

            with patch.dict(os.environ, {"TICKER_SCREENER_DATABASE_URL": "postgresql://readonly"}, clear=False):
                with patch("scripts.run_external_screener.RunService", return_value=service):
                    with patch("scripts.run_external_screener.subprocess.run", return_value=completed) as run_process:
                        run(
                            action_id="vcp_spec",
                            options={"market_data_source": "database-first"},
                            artifacts_file=root / "external-artifacts.json",
                        )

            environment = run_process.call_args.kwargs["env"]
            self.assertEqual(environment["TICKER_SCREENER_MARKET_DATA_SOURCE"], "database-first")
            self.assertEqual(environment["TICKER_SCREENER_DATABASE_URL"], "postgresql://readonly")
