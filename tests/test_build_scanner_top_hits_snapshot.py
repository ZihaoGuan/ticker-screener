from __future__ import annotations

import datetime as dt
from pathlib import Path
import tempfile
import unittest
from types import SimpleNamespace
from unittest.mock import patch

import scripts.build_scanner_top_hits_snapshot as module


class BuildScannerTopHitsSnapshotTests(unittest.TestCase):
    def test_incremental_refresh_waits_for_debounce_then_persists_changed_sources(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            refresh_states = [
                {
                    "has_current_snapshot": True,
                    "source_changed": True,
                    "latest_source_at": dt.datetime.now(dt.timezone.utc).isoformat(),
                },
                {
                    "has_current_snapshot": True,
                    "source_changed": True,
                    "latest_source_at": (dt.datetime.now(dt.timezone.utc) - dt.timedelta(seconds=301)).isoformat(),
                },
            ]

            class _Service:
                def __init__(self) -> None:
                    self.persist_calls = 0

                def get_scanner_board(self, *, now: dt.datetime) -> dict[str, object]:
                    return {"target_trading_date": "2026-10-09", "cards": []}

                def get_scanner_top_hits_snapshot_refresh_state(self, *, now: dt.datetime, board_payload: dict[str, object]) -> dict[str, object]:
                    return refresh_states.pop(0)

                def persist_scanner_top_hits_snapshot(self, *, now: dt.datetime, board_payload: dict[str, object]) -> dict[str, object]:
                    self.persist_calls += 1
                    return {"rows": [{"ticker": "NVDA"}]}

            service = _Service()
            config = SimpleNamespace(artifacts_dir=Path(temp_dir), database_url="postgresql://example", market_data_source="database-first")
            preflight = SimpleNamespace(ready=False, active_job_ids=[], pending_job_ids=["other-screener"], target_date="2026-10-09")
            with patch.object(module, "load_webapp_config", return_value=config), patch.object(module, "WatchlistService", return_value=service), patch.object(
                module, "check_required_scheduled_jobs", return_value=preflight
            ):
                self.assertEqual(module.main(["--allow-incremental-refresh"]), 0)
                self.assertEqual(service.persist_calls, 0)
                self.assertEqual(module.main(["--allow-incremental-refresh"]), 0)

            self.assertEqual(service.persist_calls, 1)


if __name__ == "__main__":
    unittest.main()
