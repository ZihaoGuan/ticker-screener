from __future__ import annotations

import argparse
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from scripts._screen_run_persistence import persist_screen_run_artifacts_if_configured


class _FakeHistoryService:
    def __init__(self, *args, **kwargs) -> None:
        self.calls: list[dict[str, object]] = []

    def is_configured(self) -> bool:
        return True

    def persist_screen_run(self, **kwargs):
        self.calls.append(dict(kwargs))
        return 77


class ScreenRunPersistenceHelperTests(unittest.TestCase):
    def test_persist_screen_run_artifacts_if_configured_loads_summary_and_raw(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            raw_path = root / "raw.json"
            summary_path = root / "summary.json"
            raw_path.write_text(
                json.dumps(
                    {
                        "hits": [{"ticker": "NVDA", "reasons": ["leader"]}],
                        "failed_tickers": [],
                    }
                ),
                encoding="utf-8",
            )
            summary_path.write_text(
                json.dumps(
                    {
                        "strategy_id": "rs",
                        "date_label": "2026-06-19",
                        "as_of_date": "2026-06-19",
                        "source": "manual-tickers",
                        "reference_date": "2026-06-19",
                        "raw_results_file": str(raw_path),
                        "watchlist_file": str(root / "watchlist.json"),
                    }
                ),
                encoding="utf-8",
            )
            args = argparse.Namespace(
                limit=25,
                tickers=["NVDA", "AAPL"],
                date_label="2026-06-19",
                as_of_date="2026-06-19",
                include_sectors=["Technology"],
                exclude_sectors=[],
                include_industries=[],
                exclude_industries=[],
                include_themes=[],
                exclude_themes=[],
                filter_precedence="exclude",
                pass_mode=None,
            )
            fake_service = _FakeHistoryService()

            with patch(
                "scripts._screen_run_persistence.load_webapp_config",
                return_value=type("Cfg", (), {"database_url": "postgres://example", "artifacts_dir": root})(),
            ), patch(
                "scripts._screen_run_persistence.ScreenerHistoryService",
                return_value=fake_service,
            ):
                run_id = persist_screen_run_artifacts_if_configured(
                    args=args,
                    summary_path=summary_path,
                )

        self.assertEqual(run_id, 77)
        self.assertEqual(len(fake_service.calls), 1)
        payload = fake_service.calls[0]
        self.assertEqual(payload["strategy_id"], "rs")
        self.assertEqual((payload["summary_payload"] or {})["source"], "manual-tickers")
        self.assertEqual((payload["raw_payload"] or {})["hits"][0]["ticker"], "NVDA")
        self.assertEqual((payload["options"] or {})["limit"], 25)
        self.assertEqual((payload["options"] or {})["market_data_source"], "internet")
        self.assertEqual((payload["options"] or {})["reference_date"], "2026-06-19")


if __name__ == "__main__":
    unittest.main()
