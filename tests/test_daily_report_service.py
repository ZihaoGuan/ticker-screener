from __future__ import annotations

from pathlib import Path
import tempfile
import unittest

from src.webapp.services.daily_report_service import DailyReportService


class DailyReportServiceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.service = DailyReportService(artifacts_dir=Path(self.temp_dir.name))

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def test_upsert_lists_and_reads_report(self) -> None:
        saved = self.service.upsert_report(
            {
                "report_date": "2026-09-18",
                "agent_id": "vcp-analyst",
                "agent_name": "VCP Analyst",
                "target_trading_date": "2026-09-17",
                "market_context": "Constructive but selective.",
                "candidate_count": 10,
                "candidates": [
                    {"ticker": "NVDA", "group": "B", "score": 88, "pivot": 190.25},
                    {"ticker": "PLTR", "group": "C", "score": 76},
                ],
                "watch_plan": ["NVDA: wait for a close above 190.25."],
            }
        )

        self.assertEqual(saved["schema_version"], 1)
        self.assertEqual(saved["analyzed_count"], 2)
        summaries = self.service.list_reports()
        self.assertEqual(summaries[0]["report_date"], "2026-09-18")
        self.assertEqual(summaries[0]["agent_id"], "vcp-analyst")
        self.assertEqual(summaries[0]["group_counts"]["B"], 1)
        self.assertEqual(summaries[0]["top_tickers"], ["NVDA"])
        self.assertEqual(self.service.get_report("2026-09-18", "vcp-analyst")["candidates"][0]["ticker"], "NVDA")

    def test_keeps_reports_from_multiple_agents_for_the_same_date(self) -> None:
        for agent_id in ("vcp-analyst", "risk-reviewer"):
            self.service.upsert_report(
                {"report_date": "2026-09-18", "agent_id": agent_id, "candidates": []}
            )

        reports = self.service.list_reports()
        self.assertEqual({item["agent_id"] for item in reports}, {"vcp-analyst", "risk-reviewer"})
        self.assertEqual(self.service.get_report("2026-09-18", "risk-reviewer")["agent_id"], "risk-reviewer")

    def test_upsert_rejects_invalid_or_duplicate_candidates(self) -> None:
        with self.assertRaisesRegex(ValueError, "invalid group"):
            self.service.upsert_report(
                {"report_date": "2026-09-18", "agent_id": "vcp-analyst", "candidates": [{"ticker": "NVDA", "group": "Z"}]}
            )
        with self.assertRaisesRegex(ValueError, "Duplicate"):
            self.service.upsert_report(
                {
                    "report_date": "2026-09-18",
                    "agent_id": "vcp-analyst",
                    "candidates": [
                        {"ticker": "NVDA", "group": "B"},
                        {"ticker": "nvda", "group": "C"},
                    ],
                }
            )

    def test_get_report_rejects_path_like_date(self) -> None:
        with self.assertRaisesRegex(ValueError, "YYYY-MM-DD"):
            self.service.get_report("../secrets")

    def test_rejects_path_like_agent_id(self) -> None:
        with self.assertRaisesRegex(ValueError, "agent_id"):
            self.service.upsert_report(
                {"report_date": "2026-09-18", "agent_id": "../other", "candidates": []}
            )


if __name__ == "__main__":
    unittest.main()
