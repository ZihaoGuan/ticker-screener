from __future__ import annotations

import datetime as dt
import tempfile
import unittest

from src.webapp.services.screener_history_service import ScreenerHistoryService


class _FakeHistoryRepository:
    def __init__(self) -> None:
        self.screen_run_payload: dict[str, object] | None = None
        self.hit_rows: list[dict[str, object]] | None = None
        self.deleted: tuple[int, str] | None = None
        self.calendar_rows: list[dict[str, object]] = []

    def is_configured(self) -> bool:
        return True

    def list_screen_runs(self, **_: object):
        return []

    def get_screen_run(self, *_: object, **__: object):
        return None

    def soft_delete_screen_run(self, run_id: int, *, reason: str) -> bool:
        self.deleted = (run_id, reason)
        return True

    def list_signal_cache_summary(self, **_: object):
        return []

    def list_signal_cache_calendar(self, **_: object):
        return list(self.calendar_rows)

    def upsert_screen_run(self, **kwargs: object) -> int:
        self.screen_run_payload = dict(kwargs)
        return 17

    def replace_screen_run_hits(self, screen_run_id: int | None, rows: list[dict[str, object]]) -> None:
        self.hit_rows = rows


class ScreenerHistoryServiceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp_dir.cleanup)
        self.repository = _FakeHistoryRepository()
        self.service = ScreenerHistoryService(repository=self.repository)  # type: ignore[arg-type]

    def test_persist_screen_run_extracts_hits_and_failures(self) -> None:
        summary_payload = {
            "date_label": "2026-05-31",
            "as_of_date": "2026-05-31",
            "passed_tickers": 1,
            "failed_tickers": 1,
            "total_tickers": 2,
            "raw_results_file": "/tmp/raw.json",
            "watchlist_file": "/tmp/watch.json",
        }
        raw_payload = {
            "hits": [{"ticker": "AAPL", "reasons": ["passed"], "score": 9.1}],
            "failed_tickers": [{"ticker": "MSFT", "error": "missing history"}],
        }

        screen_run_id = self.service.persist_screen_run(
            strategy_id="rs",
            options={"market_data_source": "database-first", "tickers": ["AAPL", "MSFT"]},
            summary_payload=summary_payload,
            raw_payload=raw_payload,
            job_run_id=99,
        )

        self.assertEqual(screen_run_id, 17)
        self.assertIsNotNone(self.repository.screen_run_payload)
        assert self.repository.screen_run_payload is not None
        self.assertEqual(self.repository.screen_run_payload["strategy_id"], "rs")
        self.assertEqual(self.repository.screen_run_payload["run_date"], dt.date(2026, 5, 31))
        self.assertEqual(self.repository.screen_run_payload["market_data_mode"], "database-first")
        self.assertEqual(len(self.repository.hit_rows or []), 2)
        self.assertEqual((self.repository.hit_rows or [])[0]["ticker"], "AAPL")
        self.assertTrue((self.repository.hit_rows or [])[0]["passed"])
        self.assertEqual((self.repository.hit_rows or [])[1]["ticker"], "MSFT")
        self.assertFalse((self.repository.hit_rows or [])[1]["passed"])

    def test_persist_screen_run_accepts_failure_list_in_summary_payload(self) -> None:
        summary_payload = {
            "date_label": "2026-03-23",
            "as_of_date": "2026-03-23",
            "passed_tickers": 48,
            "failed_tickers": [{"ticker": "VIDA", "error": "400 yahoo"}],
            "total_tickers": 3611,
            "raw_results_file": "/tmp/raw.json",
            "watchlist_file": "/tmp/watch.json",
        }
        raw_payload = {
            "hits": [{"ticker": "VZLA", "reasons": ["passed"]}],
            "failed_tickers": [{"ticker": "VIDA", "error": "400 yahoo"}],
        }

        screen_run_id = self.service.persist_screen_run(
            strategy_id="rs",
            options={"market_data_source": "internet", "tickers": []},
            summary_payload=summary_payload,
            raw_payload=raw_payload,
            job_run_id=123,
        )

        self.assertEqual(screen_run_id, 17)
        assert self.repository.screen_run_payload is not None
        self.assertEqual(self.repository.screen_run_payload["failure_count"], 1)
        self.assertEqual((self.repository.screen_run_payload["result_summary_json"] or {})["failed_tickers"], 1)

    def test_soft_delete_uses_default_reason(self) -> None:
        deleted = self.service.soft_delete(42, reason="")

        self.assertTrue(deleted)
        self.assertEqual(self.repository.deleted, (42, "Deleted from webapp"))

    def test_cache_calendar_groups_by_day_and_status(self) -> None:
        self.repository.calendar_rows = [
            {
                "id": 11,
                "strategy_id": "rs",
                "run_date": dt.date(2026, 6, 1),
                "market_data_mode": "database-first",
                "source_kind": "exchange-universe",
                "hit_count": 3,
                "failure_count": 0,
                "deleted_at": None,
                "deleted_reason": None,
                "created_at": "2026-06-01T10:00:00+00:00",
            }
        ]

        days = self.service.list_signal_cache_calendar(
            strategy_ids=["rs", "vcp"],
            start_date=dt.date(2026, 6, 1),
            end_date=dt.date(2026, 6, 2),
            include_deleted=False,
        )

        self.assertEqual(days[0]["date"], "2026-06-01")
        self.assertEqual(days[0]["status"], "partial")
        self.assertEqual(days[0]["cached_strategy_count"], 1)
        self.assertEqual(days[1]["date"], "2026-06-02")
        self.assertEqual(days[1]["status"], "none")


if __name__ == "__main__":
    unittest.main()
