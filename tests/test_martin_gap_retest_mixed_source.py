from datetime import date, timedelta
import unittest

import pandas as pd

from src.webapp.services.martin_gap_retest_service import MartinGapRetestService


class ProvenanceRepository:
    def __init__(self, rows):
        self.rows = rows

    def daily_bar_provenance(self, _ticker, _start, _end):
        return self.rows

    def daily_bar_replay_provenance(self, _ticker, _start, _end):
        return self.rows


class MixedSourceCertificationTests(unittest.TestCase):
    start = date(2026, 1, 5)
    days = [date(2026, 1, 5) + timedelta(days=offset) for offset in range(3)]

    def rows(self, sources, *, latest_split=None, unknown_split_rows=0, unknown_source_rows=0):
        return [{"source": source, "split_factor": 1.0, "trade_date": day, "rows": 1,
                 "first_date": day, "last_date": day, "latest_split_date": latest_split,
                 "history_first_date": self.start - timedelta(days=365),
                 "unknown_split_rows": unknown_split_rows, "unknown_source_rows": unknown_source_rows}
                for day, source in zip(self.days, sources)]

    def frame(self, closes=(100, 101, 102)):
        return pd.DataFrame({"Open": closes, "High": [value + 1 for value in closes],
                             "Low": [value - 1 for value in closes], "Close": closes,
                             "Volume": [1000, 1100, 1200]}, index=pd.to_datetime(self.days))

    def certify(self, sources, **kwargs):
        rows = self.rows(sources, **kwargs)
        frame = self.frame()
        service = MartinGapRetestService(ProvenanceRepository(rows), bar_loader=lambda *_a, **_k: {})
        certified = service._certify_frame("XYZ", frame, self.start, self.days[-1],
                                           historical_replay=True, mixed_source_replay=True)
        return certified, frame

    def test_mixed_sources_are_ranked_and_reported_when_actions_are_explicitly_complete(self):
        certified, frame = self.certify(["tradingview", "massive", "yfinance"])
        self.assertTrue(certified)
        self.assertEqual("tradingview+massive+yfinance", frame.attrs["provenance_source"])
        self.assertEqual(["tradingview", "massive", "yfinance"], frame.attrs["data_provenance"]["sources"])
        self.assertIn("recorded history start", frame.attrs["data_provenance"]["basis_assumption"])

    def test_raw_sources_before_latest_split_event_are_rejected(self):
        certified, _ = self.certify(["massive", "tradingview", "tradingview"], latest_split=self.days[1])
        self.assertFalse(certified)

    def test_raw_sources_after_latest_split_are_accepted(self):
        certified, frame = self.certify(["tradingview", "massive", "yfinance"], latest_split=self.days[1])
        self.assertTrue(certified)
        self.assertEqual(self.days[1].isoformat(), frame.attrs["data_provenance"]["latest_split_date"])

    def test_incomplete_actions_unknown_sources_and_transition_scale_jumps_fail_closed(self):
        self.assertFalse(self.certify(["tradingview", "massive", "massive"], unknown_split_rows=1)[0])
        self.assertFalse(self.certify(["tradingview", "massive", "massive"], unknown_source_rows=1)[0])
        rows = self.rows(["tradingview", "massive", "massive"])
        frame = self.frame((100, 50, 51))
        service = MartinGapRetestService(ProvenanceRepository(rows), bar_loader=lambda *_a, **_k: {})
        self.assertFalse(service._certify_frame("XYZ", frame, self.start, self.days[-1],
                                               historical_replay=True, mixed_source_replay=True))

    def test_unknown_provider_is_rejected(self):
        self.assertFalse(self.certify(["tradingview", "broker_csv", "massive"])[0])


if __name__ == "__main__":
    unittest.main()
