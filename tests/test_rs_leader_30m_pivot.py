import importlib.util
import json
import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path

from src.webapp.services.rs_leader_30m_pivot import (evaluate_pivot_sequence, refine_pivot_ambiguity,
    load_frozen_30m_directory, support_decline_candidate)


def bar(opening, high, low, close):
    return {"open": opening, "high": high, "low": low, "close": close, "volume": 1000}


class RSPivotTests(unittest.TestCase):
    @unittest.skipUnless(importlib.util.find_spec("pandas"), "pandas is not installed")
    def test_daily_support_decline_thresholds_are_explicit_and_inferred(self):
        import pandas as pd
        dates = pd.bdate_range("2025-12-01", periods=60)
        frame = pd.DataFrame({"Open": [110] * 59 + [100], "High": [110] * 59 + [106],
                              "Low": [100] * 60, "Close": [108] * 59 + [102], "Volume": [1000] * 60},
                             index=dates)
        result = support_decline_candidate(frame, dates[-1].date(), saved_support=101)
        self.assertEqual(result["status"], "eligible")
        self.assertTrue(result["thresholds_inferred"])
        self.assertEqual(support_decline_candidate(frame, dates[-1].date())["status"], "rejected")
        future = pd.DataFrame({"Open": [200], "High": [300], "Low": [190], "Close": [250], "Volume": [1000]},
                              index=[dates[-1] + pd.Timedelta(days=1)])
        with_future = pd.concat([frame, future])
        self.assertEqual(support_decline_candidate(with_future, dates[-1].date(), saved_support=101), result)

    def test_pivot_requires_later_bar_after_decline_and_stop_first_cancels(self):
        bars = [bar(10.5, 10.6, 10.2, 10.3), bar(10.2, 10.5, 10.1, 10.4),
                bar(10.3, 10.35, 9.9, 10.1)]
        self.assertEqual(evaluate_pivot_sequence(bars)["status"], "cancelled")
        bars = [bar(10.5, 10.6, 10.2, 10.3), bar(10.2, 10.5, 10.1, 10.4),
                bar(10.3, 10.6, 10.2, 10.5)]
        self.assertEqual(evaluate_pivot_sequence(bars)["status"], "filled")

    def test_same_bar_entry_and_stop_is_not_counted_as_fill(self):
        result = evaluate_pivot_sequence([bar(10.5, 10.6, 10.2, 10.3), bar(10.2, 10.5, 10.1, 10.4),
                                          bar(10.15, 10.6, 9.7, 10.2)])
        self.assertEqual(result["status"], "ambiguous")

    def test_green_bar_without_prior_intraday_decline_is_not_a_pivot(self):
        result = evaluate_pivot_sequence([bar(10, 10.2, 9.9, 10.1), bar(10.1, 10.5, 10, 10.4)])
        self.assertEqual(result["status"], "expired")

    def test_one_minute_bars_resolve_30m_ambiguity_when_order_is_known(self):
        bars = [bar(10.5, 10.6, 10.2, 10.3), bar(10.2, 10.5, 10.1, 10.4),
                {**bar(10.15, 10.6, 9.7, 10.2), "time": 4600}]
        ambiguous = {"status": "ambiguous", "pivot_index": 1, "bar_index": 2,
                     "entry": 10.5, "stop": 10.1}
        fine = [{**bar(10.3, 10.4, 10.2, 10.35), "time": 4600 + 60 * i} for i in range(30)]
        fine[1] = {**bar(10.4, 10.55, 10.3, 10.5), "time": 4660}
        self.assertEqual(refine_pivot_ambiguity(bars, fine, ambiguous)["status"], "filled")
        fine[0] = {**bar(10.05, 10.2, 10, 10.1), "time": 4600}
        self.assertEqual(refine_pivot_ambiguity(bars, fine, ambiguous)["status"], "cancelled")
        fine[0] = {**bar(10.2, 10.6, 10, 10.2), "time": 4600}
        self.assertEqual(refine_pivot_ambiguity(bars, fine, ambiguous)["status"], "ambiguous")

    def test_invalid_intraday_data_blocks_entry(self):
        self.assertEqual(evaluate_pivot_sequence([bar(10, 9, 9.5, 10.2)])["status"], "blocked_data")

    def test_tampered_30m_manifest_is_rejected_before_loading_bars(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "audit.json"
            path.write_text(json.dumps({"schema": "rs_leader_30m_pivot_audit_v1", "fingerprint": "bad"}))
            with self.assertRaisesRegex(ValueError, "fingerprint_mismatch"):
                load_frozen_30m_directory(directory, "2026-01-02")

    @unittest.skipUnless(shutil.which("node"), "node is not installed")
    def test_intraday_fetch_cli_accepts_30m_timeframe(self):
        script = Path(__file__).resolve().parents[1] / "scripts" / "fetch_tradingview_intraday.mjs"
        result = subprocess.run(["node", str(script), "/nonexistent", "NASDAQ:AAPL", "2026-01-02", "30"],
                                capture_output=True, text=True, check=False)
        self.assertNotEqual(result.returncode, 64)
        self.assertNotIn("[30|15|1]", result.stderr)


if __name__ == "__main__":
    unittest.main()
