from __future__ import annotations

import unittest
import json
from datetime import date, timedelta

import pandas as pd

from src.martin_daily_gap_retest import evaluate_gap_retest, rank_confirmations, resolve_gap_next_open, size_gap_order


def bars(count: int = 330) -> pd.DataFrame:
    days = [date(2025, 1, 2) + timedelta(days=i) for i in range(count)]
    close = [80 + i * .1 for i in range(count)]
    frame = pd.DataFrame({"Open": [x - .2 for x in close], "High": [x + .5 for x in close],
                          "Low": [x - .5 for x in close], "Close": close, "Volume": [1_000_000] * count},
                         index=pd.to_datetime(days))
    # One first lower-low retest and bullish daily reclaim.
    i = 300
    frame.iloc[i, frame.columns.get_loc("Open")] = frame.iloc[i]["Close"] - .7
    frame.iloc[i, frame.columns.get_loc("Low")] = frame.iloc[i]["Open"] - .1
    frame.iloc[i, frame.columns.get_loc("High")] = frame.iloc[i]["Close"] + .2
    return frame


class GapRetestEvaluatorTests(unittest.TestCase):
    def test_exact_five_percent_risk_is_eligible(self):
        count = 301
        close = [14 + i * .01 for i in range(count)]
        frame = pd.DataFrame({"Open": [x - .02 for x in close], "High": [x + .05 for x in close],
                              "Low": [x - .05 for x in close], "Close": close, "Volume": [2_000_000] * count},
                             index=pd.bdate_range("2024-01-02", periods=count))
        frame.iloc[270, frame.columns.get_loc("Low")] = 14
        frame.iloc[-1] = [16.8, 17.2, 16.15, 17, 2_000_000]
        event = {"ticker": "TEST", "original_gap_date": frame.index[270].date().isoformat(), "gap_day_low": 14}
        decision = evaluate_gap_retest(frame, event)
        self.assertEqual("confirmed", decision["status"])
        self.assertEqual(5, decision["reference_risk_pct"])
        frame.iloc[-1, frame.columns.get_loc("Low")] = 16.149999
        self.assertEqual("waiting_risk", evaluate_gap_retest(frame, event)["status"])

    def test_waits_for_first_episode_and_reconstructs_reclaim(self):
        frame = bars().iloc[:301]
        event = {"ticker": "TEST", "original_gap_date": frame.index[270].date().isoformat()}
        decision = evaluate_gap_retest(frame, event)
        self.assertEqual("confirmed", decision["status"])
        self.assertEqual(frame.index[300].date().isoformat(), decision["episode_start_date"])
        self.assertEqual(9, decision["selected_ema_period"])
        self.assertLess(decision["stop"], decision["reference_entry"])
        self.assertAlmostEqual(decision["max_entry"], decision["reference_entry"])

    def test_day_zero_is_not_entry_eligible_and_future_rows_are_excluded_by_asof_frame(self):
        frame = bars()
        event = {"ticker": "TEST", "original_gap_date": frame.index[-1].date().isoformat()}
        self.assertEqual("awaiting_first_retest", evaluate_gap_retest(frame, event)["reasons"][0])

    def test_next_open_slippage_cap_and_stop(self):
        order = {"stop": 97, "max_entry": 100, "shares": 100}
        self.assertEqual({"status": "filled", "fill_price": 99.099, "shares": 100},
                         resolve_gap_next_open(order, {"open": 99}))
        self.assertEqual("opened_at_or_below_stop", resolve_gap_next_open(order, {"open": 96})["reason"])
        self.assertEqual("opened_above_max_entry", resolve_gap_next_open(order, {"open": 100})["reason"])

    def test_sizing_applies_fixed_risk_allocation_cash_and_adv_caps(self):
        result = size_gap_order({"stop": 97, "max_entry": 100, "adv20_dollars": 50_000_000},
                                available_cash=100_000, available_risk=2_500, slots_available=8)
        self.assertEqual(166, result["shares"])
        self.assertEqual(16_600, result["reservation_cash"])
        self.assertEqual(498, result["planned_stop_risk"])
        json.dumps(result)

    def test_relative_return_zero_ranks_ahead_of_negative_return(self):
        ranked = rank_confirmations([{"ticker": "NEG", "relative_return20_pct": -1, "adr14_pct": 9, "adv20_dollars": 1},
                                     {"ticker": "ZERO", "relative_return20_pct": 0, "adr14_pct": 1, "adv20_dollars": 1}])
        self.assertEqual(["ZERO", "NEG"], [item["ticker"] for item in ranked])

    def test_bad_ohlcv_is_blocked_instead_of_clamped(self):
        frame = bars()
        frame.iloc[20, frame.columns.get_loc("Low")] = frame.iloc[20]["High"] + 1
        event = {"ticker": "TEST", "original_gap_date": frame.index[270].date().isoformat()}
        self.assertEqual("invalid_ohlcv", evaluate_gap_retest(frame, event)["reasons"][0])


if __name__ == "__main__":
    unittest.main()
