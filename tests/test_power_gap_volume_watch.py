import datetime as dt
import unittest

from src.power_gap_volume_watch import detect_many, detect_power_gap_volume, enrich_shortlisted_gaps


def bars(count=220):
    start = dt.date(2025, 1, 2)
    result = []
    day = start
    while len(result) < count:
        if day.weekday() < 5:
            result.append({"date": day.isoformat(), "open": 100, "high": 101, "low": 99,
                           "close": 100, "volume": 1_000, "adjustment_basis": "split_adjusted"})
        day += dt.timedelta(days=1)
    return result


def event_at(rows, index, *, gap=10, volume=10_000):
    prev = rows[index - 1]["close"]
    rows[index].update({"open": prev * (1 + gap / 100), "high": prev * 1.12,
                        "low": prev * .99, "close": prev * 1.10, "volume": volume})


def set_week_volume(rows, event_index, volume):
    event_date = dt.date.fromisoformat(rows[event_index]["date"])
    previous_week = event_date - dt.timedelta(days=event_date.weekday() + 7)
    previous_key = previous_week.isocalendar()[:2]
    for row in rows:
        if dt.date.fromisoformat(row["date"]).isocalendar()[:2] == previous_key:
            row["volume"] = volume


class PowerGapVolumeWatchTests(unittest.TestCase):
    def test_gap_threshold_is_inclusive_and_event_context_keeps_event_values(self):
        history = bars()
        event_at(history, 200)
        result = detect_power_gap_volume("abc", history, history[-1]["date"])
        self.assertIn("gap", result["signals"])
        event = result["power_gap_volume_context"]["gap_event"]
        self.assertAlmostEqual(event["gap_pct"], 10)
        self.assertEqual(event["date"], history[200]["date"])

    def test_volume_sma_includes_event_bar_and_suppression_is_one_bar(self):
        history = bars()
        # Solve v / ((19*1000 + v)/20) == 3 exactly.
        threshold_volume = 57_000 / 17
        event_at(history, 200, volume=threshold_volume)
        event_at(history, 201, volume=10_000)
        result = detect_power_gap_volume("abc", history, history[-1]["date"])
        self.assertEqual(result["power_gap_volume_context"]["gap_event"]["date"], history[200]["date"])
        self.assertAlmostEqual(result["power_gap_volume_context"]["gap_event"]["volume_ratio"], 3)

    def test_adjacent_trigger_suppressed_but_later_trigger_replaces(self):
        history = bars(230)
        event_at(history, 200)
        event_at(history, 201)
        event_at(history, 203)
        result = detect_power_gap_volume("abc", history, history[-1]["date"])
        self.assertEqual(result["power_gap_volume_context"]["gap_event"]["date"], history[203]["date"])

    def test_gap_requires_200_sessions_and_strictly_above_sma(self):
        history = bars(199)
        event_at(history, 198)
        self.assertNotIn("gap", detect_power_gap_volume("abc", history, history[-1]["date"])["signals"])
        history = bars(220)
        event_at(history, 200)
        history[200]["close"] = 100
        result = detect_power_gap_volume("abc", history, history[-1]["date"])
        self.assertNotIn("gap", result["signals"])

    def test_latest_day_gap_does_not_break_low_breach_check_and_expiry_is_61_sessions(self):
        history = bars(220)
        event_at(history, 219)
        result = detect_power_gap_volume("abc", history, history[-1]["date"])
        self.assertFalse(result["power_gap_volume_context"]["current_context"]["gap_day_low_breached"])
        at_60 = bars(280)
        event_at(at_60, 219)
        self.assertIn("gap", detect_power_gap_volume("abc", at_60, at_60[-1]["date"])["signals"])
        extended = bars(281)
        event_at(extended, 219)
        result = detect_power_gap_volume("abc", extended, extended[-1]["date"])
        self.assertNotIn("gap", result["signals"])

    def test_zero_range_gap_fails_and_hve_hvy_are_distinct_and_guarded(self):
        history = bars(260)
        # A prior completed week with an unusually large weekly total blocks a
        # larger daily print from becoming HVE; it still becomes the HVY bar.
        set_week_volume(history, 250, 15_000)
        history[240]["volume"] = 50_000  # > preceding completed weekly total: HVE
        history[250]["volume"] = 55_000  # prior weekly total exceeds this: not HVE
        result = detect_power_gap_volume("abc", history, history[-1]["date"])
        context = result["power_gap_volume_context"]
        self.assertEqual(context["volume_events"]["hve"]["date"], history[240]["date"])
        self.assertEqual(context["volume_events"]["hvy"]["date"], history[250]["date"])
        self.assertEqual(context["volume_events"]["hve"]["algorithm"], "daily_running_record_and_prior_250_completed_weekly_totals")
        zero_range = bars()
        event_at(zero_range, 200)
        zero_range[200].update({"high": 110, "low": 110, "close": 110})
        self.assertNotIn("gap", detect_power_gap_volume("abc", zero_range, zero_range[-1]["date"])["signals"])

    def test_hve_weekly_guard_uses_only_completed_prior_weeks_and_preserves_eligible_max(self):
        history = bars(270)
        # A volume event near the end of an unfinished week must not include
        # other future-in-week daily prints in its prior-week guard.
        set_week_volume(history, 260, 15_000)
        history[253]["volume"] = 50_000
        history[260]["volume"] = 55_000
        result = detect_power_gap_volume("abc", history, history[-1]["date"])
        events = result["power_gap_volume_context"]["volume_events"]
        self.assertEqual(events["hve"]["date"], history[253]["date"])
        self.assertEqual(events["hvy"]["date"], history[260]["date"])

    def test_future_bars_in_same_week_do_not_change_asof_hve_guard(self):
        history = bars(270)
        history[253]["volume"] = 50_000
        history[254]["volume"] = 100_000
        cutoff = history[253]["date"]
        prefix = detect_power_gap_volume("abc", history[:254], cutoff)
        full = detect_power_gap_volume("abc", history, cutoff)
        self.assertEqual(prefix["power_gap_volume_context"]["volume_events"]["hve"],
                         full["power_gap_volume_context"]["volume_events"]["hve"])

    def test_hvy_age_uses_calendar_days_inclusive_30_and_excludes_31(self):
        history = bars(252)
        history[-1]["volume"] = 20_000
        asof = dt.date.fromisoformat(history[-1]["date"])
        def append_sessions(rows, cutoff):
            day = dt.date.fromisoformat(rows[-1]["date"]) + dt.timedelta(days=1)
            while day <= cutoff:
                if day.weekday() < 5:
                    rows.append({"date": day.isoformat(), "open": 100, "high": 101, "low": 99,
                                 "close": 100, "volume": 1_000, "adjustment_basis": "split_adjusted"})
                day += dt.timedelta(days=1)
            return rows
        cutoff_30 = asof + dt.timedelta(days=30)
        bars_30 = append_sessions(list(history), cutoff_30)
        expected_30 = [row["date"] for row in bars_30]
        at_30 = detect_power_gap_volume("abc", bars_30, cutoff_30, expected_30)
        self.assertIn("hvy", at_30["signals"])
        cutoff_31 = asof + dt.timedelta(days=31)
        bars_31 = append_sessions(list(history), cutoff_31)
        expected_31 = [row["date"] for row in bars_31]
        at_31 = detect_power_gap_volume("abc", bars_31, cutoff_31, expected_31)
        self.assertNotIn("hvy", at_31["signals"])
        self.assertTrue(at_31["power_gap_volume_context"]["coverage"]["hvy_complete"])

    def test_tied_hvy_maximum_uses_latest_occurrence(self):
        history = bars(260)
        history[250]["volume"] = history[255]["volume"] = 12_000
        result = detect_power_gap_volume("abc", history, history[-1]["date"])
        self.assertEqual(result["power_gap_volume_context"]["volume_events"]["hvy"]["date"], history[255]["date"])

    def test_asof_excludes_future_bars_and_bad_ticker_does_not_stop_batch(self):
        history = bars(220)
        event_at(history, 200)
        future = {**history[-1], "date": (dt.date.fromisoformat(history[-1]["date"]) + dt.timedelta(days=1)).isoformat()}
        history.append(future)
        result = detect_power_gap_volume("abc", history, history[-2]["date"])
        self.assertEqual(result["coverage"]["history_end"], history[-2]["date"])
        batch = detect_many({"bad": [{"date": "not-a-date"}], "good": history}, history[-2]["date"])
        self.assertTrue(batch[0]["coverage"]["issues"])
        self.assertEqual(batch[1]["ticker"], "GOOD")

    def test_duplicate_dates_mixed_basis_and_missing_expected_session_are_issues(self):
        history = bars()
        duplicate = history + [dict(history[0])]
        self.assertTrue(detect_power_gap_volume("abc", duplicate, history[-1]["date"])["coverage"]["issues"])
        mixed = bars()
        mixed[-1]["adjustment_basis"] = "raw"
        self.assertTrue(detect_power_gap_volume("abc", mixed, mixed[-1]["date"])["coverage"]["issues"])
        expected = [row["date"] for row in history]
        del history[20]
        coverage = detect_power_gap_volume("abc", history, history[-1]["date"], expected)["coverage"]
        self.assertIn("gap", coverage["component_issues"])

    def test_old_missing_session_disables_hve_but_preserves_recent_gap_and_hvy(self):
        history = bars(300)
        event_at(history, 285, volume=50_000)
        expected = [row["date"] for row in history]
        del history[20]
        result = detect_power_gap_volume("abc", history, history[-1]["date"], expected)
        context = result["power_gap_volume_context"]
        self.assertIn("gap", result["signals"])
        self.assertIn("hvy", result["signals"])
        self.assertIsNone(context["volume_events"]["hve"])
        self.assertIn("hve", context["coverage"]["component_issues"])

    def test_earnings_enrichment_shortlists_distinct_gap_pairs_only(self):
        class Config:
            earnings_enrichment_provider = "yfinance"
        first = {"ticker": "ABC", "power_gap_volume_context": {"active_signals": ["gap"], "gap_event": {"date": "2025-01-02"}}}
        duplicate = {"ticker": "ABC", "power_gap_volume_context": {"active_signals": ["gap"], "gap_event": {"date": "2025-01-02"}}}
        volume_only = {"ticker": "XYZ", "power_gap_volume_context": {"gap_event": None}}
        # Provider implementation is exercised in its own tests; unknown/no-network
        # behavior remains represented by its ordinary context result.
        enriched = enrich_shortlisted_gaps([first, duplicate, volume_only], Config(), "2025-01-03", provider="unsupported")
        self.assertEqual(len(enriched), 3)
        self.assertIn("earnings_context", enriched[0]["power_gap_volume_context"])
        self.assertNotIn("earnings_context", enriched[2]["power_gap_volume_context"])


if __name__ == "__main__":
    unittest.main()
