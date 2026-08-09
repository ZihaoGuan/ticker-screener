from __future__ import annotations

import unittest

import pandas as pd

from scripts.build_technical_ratings import _attach_rs_horizon_ratings, _build_technical_snapshot_input


def _price_frame(start_close: float, daily_return: float, periods: int = 280) -> pd.DataFrame:
    index = pd.date_range(start="2025-01-02", periods=periods, freq="B")
    closes = [start_close * ((1.0 + daily_return) ** index_value) for index_value in range(periods)]
    return pd.DataFrame(
        {
            "Open": [close * 0.998 for close in closes],
            "High": [close * 1.01 for close in closes],
            "Low": [close * 0.99 for close in closes],
            "Close": closes,
            "Volume": [1_000_000 for _ in closes],
        },
        index=index,
    )


class BuildTechnicalRatingsTests(unittest.TestCase):
    def test_attaches_cross_sectional_three_and_six_month_rs_ratings(self) -> None:
        benchmark = _price_frame(100.0, 0.0002)
        as_of_date = benchmark.index[-1].date()
        snapshots = [
            _build_technical_snapshot_input("FAST", _price_frame(20.0, 0.0040), benchmark, as_of_date=as_of_date),
            _build_technical_snapshot_input("MID", _price_frame(20.0, 0.0015), benchmark, as_of_date=as_of_date),
            _build_technical_snapshot_input("SLOW", _price_frame(20.0, -0.0010), benchmark, as_of_date=as_of_date),
        ]

        _attach_rs_horizon_ratings(snapshots)
        by_ticker = {snapshot.ticker: snapshot for snapshot in snapshots}

        self.assertEqual(by_ticker["FAST"].rs_rating_3m, 99.0)
        self.assertEqual(by_ticker["MID"].rs_rating_3m, 50.0)
        self.assertEqual(by_ticker["SLOW"].rs_rating_3m, 1.0)
        self.assertEqual(by_ticker["FAST"].rs_rating_6m, 99.0)
        self.assertEqual(by_ticker["MID"].rs_rating_6m, 50.0)
        self.assertEqual(by_ticker["SLOW"].rs_rating_6m, 1.0)


if __name__ == "__main__":
    unittest.main()
