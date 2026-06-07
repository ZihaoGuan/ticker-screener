from __future__ import annotations

import unittest

import pandas as pd

from src.rsi_ma_bb_screen import find_recent_rsi_ma_bb_hit
from src.universe import UniverseTicker


def _bullish_bb_frame() -> pd.DataFrame:
    index = pd.date_range(start="2026-01-02", periods=70, freq="B")
    close = [
        100.0, 101.18645633111642, 99.65021949284596, 98.12601276333173, 98.57979839379682, 96.31358482951538,
        97.64522783241178, 98.91728356998371, 96.575964971973, 97.44293968873356, 97.40662700772981,
        98.03384553543421, 100.25250877947488, 102.63629642888799, 102.13199976240072, 103.80271702122452,
        105.18423919288946, 107.49301425187176, 105.08697234609087, 107.22644495637903, 105.78785343381217,
        103.88491025243503, 104.32219962464002, 105.96649472421404, 108.19986378067281, 107.4712574956538,
        109.0597047499595, 106.66365807236943, 108.95618855154042, 107.87348782085422, 108.04102948333464,
        106.11271269009772, 103.78185146304682, 101.27030057404396, 101.72108775982501, 101.97670403108299,
        103.50643216949945, 105.2121312253682, 104.77939666708434, 106.46610810557169, 104.91976605961646,
        106.77421373254165, 105.4769310038911, 107.62569213268291, 106.99723355778107, 106.58754834567277,
        106.62914869506886, 104.37373284640489, 102.1981871854706, 102.37287537079939, 103.39333652400711,
        101.06708036681044, 100.55083585617633, 99.8506761990321, 101.206448877259, 103.60863369270922,
        101.51157474734936, 101.68001453224835, 99.65094047905187, 101.01964140673503, 103.18072664506045,
        104.16310246717894, 104.52080997489469, 102.17343001746792, 100.10474492526696, 98.75312111120893,
        96.61180143699907, 94.73072926591884, 96.97729709623063, 96.16691266625608,
    ]
    return pd.DataFrame(
        {
            "Open": [value - 0.2 for value in close],
            "High": [value + 0.8 for value in close],
            "Low": [value - 0.8 for value in close],
            "Close": close,
            "Volume": [1_000_000.0 for _ in close],
        },
        index=index,
    )


def _bearish_bb_frame() -> pd.DataFrame:
    index = pd.date_range(start="2026-01-02", periods=70, freq="B")
    close = [
        100.0, 99.1691638241658, 97.47340969378831, 98.27808205898758, 96.19026349232529, 96.41967351385873,
        95.79811809842165, 93.63811272229519, 93.7252913882423, 91.46276968045223, 91.18099809876415,
        89.08027521663725, 87.08384028335658, 86.75643622906915, 88.44069685242934, 86.60970665817757,
        85.27590148121264, 85.9630675932406, 88.25161230552563, 88.68712704861312, 88.22052942186703,
        90.65180494983163, 88.43471835292041, 90.27706064816381, 89.27510707982219, 87.54638249660938,
        85.68534368700122, 84.7777528075109, 86.40838460311105, 84.86201650273074, 85.32001732104307,
        86.06458466567399, 85.47657237930265, 85.76529470785044, 83.62923958271706, 81.47724543254823,
        80.05703899664486, 81.0090388625538, 80.69700039090081, 79.81773624278478, 80.29554556032296,
        80.11146744217685, 79.16030242649526, 80.68219983410772, 81.72717200275558, 80.49765455636634,
        80.9197731076597, 81.09575562671695, 83.0214431045841, 84.21866955178018, 83.20835837623112,
        85.65923261369403, 83.79956150496884, 83.44017561389497, 84.77588026172121, 83.08580293502374,
        83.08061843740276, 80.82665472263994, 81.71773400531193, 83.09058833637599, 83.5057180377629,
        85.43310709691734, 84.55184466115783, 85.57832149252613, 86.10017087805122, 86.54964689946368,
        86.38067355597074, 88.13051245853345, 90.40391793407314, 90.32440962117136,
    ]
    return pd.DataFrame(
        {
            "Open": [value + 0.2 for value in close],
            "High": [value + 0.8 for value in close],
            "Low": [value - 0.8 for value in close],
            "Close": close,
            "Volume": [1_000_000.0 for _ in close],
        },
        index=index,
    )


class RsiMaBbScreenTests(unittest.TestCase):
    def test_find_recent_rsi_ma_bb_bullish_hit_returns_recent_signal(self) -> None:
        hit = find_recent_rsi_ma_bb_hit(
            _bullish_bb_frame(),
            ticker=UniverseTicker(symbol="AAPL", sector="Technology", industry="Software", exchange="NASDAQ"),
            direction="bullish",
        )

        self.assertIsNotNone(hit)
        assert hit is not None
        self.assertEqual(hit.direction, "bullish")
        self.assertLessEqual(hit.signal_age_bars, 1)
        self.assertIn("BB", hit.signal_sources)

    def test_find_recent_rsi_ma_bb_bearish_hit_returns_recent_signal(self) -> None:
        hit = find_recent_rsi_ma_bb_hit(
            _bearish_bb_frame(),
            ticker=UniverseTicker(symbol="TSLA"),
            direction="bearish",
        )

        self.assertIsNotNone(hit)
        assert hit is not None
        self.assertEqual(hit.direction, "bearish")
        self.assertLessEqual(hit.signal_age_bars, 1)
        self.assertIn("BB", hit.signal_sources)

    def test_find_recent_rsi_ma_bb_hit_returns_none_without_recent_signal(self) -> None:
        frame = _bullish_bb_frame().iloc[:-20]
        hit = find_recent_rsi_ma_bb_hit(
            frame,
            ticker=UniverseTicker(symbol="MSFT"),
            direction="bullish",
        )

        self.assertIsNone(hit)


if __name__ == "__main__":
    unittest.main()
