from __future__ import annotations

from decimal import Decimal
import unittest

from src.market_data_access import _coerce_frame


class MarketDataAccessTests(unittest.TestCase):
    def test_coerce_frame_normalizes_decimal_ohlcv_to_numeric(self) -> None:
        frame = _coerce_frame(
            [
                ("2026-06-16", Decimal("100.1"), Decimal("101.2"), Decimal("99.8"), Decimal("100.9"), Decimal("100.9"), Decimal("1234567")),
                ("2026-06-17", Decimal("101.0"), Decimal("102.0"), Decimal("100.2"), Decimal("101.7"), Decimal("101.7"), Decimal("2345678")),
            ]
        )

        assert frame is not None
        self.assertEqual(frame["Close"].dtype.kind, "f")
        self.assertEqual(frame["Volume"].dtype.kind, "f")
        self.assertAlmostEqual(float(frame["Close"].iloc[-1]), 101.7)


if __name__ == "__main__":
    unittest.main()
