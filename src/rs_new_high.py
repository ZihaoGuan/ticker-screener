from __future__ import annotations

import pandas as pd


RS_NEW_HIGH_LOOKBACK = 250


def compute_rs_new_high_flags(
    rs_line: pd.Series,
    price_reference: pd.Series,
    lookback: int = RS_NEW_HIGH_LOOKBACK,
) -> tuple[pd.Series, pd.Series]:
    """Return current-window RS highs and RS highs that precede price highs.

    Both comparisons include the current bar, matching Pine's ``ta.highest``.
    For daily bars, ``price_reference`` must be stock High.
    """
    aligned = pd.concat([rs_line, price_reference], axis=1, join="inner").dropna()
    aligned.columns = ["rs_line", "price_reference"]
    if aligned.empty:
        empty = pd.Series(dtype=bool)
        return empty, empty

    window = max(1, int(lookback))
    rs_high = aligned["rs_line"].rolling(window=window, min_periods=1).max()
    price_high = aligned["price_reference"].rolling(window=window, min_periods=1).max()
    new_high = aligned["rs_line"] >= rs_high
    new_high_before_price = new_high & (aligned["price_reference"] < price_high)
    return (
        new_high.reindex(rs_line.index, fill_value=False),
        new_high_before_price.reindex(rs_line.index, fill_value=False),
    )
