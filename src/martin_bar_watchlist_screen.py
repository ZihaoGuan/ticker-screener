from __future__ import annotations

import math
from typing import Any, Mapping

import pandas as pd


STRATEGY_ID = "martin_bar_watchlist_v1"
RULES_VERSION = STRATEGY_ID


def build_rank_context(bar_frames: Mapping[str, Any], as_of_date: Any,
                       expected_sessions: list[Any] | None = None) -> dict[str, dict[str, Any]]:
    """Build the eligible-universe 30-session return rank used by each ticker evaluation."""
    returns: dict[str, float] = {}
    for ticker, frame in bar_frames.items():
        bars = _asof(frame, as_of_date)
        if len(bars) < 120 or not _valid_ohlcv(bars) or not _has_recent_sessions(bars, expected_sessions):
            continue
        if as_of_date is not None and bars.index[-1].date() != pd.Timestamp(as_of_date).date():
            continue
        close = bars["Close"].astype(float)
        if len(bars) < 120 or float(close.iloc[-1]) < 5 or float((close * bars["Volume"].astype(float)).iloc[-20:].mean()) < 20_000_000:
            continue
        if close.iloc[-31] > 0:
            returns[str(ticker).upper()] = float(close.iloc[-1] / close.iloc[-31] - 1)
    ordered = sorted(returns, key=lambda ticker: (-returns[ticker], ticker))
    leader_count = math.ceil(len(ordered) * 0.20)
    return {
        ticker: {"return_30d": returns[ticker], "rank": index + 1,
                 "eligible_universe_size": len(ordered), "is_top_20pct": index < leader_count}
        for index, ticker in enumerate(ordered)
    }


def evaluate_martin_bar_watchlist(
    bars_as_of_T: Any,
    spy_as_of_T: Any,
    universe_rank_context: Mapping[str, Any] | None = None,
    *, ticker: str = "", as_of_date: Any = None, metadata: Mapping[str, Any] | None = None,
    expected_sessions: list[Any] | None = None,
) -> dict[str, Any] | None:
    """Return a measured discovery row, or None when no frozen v1 category matches."""
    bars = _asof(bars_as_of_T, as_of_date)
    if as_of_date is not None and (bars.empty or bars.index[-1].date() != pd.Timestamp(as_of_date).date()):
        return None
    if len(bars) < 120 or not _valid_ohlcv(bars) or not _has_recent_sessions(bars, expected_sessions):
        return None
    close = bars["Close"].astype(float)
    volume = bars["Volume"].astype(float)
    dollar_adv20 = float((close * volume).iloc[-20:].mean())
    price = float(close.iloc[-1])
    if price < 5 or dollar_adv20 < 20_000_000:
        return None
    if len(bars) < 31 or close.iloc[-31] <= 0:
        return None
    prev_close = float(close.iloc[-2])
    if prev_close <= 0:
        return None
    vol_base = float(volume.iloc[-21:-1].mean())
    if vol_base <= 0:
        return None
    vol_ratio = float(volume.iloc[-1] / vol_base)
    one_day_return = float(price / prev_close - 1)
    gap_return = float(float(bars["Open"].iloc[-1]) / prev_close - 1)
    rng = float(bars["High"].iloc[-1] - bars["Low"].iloc[-1])
    close_position = float((price - float(bars["Low"].iloc[-1])) / rng) if rng > 0 else 0.0
    context = dict((universe_rank_context or {}).get(str(ticker).upper(), {}))
    ret30 = context.get("return_30d", float(price / float(close.iloc[-31]) - 1))
    categories: list[str] = []
    reasons: list[str] = []
    if one_day_return >= 0.05 and vol_ratio >= 1.5:
        categories.append("potent_mover")
        reasons.append("close_return_ge_5pct_and_volume_ratio_ge_1_5")
    if gap_return >= 0.05 and vol_ratio >= 1.5 and close_position >= 0.5:
        categories.append("gap_mover")
        reasons.append("open_gap_ge_5pct_volume_ratio_ge_1_5_close_upper_half")
    if float(ret30) >= 0.20 and context.get("is_top_20pct", False):
        categories.append("leader_30_session")
        reasons.append("return_30d_ge_20pct_and_top_20pct_universe")
    if not categories:
        return None

    ema = {n: close.ewm(span=n, adjust=False).mean() for n in (9, 21, 50)}
    adr = ((bars["High"].astype(float) - bars["Low"].astype(float)) / close * 100).iloc[-14:].mean()
    high52 = float(bars["High"].astype(float).iloc[-252:].max()) if len(bars) >= 252 else None
    spy = _asof(spy_as_of_T, as_of_date)
    spy_close = spy["Close"].astype(float) if len(spy) >= 21 else pd.Series(dtype=float)
    stock20 = float(price / float(close.iloc[-21]) - 1) if len(close) >= 21 and close.iloc[-21] else None
    spy20 = float(spy_close.iloc[-1] / float(spy_close.iloc[-21]) - 1) if len(spy_close) >= 21 and spy_close.iloc[-21] else None
    spy30 = float(spy_close.iloc[-1] / float(spy_close.iloc[-31]) - 1) if len(spy_close) >= 31 and spy_close.iloc[-31] else None
    relative20 = stock20 - spy20 if stock20 is not None and spy20 is not None else None
    stock30 = float(price / float(close.iloc[-31]) - 1)
    relative30 = stock30 - spy30 if spy30 is not None else None
    return {
        "ticker": str(ticker).upper(), "as_of_date": str(bars.index[-1].date()),
        "rules_version": RULES_VERSION, "discovery_categories": categories, "reasons": reasons,
        "close": price, "one_day_return_pct": one_day_return * 100, "gap_return_pct": gap_return * 100,
        "volume_ratio_20_excluding_T": vol_ratio, "adv20_dollars": dollar_adv20,
        "return_30d_pct": float(ret30) * 100, "leader_rank": context.get("rank"),
        "leader_universe_size": context.get("eligible_universe_size"),
        "ema_order": "9>21>50" if ema[9].iloc[-1] > ema[21].iloc[-1] > ema[50].iloc[-1] else "other",
        "ema9_slope_pct": float(ema[9].iloc[-1] / ema[9].iloc[-6] - 1) * 100,
        "ema21_slope_pct": float(ema[21].iloc[-1] / ema[21].iloc[-6] - 1) * 100,
        "ema50_slope_pct": float(ema[50].iloc[-1] / ema[50].iloc[-6] - 1) * 100,
        "stock_spy_relative_return_20d_pct": relative20, "adr14_pct": float(adr),
        "stock_spy_relative_return_30d_pct": relative30,
        "distance_from_52w_high_pct": (price / high52 - 1) * 100 if high52 is not None else None,
        "sector": (metadata or {}).get("sector"), "industry": (metadata or {}).get("industry"),
        "instrument_classification": "uncertain_from_stored_metadata",
    }


def _asof(frame: Any, as_of_date: Any):
    if frame is None:
        return pd.DataFrame()
    result = frame.copy()
    if as_of_date is not None:
        result = result.loc[pd.to_datetime(result.index).date <= pd.Timestamp(as_of_date).date()]
    return result.sort_index().tail(272)


def _has_recent_sessions(frame: Any, expected_sessions: list[Any] | None) -> bool:
    if expected_sessions is None:
        return True
    expected = [pd.Timestamp(value).date() for value in expected_sessions]
    actual = [pd.Timestamp(value).date() for value in frame.index[-len(expected):]]
    return bool(expected) and actual == expected


def _valid_ohlcv(frame: Any) -> bool:
    required = ("Open", "High", "Low", "Close", "Volume")
    if any(column not in frame.columns for column in required) or frame.index.has_duplicates:
        return False
    values = frame[list(required)].apply(pd.to_numeric, errors="coerce")
    return bool(values.notna().all().all() and (values[["Open", "High", "Low", "Close"]] > 0).all().all() and
                (values["Volume"] >= 0).all() and (values["High"] >= values["Low"]).all() and
                (values["Low"] <= values[["Open", "Close"]].min(axis=1)).all() and
                (values["High"] >= values[["Open", "Close"]].max(axis=1)).all())
