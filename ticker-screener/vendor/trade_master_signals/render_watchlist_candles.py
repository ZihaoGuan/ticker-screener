#!/usr/bin/env python3
from __future__ import annotations

import argparse
from dataclasses import dataclass
from html import escape
import json
import math
from pathlib import Path
import re
import sys
from textwrap import wrap
from typing import Iterable

import pandas as pd
import requests


YAHOO_HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "application/json",
}
SVG_SIZE_RE = re.compile(r'<svg[^>]*\bwidth="(?P<width>[0-9.]+)"[^>]*\bheight="(?P<height>[0-9.]+)"', re.IGNORECASE)
SECTOR_ETF_MAP = {
    "Communication Services": "XLC",
    "Consumer Discretionary": "XLY",
    "Technology": "XLK",
    "Information Technology": "XLK",
    "Consumer Staples": "XLP",
    "Financial Services": "XLF",
    "Financial": "XLF",
    "Financials": "XLF",
    "Industrials": "XLI",
    "Health Care": "XLV",
    "Healthcare": "XLV",
    "Energy": "XLE",
    "Consumer Cyclical": "XLY",
    "Consumer Defensive": "XLP",
    "Materials": "XLB",
    "Basic Materials": "XLB",
    "Utilities": "XLU",
    "Real Estate": "XLRE",
}
SECTOR_ALIASES = {
    "communication services": "Communication Services",
    "consumer cyclical": "Consumer Discretionary",
    "consumer discretionary": "Consumer Discretionary",
    "consumer defensive": "Consumer Staples",
    "consumer staples": "Consumer Staples",
    "energy": "Energy",
    "financial": "Financials",
    "financial services": "Financials",
    "financials": "Financials",
    "health care": "Health Care",
    "healthcare": "Health Care",
    "industrials": "Industrials",
    "basic materials": "Materials",
    "materials": "Materials",
    "real estate": "Real Estate",
    "technology": "Information Technology",
    "information technology": "Information Technology",
    "utilities": "Utilities",
}
INDUSTRY_ETF_KEYWORDS = (
    ("semiconductor", "SOXX"),
    ("semiconductors", "SOXX"),
    ("chip", "SOXX"),
)
RS_RATING_REPLAY_THRESHOLDS = (
    195.93,
    117.11,
    99.04,
    91.66,
    80.96,
    53.64,
    24.86,
)


@dataclass
class WatchlistEntry:
    ticker: str
    setup_label: str
    summary: str
    master_note: str
    trigger_price: float | None = None
    trigger_label: str | None = None
    entry_style: str | None = None
    entry_price: float | None = None
    entry_label: str | None = None
    entry_timeframe: str | None = None
    secondary_entry_price: float | None = None
    secondary_entry_label: str | None = None
    secondary_entry_timeframe: str | None = None
    stop_price: float | None = None
    stop_label: str | None = None
    stop_timeframe: str | None = None


@dataclass
class GapZone:
    start_index: int
    end_index: int
    original_lower_price: float
    original_upper_price: float
    remaining_lower_price: float
    remaining_upper_price: float
    direction: str
    gap_percent: float
    filled: bool


@dataclass
class RSAnalysis:
    daily_line: pd.Series
    daily_score: pd.Series
    daily_rating: pd.Series
    daily_new_high: pd.Series
    daily_new_high_before_price: pd.Series
    weekly_new_high: pd.Series
    weekly_new_high_before_price: pd.Series


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Render daily candle charts for a trade-master watchlist.")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--watchlist-file", help="JSON watchlist file with structured setup notes.")
    group.add_argument("--ticker-file", help="Plain text file with one ticker per line.")
    group.add_argument("--tickers", nargs="+", help="Ticker symbols to render.")
    parser.add_argument("--output-dir", required=True, help="Directory for charts and summary artifacts.")
    parser.add_argument("--benchmark", default="SPY", help="Benchmark ticker for RS display.")
    parser.add_argument("--period", default="18mo", help="Yahoo chart range such as 1y, 18mo, 2y.")
    parser.add_argument("--lookback", type=int, default=120, help="Number of daily bars to render.")
    parser.add_argument("--split-pages", type=int, default=0, help="If > 0, emit montage pages with this many charts per page.")
    parser.add_argument("--montage-columns", type=int, default=2, help="Number of columns for split montage pages.")
    parser.add_argument("--card-width", type=int, default=700, help="Scaled width of each chart in montage pages.")
    return parser.parse_args()


def fetch_history(ticker: str, period: str) -> pd.DataFrame:
    response = requests.get(
        f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}",
        params={
            "interval": "1d",
            "range": period,
            "includeAdjustedClose": "true",
        },
        headers=YAHOO_HEADERS,
        timeout=30,
    )
    response.raise_for_status()
    payload = response.json()
    result = payload.get("chart", {}).get("result", [])
    if not result:
        error = payload.get("chart", {}).get("error")
        raise ValueError(f"No Yahoo chart result returned for {ticker}: {error}")

    chart = result[0]
    timestamps = chart.get("timestamp", [])
    quote_list = chart.get("indicators", {}).get("quote", [])
    if not timestamps or not quote_list:
        raise ValueError(f"Incomplete Yahoo chart payload for {ticker}")

    quote = quote_list[0]
    history = pd.DataFrame(
        {
            "Open": quote.get("open", []),
            "High": quote.get("high", []),
            "Low": quote.get("low", []),
            "Close": quote.get("close", []),
            "Volume": quote.get("volume", []),
        },
        index=pd.to_datetime(timestamps, unit="s", utc=True),
    )
    exchange_timezone = chart.get("meta", {}).get("exchangeTimezoneName")
    if exchange_timezone:
        history.index = history.index.tz_convert(exchange_timezone).normalize().tz_localize(None)
    else:
        history.index = history.index.tz_localize(None)
    history = history.dropna(subset=["Open", "High", "Low", "Close", "Volume"]).copy()
    if history.empty:
        raise ValueError(f"No usable price history returned for {ticker}")
    return history[["Open", "High", "Low", "Close", "Volume"]]


def fetch_company_profile(ticker: str) -> dict[str, str]:
    try:
        response = requests.get(
            f"https://query1.finance.yahoo.com/v10/finance/quoteSummary/{ticker}",
            params={"modules": "assetProfile,summaryProfile"},
            headers=YAHOO_HEADERS,
            timeout=30,
        )
        response.raise_for_status()
        payload = response.json()
    except Exception:
        return {"sector": "Unknown", "industry": "Unknown"}
    result = payload.get("quoteSummary", {}).get("result", [])
    if not result:
        return {"sector": "Unknown", "industry": "Unknown"}
    asset_profile = result[0].get("assetProfile", {})
    summary_profile = result[0].get("summaryProfile", {})
    sector = asset_profile.get("sector") or summary_profile.get("sector") or "Unknown"
    industry = asset_profile.get("industry") or summary_profile.get("industry") or "Unknown"
    return {"sector": sector, "industry": industry}


def add_price_indicators(frame: pd.DataFrame) -> pd.DataFrame:
    data = frame.copy()
    data["ema8"] = data["Close"].ewm(span=8, adjust=False).mean()
    data["ema21"] = data["Close"].ewm(span=21, adjust=False).mean()
    data["ema40"] = data["Close"].ewm(span=40, adjust=False).mean()
    data["sma50"] = data["Close"].rolling(50).mean()
    data["sma200"] = data["Close"].rolling(200).mean()
    data["avg_volume20"] = data["Volume"].rolling(20).mean()
    data["relative_volume20"] = data["Volume"] / data["avg_volume20"]
    data["adr_percent"] = ((data["High"] - data["Low"]) / data["Close"].shift(1) * 100).rolling(20).mean()
    weekly_close = data["Close"].resample("W-FRI").last().dropna()
    weekly_ema8 = weekly_close.ewm(span=8, adjust=False).mean()
    data["weekly_ema8"] = weekly_ema8.reindex(data.index, method="ffill")
    return data


def compute_ipo_vwap(frame: pd.DataFrame) -> pd.Series:
    source = frame.dropna(subset=["High", "Low", "Close", "Volume"]).copy()
    typical_price = (source["High"] + source["Low"] + source["Close"]) / 3.0
    cumulative_value = (typical_price * source["Volume"]).cumsum()
    cumulative_volume = source["Volume"].cumsum().replace(0, pd.NA)
    ipo_vwap = cumulative_value / cumulative_volume
    return ipo_vwap.reindex(frame.index)


def _coerce_optional_float(value: object) -> float | None:
    if value in (None, "", "NA", "n/a"):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _is_weekly_pullback_reclaim_setup(entry: WatchlistEntry, note_lower: str) -> bool:
    style = (entry.entry_style or "").lower()
    return (
        style in {"8w_support_pivot", "weekly_pullback_reclaim", "htf_reclaim"}
        or "htf" in style
        or "8 week" in note_lower
        or "8-week" in note_lower
        or ("pivot" in note_lower and "reclaim" in note_lower)
        or ("buy rs on weakness" in note_lower and "pivot" in note_lower)
    )


def _is_thirty_min_pivot_setup(entry: WatchlistEntry, note_lower: str) -> bool:
    style = (entry.entry_style or "").lower()
    return (
        style in {"30m_pivot", "thirty_min_pivot", "ltf_pivot"}
        or "30 min pivot" in note_lower
        or "30-minute pivot" in note_lower
        or "string of red" in note_lower
    )


def compute_rs_line(stock: pd.Series, benchmark: pd.Series) -> pd.Series:
    aligned = pd.concat([stock, benchmark], axis=1, join="inner").dropna()
    aligned.columns = ["stock", "benchmark"]
    return aligned["stock"] / aligned["benchmark"]


def compute_weighted_rs_score(stock: pd.Series, benchmark: pd.Series) -> pd.Series:
    aligned = pd.concat([stock, benchmark], axis=1, join="inner").dropna()
    aligned.columns = ["stock", "benchmark"]
    perf_stock63 = aligned["stock"] / aligned["stock"].shift(63)
    perf_stock126 = aligned["stock"] / aligned["stock"].shift(126)
    perf_stock189 = aligned["stock"] / aligned["stock"].shift(189)
    perf_stock252 = aligned["stock"] / aligned["stock"].shift(252)
    perf_bench63 = aligned["benchmark"] / aligned["benchmark"].shift(63)
    perf_bench126 = aligned["benchmark"] / aligned["benchmark"].shift(126)
    perf_bench189 = aligned["benchmark"] / aligned["benchmark"].shift(189)
    perf_bench252 = aligned["benchmark"] / aligned["benchmark"].shift(252)
    rs_stock = 0.4 * perf_stock63 + 0.2 * perf_stock126 + 0.2 * perf_stock189 + 0.2 * perf_stock252
    rs_benchmark = 0.4 * perf_bench63 + 0.2 * perf_bench126 + 0.2 * perf_bench189 + 0.2 * perf_bench252
    return (rs_stock / rs_benchmark) * 100


def _attribute_percentile(score: float, taller_perf: float, smaller_perf: float, range_up: int, range_dn: int, weight: float) -> float:
    adjusted_score = score + (score - smaller_perf) * weight
    if adjusted_score > taller_perf - 1:
        adjusted_score = taller_perf - 1
    k1 = smaller_perf / range_dn
    k2 = (taller_perf - 1) / range_up
    k3 = (k1 - k2) / (taller_perf - 1 - smaller_perf)
    rating = adjusted_score / (k1 - k3 * (score - smaller_perf))
    return max(min(rating, range_up), range_dn)


def approximate_rs_rating(score: float) -> float | None:
    if pd.isna(score):
        return None
    first, scnd, thrd, frth, ffth, sxth, svth = RS_RATING_REPLAY_THRESHOLDS
    if score >= first:
        return 99.0
    if score <= svth:
        return 1.0
    if scnd <= score < first:
        return _attribute_percentile(score, first, scnd, 98, 90, 0.33)
    if thrd <= score < scnd:
        return _attribute_percentile(score, scnd, thrd, 89, 70, 2.1)
    if frth <= score < thrd:
        return _attribute_percentile(score, thrd, frth, 69, 50, 0.0)
    if ffth <= score < frth:
        return _attribute_percentile(score, frth, ffth, 49, 30, 0.0)
    if sxth <= score < ffth:
        return _attribute_percentile(score, ffth, sxth, 29, 10, 0.0)
    return _attribute_percentile(score, sxth, svth, 9, 2, 0.0)


def compute_rs_new_high_flags(rs_line: pd.Series, price_reference: pd.Series, lookback: int) -> tuple[pd.Series, pd.Series]:
    aligned = pd.concat([rs_line, price_reference], axis=1, join="inner").dropna()
    aligned.columns = ["rs_line", "price_reference"]
    rolling_rs_high = aligned["rs_line"].rolling(window=lookback, min_periods=1).max()
    rolling_price_high = aligned["price_reference"].rolling(window=lookback, min_periods=1).max()
    tolerance = 1e-12
    new_high = aligned["rs_line"] >= (rolling_rs_high - tolerance)
    new_high_before_price = new_high & (aligned["price_reference"] < (rolling_price_high - tolerance))
    return new_high.reindex(rs_line.index, fill_value=False), new_high_before_price.reindex(rs_line.index, fill_value=False)


def compute_rs_analysis(history: pd.DataFrame, benchmark_history: pd.DataFrame, daily_lookback: int = 250, weekly_lookback: int = 52) -> RSAnalysis:
    daily_line = compute_rs_line(history["Close"], benchmark_history["Close"])
    daily_score = compute_weighted_rs_score(history["Close"], benchmark_history["Close"]).reindex(daily_line.index)
    daily_rating = daily_score.apply(approximate_rs_rating)
    daily_new_high, daily_new_high_before_price = compute_rs_new_high_flags(
        rs_line=daily_line,
        price_reference=history["High"].reindex(daily_line.index),
        lookback=daily_lookback,
    )

    weekly_stock = history.resample("W-FRI").agg({"Close": "last", "High": "max"}).dropna()
    weekly_benchmark = benchmark_history.resample("W-FRI").agg({"Close": "last"}).dropna()
    weekly_rs_line = compute_rs_line(weekly_stock["Close"], weekly_benchmark["Close"])
    weekly_new_high, weekly_new_high_before_price = compute_rs_new_high_flags(
        rs_line=weekly_rs_line,
        price_reference=weekly_stock["High"].reindex(weekly_rs_line.index),
        lookback=weekly_lookback,
    )

    return RSAnalysis(
        daily_line=daily_line,
        daily_score=daily_score,
        daily_rating=daily_rating,
        daily_new_high=daily_new_high,
        daily_new_high_before_price=daily_new_high_before_price,
        weekly_new_high=weekly_new_high,
        weekly_new_high_before_price=weekly_new_high_before_price,
    )


def _polyline_points(
    values: pd.Series,
    x_values: list[float],
    min_value: float,
    max_value: float,
    top: int,
    plot_height: int,
) -> str:
    points: list[str] = []
    value_range = max(max_value - min_value, 1e-6)
    for x, value in zip(x_values, values):
        if pd.isna(value):
            continue
        y = top + plot_height - ((float(value) - min_value) / value_range * plot_height)
        points.append(f"{x:.1f},{y:.1f}")
    return " ".join(points)


def _text_block(svg: list[str], text: str, x: int, y: int, color: str, font_size: int, width: int = 38) -> int:
    lines = wrap(text, width=width) or [text]
    for index, line in enumerate(lines):
        svg.append(
            f'<text x="{x}" y="{y + index * (font_size + 6)}" fill="{color}" '
            f'font-size="{font_size}" font-family="Menlo, Consolas, monospace">{escape(line)}</text>'
        )
    return len(lines)


def _spread_label_positions(preferred_positions: list[float], minimum_gap: float, lower_bound: float, upper_bound: float) -> list[float]:
    if not preferred_positions:
        return []
    adjusted: list[float] = []
    for preferred in preferred_positions:
        clamped = max(lower_bound, min(preferred, upper_bound))
        if adjusted and clamped < adjusted[-1] + minimum_gap:
            clamped = adjusted[-1] + minimum_gap
        adjusted.append(clamped)
    overflow = adjusted[-1] - upper_bound
    if overflow > 0:
        adjusted = [value - overflow for value in adjusted]
        if adjusted[0] < lower_bound:
            deficit = lower_bound - adjusted[0]
            adjusted = [value + deficit for value in adjusted]
    return adjusted


def _price_y(value: float, y_min: float, y_max: float, top: int, plot_height: int) -> float:
    price_range = max(y_max - y_min, 1e-6)
    return top + plot_height - ((value - y_min) / price_range * plot_height)


def detect_gap_zones(chart: pd.DataFrame) -> list[GapZone]:
    zones: list[GapZone] = []
    if len(chart) < 2:
        return zones

    for index in range(1, len(chart)):
        prev_high = float(chart["High"].iloc[index - 1])
        prev_low = float(chart["Low"].iloc[index - 1])
        current_high = float(chart["High"].iloc[index])
        current_low = float(chart["Low"].iloc[index])

        direction: str | None = None
        original_lower_price = 0.0
        original_upper_price = 0.0

        if current_low > prev_high:
            direction = "up"
            original_lower_price = prev_high
            original_upper_price = current_low
        elif current_high < prev_low:
            direction = "down"
            original_lower_price = current_high
            original_upper_price = prev_low

        if direction is None:
            continue

        end_index = len(chart) - 1
        filled = False
        remaining_lower_price = original_lower_price
        remaining_upper_price = original_upper_price
        for future_index in range(index + 1, len(chart)):
            future_high = float(chart["High"].iloc[future_index])
            future_low = float(chart["Low"].iloc[future_index])
            if direction == "up":
                remaining_upper_price = min(remaining_upper_price, max(future_low, original_lower_price))
                if future_low <= original_lower_price:
                    end_index = future_index
                    filled = True
                    remaining_upper_price = original_lower_price
                    break
            else:
                remaining_lower_price = max(remaining_lower_price, min(future_high, original_upper_price))
                if future_high >= original_upper_price:
                    end_index = future_index
                    filled = True
                    remaining_lower_price = original_upper_price
                    break

        gap_percent = (
            (original_upper_price / original_lower_price - 1) * 100 if original_lower_price else 0.0
        )
        zones.append(
            GapZone(
                start_index=index,
                end_index=end_index,
                original_lower_price=original_lower_price,
                original_upper_price=original_upper_price,
                remaining_lower_price=remaining_lower_price,
                remaining_upper_price=remaining_upper_price,
                direction=direction,
                gap_percent=gap_percent,
                filled=filled,
            )
        )
    return zones


def build_entry_plan(
    entry: WatchlistEntry,
    latest_close: float,
    trigger_price: float,
    ema8: float,
    ema21: float,
    weekly_ema8: float,
    above_trigger: bool,
) -> tuple[list[str], float, str]:
    trigger_gap = (latest_close / trigger_price - 1) * 100 if trigger_price else 0.0
    note_lower = f"{entry.summary} {entry.master_note}".lower()
    weekly_pullback_reclaim = _is_weekly_pullback_reclaim_setup(entry, note_lower)
    thirty_min_pivot = _is_thirty_min_pivot_setup(entry, note_lower)
    entry_ref = entry.entry_price if entry.entry_price is not None else trigger_price
    entry_label = entry.entry_label or ("30m pivot" if thirty_min_pivot else "Entry ref")

    if thirty_min_pivot:
        lines = [
            f"Entry plan: reclaim {entry_label} > {entry_ref:.2f}",
            "Wait for price to turn back up through the intraday pivot",
            f"Weekly 8 EMA: {weekly_ema8:.2f} | EMA21 flush ref: {ema21:.2f}",
        ]
        if entry.secondary_entry_price is not None:
            secondary_label = entry.secondary_entry_label or "Secondary support"
            lines.append(f"{secondary_label}: {entry.secondary_entry_price:.2f}")
        return lines, entry_ref, "#22c55e" if latest_close >= entry_ref else "#f97316"

    if above_trigger:
        if weekly_pullback_reclaim:
            lines = [
                f"Entry plan: reclaim hold > {entry_ref:.2f}",
                "Buy on the way back up, not during the flush down",
                f"Weekly 8 EMA: {weekly_ema8:.2f} | EMA21 flush ref: {ema21:.2f}",
            ]
        else:
            lines = [
                f"Entry plan: pullback hold {entry_ref:.2f}",
                f"Add only if trigger keeps acting as support",
                f"EMA8 ref: {ema8:.2f} | Extension: {trigger_gap:.2f}%",
            ]
        if entry.secondary_entry_price is not None:
            secondary_label = entry.secondary_entry_label or "Secondary entry"
            lines.append(f"{secondary_label}: {entry.secondary_entry_price:.2f}")
        return lines, entry_ref, "#22c55e"

    if weekly_pullback_reclaim:
        lines = [
            f"Entry plan: reclaim pivot > {entry_ref:.2f}",
            "Wait for price to turn back up through the trigger",
            f"Weekly 8 EMA: {weekly_ema8:.2f} | EMA21 flush ref: {ema21:.2f}",
        ]
    elif "8ema" in note_lower or "8 ema" in note_lower:
        lines = [
            f"Entry plan: break > {entry_ref:.2f}",
            f"Alt entry: support hold near EMA8 {ema8:.2f}",
            f"EMA21 ref: {ema21:.2f}",
        ]
    else:
        lines = [
            f"Entry plan: break > {entry_ref:.2f}",
            f"Prefer close strength instead of early anticipation",
            f"EMA8 ref: {ema8:.2f} | EMA21 ref: {ema21:.2f}",
        ]
    if entry.secondary_entry_price is not None:
        secondary_label = entry.secondary_entry_label or "Secondary entry"
        lines.append(f"{secondary_label}: {entry.secondary_entry_price:.2f}")
    return lines, entry_ref, "#f97316"


def build_stop_plan(
    entry: WatchlistEntry,
    trigger_price: float,
    ema8: float,
    ema21: float,
    weekly_ema8: float,
    recent_support: float,
    above_trigger: bool,
) -> tuple[list[str], float, str]:
    note_lower = f"{entry.summary} {entry.master_note}".lower()
    weekly_pullback_reclaim = _is_weekly_pullback_reclaim_setup(entry, note_lower)
    thirty_min_pivot = _is_thirty_min_pivot_setup(entry, note_lower)
    trigger_ref = entry.entry_price if entry.entry_price is not None else trigger_price

    if entry.stop_price is not None:
        stop_label = entry.stop_label or "Explicit stop"
        stop_price = entry.stop_price
        time_frame = f" ({entry.stop_timeframe})" if entry.stop_timeframe else ""
        lines = [
            f"Stop guide: {stop_label}{time_frame}",
            f"Stop ref: {stop_price:.2f}",
            f"Invalidate if price loses the stated level cleanly",
        ]
        return lines, stop_price, "#ef4444"

    if thirty_min_pivot:
        stop_price = min(recent_support, ema21) * 0.99
        lines = [
            "Stop guide: below recent 30m pivot low (daily proxy shown)",
            f"Stop ref: {stop_price:.2f}",
            f"Support refs: EMA21 {ema21:.2f}, Weekly 8 EMA {weekly_ema8:.2f}",
        ]
        return lines, stop_price, "#ef4444"

    if above_trigger:
        if weekly_pullback_reclaim:
            stop_price = min(trigger_ref, weekly_ema8, recent_support) * 0.985
            lines = [
                "Stop guide: below reclaim / weekly 8 EMA support",
                f"Stop ref: {stop_price:.2f}",
                f"Support refs: pivot {trigger_ref:.2f}, Weekly 8 EMA {weekly_ema8:.2f}",
            ]
        else:
            stop_price = min(trigger_ref, ema21) * 0.985
            lines = [
                f"Stop guide: below trigger / EMA21",
                f"Stop ref: {stop_price:.2f}",
                f"Support refs: trigger {trigger_ref:.2f}, EMA21 {ema21:.2f}",
            ]
        return lines, stop_price, "#ef4444"

    if weekly_pullback_reclaim:
        stop_price = min(weekly_ema8, recent_support) * 0.985
        lines = [
            "Stop guide: below weekly 8 EMA / flush low",
            f"Stop ref: {stop_price:.2f}",
            f"Support refs: EMA21 {ema21:.2f}, Weekly 8 EMA {weekly_ema8:.2f}",
        ]
    elif "8ema" in note_lower or "8 ema" in note_lower:
        stop_price = min(ema21, recent_support) * 0.99
        lines = [
            "Stop guide: below EMA21 / recent retest low",
            f"Stop ref: {stop_price:.2f}",
            f"Support refs: EMA8 {ema8:.2f}, EMA21 {ema21:.2f}",
        ]
    else:
        stop_price = min(ema21, recent_support) * 0.985
        lines = [
            "Stop guide: below recent support cluster",
            f"Stop ref: {stop_price:.2f}",
            f"Support refs: support {recent_support:.2f}, EMA21 {ema21:.2f}",
        ]
    return lines, stop_price, "#ef4444"


def build_rr_comment(latest_close: float, entry_price: float, stop_price: float, above_trigger: bool) -> str:
    risk_pct = abs((entry_price / stop_price - 1) * 100) if stop_price else 0.0
    extension_pct = abs((latest_close / entry_price - 1) * 100) if entry_price else 0.0
    if above_trigger:
        if extension_pct > 3.0:
            return f"R/R: extended {extension_pct:.1f}% above entry ref, prefer retest"
        if risk_pct <= 6.0:
            return f"R/R: tight risk around {risk_pct:.1f}%, trigger hold matters"
        return f"R/R: workable only if support holds, risk about {risk_pct:.1f}%"
    if risk_pct <= 6.0:
        return f"R/R: clean break can work, initial risk about {risk_pct:.1f}%"
    return f"R/R: wide risk around {risk_pct:.1f}%, wait for tighter entry"


def build_sector_snapshot(
    profile: dict[str, str] | None,
    period: str,
    sector_history_cache: dict[str, pd.DataFrame],
) -> dict[str, str]:
    profile = profile or {"sector": "Unknown", "industry": "Unknown"}
    raw_sector = profile.get("sector", "Unknown")
    sector = SECTOR_ALIASES.get(raw_sector.strip().lower(), raw_sector)
    industry = profile.get("industry", "Unknown")
    industry_lower = industry.lower()
    etf = next((candidate for keyword, candidate in INDUSTRY_ETF_KEYWORDS if keyword in industry_lower), None)
    if not etf:
        etf = SECTOR_ETF_MAP.get(sector)
    if not etf:
        return {
            "sector": sector,
            "industry": industry,
            "etf": "n/a",
            "ret5": "n/a",
            "ret20": "n/a",
            "trend": "n/a",
        }

    if etf not in sector_history_cache:
        sector_history_cache[etf] = add_price_indicators(fetch_history(etf, period))

    history = sector_history_cache[etf]
    latest = history.iloc[-1]
    close = float(latest["Close"])
    prior5 = float(history["Close"].iloc[-6]) if len(history) > 5 else float(history["Close"].iloc[0])
    prior20 = float(history["Close"].iloc[-21]) if len(history) > 20 else float(history["Close"].iloc[0])
    ret5 = (close / prior5 - 1) * 100 if prior5 else 0.0
    ret20 = (close / prior20 - 1) * 100 if prior20 else 0.0
    if close > float(latest["ema21"]) and close > float(latest["sma50"]) and ret20 > 0:
        trend = "strong"
    elif close > float(latest["sma50"]) or ret5 > 0:
        trend = "neutral"
    else:
        trend = "weak"
    return {
        "sector": sector,
        "industry": industry,
        "etf": etf,
        "ret5": f"{ret5:+.2f}%",
        "ret20": f"{ret20:+.2f}%",
        "trend": trend,
    }


def render_watchlist_chart(
    entry: WatchlistEntry,
    history: pd.DataFrame,
    benchmark_history: pd.DataFrame,
    output_path: str | Path,
    lookback: int = 120,
    benchmark_ticker: str = "SPY",
    profile: dict[str, str] | None = None,
    sector_snapshot: dict[str, str] | None = None,
    ipo_history: pd.DataFrame | None = None,
) -> Path:
    history_with_indicators = add_price_indicators(history)
    ipo_source = add_price_indicators(ipo_history) if ipo_history is not None else history_with_indicators
    ipo_vwap_full = compute_ipo_vwap(ipo_source)
    rs_analysis = compute_rs_analysis(history_with_indicators, benchmark_history)
    chart = history_with_indicators.tail(lookback).copy()
    chart = chart.loc[chart.index.intersection(rs_analysis.daily_line.index)]
    benchmark = benchmark_history.reindex(chart.index).dropna()
    chart = chart.loc[benchmark.index]
    chart["ipo_vwap"] = ipo_vwap_full.reindex(chart.index).ffill()
    rs_line = rs_analysis.daily_line.reindex(chart.index)
    daily_new_high = rs_analysis.daily_new_high.reindex(chart.index).fillna(False)
    daily_new_high_before_price = rs_analysis.daily_new_high_before_price.reindex(chart.index).fillna(False)
    output_file = Path(output_path)
    output_file.parent.mkdir(parents=True, exist_ok=True)

    width = 1760
    height = 1450
    left = 90
    plot_width = 960
    price_top = 80
    price_height = 560
    volume_top = 680
    volume_height = 130
    rs_top = 860
    rs_height = 140
    right_panel_x = 1210

    lows = chart["Low"]
    highs = chart["High"]
    y_min = float(lows.min()) * 0.97
    y_max = float(highs.max()) * 1.03
    volume_max = max(float(chart["Volume"].max()), 1.0)
    rs_min = float(rs_line.min()) * 0.98
    rs_max = float(rs_line.max()) * 1.02
    rs_range = max(rs_max - rs_min, 1e-6)
    x_step = plot_width / max(len(chart), 1)
    x_values = [left + (index + 0.5) * x_step for index in range(len(chart))]

    latest = chart.iloc[-1]
    prior_20_high = float(chart["High"].iloc[:-1].tail(20).max()) if len(chart) > 20 else float(chart["High"].max())
    prior_50_high = float(chart["High"].iloc[:-1].tail(50).max()) if len(chart) > 50 else float(chart["High"].max())
    trigger_price = entry.trigger_price if entry.trigger_price is not None else prior_20_high
    avg_volume20 = float(latest["avg_volume20"]) if pd.notna(latest["avg_volume20"]) else 0.0
    relative_volume20 = float(latest["relative_volume20"]) if pd.notna(latest["relative_volume20"]) else 0.0
    adr_percent = float(latest["adr_percent"]) if pd.notna(latest["adr_percent"]) else 0.0
    latest_rs_value = float(rs_line.iloc[-1]) if pd.notna(rs_line.iloc[-1]) else float("nan")
    latest_rs_score = rs_analysis.daily_score.reindex(chart.index).iloc[-1]
    latest_rs_rating = rs_analysis.daily_rating.reindex(chart.index).iloc[-1]
    latest_daily_rs_new_high = bool(daily_new_high.iloc[-1])
    latest_daily_rs_new_high_before_price = bool(daily_new_high_before_price.iloc[-1])
    latest_weekly_rs_new_high = bool(rs_analysis.weekly_new_high.iloc[-1]) if not rs_analysis.weekly_new_high.empty else False
    latest_weekly_rs_new_high_before_price = (
        bool(rs_analysis.weekly_new_high_before_price.iloc[-1]) if not rs_analysis.weekly_new_high_before_price.empty else False
    )
    latest_weekly_bar_date = rs_analysis.weekly_new_high.index[-1] if not rs_analysis.weekly_new_high.empty else None
    above_trigger = float(latest["Close"]) > trigger_price
    latest_close = float(latest["Close"])
    note_lower = f"{entry.summary} {entry.master_note}".lower()
    if entry.trigger_label:
        trigger_label = entry.trigger_label
    elif _is_weekly_pullback_reclaim_setup(entry, note_lower):
        trigger_label = "Pivot reclaim"
    else:
        trigger_label = "Watch level"
    ema8_value = float(latest["ema8"]) if pd.notna(latest["ema8"]) else latest_close
    ema21_value = float(latest["ema21"]) if pd.notna(latest["ema21"]) else latest_close
    weekly_ema8_value = float(latest["weekly_ema8"]) if pd.notna(latest["weekly_ema8"]) else latest_close
    gap_zones = detect_gap_zones(chart)
    open_gap_zones = [zone for zone in gap_zones if not zone.filled]
    recent_support = float(chart["Low"].tail(10).min()) if len(chart) >= 10 else float(chart["Low"].min())
    recent_ema21_flush = bool(((chart["Low"].tail(15) < chart["ema21"].tail(15)).fillna(False)).any() and latest_close > ema21_value)
    weekly_pullback_reclaim = _is_weekly_pullback_reclaim_setup(entry, note_lower)
    entry_lines, entry_price, entry_color = build_entry_plan(
        entry=entry,
        latest_close=latest_close,
        trigger_price=trigger_price,
        ema8=ema8_value,
        ema21=ema21_value,
        weekly_ema8=weekly_ema8_value,
        above_trigger=above_trigger,
    )
    stop_lines, stop_price, stop_color = build_stop_plan(
        entry=entry,
        trigger_price=trigger_price,
        ema8=ema8_value,
        ema21=ema21_value,
        weekly_ema8=weekly_ema8_value,
        recent_support=recent_support,
        above_trigger=above_trigger,
    )
    rr_comment = build_rr_comment(
        latest_close=latest_close,
        entry_price=entry_price,
        stop_price=stop_price,
        above_trigger=above_trigger,
    )
    sector_snapshot = sector_snapshot or {
        "sector": (profile or {}).get("sector", "Unknown"),
        "industry": (profile or {}).get("industry", "Unknown"),
        "etf": "n/a",
        "ret5": "n/a",
        "ret20": "n/a",
        "trend": "n/a",
    }

    svg = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}">',
        '<rect width="100%" height="100%" fill="#07101e" />',
        f'<text x="{left}" y="34" fill="#f8fafc" font-size="26" font-family="Menlo, Consolas, monospace">{entry.ticker} Daily Setup Chart</text>',
        f'<text x="{left}" y="58" fill="#94a3b8" font-size="15" font-family="Menlo, Consolas, monospace">Setup: {escape(entry.setup_label)} | Lookback: {lookback} sessions</text>',
    ]

    for step in range(6):
        y = price_top + step * (price_height / 5)
        price = y_max - step * ((y_max - y_min) / 5)
        svg.append(f'<line x1="{left}" y1="{y:.1f}" x2="{left + plot_width}" y2="{y:.1f}" stroke="#1e293b" stroke-width="1" />')
        svg.append(
            f'<text x="{left - 12}" y="{y + 4:.1f}" fill="#94a3b8" font-size="12" text-anchor="end" '
            f'font-family="Menlo, Consolas, monospace">{price:.2f}</text>'
        )

    visible_gap_zones = [
        zone for zone in gap_zones if zone.remaining_upper_price > zone.remaining_lower_price + 1e-6
    ]

    for zone in visible_gap_zones:
        x_start = left + zone.start_index * x_step
        x_end = left + (zone.end_index + 1) * x_step
        zone_top_y = _price_y(zone.remaining_upper_price, y_min, y_max, price_top, price_height)
        zone_bottom_y = _price_y(zone.remaining_lower_price, y_min, y_max, price_top, price_height)
        zone_y = min(zone_top_y, zone_bottom_y)
        zone_height = max(abs(zone_bottom_y - zone_top_y), 3.0)
        fill_color = "#22c55e" if zone.direction == "up" else "#ef4444"
        stroke_color = "#86efac" if zone.direction == "up" else "#fca5a5"
        opacity = "0.12" if zone.filled else "0.18"
        svg.append(
            f'<rect x="{x_start:.1f}" y="{zone_y:.1f}" width="{max(x_end - x_start, 3.0):.1f}" '
            f'height="{zone_height:.1f}" fill="{fill_color}" opacity="{opacity}" rx="4" />'
        )
        svg.append(
            f'<rect x="{x_start:.1f}" y="{zone_y:.1f}" width="{max(x_end - x_start, 3.0):.1f}" '
            f'height="{zone_height:.1f}" fill="none" stroke="{stroke_color}" stroke-width="0.8" opacity="0.35" rx="4" />'
        )

    for index, (_, row) in enumerate(chart.iterrows()):
        x = x_values[index]
        open_price = float(row["Open"])
        close_price = float(row["Close"])
        high_price = float(row["High"])
        low_price = float(row["Low"])
        color = "#22c55e" if close_price >= open_price else "#ef4444"
        wick_top = _price_y(high_price, y_min, y_max, price_top, price_height)
        wick_bottom = _price_y(low_price, y_min, y_max, price_top, price_height)
        body_top = _price_y(max(open_price, close_price), y_min, y_max, price_top, price_height)
        body_bottom = _price_y(min(open_price, close_price), y_min, y_max, price_top, price_height)
        body_height = max(body_bottom - body_top, 1.5)
        body_width = max(x_step * 0.58, 2.0)
        svg.append(f'<line x1="{x:.1f}" y1="{wick_top:.1f}" x2="{x:.1f}" y2="{wick_bottom:.1f}" stroke="{color}" stroke-width="1.3" />')
        svg.append(
            f'<rect x="{x - body_width / 2:.1f}" y="{body_top:.1f}" width="{body_width:.1f}" height="{body_height:.1f}" fill="{color}" opacity="0.9" />'
        )
        volume_height_px = float(row["Volume"]) / volume_max * volume_height
        svg.append(
            f'<rect x="{x - body_width / 2:.1f}" y="{volume_top + volume_height - volume_height_px:.1f}" '
            f'width="{body_width:.1f}" height="{volume_height_px:.1f}" fill="{color}" opacity="0.48" />'
        )
        rs_marker_y = min(price_top + price_height - 10.0, wick_bottom + 16.0)
        if bool(daily_new_high.iloc[index]):
            marker_radius = max(min(x_step * 0.24, 11.0), 4.5)
            svg.append(f'<circle cx="{x:.1f}" cy="{rs_marker_y:.1f}" r="{marker_radius:.1f}" fill="#3b82f6" opacity="0.58" />')
        if bool(daily_new_high_before_price.iloc[index]):
            ring_radius = max(min(x_step * 0.24, 11.0), 4.5) + 2.0
            svg.append(
                f'<circle cx="{x:.1f}" cy="{rs_marker_y:.1f}" r="{ring_radius:.1f}" fill="none" stroke="#93c5fd" stroke-width="1.2" opacity="0.95" />'
            )

    indicator_specs = [
        ("ema8", "#38bdf8", "EMA 8"),
        ("ema21", "#f59e0b", "EMA 21"),
        ("weekly_ema8", "#4ade80", "Weekly 8 EMA"),
        ("ipo_vwap", "#f472b6", "IPO VWAP"),
        ("sma50", "#a78bfa", "SMA 50"),
        ("sma200", "#f97316", "SMA 200"),
    ]
    for legend_index, (column, color, label) in enumerate(indicator_specs):
        points = _polyline_points(chart[column], x_values, y_min, y_max, price_top, price_height)
        svg.append(f'<polyline points="{points}" fill="none" stroke="{color}" stroke-width="2" />')
        svg.append(
            f'<text x="{right_panel_x}" y="{145 + legend_index * 20}" fill="{color}" '
            f'font-size="13" font-family="Menlo, Consolas, monospace">{label}</text>'
        )
    svg.append(
        f'<text x="{right_panel_x}" y="225" fill="#86efac" font-size="13" font-family="Menlo, Consolas, monospace">Gap up zone</text>'
    )
    svg.append(
        f'<text x="{right_panel_x}" y="245" fill="#fca5a5" font-size="13" font-family="Menlo, Consolas, monospace">Gap down zone</text>'
    )

    chart_label_x = right_panel_x - 18
    chart_label_specs: list[tuple[float, str, str]] = []
    trigger_y = _price_y(trigger_price, y_min, y_max, price_top, price_height)
    if weekly_pullback_reclaim:
        flush_zone_top_price = max(trigger_price, ema21_value)
        flush_zone_bottom_price = min(recent_support, ema21_value)
        flush_zone_top_y = _price_y(flush_zone_top_price, y_min, y_max, price_top, price_height)
        flush_zone_bottom_y = _price_y(flush_zone_bottom_price, y_min, y_max, price_top, price_height)
        flush_zone_y = min(flush_zone_top_y, flush_zone_bottom_y)
        flush_zone_height = max(abs(flush_zone_bottom_y - flush_zone_top_y), 14.0)
        svg.append(
            f'<rect x="{left}" y="{flush_zone_y:.1f}" width="{plot_width}" height="{flush_zone_height:.1f}" fill="#f59e0b" opacity="0.09" rx="6" />'
        )
        svg.append(
            f'<text x="{left + 10}" y="{max(price_top + 18, flush_zone_y - 8):.1f}" fill="#fbbf24" font-size="12" font-family="Menlo, Consolas, monospace">Flush zone: EMA21 pressure inside weekly pullback</text>'
        )
    entry_zone_top_price = max(entry_price, ema8_value if above_trigger else entry_price * 1.005)
    entry_zone_bottom_price = min(entry_price, trigger_price if above_trigger else entry_price)
    entry_zone_top_y = _price_y(entry_zone_top_price, y_min, y_max, price_top, price_height)
    entry_zone_bottom_y = _price_y(entry_zone_bottom_price, y_min, y_max, price_top, price_height)
    entry_zone_y = min(entry_zone_top_y, entry_zone_bottom_y)
    entry_zone_height = max(abs(entry_zone_bottom_y - entry_zone_top_y), 10.0)
    svg.append(
        f'<rect x="{left}" y="{entry_zone_y:.1f}" width="{plot_width}" height="{entry_zone_height:.1f}" fill="#22c55e" opacity="0.08" />'
    )
    if weekly_pullback_reclaim:
        svg.append(
            f'<line x1="{left}" y1="{trigger_y:.1f}" x2="{left + plot_width}" y2="{trigger_y:.1f}" stroke="#fde047" stroke-width="4.0" opacity="0.12" />'
        )
    svg.append(
        f'<line x1="{left}" y1="{trigger_y:.1f}" x2="{left + plot_width}" y2="{trigger_y:.1f}" '
        f'stroke="#eab308" stroke-dasharray="6 4" stroke-width="1.5" />'
    )
    chart_label_specs.append((trigger_y + 4, "#eab308", f"{trigger_label} {trigger_price:.2f}"))
    if weekly_pullback_reclaim:
        chart_label_specs.append((trigger_y - 14, "#fde68a", "Buy only after reclaim back up"))

    entry_y = _price_y(entry_price, y_min, y_max, price_top, price_height)
    svg.append(
        f'<line x1="{left}" y1="{entry_y:.1f}" x2="{left + plot_width}" y2="{entry_y:.1f}" '
        f'stroke="{entry_color}" stroke-dasharray="2 6" stroke-width="1.2" />'
    )
    chart_label_specs.append((entry_y - 10, entry_color, f"{entry.entry_label or 'Entry ref'} {entry_price:.2f}"))

    if entry.secondary_entry_price is not None:
        secondary_price = entry.secondary_entry_price
        secondary_y = _price_y(secondary_price, y_min, y_max, price_top, price_height)
        svg.append(
            f'<line x1="{left}" y1="{secondary_y:.1f}" x2="{left + plot_width}" y2="{secondary_y:.1f}" '
            f'stroke="#94a3b8" stroke-dasharray="3 7" stroke-width="1.0" />'
        )
        chart_label_specs.append((secondary_y - 10, "#94a3b8", f"{entry.secondary_entry_label or 'Secondary entry'} {secondary_price:.2f}"))

    stop_y = _price_y(stop_price, y_min, y_max, price_top, price_height)
    invalid_zone_y = stop_y
    invalid_zone_height = max(price_top + price_height - stop_y, 12.0)
    svg.append(
        f'<rect x="{left}" y="{invalid_zone_y:.1f}" width="{plot_width}" height="{invalid_zone_height:.1f}" fill="#ef4444" opacity="0.08" />'
    )
    svg.append(
        f'<line x1="{left}" y1="{stop_y:.1f}" x2="{left + plot_width}" y2="{stop_y:.1f}" '
        f'stroke="{stop_color}" stroke-dasharray="8 4" stroke-width="1.2" />'
    )
    chart_label_specs.append((stop_y + 16, stop_color, f"{entry.stop_label or 'Stop ref'} {stop_price:.2f}"))
    chart_label_specs.append((entry_zone_y - 10, "#22c55e", "Entry zone"))
    if weekly_pullback_reclaim:
        svg.append(
            f'<text x="{left + 10}" y="{entry_zone_y + entry_zone_height + 18:.1f}" fill="#86efac" font-size="12" font-family="Menlo, Consolas, monospace">Do not buy while price is still falling into support</text>'
        )
    chart_label_specs.append((min(price_top + price_height - 8, stop_y + 34), "#ef4444", "Invalid below"))

    prior_50_y = _price_y(prior_50_high, y_min, y_max, price_top, price_height)
    svg.append(
        f'<line x1="{left}" y1="{prior_50_y:.1f}" x2="{left + plot_width}" y2="{prior_50_y:.1f}" '
        f'stroke="#14b8a6" stroke-dasharray="3 5" stroke-width="1.0" />'
    )
    chart_label_specs.append((prior_50_y + 4, "#14b8a6", f"50d pivot {prior_50_high:.2f}"))

    latest_y = _price_y(latest_close, y_min, y_max, price_top, price_height)
    svg.append(f'<circle cx="{x_values[-1]:.1f}" cy="{latest_y:.1f}" r="4" fill="#f8fafc" />')
    chart_label_specs.append((latest_y - 10, "#f8fafc", f"Close {latest_close:.2f}"))

    chart_label_specs.sort(key=lambda item: item[0])
    chart_label_positions = _spread_label_positions(
        [item[0] for item in chart_label_specs],
        minimum_gap=18.0,
        lower_bound=price_top + 16,
        upper_bound=price_top + price_height - 10,
    )
    for adjusted_y, (_, color, label_text) in zip(chart_label_positions, chart_label_specs):
        svg.append(
            f'<text x="{chart_label_x}" y="{adjusted_y:.1f}" fill="{color}" font-size="12" text-anchor="end" '
            f'font-family="Menlo, Consolas, monospace">{escape(label_text)}</text>'
        )

    rs_points = _polyline_points(rs_line, x_values, rs_min, rs_max, rs_top, rs_height)
    svg.append(f'<polyline points="{rs_points}" fill="none" stroke="#22c55e" stroke-width="2.1" />')
    for x, value, is_new_high, is_new_high_before_price in zip(
        x_values,
        rs_line,
        daily_new_high,
        daily_new_high_before_price,
    ):
        if pd.isna(value):
            continue
        y = rs_top + rs_height - ((float(value) - rs_min) / rs_range * rs_height)
        if bool(is_new_high):
            svg.append(f'<circle cx="{x:.1f}" cy="{y:.1f}" r="3.0" fill="#38bdf8" opacity="0.95" />')
        if bool(is_new_high_before_price):
            svg.append(f'<circle cx="{x:.1f}" cy="{y:.1f}" r="5.1" fill="none" stroke="#f8fafc" stroke-width="1.2" opacity="0.95" />')
    for step in range(4):
        y = rs_top + step * (rs_height / 3)
        value = rs_max - step * (rs_range / 3)
        svg.append(f'<line x1="{left}" y1="{y:.1f}" x2="{left + plot_width}" y2="{y:.1f}" stroke="#1e293b" stroke-width="1" />')
        svg.append(
            f'<text x="{left - 12}" y="{y + 4:.1f}" fill="#94a3b8" font-size="12" text-anchor="end" '
            f'font-family="Menlo, Consolas, monospace">{value:.3f}</text>'
        )

    svg.append(f'<text x="{left}" y="{volume_top - 12}" fill="#94a3b8" font-size="13" font-family="Menlo, Consolas, monospace">Volume</text>')
    svg.append(
        f'<text x="{left}" y="{rs_top - 12}" fill="#94a3b8" font-size="13" font-family="Menlo, Consolas, monospace">Relative Strength vs {escape(benchmark_ticker)} (daily line) | price-panel blue dots = historical RS new highs | ring = before price</text>'
    )

    rs_rating_text = f"{int(round(latest_rs_rating))}" if pd.notna(latest_rs_rating) else "n/a"
    rs_score_text = f"{float(latest_rs_score):.2f}" if pd.notna(latest_rs_score) else "n/a"
    rs_line_text = f"{latest_rs_value:.3f}" if pd.notna(latest_rs_value) else "n/a"
    weekly_status_suffix = (
        f" ({latest_weekly_bar_date.strftime('%Y-%m-%d')})" if latest_weekly_bar_date is not None else ""
    )
    trigger_status_text = "above" if above_trigger else "below"
    latest_ipo_vwap = float(latest["ipo_vwap"]) if pd.notna(latest["ipo_vwap"]) else latest_close
    above_ipo_vwap = latest_close > latest_ipo_vwap
    info_lines = [
        f"Close: {latest_close:.2f}",
        f"Trigger status: {trigger_status_text} {trigger_label.lower()}",
        f"RS line: {rs_line_text}",
        f"RS score: {rs_score_text}",
        f"RS Rating: {rs_rating_text}",
        f"Daily RS NH: {'yes' if latest_daily_rs_new_high else 'no'}",
        f"Daily RS NH before price: {'yes' if latest_daily_rs_new_high_before_price else 'no'}",
        f"Weekly RS NH: {'yes' if latest_weekly_rs_new_high else 'no'}{weekly_status_suffix}",
        f"Weekly RS NH before price: {'yes' if latest_weekly_rs_new_high_before_price else 'no'}",
        f"Weekly 8 EMA: {weekly_ema8_value:.2f}",
        f"IPO VWAP: {latest_ipo_vwap:.2f}",
        f"Recent EMA21 flush + reclaim: {'yes' if recent_ema21_flush else 'no'}",
        f"Entry ref: {entry_price:.2f}",
        f"Stop ref: {stop_price:.2f}",
        f"20d pivot: {prior_20_high:.2f}",
        f"50d pivot: {prior_50_high:.2f}",
        f"Rel volume 20d: {relative_volume20:.2f}",
        f"Avg volume 20d: {avg_volume20:,.0f}",
        f"ADR 20d: {adr_percent:.2f}%",
        f"Above EMA 8: {'yes' if latest_close > float(latest['ema8']) else 'no'}",
        f"Above EMA 21: {'yes' if latest_close > float(latest['ema21']) else 'no'}",
        f"Above Weekly 8 EMA: {'yes' if latest_close > weekly_ema8_value else 'no'}",
        f"Above IPO VWAP: {'yes' if above_ipo_vwap else 'no'}",
        f"Above SMA 50: {'yes' if latest_close > float(latest['sma50']) else 'no'}",
        f"Gap zones: {len(gap_zones)} total | open: {len(open_gap_zones)} | visible: {len(visible_gap_zones)}",
    ]
    if entry.secondary_entry_price is not None:
        info_lines.append(f"Secondary entry: {entry.secondary_entry_price:.2f}")
    if entry.entry_timeframe:
        info_lines.append(f"Entry timeframe: {entry.entry_timeframe}")
    if entry.stop_timeframe:
        info_lines.append(f"Stop timeframe: {entry.stop_timeframe}")
    if open_gap_zones:
        latest_open_gap = open_gap_zones[-1]
        info_lines.append(
            f"Latest open gap: {latest_open_gap.direction} {latest_open_gap.remaining_lower_price:.2f}-{latest_open_gap.remaining_upper_price:.2f}"
        )
    for index, line in enumerate(info_lines):
        svg.append(
            f'<text x="{right_panel_x}" y="{250 + index * 23}" fill="#e2e8f0" font-size="14" font-family="Menlo, Consolas, monospace">{escape(line)}</text>'
        )

    sector_value = (sector_snapshot.get("sector") or "").strip().lower()
    show_sector_snapshot = sector_value not in {"", "unknown", "n/a"}
    cursor_y = 250 + len(info_lines) * 23 + 22
    if show_sector_snapshot:
        svg.append(
            f'<text x="{right_panel_x}" y="{cursor_y}" fill="#38bdf8" font-size="14" font-family="Menlo, Consolas, monospace">Sector Snapshot</text>'
        )
        sector_lines = [
            f"Sector: {sector_snapshot['sector']} | ETF: {sector_snapshot['etf']}",
            f"Industry: {sector_snapshot['industry']}",
            f"Sector 5d: {sector_snapshot['ret5']} | 20d: {sector_snapshot['ret20']}",
            f"Sector trend: {sector_snapshot['trend']}",
        ]
        for index, line in enumerate(sector_lines):
            svg.append(
                f'<text x="{right_panel_x}" y="{cursor_y + 22 + index * 18}" fill="#f8fafc" font-size="13" font-family="Menlo, Consolas, monospace">{escape(line)}</text>'
            )
        cursor_y += 22 + len(sector_lines) * 18 + 28

    entry_header_y = cursor_y
    svg.append(
        f'<text x="{right_panel_x}" y="{entry_header_y}" fill="{entry_color}" font-size="14" font-family="Menlo, Consolas, monospace">Entry Guide</text>'
    )
    for index, line in enumerate(entry_lines):
        svg.append(
            f'<text x="{right_panel_x}" y="{entry_header_y + 22 + index * 20}" fill="#f8fafc" font-size="13" font-family="Menlo, Consolas, monospace">{escape(line)}</text>'
        )
    entry_footer_y = entry_header_y + 22 + len(entry_lines) * 20
    svg.append(
        f'<text x="{right_panel_x}" y="{entry_footer_y + 6}" fill="#cbd5e1" font-size="13" font-family="Menlo, Consolas, monospace">{escape(rr_comment)}</text>'
    )

    risk_header_y = entry_footer_y + 34
    svg.append(
        f'<text x="{right_panel_x}" y="{risk_header_y}" fill="{stop_color}" font-size="14" font-family="Menlo, Consolas, monospace">Risk Guide</text>'
    )
    for index, line in enumerate(stop_lines):
        svg.append(
            f'<text x="{right_panel_x}" y="{risk_header_y + 22 + index * 20}" fill="#f8fafc" font-size="13" font-family="Menlo, Consolas, monospace">{escape(line)}</text>'
        )

    cursor_y = risk_header_y + 22 + len(stop_lines) * 20 + 24
    cursor_y += _text_block(svg, f"Summary: {entry.summary}", right_panel_x, int(cursor_y), "#f8fafc", 14, width=40) * 20
    cursor_y += 12
    _text_block(svg, f"Master note: {entry.master_note}", right_panel_x, int(cursor_y), "#94a3b8", 13, width=42)

    svg.append("</svg>")
    output_file.write_text("\n".join(svg))
    return output_file


def render_watchlist_index(
    entries: list[WatchlistEntry],
    output_path: str | Path,
    chart_dir_name: str = "charts",
    benchmark_ticker: str = "SPY",
) -> Path:
    output_file = Path(output_path)
    output_file.parent.mkdir(parents=True, exist_ok=True)
    cards: list[str] = []
    for entry in entries:
        cards.append(
            f"""
            <article class="card">
              <h2>{escape(entry.ticker)} <span>{escape(entry.setup_label)}</span></h2>
              <p>{escape(entry.summary)}</p>
              <img src="{chart_dir_name}/{escape(entry.ticker)}.svg" alt="{escape(entry.ticker)} chart" loading="lazy" />
              <pre>{escape(entry.master_note)}</pre>
            </article>
            """
        )
    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Trade Master Watchlist</title>
  <style>
    :root {{
      color-scheme: dark;
      --bg: #06101d;
      --panel: rgba(15, 23, 42, 0.88);
      --border: rgba(148, 163, 184, 0.18);
      --text: #e2e8f0;
      --muted: #94a3b8;
      --accent: #38bdf8;
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      font-family: "SF Mono", "Menlo", "Consolas", monospace;
      background:
        radial-gradient(circle at top left, rgba(56, 189, 248, 0.16), transparent 32%),
        radial-gradient(circle at top right, rgba(34, 197, 94, 0.10), transparent 28%),
        linear-gradient(180deg, #050c16 0%, var(--bg) 100%);
      color: var(--text);
    }}
    main {{
      width: min(1440px, calc(100vw - 32px));
      margin: 0 auto;
      padding: 32px 0 64px;
    }}
    header {{
      margin-bottom: 28px;
      padding: 24px;
      border: 1px solid var(--border);
      border-radius: 22px;
      background: rgba(7, 16, 30, 0.82);
      backdrop-filter: blur(12px);
    }}
    h1 {{
      margin: 0 0 8px;
      font-size: clamp(28px, 4vw, 42px);
    }}
    header p {{
      margin: 0;
      color: var(--muted);
      line-height: 1.6;
      max-width: 920px;
    }}
    .grid {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(340px, 1fr));
      gap: 18px;
    }}
    .card {{
      margin: 0;
      padding: 18px;
      border: 1px solid var(--border);
      border-radius: 20px;
      background: var(--panel);
      box-shadow: 0 24px 60px rgba(2, 8, 23, 0.35);
    }}
    .card h2 {{
      margin: 0 0 10px;
      display: flex;
      gap: 10px;
      flex-wrap: wrap;
      align-items: baseline;
      font-size: 22px;
    }}
    .card h2 span {{
      color: var(--accent);
      font-size: 13px;
      letter-spacing: 0.06em;
      text-transform: uppercase;
    }}
    .card p {{
      margin: 0 0 14px;
      color: var(--text);
      line-height: 1.55;
    }}
    .card img {{
      width: 100%;
      border-radius: 16px;
      border: 1px solid rgba(148, 163, 184, 0.14);
      background: #07101e;
      display: block;
    }}
    .card pre {{
      margin: 14px 0 0;
      white-space: pre-wrap;
      line-height: 1.5;
      color: var(--muted);
      font-size: 12px;
    }}
  </style>
</head>
<body>
  <main>
    <header>
      <h1>Trade Master Watchlist</h1>
      <p>Daily setup charts generated from live market data. Each chart shows candles, EMA 8, EMA 21, a real weekly 8 EMA, IPO VWAP, SMA 50, SMA 200, reclaim and pivot context, volume, a daily RS line versus {escape(benchmark_ticker)}, an approximate RS Rating, RS new-high markers on both the RS panel and price pane, and explicit entry or stop guides when the watchlist provides them.</p>
    </header>
    <section class="grid">
      {"".join(cards)}
    </section>
  </main>
</body>
</html>
"""
    output_file.write_text(html)
    return output_file


def _read_svg_parts(path: Path) -> tuple[float, float, str]:
    text = path.read_text()
    size_match = SVG_SIZE_RE.search(text)
    if not size_match:
        raise ValueError(f"Could not read width/height from {path}")
    width = float(size_match.group("width"))
    height = float(size_match.group("height"))
    start = text.find(">") + 1
    end = text.rfind("</svg>")
    if start <= 0 or end < 0:
        raise ValueError(f"Could not extract SVG body from {path}")
    return width, height, text[start:end].strip()


def render_split_montage_pages(
    chart_paths: list[Path],
    output_dir: Path,
    charts_per_page: int,
    columns: int,
    card_width: int,
    title_prefix: str,
) -> list[Path]:
    if not chart_paths:
        return []

    base_width, base_height, _ = _read_svg_parts(chart_paths[0])
    scale = card_width / base_width
    card_height = math.ceil(base_height * scale)
    gap = 20
    padding = 28
    header_height = 96
    pages: list[Path] = []

    for offset in range(0, len(chart_paths), charts_per_page):
        chunk = chart_paths[offset : offset + charts_per_page]
        rows = math.ceil(len(chunk) / columns)
        total_width = padding * 2 + columns * card_width + (columns - 1) * gap
        total_height = header_height + padding + rows * card_height + (rows - 1) * gap + padding
        page_num = offset // charts_per_page + 1
        parts = [
            f'<svg xmlns="http://www.w3.org/2000/svg" width="{total_width}" height="{total_height}" viewBox="0 0 {total_width} {total_height}">',
            '<defs>',
            '  <linearGradient id="bg" x1="0" y1="0" x2="0" y2="1">',
            '    <stop offset="0%" stop-color="#040b14" />',
            '    <stop offset="100%" stop-color="#06101d" />',
            '  </linearGradient>',
            '</defs>',
            f'<rect width="{total_width}" height="{total_height}" fill="url(#bg)" />',
            f'<text x="{padding}" y="42" fill="#f8fafc" font-size="34" font-family="Menlo, Consolas, monospace">{escape(title_prefix)} · Page {page_num}</text>',
            f'<text x="{padding}" y="68" fill="#94a3b8" font-size="16" font-family="Menlo, Consolas, monospace">{len(chunk)} tickers | split montage page</text>',
        ]
        for index, path in enumerate(chunk):
            _, _, body = _read_svg_parts(path)
            row = index // columns
            column = index % columns
            x = padding + column * (card_width + gap)
            y = header_height + row * (card_height + gap)
            parts.append(f'<g transform="translate({x},{y}) scale({scale:.8f})">{body}</g>')
        parts.append("</svg>")
        page_path = output_dir / f"watchlist_page_{page_num}.svg"
        page_path.write_text("\n".join(parts))
        pages.append(page_path)
    return pages


def load_watchlist_entries(args: argparse.Namespace) -> list[WatchlistEntry]:
    if args.watchlist_file:
        raw_entries = json.loads(Path(args.watchlist_file).read_text())
        entries: list[WatchlistEntry] = []
        for raw_entry in raw_entries:
            entry = dict(raw_entry)
            for key in ("trigger_price", "entry_price", "secondary_entry_price", "stop_price"):
                entry[key] = _coerce_optional_float(entry.get(key))
            entries.append(WatchlistEntry(**entry))
        return entries

    if args.ticker_file:
        tickers = [line.strip().upper() for line in Path(args.ticker_file).read_text().splitlines() if line.strip()]
    else:
        tickers = [ticker.upper() for ticker in args.tickers]
    return [
        WatchlistEntry(
            ticker=ticker,
            setup_label="Unspecified setup",
            summary="Ticker provided without structured trade-master notes.",
            master_note=f"${ticker} generated from ticker-only input.",
        )
        for ticker in tickers
    ]


def to_summary(entry: WatchlistEntry, close: float, trigger: float | None, trigger_status: str) -> dict[str, object]:
    return {
        "ticker": entry.ticker,
        "setup_label": entry.setup_label,
        "close": round(close, 2),
        "trigger_price": round(trigger, 2) if trigger is not None else None,
        "trigger_status": trigger_status,
        "entry_price": round(entry.entry_price, 2) if entry.entry_price is not None else None,
        "stop_price": round(entry.stop_price, 2) if entry.stop_price is not None else None,
    }


def main() -> int:
    args = parse_args()
    entries = load_watchlist_entries(args)
    output_dir = Path(args.output_dir)
    charts_dir = output_dir / "charts"
    output_dir.mkdir(parents=True, exist_ok=True)
    charts_dir.mkdir(parents=True, exist_ok=True)

    benchmark_history = fetch_history(args.benchmark, period=args.period)
    profile_cache: dict[str, dict[str, str]] = {}
    sector_history_cache: dict[str, pd.DataFrame] = {}
    generated: list[WatchlistEntry] = []
    summaries: list[dict[str, object]] = []
    failed: dict[str, str] = {}

    for entry in entries:
        try:
            history = fetch_history(entry.ticker, period=args.period)
            try:
                ipo_history = fetch_history(entry.ticker, period="max")
            except Exception:
                ipo_history = history
            if entry.ticker not in profile_cache:
                profile_cache[entry.ticker] = fetch_company_profile(entry.ticker)
            profile = profile_cache[entry.ticker]
            sector_snapshot = build_sector_snapshot(
                profile=profile,
                period=args.period,
                sector_history_cache=sector_history_cache,
            )
            chart_path = render_watchlist_chart(
                entry=entry,
                history=history,
                benchmark_history=benchmark_history,
                output_path=charts_dir / f"{entry.ticker}.svg",
                lookback=args.lookback,
                benchmark_ticker=args.benchmark,
                profile=profile,
                sector_snapshot=sector_snapshot,
                ipo_history=ipo_history,
            )
            chart_text = chart_path.read_text()
            close_match = re.search(r"Close: ([0-9.]+)", chart_text)
            trigger_match = re.search(r"Watch level ([0-9.]+)|Break above ([0-9.]+)|Close above ([0-9.]+)|Resistance ([0-9.]+)|Clear area ([0-9.]+)", chart_text)
            trigger_status_match = re.search(r"Trigger status: ([^<]+)", chart_text)
            generated.append(entry)
            summaries.append(
                to_summary(
                    entry,
                    close=float(close_match.group(1)) if close_match else float("nan"),
                    trigger=float(next(group for group in trigger_match.groups() if group is not None)) if trigger_match else entry.trigger_price,
                    trigger_status=trigger_status_match.group(1) if trigger_status_match else "unknown",
                )
            )
            print(f"[done] {entry.ticker}")
        except Exception as exc:
            failed[entry.ticker] = str(exc)
            print(f"[warn] {entry.ticker}: {exc}", file=sys.stderr)

    render_watchlist_index(generated, output_dir / "index.html", benchmark_ticker=args.benchmark)
    montage_pages: list[str] = []
    if args.split_pages > 0:
        chart_paths = [charts_dir / f"{entry.ticker}.svg" for entry in generated]
        montage_pages = [
            str(path)
            for path in render_split_montage_pages(
                chart_paths=chart_paths,
                output_dir=output_dir,
                charts_per_page=args.split_pages,
                columns=args.montage_columns,
                card_width=args.card_width,
                title_prefix="Trade Master Watchlist",
            )
        ]

    summary = {
        "generated_count": len(generated),
        "generated_tickers": [entry.ticker for entry in generated],
        "chart_pages": montage_pages,
        "ticker_summaries": summaries,
        "failed": failed,
    }
    (output_dir / "run_summary.json").write_text(json.dumps(summary, indent=2))
    print(f"Wrote watchlist output to {output_dir}")
    return 0 if not failed else 1


if __name__ == "__main__":
    raise SystemExit(main())
