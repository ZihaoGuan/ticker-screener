#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path
import time


LOW_CAP_THRESHOLD_DEFAULT = 1_000_000_000
REQUEST_DELAY_SECONDS_DEFAULT = 1.0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Filter screener hits below a market-cap threshold and update an auto exclude file."
    )
    parser.add_argument("--raw-file", required=True, help="Raw screener results JSON file.")
    parser.add_argument("--watchlist-file", required=True, help="Rendered watchlist JSON file.")
    parser.add_argument("--summary-file", required=True, help="Run summary JSON file.")
    parser.add_argument("--exclude-file", required=True, help="Pipeline-specific auto exclude file to update.")
    parser.add_argument("--pipeline-name", default="", help="Optional label to include in file comments.")
    parser.add_argument("--threshold", type=float, default=LOW_CAP_THRESHOLD_DEFAULT, help="Minimum market cap in USD.")
    parser.add_argument("--delay-seconds", type=float, default=REQUEST_DELAY_SECONDS_DEFAULT, help="Delay between yfinance lookups.")
    return parser.parse_args()


def _load_json(path: Path) -> object:
    return json.loads(path.read_text(encoding="utf-8"))


def _write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def _extract_ticker(item: object) -> str:
    if not isinstance(item, dict):
        return ""
    ticker = str(item.get("ticker", "")).strip().upper()
    return ticker


def _extract_watchlist_tickers(payload: object) -> list[str]:
    if not isinstance(payload, list):
        return []
    seen: set[str] = set()
    tickers: list[str] = []
    for item in payload:
        ticker = _extract_ticker(item)
        if not ticker or ticker in seen:
            continue
        seen.add(ticker)
        tickers.append(ticker)
    return tickers


def _is_429_error(exc: Exception) -> bool:
    text = str(exc).lower()
    return "429" in text or "too many requests" in text


def _is_timeout_error(exc: Exception) -> bool:
    text = str(exc).lower()
    return "timed out" in text or "timeout" in text


def _fetch_market_caps(tickers: list[str], delay_seconds: float) -> tuple[dict[str, float], bool]:
    import yfinance as yf

    market_caps: dict[str, float] = {}
    for index, ticker in enumerate(tickers):
        try:
            ticker_client = yf.Ticker(ticker)
            market_cap = None
            fast_info = getattr(ticker_client, "fast_info", None)
            if fast_info:
                try:
                    market_cap = fast_info.get("market_cap")
                except Exception:
                    market_cap = None
            if market_cap is None:
                info = ticker_client.info
                market_cap = info.get("marketCap")
            if market_cap is not None:
                market_caps[ticker] = float(market_cap)
        except Exception as exc:
            if _is_429_error(exc):
                print(f"Encountered yfinance 429 while checking {ticker}; skipping low market-cap filter step.")
                return {}, True
            if _is_timeout_error(exc):
                print(f"Encountered yfinance timeout while checking {ticker}; skipping low market-cap filter step.")
                return {}, True
            print(f"Market-cap lookup failed for {ticker}: {exc}")
        if index < len(tickers) - 1 and delay_seconds > 0:
            time.sleep(delay_seconds)
    return market_caps, False


def _filter_watchlist(payload: object, low_cap_tickers: set[str]) -> object:
    if not isinstance(payload, list):
        return payload
    return [item for item in payload if _extract_ticker(item) not in low_cap_tickers]


def _filter_raw_payload(payload: object, low_cap_tickers: set[str], market_caps: dict[str, float], threshold: float) -> object:
    if isinstance(payload, list):
        return [item for item in payload if _extract_ticker(item) not in low_cap_tickers]
    if isinstance(payload, dict):
        filtered = dict(payload)
        hits = filtered.get("hits")
        if isinstance(hits, list):
            filtered["hits"] = [item for item in hits if _extract_ticker(item) not in low_cap_tickers]
            filtered["passed_tickers"] = len(filtered["hits"])
        filtered["low_market_cap_filtered"] = [
            {
                "ticker": ticker,
                "market_cap": market_caps[ticker],
                "threshold": threshold,
            }
            for ticker in sorted(low_cap_tickers)
            if ticker in market_caps
        ]
        return filtered
    return payload


def _update_summary(payload: object, low_cap_tickers: set[str], market_caps: dict[str, float], threshold: float, surviving_count: int) -> object:
    if not isinstance(payload, dict):
        return payload
    updated = dict(payload)
    updated["passed_tickers"] = surviving_count
    updated["low_market_cap_filtered"] = [
        {
            "ticker": ticker,
            "market_cap": market_caps[ticker],
            "threshold": threshold,
        }
        for ticker in sorted(low_cap_tickers)
        if ticker in market_caps
    ]
    return updated


def _load_existing_excludes(path: Path) -> set[str]:
    if not path.exists():
        return set()
    existing: set[str] = set()
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.split("#", 1)[0].strip()
        if line:
            existing.add(line.upper())
    return existing


def _write_excludes(path: Path, tickers: set[str], pipeline_name: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    header = "# Auto-generated low market-cap exclusions"
    if pipeline_name:
        header += f" for {pipeline_name}"
    lines = [header]
    lines.extend(sorted(tickers))
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> int:
    args = parse_args()
    raw_path = Path(args.raw_file)
    watchlist_path = Path(args.watchlist_file)
    summary_path = Path(args.summary_file)
    exclude_path = Path(args.exclude_file)

    raw_payload = _load_json(raw_path)
    watchlist_payload = _load_json(watchlist_path)
    summary_payload = _load_json(summary_path)

    tickers = _extract_watchlist_tickers(watchlist_payload)
    if not tickers:
        print("No watchlist tickers found; skipping low market-cap filter.")
        return 0

    market_caps, hit_429 = _fetch_market_caps(tickers, delay_seconds=args.delay_seconds)
    if hit_429:
        return 0

    low_cap_tickers = {
        ticker
        for ticker, market_cap in market_caps.items()
        if market_cap < float(args.threshold)
    }
    if not low_cap_tickers:
        print("No low market-cap watchlist tickers found.")
        return 0

    filtered_watchlist = _filter_watchlist(watchlist_payload, low_cap_tickers)
    filtered_raw = _filter_raw_payload(raw_payload, low_cap_tickers, market_caps, float(args.threshold))
    surviving_count = len(filtered_watchlist) if isinstance(filtered_watchlist, list) else 0
    filtered_summary = _update_summary(summary_payload, low_cap_tickers, market_caps, float(args.threshold), surviving_count)

    _write_json(raw_path, filtered_raw)
    _write_json(watchlist_path, filtered_watchlist)
    _write_json(summary_path, filtered_summary)

    existing = _load_existing_excludes(exclude_path)
    _write_excludes(exclude_path, existing.union(low_cap_tickers), args.pipeline_name)

    print(f"Filtered out {len(low_cap_tickers)} low market-cap tickers: {', '.join(sorted(low_cap_tickers))}")
    print(f"Updated auto exclude file: {exclude_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
