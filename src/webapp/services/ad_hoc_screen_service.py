from __future__ import annotations

import datetime as dt
import time
from typing import Any

from src.config import AppConfig, load_app_config
from src.cookstock_bridge import use_prefetched_market_data
from src.market_data_access import load_many_ticker_windows, load_ticker_metadata_map, resolve_database_url
from src.screener_catalog import build_screener_catalog
from src.screener_engine import ScreenerInputBundle, resolve_max_trading_days


class AdHocScreenService:
    def __init__(
        self,
        *,
        app_config: AppConfig | None = None,
        database_url: str | None = None,
        timeout_seconds: float = 15.0,
    ) -> None:
        self.app_config = app_config or load_app_config()
        self.database_url = resolve_database_url(database_url)
        self.timeout_seconds = float(timeout_seconds)
        self.catalog = build_screener_catalog(self.app_config)

    def run(
        self,
        *,
        ticker: str,
        as_of_date: dt.date,
        screener_ids: list[str],
    ) -> dict[str, object]:
        normalized_ticker = str(ticker).strip().upper()
        if not normalized_ticker:
            raise ValueError("Ticker is required.")
        if not screener_ids:
            raise ValueError("At least one screener is required.")
        if not self.database_url:
            raise ValueError("Ad-hoc screening requires TICKER_SCREENER_DATABASE_URL.")

        specs: list[Any] = []
        unknown: list[str] = []
        for screener_id in screener_ids:
            spec = self.catalog.get(str(screener_id).strip())
            if spec is None:
                unknown.append(str(screener_id))
            else:
                specs.append(spec)
        if unknown:
            raise ValueError(f"Unknown screener id(s): {', '.join(sorted(unknown))}")

        trading_days_needed = resolve_max_trading_days(specs)
        frame_map = load_many_ticker_windows(
            [normalized_ticker, self.app_config.benchmark_ticker],
            as_of_date,
            trading_days_needed,
            database_url=self.database_url,
        )
        bars = frame_map.get(normalized_ticker)
        if bars is None or getattr(bars, "empty", False):
            raise ValueError(f"No daily_bars coverage for {normalized_ticker} on or before {as_of_date.isoformat()}.")

        benchmark_ticker = self.app_config.benchmark_ticker.upper()
        benchmark_bars = frame_map.get(benchmark_ticker)
        if benchmark_bars is None or getattr(benchmark_bars, "empty", False):
            raise ValueError(f"No benchmark daily_bars coverage for {benchmark_ticker}.")

        metadata_map = load_ticker_metadata_map([normalized_ticker], database_url=self.database_url)
        bundle = ScreenerInputBundle(
            ticker=normalized_ticker,
            as_of_date=as_of_date,
            bars=bars,
            benchmark_bars=benchmark_bars,
            metadata=metadata_map.get(normalized_ticker, {"ticker": normalized_ticker}),
            extras={"config": self.app_config},
        )

        started = time.perf_counter()
        screener_results: list[dict[str, object]] = []
        with use_prefetched_market_data(
            ticker_frames={normalized_ticker: bars},
            benchmark_frames={benchmark_ticker: benchmark_bars},
        ):
            for spec in specs:
                elapsed_ms = round((time.perf_counter() - started) * 1000, 2)
                if elapsed_ms >= self.timeout_seconds * 1000:
                    screener_results.append(
                        {
                            "id": spec.id,
                            "passed": False,
                            "error": f"Timed out after {self.timeout_seconds:.1f}s.",
                            "timing_ms": 0.0,
                            "metrics": {},
                            "reasons": [],
                            "hit": None,
                        }
                    )
                    continue
                before = time.perf_counter()
                evaluation = spec.evaluator(bundle) if spec.evaluator is not None else None
                timing_ms = round((time.perf_counter() - before) * 1000, 2)
                if evaluation is None:
                    screener_results.append(
                        {
                            "id": spec.id,
                            "passed": False,
                            "error": "Screener evaluator is not configured.",
                            "timing_ms": timing_ms,
                            "metrics": {},
                            "reasons": [],
                            "hit": None,
                        }
                    )
                    continue
                screener_results.append(
                    {
                        "id": spec.id,
                        "passed": evaluation.passed,
                        "error": evaluation.error,
                        "timing_ms": timing_ms,
                        "metrics": dict(evaluation.metrics),
                        "reasons": list(evaluation.reasons),
                        "hit": dict(evaluation.hit) if evaluation.hit is not None else None,
                    }
                )

        total_elapsed_ms = round((time.perf_counter() - started) * 1000, 2)
        passed_count = sum(1 for item in screener_results if item["passed"])
        return {
            "ticker": normalized_ticker,
            "as_of_date": as_of_date.isoformat(),
            "screeners": screener_results,
            "timing": {
                "total_ms": total_elapsed_ms,
                "market_data_source": "postgres_prefetch",
                "market_data_tickers_loaded": sorted(frame_map.keys()),
                "trading_days_requested": trading_days_needed,
            },
            "summary": {
                "requested_screener_count": len(specs),
                "passed_screener_count": passed_count,
                "failed_screener_count": len(specs) - passed_count,
            },
        }
