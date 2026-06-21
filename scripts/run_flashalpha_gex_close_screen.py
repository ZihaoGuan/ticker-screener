#!/usr/bin/env python3
from __future__ import annotations

import argparse
import datetime as dt
import json
from pathlib import Path
import sys


PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.artifact_paths import build_screener_artifact_paths
from src.config import load_app_config, today_label
from src.flashalpha_gex import fetch_gex_snapshot, summarize_gex_payload
from src.webapp.config import load_webapp_config
from src.webapp.services.screener_history_service import ScreenerHistoryService


STRATEGY_ID = "flashalpha_gex_close"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fetch and persist FlashAlpha GEX close snapshot.")
    parser.add_argument("--config", default=str(PROJECT_ROOT / "config" / "market_config.json"))
    parser.add_argument("--date-label", help="Override artifact date label (YYYY-MM-DD).")
    parser.add_argument("--as-of-date", help="Label the snapshot with a trading date (YYYY-MM-DD).")
    parser.add_argument("--symbol", help="Underlying symbol. Defaults to benchmark ticker from config.")
    parser.add_argument("--expiration", help="Optional expiry filter (YYYY-MM-DD).")
    parser.add_argument("--min-oi", type=int, default=0, help="Optional minimum OI filter passed to FlashAlpha.")
    return parser.parse_args()


def _write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def main() -> int:
    args = parse_args()
    config = load_app_config(args.config)
    as_of_date = dt.date.fromisoformat(args.as_of_date) if args.as_of_date else dt.date.today()
    date_label = args.date_label or today_label(as_of_date)
    symbol = str(args.symbol or config.benchmark_ticker).strip().upper()

    api_payload = fetch_gex_snapshot(symbol=symbol, expiration=args.expiration, min_oi=max(0, int(args.min_oi or 0)))
    summary_snapshot = summarize_gex_payload(api_payload)
    artifact_paths = build_screener_artifact_paths(PROJECT_ROOT / "artifacts", strategy_id=STRATEGY_ID, date_label=date_label)

    raw_payload = {
        "strategy_id": STRATEGY_ID,
        "date_label": date_label,
        "as_of_date": as_of_date.isoformat(),
        "symbol": symbol,
        "summary": summary_snapshot,
        "flashalpha_response": api_payload,
        "hits": [],
        "failed_tickers": [],
    }
    summary_payload = {
        "strategy_id": STRATEGY_ID,
        "date_label": date_label,
        "as_of_date": as_of_date.isoformat(),
        "total_tickers": 1,
        "passed_tickers": 0,
        "failed_tickers": 0,
        "source": "flashalpha",
        "raw_results_file": str(artifact_paths.raw_results_path),
        "watchlist_file": str(artifact_paths.watchlist_path),
        "ticker": summary_snapshot.get("ticker") or symbol,
        "spot": summary_snapshot.get("spot"),
        "net_gex": summary_snapshot.get("net_gex"),
        "gex_regime": summary_snapshot.get("gex_regime"),
        "gex_label": summary_snapshot.get("gex_label"),
        "gamma_flip": summary_snapshot.get("gamma_flip"),
        "distance_to_flip_pct": summary_snapshot.get("distance_to_flip_pct"),
        "call_wall": summary_snapshot.get("call_wall"),
        "put_wall": summary_snapshot.get("put_wall"),
        "atm_pin_strike": summary_snapshot.get("atm_pin_strike"),
        "top_net_gex_strike": summary_snapshot.get("top_net_gex_strike"),
        "put_call_oi_ratio": summary_snapshot.get("put_call_oi_ratio"),
        "strike_count": summary_snapshot.get("strike_count"),
        "summary": summary_snapshot.get("summary"),
        "methodology": summary_snapshot.get("methodology"),
        "api_as_of": summary_snapshot.get("as_of"),
    }

    _write_json(artifact_paths.raw_results_path, raw_payload)
    _write_json(artifact_paths.watchlist_path, [])
    _write_json(artifact_paths.summary_path, summary_payload)

    print(f"Wrote raw results to {artifact_paths.raw_results_path}")
    print(f"Wrote watchlist to {artifact_paths.watchlist_path}")
    print(f"Wrote run summary to {artifact_paths.summary_path}")

    webapp_config = load_webapp_config()
    history_service = ScreenerHistoryService(database_url=webapp_config.database_url, artifacts_dir=webapp_config.artifacts_dir)
    if history_service.is_configured():
        persisted_run_id = history_service.persist_metric_snapshot_run(
            strategy_id=STRATEGY_ID,
            options={"as_of_date": as_of_date.isoformat(), "source": "flashalpha"},
            summary_payload=summary_payload,
            raw_artifact_path=str(artifact_paths.raw_results_path),
            watchlist_artifact_path=str(artifact_paths.watchlist_path),
            job_run_id=None,
        )
        if persisted_run_id is not None:
            print(f"Persisted screen run id={persisted_run_id}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
