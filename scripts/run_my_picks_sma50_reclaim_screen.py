#!/usr/bin/env python3
from __future__ import annotations

import argparse
import datetime as dt
import json
import os
from pathlib import Path
from urllib.parse import quote
import sys


PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scripts._screen_run_persistence import persist_screen_run_artifacts_if_configured

from src.artifact_paths import build_screener_artifact_paths, watchlist_stem_from_path
from src.config import today_label
from src.market_data_access import resolve_database_url
from src.my_picks_sma50_reclaim_screen import run_my_picks_sma50_reclaim_screen
from src.my_picks_sma50_reclaim_watchlist_builder import build_my_picks_sma50_reclaim_watchlist
from src.webapp.services.discord_notification_service import DiscordNotificationService


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the My Picks 50 SMA reclaim screen.")
    parser.add_argument("--date-label", help="Override artifact date label (YYYY-MM-DD).")
    parser.add_argument("--as-of-date", help="Historical as-of date for replay mode (YYYY-MM-DD).")
    parser.add_argument(
        "--market-data-source",
        choices=("internet", "database-first"),
        default=os.environ.get("TICKER_SCREENER_MARKET_DATA_SOURCE", "database-first"),
        help="Prefer Postgres daily bars first or pull directly from the internet.",
    )
    parser.add_argument("--tickers", nargs="+", help="Optional explicit ticker list instead of current My Picks tickers.")
    return parser.parse_args()


def _write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def _build_watchlist_url(*, app_base_url: str, watchlist_file: str) -> str:
    stem = watchlist_stem_from_path(watchlist_file)
    if not stem:
        return ""
    base = str(app_base_url or "").strip().rstrip("/")
    if not base:
        return ""
    return f"{base}/watchlists?stem={quote(stem)}"


def _notify_discord_hits(
    *,
    as_of_date: dt.date,
    result_payload: dict[str, object],
    watchlist_file: str,
) -> bool:
    hits = list(result_payload.get("hits") or [])
    if not hits:
        return False
    service = DiscordNotificationService(project_root=PROJECT_ROOT)
    settings = service.get_settings()
    app_base_url = str(settings.get("effective_app_base_url") or "").strip()
    watchlist_url = _build_watchlist_url(app_base_url=app_base_url, watchlist_file=watchlist_file)

    lines = [f"My Picks 50 SMA reclaim alert: {len(hits)} hit(s) for {as_of_date.isoformat()}"]
    for hit in hits[:20]:
        if not isinstance(hit, dict):
            continue
        ticker = str(hit.get("ticker") or "").strip().upper()
        price = hit.get("current_price")
        sma50 = hit.get("sma50")
        ema9 = hit.get("ema9")
        ema21 = hit.get("ema21")
        if not ticker:
            continue
        try:
            lines.append(
                f"- {ticker}: close {float(price):.2f}, SMA50 {float(sma50):.2f}, EMA9 {float(ema9):.2f}, EMA21 {float(ema21):.2f}"
            )
        except (TypeError, ValueError):
            lines.append(f"- {ticker}")
    if len(hits) > 20:
        lines.append(f"...and {len(hits) - 20} more.")
    if watchlist_url:
        lines.append(f"Open: {watchlist_url}")
    return service.send_message("\n".join(lines))


def main() -> int:
    args = parse_args()
    os.environ.setdefault("MPLBACKEND", "Agg")
    os.environ.setdefault("MPLCONFIGDIR", "/tmp/mpl")

    as_of_date = dt.date.fromisoformat(args.as_of_date) if args.as_of_date else dt.date.today()
    date_label = args.date_label or today_label(as_of_date)
    database_url = resolve_database_url()
    result = run_my_picks_sma50_reclaim_screen(
        as_of_date=as_of_date,
        market_data_source=args.market_data_source,
        database_url=database_url,
        tickers=args.tickers,
    )
    watchlist = build_my_picks_sma50_reclaim_watchlist(result.hits)
    artifact_paths = build_screener_artifact_paths(PROJECT_ROOT / "artifacts", strategy_id="my_picks_sma50_reclaim", date_label=date_label)

    _write_json(artifact_paths.raw_results_path, result.to_dict())
    _write_json(artifact_paths.watchlist_path, watchlist)
    summary_payload = {
        "strategy_id": "my_picks_sma50_reclaim",
        "date_label": date_label,
        "as_of_date": as_of_date.isoformat(),
        "signal_profile": "my_picks_sma50_reclaim",
        "market_data_source": args.market_data_source,
        "total_tickers": result.total_tickers,
        "passed_tickers": result.passed_tickers,
        "failed_tickers": result.failed_tickers,
        "raw_results_file": str(artifact_paths.raw_results_path),
        "watchlist_file": str(artifact_paths.watchlist_path),
    }
    _write_json(artifact_paths.summary_path, summary_payload)

    print(f"Wrote raw results to {artifact_paths.raw_results_path}")
    print(f"Wrote watchlist to {artifact_paths.watchlist_path}")
    print(f"Wrote run summary to {artifact_paths.summary_path}")
    persisted_run_id = persist_screen_run_artifacts_if_configured(
        args=args,
        summary_path=artifact_paths.summary_path,
    )
    if persisted_run_id is not None:
        print(f"Persisted screen run id={persisted_run_id}")
    if _notify_discord_hits(
        as_of_date=as_of_date,
        result_payload=result.to_dict(),
        watchlist_file=str(artifact_paths.watchlist_path),
    ):
        print("Sent Discord alert for My Picks 50 SMA reclaim hits.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
