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

from src.webapp.config import load_webapp_config
from src.webapp.services.martin_rs_replay import run_database_replay


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="Replay an EMPTY RS Expanded book using dated, point-in-time saved snapshots. Defaults to read-only preflight.")
    parser.add_argument("--from-date", required=True, type=dt.date.fromisoformat)
    parser.add_argument("--to-date", required=True, type=dt.date.fromisoformat)
    modes = parser.add_mutually_exclusive_group()
    modes.add_argument("--dry-run", action="store_true", help="Read-only input preflight (default); no fill simulation.")
    modes.add_argument("--apply", action="store_true", help="Save the full range atomically; refuses an existing RS Expanded book.")
    args = parser.parse_args(argv)
    if args.to_date < args.from_date:
        parser.error("--to-date must be on or after --from-date")
    config = load_webapp_config()
    if not config.database_url:
        parser.error("TICKER_SCREENER_DATABASE_URL is required")
    try:
        result = run_database_replay(config.database_url, args.from_date, args.to_date, apply=args.apply)
    except ImportError as exc:
        print(json.dumps({"saved": False, "error": f"Missing dependency: {exc.name}. Install project requirements and psycopg[binary]."}))
        return 2
    except ValueError as exc:
        print(json.dumps({"saved": False, "error": str(exc)}))
        return 2
    except Exception as exc:
        # Do not print connection strings or claim certainty after a lost commit acknowledgement.
        print(json.dumps({"saved": "unconfirmed", "error": f"{type(exc).__name__}: replay did not complete cleanly. Check database/schema and verify the book before retrying."}))
        return 2
    print(json.dumps(result, default=str, indent=2))
    return 0 if result["ready"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
