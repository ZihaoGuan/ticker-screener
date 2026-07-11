from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.webapp.services.tiger_positions_service import TigerPositionsService


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Sync Tiger positions for all enabled web users.")
    parser.add_argument("--database-url", default="", help="Optional Postgres connection string override.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    service = TigerPositionsService(database_url=args.database_url)
    result = service.sync_all_enabled_users()
    print(json.dumps(result, indent=2, default=str))
    return 0 if int(result.get("failure_count") or 0) == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
