"""Price-history persistence evidence emitted by one split-refresh invocation."""
from __future__ import annotations

import json
from pathlib import Path

RESULT_PREFIX = "SPLIT_REFRESH_RESULT="


def persistence_fields(results: list[dict[str, object]]) -> dict[str, object]:
    attempted = [row for row in results if not str(row.get("status", "")).startswith("ignored_")]
    written = sum(int(row.get("rows_persisted") or 0) for row in attempted)
    uncertain = any(row.get("rows_persisted") is None for row in attempted)
    failed = any(not str(row.get("status", "")).startswith("synced") for row in attempted)
    state = ("not_applicable" if not attempted else "partial" if written and (failed or uncertain)
             else "complete" if written else "unknown" if uncertain else "none")
    messages = {
        "not_applicable": "No eligible split events required a price-history refresh.",
        "complete": f"Committed {written} price-history rows; all attempted tickers refreshed.",
        "partial": f"Committed {written} price-history rows; some ticker refreshes failed or could not be confirmed.",
        "unknown": "Price-history persistence could not be confirmed; inspect the current run log.",
        "none": "No price-history rows persisted; see per-ticker results.",
    }
    return {"persistence_state": state, "persisted_to_db": True if written else None if uncertain or not attempted else False,
            "persistence_message": messages[state],
            "split_refresh_summary": {"version": 1, "ticker_results": results, "rows_persisted": written}}


def read_persistence_fields(log_file: str) -> dict[str, object]:
    """Read only this job's log, never an older same-date child manifest."""
    unknown = persistence_fields([{"ticker": "", "status": "unknown", "rows_persisted": None}])
    unknown.pop("split_refresh_summary")
    try:
        # Stream logs so a long download log does not need to fit in memory.
        found = None
        with Path(log_file).open(encoding="utf-8") as handle:
            for line in handle:
                if not line.startswith(RESULT_PREFIX) or len(line) > 2_000_000:
                    continue
                try:
                    payload = json.loads(line[len(RESULT_PREFIX):])
                    if payload.get("version") == 1 and isinstance(payload.get("ticker_results"), list):
                        rows = payload["ticker_results"]
                        if all(isinstance(row, dict) and isinstance(row.get("status"), str)
                               and (row.get("rows_persisted") is None or
                                    type(row.get("rows_persisted")) is int and row["rows_persisted"] >= 0)
                               for row in rows):
                            found = persistence_fields(rows)
                except (ValueError, TypeError, AttributeError):
                    continue
        return found or unknown
    except OSError:
        return unknown
