"""Bounded, versioned transfer format for GitHub-computed technical ratings."""

from __future__ import annotations

import datetime as dt
import gzip
import io
import json
import math
from pathlib import Path
import re

from .constants import TECHNICAL_INDICATOR_TIMEFRAMES
from .models import TechnicalIndicatorRatingSnapshot
from .technical_indicator import score_to_label


ACTION_ID = "build_technical_indicator_ratings"
MAX_COMPRESSED_BYTES = 10 * 1024 * 1024
MAX_JSON_BYTES = 40 * 1024 * 1024
MAX_TICKERS = 20000
_TICKER = re.compile(r"[A-Z0-9][A-Z0-9.^_-]{0,19}\Z")
_ROW_FIELDS = set(TechnicalIndicatorRatingSnapshot.__dataclass_fields__)


def write_export(path: Path, *, as_of_date: dt.date, tickers: list[str], ratings: list[TechnicalIndicatorRatingSnapshot]) -> None:
    payload = {
        "schema_version": 1,
        "action_id": ACTION_ID,
        "as_of_date": as_of_date.isoformat(),
        "tickers": tickers,
        "row_count": len(ratings),
        "rows": [{**item.to_record(), "as_of_date": item.as_of_date.isoformat()} for item in ratings],
    }
    data = json.dumps(payload, allow_nan=False, separators=(",", ":")).encode("utf-8")
    if len(data) > MAX_JSON_BYTES:
        raise ValueError("Technical rating export exceeds size limit.")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(gzip.compress(data, mtime=0))
    if path.stat().st_size > MAX_COMPRESSED_BYTES:
        path.unlink()
        raise ValueError("Compressed technical rating export exceeds size limit.")


def read_export(data: bytes, *, expected_date: str = "") -> tuple[dt.date, list[str], list[TechnicalIndicatorRatingSnapshot]]:
    if len(data) > MAX_COMPRESSED_BYTES:
        raise ValueError("Technical rating artifact exceeds size limit.")
    try:
        with gzip.GzipFile(fileobj=io.BytesIO(data)) as stream:
            raw = stream.read(MAX_JSON_BYTES + 1)
        if len(raw) > MAX_JSON_BYTES:
            raise ValueError("Technical rating artifact expands beyond size limit.")
        payload = json.loads(raw)
    except (OSError, EOFError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError("Technical rating artifact is invalid.") from exc
    if not isinstance(payload, dict) or set(payload) != {"schema_version", "action_id", "as_of_date", "tickers", "row_count", "rows"}:
        raise ValueError("Technical rating artifact schema is invalid.")
    if type(payload["schema_version"]) is not int or payload["schema_version"] != 1 or payload["action_id"] != ACTION_ID:
        raise ValueError("Technical rating artifact identity is invalid.")
    try:
        as_of_date = dt.date.fromisoformat(payload["as_of_date"])
    except (TypeError, ValueError) as exc:
        raise ValueError("Technical rating artifact date is invalid.") from exc
    if as_of_date.isoformat() != payload["as_of_date"] or (expected_date and expected_date != payload["as_of_date"]):
        raise ValueError("Technical rating artifact date does not match job.")
    tickers = payload["tickers"]
    rows = payload["rows"]
    if not isinstance(tickers, list) or not 1 <= len(tickers) <= MAX_TICKERS or any(not isinstance(t, str) or not _TICKER.fullmatch(t) for t in tickers) or len(set(tickers)) != len(tickers):
        raise ValueError("Technical rating artifact ticker scope is invalid.")
    if not isinstance(rows, list) or type(payload["row_count"]) is not int or payload["row_count"] != len(rows) or len(rows) != len(tickers) * len(TECHNICAL_INDICATOR_TIMEFRAMES):
        raise ValueError("Technical rating artifact row count is invalid.")
    expected_pairs = {(ticker, tf) for ticker in tickers for tf in TECHNICAL_INDICATOR_TIMEFRAMES}
    parsed: list[TechnicalIndicatorRatingSnapshot] = []
    for row in rows:
        if not isinstance(row, dict) or set(row) != _ROW_FIELDS:
            raise ValueError("Technical rating row schema is invalid.")
        if not isinstance(row["ticker"], str) or not isinstance(row["timeframe"], str):
            raise ValueError("Technical rating row identity is invalid.")
        pair = (row["ticker"], row["timeframe"])
        if pair not in expected_pairs or row["as_of_date"] != as_of_date.isoformat():
            raise ValueError("Technical rating row ticker, timeframe, or date is invalid.")
        expected_pairs.remove(pair)
        status = row["technical_status"]
        scores = [row[key] for key in ("moving_average_score", "oscillator_score", "overall_score")]
        reason = row["technical_status_reason"]
        missing = row["missing_metric_names"]
        if status == "ok":
            if any(type(v) not in (int, float) or not math.isfinite(v) or not -1 <= v <= 1 for v in scores) or row["rating_label"] != score_to_label(scores[2]) or reason is not None or missing != []:
                raise ValueError("Technical rating ok row is invalid.")
        elif status == "missing_metrics":
            if any(v is not None for v in scores) or row["rating_label"] is not None or not isinstance(reason, str) or not reason or len(reason) > 500:
                raise ValueError("Technical rating missing row is invalid.")
        else:
            raise ValueError("Technical rating status is invalid.")
        if not isinstance(missing, list) or len(missing) > 100 or any(not isinstance(v, str) or not 1 <= len(v) <= 100 for v in missing):
            raise ValueError("Technical rating missing metrics are invalid.")
        parsed.append(TechnicalIndicatorRatingSnapshot(**{**row, "as_of_date": as_of_date}))
    if expected_pairs:
        raise ValueError("Technical rating artifact has missing rows.")
    return as_of_date, tickers, parsed
