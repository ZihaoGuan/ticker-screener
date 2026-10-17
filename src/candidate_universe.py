from __future__ import annotations

from dataclasses import dataclass
import datetime as dt
from typing import Sequence

from .daily_screening_features import FEATURE_CODE_VERSION
from .market_data_access import _connect


@dataclass(frozen=True)
class CandidateSelection:
    tickers: list[str]
    eligible_count: int
    candidate_count: int
    fallback_reason: str | None = None


def select_candidates(
    strategy_id: str,
    as_of_date: dt.date,
    eligible_tickers: Sequence[str],
    database_url: str | None = None,
    *,
    limit: int | None = None,
    explicit_tickers: bool = False,
) -> CandidateSelection:
    eligible = list(dict.fromkeys(str(t).strip().upper() for t in eligible_tickers if str(t).strip()))
    if explicit_tickers:
        selected = eligible[:limit] if limit is not None else eligible
        return CandidateSelection(selected, len(eligible), len(selected), "explicit_tickers_bypass")
    if strategy_id == "weinstein_stage2_early":
        selected = eligible[:limit] if limit is not None else eligible
        return CandidateSelection(selected, len(eligible), len(selected), "no_safe_prefilter")
    try:
        connection = _connect(database_url)
    except Exception:
        connection = None
    if connection is None:
        candidates, reason = eligible, "database_unavailable"
    else:
        try:
            with connection.cursor() as cursor:
                cursor.execute(
                    """SELECT ticker, close, return_1y_pct, sma50, sma150, sma200,
                    weekly_ema30, prior_weekly_ema30
                    FROM ticker_daily_screening_features
                    WHERE trade_date = %s AND code_version = %s AND ticker = ANY(%s)""",
                    (as_of_date, FEATURE_CODE_VERSION, eligible),
                )
                rows = cursor.fetchall()
        except Exception:
            rows = []
            reason = "snapshot_unavailable"
        else:
            reason = ""
        finally:
            connection.close()
        if not reason and (not eligible or len(rows) / len(eligible) < 0.90):
            candidates, reason = eligible, "stale_or_incomplete_snapshot"
        elif not reason:
            feature_map = {str(row[0]).upper(): row[1:] for row in rows}
            keep: list[str] = []
            for ticker in eligible:
                values = feature_map.get(ticker)
                if values is None:
                    keep.append(ticker)
                    continue
                close, return_1y, sma50, sma150, sma200, weekly_ema, prior_weekly_ema = values
                if strategy_id == "one_year_winners":
                    passed = close is not None and return_1y is not None and float(close) > 20 and float(return_1y) > 30
                elif strategy_id == "trend_template":
                    passed = all(v is not None for v in (close, sma50, sma150, sma200)) and float(close) > float(sma50) and float(sma50) > float(sma150) and float(sma150) > float(sma200)
                else:
                    candidates, reason = eligible, "unsupported_strategy"
                    break
                if passed:
                    keep.append(ticker)
            else:
                candidates = keep
    if limit is not None:
        candidates = candidates[:max(0, int(limit))]
    return CandidateSelection(candidates, len(eligible), len(candidates), reason or None)
