from __future__ import annotations

from dataclasses import asdict, dataclass
import datetime as dt
from numbers import Real
import re
from typing import Sequence

from .ratings.repository import RatingsRepository


_PERCENT_LABEL_RE = re.compile(r"^\(?-?\d+(?:\.\d+)?%\)?$")
_MARKET_TIME_LABEL_RE = re.compile(r"^\d{1,2}:\d{2}(?::\d{2})?\s*(?:AM|PM)\s+[A-Z]{2,4}$", re.IGNORECASE)


@dataclass(frozen=True)
class IndustryGroupRsRankHit:
    ticker: str
    signal_date: str
    sector: str | None
    industry: str | None
    industry_group: str | None
    industry_group_rs_rank: float
    industry_group_member_count: int | None
    daily_rs_rating: float | None
    weekly_rs_rating: float | None
    leadership_score: float | None
    technical_rating: float | None
    rating_band: str | None
    reasons: list[str]

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True)
class IndustryGroupRsRankScreenResult:
    run_date: str
    total_tickers: int
    snapshot_count: int
    ranked_snapshot_count: int
    passed_tickers: int
    minimum_rank: float
    failed_tickers: list[dict[str, str]]
    hits: list[IndustryGroupRsRankHit]

    def to_dict(self) -> dict[str, object]:
        return {
            "run_date": self.run_date,
            "total_tickers": self.total_tickers,
            "snapshot_count": self.snapshot_count,
            "ranked_snapshot_count": self.ranked_snapshot_count,
            "passed_tickers": self.passed_tickers,
            "minimum_rank": self.minimum_rank,
            "failed_tickers": list(self.failed_tickers),
            "hits": [item.to_dict() for item in self.hits],
        }


def _coerce_real(value: object) -> float | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, Real):
        return float(value)
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _normalize_label(value: object) -> str:
    return str(value or "").strip()


def _is_valid_industry_group_label(value: object) -> bool:
    label = _normalize_label(value)
    if not label or label == "(blank)":
        return False
    if _PERCENT_LABEL_RE.fullmatch(label):
        return False
    if _MARKET_TIME_LABEL_RE.fullmatch(label):
        return False
    return True


def run_industry_group_rs_rank_screen(*, database_url: str, as_of_date: dt.date | None = None, tickers: Sequence[str] | None = None, minimum_rank: float = 90.0) -> IndustryGroupRsRankScreenResult:
    repository = RatingsRepository(database_url)
    target_tickers = [str(item or "").strip().upper() for item in (tickers or []) if str(item or "").strip()]
    if not target_tickers:
        target_tickers = repository.list_active_tickers()
    snapshots = repository.load_latest_technical_rating_snapshots_for_tickers(target_tickers, as_of_date=as_of_date, allow_older_as_of_date=True)
    snapshot_count = 0
    ranked_snapshot_count = 0
    hits: list[IndustryGroupRsRankHit] = []
    for ticker in target_tickers:
        snapshot = snapshots.get(ticker) or {}
        if snapshot:
            snapshot_count += 1
        industry_group = snapshot.get("industry_group")
        rank = _coerce_real(snapshot.get("industry_group_rs_rank"))
        if rank is not None:
            ranked_snapshot_count += 1
        if not _is_valid_industry_group_label(industry_group):
            continue
        if rank is None or rank <= float(minimum_rank):
            continue
        hits.append(IndustryGroupRsRankHit(
            ticker=ticker,
            signal_date=str(snapshot.get("as_of_date") or (as_of_date.isoformat() if as_of_date else "")),
            sector=snapshot.get("sector"),
            industry=snapshot.get("industry"),
            industry_group=industry_group,
            industry_group_rs_rank=round(rank, 1),
            industry_group_member_count=int(snapshot["industry_group_member_count"]) if snapshot.get("industry_group_member_count") is not None else None,
            daily_rs_rating=float(snapshot["daily_rs_rating"]) if snapshot.get("daily_rs_rating") is not None else None,
            weekly_rs_rating=float(snapshot["weekly_rs_rating"]) if snapshot.get("weekly_rs_rating") is not None else None,
            leadership_score=float(snapshot["leadership_score"]) if snapshot.get("leadership_score") is not None else None,
            technical_rating=float(snapshot["overall_rating"]) if snapshot.get("overall_rating") is not None else None,
            rating_band=snapshot.get("rating_band"),
            reasons=[
                f"Industry-group RS {rank:.1f}",
                f"Daily RS {float(snapshot['daily_rs_rating']):.1f}" if snapshot.get("daily_rs_rating") is not None else "Daily RS unavailable",
                f"Leadership {float(snapshot['leadership_score']):.1f}" if snapshot.get("leadership_score") is not None else "Leadership unavailable",
            ],
        ))
    hits.sort(key=lambda item: (item.industry_group_rs_rank, item.daily_rs_rating if item.daily_rs_rating is not None else -1.0, item.leadership_score if item.leadership_score is not None else -1.0, item.ticker), reverse=True)
    return IndustryGroupRsRankScreenResult(
        run_date=(as_of_date or dt.date.today()).isoformat(),
        total_tickers=len(target_tickers),
        snapshot_count=snapshot_count,
        ranked_snapshot_count=ranked_snapshot_count,
        passed_tickers=len(hits),
        minimum_rank=float(minimum_rank),
        failed_tickers=[],
        hits=hits,
    )
