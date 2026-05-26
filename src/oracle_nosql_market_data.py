from __future__ import annotations

from dataclasses import asdict, dataclass
import datetime as dt
from typing import Iterable, Iterator, Sequence, TypeVar


@dataclass(frozen=True)
class TickerMetadataRow:
    ticker: str
    exchange: str | None
    sector: str | None
    industry: str | None
    is_active: bool
    source: str
    updated_at: str

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True)
class DailyBarRow:
    ticker: str
    trade_date: str
    open: float | None
    high: float | None
    low: float | None
    close: float | None
    adj_close: float | None
    volume: int | None
    dividend: float | None
    split_factor: float | None
    exchange: str | None
    sector: str | None
    source: str
    updated_at: str

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


def utc_now_iso() -> str:
    return dt.datetime.now(dt.timezone.utc).replace(microsecond=0).isoformat()


def build_daily_bars_table_ddl(table_name: str) -> str:
    return f"""
CREATE TABLE IF NOT EXISTS {table_name} (
  ticker STRING,
  trade_date STRING,
  open NUMBER,
  high NUMBER,
  low NUMBER,
  close NUMBER,
  adj_close NUMBER,
  volume LONG,
  dividend NUMBER,
  split_factor NUMBER,
  exchange STRING,
  sector STRING,
  source STRING,
  updated_at STRING,
  PRIMARY KEY(SHARD(ticker), trade_date)
)
""".strip()


def build_ticker_metadata_table_ddl(table_name: str) -> str:
    return f"""
CREATE TABLE IF NOT EXISTS {table_name} (
  ticker STRING,
  exchange STRING,
  sector STRING,
  industry STRING,
  is_active BOOLEAN,
  source STRING,
  updated_at STRING,
  PRIMARY KEY(ticker)
)
""".strip()


T = TypeVar("T")


def chunked(items: Sequence[T], size: int) -> Iterator[Sequence[T]]:
    if size <= 0:
        raise ValueError("chunk size must be positive")
    for index in range(0, len(items), size):
        yield items[index : index + size]


def rows_to_jsonl_lines(rows: Iterable[dict[str, object]]) -> Iterator[str]:
    import json

    for row in rows:
        yield json.dumps(row, separators=(",", ":"), sort_keys=True)
