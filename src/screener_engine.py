from __future__ import annotations

from dataclasses import dataclass, field
import datetime as dt
from typing import Any, Callable, Mapping


ScreenerEvaluator = Callable[["ScreenerInputBundle"], "ScreenerEvaluationResult"]


@dataclass(frozen=True)
class ScreenerSpec:
    id: str
    required_inputs: tuple[str, ...]
    lookback_trading_days: int
    warmup_trading_days: int = 0
    evaluator: ScreenerEvaluator | None = None

    @property
    def total_trading_days(self) -> int:
        return max(1, int(self.lookback_trading_days) + int(self.warmup_trading_days))


@dataclass(frozen=True)
class ScreenerInputBundle:
    ticker: str
    as_of_date: dt.date
    bars: Any
    benchmark_bars: Any = None
    metadata: Mapping[str, object] | None = None
    earnings_events: Any = None
    extras: Mapping[str, object] = field(default_factory=dict)


@dataclass(frozen=True)
class ScreenerEvaluationResult:
    passed: bool
    metrics: Mapping[str, object] = field(default_factory=dict)
    reasons: tuple[str, ...] = ()
    hit: Mapping[str, object] | None = None
    error: str | None = None


def resolve_max_trading_days(specs: list[ScreenerSpec]) -> int:
    if not specs:
        return 1
    return max(spec.total_trading_days for spec in specs)
