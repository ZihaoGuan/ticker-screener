from __future__ import annotations

import datetime as dt
from dataclasses import dataclass
from typing import Any, Protocol


class SignalProvider(Protocol):
    def load_signals(
        self,
        *,
        screener_ids: list[str],
        start_date: dt.date,
        end_date: dt.date,
        include_deleted: bool = False,
    ) -> list[dict[str, Any]]: ...


class EntryRuleEvaluator(Protocol):
    def build_entries(
        self,
        *,
        signals: list[dict[str, Any]],
        entry_rule: dict[str, Any],
        position_rules: dict[str, Any] | None = None,
    ) -> list[dict[str, Any]]: ...


class ExitRuleEvaluator(Protocol):
    def evaluate(
        self,
        *,
        entry: dict[str, Any],
        bars: Any,
        rule: dict[str, Any],
    ) -> dict[str, Any] | None: ...


class RuleCompiler(Protocol):
    def compile(self, payload: dict[str, Any]) -> dict[str, Any]: ...


@dataclass(frozen=True)
class DisabledRuleCompiler:
    reason: str = "User-defined scripts are disabled in v1."

    def compile(self, payload: dict[str, Any]) -> dict[str, Any]:
        raise ValueError(self.reason)


class MinCountSameDayEntryEvaluator:
    def build_entries(
        self,
        *,
        signals: list[dict[str, Any]],
        entry_rule: dict[str, Any],
        position_rules: dict[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        if str(entry_rule.get("mode") or "") != "min_count_same_day":
            raise ValueError("Unsupported entry_rule.mode")
        screener_ids = {str(item).strip() for item in entry_rule.get("screener_ids", []) if str(item).strip()}
        if not screener_ids:
            raise ValueError("entry_rule.screener_ids must be non-empty")
        min_count = max(1, int(entry_rule.get("min_count") or len(screener_ids)))
        grouped: dict[tuple[str, dt.date], dict[str, Any]] = {}
        for row in signals:
            ticker = str(row.get("ticker") or "").strip().upper()
            signal_date = row.get("signal_date")
            strategy_id = str(row.get("strategy_id") or "").strip()
            if not ticker or not isinstance(signal_date, dt.date) or strategy_id not in screener_ids:
                continue
            key = (ticker, signal_date)
            current = grouped.setdefault(
                key,
                {
                    "ticker": ticker,
                    "signal_date": signal_date,
                    "screener_ids": set(),
                    "signal_rows": [],
                },
            )
            current["screener_ids"].add(strategy_id)
            current["signal_rows"].append(row)

        entries: list[dict[str, Any]] = []
        for item in grouped.values():
            if len(item["screener_ids"]) < min_count:
                continue
            entries.append(
                {
                    "ticker": item["ticker"],
                    "signal_date": item["signal_date"],
                    "matched_screener_ids": sorted(item["screener_ids"]),
                    "matched_count": len(item["screener_ids"]),
                    "signal_rows": item["signal_rows"],
                }
            )

        entries.sort(key=lambda item: (item["signal_date"], item["ticker"]))
        limit = None
        if position_rules and position_rules.get("max_positions_per_day") not in (None, ""):
            limit = max(1, int(position_rules["max_positions_per_day"]))
        if limit is None:
            return entries

        limited: list[dict[str, Any]] = []
        counts_by_date: dict[dt.date, int] = {}
        for entry in entries:
            signal_date = entry["signal_date"]
            count = counts_by_date.get(signal_date, 0)
            if count >= limit:
                continue
            limited.append(entry)
            counts_by_date[signal_date] = count + 1
        return limited


def build_rule_label(rule: dict[str, Any]) -> str:
    kind = str(rule.get("kind") or "").strip()
    if kind == "fixed_hold":
        return f"hold_{int(rule.get('trading_days') or 0)}d"
    if kind == "stop_loss_pct":
        return f"stop_{float(rule.get('percent') or 0):g}pct"
    if kind == "take_profit_pct":
        return f"take_{float(rule.get('percent') or 0):g}pct"
    if kind == "close_below_ema":
        return f"close_below_ema_{int(rule.get('period') or 0)}"
    if kind == "first_of":
        return "first_of"
    return kind or "rule"

