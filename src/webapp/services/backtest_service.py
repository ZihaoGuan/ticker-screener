from __future__ import annotations

import datetime as dt
from html import escape
import json
from pathlib import Path
import statistics
from typing import Any

import pandas as pd

from src.market_data_access import load_many_ticker_windows_for_range, resolve_database_url
from src.webapp.backtest_engine import (
    DisabledRuleCompiler,
    MinCountSameDayEntryEvaluator,
    build_rule_label,
)
from src.webapp.repositories.history_repository import HistoryRepository


DEFAULT_EXIT_RULES = [
    {"kind": "fixed_hold", "trading_days": 1},
    {"kind": "fixed_hold", "trading_days": 5},
    {"kind": "fixed_hold", "trading_days": 10},
    {"kind": "fixed_hold", "trading_days": 20},
]


class BacktestService:
    def __init__(
        self,
        *,
        database_url: str = "",
        artifacts_dir: Path | None = None,
        repository: HistoryRepository | None = None,
    ) -> None:
        self.database_url = resolve_database_url(database_url)
        self.artifacts_dir = artifacts_dir or (Path(__file__).resolve().parents[3] / "artifacts")
        self.repository = repository or HistoryRepository(database_url=self.database_url, artifacts_dir=self.artifacts_dir)
        self.entry_evaluator = MinCountSameDayEntryEvaluator()
        self.rule_compiler = DisabledRuleCompiler()

    def list_runs(self, *, limit: int = 20) -> list[dict[str, Any]]:
        return self.repository.list_backtest_runs(limit=limit)

    def default_templates(self) -> list[dict[str, Any]]:
        return [
            {
                "label": "Min Count Same Day",
                "description": "Enter when a ticker passes at least N selected screeners on the same date.",
                "entry_rule": {"mode": "min_count_same_day", "screener_ids": ["rs", "vcp"], "min_count": 2},
                "exit_rules": DEFAULT_EXIT_RULES,
                "signal_cache_policy": "reuse_then_fill",
                "market_data_mode": "database_only",
            }
        ]

    def run_backtest(self, payload: dict[str, Any], *, job_run_id: int | None = None) -> dict[str, Any]:
        entry_rule = dict(payload.get("entry_rule") or {})
        date_range = dict(payload.get("date_range") or {})
        signal_cache_policy = str(payload.get("signal_cache_policy") or "reuse_then_fill")
        market_data_mode = str(payload.get("market_data_mode") or "database_only")
        exit_rules = list(payload.get("exit_rules") or DEFAULT_EXIT_RULES)
        position_rules = dict(payload.get("position_rules") or {})
        start_date = dt.date.fromisoformat(str(date_range.get("start_date") or ""))
        end_date = dt.date.fromisoformat(str(date_range.get("end_date") or ""))
        screener_ids = [str(item).strip() for item in entry_rule.get("screener_ids", []) if str(item).strip()]
        if str(entry_rule.get("mode") or "") != "min_count_same_day":
            raise ValueError("Only min_count_same_day entry_rule.mode is supported.")
        if not screener_ids:
            raise ValueError("entry_rule.screener_ids must be non-empty.")
        if signal_cache_policy not in {"reuse_then_fill", "reuse_only"}:
            raise ValueError("signal_cache_policy must be reuse_then_fill or reuse_only.")
        if market_data_mode != "database_only":
            raise ValueError("market_data_mode must be database_only for reproducible backtests.")

        compiled_rules = [self._validate_rule(dict(rule)) for rule in exit_rules]
        signals = self.repository.load_cached_signals(screener_ids=screener_ids, start_date=start_date, end_date=end_date)
        entries = self.entry_evaluator.build_entries(signals=signals, entry_rule=entry_rule, position_rules=position_rules)
        if signal_cache_policy == "reuse_only" and not entries:
            raise ValueError("No cached screener signals found for reuse_only policy.")

        tickers = sorted({str(entry["ticker"]).upper() for entry in entries})
        max_hold_days = self._max_hold_days(compiled_rules)
        bars_by_ticker = load_many_ticker_windows_for_range(
            tickers,
            start_date,
            end_date + dt.timedelta(days=max(35, max_hold_days * 3)),
            max(260, max_hold_days + 5),
            database_url=self.database_url,
        )
        missing_tickers = sorted([ticker for ticker in tickers if ticker not in bars_by_ticker])
        partial = bool(missing_tickers)

        trade_results: dict[str, list[dict[str, Any]]] = {build_rule_label(rule): [] for rule in compiled_rules}
        for entry in entries:
            ticker = str(entry["ticker"]).upper()
            bars = bars_by_ticker.get(ticker)
            if bars is None or getattr(bars, "empty", False):
                continue
            prepared = self._prepare_entry(entry, bars)
            if prepared is None:
                continue
            for rule in compiled_rules:
                label = build_rule_label(rule)
                result = self._evaluate_rule(prepared, bars, rule)
                if result is not None:
                    trade_results[label].append(result)

        summary = {
            "entry_count": len(entries),
            "signal_count": len(signals),
            "missing_tickers": missing_tickers,
            "partial": partial,
            "signal_cache_policy": signal_cache_policy,
            "market_data_mode": market_data_mode,
            "results_by_rule": {label: self._summarize_trades(rows) for label, rows in trade_results.items()},
        }
        strategy_id = f"combo:{'+'.join(screener_ids)}"
        artifact_dir = self.artifacts_dir / "output" / f"backtest_{strategy_id.replace(':', '_').replace('+', '_')}_{start_date}_{end_date}"
        artifact_dir.mkdir(parents=True, exist_ok=True)
        json_path = artifact_dir / "result.json"
        html_path = artifact_dir / "index.html"
        result_payload = {
            "strategy_id": strategy_id,
            "entry_rule": entry_rule,
            "date_range": {"start_date": start_date.isoformat(), "end_date": end_date.isoformat()},
            "signal_cache_policy": signal_cache_policy,
            "market_data_mode": market_data_mode,
            "exit_rules": compiled_rules,
            "position_rules": position_rules,
            "summary": summary,
            "trade_results": trade_results,
        }
        backtest_run_id = self.repository.create_backtest_run(
            strategy_id=strategy_id,
            start_date=start_date,
            end_date=end_date,
            parameters={
                "entry_rule": entry_rule,
                "exit_rules": compiled_rules,
                "signal_cache_policy": signal_cache_policy,
                "market_data_mode": market_data_mode,
                "position_rules": position_rules,
            },
            summary=summary,
            html_report_path=str(html_path),
            json_report_path=str(json_path),
            job_run_id=job_run_id,
        )
        result_payload["backtest_run_id"] = backtest_run_id
        json_path.write_text(json.dumps(result_payload, indent=2, default=self._json_default), encoding="utf-8")
        html_path.write_text(self._build_html_report(result_payload), encoding="utf-8")
        return {
            "backtest_run_id": backtest_run_id,
            "strategy_id": strategy_id,
            "summary": summary,
            "json_report_path": str(json_path),
            "html_report_path": str(html_path),
        }

    def _prepare_entry(self, entry: dict[str, Any], bars: Any) -> dict[str, Any] | None:
        target = pd.Timestamp(entry["signal_date"])
        later = bars[bars.index >= target]
        if later.empty:
            return None
        entry_date = later.index[0]
        position = bars.index.get_loc(entry_date)
        if isinstance(position, slice):
            position = position.start
        return {
            **entry,
            "entry_date": entry_date.date(),
            "entry_index": int(position),
            "entry_price": float(later.iloc[0]["Close"]),
        }

    def _validate_rule(self, rule: dict[str, Any]) -> dict[str, Any]:
        kind = str(rule.get("kind") or "").strip()
        if kind == "fixed_hold":
            if int(rule.get("trading_days") or 0) <= 0:
                raise ValueError("fixed_hold.trading_days must be > 0")
            return {"kind": kind, "trading_days": int(rule["trading_days"])}
        if kind in {"stop_loss_pct", "take_profit_pct"}:
            if float(rule.get("percent") or 0) <= 0:
                raise ValueError(f"{kind}.percent must be > 0")
            return {"kind": kind, "percent": float(rule["percent"])}
        if kind == "close_below_ema":
            period = int(rule.get("period") or 0)
            confirm_bars = int(rule.get("confirm_bars") or 1)
            if period not in {8, 21, 50, 200}:
                raise ValueError("close_below_ema.period must be one of 8, 21, 50, 200")
            if confirm_bars not in {1, 2}:
                raise ValueError("close_below_ema.confirm_bars must be 1 or 2")
            return {"kind": kind, "period": period, "confirm_bars": confirm_bars}
        if kind == "first_of":
            nested = [self._validate_rule(dict(item)) for item in rule.get("rules") or []]
            if not nested:
                raise ValueError("first_of.rules must be non-empty")
            return {"kind": kind, "rules": nested}
        raise ValueError(f"Unsupported exit rule kind: {kind}")

    def _evaluate_rule(self, entry: dict[str, Any], bars: Any, rule: dict[str, Any]) -> dict[str, Any] | None:
        kind = rule["kind"]
        if kind == "fixed_hold":
            return self._eval_fixed_hold(entry, bars, int(rule["trading_days"]))
        if kind == "stop_loss_pct":
            return self._eval_stop_loss(entry, bars, float(rule["percent"]))
        if kind == "take_profit_pct":
            return self._eval_take_profit(entry, bars, float(rule["percent"]))
        if kind == "close_below_ema":
            return self._eval_close_below_ema(entry, bars, int(rule["period"]), int(rule.get("confirm_bars") or 1))
        if kind == "first_of":
            results = [self._evaluate_rule(entry, bars, nested) for nested in rule["rules"]]
            filtered = [item for item in results if item is not None]
            if not filtered:
                return None
            filtered.sort(key=lambda item: (item["exit_date"], item["exit_order"]))
            chosen = dict(filtered[0])
            chosen["exit_rule"] = build_rule_label(rule)
            return chosen
        return None

    def _eval_fixed_hold(self, entry: dict[str, Any], bars: Any, trading_days: int) -> dict[str, Any] | None:
        exit_index = int(entry["entry_index"]) + trading_days
        if exit_index >= len(bars.index):
            return None
        row = bars.iloc[exit_index]
        exit_date = bars.index[exit_index].date()
        exit_price = float(row["Close"])
        return self._build_trade_result(entry, exit_date, exit_price, f"hold_{trading_days}d", exit_index)

    def _eval_stop_loss(self, entry: dict[str, Any], bars: Any, percent: float) -> dict[str, Any] | None:
        threshold = float(entry["entry_price"]) * (1.0 - percent / 100.0)
        for offset in range(int(entry["entry_index"]) + 1, len(bars.index)):
            row = bars.iloc[offset]
            open_price = float(row["Open"])
            low_price = float(row["Low"])
            if open_price <= threshold:
                return self._build_trade_result(entry, bars.index[offset].date(), open_price, f"stop_{percent:g}pct", offset)
            if low_price <= threshold:
                return self._build_trade_result(entry, bars.index[offset].date(), threshold, f"stop_{percent:g}pct", offset)
        return None

    def _eval_take_profit(self, entry: dict[str, Any], bars: Any, percent: float) -> dict[str, Any] | None:
        threshold = float(entry["entry_price"]) * (1.0 + percent / 100.0)
        for offset in range(int(entry["entry_index"]) + 1, len(bars.index)):
            row = bars.iloc[offset]
            open_price = float(row["Open"])
            high_price = float(row["High"])
            if open_price >= threshold:
                return self._build_trade_result(entry, bars.index[offset].date(), open_price, f"take_{percent:g}pct", offset)
            if high_price >= threshold:
                return self._build_trade_result(entry, bars.index[offset].date(), threshold, f"take_{percent:g}pct", offset)
        return None

    def _eval_close_below_ema(self, entry: dict[str, Any], bars: Any, period: int, confirm_bars: int) -> dict[str, Any] | None:
        ema = bars["Close"].ewm(span=period, adjust=False).mean()
        streak = 0
        for offset in range(int(entry["entry_index"]) + 1, len(bars.index)):
            close_price = float(bars.iloc[offset]["Close"])
            ema_value = float(ema.iloc[offset])
            if close_price < ema_value:
                streak += 1
            else:
                streak = 0
            if streak >= confirm_bars:
                return self._build_trade_result(entry, bars.index[offset].date(), close_price, f"close_below_ema_{period}", offset)
        return None

    def _build_trade_result(self, entry: dict[str, Any], exit_date: dt.date, exit_price: float, exit_rule: str, exit_order: int) -> dict[str, Any]:
        return {
            "ticker": entry["ticker"],
            "signal_date": entry["signal_date"].isoformat(),
            "entry_date": entry["entry_date"].isoformat(),
            "entry_price": round(float(entry["entry_price"]), 4),
            "exit_date": exit_date.isoformat(),
            "exit_price": round(float(exit_price), 4),
            "return_pct": round(((float(exit_price) / float(entry["entry_price"])) - 1.0) * 100.0, 2),
            "matched_count": int(entry["matched_count"]),
            "matched_screener_ids": list(entry["matched_screener_ids"]),
            "exit_rule": exit_rule,
            "exit_order": int(exit_order),
        }

    def _summarize_trades(self, rows: list[dict[str, Any]]) -> dict[str, Any]:
        returns = [float(row["return_pct"]) for row in rows]
        if not returns:
            return {"trade_count": 0, "avg_return_pct": None, "median_return_pct": None, "win_rate_pct": None}
        wins = sum(1 for value in returns if value > 0)
        return {
            "trade_count": len(returns),
            "avg_return_pct": round(sum(returns) / len(returns), 2),
            "median_return_pct": round(statistics.median(returns), 2),
            "win_rate_pct": round((wins / len(returns)) * 100.0, 1),
        }

    def _max_hold_days(self, rules: list[dict[str, Any]]) -> int:
        best = 20
        for rule in rules:
            kind = rule["kind"]
            if kind == "fixed_hold":
                best = max(best, int(rule["trading_days"]))
            elif kind == "first_of":
                best = max(best, self._max_hold_days(list(rule["rules"])))
        return best

    def _build_html_report(self, payload: dict[str, Any]) -> str:
        cards = []
        for label, summary in payload["summary"]["results_by_rule"].items():
            cards.append(
                "<article><h3>{}</h3><p>Trades: {}</p><p>Avg: {}</p><p>Win Rate: {}</p></article>".format(
                    escape(label),
                    summary["trade_count"],
                    escape(str(summary["avg_return_pct"])),
                    escape(str(summary["win_rate_pct"])),
                )
            )
        return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <title>Backtest Report</title>
  <style>
    body {{ font-family: sans-serif; margin: 24px; background: #f5f1e8; color: #1f1d1a; }}
    section {{ margin-bottom: 24px; }}
    article {{ background: white; border-radius: 12px; padding: 16px; margin-bottom: 12px; }}
  </style>
</head>
<body>
  <section>
    <h1>{escape(payload["strategy_id"])}</h1>
    <p>{escape(json.dumps(payload["date_range"]))}</p>
  </section>
  <section>
    {''.join(cards)}
  </section>
</body>
</html>
"""

    def _json_default(self, value: Any) -> Any:
        if isinstance(value, dt.date):
            return value.isoformat()
        raise TypeError(f"Unsupported JSON value: {type(value)!r}")
