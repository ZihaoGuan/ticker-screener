from __future__ import annotations

import datetime as dt
import json
import statistics
from pathlib import Path
from typing import Any

from src.config import load_app_config
from src.market_data_access import load_many_ticker_windows_for_range, load_ticker_metadata_map
from src.webapp.repositories.history_repository import HistoryRepository


def normalize_strategy_ids(strategy_ids: list[str]) -> list[str]:
    ordered: list[str] = []
    for item in strategy_ids:
        normalized = str(item).strip()
        if normalized and normalized not in ordered:
            ordered.append(normalized)
    return ordered


def build_strategy_set_key(strategy_ids: list[str]) -> str:
    return ",".join(sorted(normalize_strategy_ids(strategy_ids)))


class OverlapBacktestService:
    def __init__(
        self,
        *,
        database_url: str = "",
        artifacts_dir: Path | None = None,
        repository: HistoryRepository | None = None,
    ) -> None:
        self.artifacts_dir = artifacts_dir or (Path(__file__).resolve().parents[3] / "artifacts")
        self.repository = repository or HistoryRepository(database_url=database_url, artifacts_dir=self.artifacts_dir)
        self.app_config = load_app_config()

    def is_configured(self) -> bool:
        return self.repository.is_configured()

    def build_overlap_for_date(
        self,
        *,
        run_date: dt.date,
        strategy_ids: list[str],
        market_data_mode: str = "database-first",
        candidate_threshold: int = 4,
        source_job_run_id: int | None = None,
    ) -> dict[str, Any]:
        normalized_strategy_ids = normalize_strategy_ids(strategy_ids)
        if not normalized_strategy_ids:
            raise ValueError("strategy_ids required")
        rows = self.repository.load_cached_signals(
            screener_ids=normalized_strategy_ids,
            start_date=run_date,
            end_date=run_date,
            include_deleted=False,
        )
        ticker_map: dict[str, dict[str, Any]] = {}
        for row in rows:
            ticker = str(row.get("ticker") or "").strip().upper()
            strategy_id = str(row.get("strategy_id") or "").strip()
            if not ticker or not strategy_id:
                continue
            entry = ticker_map.setdefault(
                ticker,
                {
                    "ticker": ticker,
                    "pipeline_count": 0,
                    "signal_count": 0,
                    "pipeline_ids": [],
                    "pipeline_labels": [],
                },
            )
            if strategy_id in entry["pipeline_ids"]:
                continue
            entry["pipeline_ids"].append(strategy_id)
            entry["pipeline_labels"].append(strategy_id)
            entry["pipeline_count"] += 1
            entry["signal_count"] += 1

        metadata_map = load_ticker_metadata_map(ticker_map.keys(), database_url=self.repository.database_url)
        entries: list[dict[str, Any]] = []
        member_rows: list[dict[str, Any]] = []
        for ticker in sorted(ticker_map):
            entry = ticker_map[ticker]
            metadata = metadata_map.get(ticker, {})
            enriched = {
                **entry,
                "sector": metadata.get("sector"),
                "industry": metadata.get("industry"),
            }
            entries.append(enriched)
            member_rows.append(
                {
                    "run_date": run_date,
                    "ticker": ticker,
                    "signal_count": int(entry["signal_count"]),
                    "contributing_strategies_json": list(entry["pipeline_ids"]),
                    "metadata_json": {"sector": metadata.get("sector"), "industry": metadata.get("industry")},
                }
            )

        pipeline_status = []
        for strategy_id in normalized_strategy_ids:
            count = sum(1 for entry in entries if strategy_id in entry["pipeline_ids"])
            pipeline_status.append(
                {
                    "id": strategy_id,
                    "label": strategy_id,
                    "count": count,
                    "file_present": True,
                }
            )

        payload = {
            "date_label": run_date.isoformat(),
            "strategy_ids": normalized_strategy_ids,
            "strategy_set_key": build_strategy_set_key(normalized_strategy_ids),
            "market_data_mode": market_data_mode,
            "candidate_threshold": int(candidate_threshold),
            "unique_ticker_count": len(entries),
            "overlap_two_plus_count": sum(1 for entry in entries if int(entry["signal_count"]) >= 2),
            "overlap_three_plus_count": sum(1 for entry in entries if int(entry["signal_count"]) >= 3),
            "overlap_four_plus_count": sum(1 for entry in entries if int(entry["signal_count"]) >= 4),
            "candidate_count": sum(1 for entry in entries if int(entry["signal_count"]) >= int(candidate_threshold)),
            "overlap_two_plus": [entry for entry in entries if int(entry["signal_count"]) >= 2],
            "overlap_three_plus": [entry for entry in entries if int(entry["signal_count"]) >= 3],
            "overlap_four_plus": [entry for entry in entries if int(entry["signal_count"]) >= 4],
            "pipeline_status": pipeline_status,
            "pipeline_tickers": {item["id"]: [entry["ticker"] for entry in entries if item["id"] in entry["pipeline_ids"]] for item in pipeline_status},
            "fearzone_tickers": [entry["ticker"] for entry in entries if "fearzone" in entry["pipeline_ids"]],
        }
        artifact_path = self._write_overlap_artifact(run_date, payload)
        overlap_run_id = self.repository.upsert_overlap_run(
            run_date=run_date,
            strategy_set_key=payload["strategy_set_key"],
            strategy_ids=normalized_strategy_ids,
            market_data_mode=market_data_mode,
            candidate_threshold=int(candidate_threshold),
            source_job_run_id=source_job_run_id,
            artifact_path=str(artifact_path),
            summary_json=payload,
        )
        self.repository.replace_overlap_run_members(overlap_run_id, member_rows)
        payload["overlap_run_id"] = overlap_run_id
        payload["artifact_path"] = str(artifact_path)
        return payload

    def list_overlap_coverage(
        self,
        *,
        strategy_ids: list[str],
        start_date: dt.date,
        end_date: dt.date,
        candidate_threshold: int = 4,
    ) -> list[dict[str, Any]]:
        normalized_strategy_ids = normalize_strategy_ids(strategy_ids)
        screen_days = self.repository.list_signal_cache_calendar(
            strategy_ids=normalized_strategy_ids or None,
            start_date=start_date,
            end_date=end_date,
            include_deleted=False,
        )
        overlap_runs = self.repository.list_overlap_runs(
            strategy_set_key=build_strategy_set_key(normalized_strategy_ids),
            start_date=start_date,
            end_date=end_date,
            candidate_threshold=candidate_threshold,
            limit=10000,
        )
        screen_by_date: dict[dt.date, list[dict[str, Any]]] = {}
        for row in screen_days:
            run_date = row.get("run_date")
            if isinstance(run_date, dt.date):
                screen_by_date.setdefault(run_date, []).append(row)
        overlap_by_date: dict[dt.date, dict[str, Any]] = {}
        for row in overlap_runs:
            run_date = row.get("run_date")
            if isinstance(run_date, dt.date) and run_date not in overlap_by_date:
                overlap_by_date[run_date] = row

        days: list[dict[str, Any]] = []
        cursor = start_date
        expected_strategy_count = len(normalized_strategy_ids)
        while cursor <= end_date:
            rows = screen_by_date.get(cursor, [])
            strategy_ids_done = sorted({str(row.get("strategy_id") or "") for row in rows if str(row.get("strategy_id") or "")})
            overlap_row = overlap_by_date.get(cursor)
            overlap_summary = overlap_row.get("summary_json") if isinstance(overlap_row, dict) else {}
            if not isinstance(overlap_summary, dict):
                overlap_summary = {}
            days.append(
                {
                    "date": cursor.isoformat(),
                    "expected_strategy_count": expected_strategy_count,
                    "screened_strategy_count": len(strategy_ids_done),
                    "screened_strategy_ids": strategy_ids_done,
                    "missing_strategy_ids": [item for item in normalized_strategy_ids if item not in strategy_ids_done],
                    "screen_status": (
                        "none"
                        if not strategy_ids_done
                        else "partial"
                        if len(strategy_ids_done) < expected_strategy_count
                        else "complete"
                    ),
                    "overlap_ready": bool(overlap_row),
                    "candidate_count": int(overlap_summary.get("candidate_count") or 0),
                    "overlap_two_plus_count": int(overlap_summary.get("overlap_two_plus_count") or 0),
                    "overlap_three_plus_count": int(overlap_summary.get("overlap_three_plus_count") or 0),
                    "overlap_four_plus_count": int(overlap_summary.get("overlap_four_plus_count") or 0),
                    "overlap_run_id": overlap_row.get("id") if overlap_row else None,
                    "updated_at": overlap_row.get("created_at") if overlap_row else None,
                }
            )
            cursor += dt.timedelta(days=1)
        return days

    def run_backtest(
        self,
        *,
        start_date: dt.date,
        end_date: dt.date,
        strategy_ids: list[str],
        entry_signal_threshold: int = 4,
        hold_periods: list[int] | None = None,
        job_run_id: int | None = None,
    ) -> dict[str, Any]:
        normalized_strategy_ids = normalize_strategy_ids(strategy_ids)
        if not normalized_strategy_ids:
            raise ValueError("strategy_ids required")
        holds = sorted({int(item) for item in (hold_periods or [5, 10]) if int(item) > 0})
        if not holds:
            raise ValueError("hold periods required")
        strategy_set_key = build_strategy_set_key(normalized_strategy_ids)
        overlap_members = self.repository.list_overlap_run_members(
            start_date=start_date,
            end_date=end_date,
            min_signal_count=entry_signal_threshold,
            strategy_set_key=strategy_set_key,
        )
        benchmark = self.app_config.benchmark_ticker.upper()
        tickers = sorted({str(row.get("ticker") or "").strip().upper() for row in overlap_members if str(row.get("ticker") or "").strip()})
        if not tickers:
            summary = self._empty_backtest_summary(
                start_date=start_date,
                end_date=end_date,
                strategy_ids=normalized_strategy_ids,
                strategy_set_key=strategy_set_key,
                entry_signal_threshold=entry_signal_threshold,
                hold_periods=holds,
            )
            backtest_run_id = self.repository.create_backtest_run(
                strategy_id="overlap_v1",
                strategy_set_key=strategy_set_key,
                strategy_ids=normalized_strategy_ids,
                start_date=start_date,
                end_date=end_date,
                parameters=summary["parameters"],
                summary=summary["summary"],
                job_run_id=job_run_id,
            )
            summary["backtest_run_id"] = backtest_run_id
            return summary

        extended_end = end_date + dt.timedelta(days=max(holds) * 4)
        frame_map = load_many_ticker_windows_for_range(
            [*tickers, benchmark],
            start_date,
            extended_end,
            5,
            database_url=self.repository.database_url,
        )
        benchmark_frame = frame_map.get(benchmark)
        trades: list[dict[str, Any]] = []
        for row in overlap_members:
            signal_date = row.get("run_date")
            if not isinstance(signal_date, dt.date):
                continue
            ticker = str(row.get("ticker") or "").strip().upper()
            frame = frame_map.get(ticker)
            hold_results = self._compute_hold_results(
                ticker_frame=frame,
                benchmark_frame=benchmark_frame,
                signal_date=signal_date,
                hold_periods=holds,
            )
            if not hold_results:
                continue
            entry_date = hold_results["entry_date"]
            entry_price = hold_results["entry_price"]
            results_by_hold = hold_results["holds"]
            trades.append(
                {
                    "signal_date": signal_date,
                    "ticker": ticker,
                    "signal_count": int(row.get("signal_count") or 0),
                    "contributing_strategies_json": list(row.get("contributing_strategies_json") or []),
                    "entry_date": entry_date,
                    "entry_price": entry_price,
                    "hold_results_json": results_by_hold,
                    "metadata_json": dict(row.get("metadata_json") or {}),
                }
            )

        summary_payload = self._build_backtest_summary(
            start_date=start_date,
            end_date=end_date,
            strategy_ids=normalized_strategy_ids,
            strategy_set_key=strategy_set_key,
            entry_signal_threshold=entry_signal_threshold,
            hold_periods=holds,
            trades=trades,
        )
        artifact_path = self._write_backtest_artifact(summary_payload)
        backtest_run_id = self.repository.create_backtest_run(
            strategy_id="overlap_v1",
            strategy_set_key=strategy_set_key,
            strategy_ids=normalized_strategy_ids,
            start_date=start_date,
            end_date=end_date,
            parameters=summary_payload["parameters"],
            summary=summary_payload["summary"],
            job_run_id=job_run_id,
            artifact_path=str(artifact_path),
            json_report_path=str(artifact_path),
        )
        self.repository.replace_backtest_run_trades(backtest_run_id, trades)
        summary_payload["backtest_run_id"] = backtest_run_id
        summary_payload["artifact_path"] = str(artifact_path)
        return summary_payload

    def list_backtest_runs(self, *, limit: int = 30) -> list[dict[str, Any]]:
        return self.repository.list_backtest_runs_v2(limit=limit)

    def get_backtest_run(self, run_id: int) -> dict[str, Any] | None:
        return self.repository.get_backtest_run_v2(run_id)

    def _compute_hold_results(
        self,
        *,
        ticker_frame: Any,
        benchmark_frame: Any,
        signal_date: dt.date,
        hold_periods: list[int],
    ) -> dict[str, Any]:
        if ticker_frame is None or getattr(ticker_frame, "empty", True):
            return {}
        ticker_dates = [index.date() if hasattr(index, "date") else index for index in ticker_frame.index]
        if signal_date not in ticker_dates:
            return {}
        entry_index = ticker_dates.index(signal_date)
        entry_row = ticker_frame.iloc[entry_index]
        entry_price = float(entry_row["Close"])
        hold_results: dict[str, Any] = {}
        for hold in hold_periods:
            exit_index = entry_index + hold
            if exit_index >= len(ticker_frame.index):
                continue
            exit_row = ticker_frame.iloc[exit_index]
            exit_date_value = ticker_frame.index[exit_index]
            exit_date = exit_date_value.date() if hasattr(exit_date_value, "date") else exit_date_value
            exit_price = float(exit_row["Close"])
            return_pct = ((exit_price / entry_price) - 1.0) * 100.0 if entry_price else 0.0
            spy_return_pct = None
            if benchmark_frame is not None and not getattr(benchmark_frame, "empty", True):
                benchmark_dates = [index.date() if hasattr(index, "date") else index for index in benchmark_frame.index]
                if signal_date in benchmark_dates:
                    benchmark_entry_index = benchmark_dates.index(signal_date)
                    benchmark_exit_index = benchmark_entry_index + hold
                    if benchmark_exit_index < len(benchmark_frame.index):
                        benchmark_entry_price = float(benchmark_frame.iloc[benchmark_entry_index]["Close"])
                        benchmark_exit_price = float(benchmark_frame.iloc[benchmark_exit_index]["Close"])
                        if benchmark_entry_price:
                            spy_return_pct = ((benchmark_exit_price / benchmark_entry_price) - 1.0) * 100.0
            hold_results[str(hold)] = {
                "hold_days": hold,
                "exit_date": exit_date.isoformat(),
                "exit_price": round(exit_price, 4),
                "return_pct": round(return_pct, 4),
                "spy_return_pct": round(spy_return_pct, 4) if spy_return_pct is not None else None,
                "excess_return_pct": round(return_pct - spy_return_pct, 4) if spy_return_pct is not None else None,
            }
        if not hold_results:
            return {}
        entry_date_value = ticker_frame.index[entry_index]
        entry_date = entry_date_value.date() if hasattr(entry_date_value, "date") else entry_date_value
        return {
            "entry_date": entry_date,
            "entry_price": round(entry_price, 4),
            "holds": hold_results,
        }

    def _build_backtest_summary(
        self,
        *,
        start_date: dt.date,
        end_date: dt.date,
        strategy_ids: list[str],
        strategy_set_key: str,
        entry_signal_threshold: int,
        hold_periods: list[int],
        trades: list[dict[str, Any]],
    ) -> dict[str, Any]:
        per_hold: dict[str, Any] = {}
        for hold in hold_periods:
            results = [trade["hold_results_json"].get(str(hold)) for trade in trades if str(hold) in trade["hold_results_json"]]
            returns = [float(item["return_pct"]) for item in results]
            spy_returns = [float(item["spy_return_pct"]) for item in results if item.get("spy_return_pct") is not None]
            excess_returns = [float(item["excess_return_pct"]) for item in results if item.get("excess_return_pct") is not None]
            per_hold[str(hold)] = {
                "trade_count": len(results),
                "avg_return_pct": round(statistics.fmean(returns), 4) if returns else None,
                "median_return_pct": round(statistics.median(returns), 4) if returns else None,
                "win_rate_pct": round((sum(1 for value in returns if value > 0) / len(returns)) * 100.0, 2) if returns else None,
                "avg_spy_return_pct": round(statistics.fmean(spy_returns), 4) if spy_returns else None,
                "avg_excess_return_pct": round(statistics.fmean(excess_returns), 4) if excess_returns else None,
            }
        return {
            "parameters": {
                "strategy_ids": strategy_ids,
                "strategy_set_key": strategy_set_key,
                "start_date": start_date.isoformat(),
                "end_date": end_date.isoformat(),
                "entry_signal_threshold": int(entry_signal_threshold),
                "hold_periods": hold_periods,
                "benchmark_ticker": self.app_config.benchmark_ticker.upper(),
                "entry_price_basis": "same_day_close",
            },
            "summary": {
                "trade_count": len(trades),
                "holds": per_hold,
            },
            "trades": [
                {
                    "signal_date": trade["signal_date"].isoformat(),
                    "ticker": trade["ticker"],
                    "signal_count": trade["signal_count"],
                    "contributing_strategies": trade["contributing_strategies_json"],
                    "entry_date": trade["entry_date"].isoformat(),
                    "entry_price": trade["entry_price"],
                    "hold_results": trade["hold_results_json"],
                }
                for trade in trades
            ],
        }

    def _empty_backtest_summary(
        self,
        *,
        start_date: dt.date,
        end_date: dt.date,
        strategy_ids: list[str],
        strategy_set_key: str,
        entry_signal_threshold: int,
        hold_periods: list[int],
    ) -> dict[str, Any]:
        return self._build_backtest_summary(
            start_date=start_date,
            end_date=end_date,
            strategy_ids=strategy_ids,
            strategy_set_key=strategy_set_key,
            entry_signal_threshold=entry_signal_threshold,
            hold_periods=hold_periods,
            trades=[],
        )

    def _write_overlap_artifact(self, run_date: dt.date, payload: dict[str, Any]) -> Path:
        path = self.artifacts_dir / "raw" / f"daily_overlap_summary_{run_date.isoformat()}.json"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        return path

    def _write_backtest_artifact(self, payload: dict[str, Any]) -> Path:
        params = payload["parameters"]
        timestamp = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        path = self.artifacts_dir / "raw" / (
            f"overlap_backtest_v1_{params['start_date']}_{params['end_date']}_{timestamp}.json"
        )
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        return path
