from __future__ import annotations

import datetime as dt
import hashlib
import json
from pathlib import Path
from typing import Any

from src.config import load_app_config
from src.webapp.repositories.history_repository import HistoryRepository


def stable_json_hash(payload: dict[str, Any]) -> str:
    return hashlib.sha256(json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")).hexdigest()[:16]


class ScreenerHistoryService:
    def __init__(
        self,
        *,
        database_url: str = "",
        artifacts_dir: Path | None = None,
        repository: HistoryRepository | None = None,
    ) -> None:
        self.repository = repository or HistoryRepository(database_url=database_url, artifacts_dir=artifacts_dir)
        self.artifacts_dir = artifacts_dir

    def is_configured(self) -> bool:
        return self.repository.is_configured()

    def list_runs(
        self,
        *,
        strategy_id: str = "",
        start_date: dt.date | None = None,
        end_date: dt.date | None = None,
        include_deleted: bool = False,
        config_hash: str = "",
        has_hits: bool | None = None,
        limit: int = 50,
        offset: int = 0,
    ) -> list[dict[str, Any]]:
        return self.repository.list_screen_runs(
            strategy_id=strategy_id,
            start_date=start_date,
            end_date=end_date,
            include_deleted=include_deleted,
            config_hash=config_hash,
            has_hits=has_hits,
            limit=limit,
            offset=offset,
        )

    def get_run(self, run_id: int, *, include_hits: bool = False, hit_limit: int = 200, hit_offset: int = 0) -> dict[str, Any] | None:
        return self.repository.get_screen_run(run_id, include_hits=include_hits, hit_limit=hit_limit, hit_offset=hit_offset)

    def soft_delete(self, run_id: int, *, reason: str) -> bool:
        clean_reason = str(reason).strip() or "Deleted from webapp"
        return self.repository.soft_delete_screen_run(run_id, reason=clean_reason)

    def list_signal_cache_summary(
        self,
        *,
        strategy_ids: list[str] | None = None,
        start_date: dt.date | None = None,
        end_date: dt.date | None = None,
    ) -> list[dict[str, Any]]:
        return self.repository.list_signal_cache_summary(strategy_ids=strategy_ids, start_date=start_date, end_date=end_date)

    def list_signal_cache_calendar(
        self,
        *,
        strategy_ids: list[str] | None = None,
        start_date: dt.date,
        end_date: dt.date,
        include_deleted: bool = False,
    ) -> list[dict[str, Any]]:
        rows = self.repository.list_signal_cache_calendar(
            strategy_ids=strategy_ids,
            start_date=start_date,
            end_date=end_date,
            include_deleted=include_deleted,
        )
        selected_count = len(strategy_ids or [])
        grouped: dict[dt.date, list[dict[str, Any]]] = {}
        for row in rows:
            run_date = row.get("run_date")
            if not isinstance(run_date, dt.date):
                continue
            grouped.setdefault(run_date, []).append(row)

        result: list[dict[str, Any]] = []
        cursor = start_date
        while cursor <= end_date:
            day_rows = grouped.get(cursor, [])
            strategy_map: dict[str, dict[str, Any]] = {}
            for row in day_rows:
                strategy_id = str(row.get("strategy_id") or "")
                if not strategy_id or strategy_id in strategy_map:
                    continue
                strategy_map[strategy_id] = {
                    "run_id": int(row["id"]),
                    "strategy_id": strategy_id,
                    "hit_count": int(row.get("hit_count") or 0),
                    "failure_count": int(row.get("failure_count") or 0),
                    "market_data_mode": str(row.get("market_data_mode") or ""),
                    "source_kind": str(row.get("source_kind") or ""),
                    "deleted_at": row.get("deleted_at"),
                    "deleted_reason": row.get("deleted_reason"),
                    "created_at": row.get("created_at"),
                }
            strategies = sorted(strategy_map.values(), key=lambda item: (item["strategy_id"], item["created_at"] or ""))
            cached_strategy_count = len(strategies)
            hit_strategy_count = sum(1 for item in strategies if int(item["hit_count"]) > 0)
            total_hits = sum(int(item["hit_count"]) for item in strategies)
            expected_strategy_count = selected_count if selected_count > 0 else cached_strategy_count
            status = "none"
            if cached_strategy_count == 0:
                status = "none"
            elif expected_strategy_count > 0 and cached_strategy_count < expected_strategy_count:
                status = "partial"
            elif hit_strategy_count > 0:
                status = "cached_with_hits"
            else:
                status = "cached_no_hits"
            result.append(
                {
                    "date": cursor.isoformat(),
                    "strategy_count": expected_strategy_count,
                    "cached_strategy_count": cached_strategy_count,
                    "hit_strategy_count": hit_strategy_count,
                    "total_hits": total_hits,
                    "status": status,
                    "strategies": strategies,
                }
            )
            cursor += dt.timedelta(days=1)
        return result

    def persist_screen_run(
        self,
        *,
        strategy_id: str,
        options: dict[str, Any],
        summary_payload: dict[str, Any],
        raw_payload: dict[str, Any],
        job_run_id: int | None = None,
    ) -> int | None:
        config_json = load_app_config().to_dict()
        if options.get("limit") is not None:
            config_json["max_tickers"] = int(options["limit"])
        scope_json = {
            "limit": options.get("limit"),
            "tickers": list(options.get("tickers") or []),
            "filter_precedence": options.get("filter_precedence"),
            "include_sectors": list(options.get("include_sectors") or []),
            "exclude_sectors": list(options.get("exclude_sectors") or []),
            "include_industries": list(options.get("include_industries") or []),
            "exclude_industries": list(options.get("exclude_industries") or []),
            "include_themes": list(options.get("include_themes") or []),
            "exclude_themes": list(options.get("exclude_themes") or []),
            "source": options.get("source"),
            "reference_date": options.get("reference_date"),
        }
        config_hash = stable_json_hash(config_json)
        scope_hash = stable_json_hash(scope_json)
        signal_date = self._resolve_run_date(summary_payload, options)
        failed_ticker_count = self._coerce_failure_count(summary_payload.get("failed_tickers"))
        result_summary = {
            "date_label": summary_payload.get("date_label"),
            "as_of_date": summary_payload.get("as_of_date"),
            "total_tickers": int(summary_payload.get("total_tickers") or 0),
            "passed_tickers": int(summary_payload.get("passed_tickers") or 0),
            "failed_tickers": failed_ticker_count,
        }
        source_kind = str(summary_payload.get("source") or ("manual-tickers" if scope_json["tickers"] else "exchange-universe"))
        screen_run_id = self.repository.upsert_screen_run(
            strategy_id=strategy_id,
            run_date=signal_date,
            job_run_id=job_run_id,
            config_json=config_json,
            config_hash=config_hash,
            scope_json=scope_json,
            scope_hash=scope_hash,
            market_data_mode=str(options.get("market_data_source") or "internet"),
            source_kind=source_kind,
            hit_count=int(summary_payload.get("passed_tickers") or 0),
            failure_count=failed_ticker_count,
            result_summary_json=result_summary,
            raw_artifact_path=str(summary_payload.get("raw_results_file") or ""),
            watchlist_artifact_path=str(summary_payload.get("watchlist_file") or ""),
        )
        rows = self._build_hit_rows(strategy_id=strategy_id, signal_date=signal_date, raw_payload=raw_payload)
        self.repository.replace_screen_run_hits(screen_run_id, rows)
        return screen_run_id

    def persist_metric_snapshot_run(
        self,
        *,
        strategy_id: str,
        options: dict[str, Any],
        summary_payload: dict[str, Any],
        raw_artifact_path: str,
        watchlist_artifact_path: str,
        job_run_id: int | None = None,
    ) -> int | None:
        config_json = load_app_config().to_dict()
        scope_json = {
            "source": options.get("source"),
            "as_of_date": options.get("as_of_date"),
            "tickers": [],
        }
        config_hash = stable_json_hash(config_json)
        scope_hash = stable_json_hash(scope_json)
        signal_date = self._resolve_run_date(summary_payload, options)
        result_summary = dict(summary_payload)
        result_summary.setdefault("date_label", signal_date.isoformat())
        result_summary.setdefault("as_of_date", signal_date.isoformat())
        screen_run_id = self.repository.upsert_screen_run(
            strategy_id=strategy_id,
            run_date=signal_date,
            job_run_id=job_run_id,
            config_json=config_json,
            config_hash=config_hash,
            scope_json=scope_json,
            scope_hash=scope_hash,
            market_data_mode="api",
            source_kind=str(summary_payload.get("source") or "market-snapshot"),
            hit_count=0,
            failure_count=int(summary_payload.get("failed_tickers") or 0),
            result_summary_json=result_summary,
            raw_artifact_path=raw_artifact_path,
            watchlist_artifact_path=watchlist_artifact_path,
            notes="Market metric snapshot",
        )
        self.repository.replace_screen_run_hits(screen_run_id, [])
        return screen_run_id

    def persist_snapshot_run(
        self,
        *,
        strategy_id: str,
        run_date: dt.date,
        summary_payload: dict[str, Any],
        hit_rows: list[dict[str, Any]],
        config_json: dict[str, Any] | None = None,
        scope_json: dict[str, Any] | None = None,
        market_data_mode: str = "derived",
        source_kind: str = "derived-snapshot",
        notes: str = "",
        job_run_id: int | None = None,
    ) -> int | None:
        resolved_config_json = dict(config_json or {})
        resolved_scope_json = dict(scope_json or {})
        screen_run_id = self.repository.upsert_screen_run(
            strategy_id=strategy_id,
            run_date=run_date,
            job_run_id=job_run_id,
            config_json=resolved_config_json,
            config_hash=stable_json_hash(resolved_config_json),
            scope_json=resolved_scope_json,
            scope_hash=stable_json_hash(resolved_scope_json),
            market_data_mode=market_data_mode,
            source_kind=source_kind,
            hit_count=len(hit_rows),
            failure_count=int(summary_payload.get("failed_tickers") or 0),
            result_summary_json=dict(summary_payload),
            raw_artifact_path="",
            watchlist_artifact_path="",
            notes=notes,
        )
        self.repository.replace_screen_run_hits(screen_run_id, hit_rows)
        return screen_run_id

    def _coerce_failure_count(self, raw_value: Any) -> int:
        if isinstance(raw_value, list):
            return len(raw_value)
        if raw_value in (None, ""):
            return 0
        return int(raw_value)

    def _resolve_run_date(self, summary_payload: dict[str, Any], options: dict[str, Any]) -> dt.date:
        for value in (
            summary_payload.get("as_of_date"),
            options.get("as_of_date"),
            summary_payload.get("date_label"),
            options.get("date_label"),
        ):
            if isinstance(value, str) and value.strip():
                try:
                    return dt.date.fromisoformat(value.strip())
                except ValueError:
                    continue
        return dt.date.today()

    def _build_hit_rows(self, *, strategy_id: str, signal_date: dt.date, raw_payload: dict[str, Any]) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        hits = raw_payload.get("hits")
        if isinstance(hits, list):
            for index, item in enumerate(hits, start=1):
                if not isinstance(item, dict):
                    continue
                ticker = self._extract_ticker(item)
                if not ticker:
                    continue
                reasons = item.get("reasons")
                rows.append(
                    {
                        "strategy_id": strategy_id,
                        "signal_date": signal_date,
                        "ticker": ticker,
                        "passed": True,
                        "rank": index,
                        "metrics_json": self._extract_metrics(item),
                        "reasons_json": list(reasons) if isinstance(reasons, list) else [],
                        "hit_payload_json": item,
                    }
                )

        failures = raw_payload.get("failed_tickers")
        if isinstance(failures, list):
            for item in failures:
                if not isinstance(item, dict):
                    continue
                ticker = self._extract_ticker(item)
                if not ticker:
                    continue
                rows.append(
                    {
                        "strategy_id": strategy_id,
                        "signal_date": signal_date,
                        "ticker": ticker,
                        "passed": False,
                        "rank": None,
                        "metrics_json": {},
                        "reasons_json": [str(item.get("error") or "").strip()] if item.get("error") else [],
                        "hit_payload_json": item,
                    }
                )
        return rows

    def _extract_ticker(self, payload: dict[str, Any]) -> str:
        for key in ("ticker", "symbol"):
            value = payload.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip().upper()
        return ""

    def _extract_metrics(self, payload: dict[str, Any]) -> dict[str, Any]:
        preferred = {}
        for key in ("score", "lastPrice", "triggerPrice", "entryPrice", "breakout_date", "signal_date"):
            if key in payload:
                preferred[key] = payload[key]
        return preferred
