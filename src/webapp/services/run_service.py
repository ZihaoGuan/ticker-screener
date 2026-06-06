from __future__ import annotations

from dataclasses import dataclass
import datetime as dt
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import time
import threading
import uuid
from typing import Any

from src.artifact_paths import watchlist_stem_from_path
from src.config import load_app_config
from src.market_data_access import db_frame_has_recent_coverage, load_many_ticker_windows
from src.screener_catalog import build_screener_catalog
from src.universe_filters import build_filter_option_catalog
from src.universe import UniverseTicker, load_universe
from src.universe_filters import UniverseFilterCriteria, filter_universe_by_criteria
from src.webapp.services.screener_history_service import ScreenerHistoryService
from src.webapp.repositories.history_repository import HistoryRepository


@dataclass(frozen=True)
class RunAction:
    action_id: str
    label: str
    script_path: str
    supports_limit: bool = True
    extra_args: tuple[str, ...] = ()
    fields: tuple["RunField", ...] = ()
    visible_in_runs: bool = True


@dataclass(frozen=True)
class RunField:
    field_id: str
    label: str
    field_type: str
    placeholder: str | None = None
    help_text: str | None = None
    options: tuple[tuple[str, str], ...] = ()


class RunService:
    _progress_pattern = re.compile(r"\[(\d{1,6})/(\d{1,6})\]")
    _passed_pattern = re.compile(r"passed=(\d{1,6})")
    _summary_path_pattern = re.compile(r"Wrote run summary to (.+)$")
    _watchlist_path_pattern = re.compile(r"Wrote watchlist to (.+)$")
    _filter_catalog_cache: dict[str, dict[str, list[str]]] = {}
    _limit_field = RunField(
        "limit",
        "Universe Limit",
        "number",
        placeholder="Optional",
        help_text="Leave blank to scan the full configured universe.",
    )
    _tickers_field = RunField(
        "tickers",
        "Tickers",
        "text",
        placeholder="AAPL NVDA CRWD",
        help_text="Optional space- or comma-separated ticker list.",
    )
    _date_label_field = RunField(
        "date_label",
        "Date Label",
        "date",
        help_text="Optional artifact label override.",
    )
    _as_of_date_field = RunField(
        "as_of_date",
        "As Of Date",
        "date",
        help_text="Optional historical replay date.",
    )
    _source_field = RunField(
        "source",
        "Source",
        "select",
        help_text="Choose whether PEG scans the full universe or the earnings watchlist.",
        options=(("universe", "Exchange Universe"), ("earnings-watchlist", "Earnings Watchlist")),
    )
    _reference_date_field = RunField(
        "reference_date",
        "Reference Date",
        "date",
        help_text="Optional date anchor for the earnings watchlist source.",
    )
    _market_data_source_field = RunField(
        "market_data_source",
        "Market Data Source",
        "select",
        help_text="Choose whether screeners pull directly from the internet or prefer Postgres daily_bars and fall back to the internet if needed.",
        options=(("internet", "Internet"), ("database-first", "Database First, Fallback to Internet")),
    )
    _filter_precedence_field = RunField(
        "filter_precedence",
        "Filter Precedence",
        "select",
        help_text="Choose which side wins when the same sector, industry, or theme appears in both include and exclude.",
        options=(("exclude", "Exclude First"), ("include", "Include First")),
    )
    _include_sectors_field = RunField("include_sectors", "Only Sectors", "multiselect")
    _exclude_sectors_field = RunField("exclude_sectors", "Exclude Sectors", "multiselect")
    _include_industries_field = RunField("include_industries", "Only Industries", "multiselect")
    _exclude_industries_field = RunField("exclude_industries", "Exclude Industries", "multiselect")
    _include_themes_field = RunField("include_themes", "Only Themes", "multiselect")
    _exclude_themes_field = RunField("exclude_themes", "Exclude Themes", "multiselect")
    _actions = {
        "screener_history_batch": RunAction(
            "screener_history_batch",
            "Batch Screener History Cache",
            "scripts/run_screener_history_batch.py",
            supports_limit=False,
            visible_in_runs=False,
        ),
        "backtest_v1": RunAction(
            "backtest_v1",
            "Run Backtest",
            "scripts/run_backtest.py",
            supports_limit=False,
            visible_in_runs=False,
        ),
        "sync_postgres_market_data": RunAction(
            "sync_postgres_market_data",
            "Sync Postgres Market Data",
            "scripts/sync_postgres_market_data.py",
            supports_limit=False,
            fields=(
                _tickers_field,
            ),
            visible_in_runs=False,
        ),
        "earnings_weekly_criteria": RunAction(
            "earnings_weekly_criteria",
            "Run Earnings Weekly Criteria",
            "scripts/run_earnings_weekly_criteria_screen.py",
            fields=(
                _limit_field,
                _date_label_field,
                _reference_date_field,
            ),
        ),
        "rs": RunAction(
            "rs",
            "Run RS",
            "scripts/run_rs_screen.py",
            fields=(
                _limit_field,
                _tickers_field,
                _date_label_field,
                _as_of_date_field,
                _market_data_source_field,
                _filter_precedence_field,
                _include_sectors_field,
                _exclude_sectors_field,
                _include_industries_field,
                _exclude_industries_field,
                _include_themes_field,
                _exclude_themes_field,
            ),
        ),
        "vcp": RunAction(
            "vcp",
            "Run VCP",
            "scripts/run_vcp_screen.py",
            fields=(
                _limit_field,
                _tickers_field,
                _date_label_field,
                _as_of_date_field,
                _market_data_source_field,
                _filter_precedence_field,
                _include_sectors_field,
                _exclude_sectors_field,
                _include_industries_field,
                _exclude_industries_field,
                _include_themes_field,
                _exclude_themes_field,
            ),
        ),
        "cup_handle": RunAction(
            "cup_handle",
            "Run Cup Handle",
            "scripts/run_cup_handle_screen.py",
            fields=(
                _limit_field,
                _tickers_field,
                _date_label_field,
                _as_of_date_field,
                _market_data_source_field,
                _filter_precedence_field,
                _include_sectors_field,
                _exclude_sectors_field,
                _include_industries_field,
                _exclude_industries_field,
                _include_themes_field,
                _exclude_themes_field,
            ),
        ),
        "gap_fill": RunAction(
            "gap_fill",
            "Run Gap Fill",
            "scripts/run_gap_fill_screen.py",
            fields=(
                _limit_field,
                _tickers_field,
                _date_label_field,
                _as_of_date_field,
                _market_data_source_field,
                _filter_precedence_field,
                _include_sectors_field,
                _exclude_sectors_field,
                _include_industries_field,
                _exclude_industries_field,
                _include_themes_field,
                _exclude_themes_field,
            ),
        ),
        "hve": RunAction(
            "hve",
            "Run HVE",
            "scripts/run_hve_screen.py",
            fields=(
                _limit_field,
                _tickers_field,
                _date_label_field,
                _as_of_date_field,
                _market_data_source_field,
                _filter_precedence_field,
                _include_sectors_field,
                _exclude_sectors_field,
                _include_industries_field,
                _exclude_industries_field,
                _include_themes_field,
                _exclude_themes_field,
            ),
        ),
        "inside_dryup": RunAction(
            "inside_dryup",
            "Run Inside Dry-Up",
            "scripts/run_inside_dryup_screen.py",
            fields=(
                _limit_field,
                _tickers_field,
                _date_label_field,
                _as_of_date_field,
                _market_data_source_field,
                _filter_precedence_field,
                _include_sectors_field,
                _exclude_sectors_field,
                _include_industries_field,
                _exclude_industries_field,
                _include_themes_field,
                _exclude_themes_field,
            ),
        ),
        "ftd_sweep": RunAction(
            "ftd_sweep",
            "Run FTD Sweep",
            "scripts/run_ftd_sweep_screen.py",
            fields=(
                _limit_field,
                _tickers_field,
                _date_label_field,
                _as_of_date_field,
                _market_data_source_field,
                _filter_precedence_field,
                _include_sectors_field,
                _exclude_sectors_field,
                _include_industries_field,
                _exclude_industries_field,
                _include_themes_field,
                _exclude_themes_field,
            ),
        ),
        "fearzone": RunAction(
            "fearzone",
            "Run Fearzone",
            "scripts/run_fearzone_screen.py",
            fields=(
                _limit_field,
                _tickers_field,
                _date_label_field,
                _as_of_date_field,
                _market_data_source_field,
                _filter_precedence_field,
                _include_sectors_field,
                _exclude_sectors_field,
                _include_industries_field,
                _exclude_industries_field,
                _include_themes_field,
                _exclude_themes_field,
            ),
        ),
        "weekly_htf_pullback": RunAction(
            "weekly_htf_pullback",
            "Run Weekly HTF Pullback",
            "scripts/run_weekly_htf_pullback_screen.py",
            fields=(
                _limit_field,
                _tickers_field,
                _date_label_field,
                _as_of_date_field,
                _market_data_source_field,
                _filter_precedence_field,
                _include_sectors_field,
                _exclude_sectors_field,
                _include_industries_field,
                _exclude_industries_field,
                _include_themes_field,
                _exclude_themes_field,
            ),
        ),
        "htf_8w_runup": RunAction(
            "htf_8w_runup",
            "Run HTF 8W Runup",
            "scripts/run_htf_runup_screen.py",
            fields=(
                _limit_field,
                _tickers_field,
                _date_label_field,
                _as_of_date_field,
                _market_data_source_field,
                _filter_precedence_field,
                _include_sectors_field,
                _exclude_sectors_field,
                _include_industries_field,
                _exclude_industries_field,
                _include_themes_field,
                _exclude_themes_field,
            ),
        ),
        "weekly_rs": RunAction(
            "weekly_rs",
            "Run Weekly RS",
            "scripts/run_weekly_rs_screen.py",
            fields=(
                _limit_field,
                _tickers_field,
                _date_label_field,
                _market_data_source_field,
                _filter_precedence_field,
                _include_sectors_field,
                _exclude_sectors_field,
                _include_industries_field,
                _exclude_industries_field,
                _include_themes_field,
                _exclude_themes_field,
            ),
        ),
        "near_200ma": RunAction(
            "near_200ma",
            "Run Near 200MA",
            "scripts/run_near_200ma_screen.py",
            fields=(
                _limit_field,
                _tickers_field,
                _date_label_field,
                _market_data_source_field,
                _filter_precedence_field,
                _include_sectors_field,
                _exclude_sectors_field,
                _include_industries_field,
                _exclude_industries_field,
                _include_themes_field,
                _exclude_themes_field,
            ),
        ),
        "lost_21ema": RunAction(
            "lost_21ema",
            "Run Lost 21EMA",
            "scripts/run_lost_21ema_screen.py",
            fields=(
                _limit_field,
                _tickers_field,
                _date_label_field,
                _market_data_source_field,
                _filter_precedence_field,
                _include_sectors_field,
                _exclude_sectors_field,
                _include_industries_field,
                _exclude_industries_field,
                _include_themes_field,
                _exclude_themes_field,
            ),
        ),
        "legacy_peg": RunAction(
            "legacy_peg",
            "Run Legacy PEG",
            "scripts/run_peg_screen.py",
            extra_args=("--strategy-profile", "legacy"),
            fields=(
                _limit_field,
                _tickers_field,
                _date_label_field,
                _as_of_date_field,
                _source_field,
                _reference_date_field,
                _market_data_source_field,
                _filter_precedence_field,
                _include_sectors_field,
                _exclude_sectors_field,
                _include_industries_field,
                _exclude_industries_field,
                _include_themes_field,
                _exclude_themes_field,
            ),
        ),
        "sean_peg": RunAction(
            "sean_peg",
            "Run Sean PEG",
            "scripts/run_peg_screen.py",
            extra_args=("--strategy-profile", "sean-peg"),
            fields=(
                _limit_field,
                _tickers_field,
                _date_label_field,
                _as_of_date_field,
                _source_field,
                _reference_date_field,
                _market_data_source_field,
                _filter_precedence_field,
                _include_sectors_field,
                _exclude_sectors_field,
                _include_industries_field,
                _exclude_industries_field,
                _include_themes_field,
                _exclude_themes_field,
            ),
        ),
    }
    _jobs_lock = threading.Lock()
    _jobs: list[dict[str, Any]] = []
    _jobs_by_id: dict[str, dict[str, Any]] = {}

    def __init__(self, project_root: Path, *, database_url: str = "", artifacts_dir: Path | None = None) -> None:
        self.project_root = project_root
        self.database_url = database_url
        self.artifacts_dir = artifacts_dir or (project_root / "artifacts")
        self.history_repository = HistoryRepository(database_url=database_url, artifacts_dir=self.artifacts_dir)
        self.screener_history_service = ScreenerHistoryService(
            database_url=database_url,
            artifacts_dir=self.artifacts_dir,
            repository=self.history_repository,
        )

    def list_actions(self) -> list[dict[str, Any]]:
        filter_catalog = self._get_filter_catalog()
        return [
            {
                "id": action.action_id,
                "label": action.label,
                "command": " ".join([sys.executable, action.script_path, *action.extra_args]).strip(),
                "supports_limit": action.supports_limit,
                "fields": [
                    {
                        "id": field.field_id,
                        "label": field.label,
                        "type": field.field_type,
                        "placeholder": field.placeholder,
                        "help_text": field.help_text,
                        "options": self._field_options(field, filter_catalog),
                    }
                    for field in action.fields
                ],
            }
            for action in self._actions.values()
            if action.visible_in_runs
        ]

    def list_jobs(self, limit: int = 20) -> list[dict[str, Any]]:
        with self._jobs_lock:
            jobs = [self._serialize_job(item) for item in self._jobs[:limit]]
        return self._attach_child_jobs(jobs)

    def get_job(self, job_id: str) -> dict[str, Any]:
        with self._jobs_lock:
            job = self._jobs_by_id.get(job_id)
            if job is None:
                raise ValueError(f"Unknown job: {job_id}")
            jobs = [self._serialize_job(job)]
        enriched = self._attach_child_jobs(jobs)
        return enriched[0]

    def precheck(self, action_id: str, *, options: dict[str, Any] | None = None) -> dict[str, Any]:
        action = self._actions.get(action_id)
        if action is None:
            raise ValueError(f"Unknown run action: {action_id}")
        normalized = self._normalize_options(action, options or {})
        market_data_source = str(normalized.get("market_data_source") or "internet").strip().lower()
        if market_data_source != "database-first":
            return {
                "applicable": False,
                "configured": self.history_repository.is_configured(),
                "action_id": action_id,
                "market_data_source": market_data_source or "internet",
                "message": "DB coverage precheck is only used for database-first runs.",
            }
        if not self.history_repository.is_configured():
            return {
                "applicable": False,
                "configured": False,
                "action_id": action_id,
                "market_data_source": market_data_source,
                "message": "Database URL is not configured for DB coverage precheck.",
            }

        config = load_app_config()
        catalog = build_screener_catalog(config)
        spec = catalog.get(action_id)
        if spec is None:
            return {
                "applicable": False,
                "configured": True,
                "action_id": action_id,
                "market_data_source": market_data_source,
                "message": "DB coverage precheck is not available for this screener yet.",
            }

        target_date = self._resolve_as_of_date(normalized)
        lookback_trading_days = int(spec.lookback_trading_days) + int(spec.warmup_trading_days)
        universe = self._resolve_precheck_universe(config=config, normalized=normalized)
        universe_symbols = [item.symbol.upper() for item in universe]
        benchmark_ticker = config.benchmark_ticker.upper()
        query_tickers = universe_symbols + ([benchmark_ticker] if "benchmark_bars" in spec.required_inputs else [])
        frames = load_many_ticker_windows(
            query_tickers,
            target_date,
            lookback_trading_days,
            database_url=self.database_url,
        )
        benchmark_ready = True
        benchmark_bar_count = None
        if "benchmark_bars" in spec.required_inputs:
            benchmark_frame = frames.get(benchmark_ticker)
            benchmark_bar_count = len(benchmark_frame) if benchmark_frame is not None else 0
            benchmark_ready = self._frame_is_db_ready(benchmark_frame, target_date, lookback_trading_days)

        db_ready_tickers = 0
        fallback_tickers: list[str] = []
        for symbol in universe_symbols:
            frame = frames.get(symbol)
            ticker_ready = self._frame_is_db_ready(frame, target_date, lookback_trading_days)
            if ticker_ready and benchmark_ready:
                db_ready_tickers += 1
            else:
                fallback_tickers.append(symbol)

        total_tickers = len(universe_symbols)
        return {
            "applicable": True,
            "configured": True,
            "action_id": action_id,
            "market_data_source": market_data_source,
            "as_of_date": target_date.isoformat(),
            "lookback_trading_days": lookback_trading_days,
            "total_tickers": total_tickers,
            "db_ready_tickers": db_ready_tickers,
            "fallback_tickers": len(fallback_tickers),
            "db_ready_pct": round((db_ready_tickers / total_tickers) * 100, 1) if total_tickers > 0 else 0.0,
            "sample_fallback_tickers": fallback_tickers[:12],
            "benchmark": {
                "ticker": benchmark_ticker,
                "required": "benchmark_bars" in spec.required_inputs,
                "db_ready": benchmark_ready,
                "bar_count": benchmark_bar_count,
            },
            "notes": [
                "Counts estimate whether DB coverage is good enough before fallback would be needed.",
                "Fallback-needed means at least one required DB input looks incomplete or too stale for this screener.",
            ],
        }

    def cancel(self, job_id: str) -> dict[str, Any]:
        with self._jobs_lock:
            job = self._jobs_by_id.get(job_id)
            if job is None:
                raise ValueError(f"Unknown job: {job_id}")
            process = job.get("_process")
            if job.get("status") != "running" or process is None:
                raise ValueError(f"Job is not running: {job_id}")
            job["cancel_requested"] = True
            self._append_log_line(job, f"Cancellation requested at {self._now_iso()}")
            process.terminate()
            return self._serialize_job(job)

    def launch(self, action_id: str, *, options: dict[str, Any] | None = None) -> str:
        action = self._actions.get(action_id)
        if action is None:
            raise ValueError(f"Unknown run action: {action_id}")

        normalized = self._normalize_options(action, options or {})
        request_payload = {"action_id": action_id, "options": normalized}
        job_run_id = self.history_repository.create_job_run(
            job_type=self._job_type_for_action(action_id),
            job_name=action.label,
            status="running",
            trigger_source="manual",
            request_payload=request_payload,
            parent_job_run_id=None,
        )
        if job_run_id is not None:
            normalized["job_run_id"] = job_run_id
        job_id = uuid.uuid4().hex[:12]
        started_at = dt.datetime.now(dt.timezone.utc).replace(microsecond=0).isoformat()
        command = self.build_command(action_id, normalized, normalized=True)

        job = {
            "job_id": job_id,
            "action_id": action_id,
            "job_run_id": job_run_id,
            "label": action.label,
            "status": "running",
            "command": " ".join(command),
            "started_at": started_at,
            "finished_at": "",
            "return_code": None,
            "log_tail": "Starting...\n",
            "progress_current": None,
            "progress_total": None,
            "progress_percent": None,
            "progress_label": "Starting…",
            "success_count": 0,
            "watchlist_file": "",
            "summary_file": "",
            "cancel_requested": False,
            "options": normalized,
            "_started_monotonic": time.monotonic(),
        }

        with self._jobs_lock:
            self._jobs.insert(0, job)
            self._jobs_by_id[job_id] = job
            del self._jobs[50:]

        env = os.environ.copy()
        if normalized.get("market_data_source"):
            env["TICKER_SCREENER_MARKET_DATA_SOURCE"] = str(normalized["market_data_source"])
        if self.database_url:
            env["TICKER_SCREENER_DATABASE_URL"] = self.database_url
        thread = threading.Thread(target=self._run_job, args=(job_id, command, env), daemon=True)
        thread.start()
        return job_id

    def build_command(self, action_id: str, options: dict[str, Any] | None = None, *, normalized: bool = False) -> list[str]:
        action = self._actions.get(action_id)
        if action is None:
            raise ValueError(f"Unknown run action: {action_id}")

        normalized_options = dict(options or {}) if normalized else self._normalize_options(action, options or {})
        command = [sys.executable, action.script_path]
        command.extend(action.extra_args)
        if action.supports_limit and normalized_options.get("limit") is not None:
            command.extend(["--limit", str(normalized_options["limit"])])
        if normalized_options.get("tickers"):
            command.append("--tickers")
            command.extend(normalized_options["tickers"])
        if normalized_options.get("date_label"):
            command.extend(["--date-label", str(normalized_options["date_label"])])
        if normalized_options.get("as_of_date"):
            command.extend(["--as-of-date", str(normalized_options["as_of_date"])])
        if normalized_options.get("source"):
            command.extend(["--source", str(normalized_options["source"])])
        if normalized_options.get("reference_date"):
            command.extend(["--reference-date", str(normalized_options["reference_date"])])
        if normalized_options.get("start_date"):
            command.extend(["--start-date", str(normalized_options["start_date"])])
        if normalized_options.get("end_date"):
            command.extend(["--end-date", str(normalized_options["end_date"])])
        if normalized_options.get("chunk_size") is not None:
            command.extend(["--chunk-size", str(normalized_options["chunk_size"])])
        if action_id == "sync_postgres_market_data" and normalized_options.get("include_excluded_tickers"):
            command.append("--include-excluded-tickers")
        if normalized_options.get("strategy_ids_json"):
            command.extend(["--strategy-ids-json", str(normalized_options["strategy_ids_json"])])
        if normalized_options.get("overwrite_policy"):
            command.extend(["--overwrite-policy", str(normalized_options["overwrite_policy"])])
        if normalized_options.get("scope_json"):
            command.extend(["--scope-json", str(normalized_options["scope_json"])])
        if normalized_options.get("entry_rule_json"):
            command.extend(["--entry-rule-json", str(normalized_options["entry_rule_json"])])
        if normalized_options.get("date_range_json"):
            command.extend(["--date-range-json", str(normalized_options["date_range_json"])])
        if normalized_options.get("exit_rules_json"):
            command.extend(["--exit-rules-json", str(normalized_options["exit_rules_json"])])
        if normalized_options.get("position_rules_json"):
            command.extend(["--position-rules-json", str(normalized_options["position_rules_json"])])
        if normalized_options.get("signal_cache_policy"):
            command.extend(["--signal-cache-policy", str(normalized_options["signal_cache_policy"])])
        if normalized_options.get("market_data_mode"):
            command.extend(["--market-data-mode", str(normalized_options["market_data_mode"])])
        if action_id == "screener_history_batch" and normalized_options.get("market_data_source"):
            command.extend(["--market-data-source", str(normalized_options["market_data_source"])])
        if action_id in {"screener_history_batch", "backtest_v1"} and normalized_options.get("job_run_id") is not None:
            command.extend(["--job-run-id", str(normalized_options["job_run_id"])])
        if normalized_options.get("filter_precedence"):
            command.extend(["--filter-precedence", str(normalized_options["filter_precedence"])])
        self._append_multi_args(command, "--include-sectors", normalized_options.get("include_sectors"))
        self._append_multi_args(command, "--exclude-sectors", normalized_options.get("exclude_sectors"))
        self._append_multi_args(command, "--include-industries", normalized_options.get("include_industries"))
        self._append_multi_args(command, "--exclude-industries", normalized_options.get("exclude_industries"))
        self._append_multi_args(command, "--include-themes", normalized_options.get("include_themes"))
        self._append_multi_args(command, "--exclude-themes", normalized_options.get("exclude_themes"))
        return command

    def _normalize_options(self, action: RunAction, options: dict[str, Any]) -> dict[str, Any]:
        normalized: dict[str, Any] = {}
        if action.supports_limit:
            raw_limit = options.get("limit")
            if raw_limit not in (None, ""):
                try:
                    limit = int(raw_limit)
                except (TypeError, ValueError) as exc:
                    raise ValueError("Limit must be an integer.") from exc
                if limit <= 0 or limit > 10000:
                    raise ValueError("Limit must be between 1 and 10000.")
                normalized["limit"] = limit

        raw_tickers = options.get("tickers")
        if isinstance(raw_tickers, str) and raw_tickers.strip():
            tickers = [item.strip().upper() for item in re.split(r"[\s,]+", raw_tickers.strip()) if item.strip()]
            if tickers:
                normalized["tickers"] = tickers
        elif isinstance(raw_tickers, list):
            tickers = [str(item).strip().upper() for item in raw_tickers if str(item).strip()]
            if tickers:
                normalized["tickers"] = tickers

        for key in (
            "date_label",
            "as_of_date",
            "reference_date",
            "source",
            "filter_precedence",
            "market_data_source",
            "start_date",
            "end_date",
            "overwrite_policy",
            "signal_cache_policy",
            "market_data_mode",
        ):
            value = options.get(key)
            if isinstance(value, str) and value.strip():
                normalized[key] = value.strip()

        for key in ("chunk_size",):
            value = options.get(key)
            if value in (None, ""):
                continue
            try:
                normalized[key] = int(value)
            except (TypeError, ValueError) as exc:
                raise ValueError(f"{key.replace('_', ' ').title()} must be an integer.") from exc

        if "include_excluded_tickers" in options:
            normalized["include_excluded_tickers"] = bool(options.get("include_excluded_tickers"))

        for key in (
            "include_sectors",
            "exclude_sectors",
            "include_industries",
            "exclude_industries",
            "include_themes",
            "exclude_themes",
        ):
            value = options.get(key)
            if isinstance(value, list):
                normalized_values = [str(item).strip() for item in value if str(item).strip()]
                if normalized_values:
                    normalized[key] = normalized_values

        if isinstance(options.get("strategy_ids"), list):
            strategy_ids = [str(item).strip() for item in options["strategy_ids"] if str(item).strip()]
            if strategy_ids:
                normalized["strategy_ids"] = strategy_ids
                normalized["strategy_ids_json"] = json.dumps(strategy_ids)

        for key, fallback in (
            ("scope", {}),
            ("entry_rule", {}),
            ("date_range", {}),
            ("position_rules", {}),
        ):
            value = options.get(key)
            if isinstance(value, dict):
                normalized[f"{key}_json"] = json.dumps(value)
        if isinstance(options.get("exit_rules"), list):
            normalized["exit_rules_json"] = json.dumps(options["exit_rules"])
        if "job_run_id" in options and options.get("job_run_id") not in (None, ""):
            normalized["job_run_id"] = int(options["job_run_id"])

        return normalized

    def _append_multi_args(self, command: list[str], flag: str, values: list[str] | None) -> None:
        if values:
            command.append(flag)
            command.extend(values)

    def _get_filter_catalog(self) -> dict[str, list[str]]:
        cache_key = str(self.project_root)
        cached = self._filter_catalog_cache.get(cache_key)
        if cached is not None:
            return cached
        try:
            catalog = build_filter_option_catalog(load_app_config())
        except Exception:
            catalog = {"sectors": [], "industries": [], "themes": []}
        self._filter_catalog_cache[cache_key] = catalog
        return catalog

    def _field_options(self, field: RunField, filter_catalog: dict[str, list[str]]) -> list[dict[str, str]]:
        if field.field_id.endswith("sectors"):
            return [{"value": value, "label": value} for value in filter_catalog.get("sectors", [])]
        if field.field_id.endswith("industries"):
            return [{"value": value, "label": value} for value in filter_catalog.get("industries", [])]
        if field.field_id.endswith("themes"):
            return [{"value": value, "label": value} for value in filter_catalog.get("themes", [])]
        return [{"value": value, "label": label} for value, label in field.options]

    def _run_job(self, job_id: str, command: list[str], env: dict[str, str]) -> None:
        process = subprocess.Popen(
            command,
            cwd=str(self.project_root),
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
        )

        log_lines: list[str] = []
        with self._jobs_lock:
            job = self._jobs_by_id[job_id]
            job["_process"] = process
        assert process.stdout is not None
        for line in process.stdout:
            log_lines.append(line.rstrip())
            log_lines = log_lines[-80:]
            with self._jobs_lock:
                job = self._jobs_by_id[job_id]
                job["log_tail"] = "\n".join(log_lines)
                self._update_progress(job, log_lines)
                self._update_artifacts(job, line.rstrip())

        return_code = process.wait()
        finished_at = dt.datetime.now(dt.timezone.utc).replace(microsecond=0).isoformat()
        with self._jobs_lock:
            job = self._jobs_by_id[job_id]
            was_cancelled = bool(job.get("cancel_requested"))
            job["status"] = "cancelled" if was_cancelled else ("success" if return_code == 0 else "failed")
            job["return_code"] = return_code
            job["finished_at"] = finished_at
            job["log_tail"] = "\n".join(log_lines) if log_lines else job["log_tail"]
            job["_finished_monotonic"] = time.monotonic()
            job.pop("_process", None)
            self._load_summary_metadata(job)
            if was_cancelled:
                job["progress_label"] = "Cancelled"
            elif return_code == 0:
                job["progress_percent"] = 100
                job["progress_label"] = "Completed"
            elif job.get("progress_percent") is None:
                job["progress_label"] = "Failed"
        self._persist_completed_job(job_id)

    def _update_progress(self, job: dict[str, Any], log_lines: list[str]) -> None:
        current = None
        total = None
        last_line = ""
        success_count = None
        for line in reversed(log_lines):
            match = self._progress_pattern.search(line)
            if match:
                current = int(match.group(1))
                total = int(match.group(2))
                last_line = line
            if success_count is None:
                passed_match = self._passed_pattern.search(line)
                if passed_match:
                    success_count = int(passed_match.group(1))
            if current is not None and total is not None and success_count is not None:
                break

        if success_count is not None:
            job["success_count"] = success_count

        if current is None or total is None or total <= 0:
            return

        percent = max(0, min(100, round((current / total) * 100)))
        job["progress_current"] = current
        job["progress_total"] = total
        job["progress_percent"] = percent
        detail = "screening" if "screening" in last_line.lower() else "processing"
        job["progress_label"] = f"{current}/{total} {detail}"

    def _update_artifacts(self, job: dict[str, Any], line: str) -> None:
        watchlist_match = self._watchlist_path_pattern.search(line)
        if watchlist_match:
            job["watchlist_file"] = watchlist_match.group(1).strip()

        summary_match = self._summary_path_pattern.search(line)
        if summary_match:
            job["summary_file"] = summary_match.group(1).strip()

    def _load_summary_metadata(self, job: dict[str, Any]) -> None:
        summary_file = str(job.get("summary_file") or "").strip()
        if not summary_file:
            return
        summary_path = Path(summary_file)
        if not summary_path.exists():
            return
        try:
            payload = json.loads(summary_path.read_text(encoding="utf-8"))
        except Exception:
            return
        passed_tickers = payload.get("passed_tickers")
        if isinstance(passed_tickers, int):
            job["success_count"] = passed_tickers
        watchlist_file = payload.get("watchlist_file")
        if isinstance(watchlist_file, str) and watchlist_file.strip():
            job["watchlist_file"] = watchlist_file.strip()
        raw_results_file = payload.get("raw_results_file")
        if isinstance(raw_results_file, str) and raw_results_file.strip():
            job["raw_results_file"] = raw_results_file.strip()

    def _serialize_job(self, job: dict[str, Any]) -> dict[str, Any]:
        duration_seconds = self._job_duration_seconds(job)
        watchlist_file = str(job.get("watchlist_file") or "")
        watchlist_stem = self._watchlist_stem_from_path(watchlist_file)
        return {
            "job_id": str(job.get("job_id") or ""),
            "action_id": str(job.get("action_id") or ""),
            "label": str(job.get("label") or ""),
            "status": str(job.get("status") or "failed"),
            "command": str(job.get("command") or ""),
            "started_at": str(job.get("started_at") or ""),
            "finished_at": str(job.get("finished_at") or ""),
            "return_code": job.get("return_code"),
            "log_tail": str(job.get("log_tail") or ""),
            "progress_current": job.get("progress_current"),
            "progress_total": job.get("progress_total"),
            "progress_percent": job.get("progress_percent"),
            "progress_label": job.get("progress_label"),
            "success_count": int(job.get("success_count") or 0),
            "watchlist_file": watchlist_file,
            "watchlist_stem": watchlist_stem,
            "watchlist_url": f"/watchlists?stem={watchlist_stem}" if watchlist_stem else "",
            "summary_file": str(job.get("summary_file") or ""),
            "raw_results_file": str(job.get("raw_results_file") or ""),
            "job_run_id": job.get("job_run_id"),
            "screen_run_id": job.get("screen_run_id"),
            "backtest_run_id": job.get("backtest_run_id"),
            "cancel_requested": bool(job.get("cancel_requested")),
            "duration_seconds": duration_seconds,
            "child_jobs": [],
            "child_job_summary": {"total": 0, "running": 0, "success": 0, "failed": 0, "cancelled": 0},
        }

    def _job_duration_seconds(self, job: dict[str, Any]) -> int:
        started = job.get("_started_monotonic")
        if not isinstance(started, (int, float)):
            return 0
        finished = job.get("_finished_monotonic")
        end = finished if isinstance(finished, (int, float)) else time.monotonic()
        return max(0, int(round(end - started)))

    def _append_log_line(self, job: dict[str, Any], line: str) -> None:
        log_tail = str(job.get("log_tail") or "")
        log_lines = log_tail.splitlines() if log_tail else []
        log_lines.append(line)
        log_lines = log_lines[-80:]
        job["log_tail"] = "\n".join(log_lines)

    def _now_iso(self) -> str:
        return dt.datetime.now(dt.timezone.utc).replace(microsecond=0).isoformat()

    def _watchlist_stem_from_path(self, watchlist_file: str) -> str:
        if not watchlist_file:
            return ""
        return watchlist_stem_from_path(watchlist_file.strip())

    def _job_type_for_action(self, action_id: str) -> str:
        if action_id in {"screener_history_batch"}:
            return "screen_cache_batch"
        if action_id in {"backtest_v1"}:
            return "backtest"
        if action_id in {"sync_postgres_market_data"}:
            return "admin_sync"
        return "screen_run"

    def _persist_completed_job(self, job_id: str) -> None:
        with self._jobs_lock:
            job = dict(self._jobs_by_id.get(job_id) or {})
        if not job:
            return
        result_payload = {
            "job_id": job.get("job_id"),
            "status": job.get("status"),
            "return_code": job.get("return_code"),
            "summary_file": job.get("summary_file"),
            "watchlist_file": job.get("watchlist_file"),
            "raw_results_file": job.get("raw_results_file"),
            "success_count": job.get("success_count"),
        }
        artifact_path = str(job.get("summary_file") or job.get("watchlist_file") or "")
        self.history_repository.update_job_run(
            job.get("job_run_id"),
            status=str(job.get("status") or "failed"),
            result_payload=result_payload,
            artifact_path=artifact_path or None,
            finished_at=str(job.get("finished_at")) if job.get("finished_at") else None,
        )
        if str(job.get("status")) != "success":
            return
        action_id = str(job.get("action_id") or "")
        if action_id in {"screener_history_batch", "sync_postgres_market_data"}:
            return
        summary_file = str(job.get("summary_file") or "").strip()
        if not summary_file:
            return
        summary_path = Path(summary_file)
        if not summary_path.exists():
            return
        try:
            summary_payload = json.loads(summary_path.read_text(encoding="utf-8"))
        except Exception:
            return
        if action_id == "backtest_v1":
            self._persist_backtest_job(job, summary_payload)
            return
        raw_results_file = str(summary_payload.get("raw_results_file") or job.get("raw_results_file") or "").strip()
        if not raw_results_file:
            return
        raw_path = Path(raw_results_file)
        if not raw_path.exists():
            return
        try:
            raw_payload = json.loads(raw_path.read_text(encoding="utf-8"))
        except Exception:
            return
        screen_run_id = self.screener_history_service.persist_screen_run(
            strategy_id=action_id,
            options=dict(job.get("options") or {}),
            summary_payload=summary_payload,
            raw_payload=raw_payload,
            job_run_id=job.get("job_run_id"),
        )
        if screen_run_id is not None:
            with self._jobs_lock:
                live = self._jobs_by_id.get(job_id)
                if live is not None:
                    live["screen_run_id"] = screen_run_id

    def _persist_backtest_job(self, job: dict[str, Any], summary_payload: dict[str, Any]) -> None:
        backtest_run_id = summary_payload.get("backtest_run_id")
        if backtest_run_id is None:
            return
        with self._jobs_lock:
            live = self._jobs_by_id.get(str(job.get("job_id") or ""))
            if live is not None:
                live["backtest_run_id"] = backtest_run_id

    def _attach_child_jobs(self, jobs: list[dict[str, Any]]) -> list[dict[str, Any]]:
        parent_job_run_ids = [
            int(job["job_run_id"])
            for job in jobs
            if job.get("action_id") == "screener_history_batch" and isinstance(job.get("job_run_id"), int)
        ]
        if not parent_job_run_ids:
            return jobs
        child_rows = self.history_repository.list_child_job_runs(parent_job_run_ids)
        grouped: dict[int, list[dict[str, Any]]] = {}
        for row in child_rows:
            parent_id = row.get("parent_job_run_id")
            if isinstance(parent_id, int):
                grouped.setdefault(parent_id, []).append(self._serialize_child_job_run(row))
        for job in jobs:
            parent_id = job.get("job_run_id")
            if not isinstance(parent_id, int):
                continue
            child_jobs = grouped.get(parent_id, [])
            job["child_jobs"] = child_jobs
            job["child_job_summary"] = self._summarize_child_jobs(child_jobs)
        return jobs

    def _serialize_child_job_run(self, row: dict[str, Any]) -> dict[str, Any]:
        request_payload = row.get("request_payload") if isinstance(row.get("request_payload"), dict) else {}
        result_payload = row.get("result_payload") if isinstance(row.get("result_payload"), dict) else {}
        started_at = self._stringify_timestamp(row.get("started_at"))
        finished_at = self._stringify_timestamp(row.get("finished_at"))
        return {
            "job_run_id": int(row["id"]),
            "parent_job_run_id": row.get("parent_job_run_id"),
            "job_type": str(row.get("job_type") or ""),
            "label": str(row.get("job_name") or ""),
            "status": str(row.get("status") or "failed"),
            "started_at": started_at,
            "finished_at": finished_at,
            "artifact_path": str(row.get("artifact_path") or ""),
            "command": str(request_payload.get("command") or ""),
            "strategy_id": str(result_payload.get("strategy_id") or request_payload.get("strategy_id") or ""),
            "run_date": str(result_payload.get("run_date") or request_payload.get("run_date") or ""),
            "screen_run_id": result_payload.get("screen_run_id"),
            "success_count": int(result_payload.get("success_count") or 0),
            "summary_file": str(result_payload.get("summary_file") or ""),
            "watchlist_file": str(result_payload.get("watchlist_file") or ""),
            "raw_results_file": str(result_payload.get("raw_results_file") or ""),
            "log_tail": str(result_payload.get("log_tail") or ""),
            "log_file": str(result_payload.get("log_file") or ""),
            "message": str(result_payload.get("message") or ""),
            "skipped": bool(result_payload.get("skipped")),
            "duration_seconds": self._duration_seconds_from_iso(started_at, finished_at),
        }

    def _summarize_child_jobs(self, child_jobs: list[dict[str, Any]]) -> dict[str, int]:
        summary = {"total": len(child_jobs), "running": 0, "success": 0, "failed": 0, "cancelled": 0}
        for job in child_jobs:
            status = str(job.get("status") or "")
            if status in summary:
                summary[status] += 1
        return summary

    def _duration_seconds_from_iso(self, started_at: str, finished_at: str) -> int:
        if not started_at:
            return 0
        try:
            started = dt.datetime.fromisoformat(started_at)
        except ValueError:
            return 0
        end_raw = finished_at or self._now_iso()
        try:
            finished = dt.datetime.fromisoformat(end_raw)
        except ValueError:
            return 0
        return max(0, int(round((finished - started).total_seconds())))

    def _stringify_timestamp(self, value: Any) -> str:
        if isinstance(value, dt.datetime):
            return value.isoformat()
        return str(value or "")

    def _resolve_as_of_date(self, normalized: dict[str, Any]) -> dt.date:
        value = str(normalized.get("as_of_date") or "").strip()
        if value:
            return dt.date.fromisoformat(value)
        return dt.date.today()

    def _resolve_precheck_universe(self, *, config: Any, normalized: dict[str, Any]) -> list[UniverseTicker]:
        tickers = normalized.get("tickers")
        if isinstance(tickers, list) and tickers:
            return [UniverseTicker(symbol=str(item).strip().upper()) for item in tickers if str(item).strip()]
        universe = load_universe(config, limit=normalized.get("limit"))
        criteria = UniverseFilterCriteria(
            filter_precedence=str(normalized.get("filter_precedence") or "exclude"),
            include_sectors=tuple(str(item).strip().lower() for item in normalized.get("include_sectors") or [] if str(item).strip()),
            exclude_sectors=tuple(str(item).strip().lower() for item in normalized.get("exclude_sectors") or [] if str(item).strip()),
            include_industries=tuple(str(item).strip().lower() for item in normalized.get("include_industries") or [] if str(item).strip()),
            exclude_industries=tuple(str(item).strip().lower() for item in normalized.get("exclude_industries") or [] if str(item).strip()),
            include_themes=tuple(str(item).strip().lower() for item in normalized.get("include_themes") or [] if str(item).strip()),
            exclude_themes=tuple(str(item).strip().lower() for item in normalized.get("exclude_themes") or [] if str(item).strip()),
        )
        return filter_universe_by_criteria(universe, criteria)

    def _frame_is_db_ready(self, frame: Any, target_date: dt.date, minimum_rows: int) -> bool:
        if frame is None:
            return False
        try:
            row_count = len(frame)
        except TypeError:
            return False
        return row_count >= minimum_rows and db_frame_has_recent_coverage(frame, target_date)
