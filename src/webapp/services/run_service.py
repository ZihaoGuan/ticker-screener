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

from src.config import load_app_config
from src.universe_filters import build_filter_option_catalog


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

    def __init__(self, project_root: Path) -> None:
        self.project_root = project_root

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
            return [self._serialize_job(item) for item in self._jobs[:limit]]

    def get_job(self, job_id: str) -> dict[str, Any]:
        with self._jobs_lock:
            job = self._jobs_by_id.get(job_id)
            if job is None:
                raise ValueError(f"Unknown job: {job_id}")
            return self._serialize_job(job)

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
        job_id = uuid.uuid4().hex[:12]
        started_at = dt.datetime.now(dt.timezone.utc).replace(microsecond=0).isoformat()
        command = [sys.executable, action.script_path]
        command.extend(action.extra_args)
        if action.supports_limit and normalized.get("limit") is not None:
            command.extend(["--limit", str(normalized["limit"])])
        if normalized.get("tickers"):
            command.append("--tickers")
            command.extend(normalized["tickers"])
        if normalized.get("date_label"):
            command.extend(["--date-label", str(normalized["date_label"])])
        if normalized.get("as_of_date"):
            command.extend(["--as-of-date", str(normalized["as_of_date"])])
        if normalized.get("source"):
            command.extend(["--source", str(normalized["source"])])
        if normalized.get("reference_date"):
            command.extend(["--reference-date", str(normalized["reference_date"])])
        if normalized.get("start_date"):
            command.extend(["--start-date", str(normalized["start_date"])])
        if normalized.get("end_date"):
            command.extend(["--end-date", str(normalized["end_date"])])
        if normalized.get("chunk_size") is not None:
            command.extend(["--chunk-size", str(normalized["chunk_size"])])
        if normalized.get("filter_precedence"):
            command.extend(["--filter-precedence", str(normalized["filter_precedence"])])
        self._append_multi_args(command, "--include-sectors", normalized.get("include_sectors"))
        self._append_multi_args(command, "--exclude-sectors", normalized.get("exclude_sectors"))
        self._append_multi_args(command, "--include-industries", normalized.get("include_industries"))
        self._append_multi_args(command, "--exclude-industries", normalized.get("exclude_industries"))
        self._append_multi_args(command, "--include-themes", normalized.get("include_themes"))
        self._append_multi_args(command, "--exclude-themes", normalized.get("exclude_themes"))

        job = {
            "job_id": job_id,
            "action_id": action_id,
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
            "_started_monotonic": time.monotonic(),
        }

        with self._jobs_lock:
            self._jobs.insert(0, job)
            self._jobs_by_id[job_id] = job
            del self._jobs[50:]

        env = os.environ.copy()
        if normalized.get("market_data_source"):
            env["TICKER_SCREENER_MARKET_DATA_SOURCE"] = str(normalized["market_data_source"])
        thread = threading.Thread(target=self._run_job, args=(job_id, command, env), daemon=True)
        thread.start()
        return job_id

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

        for key in (
            "date_label",
            "as_of_date",
            "reference_date",
            "source",
            "filter_precedence",
            "market_data_source",
            "start_date",
            "end_date",
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

    def _serialize_job(self, job: dict[str, Any]) -> dict[str, Any]:
        duration_seconds = self._job_duration_seconds(job)
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
            "watchlist_file": str(job.get("watchlist_file") or ""),
            "summary_file": str(job.get("summary_file") or ""),
            "cancel_requested": bool(job.get("cancel_requested")),
            "duration_seconds": duration_seconds,
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
