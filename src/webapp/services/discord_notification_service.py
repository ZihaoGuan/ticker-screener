from __future__ import annotations

import json
from pathlib import Path
from typing import Any
from urllib import request
from urllib.parse import quote

from src.artifact_paths import strategy_id_from_legacy_stem, watchlist_stem_from_path


_SCANNER_ROUTE_CONFIG: tuple[tuple[str, str], ...] = (
    ("weekly_rs", "weekly_rs"),
    ("rs", "rs"),
    ("sean_gap_up", "sean_peg"),
    ("gap_fill", "gap_fill"),
    ("macd_golden_cross", "macd_golden_cross"),
    ("inside_dryup_v2", "inside_dryup_v2"),
    ("wyckoff_buy_signal", "wyckoff_buy_signal"),
    ("wyckoff_sell_signal", "wyckoff_sell_signal"),
    ("sepa_vcp", "sepa_vcp"),
    ("weekly_tight_close", "weekly_tight_close"),
    ("weinstein_stage2_early", "weinstein_stage2_early"),
    ("ema21_pullback_buy", "ema21_pullback_buy"),
    ("sma200_pullback_buy", "sma200_pullback_buy"),
    ("trend_template", "trend_template"),
    ("sean_breakout", "sean_breakout"),
    ("fearzone", "fearzone"),
    ("td9_bullish", "td9_bullish"),
)


class DiscordNotificationService:
    def __init__(self, *, project_root: Path, app_base_url: str = "") -> None:
        self.project_root = project_root
        self.settings_path = project_root / "config" / "discord_notifications.json"
        self.default_app_base_url = str(app_base_url or "").strip()
        self._route_id_by_scanner_id = {route_id: route_id for route_id, _ in _SCANNER_ROUTE_CONFIG}
        self._route_id_by_strategy_id = {strategy_id: route_id for route_id, strategy_id in _SCANNER_ROUTE_CONFIG if strategy_id}

    def get_settings(self) -> dict[str, Any]:
        payload = self._load_settings()
        webhook_url = str(payload.get("webhook_url") or "").strip()
        saved_base_url = str(payload.get("app_base_url") or "").strip()
        effective_base_url = saved_base_url or self.default_app_base_url
        return {
            "webhook_url": webhook_url,
            "app_base_url": saved_base_url,
            "effective_app_base_url": effective_base_url,
            "enabled": bool(webhook_url and saved_base_url),
        }

    def update_settings(self, *, webhook_url: str, app_base_url: str) -> dict[str, Any]:
        payload = {
            "webhook_url": str(webhook_url or "").strip(),
            "app_base_url": str(app_base_url or "").strip(),
        }
        self.settings_path.parent.mkdir(parents=True, exist_ok=True)
        self.settings_path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
        return self.get_settings()

    def notify_job_completion(
        self,
        *,
        action_id: str,
        job_label: str,
        status: str,
        success_count: int | None = None,
        trigger_source: str = "",
        watchlist_file: str = "",
    ) -> bool:
        settings = self.get_settings()
        webhook_url = str(settings.get("webhook_url") or "").strip()
        app_base_url = str(settings.get("app_base_url") or "").strip()
        if not webhook_url or not app_base_url:
            return False
        message = self.build_completion_message(
            action_id=action_id,
            job_label=job_label,
            status=status,
            success_count=success_count,
            trigger_source=trigger_source,
            watchlist_file=watchlist_file,
            app_base_url=app_base_url,
        )
        if not message:
            return False
        self._post_webhook(webhook_url=webhook_url, message=message)
        return True

    def build_completion_message(
        self,
        *,
        action_id: str,
        job_label: str,
        status: str,
        success_count: int | None = None,
        trigger_source: str = "",
        watchlist_file: str = "",
        app_base_url: str,
    ) -> str | None:
        destination_path = self._destination_path(
            action_id=action_id,
            status=status,
            watchlist_file=watchlist_file,
        )
        if destination_path is None:
            return None
        link = self._join_url(app_base_url, destination_path)
        normalized_status = str(status or "unknown").strip().lower() or "unknown"
        source_label = str(trigger_source or "manual").strip() or "manual"
        lines = [
            f"Scanner job {normalized_status}: {str(job_label or action_id or 'Unknown job').strip()}",
            f"Trigger: {source_label}",
        ]
        if normalized_status == "success" and success_count is not None:
            lines.append(f"Hits: {max(0, int(success_count))}")
        lines.append(f"Open: {link}")
        return "\n".join(lines)

    def _destination_path(self, *, action_id: str, status: str, watchlist_file: str) -> str | None:
        if self._job_type_for_action(action_id) != "screen_run":
            return None
        if str(status or "").strip().lower() != "success":
            return "/screeners"
        route_id = self._scanner_route_id(action_id=action_id, watchlist_file=watchlist_file)
        if not route_id:
            return "/screeners"
        return f"/scanner/{quote(route_id)}"

    def _scanner_route_id(self, *, action_id: str, watchlist_file: str) -> str:
        candidates: list[str] = []
        clean_action_id = str(action_id or "").strip()
        if clean_action_id:
            candidates.append(clean_action_id)
        watchlist_stem = watchlist_stem_from_path(str(watchlist_file or "").strip())
        if watchlist_stem:
            candidates.append(strategy_id_from_legacy_stem(watchlist_stem))
        for candidate in candidates:
            if candidate in self._route_id_by_scanner_id:
                return self._route_id_by_scanner_id[candidate]
            if candidate in self._route_id_by_strategy_id:
                return self._route_id_by_strategy_id[candidate]
        return ""

    def _job_type_for_action(self, action_id: str) -> str:
        if action_id in {"screener_history_batch", "signal_warm_batch"}:
            return "screen_cache_batch"
        if action_id in {"overlap_backtest_v1"}:
            return "backtest_run"
        if action_id in {
            "sync_postgres_market_data",
            "reload_postgres_market_data_date",
            "sync_finviz_fundamentals",
            "sync_chart_fundamentals_cache",
            "build_sector_rating_baselines",
            "build_ticker_ratings",
            "build_technical_ratings",
            "build_technical_indicator_ratings",
            "run_finviz_ratings_pipeline",
        }:
            return "admin_sync"
        return "screen_run"

    def _load_settings(self) -> dict[str, Any]:
        if not self.settings_path.exists():
            return {}
        try:
            payload = json.loads(self.settings_path.read_text(encoding="utf-8"))
        except Exception:
            return {}
        return payload if isinstance(payload, dict) else {}

    def _post_webhook(self, *, webhook_url: str, message: str) -> None:
        payload = json.dumps(
            {
                "content": message,
                "allowed_mentions": {"parse": []},
            }
        ).encode("utf-8")
        req = request.Request(
            webhook_url,
            data=payload,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with request.urlopen(req, timeout=10):
            return

    def _join_url(self, base_url: str, path: str) -> str:
        clean_base = str(base_url or "").strip().rstrip("/")
        clean_path = path if path.startswith("/") else f"/{path}"
        return f"{clean_base}{clean_path}"
