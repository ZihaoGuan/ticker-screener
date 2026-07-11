from __future__ import annotations

import datetime as dt
import os
from dataclasses import dataclass
from typing import Any

import pandas as pd

from src.webapp.repositories.position_decision_repository import PositionDecisionRepository
from src.webapp.repositories.tiger_positions_repository import TigerPositionsRepository


DEFAULT_PRIVATE_KEY_ENV_VAR = "TIGER_PRIVATE_KEY"


@dataclass(frozen=True)
class TigerAccountConfig:
    tiger_id: str
    account: str
    private_key: str


class TigerPositionsService:
    def __init__(
        self,
        *,
        repository: TigerPositionsRepository | None = None,
        decision_repository: PositionDecisionRepository | None = None,
        database_url: str = "",
    ) -> None:
        self.repository = repository or TigerPositionsRepository(database_url=database_url)
        self.decision_repository = decision_repository or PositionDecisionRepository(database_url=database_url)

    def get_context(self, *, user_id: int) -> dict[str, Any]:
        settings = self.repository.get_user_settings(user_id) or self._default_settings(user_id=user_id)
        positions = [self._serialize_position(item) for item in self.repository.list_latest_positions(user_id)]
        self._attach_position_actions(positions)
        return {
            "database_configured": self.repository.is_configured(),
            "settings": self._serialize_settings(settings),
            "summary": self._build_summary(positions, settings=settings),
            "positions": positions,
        }

    def update_settings(
        self,
        *,
        user_id: int,
        display_name: str,
        tiger_id: str,
        account: str,
        private_key_env_var: str = DEFAULT_PRIVATE_KEY_ENV_VAR,
        is_enabled: bool = True,
    ) -> dict[str, Any]:
        self._require_configured()
        normalized_display_name = str(display_name or "").strip()
        normalized_tiger_id = str(tiger_id or "").strip()
        normalized_account = str(account or "").strip()
        normalized_env_var = str(private_key_env_var or DEFAULT_PRIVATE_KEY_ENV_VAR).strip() or DEFAULT_PRIVATE_KEY_ENV_VAR
        if not normalized_tiger_id:
            raise ValueError("Tiger developer ID is required.")
        if not normalized_account:
            raise ValueError("Tiger account is required.")
        updated = self.repository.upsert_user_settings(
            user_id=user_id,
            display_name=normalized_display_name,
            tiger_id=normalized_tiger_id,
            account=normalized_account,
            private_key_env_var=normalized_env_var,
            is_enabled=bool(is_enabled),
        )
        if updated is None:
            raise ValueError("Failed to save Tiger settings.")
        return self._serialize_settings(updated)

    def sync_user_positions(self, *, user_id: int) -> dict[str, Any]:
        self._require_configured()
        settings = self.repository.get_user_settings(user_id)
        if settings is None:
            raise ValueError("Tiger settings are not configured for this account.")
        if not bool(settings.get("is_enabled")):
            raise ValueError("Tiger sync is disabled for this account.")
        account_config = self._resolve_account_config(settings)
        captured_at = dt.datetime.now(dt.timezone.utc).replace(microsecond=0)
        try:
            positions = self._load_positions_from_tiger(account_config)
        except Exception as exc:
            self.repository.record_sync_status(user_id=user_id, synced_at=None, error_text=str(exc))
            raise
        inserted_count = self.repository.insert_position_batch(
            user_id=user_id,
            tiger_account=account_config.account,
            captured_at=captured_at,
            as_of_date=captured_at.date(),
            positions=positions,
        )
        self.repository.record_sync_status(user_id=user_id, synced_at=captured_at, error_text="")
        return {
            "ok": True,
            "synced_count": inserted_count,
            "captured_at": captured_at.isoformat(),
            "account": account_config.account,
        }

    def sync_all_enabled_users(self) -> dict[str, Any]:
        self._require_configured()
        results: list[dict[str, Any]] = []
        for settings in self.repository.list_enabled_settings():
            user_id = int(settings.get("user_id") or 0)
            if user_id <= 0:
                continue
            try:
                result = self.sync_user_positions(user_id=user_id)
                results.append({"user_id": user_id, "status": "success", **result})
            except Exception as exc:
                results.append({"user_id": user_id, "status": "failed", "error": str(exc)})
        return {
            "ok": True,
            "processed_count": len(results),
            "success_count": sum(1 for item in results if item.get("status") == "success"),
            "failure_count": sum(1 for item in results if item.get("status") == "failed"),
            "results": results,
        }

    def _default_settings(self, *, user_id: int) -> dict[str, Any]:
        return {
            "user_id": user_id,
            "display_name": "",
            "tiger_id": "",
            "account": "",
            "private_key_env_var": DEFAULT_PRIVATE_KEY_ENV_VAR,
            "is_enabled": False,
            "last_synced_at": None,
            "last_sync_error": None,
            "created_at": None,
            "updated_at": None,
        }

    def _serialize_settings(self, row: dict[str, Any]) -> dict[str, Any]:
        env_var = str(row.get("private_key_env_var") or DEFAULT_PRIVATE_KEY_ENV_VAR).strip() or DEFAULT_PRIVATE_KEY_ENV_VAR
        return {
            "user_id": int(row.get("user_id") or 0),
            "display_name": str(row.get("display_name") or ""),
            "tiger_id": str(row.get("tiger_id") or ""),
            "account": str(row.get("account") or ""),
            "private_key_env_var": env_var,
            "is_enabled": bool(row.get("is_enabled")),
            "last_synced_at": row.get("last_synced_at"),
            "last_sync_error": row.get("last_sync_error"),
            "has_private_key": bool(os.getenv(env_var, "").strip()),
        }

    def _serialize_position(self, row: dict[str, Any]) -> dict[str, Any]:
        quantity = _to_float(row.get("quantity"))
        average_cost = _to_float(row.get("average_cost"))
        market_price = _to_float(row.get("market_price"))
        market_value = _to_float(row.get("market_value"))
        unrealized_pl = _to_float(row.get("unrealized_pl"))
        cost_basis = quantity * average_cost if quantity is not None and average_cost is not None else None
        unrealized_pl_pct = None
        if cost_basis and unrealized_pl is not None:
            unrealized_pl_pct = (unrealized_pl / cost_basis) * 100.0
        return {
            "id": int(row.get("id") or 0),
            "ticker": str(row.get("ticker") or "").upper(),
            "tiger_account": str(row.get("tiger_account") or ""),
            "quantity": quantity,
            "average_cost": average_cost,
            "market_price": market_price,
            "market_value": market_value,
            "cost_basis": cost_basis,
            "unrealized_pl": unrealized_pl,
            "unrealized_pl_pct": unrealized_pl_pct,
            "currency": str(row.get("currency") or "USD"),
            "as_of_date": row.get("as_of_date"),
            "captured_at": row.get("captured_at"),
            "raw_json": row.get("raw_json") or {},
            "position_action": None,
        }

    def _attach_position_actions(self, positions: list[dict[str, Any]]) -> None:
        tickers = [str(item.get("ticker") or "").upper() for item in positions if str(item.get("ticker") or "").strip()]
        if not tickers:
            return
        decision_map = self.decision_repository.load_latest_decision_map(tickers)
        for item in positions:
            decision = decision_map.get(str(item.get("ticker") or "").upper())
            item["position_action"] = _serialize_position_action(decision)

    def _build_summary(self, positions: list[dict[str, Any]], *, settings: dict[str, Any]) -> dict[str, Any]:
        total_market_value = sum(float(item.get("market_value") or 0.0) for item in positions)
        total_cost_basis = sum(float(item.get("cost_basis") or 0.0) for item in positions)
        total_unrealized_pl = sum(float(item.get("unrealized_pl") or 0.0) for item in positions)
        total_unrealized_pl_pct = (total_unrealized_pl / total_cost_basis * 100.0) if total_cost_basis > 0 else 0.0
        action_counts = {"add_position": 0, "hold_position": 0, "trim_reduce": 0}
        for item in positions:
            action = str((item.get("position_action") or {}).get("action") or "")
            if action in action_counts:
                action_counts[action] += 1
        return {
            "position_count": len(positions),
            "total_market_value": round(total_market_value, 2),
            "total_cost_basis": round(total_cost_basis, 2),
            "total_unrealized_pl": round(total_unrealized_pl, 2),
            "total_unrealized_pl_pct": round(total_unrealized_pl_pct, 2),
            "add_count": action_counts["add_position"],
            "hold_count": action_counts["hold_position"],
            "trim_count": action_counts["trim_reduce"],
            "last_synced_at": settings.get("last_synced_at"),
        }

    def _require_configured(self) -> None:
        if not self.repository.is_configured():
            raise ValueError("Database URL is not configured for Tiger positions.")

    def _resolve_account_config(self, settings: dict[str, Any]) -> TigerAccountConfig:
        tiger_id = str(settings.get("tiger_id") or "").strip()
        account = str(settings.get("account") or "").strip()
        env_var = str(settings.get("private_key_env_var") or DEFAULT_PRIVATE_KEY_ENV_VAR).strip() or DEFAULT_PRIVATE_KEY_ENV_VAR
        private_key = os.getenv(env_var, "").strip()
        if not tiger_id:
            raise ValueError("Tiger developer ID is missing.")
        if not account:
            raise ValueError("Tiger account is missing.")
        if not private_key:
            raise ValueError(f"Set the Tiger RSA private key in env var {env_var}.")
        return TigerAccountConfig(tiger_id=tiger_id, account=account, private_key=private_key)

    def _load_positions_from_tiger(self, config: TigerAccountConfig) -> list[dict[str, Any]]:
        try:
            from tigeropen.tiger_open_config import TigerOpenClientConfig
            from tigeropen.trade.trade_client import TradeClient
        except ImportError as exc:
            raise ValueError("Tiger SDK is not installed. Install the `tigeropen` package first.") from exc

        client_config = TigerOpenClientConfig()
        client_config.tiger_id = config.tiger_id
        client_config.account = config.account
        client_config.private_key = config.private_key
        client = TradeClient(client_config)

        result: Any = None
        for method_name in ("get_positions", "get_position", "position_list"):
            method = getattr(client, method_name, None)
            if callable(method):
                result = method()
                if result is not None:
                    break
        if result is None:
            raise ValueError("Could not load Tiger positions from the SDK client.")
        return self._normalize_tiger_positions(result)

    def _normalize_tiger_positions(self, raw_result: Any) -> list[dict[str, Any]]:
        rows = _coerce_rows(raw_result)
        positions: list[dict[str, Any]] = []
        for row in rows:
            if not isinstance(row, dict):
                continue
            ticker = _pick_first_text(row, "symbol", "ticker", "contract_code", "stock_code")
            quantity = _pick_first_number(row, "quantity", "position", "shares", "qty")
            if not ticker or quantity in (None, 0):
                continue
            average_cost = _pick_first_number(row, "average_cost", "average_cost_by_average", "cost_price", "costPrice", "averagePrice")
            market_price = _pick_first_number(row, "market_price", "latest_price", "last_price", "price", "marketPrice")
            market_value = _pick_first_number(row, "market_value", "position_value", "marketValue")
            if market_value is None and quantity is not None and market_price is not None:
                market_value = quantity * market_price
            unrealized_pl = _pick_first_number(row, "unrealized_pl", "unrealized_pnl", "unrealized_profit_loss", "float_pnl")
            currency = _pick_first_text(row, "currency", "currency_code") or "USD"
            positions.append(
                {
                    "ticker": ticker.upper(),
                    "quantity": quantity,
                    "average_cost": average_cost,
                    "market_price": market_price,
                    "market_value": market_value,
                    "unrealized_pl": unrealized_pl,
                    "currency": currency,
                    "raw_json": row,
                }
            )
        return positions


def _coerce_rows(raw_result: Any) -> list[dict[str, Any]]:
    if isinstance(raw_result, pd.DataFrame):
        return raw_result.to_dict(orient="records")
    if isinstance(raw_result, list):
        return [item for item in raw_result if isinstance(item, dict)]
    if isinstance(raw_result, tuple):
        return [item for item in raw_result if isinstance(item, dict)]
    if isinstance(raw_result, dict):
        for key in ("items", "positions", "data"):
            value = raw_result.get(key)
            if isinstance(value, list):
                return [item for item in value if isinstance(item, dict)]
        return [raw_result]
    if hasattr(raw_result, "to_dict"):
        converted = raw_result.to_dict()
        if isinstance(converted, list):
            return [item for item in converted if isinstance(item, dict)]
        if isinstance(converted, dict):
            return [converted]
    return []


def _pick_first_text(row: dict[str, Any], *keys: str) -> str | None:
    for key in keys:
        value = row.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return None


def _pick_first_number(row: dict[str, Any], *keys: str) -> float | None:
    for key in keys:
        value = _to_float(row.get(key))
        if value is not None:
            return value
    return None


def _to_float(value: Any) -> float | None:
    if value in (None, "", "None"):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _serialize_position_action(row: dict[str, Any] | None) -> dict[str, Any] | None:
    if not row:
        return None
    evidence = row.get("evidence_json")
    return {
        "as_of_date": row.get("as_of_date"),
        "action": row.get("action"),
        "action_score": _to_float(row.get("action_score")),
        "regime_state": row.get("regime_state"),
        "trend_state": row.get("trend_state"),
        "extension_state": row.get("extension_state"),
        "support_reference": row.get("support_reference"),
        "atr_dist_21": _to_float(row.get("atr_dist_21")),
        "atr_dist_10w": _to_float(row.get("atr_dist_10w")),
        "atr_pct": _to_float(row.get("atr_pct")),
        "daily_atr_ratio": _to_float(row.get("daily_atr_ratio")),
        "close_price": _to_float(row.get("close_price")),
        "ema21": _to_float(row.get("ema21")),
        "sma50": _to_float(row.get("sma50")),
        "sma10w": _to_float(row.get("sma10w")),
        "danger_signal_count": int(row.get("danger_signal_count") or 0),
        "reason_summary": row.get("reason_summary"),
        "evidence": evidence if isinstance(evidence, dict) else {},
    }
