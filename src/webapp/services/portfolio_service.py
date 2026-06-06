from __future__ import annotations

import csv
import datetime as dt
from io import StringIO
from typing import Any

import pandas as pd

from src.config import load_app_config
from src.ftd_sweep_screen import find_recent_ftd_sweep_hit
from src.hve_screen import find_recent_hve_hit
from src.inside_dryup_screen import find_recent_inside_dryup_hit
from src.market_data_access import db_frame_has_recent_coverage, load_many_ticker_windows, load_ticker_metadata_map
from src.ticker_filters import normalize_ticker_symbol
from src.universe import UniverseTicker
from src.webapp.repositories.portfolio_repository import PortfolioRepository


DEFAULT_PORTFOLIO_NAME = "Main"
TP1_SELL_FRACTION = 0.40
TP2_SELL_FRACTION = 0.60
AVERAGE_UP_SHARE_FRACTION = 0.25
ADVICE_LOOKBACK_DAYS = 320
STALE_TOLERANCE_DAYS = 7


class PortfolioService:
    def __init__(self, *, repository: PortfolioRepository | None = None, database_url: str = "") -> None:
        self.repository = repository or PortfolioRepository(database_url=database_url)
        self.database_url = self.repository.database_url
        self.config = load_app_config()

    def get_context(self) -> dict[str, Any]:
        base_rows = self.repository.list_positions()
        positions = [self._serialize_position(row) for row in self._build_positions_with_transactions(base_rows)]
        summary = self._build_summary(positions)
        return {
            "database_configured": self.repository.is_configured(),
            "summary": summary,
            "positions": positions,
            "portfolios": [self._serialize_portfolio(item) for item in self.repository.list_portfolios()],
            "market_regime": {
                "title": "Market Regime Placeholder",
                "status": "deferred",
                "description": "Space reserved for VIX, Fear & Greed, or other macro gauges in a later iteration.",
            },
        }

    def record_transaction(
        self,
        position_id: int,
        *,
        side: str,
        shares: object,
        price: object,
        trade_date: object,
        fees: object = 0,
        notes: str = "",
        actor_user_id: int | None = None,
    ) -> dict[str, Any]:
        self._require_configured()
        position = self.repository.get_position(position_id)
        if position is None:
            raise ValueError("Position not found.")
        normalized_side = str(side or "").strip().lower()
        if normalized_side not in {"buy", "sell"}:
            raise ValueError("side must be buy or sell.")
        parsed_shares = self._parse_positive_number(shares, field="shares")
        parsed_price = self._parse_positive_number(price, field="price")
        parsed_trade_date = self._parse_date(trade_date, field="trade_date")
        parsed_fees = self._parse_non_negative_number(fees, field="fees")

        if normalized_side == "sell":
            state = self._compute_position_state(position, self.repository.list_position_transactions([position_id]))
            if parsed_shares > float(state["shares"] or 0):
                raise ValueError("sell shares exceed current holding.")

        created = self.repository.create_transaction(
            position_id=position_id,
            trade_date=parsed_trade_date,
            side=normalized_side,
            shares=parsed_shares,
            price=parsed_price,
            fees=parsed_fees,
            notes=str(notes or "").strip(),
            created_by_user_id=actor_user_id,
        )
        if created is None:
            raise ValueError("Failed to create transaction.")
        return self._serialize_transaction(created)

    def create_position(
        self,
        *,
        ticker: str,
        shares: object,
        entry_price: object,
        opened_at: object,
        notes: str = "",
        portfolio_name: str = DEFAULT_PORTFOLIO_NAME,
        actor_user_id: int | None = None,
    ) -> dict[str, Any]:
        self._require_configured()
        normalized_ticker = self._normalize_ticker(ticker)
        parsed_shares = self._parse_positive_number(shares, field="shares")
        parsed_entry_price = self._parse_positive_number(entry_price, field="entry_price")
        parsed_opened_at = self._parse_date(opened_at, field="opened_at")
        portfolio = self._get_or_create_portfolio(portfolio_name, actor_user_id=actor_user_id)
        self.repository.ensure_ticker_metadata_stub(normalized_ticker)
        created = self.repository.create_position(
            portfolio_id=int(portfolio["id"]),
            ticker=normalized_ticker,
            shares=parsed_shares,
            entry_price=parsed_entry_price,
            opened_at=parsed_opened_at,
            notes=str(notes or "").strip(),
            created_by_user_id=actor_user_id,
        )
        if created is None:
            raise ValueError("Failed to create portfolio position.")
        return self._serialize_position(
            {
                **created,
                "portfolio_name": portfolio["name"],
            }
        )

    def update_position(
        self,
        position_id: int,
        *,
        ticker: str,
        shares: object,
        entry_price: object,
        opened_at: object,
        notes: str = "",
        portfolio_name: str = DEFAULT_PORTFOLIO_NAME,
        actor_user_id: int | None = None,
    ) -> dict[str, Any]:
        self._require_configured()
        existing = self.repository.get_position(position_id)
        if existing is None:
            raise ValueError("Position not found.")
        normalized_ticker = self._normalize_ticker(ticker)
        parsed_shares = self._parse_positive_number(shares, field="shares")
        parsed_entry_price = self._parse_positive_number(entry_price, field="entry_price")
        parsed_opened_at = self._parse_date(opened_at, field="opened_at")
        portfolio = self._get_or_create_portfolio(portfolio_name, actor_user_id=actor_user_id)
        self.repository.ensure_ticker_metadata_stub(normalized_ticker)
        updated = self.repository.update_position(
            position_id,
            portfolio_id=int(portfolio["id"]),
            ticker=normalized_ticker,
            shares=parsed_shares,
            entry_price=parsed_entry_price,
            opened_at=parsed_opened_at,
            notes=str(notes or "").strip(),
            updated_by_user_id=actor_user_id,
        )
        if updated is None:
            raise ValueError("Failed to update portfolio position.")
        return self._serialize_position(
            {
                **updated,
                "portfolio_name": portfolio["name"],
            }
        )

    def delete_position(self, position_id: int) -> None:
        self._require_configured()
        if not self.repository.delete_position(position_id):
            raise ValueError("Position not found.")

    def import_csv(
        self,
        *,
        csv_text: str,
        portfolio_name: str = DEFAULT_PORTFOLIO_NAME,
        actor_user_id: int | None = None,
        source_name: str = "portfolio.csv",
    ) -> dict[str, Any]:
        self._require_configured()
        raw_csv = str(csv_text or "")
        if not raw_csv.strip():
            raise ValueError("CSV content is required.")
        reader = csv.DictReader(StringIO(raw_csv))
        if reader.fieldnames is None:
            raise ValueError("CSV header is required.")
        required = {"ticker", "shares", "entry_price", "opened_at"}
        header_map = {str(name or "").strip().lower(): name for name in reader.fieldnames}
        missing = sorted(required - set(header_map.keys()))
        if missing:
            raise ValueError(f"CSV missing required columns: {', '.join(missing)}")

        portfolio = self._get_or_create_portfolio(portfolio_name, actor_user_id=actor_user_id)
        accepted: list[dict[str, Any]] = []
        errors: list[dict[str, Any]] = []

        for row_number, row in enumerate(reader, start=2):
            try:
                row_portfolio_name = str(row.get(header_map.get("portfolio_name", ""), "") or "").strip() or portfolio["name"]
                created = self.create_position(
                    ticker=str(row.get(header_map["ticker"], "") or ""),
                    shares=row.get(header_map["shares"], ""),
                    entry_price=row.get(header_map["entry_price"], ""),
                    opened_at=row.get(header_map["opened_at"], ""),
                    notes=str(row.get(header_map.get("notes", ""), "") or ""),
                    portfolio_name=row_portfolio_name,
                    actor_user_id=actor_user_id,
                )
                accepted.append({"row": row_number, "position": created})
            except ValueError as exc:
                errors.append({"row": row_number, "message": str(exc)})

        batch_id = self.repository.create_import_batch(
            portfolio_id=int(portfolio["id"]),
            source_name=str(source_name or "portfolio.csv"),
            imported_by_user_id=actor_user_id,
            row_count=len(accepted) + len(errors),
            accepted_count=len(accepted),
            error_count=len(errors),
            raw_csv_text=raw_csv,
            summary_json={"accepted": accepted, "errors": errors},
        )
        return {
            "ok": True,
            "portfolio_name": portfolio["name"],
            "import_batch_id": batch_id,
            "accepted_count": len(accepted),
            "error_count": len(errors),
            "accepted": accepted,
            "errors": errors,
        }

    def refresh_advice(
        self,
        *,
        position_id: int | None = None,
        ticker: str = "",
    ) -> dict[str, Any]:
        self._require_configured()
        all_rows = self._build_positions_with_transactions(self.repository.list_positions())
        normalized_ticker = self._normalize_ticker(ticker) if str(ticker or "").strip() else ""
        rows = [
            row
            for row in all_rows
            if (position_id is None or int(row.get("id") or 0) == position_id)
            and (not normalized_ticker or str(row.get("ticker") or "").upper() == normalized_ticker)
        ]
        if not rows:
            raise ValueError("No matching positions found.")

        tickers = [str(row.get("ticker") or "").upper() for row in rows]
        as_of_date = dt.date.today()
        frames = load_many_ticker_windows(
            tickers,
            as_of_date,
            ADVICE_LOOKBACK_DAYS,
            database_url=self.database_url,
        )
        metadata_map = load_ticker_metadata_map(tickers, database_url=self.database_url)
        recent_hits = self.repository.list_recent_signal_hits(tickers)

        refreshed_rows: list[dict[str, Any]] = []
        for row in rows:
            payload = self._build_advice_for_position(
                row=row,
                frame=frames.get(str(row.get("ticker") or "").upper()),
                metadata=metadata_map.get(str(row.get("ticker") or "").upper(), {}),
                recent_hits=recent_hits.get(str(row.get("ticker") or "").upper(), []),
                as_of_date=as_of_date,
            )
            self.repository.upsert_advice_snapshot(int(row["id"]), **payload)
            refreshed_rows.append({"position_id": int(row["id"]), "ticker": str(row.get("ticker") or "").upper()})

        return {
            "ok": True,
            "refreshed_count": len(refreshed_rows),
            "positions": refreshed_rows,
        }

    def _build_advice_for_position(
        self,
        *,
        row: dict[str, Any],
        frame: Any,
        metadata: dict[str, Any],
        recent_hits: list[dict[str, Any]],
        as_of_date: dt.date,
    ) -> dict[str, Any]:
        if frame is None or frame.empty:
            return {
                "as_of_date": as_of_date,
                "latest_trade_date": None,
                "market_data_status": "missing",
                "close_price": None,
                "signal_status": "review",
                "stop_loss_price": None,
                "tp1_price": None,
                "tp2_price": None,
                "tp1_sell_fraction": TP1_SELL_FRACTION,
                "tp2_sell_fraction": TP2_SELL_FRACTION,
                "average_up_price": None,
                "average_up_share_fraction": AVERAGE_UP_SHARE_FRACTION,
                "blended_entry_after_average_up": None,
                "net_cost_after_tp1": None,
                "remaining_cost_basis_after_tp1": None,
                "explanation": "No database price history is available for this ticker yet. Run a history sync before relying on advice.",
                "data_source": "database",
                "signal_context_json": {"recent_hits": recent_hits},
            }

        frame = frame.sort_index().copy()
        latest_index = frame.index.max()
        latest_trade_date = latest_index.date() if hasattr(latest_index, "date") else latest_index
        if not db_frame_has_recent_coverage(frame, as_of_date, tolerance_days=STALE_TOLERANCE_DAYS):
            latest_close = float(frame["Close"].iloc[-1])
            return {
                "as_of_date": as_of_date,
                "latest_trade_date": latest_trade_date,
                "market_data_status": "stale",
                "close_price": latest_close,
                "signal_status": "review",
                "stop_loss_price": None,
                "tp1_price": None,
                "tp2_price": None,
                "tp1_sell_fraction": TP1_SELL_FRACTION,
                "tp2_sell_fraction": TP2_SELL_FRACTION,
                "average_up_price": None,
                "average_up_share_fraction": AVERAGE_UP_SHARE_FRACTION,
                "blended_entry_after_average_up": None,
                "net_cost_after_tp1": None,
                "remaining_cost_basis_after_tp1": None,
                "explanation": f"Latest close is from {latest_trade_date.isoformat()}; refresh market data before using stop or take-profit advice.",
                "data_source": "database",
                "signal_context_json": {"recent_hits": recent_hits},
            }

        normalized = frame.copy()
        normalized["ma20"] = normalized["Close"].rolling(20).mean()
        normalized["ma50"] = normalized["Close"].rolling(50).mean()
        normalized["ma200"] = normalized["Close"].rolling(200).mean()
        normalized["ema21"] = normalized["Close"].ewm(span=21, adjust=False).mean()
        normalized["atr14"] = _compute_atr(normalized, length=14)

        latest = normalized.iloc[-1]
        close_price = float(latest["Close"])
        ma20 = _safe_float(latest.get("ma20"))
        ma50 = _safe_float(latest.get("ma50"))
        ma200 = _safe_float(latest.get("ma200"))
        ema21 = _safe_float(latest.get("ema21"))
        atr14 = _safe_float(latest.get("atr14"))

        universe_ticker = UniverseTicker(
            symbol=str(row.get("ticker") or "").upper(),
            sector=str(metadata.get("sector") or "") or None,
            industry=str(metadata.get("industry") or "") or None,
            exchange=str(metadata.get("exchange") or "") or None,
        )
        hve_hit = find_recent_hve_hit(normalized, ticker=universe_ticker)
        inside_dryup_hit = find_recent_inside_dryup_hit(normalized, ticker=universe_ticker)
        ftd_hit = find_recent_ftd_sweep_hit(
            normalized,
            ticker=universe_ticker,
            benchmark_ticker=self.config.benchmark_ticker,
            config=self.config,
        )

        active_signals = [
            item
            for item in (
                ("hve", hve_hit is not None),
                ("inside_dryup", inside_dryup_hit is not None),
                ("ftd_sweep", ftd_hit is not None),
            )
            if item[1]
        ]
        recent_signal_ids = [str(item.get("strategy_id") or "") for item in recent_hits]
        has_bearish_history = "lost_21ema" in recent_signal_ids
        bullish_score = sum(1 for _, enabled in active_signals if enabled)
        if close_price > 0 and ma50 and close_price > ma50:
            bullish_score += 1
        if ma50 and ma200 and ma50 > ma200:
            bullish_score += 1
        if ema21 and ma50 and ema21 > ma50:
            bullish_score += 1

        if has_bearish_history or (ma50 and close_price < ma50):
            signal_status = "trim"
        elif bullish_score >= 4:
            signal_status = "hold"
        elif bullish_score >= 2:
            signal_status = "raise_stop"
        else:
            signal_status = "review"

        atr_value = atr14 if atr14 and atr14 > 0 else close_price * 0.05
        stop_loss_price = _round_price(
            max(
                close_price - (1.8 * atr_value),
                (ma50 or close_price * 0.92) * 0.99,
                close_price * 0.88,
            )
        )
        tp1_multiple = 1.8 if signal_status == "hold" else 1.2
        tp2_multiple = 3.2 if signal_status == "hold" else 2.2
        tp1_price = _round_price(close_price + (tp1_multiple * atr_value))
        tp2_price = _round_price(close_price + (tp2_multiple * atr_value))
        average_up_price = _round_price(
            max(
                close_price + (0.5 * atr_value),
                (ma20 or close_price) * 1.01,
                close_price * 1.02,
            )
        )

        shares = _safe_float(row.get("shares")) or 0.0
        entry_price = _safe_float(row.get("entry_price")) or 0.0
        net_cost_after_tp1, remaining_cost_basis = _compute_remaining_cost_basis_per_share(
            shares=shares,
            entry_price=entry_price,
            take_profit_price=tp1_price,
            sell_fraction=TP1_SELL_FRACTION,
        )
        blended_entry_after_average_up = _compute_blended_entry_after_average_up(
            shares=shares,
            entry_price=entry_price,
            add_price=average_up_price,
            add_fraction=AVERAGE_UP_SHARE_FRACTION,
        )

        explanation_parts = []
        if active_signals:
            explanation_parts.append(
                "Active signals: " + ", ".join(signal_id.replace("_", " ").upper() for signal_id, _ in active_signals)
            )
        if recent_hits:
            explanation_parts.append(
                "Recent screener hits: "
                + ", ".join(f"{str(item.get('strategy_id') or '').upper()} {item.get('signal_date')}" for item in recent_hits[:3])
            )
        trend_note = []
        if ma50:
            trend_note.append(f"close vs 50D {close_price / ma50 - 1:+.1%}")
        if ma200:
            trend_note.append(f"50D vs 200D {(ma50 or close_price) / ma200 - 1:+.1%}" if ma50 else f"close vs 200D {close_price / ma200 - 1:+.1%}")
        if trend_note:
            explanation_parts.append("Trend context: " + ", ".join(trend_note))
        if signal_status in {"hold", "raise_stop"} and average_up_price is not None and blended_entry_after_average_up is not None:
            explanation_parts.append(
                f"Average-up path: add {int(AVERAGE_UP_SHARE_FRACTION * 100)}% near {average_up_price:.2f} for blended entry {blended_entry_after_average_up:.2f}."
            )
        if not explanation_parts:
            explanation_parts.append("Advice uses the latest close plus moving-average and volatility fallback rules.")

        return {
            "as_of_date": as_of_date,
            "latest_trade_date": latest_trade_date,
            "market_data_status": "ready",
            "close_price": _round_price(close_price),
            "signal_status": signal_status,
            "stop_loss_price": stop_loss_price,
            "tp1_price": tp1_price,
            "tp2_price": tp2_price,
            "tp1_sell_fraction": TP1_SELL_FRACTION,
            "tp2_sell_fraction": TP2_SELL_FRACTION,
            "average_up_price": average_up_price,
            "average_up_share_fraction": AVERAGE_UP_SHARE_FRACTION,
            "blended_entry_after_average_up": _round_price(blended_entry_after_average_up) if blended_entry_after_average_up is not None else None,
            "net_cost_after_tp1": _round_price(net_cost_after_tp1) if net_cost_after_tp1 is not None else None,
            "remaining_cost_basis_after_tp1": _round_price(remaining_cost_basis) if remaining_cost_basis is not None else None,
            "explanation": " ".join(explanation_parts),
            "data_source": "database",
            "signal_context_json": {
                "active_signals": [signal_id for signal_id, _ in active_signals],
                "recent_hits": recent_hits,
                "ma20": _round_price(ma20) if ma20 is not None else None,
                "ma50": _round_price(ma50) if ma50 is not None else None,
                "ma200": _round_price(ma200) if ma200 is not None else None,
                "atr14": _round_price(atr14) if atr14 is not None else None,
            },
        }

    def _build_summary(self, positions: list[dict[str, Any]]) -> dict[str, Any]:
        total_market_value = 0.0
        total_cost_basis = 0.0
        last_refreshed_at = ""
        stale_count = 0
        missing_count = 0

        for position in positions:
            if bool(position.get("is_closed")):
                continue
            shares = _safe_float(position.get("shares")) or 0.0
            entry_price = _safe_float(position.get("entry_price")) or 0.0
            close_price = _safe_float(position.get("advice", {}).get("close_price"))
            total_cost_basis += shares * entry_price
            if close_price is not None:
                total_market_value += shares * close_price
            status = str(position.get("advice", {}).get("market_data_status") or "pending")
            if status == "stale":
                stale_count += 1
            elif status == "missing":
                missing_count += 1
            refreshed_at = str(position.get("advice", {}).get("refreshed_at") or "")
            if refreshed_at and refreshed_at > last_refreshed_at:
                last_refreshed_at = refreshed_at

        total_unrealized_pl = total_market_value - total_cost_basis
        return {
            "position_count": sum(1 for item in positions if not bool(item.get("is_closed"))),
            "total_market_value": round(total_market_value, 2),
            "total_cost_basis": round(total_cost_basis, 2),
            "total_unrealized_pl": round(total_unrealized_pl, 2),
            "total_unrealized_pl_pct": round((total_unrealized_pl / total_cost_basis) * 100.0, 2) if total_cost_basis > 0 else 0.0,
            "stale_advice_count": stale_count,
            "missing_advice_count": missing_count,
            "last_refreshed_at": last_refreshed_at or None,
        }

    def _serialize_portfolio(self, row: dict[str, Any]) -> dict[str, Any]:
        return {
            "id": int(row.get("id") or 0),
            "name": str(row.get("name") or DEFAULT_PORTFOLIO_NAME),
            "created_by_user_id": _safe_int(row.get("created_by_user_id")),
            "created_at": _to_iso_datetime(row.get("created_at")),
            "updated_at": _to_iso_datetime(row.get("updated_at")),
        }

    def _serialize_position(self, row: dict[str, Any]) -> dict[str, Any]:
        shares = _safe_float(row.get("shares")) or 0.0
        entry_price = _safe_float(row.get("entry_price")) or 0.0
        close_price = _safe_float(row.get("close_price"))
        market_value = shares * close_price if close_price is not None else None
        unrealized_pl = market_value - (shares * entry_price) if market_value is not None else None
        unrealized_pl_pct = ((close_price / entry_price) - 1.0) * 100.0 if close_price is not None and entry_price > 0 else None
        return {
            "id": int(row.get("id") or 0),
            "portfolio_id": int(row.get("portfolio_id") or 0),
            "portfolio_name": str(row.get("portfolio_name") or DEFAULT_PORTFOLIO_NAME),
            "ticker": str(row.get("ticker") or "").upper(),
            "shares": round(shares, 6),
            "entry_price": round(entry_price, 6),
            "opened_at": _to_iso_date(row.get("opened_at")),
            "notes": str(row.get("notes") or ""),
            "created_at": _to_iso_datetime(row.get("created_at")),
            "updated_at": _to_iso_datetime(row.get("updated_at")),
            "seed_shares": round(_safe_float(row.get("seed_shares")) or shares, 6),
            "seed_entry_price": round(_safe_float(row.get("seed_entry_price")) or entry_price, 6),
            "realized_pl": round(_safe_float(row.get("realized_pl")) or 0.0, 2),
            "is_closed": bool(row.get("is_closed")),
            "market_value": round(market_value, 2) if market_value is not None else None,
            "unrealized_pl": round(unrealized_pl, 2) if unrealized_pl is not None else None,
            "unrealized_pl_pct": round(unrealized_pl_pct, 2) if unrealized_pl_pct is not None else None,
            "transactions": [self._serialize_transaction(item) for item in list(row.get("transactions") or [])],
            "advice": {
                "as_of_date": _to_iso_date(row.get("as_of_date")),
                "latest_trade_date": _to_iso_date(row.get("latest_trade_date")),
                "market_data_status": str(row.get("market_data_status") or "pending"),
                "close_price": _round_price(_safe_float(row.get("close_price"))),
                "signal_status": str(row.get("signal_status") or "review"),
                "stop_loss_price": _round_price(_safe_float(row.get("stop_loss_price"))),
                "tp1_price": _round_price(_safe_float(row.get("tp1_price"))),
                "tp2_price": _round_price(_safe_float(row.get("tp2_price"))),
                "tp1_sell_fraction": _safe_float(row.get("tp1_sell_fraction")),
                "tp2_sell_fraction": _safe_float(row.get("tp2_sell_fraction")),
                "average_up_price": _round_price(_safe_float(row.get("average_up_price"))),
                "average_up_share_fraction": _safe_float(row.get("average_up_share_fraction")),
                "blended_entry_after_average_up": _round_price(_safe_float(row.get("blended_entry_after_average_up"))),
                "net_cost_after_tp1": _round_price(_safe_float(row.get("net_cost_after_tp1"))),
                "remaining_cost_basis_after_tp1": _round_price(_safe_float(row.get("remaining_cost_basis_after_tp1"))),
                "explanation": str(row.get("explanation") or ""),
                "data_source": str(row.get("data_source") or ""),
                "signal_context": dict(row.get("signal_context_json") or {}),
                "refreshed_at": _to_iso_datetime(row.get("refreshed_at")),
            },
        }

    def _get_or_create_portfolio(self, portfolio_name: str, *, actor_user_id: int | None) -> dict[str, Any]:
        clean_name = str(portfolio_name or "").strip() or DEFAULT_PORTFOLIO_NAME
        portfolio = self.repository.get_or_create_portfolio(name=clean_name, created_by_user_id=actor_user_id)
        if portfolio is None:
            raise ValueError("Failed to access portfolio.")
        return portfolio

    def _normalize_ticker(self, ticker: str) -> str:
        normalized = normalize_ticker_symbol(ticker)
        if not normalized:
            raise ValueError("Ticker is required.")
        return normalized

    def _parse_positive_number(self, value: object, *, field: str) -> float:
        try:
            parsed = float(value)
        except (TypeError, ValueError) as exc:
            raise ValueError(f"{field} must be a number.") from exc
        if parsed <= 0:
            raise ValueError(f"{field} must be greater than 0.")
        return parsed

    def _parse_date(self, value: object, *, field: str) -> dt.date:
        try:
            return dt.date.fromisoformat(str(value))
        except (TypeError, ValueError) as exc:
            raise ValueError(f"{field} must be an ISO date (YYYY-MM-DD).") from exc

    def _parse_non_negative_number(self, value: object, *, field: str) -> float:
        if value in (None, ""):
            return 0.0
        try:
            parsed = float(value)
        except (TypeError, ValueError) as exc:
            raise ValueError(f"{field} must be a number.") from exc
        if parsed < 0:
            raise ValueError(f"{field} must be 0 or greater.")
        return parsed

    def _build_positions_with_transactions(self, base_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        position_ids = [int(row.get("id") or 0) for row in base_rows if int(row.get("id") or 0) > 0]
        transactions = self.repository.list_position_transactions(position_ids)
        grouped: dict[int, list[dict[str, Any]]] = {}
        for item in transactions:
            grouped.setdefault(int(item.get("position_id") or 0), []).append(item)

        payload: list[dict[str, Any]] = []
        for row in base_rows:
            position_id = int(row.get("id") or 0)
            state = self._compute_position_state(row, grouped.get(position_id, []))
            payload.append({**row, **state})
        return payload

    def _compute_position_state(self, row: dict[str, Any], transactions: list[dict[str, Any]]) -> dict[str, Any]:
        seed_shares = _safe_float(row.get("shares")) or 0.0
        seed_entry_price = _safe_float(row.get("entry_price")) or 0.0
        current_shares = seed_shares
        total_cost = (seed_shares * seed_entry_price) + (_safe_float(row.get("fees")) or 0.0)
        realized_pl = 0.0
        serialized_transactions = [
            {
                "id": 0,
                "position_id": int(row.get("id") or 0),
                "trade_date": _to_iso_date(row.get("opened_at")),
                "side": "buy",
                "shares": round(seed_shares, 6),
                "price": _round_price(seed_entry_price),
                "fees": 0.0,
                "notes": "Initial position",
                "created_at": _to_iso_datetime(row.get("created_at")),
            }
        ]
        serialized_transactions.extend(self._serialize_transaction(item) for item in transactions)

        for item in transactions:
            side = str(item.get("side") or "").lower()
            shares = _safe_float(item.get("shares")) or 0.0
            price = _safe_float(item.get("price")) or 0.0
            fees = _safe_float(item.get("fees")) or 0.0
            if shares <= 0:
                continue
            if side == "buy":
                total_cost += (shares * price) + fees
                current_shares += shares
                continue
            if side == "sell":
                if current_shares <= 0:
                    continue
                average_cost = total_cost / current_shares if current_shares > 0 else 0.0
                realized_pl += (shares * price) - fees - (shares * average_cost)
                total_cost -= shares * average_cost
                current_shares = max(0.0, current_shares - shares)

        entry_price = (total_cost / current_shares) if current_shares > 0 else 0.0
        first_buy_date = _to_iso_date(row.get("opened_at")) or ""
        if serialized_transactions:
            buy_dates = [str(item.get("trade_date") or "") for item in serialized_transactions if str(item.get("side") or "").lower() == "buy" and str(item.get("trade_date") or "")]
            if buy_dates:
                first_buy_date = min(([first_buy_date] if first_buy_date else []) + buy_dates)

        return {
            "shares": round(current_shares, 6),
            "entry_price": round(entry_price, 6) if current_shares > 0 else 0.0,
            "opened_at": first_buy_date or None,
            "seed_shares": seed_shares,
            "seed_entry_price": seed_entry_price,
            "realized_pl": round(realized_pl, 2),
            "is_closed": current_shares <= 0,
            "transactions": serialized_transactions,
        }

    def _serialize_transaction(self, row: dict[str, Any]) -> dict[str, Any]:
        return {
            "id": int(row.get("id") or 0),
            "position_id": int(row.get("position_id") or 0),
            "trade_date": _to_iso_date(row.get("trade_date")),
            "side": str(row.get("side") or "").lower(),
            "shares": round(_safe_float(row.get("shares")) or 0.0, 6),
            "price": _round_price(_safe_float(row.get("price"))),
            "fees": _round_price(_safe_float(row.get("fees"))),
            "notes": str(row.get("notes") or ""),
            "created_at": _to_iso_datetime(row.get("created_at")),
        }

    def _require_configured(self) -> None:
        if not self.repository.is_configured():
            raise ValueError("TICKER_SCREENER_DATABASE_URL not set.")


def _to_iso_date(value: object) -> str | None:
    if isinstance(value, dt.datetime):
        return value.date().isoformat()
    if isinstance(value, dt.date):
        return value.isoformat()
    text = str(value or "").strip()
    return text or None


def _to_iso_datetime(value: object) -> str | None:
    if isinstance(value, dt.datetime):
        return value.isoformat()
    text = str(value or "").strip()
    return text or None


def _safe_float(value: object) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _safe_int(value: object) -> int | None:
    if value in (None, ""):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _round_price(value: float | None) -> float | None:
    if value is None:
        return None
    return round(float(value), 2)


def _compute_atr(frame: pd.DataFrame, *, length: int) -> pd.Series:
    high_low = frame["High"] - frame["Low"]
    high_close = (frame["High"] - frame["Close"].shift(1)).abs()
    low_close = (frame["Low"] - frame["Close"].shift(1)).abs()
    true_range = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
    return true_range.rolling(length).mean()


def _compute_remaining_cost_basis_per_share(
    *,
    shares: float,
    entry_price: float,
    take_profit_price: float | None,
    sell_fraction: float,
) -> tuple[float | None, float | None]:
    if shares <= 0 or entry_price <= 0 or take_profit_price is None or sell_fraction <= 0 or sell_fraction >= 1:
        return None, None
    total_cost_basis = shares * entry_price
    sold_shares = shares * sell_fraction
    remaining_shares = shares - sold_shares
    if remaining_shares <= 0:
        return None, None
    sale_proceeds = sold_shares * take_profit_price
    remaining_cost_basis_total = total_cost_basis - sale_proceeds
    remaining_cost_basis_per_share = remaining_cost_basis_total / remaining_shares
    return remaining_cost_basis_per_share, remaining_cost_basis_total


def _compute_blended_entry_after_average_up(
    *,
    shares: float,
    entry_price: float,
    add_price: float | None,
    add_fraction: float,
) -> float | None:
    if shares <= 0 or entry_price <= 0 or add_price is None or add_fraction <= 0:
        return None
    added_shares = shares * add_fraction
    total_shares = shares + added_shares
    if total_shares <= 0:
        return None
    total_cost = (shares * entry_price) + (added_shares * add_price)
    return total_cost / total_shares
