from __future__ import annotations

import copy
import datetime as dt
import threading
import time
from pathlib import Path
from typing import Any

from ...momentum_etf_holdings import MOMENTUM_ETFS, load_momentum_etf_holdings_cache
from .watchlist_service import WatchlistService


_CACHE_TTL_SECONDS = 10 * 60
_cache: dict[tuple[str, str], tuple[float, dict[str, Any]]] = {}
_cache_lock = threading.Lock()


class MomentumEtfPortfolioService:
    """Joins the issuer holdings cache to the persisted Top Hits read model."""

    def __init__(self, *, artifacts_dir: Path, watchlist_service: WatchlistService):
        self.artifacts_dir = Path(artifacts_dir)
        self.watchlist_service = watchlist_service

    def get_payload(self) -> dict[str, Any]:
        holdings_cache = load_momentum_etf_holdings_cache(self.artifacts_dir)
        generated_at = str((holdings_cache or {}).get("generated_at") or "")
        cache_key = (str(self.artifacts_dir), generated_at)
        cached = _read_cache(cache_key)
        if cached is not None:
            return cached

        top_hits = self.watchlist_service.get_scanner_top_hits_snapshot_payload()
        top_hit_by_ticker = {
            str(row.get("ticker") or "").upper(): row
            for row in top_hits.get("rows", [])
            if isinstance(row, dict) and str(row.get("ticker") or "").strip()
        }
        result_map = (holdings_cache or {}).get("results", {})
        result_map = result_map if isinstance(result_map, dict) else {}
        funds: list[dict[str, Any]] = []
        by_ticker: dict[str, dict[str, Any]] = {}
        for spec in MOMENTUM_ETFS:
            source = result_map.get(spec.ticker)
            holdings = source.get("holdings", []) if isinstance(source, dict) else []
            holdings = [item for item in holdings if isinstance(item, dict)]
            funds.append({
                "ticker": spec.ticker,
                "name": spec.name,
                "provider": spec.provider,
                "source_url": str((source or {}).get("source_url") or spec.source_url),
                "issuer_source_url": spec.source_url,
                "source_kind": str((source or {}).get("source_kind") or "issuer"),
                "as_of_date": str((source or {}).get("as_of_date") or ""),
                "fetched_at": str((source or {}).get("fetched_at") or ""),
                "holding_count": len(holdings),
                "reported_holding_count": int((source or {}).get("reported_holding_count") or len(holdings)),
                "is_complete": bool((source or {}).get("is_complete", bool(source))),
                "top_hits_count": sum(1 for holding in holdings if str(holding.get("ticker") or "").upper() in top_hit_by_ticker),
                "available": bool(source),
            })
            for holding in holdings:
                ticker = str(holding.get("ticker") or "").upper().strip()
                if not ticker:
                    continue
                bucket = by_ticker.setdefault(ticker, {
                    "ticker": ticker,
                    "company": str(holding.get("name") or ""),
                    "sector": str(holding.get("sector") or ""),
                    "funds": [],
                    "etf_count": 0,
                    "combined_weight": 0.0,
                })
                weight = _coerce_float(holding.get("weight"))
                bucket["funds"].append({"ticker": spec.ticker, "weight": weight})
                bucket["etf_count"] += 1
                bucket["combined_weight"] += weight or 0.0

        rows: list[dict[str, Any]] = []
        for ticker, row in by_ticker.items():
            top_hit = top_hit_by_ticker.get(ticker)
            if top_hit:
                row.update({
                    "top_hit": True,
                    "scanner_count": int(top_hit.get("scanner_count") or 0),
                    "scanner_labels": list(top_hit.get("scanner_labels") or []),
                    "daily_rs_rating": top_hit.get("daily_rs_rating"),
                    "stage_analysis": top_hit.get("stage_analysis"),
                    "strike_zone": top_hit.get("strike_zone"),
                    "atr_to_sma50": top_hit.get("atr_to_sma50"),
                    "earnings_date": top_hit.get("earnings_date"),
                    "earnings_days": top_hit.get("earnings_days"),
                    "change_pct": top_hit.get("change_pct"),
                })
                row["company"] = str(top_hit.get("company") or row["company"])
                row["sector"] = str(top_hit.get("sector") or row["sector"])
            else:
                row.update({"top_hit": False, "scanner_count": 0, "scanner_labels": []})
            row["funds"].sort(key=lambda item: str(item["ticker"]))
            row["combined_weight"] = round(float(row["combined_weight"]), 4)
            rows.append(row)
        rows.sort(key=lambda item: (-int(item["etf_count"]), -int(item["scanner_count"]), -float(item["combined_weight"]), str(item["ticker"])))
        payload = {
            "generated_at": dt.datetime.now(dt.timezone.utc).isoformat(),
            "holdings_generated_at": generated_at or None,
            "source_data_as_of": top_hits.get("snapshot", {}).get("source_data_as_of"),
            "errors": dict((holdings_cache or {}).get("errors") or {}),
            "funds": funds,
            "rows": rows,
            "summary": {
                "total_unique_holdings": len(rows),
                "overlap_holding_count": sum(1 for row in rows if int(row["etf_count"]) > 1),
                "top_hits_count": sum(1 for row in rows if row["top_hit"]),
            },
        }
        _write_cache(cache_key, payload)
        return payload


def _coerce_float(value: object) -> float | None:
    try:
        return float(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def _read_cache(key: tuple[str, str]) -> dict[str, Any] | None:
    with _cache_lock:
        entry = _cache.get(key)
        if entry is None or entry[0] < time.time():
            _cache.pop(key, None)
            return None
        return copy.deepcopy(entry[1])


def _write_cache(key: tuple[str, str], payload: dict[str, Any]) -> None:
    with _cache_lock:
        _cache[key] = (time.time() + _CACHE_TTL_SECONDS, copy.deepcopy(payload))


def clear_momentum_etf_portfolio_cache() -> None:
    with _cache_lock:
        _cache.clear()
