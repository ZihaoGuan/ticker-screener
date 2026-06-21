from __future__ import annotations

import json
import math
import os
from typing import Any
from urllib import error, parse, request


FLASHALPHA_BASE_URL = "https://lab.flashalpha.com"


def fetch_gex_snapshot(
    *,
    symbol: str,
    expiration: str | None = None,
    min_oi: int = 0,
    api_key: str | None = None,
    base_url: str | None = None,
    timeout_seconds: int = 20,
) -> dict[str, Any]:
    normalized_symbol = str(symbol or "").strip().upper()
    if not normalized_symbol:
        raise ValueError("Symbol is required.")

    resolved_api_key = str(api_key or os.getenv("FLASHALPHA_API_KEY") or "").strip()
    if not resolved_api_key:
        raise ValueError("FLASHALPHA_API_KEY is not set.")

    encoded_symbol = parse.quote(normalized_symbol, safe="")
    query_params: dict[str, str] = {}
    if expiration:
        query_params["expiration"] = str(expiration).strip()
    if min_oi > 0:
        query_params["min_oi"] = str(int(min_oi))
    query_string = f"?{parse.urlencode(query_params)}" if query_params else ""
    url = f"{(base_url or FLASHALPHA_BASE_URL).rstrip('/')}/v1/exposure/gex/{encoded_symbol}{query_string}"

    req = request.Request(url, headers={"X-Api-Key": resolved_api_key, "Accept": "application/json"})
    try:
        with request.urlopen(req, timeout=timeout_seconds) as response:
            payload = json.loads(response.read().decode("utf-8"))
    except error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        detail = body.strip() or f"HTTP {exc.code}"
        raise ValueError(f"FlashAlpha request failed: {detail}") from exc
    except error.URLError as exc:
        raise ValueError(f"FlashAlpha request failed: {exc.reason}") from exc

    if not isinstance(payload, dict):
        raise ValueError("FlashAlpha response was not a JSON object.")
    return payload


def summarize_gex_payload(payload: dict[str, Any]) -> dict[str, Any]:
    symbol = str(payload.get("symbol") or "").strip().upper()
    underlying_price = _to_float(payload.get("underlying_price"))
    net_gex = _to_float(payload.get("net_gex"))
    gamma_flip = _to_float(payload.get("gamma_flip"))
    strikes = payload.get("strikes") if isinstance(payload.get("strikes"), list) else []

    call_wall: float | None = None
    put_wall: float | None = None
    atm_pin_strike: float | None = None
    top_net_gex_strike: float | None = None
    put_call_oi_ratio: float | None = None

    strongest_call_gex = -1.0
    strongest_put_gex = -1.0
    strongest_abs_net = -1.0
    total_call_oi = 0.0
    total_put_oi = 0.0
    pin_candidates: list[tuple[float, float]] = []

    for item in strikes:
        if not isinstance(item, dict):
            continue
        strike = _to_float(item.get("strike"))
        if strike is None:
            continue
        call_gex = max(_to_float(item.get("call_gex")) or 0.0, 0.0)
        put_gex = max(_to_float(item.get("put_gex")) or 0.0, 0.0)
        abs_net_gex = abs(_to_float(item.get("net_gex")) or 0.0)
        call_oi = max(_to_float(item.get("call_oi")) or 0.0, 0.0)
        put_oi = max(_to_float(item.get("put_oi")) or 0.0, 0.0)
        total_call_oi += call_oi
        total_put_oi += put_oi
        if call_gex > strongest_call_gex:
            strongest_call_gex = call_gex
            call_wall = strike
        if put_gex > strongest_put_gex:
            strongest_put_gex = put_gex
            put_wall = strike
        if abs_net_gex > strongest_abs_net:
            strongest_abs_net = abs_net_gex
            top_net_gex_strike = strike
        pin_candidates.append((strike, call_gex + put_gex))

    if total_call_oi > 0:
        put_call_oi_ratio = round(total_put_oi / total_call_oi, 2)

    if underlying_price is not None and pin_candidates:
        pin_candidates.sort(key=lambda item: (-item[1], abs(item[0] - underlying_price)))
        atm_pin_strike = pin_candidates[0][0]

    distance_to_flip_pct = None
    if underlying_price not in (None, 0) and gamma_flip not in (None, 0):
        distance_to_flip_pct = round(((underlying_price / gamma_flip) - 1.0) * 100.0, 2)

    gex_regime = "negative" if net_gex is not None and net_gex < 0 else "positive"
    gex_label = "Negative Gamma" if gex_regime == "negative" else "Positive Gamma"

    return {
        "ticker": symbol,
        "as_of": str(payload.get("as_of") or ""),
        "spot": round(underlying_price, 3) if underlying_price is not None else None,
        "net_gex": round(net_gex, 2) if net_gex is not None else None,
        "gex_regime": gex_regime,
        "gex_label": gex_label,
        "gamma_flip": round(gamma_flip, 2) if gamma_flip is not None else None,
        "distance_to_flip_pct": distance_to_flip_pct,
        "call_wall": round(call_wall, 2) if call_wall is not None else None,
        "put_wall": round(put_wall, 2) if put_wall is not None else None,
        "atm_pin_strike": round(atm_pin_strike, 2) if atm_pin_strike is not None else None,
        "top_net_gex_strike": round(top_net_gex_strike, 2) if top_net_gex_strike is not None else None,
        "put_call_oi_ratio": put_call_oi_ratio,
        "strike_count": len([item for item in strikes if isinstance(item, dict)]),
        "summary": _build_summary(
            gex_regime=gex_regime,
            gamma_flip=gamma_flip,
            underlying_price=underlying_price,
            put_wall=put_wall,
            call_wall=call_wall,
        ),
        "methodology": "FlashAlpha GEX API snapshot persisted at close; dashboard reads stored DB summary only.",
    }


def _to_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        converted = float(value)
    except (TypeError, ValueError):
        return None
    if math.isnan(converted) or math.isinf(converted):
        return None
    return converted


def _build_summary(
    *,
    gex_regime: str,
    gamma_flip: float | None,
    underlying_price: float | None,
    put_wall: float | None,
    call_wall: float | None,
) -> str:
    regime_copy = "Dealers likely dampen moves" if gex_regime == "positive" else "Dealers may amplify moves"
    if gamma_flip is not None and underlying_price is not None:
        flip_copy = f"spot {'above' if underlying_price >= gamma_flip else 'below'} gamma flip {gamma_flip:.2f}"
    else:
        flip_copy = "gamma flip unavailable"
    walls: list[str] = []
    if put_wall is not None:
        walls.append(f"put wall {put_wall:.2f}")
    if call_wall is not None:
        walls.append(f"call wall {call_wall:.2f}")
    wall_copy = ", ".join(walls) if walls else "walls unavailable"
    return f"{regime_copy}; {flip_copy}; {wall_copy}."
