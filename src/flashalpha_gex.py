from __future__ import annotations

import json
import math
from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any
from urllib import error, parse, request

import numpy as np


CBOE_DELAYED_QUOTES_BASE_URL = "https://cdn.cboe.com/api/global/delayed_quotes/options"
_DEFAULT_RISK_FREE_RATE = 0.03
_PROFILE_POINT_COUNT = 300


@dataclass(frozen=True)
class _OptionRow:
    expiry: date
    option_type: str
    strike: float
    iv: float
    gamma: float
    open_interest: float
    volume: float
    effective_open_interest: float
    time_to_expiry_years: float


def fetch_gex_snapshot(
    *,
    symbol: str,
    expiration: str | None = None,
    min_oi: int = 0,
    api_key: str | None = None,
    base_url: str | None = None,
    timeout_seconds: int = 20,
) -> dict[str, Any]:
    del api_key

    normalized_symbol = str(symbol or "").strip().upper()
    if not normalized_symbol:
        raise ValueError("Symbol is required.")

    encoded_symbol = parse.quote(normalized_symbol, safe="")
    url = f"{(base_url or CBOE_DELAYED_QUOTES_BASE_URL).rstrip('/')}/{encoded_symbol}.json"
    req = request.Request(url, headers={"Accept": "application/json"})
    try:
        with request.urlopen(req, timeout=timeout_seconds) as response:
            payload = json.loads(response.read().decode("utf-8"))
    except error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        detail = body.strip() or f"HTTP {exc.code}"
        raise ValueError(f"CBOE request failed: {detail}") from exc
    except error.URLError as exc:
        raise ValueError(f"CBOE request failed: {exc.reason}") from exc

    if not isinstance(payload, dict):
        raise ValueError("CBOE response was not a JSON object.")

    data = payload.get("data")
    if not isinstance(data, dict):
        raise ValueError("CBOE response did not include data.")

    options = data.get("options")
    if not isinstance(options, list) or not options:
        raise ValueError(f"CBOE response did not include options for {normalized_symbol}.")

    as_of = _parse_as_of(payload.get("timestamp"))
    spot_price = _to_float(data.get("current_price"))
    if spot_price is None or spot_price <= 0:
        raise ValueError(f"CBOE response did not include a valid spot price for {normalized_symbol}.")

    parsed_rows: list[_OptionRow] = []
    for item in options:
        row = _parse_option_row(item, as_of_date=as_of.date())
        if row is None:
            continue
        if min_oi > 0 and row.effective_open_interest < float(min_oi):
            continue
        parsed_rows.append(row)

    if not parsed_rows:
        raise ValueError(f"No usable options rows returned by CBOE for {normalized_symbol}.")

    selected_expiry, expiry_mode = _select_expiry(
        rows=parsed_rows,
        as_of_date=as_of.date(),
        requested_expiration=expiration,
    )
    selected_rows = [row for row in parsed_rows if row.expiry == selected_expiry]
    if not selected_rows:
        raise ValueError(f"No options rows available for expiry {selected_expiry.isoformat()}.")

    strikes = _build_strike_payload(selected_rows=selected_rows, spot_price=spot_price)
    if not strikes:
        raise ValueError(f"No strike-level GEX data could be derived for {normalized_symbol}.")

    net_gex = sum((_to_float(item.get("net_gex")) or 0.0) for item in strikes)
    gamma_flip = _find_gamma_flip(selected_rows=selected_rows, spot_price=spot_price)

    return {
        "symbol": normalized_symbol,
        "underlying_price": round(spot_price, 4),
        "as_of": as_of.isoformat(),
        "expiration": selected_expiry.isoformat(),
        "expiration_mode": expiry_mode,
        "source": "cboe_delayed_quotes",
        "source_url": url,
        "net_gex": round(net_gex, 2),
        "gamma_flip": round(gamma_flip, 2) if gamma_flip is not None else None,
        "strikes": strikes,
    }


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
        put_gex = abs(min(_to_float(item.get("put_gex")) or 0.0, 0.0))
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
    expiration = str(payload.get("expiration") or "")
    expiration_mode = str(payload.get("expiration_mode") or "")

    return {
        "ticker": symbol,
        "as_of": str(payload.get("as_of") or ""),
        "spot": round(underlying_price, 3) if underlying_price is not None else None,
        "front_expiry": expiration,
        "expiration_mode": expiration_mode,
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
            expiration=expiration,
            expiration_mode=expiration_mode,
        ),
        "methodology": "CBOE delayed options chain, defaulting to 0DTE when available and otherwise the nearest expiry. Gamma exposure uses option gamma times effective OI, with a 0DTE/1DTE volume fallback to better reflect same-day positioning.",
    }


def _parse_as_of(value: Any) -> datetime:
    raw = str(value or "").strip()
    if not raw:
        raise ValueError("CBOE response did not include a timestamp.")
    normalized = raw.replace("Z", "+00:00")
    try:
        return datetime.fromisoformat(normalized)
    except ValueError as exc:
        raise ValueError(f"Unable to parse CBOE timestamp: {raw}") from exc


def _parse_option_row(item: Any, *, as_of_date: date) -> _OptionRow | None:
    if not isinstance(item, dict):
        return None
    option_symbol = str(item.get("option") or "").strip().upper()
    parsed = _parse_occ_option_symbol(option_symbol)
    if parsed is None:
        return None
    expiry, option_type, strike = parsed
    iv = _to_float(item.get("iv"))
    gamma = _to_float(item.get("gamma"))
    open_interest = max(_to_float(item.get("open_interest")) or 0.0, 0.0)
    volume = max(_to_float(item.get("volume")) or 0.0, 0.0)
    if iv is None or iv <= 0 or gamma is None or gamma < 0:
        return None

    business_days = int(np.busday_count(as_of_date, expiry))
    time_to_expiry_years = 1.0 / 252.0 if business_days <= 0 else business_days / 252.0
    effective_open_interest = volume if business_days <= 1 and volume > 0 else open_interest
    if effective_open_interest <= 0:
        return None

    return _OptionRow(
        expiry=expiry,
        option_type=option_type,
        strike=strike,
        iv=iv,
        gamma=gamma,
        open_interest=open_interest,
        volume=volume,
        effective_open_interest=effective_open_interest,
        time_to_expiry_years=time_to_expiry_years,
    )


def _parse_occ_option_symbol(option_symbol: str) -> tuple[date, str, float] | None:
    length = len(option_symbol)
    cp_index = None
    for idx in range(max(0, length - 15), max(0, length - 8)):
        if option_symbol[idx] in {"C", "P"}:
            cp_index = idx
            break
    if cp_index is None or cp_index < 6 or cp_index + 8 >= length + 1:
        return None

    expiry_code = option_symbol[cp_index - 6 : cp_index]
    strike_code = option_symbol[cp_index + 1 : cp_index + 9]
    if len(expiry_code) != 6 or len(strike_code) != 8 or not expiry_code.isdigit() or not strike_code.isdigit():
        return None

    try:
        expiry = datetime.strptime(expiry_code, "%y%m%d").date()
    except ValueError:
        return None

    option_type = "call" if option_symbol[cp_index] == "C" else "put"
    strike = int(strike_code) / 1000.0
    return expiry, option_type, strike


def _select_expiry(
    *,
    rows: list[_OptionRow],
    as_of_date: date,
    requested_expiration: str | None,
) -> tuple[date, str]:
    expiries = sorted({row.expiry for row in rows})
    if not expiries:
        raise ValueError("No expirations were available.")

    if requested_expiration:
        target = date.fromisoformat(str(requested_expiration).strip())
        if target not in expiries:
            raise ValueError(f"Requested expiration {target.isoformat()} is not available in the CBOE chain.")
        return target, "requested"

    if as_of_date in expiries:
        return as_of_date, "0dte"

    future_expiries = [expiry for expiry in expiries if expiry > as_of_date]
    if future_expiries:
        return future_expiries[0], "nearest"

    return expiries[0], "nearest"


def _build_strike_payload(*, selected_rows: list[_OptionRow], spot_price: float) -> list[dict[str, Any]]:
    by_strike: dict[float, dict[str, float]] = defaultdict(
        lambda: {
            "call_gex": 0.0,
            "put_gex": 0.0,
            "call_oi": 0.0,
            "put_oi": 0.0,
        }
    )

    for row in selected_rows:
        strike_bucket = by_strike[row.strike]
        gex_value = row.gamma * row.effective_open_interest * spot_price * spot_price
        if row.option_type == "call":
            strike_bucket["call_gex"] += gex_value
            strike_bucket["call_oi"] += row.effective_open_interest
        else:
            strike_bucket["put_gex"] -= gex_value
            strike_bucket["put_oi"] += row.effective_open_interest

    strikes: list[dict[str, Any]] = []
    for strike in sorted(by_strike):
        strike_bucket = by_strike[strike]
        net_gex = strike_bucket["call_gex"] + strike_bucket["put_gex"]
        strikes.append(
            {
                "strike": round(strike, 3),
                "call_gex": round(strike_bucket["call_gex"], 2),
                "put_gex": round(strike_bucket["put_gex"], 2),
                "net_gex": round(net_gex, 2),
                "call_oi": round(strike_bucket["call_oi"], 2),
                "put_oi": round(strike_bucket["put_oi"], 2),
            }
        )
    return strikes


def _find_gamma_flip(*, selected_rows: list[_OptionRow], spot_price: float) -> float | None:
    valid_rows = [
        row
        for row in selected_rows
        if row.iv > 0 and row.time_to_expiry_years > 0 and row.effective_open_interest > 0
    ]
    if not valid_rows:
        return None

    levels = np.linspace(spot_price * 0.5, spot_price * 1.5, _PROFILE_POINT_COUNT)
    profile = np.array([_net_gamma_at_level(level=level, rows=valid_rows) for level in levels], dtype=float)
    sign_change_idx = np.where(np.diff(np.sign(profile)) != 0)[0]
    if sign_change_idx.size == 0:
        return None

    idx = int(sign_change_idx[0])
    lower_level = float(levels[idx])
    upper_level = float(levels[idx + 1])
    lower_value = float(profile[idx])
    upper_value = float(profile[idx + 1])
    if upper_value == lower_value:
        return lower_level
    return upper_level - ((upper_level - lower_level) * upper_value / (upper_value - lower_value))


def _net_gamma_at_level(*, level: float, rows: list[_OptionRow]) -> float:
    net_gamma = 0.0
    for row in rows:
        gamma = _black_scholes_gamma(
            spot_price=level,
            strike=row.strike,
            volatility=row.iv,
            time_to_expiry_years=row.time_to_expiry_years,
            risk_free_rate=_DEFAULT_RISK_FREE_RATE,
        )
        if gamma is None:
            continue
        exposure = gamma * row.effective_open_interest * level * level
        net_gamma += exposure if row.option_type == "call" else -exposure
    return net_gamma


def _black_scholes_gamma(
    *,
    spot_price: float,
    strike: float,
    volatility: float,
    time_to_expiry_years: float,
    risk_free_rate: float,
) -> float | None:
    if spot_price <= 0 or strike <= 0 or volatility <= 0 or time_to_expiry_years <= 0:
        return None
    sqrt_t = math.sqrt(time_to_expiry_years)
    denom = volatility * sqrt_t
    if denom <= 0:
        return None
    d1 = (
        math.log(spot_price / strike)
        + (risk_free_rate + 0.5 * volatility * volatility) * time_to_expiry_years
    ) / denom
    pdf = math.exp(-0.5 * d1 * d1) / math.sqrt(2.0 * math.pi)
    return pdf / (spot_price * volatility * sqrt_t)


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
    expiration: str,
    expiration_mode: str,
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
    expiry_copy = f"{expiration_mode or 'selected'} expiry {expiration}" if expiration else "expiry unavailable"
    return f"{regime_copy}; {flip_copy}; {wall_copy}; {expiry_copy}."
