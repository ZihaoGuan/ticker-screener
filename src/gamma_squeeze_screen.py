from __future__ import annotations

from dataclasses import asdict, dataclass
import datetime as dt
from typing import Any, Iterable

from .config import AppConfig
from .flashalpha_gex import build_gamma_exposure_report
from .ticker_filters import filter_symbols, load_excluded_tickers
from .universe import UniverseTicker


DEFAULT_GAMMA_SQUEEZE_TICKERS: tuple[str, ...] = (
    "SPY",
    "QQQ",
    "IWM",
    "TSLA",
    "NVDA",
    "AAPL",
    "META",
    "AMZN",
    "GOOGL",
    "MSFT",
    "AMD",
    "PLTR",
    "SMCI",
    "NFLX",
)

_NEGATIVE_GEX_FLOOR_BN = -2.0
_POSITIVE_GEX_CEILING_BN = 2.0


@dataclass(frozen=True)
class GammaSqueezeHit:
    ticker: str
    sector: str | None
    industry: str | None
    exchange: str | None
    as_of: str
    spot_price: float
    net_gex: float
    net_gex_bn: float
    gex_regime: str
    gamma_flip: float | None
    distance_to_flip_pct: float | None
    call_wall: float | None
    put_wall: float | None
    distance_to_call_wall_pct: float | None
    distance_to_put_wall_pct: float | None
    top_net_gex_strike: float | None
    put_call_oi_ratio: float | None
    call_oi_above_spot: float
    total_call_oi: float
    total_put_oi: float
    total_oi: float
    call_oi_above_ratio: float
    squeeze_score: float
    score_components: dict[str, float]
    reasons: list[str]
    source_url: str

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True)
class GammaSqueezeScreenResult:
    run_date: str
    total_tickers: int
    passed_tickers: int
    failed_tickers: list[dict[str, str]]
    hits: list[GammaSqueezeHit]

    def to_dict(self) -> dict[str, object]:
        return {
            "run_date": self.run_date,
            "total_tickers": self.total_tickers,
            "passed_tickers": self.passed_tickers,
            "failed_tickers": self.failed_tickers,
            "hits": [item.to_dict() for item in self.hits],
        }


def default_gamma_squeeze_universe(config: AppConfig, *, limit: int | None = None) -> list[UniverseTicker]:
    excluded = load_excluded_tickers(config)
    tickers = list(filter_symbols(DEFAULT_GAMMA_SQUEEZE_TICKERS, excluded))
    if limit is not None and limit > 0:
        tickers = tickers[:limit]
    return [UniverseTicker(symbol=ticker) for ticker in tickers]


def build_gamma_squeeze_hit(
    report: dict[str, Any],
    *,
    ticker: UniverseTicker,
    min_squeeze_score: float = 65.0,
) -> GammaSqueezeHit | None:
    normalized_report = _normalize_report(report)
    squeeze_score = _compute_squeeze_score(normalized_report)
    if squeeze_score < float(min_squeeze_score):
        return None
    return _build_hit_payload(normalized_report, ticker=ticker, squeeze_score=squeeze_score)


def run_gamma_squeeze_screen(
    config: AppConfig,
    tickers: Iterable[UniverseTicker],
    *,
    as_of_date: dt.date | None = None,
    min_squeeze_score: float = 65.0,
    timeout_seconds: int = 20,
) -> GammaSqueezeScreenResult:
    universe = list(tickers)
    hits: list[GammaSqueezeHit] = []
    failures: list[dict[str, str]] = []
    total_tickers = len(universe)
    run_date = as_of_date or dt.date.today()

    print(f"starting gamma squeeze screen: total={total_tickers}")

    for position, ticker in enumerate(universe, start=1):
        print(f"[{position}/{total_tickers}] screening {ticker.symbol} | passed={len(hits)}")
        try:
            report = build_gamma_exposure_report(symbol=ticker.symbol, timeout_seconds=timeout_seconds)
            hit = build_gamma_squeeze_hit(report, ticker=ticker, min_squeeze_score=min_squeeze_score)
            if hit is None:
                print(f"[{position}/{total_tickers}] {ticker.symbol} filtered: squeeze score below threshold | passed={len(hits)}")
                continue
            hits.append(hit)
            print(
                f"[{position}/{total_tickers}] {ticker.symbol} passed gamma squeeze "
                f"{hit.squeeze_score:.1f} regime={hit.gex_regime} | passed={len(hits)}"
            )
        except Exception as exc:
            failures.append({"ticker": ticker.symbol, "error": str(exc)})
            print(f"[{position}/{total_tickers}] {ticker.symbol} failed: {exc}")

    hits.sort(key=lambda item: (-item.squeeze_score, item.ticker))
    return GammaSqueezeScreenResult(
        run_date=run_date.isoformat(),
        total_tickers=total_tickers,
        passed_tickers=len(hits),
        failed_tickers=failures,
        hits=hits,
    )


def _normalize_report(report: dict[str, Any]) -> dict[str, Any]:
    strikes = report.get("strikes") if isinstance(report.get("strikes"), list) else []
    normalized_strikes: list[dict[str, float]] = []
    for item in strikes:
        if not isinstance(item, dict):
            continue
        strike = _coerce_float(item.get("strike"))
        if strike is None:
            continue
        normalized_strikes.append(
            {
                "strike": strike,
                "call_oi": _coerce_float(item.get("call_oi")) or 0.0,
                "put_oi": _coerce_float(item.get("put_oi")) or 0.0,
                "net_gex": _coerce_float(item.get("net_gex")) or 0.0,
            }
        )

    spot_price = _coerce_float(report.get("underlying_price")) or 0.0
    total_call_oi = sum(item["call_oi"] for item in normalized_strikes)
    total_put_oi = sum(item["put_oi"] for item in normalized_strikes)
    call_oi_above_spot = sum(item["call_oi"] for item in normalized_strikes if item["strike"] > spot_price)
    total_oi = total_call_oi + total_put_oi
    call_oi_above_ratio = (call_oi_above_spot / total_oi) if total_oi > 0 else 0.0

    return {
        "as_of": str(report.get("as_of") or ""),
        "spot_price": spot_price,
        "net_gex": _coerce_float(report.get("net_gex")) or 0.0,
        "gamma_flip": _coerce_float(report.get("gamma_flip")),
        "call_wall": _coerce_float(report.get("call_wall")),
        "put_wall": _coerce_float(report.get("put_wall")),
        "top_net_gex_strike": _coerce_float(report.get("top_net_gex_strike")),
        "put_call_oi_ratio": _coerce_float(report.get("put_call_oi_ratio")),
        "source_url": str(report.get("source_url") or ""),
        "strikes": normalized_strikes,
        "call_oi_above_spot": call_oi_above_spot,
        "total_call_oi": total_call_oi,
        "total_put_oi": total_put_oi,
        "total_oi": total_oi,
        "call_oi_above_ratio": call_oi_above_ratio,
    }


def _build_hit_payload(normalized_report: dict[str, Any], *, ticker: UniverseTicker, squeeze_score: float) -> GammaSqueezeHit:
    spot_price = float(normalized_report["spot_price"])
    net_gex = float(normalized_report["net_gex"])
    gamma_flip = _coerce_float(normalized_report.get("gamma_flip"))
    call_wall = _coerce_float(normalized_report.get("call_wall"))
    put_wall = _coerce_float(normalized_report.get("put_wall"))
    put_call_oi_ratio = _coerce_float(normalized_report.get("put_call_oi_ratio"))
    call_oi_above_ratio = float(normalized_report["call_oi_above_ratio"])
    distance_to_flip_pct = _distance_pct(spot_price, gamma_flip)
    distance_to_call_wall_pct = _distance_pct(spot_price, call_wall)
    distance_to_put_wall_pct = _distance_pct(spot_price, put_wall)
    score_components = _score_components(normalized_report)
    reasons = _build_reasons(
        spot_price=spot_price,
        net_gex=net_gex,
        gamma_flip=gamma_flip,
        call_wall=call_wall,
        put_call_oi_ratio=put_call_oi_ratio,
        call_oi_above_ratio=call_oi_above_ratio,
        distance_to_flip_pct=distance_to_flip_pct,
        distance_to_call_wall_pct=distance_to_call_wall_pct,
    )
    return GammaSqueezeHit(
        ticker=ticker.symbol,
        sector=ticker.sector,
        industry=ticker.industry,
        exchange=ticker.exchange,
        as_of=str(normalized_report.get("as_of") or ""),
        spot_price=round(spot_price, 4),
        net_gex=round(net_gex, 2),
        net_gex_bn=round(net_gex / 1_000_000_000.0, 4),
        gex_regime="negative" if net_gex < 0 else "positive",
        gamma_flip=round(gamma_flip, 4) if gamma_flip is not None else None,
        distance_to_flip_pct=distance_to_flip_pct,
        call_wall=round(call_wall, 4) if call_wall is not None else None,
        put_wall=round(put_wall, 4) if put_wall is not None else None,
        distance_to_call_wall_pct=distance_to_call_wall_pct,
        distance_to_put_wall_pct=distance_to_put_wall_pct,
        top_net_gex_strike=_coerce_float(normalized_report.get("top_net_gex_strike")),
        put_call_oi_ratio=round(put_call_oi_ratio, 4) if put_call_oi_ratio is not None else None,
        call_oi_above_spot=round(float(normalized_report["call_oi_above_spot"]), 2),
        total_call_oi=round(float(normalized_report["total_call_oi"]), 2),
        total_put_oi=round(float(normalized_report["total_put_oi"]), 2),
        total_oi=round(float(normalized_report["total_oi"]), 2),
        call_oi_above_ratio=round(call_oi_above_ratio, 4),
        squeeze_score=round(squeeze_score, 2),
        score_components=score_components,
        reasons=reasons,
        source_url=str(normalized_report.get("source_url") or ""),
    )


def _compute_squeeze_score(normalized_report: dict[str, Any]) -> float:
    components = _score_components(normalized_report)
    score = (
        (components["gex_regime"] * 0.35)
        + (components["flip_proximity"] * 0.25)
        + (components["call_wall_proximity"] * 0.20)
        + (components["call_oi_above"] * 0.10)
        + (components["put_call_oi_ratio"] * 0.10)
    )
    return max(0.0, min(100.0, score))


def _score_components(normalized_report: dict[str, Any]) -> dict[str, float]:
    net_gex_bn = (float(normalized_report["net_gex"]) / 1_000_000_000.0) if normalized_report["net_gex"] else 0.0
    gamma_flip = _coerce_float(normalized_report.get("gamma_flip"))
    call_wall = _coerce_float(normalized_report.get("call_wall"))
    spot_price = float(normalized_report["spot_price"])
    distance_to_flip_pct = _distance_pct(spot_price, gamma_flip)
    distance_to_call_wall_pct = _distance_pct(spot_price, call_wall)
    put_call_oi_ratio = _coerce_float(normalized_report.get("put_call_oi_ratio"))
    call_oi_above_ratio = float(normalized_report["call_oi_above_ratio"])

    gex_score = 100.0 * ((_POSITIVE_GEX_CEILING_BN - net_gex_bn) / (_POSITIVE_GEX_CEILING_BN - _NEGATIVE_GEX_FLOOR_BN))
    gex_score = max(0.0, min(100.0, gex_score))

    flip_score = 40.0
    if distance_to_flip_pct is not None:
        abs_distance = abs(distance_to_flip_pct)
        if abs_distance <= 1.0:
            flip_score = 100.0
        elif abs_distance <= 2.5:
            flip_score = 85.0
        elif abs_distance <= 5.0:
            flip_score = 65.0
        elif abs_distance <= 8.0:
            flip_score = 40.0
        else:
            flip_score = 10.0
        if distance_to_flip_pct < 0:
            flip_score = min(100.0, flip_score + 10.0)

    call_wall_score = 0.0
    if distance_to_call_wall_pct is not None:
        if 0.0 <= distance_to_call_wall_pct <= 2.0:
            call_wall_score = 100.0
        elif 2.0 < distance_to_call_wall_pct <= 5.0:
            call_wall_score = 80.0
        elif 5.0 < distance_to_call_wall_pct <= 8.0:
            call_wall_score = 55.0
        elif distance_to_call_wall_pct < 0.0:
            call_wall_score = 20.0

    call_oi_score = max(0.0, min(100.0, call_oi_above_ratio * 160.0))

    pcr_score = 50.0
    if put_call_oi_ratio is not None:
        if put_call_oi_ratio <= 0.7:
            pcr_score = 100.0
        elif put_call_oi_ratio <= 1.0:
            pcr_score = 75.0
        elif put_call_oi_ratio <= 1.3:
            pcr_score = 45.0
        elif put_call_oi_ratio <= 1.6:
            pcr_score = 20.0
        else:
            pcr_score = 0.0

    return {
        "gex_regime": round(gex_score, 2),
        "flip_proximity": round(flip_score, 2),
        "call_wall_proximity": round(call_wall_score, 2),
        "call_oi_above": round(call_oi_score, 2),
        "put_call_oi_ratio": round(pcr_score, 2),
    }


def _build_reasons(
    *,
    spot_price: float,
    net_gex: float,
    gamma_flip: float | None,
    call_wall: float | None,
    put_call_oi_ratio: float | None,
    call_oi_above_ratio: float,
    distance_to_flip_pct: float | None,
    distance_to_call_wall_pct: float | None,
) -> list[str]:
    reasons = [
        (
            f"Net GEX {net_gex / 1_000_000_000.0:+.2f}B in "
            f"{'negative' if net_gex < 0 else 'positive'} gamma regime"
        ),
        f"Call OI above spot {call_oi_above_ratio * 100.0:.1f}% of total listed OI",
    ]
    if gamma_flip is not None and distance_to_flip_pct is not None:
        reasons.append(
            f"Spot {spot_price:.2f} is {abs(distance_to_flip_pct):.2f}% "
            f"{'below' if distance_to_flip_pct < 0 else 'above'} gamma flip {gamma_flip:.2f}"
        )
    if call_wall is not None and distance_to_call_wall_pct is not None:
        if distance_to_call_wall_pct >= 0:
            reasons.append(f"Call wall {call_wall:.2f} sits {distance_to_call_wall_pct:.2f}% overhead")
        else:
            reasons.append(f"Spot already above call wall {call_wall:.2f} by {abs(distance_to_call_wall_pct):.2f}%")
    if put_call_oi_ratio is not None:
        reasons.append(f"Put/Call OI ratio {put_call_oi_ratio:.2f}")
    return reasons


def _distance_pct(spot_price: float, target: float | None) -> float | None:
    if target in (None, 0.0) or spot_price <= 0:
        return None
    return round(((target / spot_price) - 1.0) * 100.0, 2)


def _coerce_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None
