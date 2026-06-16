from __future__ import annotations

from dataclasses import asdict, dataclass
import datetime as dt

import numpy as np
import pandas as pd

from .config import AppConfig
from .cookstock_bridge import freeze_cookstock_today, iter_prefetched_cookstock_batches, load_configured_cookstock
from .rs_rating_screen import approximate_rs_rating, compute_latest_weighted_rs_score
from .universe import UniverseTicker


LEIF_HTF_CONFIG = {
    "pole_min_gain": 0.90,
    "pole_max_days": 40,
    "pole_min_vol_ratio": 1.40,
    "pole_min_up_day_pct": 0.55,
    "flag_min_drawdown": 0.10,
    "flag_max_drawdown": 0.25,
    "flag_min_days": 5,
    "flag_max_days": 25,
    "flag_must_be_above_50ma": True,
    "flag_vol_dry_ratio": 0.75,
    "breakout_min_buffer": 0.10,
    "volume_ratio": 1.50,
    "rs_min_rating": 80.0,
    "min_price": 5.0,
    "min_score": 5.0,
}
LEIF_HTF_RS_LOOKBACK = 252
LEIF_HTF_MA_PERIOD = 50
LEIF_HTF_LOOKBACK_DAYS = 260


@dataclass(frozen=True)
class LeifHighTightFlagHit:
    ticker: str
    sector: str | None
    industry: str | None
    exchange: str | None
    signal_date: str
    benchmark_ticker: str
    current_price: float
    pivot_price: float
    breakout_volume_ratio: float
    rs_rating: float
    rs_score: float
    pole_gain_pct: float
    pole_days: int
    flag_days: int
    flag_drawdown_pct: float
    flag_low: float
    flag_high: float
    up_day_pct: float
    score: float
    score_pole: float
    score_flag: float
    score_volume: float
    score_tech: float
    score_breakout: float
    score_catalyst: float
    reasons: list[str]

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True)
class LeifHighTightFlagScreenResult:
    run_date: str
    benchmark_ticker: str
    total_tickers: int
    passed_tickers: int
    failed_tickers: list[dict[str, str]]
    hits: list[LeifHighTightFlagHit]

    def to_dict(self) -> dict[str, object]:
        return {
            "run_date": self.run_date,
            "benchmark_ticker": self.benchmark_ticker,
            "total_tickers": self.total_tickers,
            "passed_tickers": self.passed_tickers,
            "failed_tickers": self.failed_tickers,
            "hits": [item.to_dict() for item in self.hits],
        }


def _normalize_price_frame(frame: pd.DataFrame, *, include_volume: bool = True) -> pd.DataFrame:
    required = ["Close"] if not include_volume else ["Open", "High", "Low", "Close", "Volume"]
    available = {str(column).lower(): column for column in frame.columns}
    missing = [column for column in required if column.lower() not in available]
    if missing:
        return pd.DataFrame()
    normalized = frame[[available[column.lower()] for column in required]].copy()
    normalized.columns = required
    normalized = normalized.dropna(subset=required).sort_index()
    if not isinstance(normalized.index, pd.DatetimeIndex):
        normalized.index = pd.to_datetime(normalized.index)
    return normalized


def _build_price_frame_from_rows(rows: list[dict[str, object]], *, include_volume: bool = True) -> pd.DataFrame:
    if not rows:
        return pd.DataFrame()
    payload: dict[str, object] = {
        "Date": pd.to_datetime([row.get("formatted_date") for row in rows]),
        "Close": [row.get("close") for row in rows],
    }
    if include_volume:
        payload.update(
            {
                "Open": [row.get("open") for row in rows],
                "High": [row.get("high") for row in rows],
                "Low": [row.get("low") for row in rows],
                "Volume": [row.get("volume") for row in rows],
            }
        )
    frame = pd.DataFrame(payload)
    required = ["Date", "Close"] if not include_volume else ["Date", "Open", "High", "Low", "Close", "Volume"]
    return frame.dropna(subset=required).set_index("Date").sort_index()


def _compute_rs_snapshot(stock_frame: pd.DataFrame, benchmark_frame: pd.DataFrame) -> tuple[float | None, float | None]:
    stock = _normalize_price_frame(stock_frame, include_volume=False)
    benchmark = _normalize_price_frame(benchmark_frame, include_volume=False)
    if stock.empty or benchmark.empty:
        return None, None
    aligned = stock.join(benchmark.rename(columns={"Close": "BenchmarkClose"}), how="inner").dropna()
    if len(aligned) < LEIF_HTF_RS_LOOKBACK + 1:
        return None, None
    rs_score = compute_latest_weighted_rs_score(aligned["Close"], aligned["BenchmarkClose"])
    if rs_score is None:
        return None, None
    rs_rating = approximate_rs_rating(float(rs_score))
    if rs_rating is None:
        return None, None
    return float(rs_score), float(rs_rating)


def _score_pattern(
    closes: np.ndarray,
    volumes: np.ndarray,
    pole_start: int,
    pole_end: int,
    breakout_index: int,
    rs_rating: float,
) -> tuple[float, dict[str, float]]:
    scores: dict[str, float] = {}

    pole_gain = (closes[pole_end] - closes[pole_start]) / closes[pole_start]
    pole_days = pole_end - pole_start
    if pole_gain >= 1.20:
        gain_sub = 10.0
    elif pole_gain >= 1.00:
        gain_sub = 7.0 + (pole_gain - 1.00) / 0.20 * 3.0
    elif pole_gain >= 0.90:
        gain_sub = 5.0 + (pole_gain - 0.90) / 0.10 * 2.0
    else:
        gain_sub = pole_gain / 0.90 * 5.0
    speed_sub = max(0.0, 10.0 - (pole_days - 20) * 0.25)
    pole_slice = closes[pole_start : pole_end + 1]
    up_days = sum(1 for index in range(1, len(pole_slice)) if pole_slice[index] > pole_slice[index - 1])
    up_day_pct = up_days / max(len(pole_slice) - 1, 1)
    upday_sub = min(10.0, up_day_pct * 10.0 / 0.7)
    scores["pole"] = gain_sub * 0.5 + speed_sub * 0.3 + upday_sub * 0.2

    flag_segment = closes[pole_end:breakout_index]
    flag_high_value = float(flag_segment.max())
    flag_low_value = float(flag_segment.min())
    flag_drawdown = (flag_high_value - flag_low_value) / flag_high_value
    flag_days = breakout_index - pole_end
    if flag_drawdown <= 0.15:
        drawdown_sub = 10.0
    elif flag_drawdown <= 0.20:
        drawdown_sub = 7.0 + (0.20 - flag_drawdown) / 0.05 * 3.0
    elif flag_drawdown <= 0.25:
        drawdown_sub = 5.0 + (0.25 - flag_drawdown) / 0.05 * 2.0
    else:
        drawdown_sub = 0.0
    if flag_days <= 15:
        duration_sub = 10.0
    elif flag_days <= 20:
        duration_sub = 7.0
    else:
        duration_sub = 5.0
    if len(flag_segment) > 2:
        flag_returns = np.diff(flag_segment) / flag_segment[:-1]
        flag_volatility = float(np.std(flag_returns))
        tightness_sub = max(0.0, 10.0 - flag_volatility * 200.0)
    else:
        tightness_sub = 5.0
    scores["flag"] = drawdown_sub * 0.4 + duration_sub * 0.3 + tightness_sub * 0.3

    pre_pole_volume = volumes[max(pole_start - 20, 0) : pole_start]
    pre_pole_avg = float(np.nanmean(pre_pole_volume)) if len(pre_pole_volume) else 1.0
    pole_volume_avg = float(np.nanmean(volumes[pole_start : pole_end + 1]))
    flag_volume_avg = float(np.nanmean(volumes[pole_end:breakout_index]))
    breakout_volume = float(volumes[breakout_index])
    pre_breakout_avg = float(np.nanmean(volumes[max(breakout_index - 20, 0) : breakout_index]))
    if pre_pole_avg > 0.0:
        pole_volume_ratio = pole_volume_avg / pre_pole_avg
        pole_volume_sub = min(10.0, max(0.0, (pole_volume_ratio - 1.0) * 10.0))
    else:
        pole_volume_sub = 5.0
    if pole_volume_avg > 0.0:
        dry_ratio = flag_volume_avg / pole_volume_avg
        dry_sub = max(0.0, 10.0 - dry_ratio * 10.0)
    else:
        dry_sub = 5.0
    if pre_breakout_avg > 0.0:
        breakout_volume_ratio = breakout_volume / pre_breakout_avg
        breakout_volume_sub = min(10.0, max(0.0, (breakout_volume_ratio - 1.0) / 2.0 * 10.0))
    else:
        breakout_volume_sub = 5.0
    scores["volume"] = pole_volume_sub * 0.3 + dry_sub * 0.4 + breakout_volume_sub * 0.3

    rs_sub = min(10.0, max(0.0, (rs_rating - 50.0) / 50.0 * 10.0))
    high_52 = float(np.nanmax(closes[max(breakout_index - 252, 0) : breakout_index + 1]))
    near_high_pct = closes[breakout_index] / high_52 if high_52 > 0.0 else 0.0
    high_sub = min(10.0, near_high_pct * 10.0)
    ma50 = float(np.nanmean(closes[max(breakout_index - 50, 0) : breakout_index]))
    ma20 = float(np.nanmean(closes[max(breakout_index - 20, 0) : breakout_index]))
    ma_sub = 0.0
    if closes[breakout_index] > ma50:
        ma_sub += 5.0
    if closes[breakout_index] > ma20:
        ma_sub += 5.0
    scores["technical"] = rs_sub * 0.4 + high_sub * 0.3 + ma_sub * 0.3

    pivot = float(closes[pole_end:breakout_index].max()) + float(LEIF_HTF_CONFIG["breakout_min_buffer"])
    today_close = float(closes[breakout_index])
    excess_pct = (today_close - pivot) / pivot if pivot > 0.0 else 0.0
    if excess_pct < 0.0:
        proximity_sub = 0.0
    elif excess_pct <= 0.02:
        proximity_sub = 10.0
    elif excess_pct <= 0.05:
        proximity_sub = 7.0
    else:
        proximity_sub = max(0.0, 10.0 - excess_pct * 100.0)
    reward_target = today_close * (1.0 + pole_gain * 0.5)
    risk_amount = today_close - flag_low_value
    reward_amount = reward_target - today_close
    reward_risk_ratio = reward_amount / risk_amount if risk_amount > 0.0 else 0.0
    reward_risk_sub = min(10.0, reward_risk_ratio * 10.0 / 3.0)
    scores["breakout"] = proximity_sub * 0.6 + reward_risk_sub * 0.4

    if pole_start > 0:
        gap_pct = (closes[pole_start] - closes[pole_start - 1]) / closes[pole_start - 1]
        lookback_volume = volumes[max(pole_start - 20, 0) : pole_start]
        gap_volume_ratio = volumes[pole_start] / max(float(np.nanmean(lookback_volume)), 1.0)
        if gap_pct >= 0.10 and gap_volume_ratio >= 2.0:
            catalyst_sub = 10.0
        elif gap_pct >= 0.05:
            catalyst_sub = 7.0
        else:
            catalyst_sub = 4.0
    else:
        catalyst_sub = 4.0
    scores["catalyst"] = catalyst_sub

    weights = {
        "pole": 0.25,
        "flag": 0.25,
        "volume": 0.20,
        "technical": 0.15,
        "breakout": 0.10,
        "catalyst": 0.05,
    }
    composite = sum(scores[key] * weights[key] for key in weights)
    return round(composite, 2), {key: round(value, 1) for key, value in scores.items()}


def find_leif_high_tight_flag_hit(
    stock_frame: pd.DataFrame,
    benchmark_frame: pd.DataFrame,
    *,
    ticker: UniverseTicker,
    benchmark_ticker: str,
) -> LeifHighTightFlagHit | None:
    bars = _normalize_price_frame(stock_frame, include_volume=True)
    benchmark = _normalize_price_frame(benchmark_frame, include_volume=False)
    if bars.empty or benchmark.empty or len(bars) < LEIF_HTF_LOOKBACK_DAYS:
        return None

    latest_close = float(bars["Close"].iloc[-1])
    if latest_close < float(LEIF_HTF_CONFIG["min_price"]):
        return None

    rs_score, rs_rating = _compute_rs_snapshot(bars, benchmark)
    if rs_score is None or rs_rating is None or rs_rating < float(LEIF_HTF_CONFIG["rs_min_rating"]):
        return None

    closes = bars["Close"].to_numpy(dtype=float)
    volumes = bars["Volume"].to_numpy(dtype=float)
    breakout_index = len(closes) - 1
    ma50_at_breakout = float(np.nanmean(closes[max(breakout_index - LEIF_HTF_MA_PERIOD, 0) : breakout_index]))

    for pole_end in range(
        breakout_index - int(LEIF_HTF_CONFIG["flag_min_days"]),
        max(breakout_index - int(LEIF_HTF_CONFIG["flag_max_days"]) - 1, 0),
        -1,
    ):
        flag_days = breakout_index - pole_end
        if flag_days < int(LEIF_HTF_CONFIG["flag_min_days"]) or flag_days > int(LEIF_HTF_CONFIG["flag_max_days"]):
            continue

        flag_segment = closes[pole_end:breakout_index]
        if len(flag_segment) < 2:
            continue
        flag_high_value = float(flag_segment.max())
        flag_low_value = float(flag_segment.min())
        if flag_high_value <= 0.0:
            continue
        flag_drawdown = (flag_high_value - flag_low_value) / flag_high_value
        if flag_drawdown < float(LEIF_HTF_CONFIG["flag_min_drawdown"]) or flag_drawdown > float(LEIF_HTF_CONFIG["flag_max_drawdown"]):
            continue
        if bool(LEIF_HTF_CONFIG["flag_must_be_above_50ma"]) and flag_low_value < ma50_at_breakout:
            continue

        pivot_price = flag_high_value + float(LEIF_HTF_CONFIG["breakout_min_buffer"])
        if latest_close < pivot_price:
            continue

        breakout_volume_window = volumes[max(breakout_index - 20, 0) : breakout_index]
        if len(breakout_volume_window) == 0:
            continue
        breakout_volume_avg = float(np.nanmean(breakout_volume_window))
        if breakout_volume_avg <= 0.0:
            continue
        breakout_volume_ratio = float(volumes[breakout_index] / breakout_volume_avg)
        if breakout_volume_ratio < float(LEIF_HTF_CONFIG["volume_ratio"]):
            continue

        for pole_start in range(
            pole_end - 1,
            max(pole_end - int(LEIF_HTF_CONFIG["pole_max_days"]) - 1, 0),
            -1,
        ):
            pole_low = float(closes[pole_start])
            if pole_low <= 0.0:
                continue
            pole_gain = (flag_high_value - pole_low) / pole_low
            if pole_gain < float(LEIF_HTF_CONFIG["pole_min_gain"]):
                continue

            pre_pole_volume = volumes[max(pole_start - 20, 0) : pole_start]
            pre_pole_volume_avg = float(np.nanmean(pre_pole_volume)) if len(pre_pole_volume) else 0.0
            pole_volume_avg = float(np.nanmean(volumes[pole_start : pole_end + 1]))
            if pre_pole_volume_avg > 0.0 and pole_volume_avg / pre_pole_volume_avg < float(LEIF_HTF_CONFIG["pole_min_vol_ratio"]):
                continue

            pole_closes = closes[pole_start : pole_end + 1]
            up_days = sum(1 for index in range(1, len(pole_closes)) if pole_closes[index] > pole_closes[index - 1])
            up_day_pct = up_days / max(len(pole_closes) - 1, 1)
            if up_day_pct < float(LEIF_HTF_CONFIG["pole_min_up_day_pct"]):
                continue

            flag_volume_avg = float(np.nanmean(volumes[pole_end:breakout_index]))
            if pole_volume_avg > 0.0 and flag_volume_avg / pole_volume_avg > float(LEIF_HTF_CONFIG["flag_vol_dry_ratio"]):
                continue

            composite_score, component_scores = _score_pattern(closes, volumes, pole_start, pole_end, breakout_index, rs_rating)
            if composite_score < float(LEIF_HTF_CONFIG["min_score"]):
                continue

            reasons = [
                f"Leif HTF score {composite_score:.2f} with pole {component_scores['pole']:.1f}, flag {component_scores['flag']:.1f}, volume {component_scores['volume']:.1f}",
                f"pole gained {pole_gain * 100.0:.1f}% in {pole_end - pole_start} bars with {up_day_pct * 100.0:.1f}% up days",
                f"flag pulled back {flag_drawdown * 100.0:.1f}% across {flag_days} bars and held above 50-day average {ma50_at_breakout:.2f}",
                f"breakout closed {latest_close:.2f} above pivot {pivot_price:.2f} on {breakout_volume_ratio:.2f}x 20-day volume",
                f"RS rating {rs_rating:.1f} with weighted score {rs_score:.2f}",
            ]
            return LeifHighTightFlagHit(
                ticker=ticker.symbol,
                sector=ticker.sector,
                industry=ticker.industry,
                exchange=ticker.exchange,
                signal_date=bars.index[-1].date().isoformat(),
                benchmark_ticker=benchmark_ticker,
                current_price=latest_close,
                pivot_price=round(pivot_price, 4),
                breakout_volume_ratio=round(breakout_volume_ratio, 2),
                rs_rating=round(rs_rating, 1),
                rs_score=round(rs_score, 2),
                pole_gain_pct=round(pole_gain * 100.0, 1),
                pole_days=pole_end - pole_start,
                flag_days=flag_days,
                flag_drawdown_pct=round(flag_drawdown * 100.0, 1),
                flag_low=round(flag_low_value, 4),
                flag_high=round(flag_high_value, 4),
                up_day_pct=round(up_day_pct * 100.0, 1),
                score=composite_score,
                score_pole=component_scores["pole"],
                score_flag=component_scores["flag"],
                score_volume=component_scores["volume"],
                score_tech=component_scores["technical"],
                score_breakout=component_scores["breakout"],
                score_catalyst=component_scores["catalyst"],
                reasons=reasons,
            )

    return None


def run_leif_high_tight_flag_screen(
    config: AppConfig,
    tickers: list[UniverseTicker],
    *,
    as_of_date: dt.date | None = None,
) -> LeifHighTightFlagScreenResult:
    cookstock = load_configured_cookstock(config)
    hits: list[LeifHighTightFlagHit] = []
    failures: list[dict[str, str]] = []
    total_tickers = len(tickers)
    run_date = as_of_date or dt.date.today()

    print(f"starting Leif high tight flag screen: total={total_tickers}")

    with freeze_cookstock_today(cookstock, as_of_date):
        position = 0
        for ticker_batch in iter_prefetched_cookstock_batches(
            config,
            tickers,
            as_of_date=as_of_date,
            history_lookback_days=LEIF_HTF_LOOKBACK_DAYS,
            benchmark_ticker=config.benchmark_ticker,
        ):
            for ticker in ticker_batch:
                position += 1
                print(f"[{position}/{total_tickers}] screening {ticker.symbol} | passed={len(hits)}")
                try:
                    financials = cookstock.cookFinancials(
                        ticker.symbol,
                        benchmarkTicker=config.benchmark_ticker,
                        historyLookbackDays=LEIF_HTF_LOOKBACK_DAYS,
                    )
                    stock_frame = _build_price_frame_from_rows(financials._get_clean_price_data())
                    benchmark_frame = _build_price_frame_from_rows(
                        financials._get_benchmark_price_data(config.benchmark_ticker),
                        include_volume=False,
                    )
                    hit = find_leif_high_tight_flag_hit(
                        stock_frame,
                        benchmark_frame,
                        ticker=ticker,
                        benchmark_ticker=config.benchmark_ticker,
                    )
                    if hit is None:
                        print(f"[{position}/{total_tickers}] {ticker.symbol} filtered: no Leif HTF breakout | passed={len(hits)}")
                        continue
                    hits.append(hit)
                    print(
                        f"[{position}/{total_tickers}] {ticker.symbol} passed Leif HTF "
                        f"score={hit.score:.2f} rs={hit.rs_rating:.1f} "
                        f"pole={hit.pole_gain_pct:.1f}% flag={hit.flag_drawdown_pct:.1f}% | passed={len(hits)}"
                    )
                except Exception as exc:
                    failures.append({"ticker": ticker.symbol, "error": str(exc)})
                    print(f"[{position}/{total_tickers}] {ticker.symbol} failed: {exc}")

    hits.sort(key=lambda hit: (-hit.score, -hit.score_breakout, -hit.breakout_volume_ratio, hit.ticker))

    return LeifHighTightFlagScreenResult(
        run_date=run_date.isoformat(),
        benchmark_ticker=config.benchmark_ticker,
        total_tickers=total_tickers,
        passed_tickers=len(hits),
        failed_tickers=failures,
        hits=hits,
    )
