from __future__ import annotations

from dataclasses import asdict, dataclass
import datetime as dt

import numpy as np
import pandas as pd

from .config import AppConfig
from .cookstock_bridge import freeze_cookstock_today, iter_prefetched_cookstock_batches, load_configured_cookstock
from .universe import UniverseTicker


WYCKOFF_LOOKBACK = 50
WYCKOFF_VOLUME_CLIMAX_MULTIPLIER = 1.8
WYCKOFF_RSI_LENGTH = 14
WYCKOFF_HISTORY_DAYS = 320


@dataclass(frozen=True)
class WyckoffSignalHit:
    ticker: str
    sector: str | None
    industry: str | None
    exchange: str | None
    signal_date: str
    signal_type: str
    phase: str
    sub_phase: str
    current_price: float
    accum_score: int
    dist_score: int
    price_position: float
    volume_state: str
    event_flags: list[str]
    reasons: list[str]

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True)
class WyckoffSignalScreenResult:
    run_date: str
    signal_type: str
    total_tickers: int
    passed_tickers: int
    failed_tickers: list[dict[str, str]]
    hits: list[WyckoffSignalHit]

    def to_dict(self) -> dict[str, object]:
        return {
            "run_date": self.run_date,
            "signal_type": self.signal_type,
            "total_tickers": self.total_tickers,
            "passed_tickers": self.passed_tickers,
            "failed_tickers": self.failed_tickers,
            "hits": [item.to_dict() for item in self.hits],
        }


def _normalize_bars_frame(frame: pd.DataFrame) -> pd.DataFrame:
    required = ["Open", "High", "Low", "Close", "Volume"]
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


def _build_price_frame(financials) -> pd.DataFrame:
    rows = financials._get_clean_price_data()
    if not rows:
        return pd.DataFrame()
    frame = pd.DataFrame(
        {
            "Date": pd.to_datetime([row.get("formatted_date") for row in rows]),
            "Open": [row.get("open") for row in rows],
            "High": [row.get("high") for row in rows],
            "Low": [row.get("low") for row in rows],
            "Close": [row.get("close") for row in rows],
            "Volume": [row.get("volume") for row in rows],
        }
    )
    return frame.dropna(subset=["Date", "Open", "High", "Low", "Close", "Volume"]).set_index("Date").sort_index()


def _rsi(close: pd.Series, length: int) -> pd.Series:
    delta = close.diff()
    gain = delta.clip(lower=0.0)
    loss = -delta.clip(upper=0.0)
    avg_gain = gain.ewm(alpha=1 / length, adjust=False, min_periods=length).mean()
    avg_loss = loss.ewm(alpha=1 / length, adjust=False, min_periods=length).mean()
    rs = avg_gain / avg_loss.replace(0, np.nan)
    return 100 - (100 / (1 + rs))


def compute_wyckoff_frame(frame: pd.DataFrame) -> pd.DataFrame:
    bars = _normalize_bars_frame(frame)
    minimum_bars = max(WYCKOFF_LOOKBACK + 5, 200)
    if bars.empty or len(bars) < minimum_bars:
        return pd.DataFrame()

    bars = bars.copy()
    bars["avgVol"] = bars["Volume"].rolling(WYCKOFF_LOOKBACK).mean()
    bars["highVol"] = bars["Volume"] > bars["avgVol"] * WYCKOFF_VOLUME_CLIMAX_MULTIPLIER
    bars["lowVol"] = bars["Volume"] < bars["avgVol"] * 0.55
    bars["ema20"] = bars["Close"].ewm(span=20, adjust=False).mean()
    bars["ema50"] = bars["Close"].ewm(span=50, adjust=False).mean()
    bars["ema200"] = bars["Close"].ewm(span=200, adjust=False).mean()
    bars["rsi"] = _rsi(bars["Close"], WYCKOFF_RSI_LENGTH)
    bars["macdLine"] = bars["Close"].ewm(span=12, adjust=False).mean() - bars["Close"].ewm(span=26, adjust=False).mean()
    bars["sigLine"] = bars["macdLine"].ewm(span=9, adjust=False).mean()
    bars["macdHist"] = bars["macdLine"] - bars["sigLine"]

    previous_close = bars["Close"].shift(1)
    true_range = pd.concat(
        [
            bars["High"] - bars["Low"],
            (bars["High"] - previous_close).abs(),
            (bars["Low"] - previous_close).abs(),
        ],
        axis=1,
    ).max(axis=1)
    bars["atr14"] = true_range.rolling(14).mean()
    bars["ltTrend"] = bars["ema50"] > bars["ema200"]
    bars["stTrend"] = bars["ema20"] > bars["ema50"]
    bars["hiLB"] = bars["High"].rolling(WYCKOFF_LOOKBACK).max()
    bars["loLB"] = bars["Low"].rolling(WYCKOFF_LOOKBACK).min()
    bars["pricePos"] = (bars["Close"] - bars["loLB"]) / np.maximum((bars["hiLB"] - bars["loLB"]).to_numpy(dtype=float), (bars["atr14"] * 0.01).to_numpy(dtype=float))
    bars["rvol"] = bars["Close"].rolling(20).std() / bars["Close"].rolling(20).mean()
    bars["avgRvol"] = bars["rvol"].rolling(WYCKOFF_LOOKBACK).mean()
    bars["inRange"] = bars["rvol"] < bars["avgRvol"] * 0.85

    price_range = (bars["High"] - bars["Low"]).replace(0.0, np.nan)
    bars["isSC"] = (
        (bars["Close"] < bars["Open"])
        & bars["highVol"]
        & (((bars["High"] - bars["Close"]) / price_range) > 0.6)
        & (bars["Close"] <= bars["loLB"] * 1.002)
    )
    bars["isBC"] = (
        (bars["Close"] > bars["Open"])
        & bars["highVol"]
        & (((bars["Close"] - bars["Low"]) / price_range) > 0.6)
        & (bars["Close"] >= bars["hiLB"] * 0.998)
    )

    spring_low = bars["Low"].rolling(20).min()
    supply_high = bars["High"].rolling(20).max()
    volume_sma_5 = bars["Volume"].rolling(5).mean()
    volume_sma_10 = bars["Volume"].rolling(10).mean()
    highest_close_10 = bars["Close"].rolling(10).max()
    lowest_close_10 = bars["Close"].rolling(10).min()
    bars["isSpring"] = (
        (bars["Close"] > bars["Open"])
        & (bars["Low"] < spring_low.shift(1))
        & (bars["Close"] > spring_low.shift(1))
        & (bars["Volume"] > volume_sma_5)
    )
    bars["isUTAD"] = (
        (bars["Close"] < bars["Open"])
        & (bars["High"] > supply_high.shift(1))
        & (bars["Close"] < supply_high.shift(1))
        & (bars["Volume"] > volume_sma_5)
    )
    bars["isSOS"] = (
        (bars["Close"] > bars["Open"])
        & (bars["Close"] > highest_close_10.shift(1))
        & (bars["Volume"] > volume_sma_10)
        & (bars["macdHist"] > 0)
    )
    bars["isSOW"] = (
        (bars["Close"] < bars["Open"])
        & (bars["Close"] < lowest_close_10.shift(1))
        & (bars["Volume"] > volume_sma_10)
        & (bars["macdHist"] < 0)
    )
    bars["isLPS"] = (
        (bars["Close"] > bars["Open"])
        & (~bars["highVol"])
        & bars["stTrend"]
        & (bars["pricePos"] > 0.4)
    )
    bars["isLPSY"] = (
        (bars["Close"] < bars["Open"])
        & (~bars["highVol"])
        & (~bars["stTrend"])
        & (bars["pricePos"] < 0.6)
    )

    accum_scores: list[int] = []
    dist_scores: list[int] = []
    accum_consecutive: list[int] = []
    dist_consecutive: list[int] = []
    accumulation_flags: list[bool] = []
    distribution_flags: list[bool] = []
    accum_phases: list[int] = []
    dist_phases: list[int] = []
    buy_raw_flags: list[bool] = []
    sell_raw_flags: list[bool] = []
    hold_raw_flags: list[bool] = []
    buy_signal_flags: list[bool] = []
    sell_signal_flags: list[bool] = []
    hold_signal_flags: list[bool] = []
    signal_states: list[int] = []

    accum_consec = 0
    dist_consec = 0
    accum_phase = 0
    dist_phase = 0
    last_sig = 0

    for index, row in bars.iterrows():
        accum_score = 0
        dist_score = 0
        if pd.notna(row["pricePos"]) and float(row["pricePos"]) < 0.35:
            accum_score += 2
        if pd.notna(row["pricePos"]) and float(row["pricePos"]) > 0.65:
            dist_score += 2
        if pd.notna(row["ltTrend"]) and not bool(row["ltTrend"]):
            accum_score += 1
        if pd.notna(row["ltTrend"]) and bool(row["ltTrend"]):
            dist_score += 1
        if pd.notna(row["inRange"]) and bool(row["inRange"]):
            accum_score += 2
            dist_score += 2
        if pd.notna(row["lowVol"]) and bool(row["lowVol"]) and pd.notna(row["inRange"]) and bool(row["inRange"]):
            accum_score += 1
            dist_score += 1
        if pd.notna(row["rsi"]) and float(row["rsi"]) < 50:
            accum_score += 1
        if pd.notna(row["rsi"]) and float(row["rsi"]) > 50:
            dist_score += 1
        macd_hist = row["macdHist"]
        macd_hist_3 = bars.loc[:index, "macdHist"].iloc[-4] if len(bars.loc[:index]) >= 4 else np.nan
        if pd.notna(macd_hist) and pd.notna(macd_hist_3) and float(macd_hist) > float(macd_hist_3):
            accum_score += 1
        if pd.notna(macd_hist) and pd.notna(macd_hist_3) and float(macd_hist) < float(macd_hist_3):
            dist_score += 1

        accum_raw = accum_score >= 5 and accum_score > dist_score
        dist_raw = dist_score >= 5 and dist_score > accum_score
        if accum_raw and not dist_raw:
            accum_consec += 1
            dist_consec = 0
        elif dist_raw and not accum_raw:
            dist_consec += 1
            accum_consec = 0
        else:
            accum_consec = 0
            dist_consec = 0

        in_accumulation = accum_consec >= 3
        in_distribution = dist_consec >= 3

        if not in_accumulation:
            accum_phase = 0
        if not in_distribution:
            dist_phase = 0

        if in_accumulation:
            if accum_phase == 0:
                accum_phase = 1
            if accum_phase == 1 and bool(row["inRange"]):
                accum_phase = 2
            if accum_phase == 2 and bool(row["isSpring"]):
                accum_phase = 3
            if accum_phase == 3 and bool(row["isSOS"]):
                accum_phase = 4
            if accum_phase == 4 and bool(row["stTrend"]) and not bool(row["inRange"]):
                accum_phase = 5

        if in_distribution:
            if dist_phase == 0:
                dist_phase = 1
            if dist_phase == 1 and bool(row["inRange"]):
                dist_phase = 2
            if dist_phase == 2 and bool(row["isUTAD"]):
                dist_phase = 3
            if dist_phase == 3 and bool(row["isSOW"]):
                dist_phase = 4
            if dist_phase == 4 and not bool(row["stTrend"]) and not bool(row["inRange"]):
                dist_phase = 5

        accum_c_d = in_accumulation and accum_phase in (3, 4)
        dist_c_d = in_distribution and dist_phase in (3, 4)
        is_spring_buy = bool(row["isSpring"]) and not in_distribution
        is_lps_buy = bool(row["isLPS"]) and in_accumulation
        is_utad_sell = bool(row["isUTAD"]) and not in_accumulation
        is_lpsy_sell = bool(row["isLPSY"]) and in_distribution

        buy_raw = not in_distribution and (accum_c_d or is_spring_buy or is_lps_buy)
        sell_raw = not in_accumulation and (dist_c_d or is_utad_sell or is_lpsy_sell)
        hold_raw = not buy_raw and not sell_raw and (bool(row["inRange"]) or (in_accumulation and accum_phase == 2) or (in_distribution and dist_phase == 2))
        cur_sig = 1 if buy_raw else 2 if sell_raw else 3 if hold_raw else 0
        buy_signal = buy_raw and last_sig != 1
        sell_signal = sell_raw and last_sig != 2
        hold_signal = hold_raw and last_sig != 3
        last_sig = cur_sig if cur_sig != 0 else 0

        accum_scores.append(accum_score)
        dist_scores.append(dist_score)
        accum_consecutive.append(accum_consec)
        dist_consecutive.append(dist_consec)
        accumulation_flags.append(in_accumulation)
        distribution_flags.append(in_distribution)
        accum_phases.append(accum_phase)
        dist_phases.append(dist_phase)
        buy_raw_flags.append(buy_raw)
        sell_raw_flags.append(sell_raw)
        hold_raw_flags.append(hold_raw)
        buy_signal_flags.append(buy_signal)
        sell_signal_flags.append(sell_signal)
        hold_signal_flags.append(hold_signal)
        signal_states.append(cur_sig)

    bars["accumScore"] = accum_scores
    bars["distScore"] = dist_scores
    bars["accumConsec"] = accum_consecutive
    bars["distConsec"] = dist_consecutive
    bars["inAccumulation"] = accumulation_flags
    bars["inDistribution"] = distribution_flags
    bars["accumPhase"] = accum_phases
    bars["distPhase"] = dist_phases
    bars["buyRaw"] = buy_raw_flags
    bars["sellRaw"] = sell_raw_flags
    bars["holdRaw"] = hold_raw_flags
    bars["buySignal"] = buy_signal_flags
    bars["sellSignal"] = sell_signal_flags
    bars["holdSignal"] = hold_signal_flags
    bars["signalState"] = signal_states
    return bars


def _phase_text(row: pd.Series) -> tuple[str, str]:
    if bool(row.get("inAccumulation")):
        phase = int(row.get("accumPhase") or 0)
        labels = {
            1: "Phase A - Stopping Decline",
            2: "Phase B - Building Cause",
            3: "Phase C - Spring / Test",
            4: "Phase D - Mark-Up Begins",
            5: "Phase E - Trending Higher",
        }
        return "ACCUMULATION", labels.get(phase, "Accumulation")
    if bool(row.get("inDistribution")):
        phase = int(row.get("distPhase") or 0)
        labels = {
            1: "Phase A - Stopping Advance",
            2: "Phase B - Building Cause",
            3: "Phase C - UTAD / Test",
            4: "Phase D - Mark-Down Begins",
            5: "Phase E - Trending Lower",
        }
        return "DISTRIBUTION", labels.get(phase, "Distribution")
    return "NEUTRAL", "Neutral"


def _volume_state(row: pd.Series) -> str:
    if bool(row.get("highVol")):
        return "HIGH_CLIMAX"
    if bool(row.get("lowVol")):
        return "LOW_DRY_UP"
    return "NORMAL"


def _event_flags(row: pd.Series) -> list[str]:
    flags: list[str] = []
    for key, label in (
        ("isSC", "SC"),
        ("isBC", "BC"),
        ("isSpring", "SPRING"),
        ("isUTAD", "UTAD"),
        ("isSOS", "SOS"),
        ("isSOW", "SOW"),
        ("isLPS", "LPS"),
        ("isLPSY", "LPSY"),
    ):
        if bool(row.get(key)):
            flags.append(label)
    return flags


def find_recent_wyckoff_signal_hit(
    frame: pd.DataFrame,
    *,
    ticker: UniverseTicker,
    signal_type: str,
) -> WyckoffSignalHit | None:
    bars = compute_wyckoff_frame(frame)
    if bars.empty:
        return None
    latest = bars.iloc[-1]
    if signal_type == "buy" and not bool(latest["buySignal"]):
        return None
    if signal_type == "sell" and not bool(latest["sellSignal"]):
        return None

    phase, sub_phase = _phase_text(latest)
    event_flags = _event_flags(latest)
    reasons = [
        f"Wyckoff {signal_type.upper()} signal fired",
        f"phase {phase} / {sub_phase}",
        f"volume state {_volume_state(latest)}",
        f"accum score {int(latest['accumScore'])}, dist score {int(latest['distScore'])}",
    ]
    if event_flags:
        reasons.append(f"events: {', '.join(event_flags)}")

    return WyckoffSignalHit(
        ticker=ticker.symbol,
        sector=ticker.sector,
        industry=ticker.industry,
        exchange=ticker.exchange,
        signal_date=bars.index[-1].date().isoformat(),
        signal_type=signal_type,
        phase=phase,
        sub_phase=sub_phase,
        current_price=float(latest["Close"]),
        accum_score=int(latest["accumScore"]),
        dist_score=int(latest["distScore"]),
        price_position=float(latest["pricePos"]) if pd.notna(latest["pricePos"]) else 0.0,
        volume_state=_volume_state(latest),
        event_flags=event_flags,
        reasons=reasons,
    )


def compute_wyckoff_markers(frame: pd.DataFrame, *, visible_dates: set[str]) -> list[dict[str, object]]:
    bars = compute_wyckoff_frame(frame)
    if bars.empty:
        return []
    markers: list[dict[str, object]] = []
    for index, row in bars.iterrows():
        time_value = pd.Timestamp(index).date().isoformat()
        if time_value not in visible_dates:
            continue
        if bool(row["isBC"]):
            markers.append({"time": time_value, "kind": "wyckoff_buying_climax", "label": "BC"})
        if bool(row["buySignal"]):
            markers.append({"time": time_value, "kind": "wyckoff_buy_signal", "label": "BUY"})
        if bool(row["sellSignal"]):
            markers.append({"time": time_value, "kind": "wyckoff_sell_signal", "label": "SELL"})
        if bool(row["holdSignal"]):
            markers.append({"time": time_value, "kind": "wyckoff_hold_signal", "label": "HOLD"})
    return markers


def run_wyckoff_signal_screen(
    config: AppConfig,
    tickers: list[UniverseTicker],
    *,
    signal_type: str,
    as_of_date: dt.date | None = None,
) -> WyckoffSignalScreenResult:
    cookstock = load_configured_cookstock(config)
    hits: list[WyckoffSignalHit] = []
    failures: list[dict[str, str]] = []
    total_tickers = len(tickers)
    run_date = as_of_date or dt.date.today()

    print(f"starting wyckoff {signal_type} signal screen: total={total_tickers}")

    with freeze_cookstock_today(cookstock, as_of_date):
        position = 0
        for ticker_batch in iter_prefetched_cookstock_batches(
            config,
            tickers,
            as_of_date=as_of_date,
            history_lookback_days=WYCKOFF_HISTORY_DAYS,
            benchmark_ticker=config.benchmark_ticker,
        ):
            for ticker in ticker_batch:
                position += 1
                print(f"[{position}/{total_tickers}] screening {ticker.symbol} | passed={len(hits)}")
                try:
                    financials = cookstock.cookFinancials(
                        ticker.symbol,
                        benchmarkTicker=config.benchmark_ticker,
                        historyLookbackDays=WYCKOFF_HISTORY_DAYS,
                    )
                    frame = _build_price_frame(financials)
                    hit = find_recent_wyckoff_signal_hit(frame, ticker=ticker, signal_type=signal_type)
                    if hit is None:
                        print(f"[{position}/{total_tickers}] {ticker.symbol} filtered: no wyckoff {signal_type} signal | passed={len(hits)}")
                        continue
                    hits.append(hit)
                    print(
                        f"[{position}/{total_tickers}] {ticker.symbol} passed wyckoff {signal_type}: "
                        f"{hit.phase} / {hit.sub_phase} | passed={len(hits)}"
                    )
                except Exception as exc:
                    failures.append({"ticker": ticker.symbol, "error": str(exc)})
                    print(f"[{position}/{total_tickers}] {ticker.symbol} failed: {exc}")

    hits.sort(key=lambda hit: (-hit.accum_score if signal_type == "buy" else -hit.dist_score, hit.ticker))
    return WyckoffSignalScreenResult(
        run_date=run_date.isoformat(),
        signal_type=signal_type,
        total_tickers=total_tickers,
        passed_tickers=len(hits),
        failed_tickers=failures,
        hits=hits,
    )
