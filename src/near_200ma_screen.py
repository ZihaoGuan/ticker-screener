from __future__ import annotations

from dataclasses import asdict, dataclass
import datetime as dt

from .config import AppConfig
from .cookstock_bridge import freeze_cookstock_today, iter_prefetched_cookstock_batches
from .cookstock_bridge import load_configured_cookstock
from .market_data_access import load_many_ticker_windows, resolve_database_url
from .trendline_snapshots import load_latest_trendline_snapshot_map
from .universe import UniverseTicker


MA_SHORT = 20
MA_MEDIUM = 50
MA_LONG = 200
PRICE_HISTORY_DAYS = 320
MAX_DISTANCE_TO_MA200_PCT = 4.0
RANGE_LOOKBACK_DAYS = 10


@dataclass(frozen=True)
class Near200MaHit:
    ticker: str
    sector: str | None
    industry: str | None
    exchange: str | None
    benchmark_ticker: str
    case_group: str
    current_price: float
    ma20: float
    ma50: float
    ma200: float
    distance_to_ma20_pct: float
    distance_to_ma50_pct: float
    distance_to_ma200_pct: float
    avg_volume_20: float
    avg_dollar_volume_20: float
    recent_low: float
    recent_high: float
    reasons: list[str]

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True)
class Near200MaScreenResult:
    run_date: str
    benchmark_ticker: str
    total_tickers: int
    passed_tickers: int
    failed_tickers: list[dict[str, str]]
    hits: list[Near200MaHit]

    def to_dict(self) -> dict[str, object]:
        return {
            "run_date": self.run_date,
            "benchmark_ticker": self.benchmark_ticker,
            "total_tickers": self.total_tickers,
            "passed_tickers": self.passed_tickers,
            "failed_tickers": self.failed_tickers,
            "hits": [item.to_dict() for item in self.hits],
        }


def _sma(values: list[float], length: int) -> float | None:
    if len(values) < length:
        return None
    return sum(values[-length:]) / float(length)


def _pct_from_level(current_price: float, level: float) -> float:
    if level <= 0:
        return 0.0
    return ((current_price / level) - 1.0) * 100.0


def _to_hit(
    ticker: UniverseTicker,
    benchmark_ticker: str,
    *,
    case_group: str,
    current_price: float,
    ma20: float,
    ma50: float,
    ma200: float,
    avg_volume_20: float,
    avg_dollar_volume_20: float,
    recent_low: float,
    recent_high: float,
) -> Near200MaHit:
    distance_to_ma20_pct = _pct_from_level(current_price, ma20)
    distance_to_ma50_pct = _pct_from_level(current_price, ma50)
    distance_to_ma200_pct = _pct_from_level(current_price, ma200)
    if case_group == "bull":
        reasons = [
            f"below 200D MA by {abs(distance_to_ma200_pct):.1f}%",
            f"holding above 20D MA {ma20:.2f}",
            f"holding above 50D MA {ma50:.2f}",
            "short and medium moving averages are acting as support into 200D resistance",
        ]
    else:
        reasons = [
            f"above 200D MA by {distance_to_ma200_pct:.1f}%",
            f"trading below 20D MA {ma20:.2f}",
            f"trading below 50D MA {ma50:.2f}",
            "short and medium moving averages are pressing price down toward 200D support",
        ]
    return Near200MaHit(
        ticker=ticker.symbol,
        sector=ticker.sector,
        industry=ticker.industry,
        exchange=ticker.exchange,
        benchmark_ticker=benchmark_ticker,
        case_group=case_group,
        current_price=current_price,
        ma20=ma20,
        ma50=ma50,
        ma200=ma200,
        distance_to_ma20_pct=distance_to_ma20_pct,
        distance_to_ma50_pct=distance_to_ma50_pct,
        distance_to_ma200_pct=distance_to_ma200_pct,
        avg_volume_20=avg_volume_20,
        avg_dollar_volume_20=avg_dollar_volume_20,
        recent_low=recent_low,
        recent_high=recent_high,
        reasons=reasons,
    )


def run_near_200ma_screen(
    config: AppConfig,
    tickers: list[UniverseTicker],
    *,
    as_of_date: dt.date | None = None,
) -> Near200MaScreenResult:
    run_date = as_of_date or dt.date.today()
    total_tickers = len(tickers)

    print(
        "starting near-200D-MA screen: "
        f"total={total_tickers}, "
        f"ma_stack=MA{MA_SHORT}/MA{MA_MEDIUM}/MA{MA_LONG}, "
        f"max_distance={MAX_DISTANCE_TO_MA200_PCT:.1f}%, "
        "market_cap_filter=post-screen > $1B"
    )

    database_url = resolve_database_url("")
    db_result = _run_near_200ma_screen_from_db(
        config,
        tickers,
        as_of_date=run_date,
        database_url=database_url,
    )
    if db_result is not None:
        return db_result

    cookstock = load_configured_cookstock(config)
    hits: list[Near200MaHit] = []
    failures: list[dict[str, str]] = []
    with freeze_cookstock_today(cookstock, as_of_date):
        position = 0
        for ticker_batch in iter_prefetched_cookstock_batches(
            config,
            tickers,
            as_of_date=as_of_date,
            history_lookback_days=PRICE_HISTORY_DAYS,
            benchmark_ticker=config.benchmark_ticker,
        ):
            for ticker in ticker_batch:
                position += 1
                print(f"[{position}/{total_tickers}] screening {ticker.symbol} | passed={len(hits)}")
                try:
                    financials = cookstock.cookFinancials(
                        ticker.symbol,
                        benchmarkTicker=config.benchmark_ticker,
                        historyLookbackDays=PRICE_HISTORY_DAYS,
                    )
                    price_rows = financials._get_clean_price_data()
                    closes = [float(item["close"]) for item in price_rows if item.get("close") is not None]
                    if len(closes) < MA_LONG:
                        print(f"[{position}/{total_tickers}] {ticker.symbol} filtered: insufficient price history | passed={len(hits)}")
                        continue

                    current_price = closes[-1]
                    ma20 = _sma(closes, MA_SHORT)
                    ma50 = _sma(closes, MA_MEDIUM)
                    ma200 = _sma(closes, MA_LONG)
                    if ma20 is None or ma50 is None or ma200 is None:
                        print(f"[{position}/{total_tickers}] {ticker.symbol} filtered: missing moving averages | passed={len(hits)}")
                        continue

                    distance_to_ma200_pct = _pct_from_level(current_price, ma200)
                    if abs(distance_to_ma200_pct) > MAX_DISTANCE_TO_MA200_PCT:
                        print(
                            f"[{position}/{total_tickers}] {ticker.symbol} filtered: "
                            f"{distance_to_ma200_pct:+.2f}% vs MA200 | passed={len(hits)}"
                        )
                        continue

                    recent_rows = price_rows[-RANGE_LOOKBACK_DAYS:]
                    recent_low = min(float(item["low"]) for item in recent_rows if item.get("low") is not None)
                    recent_high = max(float(item["high"]) for item in recent_rows if item.get("high") is not None)
                    avg_volume_20 = float(financials._get_average_volume(20))
                    avg_dollar_volume_20 = float(financials._get_average_dollar_volume(20))

                    case_group: str | None = None
                    if current_price < ma200 and current_price > ma20 > ma50:
                        case_group = "bull"
                    elif current_price > ma200 and current_price < ma20 < ma50:
                        case_group = "bear"

                    if case_group is None:
                        print(
                            f"[{position}/{total_tickers}] {ticker.symbol} filtered: "
                            f"no bull/bear 200D setup (price {current_price:.2f}, ma20 {ma20:.2f}, ma50 {ma50:.2f}, ma200 {ma200:.2f}) "
                            f"| passed={len(hits)}"
                        )
                        continue

                    hit = _to_hit(
                        ticker,
                        config.benchmark_ticker,
                        case_group=case_group,
                        current_price=current_price,
                        ma20=ma20,
                        ma50=ma50,
                        ma200=ma200,
                        avg_volume_20=avg_volume_20,
                        avg_dollar_volume_20=avg_dollar_volume_20,
                        recent_low=recent_low,
                        recent_high=recent_high,
                    )
                    hits.append(hit)
                    print(
                        f"[{position}/{total_tickers}] {ticker.symbol} passed: "
                        f"{case_group} case, {hit.distance_to_ma200_pct:+.2f}% vs MA200 | passed={len(hits)}"
                    )
                except Exception as exc:
                    failures.append({"ticker": ticker.symbol, "error": str(exc)})
                    print(f"[{position}/{total_tickers}] {ticker.symbol} error: {exc} | passed={len(hits)}")

    hits.sort(
        key=lambda item: (
            0 if item.case_group == "bull" else 1,
            abs(item.distance_to_ma200_pct),
            -item.avg_dollar_volume_20,
            item.ticker,
        )
    )

    return Near200MaScreenResult(
        run_date=run_date.isoformat(),
        benchmark_ticker=config.benchmark_ticker,
        total_tickers=total_tickers,
        passed_tickers=len(hits),
        failed_tickers=failures,
        hits=hits,
    )


def _run_near_200ma_screen_from_db(
    config: AppConfig,
    tickers: list[UniverseTicker],
    *,
    as_of_date: dt.date,
    database_url: str,
) -> Near200MaScreenResult | None:
    snapshot_map = load_latest_trendline_snapshot_map(
        [item.symbol for item in tickers],
        as_of_date=as_of_date,
        database_url=database_url,
    )
    if not snapshot_map:
        return None

    frame_map = load_many_ticker_windows(
        [item.symbol for item in tickers],
        as_of_date,
        max(MA_SHORT, RANGE_LOOKBACK_DAYS),
        database_url=database_url,
    )
    hits: list[Near200MaHit] = []
    failures: list[dict[str, str]] = []
    total_tickers = len(tickers)

    for position, ticker in enumerate(tickers, start=1):
        print(f"[{position}/{total_tickers}] screening {ticker.symbol} from trendline snapshots | passed={len(hits)}")
        try:
            snapshot = snapshot_map.get(ticker.symbol.upper())
            frame = frame_map.get(ticker.symbol.upper())
            if snapshot is None or frame is None or getattr(frame, "empty", False):
                print(f"[{position}/{total_tickers}] {ticker.symbol} filtered: missing trendline snapshot or bars | passed={len(hits)}")
                continue

            closes = [float(value) for value in frame["Close"].tail(MA_SHORT).tolist()]
            if len(closes) < MA_SHORT:
                print(f"[{position}/{total_tickers}] {ticker.symbol} filtered: insufficient DB window | passed={len(hits)}")
                continue

            current_price = float(snapshot["close"])
            ma20 = _sma(closes, MA_SHORT)
            ma50 = float(snapshot["daily_sma50"]) if snapshot.get("daily_sma50") is not None else None
            ma200 = float(snapshot["daily_sma200"]) if snapshot.get("daily_sma200") is not None else None
            if ma20 is None or ma50 is None or ma200 is None:
                print(f"[{position}/{total_tickers}] {ticker.symbol} filtered: missing trendline levels | passed={len(hits)}")
                continue

            distance_to_ma200_pct = _pct_from_level(current_price, ma200)
            if abs(distance_to_ma200_pct) > MAX_DISTANCE_TO_MA200_PCT:
                print(
                    f"[{position}/{total_tickers}] {ticker.symbol} filtered: "
                    f"{distance_to_ma200_pct:+.2f}% vs MA200 | passed={len(hits)}"
                )
                continue

            recent_frame = frame.tail(RANGE_LOOKBACK_DAYS)
            recent_low = float(recent_frame["Low"].min())
            recent_high = float(recent_frame["High"].max())
            volume_window = frame.tail(MA_SHORT)
            avg_volume_20 = float(volume_window["Volume"].mean())
            avg_dollar_volume_20 = float((volume_window["Close"] * volume_window["Volume"]).mean())

            case_group: str | None = None
            if current_price < ma200 and current_price > ma20 > ma50:
                case_group = "bull"
            elif current_price > ma200 and current_price < ma20 < ma50:
                case_group = "bear"

            if case_group is None:
                print(
                    f"[{position}/{total_tickers}] {ticker.symbol} filtered: "
                    f"no bull/bear 200D setup (price {current_price:.2f}, ma20 {ma20:.2f}, ma50 {ma50:.2f}, ma200 {ma200:.2f}) "
                    f"| passed={len(hits)}"
                )
                continue

            hit = _to_hit(
                ticker,
                config.benchmark_ticker,
                case_group=case_group,
                current_price=current_price,
                ma20=ma20,
                ma50=ma50,
                ma200=ma200,
                avg_volume_20=avg_volume_20,
                avg_dollar_volume_20=avg_dollar_volume_20,
                recent_low=recent_low,
                recent_high=recent_high,
            )
            hits.append(hit)
            print(
                f"[{position}/{total_tickers}] {ticker.symbol} passed: "
                f"{hit.case_group} case, {hit.distance_to_ma200_pct:+.2f}% vs MA200 | passed={len(hits)}"
            )
        except Exception as exc:
            failures.append({"ticker": ticker.symbol, "error": str(exc)})
            print(f"[{position}/{total_tickers}] {ticker.symbol} error: {exc} | passed={len(hits)}")

    hits.sort(
        key=lambda item: (
            0 if item.case_group == "bull" else 1,
            abs(item.distance_to_ma200_pct),
            -item.avg_dollar_volume_20,
            item.ticker,
        )
    )
    return Near200MaScreenResult(
        run_date=as_of_date.isoformat(),
        benchmark_ticker=config.benchmark_ticker,
        total_tickers=total_tickers,
        passed_tickers=len(hits),
        failed_tickers=failures,
        hits=hits,
    )
