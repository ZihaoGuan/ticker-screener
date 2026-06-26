#!/usr/bin/env python3
from __future__ import annotations

import argparse
import datetime as dt
import json
from pathlib import Path
import sys
from itertools import combinations
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import numpy as np
import pandas as pd

from src.market_data_access import load_many_ticker_windows, resolve_database_url

try:
    from finvizfinance.screener.overview import Overview
except ImportError:  # pragma: no cover - dependency checked at runtime
    Overview = None

try:
    from statsmodels.tsa.stattools import adfuller
except ImportError:  # pragma: no cover - dependency checked at runtime
    adfuller = None

try:
    import yfinance as yf
except ImportError:  # pragma: no cover - dependency checked at runtime
    yf = None


REPO_VENV_PYTHON = PROJECT_ROOT.parent / "venv" / "bin" / "python"
DEFAULT_SECTORS = (
    "Technology",
    "Financial",
    "Healthcare",
    "Industrials",
    "Consumer Cyclical",
    "Communication Services",
    "Energy",
    "Basic Materials",
    "Consumer Defensive",
    "Utilities",
    "Real Estate",
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run pair-trade screener using FINVIZ universe filters plus local daily_bars history.",
    )
    parser.add_argument("--date-label", help="Optional artifact folder label. Defaults to today's date.")
    parser.add_argument("--as-of-date", help="Optional analysis date. Defaults to today.")
    parser.add_argument("--limit", type=int, default=25, help="Maximum pairs to keep in final report.")
    parser.add_argument("--lookback-days", type=int, default=504, help="Trading days of history to load from daily_bars.")
    parser.add_argument("--min-history-days", type=int, default=252, help="Minimum overlapping history required for a pair.")
    parser.add_argument("--min-correlation", type=float, default=0.8, help="Minimum return correlation required before cointegration test.")
    parser.add_argument("--max-half-life", type=float, default=90.0, help="Maximum half-life in trading days.")
    parser.add_argument("--entry-zscore", type=float, default=2.0, help="Absolute z-score threshold for actionable signal.")
    parser.add_argument("--tickers-per-group", type=int, default=16, help="Maximum FINVIZ tickers fetched per sector or industry.")
    parser.add_argument("--include-sectors", nargs="+", help="Restrict scan to these sectors.")
    parser.add_argument("--include-industries", nargs="+", help="Restrict scan to these industries.")
    return parser.parse_args()


def _preferred_python() -> Path:
    if REPO_VENV_PYTHON.exists():
        return REPO_VENV_PYTHON
    return Path(sys.executable)


def _maybe_reexec() -> int | None:
    preferred = _preferred_python().resolve()
    current = Path(sys.executable).resolve()
    if preferred == current:
        return None
    import subprocess

    completed = subprocess.run(
        [str(preferred), str(Path(__file__).resolve()), *sys.argv[1:]],
        cwd=str(PROJECT_ROOT),
    )
    return int(completed.returncode)


def _parse_date(value: str | None) -> dt.date:
    if value:
        return dt.date.fromisoformat(value)
    return dt.date.today()


def _normalize_names(values: list[str] | None) -> list[str]:
    result: list[str] = []
    seen: set[str] = set()
    for item in values or []:
        text = str(item or "").strip()
        if not text:
            continue
        key = text.casefold()
        if key in seen:
            continue
        seen.add(key)
        result.append(text)
    return result


def _group_mode(args: argparse.Namespace) -> tuple[str, list[str]]:
    industries = _normalize_names(args.include_industries)
    if industries:
        return "industry", industries
    sectors = _normalize_names(args.include_sectors)
    if sectors:
        return "sector", sectors
    return "sector", list(DEFAULT_SECTORS)


def _fetch_group_universe(*, group_mode: str, group_name: str, tickers_per_group: int) -> list[dict[str, Any]]:
    if Overview is None:
        raise RuntimeError("finvizfinance is not installed. Install it before running pair trade screener.")

    filters_dict = {
        "Average Volume": "Over 1M",
        "Market Cap.": "+Mid (over $2bln)",
        "Price": "Over $5",
    }
    if group_mode == "industry":
        filters_dict["Industry"] = group_name
    else:
        filters_dict["Sector"] = group_name
        filters_dict["Industry"] = "Stocks only (ex-Funds)"

    overview = Overview()
    overview.set_filter(filters_dict=filters_dict)
    frame = overview.screener_view(order="Market Cap.", limit=tickers_per_group, verbose=0, ascend=False)
    if frame is None or frame.empty:
        return []

    rows: list[dict[str, Any]] = []
    for _, row in frame.iterrows():
        symbol = str(row.get("Ticker", "")).strip().upper()
        if not symbol:
            continue
        rows.append(
            {
                "symbol": symbol,
                "company_name": str(row.get("Company", "")).strip(),
                "sector": str(row.get("Sector", "")).strip(),
                "industry": str(row.get("Industry", "")).strip(),
                "price": _coerce_float(row.get("Price")),
                "market_cap": _coerce_market_cap(row.get("Market Cap")),
                "avg_volume": _coerce_int(row.get("Volume")),
                "group_mode": group_mode,
                "group_name": group_name,
            }
        )
    return rows


def _coerce_float(value: Any) -> float | None:
    if value in (None, "", "-"):
        return None
    try:
        return float(str(value).replace("%", "").replace(",", ""))
    except (TypeError, ValueError):
        return None


def _coerce_int(value: Any) -> int | None:
    if value in (None, "", "-"):
        return None
    try:
        return int(float(str(value).replace(",", "")))
    except (TypeError, ValueError):
        return None


def _coerce_market_cap(value: Any) -> int | None:
    text = str(value or "").strip().upper()
    if not text:
        return None
    multiplier = 1
    if text.endswith("B"):
        multiplier = 1_000_000_000
        text = text[:-1]
    elif text.endswith("M"):
        multiplier = 1_000_000
        text = text[:-1]
    elif text.endswith("K"):
        multiplier = 1_000
        text = text[:-1]
    try:
        return int(float(text) * multiplier)
    except (TypeError, ValueError):
        return None


def _extract_adj_close(frame: Any, *, lookback_days: int) -> pd.Series | None:
    if frame is None or getattr(frame, "empty", True):
        return None
    if "Adj Close" in frame.columns:
        series = pd.to_numeric(frame["Adj Close"], errors="coerce")
    elif "Close" in frame.columns:
        series = pd.to_numeric(frame["Close"], errors="coerce")
    else:
        return None
    series = series.dropna().tail(lookback_days)
    return series if len(series) > 0 else None


def _fetch_yfinance_history(
    tickers: list[str],
    *,
    as_of_date: dt.date,
    lookback_days: int,
) -> dict[str, Any]:
    if yf is None:
        return {}
    start_date = as_of_date - dt.timedelta(days=max(lookback_days * 2, 365))
    end_date = as_of_date + dt.timedelta(days=1)
    frame_map: dict[str, Any] = {}
    for ticker in tickers:
        symbol = str(ticker or "").strip().upper()
        if not symbol:
            continue
        try:
            history = yf.download(
                symbol,
                start=start_date.isoformat(),
                end=end_date.isoformat(),
                auto_adjust=False,
                progress=False,
                threads=False,
            )
        except Exception:
            continue
        if history is None or getattr(history, "empty", True):
            continue
        frame_map[symbol] = history.tail(lookback_days)
    return frame_map


def _load_price_history(
    tickers: list[str],
    *,
    as_of_date: dt.date,
    lookback_days: int,
    database_url: str,
) -> tuple[dict[str, Any], str]:
    frame_map = load_many_ticker_windows(
        tickers,
        as_of_date,
        lookback_days,
        database_url=database_url or None,
    )
    if frame_map:
        return frame_map, "daily_bars"
    fallback_map = _fetch_yfinance_history(
        tickers,
        as_of_date=as_of_date,
        lookback_days=lookback_days,
    )
    if fallback_map:
        return fallback_map, "yfinance"
    return {}, "unavailable"


def _pair_returns_correlation(series_a: pd.Series, series_b: pd.Series) -> float | None:
    returns_a = series_a.pct_change().dropna()
    returns_b = series_b.pct_change().dropna()
    common_dates = returns_a.index.intersection(returns_b.index)
    if len(common_dates) < 60:
        return None
    value = returns_a.loc[common_dates].corr(returns_b.loc[common_dates])
    if value is None or np.isnan(value):
        return None
    return float(value)


def _hedge_ratio(series_a: pd.Series, series_b: pd.Series) -> tuple[float, pd.Series, pd.Series] | None:
    common_dates = series_a.index.intersection(series_b.index)
    if len(common_dates) < 120:
        return None
    aligned_a = series_a.loc[common_dates]
    aligned_b = series_b.loc[common_dates]
    matrix = np.vstack([aligned_b.values, np.ones(len(aligned_b))]).T
    beta, _intercept = np.linalg.lstsq(matrix, aligned_a.values, rcond=None)[0]
    return float(beta), aligned_a, aligned_b


def _cointegration_metrics(spread: pd.Series) -> tuple[float | None, float | None] | None:
    if adfuller is None:
        return None
    cleaned = spread.dropna()
    if len(cleaned) < 120:
        return None
    result = adfuller(cleaned, maxlag=1, regression="c", autolag=None)
    return float(result[0]), float(result[1])


def _half_life(spread: pd.Series) -> float | None:
    cleaned = spread.dropna()
    if len(cleaned) < 60:
        return None
    lagged = cleaned.shift(1).dropna()
    delta = cleaned.diff().dropna()
    common = lagged.index.intersection(delta.index)
    if len(common) < 60:
        return None
    slope, _intercept = np.polyfit(lagged.loc[common].values, delta.loc[common].values, 1)
    if slope >= 0:
        return None
    half_life = -np.log(2.0) / slope
    if not np.isfinite(half_life):
        return None
    return float(half_life)


def _current_zscore(spread: pd.Series, *, window: int = 90) -> float | None:
    cleaned = spread.dropna()
    if len(cleaned) < max(30, window):
        return None
    trailing = cleaned.tail(window)
    std = float(trailing.std())
    if std <= 0:
        return None
    value = (float(trailing.iloc[-1]) - float(trailing.mean())) / std
    return float(value)


def _signal_from_zscore(zscore: float | None, *, entry_zscore: float) -> str:
    if zscore is None:
        return "NONE"
    if zscore <= -abs(entry_zscore):
        return "LONG_A_SHORT_B"
    if zscore >= abs(entry_zscore):
        return "SHORT_A_LONG_B"
    if abs(zscore) >= max(1.5, abs(entry_zscore) * 0.75):
        return "WATCH"
    return "NONE"


def _opportunity_score(*, correlation: float, p_value: float, zscore: float | None, half_life: float | None) -> float:
    z_component = min(abs(zscore or 0.0), 3.5) / 3.5 * 35.0
    corr_component = min(max(correlation, 0.0), 1.0) * 35.0
    p_component = max(0.0, 1.0 - min(max(p_value, 0.0), 1.0)) * 20.0
    if half_life is None:
        half_life_component = 0.0
    else:
        half_life_component = max(0.0, 10.0 - min(abs(half_life - 20.0), 40.0) / 40.0 * 10.0)
    return round(z_component + corr_component + p_component + half_life_component, 2)


def _analyze_pairs(
    *,
    group_rows: dict[str, list[dict[str, Any]]],
    frame_map: dict[str, Any],
    min_correlation: float,
    min_history_days: int,
    max_half_life: float,
    entry_zscore: float,
) -> tuple[list[dict[str, Any]], dict[str, int]]:
    pairs: list[dict[str, Any]] = []
    pairs_considered = 0
    correlation_pass = 0
    cointegration_pass = 0

    for group_name, stocks in group_rows.items():
        symbols = [str(item["symbol"]) for item in stocks if str(item.get("symbol") or "").strip()]
        stock_by_symbol = {str(item["symbol"]): item for item in stocks}
        for symbol_a, symbol_b in combinations(symbols, 2):
            pairs_considered += 1
            series_a = _extract_adj_close(frame_map.get(symbol_a), lookback_days=10_000)
            series_b = _extract_adj_close(frame_map.get(symbol_b), lookback_days=10_000)
            if series_a is None or series_b is None:
                continue
            common_dates = series_a.index.intersection(series_b.index)
            if len(common_dates) < min_history_days:
                continue
            series_a = series_a.loc[common_dates]
            series_b = series_b.loc[common_dates]
            correlation = _pair_returns_correlation(series_a, series_b)
            if correlation is None or correlation < min_correlation:
                continue
            correlation_pass += 1
            hedge_result = _hedge_ratio(series_a, series_b)
            if hedge_result is None:
                continue
            beta, aligned_a, aligned_b = hedge_result
            spread = aligned_a - (beta * aligned_b)
            half_life = _half_life(spread)
            if half_life is None or half_life > max_half_life:
                continue
            coint = _cointegration_metrics(spread)
            adf_statistic: float | None = None
            p_value: float | None = None
            if coint is not None:
                adf_statistic, p_value = coint
                if p_value is None or p_value >= 0.05:
                    continue
            cointegration_pass += 1
            zscore = _current_zscore(spread)
            signal = _signal_from_zscore(zscore, entry_zscore=entry_zscore)
            stock_a = stock_by_symbol[symbol_a]
            stock_b = stock_by_symbol[symbol_b]
            pairs.append(
                {
                    "pair": f"{symbol_a}/{symbol_b}",
                    "stock_a": symbol_a,
                    "stock_b": symbol_b,
                    "company_a": stock_a.get("company_name"),
                    "company_b": stock_b.get("company_name"),
                    "sector": stock_a.get("sector") or stock_b.get("sector"),
                    "industry": stock_a.get("industry") if stock_a.get("industry") == stock_b.get("industry") else None,
                    "group_name": group_name,
                    "correlation": round(correlation, 4),
                    "beta": round(beta, 4),
                    "cointegration_pvalue": round(p_value, 6) if p_value is not None else None,
                    "adf_statistic": round(adf_statistic, 4) if adf_statistic is not None else None,
                    "half_life_days": round(float(half_life), 1),
                    "current_zscore": round(float(zscore), 4) if zscore is not None else None,
                    "signal": signal,
                    "actionable": signal in {"LONG_A_SHORT_B", "SHORT_A_LONG_B"},
                    "opportunity_score": _opportunity_score(
                        correlation=correlation,
                        p_value=p_value or 0.05,
                        zscore=zscore,
                        half_life=half_life,
                    ),
                    "latest_date": aligned_a.index[-1].date().isoformat(),
                    "price_a": round(float(aligned_a.iloc[-1]), 2),
                    "price_b": round(float(aligned_b.iloc[-1]), 2),
                    "market_cap_a": stock_a.get("market_cap"),
                    "market_cap_b": stock_b.get("market_cap"),
                    "avg_volume_a": stock_a.get("avg_volume"),
                    "avg_volume_b": stock_b.get("avg_volume"),
                }
            )

    pairs.sort(
        key=lambda item: (
            1 if item.get("actionable") else 0,
            float(item.get("opportunity_score") or 0.0),
            float(item.get("correlation") or 0.0),
        ),
        reverse=True,
    )
    return pairs, {
        "pairs_considered": pairs_considered,
        "correlation_pass": correlation_pass,
        "cointegration_pass": cointegration_pass,
    }


def _render_markdown(payload: dict[str, Any]) -> str:
    summary = payload.get("summary") if isinstance(payload.get("summary"), dict) else {}
    metadata = payload.get("metadata") if isinstance(payload.get("metadata"), dict) else {}
    pairs = payload.get("pairs") if isinstance(payload.get("pairs"), list) else []
    lines = [
        "# Pair Trade Screener Report",
        "",
        f"Generated: {payload.get('generated_at', '')}",
        f"As of: {metadata.get('as_of_date', '')}",
        f"Universe mode: {metadata.get('group_mode', '')}",
        f"Groups: {', '.join(metadata.get('included_groups') or []) or 'All default sectors'}",
        "",
        "## Summary",
        "",
        f"- Universe size: {summary.get('universe_size', 0)}",
        f"- Pairs analyzed: {summary.get('pairs_analyzed', 0)}",
        f"- Cointegrated pairs: {summary.get('cointegrated_pairs', 0)}",
        f"- Actionable pairs: {summary.get('actionable_pairs', 0)}",
        "",
        "## Top Pairs",
        "",
        "| Pair | Group | Corr | P-Value | Half-Life | Z-Score | Signal |",
        "|---|---|---:|---:|---:|---:|---|",
    ]
    for pair in pairs[:25]:
        lines.append(
            f"| {pair.get('pair', '--')} | {pair.get('group_name', '--')} | "
            f"{_fmt_num(pair.get('correlation'))} | {_fmt_num(pair.get('cointegration_pvalue'))} | "
            f"{_fmt_num(pair.get('half_life_days'))} | {_fmt_num(pair.get('current_zscore'))} | "
            f"{pair.get('signal', 'NONE')} |"
        )
    return "\n".join(lines) + "\n"


def _fmt_num(value: Any) -> str:
    if value in (None, ""):
        return "--"
    if isinstance(value, int):
        return str(value)
    try:
        return f"{float(value):.2f}"
    except (TypeError, ValueError):
        return str(value)


def main() -> int:
    reexec_code = _maybe_reexec()
    if reexec_code is not None:
        return reexec_code

    args = parse_args()
    date_label = args.date_label or dt.date.today().isoformat()
    as_of_date = _parse_date(args.as_of_date)
    database_url = resolve_database_url(None)

    group_mode, group_names = _group_mode(args)
    output_dir = PROJECT_ROOT / "artifacts" / "reports" / "pair_trade_screener" / date_label
    output_dir.mkdir(parents=True, exist_ok=True)

    print("Pair Trade Screener starting...")
    print(f"  Group mode: {group_mode}")
    print(f"  Groups: {', '.join(group_names)}")
    print(f"  As of date: {as_of_date.isoformat()}")
    print(f"  Lookback days: {args.lookback_days}")
    print(f"  Min correlation: {args.min_correlation}")

    group_rows: dict[str, list[dict[str, Any]]] = {}
    all_symbols: list[str] = []
    for group_name in group_names:
        try:
            rows = _fetch_group_universe(
                group_mode=group_mode,
                group_name=group_name,
                tickers_per_group=max(2, args.tickers_per_group),
            )
        except Exception as exc:
            print(f"ERROR: FINVIZ fetch failed for {group_name}: {exc}", file=sys.stderr)
            return 1
        if not rows:
            continue
        group_rows[group_name] = rows
        all_symbols.extend(str(item["symbol"]) for item in rows)
        print(f"  FINVIZ group {group_name}: {len(rows)} tickers")

    if not group_rows:
        print("ERROR: no FINVIZ universe rows found.", file=sys.stderr)
        return 1

    normalized_symbols = sorted({symbol for symbol in all_symbols if symbol})
    frame_map, history_source = _load_price_history(
        normalized_symbols,
        as_of_date=as_of_date,
        lookback_days=max(args.lookback_days, args.min_history_days),
        database_url=database_url,
    )
    print(f"  history source: {history_source}")
    print(f"  history loaded: {len(frame_map)} tickers")
    if not frame_map:
        print("ERROR: no price history loaded for pair screener.", file=sys.stderr)
        return 1

    pairs, stats = _analyze_pairs(
        group_rows=group_rows,
        frame_map=frame_map,
        min_correlation=float(args.min_correlation),
        min_history_days=int(args.min_history_days),
        max_half_life=float(args.max_half_life),
        entry_zscore=float(args.entry_zscore),
    )
    final_pairs = pairs[: max(1, int(args.limit))]
    actionable_pairs = [item for item in final_pairs if item.get("actionable")]
    summary = {
        "universe_size": len(normalized_symbols),
        "pairs_analyzed": stats["pairs_considered"],
        "correlation_pass": stats["correlation_pass"],
        "cointegrated_pairs": stats["cointegration_pass"],
        "actionable_pairs": len(actionable_pairs),
        "top_pair": final_pairs[0]["pair"] if final_pairs else None,
    }
    payload = {
        "report_type": "pair_trade_screener",
        "generated_at": dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "metadata": {
            "date_label": date_label,
            "as_of_date": as_of_date.isoformat(),
            "group_mode": group_mode,
            "included_groups": group_names,
            "lookback_days": int(args.lookback_days),
            "min_history_days": int(args.min_history_days),
            "min_correlation": float(args.min_correlation),
            "max_half_life": float(args.max_half_life),
            "entry_zscore": float(args.entry_zscore),
            "tickers_per_group": int(args.tickers_per_group),
            "stats_test_mode": "cointegration" if adfuller is not None else "correlation_half_life_fallback",
            "data_sources": {
                "universe": "finvizfinance",
                "history": history_source,
            },
        },
        "summary": summary,
        "pairs": final_pairs,
    }

    timestamp = dt.datetime.now().strftime("%Y-%m-%d_%H%M%S")
    json_path = output_dir / f"pair_trade_screener_{timestamp}.json"
    md_path = output_dir / f"pair_trade_screener_{timestamp}.md"
    json_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    md_path.write_text(_render_markdown(payload), encoding="utf-8")

    print(f"Wrote run summary to {json_path}")
    print(f"Wrote markdown report to {md_path}")
    print(f"Top pairs kept: {len(final_pairs)}")
    if final_pairs:
        print(f"Top pair: {final_pairs[0]['pair']} signal={final_pairs[0].get('signal')}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
