#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys
from typing import TYPE_CHECKING, Any

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.config import load_app_config, today_label
from src.oracle_nosql_market_data import (
    DailyBarRow,
    TickerMetadataRow,
    build_daily_bars_table_ddl,
    build_ticker_metadata_table_ddl,
    chunked,
    rows_to_jsonl_lines,
    utc_now_iso,
)
from src.ticker_filters import load_excluded_tickers

if TYPE_CHECKING:
    from src.universe import UniverseTicker


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Export or sync daily market data into Oracle NoSQL-friendly tables."
    )
    parser.add_argument("--config", default="", help="Optional market config path.")
    parser.add_argument("--start-date", default="2020-01-01", help="Inclusive history start date (YYYY-MM-DD).")
    parser.add_argument("--end-date", default=today_label(), help="Inclusive history end date (YYYY-MM-DD).")
    parser.add_argument(
        "--incremental-days",
        type=int,
        default=0,
        help="If set, ignore --start-date and sync only the last N calendar days through --end-date.",
    )
    parser.add_argument("--limit", type=int, help="Optional universe limit for smoke runs.")
    parser.add_argument("--tickers", nargs="+", help="Optional explicit ticker list instead of the configured universe.")
    parser.add_argument(
        "--chunk-size",
        type=int,
        default=100,
        help="Number of tickers per yfinance download call.",
    )
    parser.add_argument(
        "--export-jsonl-dir",
        default="",
        help="Optional explicit export directory. Defaults to artifacts/raw/oracle_nosql_export_<date>/",
    )
    parser.add_argument(
        "--daily-bars-table",
        default="daily_bars",
        help="Oracle NoSQL table name for daily bars.",
    )
    parser.add_argument(
        "--ticker-metadata-table",
        default="ticker_metadata",
        help="Oracle NoSQL table name for ticker metadata.",
    )
    parser.add_argument(
        "--source-label",
        default="yfinance",
        help="Source label stored in exported rows.",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Apply exported rows directly to Oracle NoSQL using the Python SDK.",
    )
    parser.add_argument(
        "--create-tables",
        action="store_true",
        help="Create Oracle NoSQL tables before writing rows. Useful for the first seed run.",
    )
    parser.add_argument("--endpoint", default="", help="Oracle NoSQL endpoint. Required with --apply.")
    parser.add_argument("--compartment", default="", help="Oracle Cloud compartment name or OCID.")
    parser.add_argument(
        "--oci-profile",
        default="DEFAULT",
        help="OCI config profile name for Oracle NoSQL auth when using --apply.",
    )
    parser.add_argument(
        "--oci-config-file",
        default="",
        help="Optional OCI config file path. Defaults to the standard OCI SDK location.",
    )
    parser.add_argument(
        "--instance-principal",
        action="store_true",
        help="Use OCI instance principal auth instead of local OCI config when using --apply.",
    )
    return parser.parse_args()


def _resolve_date_window(args: argparse.Namespace) -> tuple[str, str]:
    import datetime as dt

    end_date = dt.date.fromisoformat(args.end_date)
    if int(args.incremental_days) > 0:
        start_date = end_date - dt.timedelta(days=int(args.incremental_days))
        return start_date.isoformat(), end_date.isoformat()
    return args.start_date, args.end_date


def _manual_tickers(symbols: list[str], excluded: set[str]) -> list["UniverseTicker"]:
    from src.universe import UniverseTicker

    seen: set[str] = set()
    result: list[UniverseTicker] = []
    for raw in symbols:
        symbol = str(raw).strip().upper()
        if not symbol or symbol in excluded or symbol in seen:
            continue
        seen.add(symbol)
        result.append(UniverseTicker(symbol=symbol))
    return result


def _load_target_universe(args: argparse.Namespace) -> tuple[Any, list["UniverseTicker"]]:
    from src.universe import load_universe

    config = load_app_config(args.config or None)
    excluded = load_excluded_tickers(config)
    if args.tickers:
        return config, _manual_tickers(args.tickers, excluded)
    return config, load_universe(config, limit=args.limit)


def _normalize_download_chunk(data: pd.DataFrame, tickers: list[str]) -> dict[str, pd.DataFrame]:
    import pandas as pd

    frames: dict[str, pd.DataFrame] = {}
    if data is None or data.empty:
        return frames

    if isinstance(data.columns, pd.MultiIndex):
        available = set(data.columns.get_level_values(0))
        for ticker in tickers:
            if ticker not in available:
                continue
            frame = data[ticker].copy()
            frame.columns = [str(column) for column in frame.columns]
            frames[ticker] = frame
        return frames

    if tickers:
        frame = data.copy()
        frame.columns = [str(column) for column in frame.columns]
        frames[tickers[0]] = frame
    return frames


def _download_history(tickers: list[str], start_date: str, end_date: str, chunk_size: int) -> dict[str, pd.DataFrame]:
    import pandas as pd
    import yfinance as yf

    history_by_ticker: dict[str, pd.DataFrame] = {}
    for chunk_index, chunk in enumerate(chunked(tickers, chunk_size), start=1):
        chunk_list = list(chunk)
        print(
            f"downloading chunk {chunk_index}: "
            f"{chunk_list[0]}..{chunk_list[-1]} ({len(chunk_list)} tickers)",
            flush=True,
        )
        data = yf.download(
            tickers=chunk_list,
            start=start_date,
            end=(pd.Timestamp(end_date) + pd.Timedelta(days=1)).date().isoformat(),
            interval="1d",
            auto_adjust=False,
            progress=False,
            group_by="ticker",
            threads=False,
        )
        history_by_ticker.update(_normalize_download_chunk(data, chunk_list))
    return history_by_ticker


def _clean_nullable_number(value: object) -> float | None:
    import pandas as pd

    if value is None or pd.isna(value):
        return None
    return float(value)


def _clean_nullable_int(value: object) -> int | None:
    import pandas as pd

    if value is None or pd.isna(value):
        return None
    return int(value)


def _build_metadata_rows(universe: list["UniverseTicker"], source_label: str, updated_at: str) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for ticker in universe:
        rows.append(
            TickerMetadataRow(
                ticker=ticker.symbol,
                exchange=ticker.exchange,
                sector=ticker.sector,
                industry=ticker.industry,
                is_active=True,
                source=source_label,
                updated_at=updated_at,
            ).to_dict()
        )
    return rows


def _build_daily_bar_rows(
    universe: list["UniverseTicker"],
    history_by_ticker: dict[str, pd.DataFrame],
    source_label: str,
    updated_at: str,
) -> list[dict[str, object]]:
    import pandas as pd

    universe_by_symbol = {item.symbol: item for item in universe}
    rows: list[dict[str, object]] = []
    for ticker, history in history_by_ticker.items():
        if ticker in universe_by_symbol:
            meta = universe_by_symbol[ticker]
        else:
            from src.universe import UniverseTicker

            meta = UniverseTicker(symbol=ticker)
        if history.empty:
            continue
        frame = history.copy().reset_index()
        date_column = "Date" if "Date" in frame.columns else str(frame.columns[0])
        if "Adj Close" not in frame.columns and "Close" in frame.columns:
            frame["Adj Close"] = frame["Close"]
        if "Dividends" not in frame.columns:
            frame["Dividends"] = 0.0
        if "Stock Splits" not in frame.columns:
            frame["Stock Splits"] = 0.0
        for _, row in frame.iterrows():
            trade_date = pd.Timestamp(row[date_column]).date().isoformat()
            rows.append(
                DailyBarRow(
                    ticker=ticker,
                    trade_date=trade_date,
                    open=_clean_nullable_number(row.get("Open")),
                    high=_clean_nullable_number(row.get("High")),
                    low=_clean_nullable_number(row.get("Low")),
                    close=_clean_nullable_number(row.get("Close")),
                    adj_close=_clean_nullable_number(row.get("Adj Close")),
                    volume=_clean_nullable_int(row.get("Volume")),
                    dividend=_clean_nullable_number(row.get("Dividends")),
                    split_factor=_clean_nullable_number(row.get("Stock Splits")),
                    exchange=meta.exchange,
                    sector=meta.sector,
                    source=source_label,
                    updated_at=updated_at,
                ).to_dict()
            )
    rows.sort(key=lambda item: (str(item["ticker"]), str(item["trade_date"])))
    return rows


def _write_jsonl(path: Path, rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        for line in rows_to_jsonl_lines(rows):
            handle.write(line)
            handle.write("\n")


def _write_manifest(
    export_dir: Path,
    args: argparse.Namespace,
    start_date: str,
    end_date: str,
    metadata_rows: list[dict[str, object]],
    bar_rows: list[dict[str, object]],
    downloaded_tickers: int,
) -> None:
    manifest = {
        "generated_at": utc_now_iso(),
        "start_date": start_date,
        "end_date": end_date,
        "incremental_days": int(args.incremental_days),
        "daily_bars_table": args.daily_bars_table,
        "ticker_metadata_table": args.ticker_metadata_table,
        "source_label": args.source_label,
        "ticker_count_requested": len(metadata_rows),
        "ticker_count_with_history": downloaded_tickers,
        "daily_bar_row_count": len(bar_rows),
        "files": {
            "ticker_metadata_jsonl": "ticker_metadata.jsonl",
            "daily_bars_jsonl": "daily_bars.jsonl",
            "ddl_sql": "oracle_nosql_schema.sql",
        },
    }
    (export_dir / "manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")


def _export_files(
    export_dir: Path,
    args: argparse.Namespace,
    start_date: str,
    end_date: str,
    metadata_rows: list[dict[str, object]],
    bar_rows: list[dict[str, object]],
    downloaded_tickers: int,
) -> None:
    export_dir.mkdir(parents=True, exist_ok=True)
    _write_jsonl(export_dir / "ticker_metadata.jsonl", metadata_rows)
    _write_jsonl(export_dir / "daily_bars.jsonl", bar_rows)
    ddl = "\n\n".join(
        [
            build_ticker_metadata_table_ddl(args.ticker_metadata_table) + ";",
            build_daily_bars_table_ddl(args.daily_bars_table) + ";",
        ]
    )
    (export_dir / "oracle_nosql_schema.sql").write_text(ddl + "\n", encoding="utf-8")
    _write_manifest(export_dir, args, start_date, end_date, metadata_rows, bar_rows, downloaded_tickers)


def _create_handle(args: argparse.Namespace):
    try:
        from borneo import NoSQLHandle, NoSQLHandleConfig
        from borneo.iam import SignatureProvider
    except ImportError as exc:
        raise RuntimeError(
            "Oracle NoSQL SDK is not installed. Install `borneo` and `oci` before using --apply."
        ) from exc

    if not args.endpoint:
        raise RuntimeError("--endpoint is required when using --apply.")

    if args.instance_principal:
        provider = SignatureProvider.create_with_instance_principal()
    elif args.oci_config_file:
        provider = SignatureProvider(config_file=args.oci_config_file, profile_name=args.oci_profile)
    else:
        provider = SignatureProvider(profile_name=args.oci_profile)

    config = NoSQLHandleConfig(args.endpoint, provider)
    if args.compartment:
        config.set_default_compartment(args.compartment)
    return NoSQLHandle(config)


def _create_tables(handle: Any, args: argparse.Namespace) -> None:
    from borneo import TableRequest

    statements = [
        build_ticker_metadata_table_ddl(args.ticker_metadata_table),
        build_daily_bars_table_ddl(args.daily_bars_table),
    ]
    for statement in statements:
        request = TableRequest().set_statement(statement)
        if args.compartment:
            request.set_compartment(args.compartment)
        result = handle.do_table_request(request, 40000, 3000)
        result.wait_for_completion(handle, 40000, 3000)


def _put_rows(handle: Any, table_name: str, rows: list[dict[str, object]], compartment: str) -> None:
    from borneo import PutRequest

    for index, row in enumerate(rows, start=1):
        request = PutRequest().set_table_name(table_name).set_value(row)
        if compartment:
            request.set_compartment(compartment)
        handle.put(request)
        if index % 1000 == 0:
            print(f"applied {index} rows into {table_name}", flush=True)


def _apply_rows(
    args: argparse.Namespace,
    metadata_rows: list[dict[str, object]],
    bar_rows: list[dict[str, object]],
) -> None:
    handle = _create_handle(args)
    try:
        if args.create_tables:
            _create_tables(handle, args)
        _put_rows(handle, args.ticker_metadata_table, metadata_rows, args.compartment)
        _put_rows(handle, args.daily_bars_table, bar_rows, args.compartment)
    finally:
        handle.close()


def main() -> int:
    args = parse_args()
    config, universe = _load_target_universe(args)
    del config  # universe loading is the only config-dependent work here.
    start_date, end_date = _resolve_date_window(args)

    symbols = [item.symbol for item in universe]
    print(f"target_ticker_count={len(symbols)}", flush=True)
    print(f"date_window={start_date}..{end_date}", flush=True)
    history_by_ticker = _download_history(symbols, start_date, end_date, args.chunk_size)
    updated_at = utc_now_iso()
    metadata_rows = _build_metadata_rows(universe, args.source_label, updated_at)
    bar_rows = _build_daily_bar_rows(universe, history_by_ticker, args.source_label, updated_at)

    default_export_dir = PROJECT_ROOT / "artifacts" / "raw" / f"oracle_nosql_export_{today_label()}"
    export_dir = Path(args.export_jsonl_dir) if args.export_jsonl_dir else default_export_dir
    _export_files(export_dir, args, start_date, end_date, metadata_rows, bar_rows, len(history_by_ticker))

    print(f"export_dir={export_dir}", flush=True)
    print(f"ticker_metadata_rows={len(metadata_rows)}", flush=True)
    print(f"daily_bar_rows={len(bar_rows)}", flush=True)
    print(
        "ddl_tables="
        f"{args.ticker_metadata_table},{args.daily_bars_table}",
        flush=True,
    )

    if args.apply:
        _apply_rows(args, metadata_rows, bar_rows)
        print("oracle_nosql_apply=done", flush=True)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
