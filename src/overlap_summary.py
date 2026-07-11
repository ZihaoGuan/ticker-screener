from __future__ import annotations

import json
import re
from collections import defaultdict
from html import escape
from pathlib import Path

from src.artifact_paths import build_screener_artifact_paths, resolve_legacy_paths
from src.etf_matcher import infer_theme_tags_for_ticker, load_etf_catalog, load_ticker_theme_overrides


PIPELINES = (
    {
        "id": "rs",
        "label": "RS",
        "filename": "rs_new_high_before_price_{date}.json",
    },
    {
        "id": "weekly_rs",
        "label": "Weekly RS New High Before Price",
        "filename": "weekly_rs_new_high_{date}.json",
    },
    {
        "id": "sean_peg",
        "label": "Sean PEG",
        "filename": "sean_peg_earnings_gap_{date}.json",
        "fallback_filename": "peg_earnings_gap_{date}.json",
    },
    {
        "id": "legacy_peg",
        "label": "Legacy PEG",
        "filename": "legacy_peg_earnings_gap_{date}.json",
        "fallback_filename": "peg_earnings_gap_{date}.json",
    },
    {
        "id": "canslim",
        "label": "CANSLIM",
        "filename": "canslim_{date}.json",
    },
    {
        "id": "vcp",
        "label": "VCP",
        "filename": "vcp_{date}.json",
    },
    {
        "id": "weekly_vcp",
        "label": "Weekly VCP",
        "filename": "weekly_vcp_{date}.json",
    },
    {
        "id": "weekly_vcp_scored",
        "label": "Weekly VCP Scored",
        "filename": "weekly_vcp_scored_{date}.json",
    },
    {
        "id": "cup_handle",
        "label": "Cup and Handle",
        "filename": "cup_handle_{date}.json",
    },
    {
        "id": "weekly_htf_pullback",
        "label": "Weekly HTF 8W Pullback",
        "filename": "weekly_htf_pullback_{date}.json",
    },
    {
        "id": "eight_week_100_runup",
        "label": "8W 100% Runup",
        "filename": "eight_week_100_runup_{date}.json",
    },
    {
        "id": "gap_fill",
        "label": "Gap Fill",
        "filename": "gap_fill_{date}.json",
    },
    {
        "id": "near_200ma",
        "label": "Near 200MA",
        "filename": "near_200ma_{date}.json",
    },
    {
        "id": "hve",
        "label": "HVE",
        "filename": "hve_{date}.json",
    },
    {
        "id": "inside_dryup",
        "label": "Inside Dry-Up",
        "filename": "inside_dryup_{date}.json",
    },
    {
        "id": "inside_dryup_v2",
        "label": "Inside Day + Extreme Dry-Up",
        "filename": "inside_dryup_v2_{date}.json",
    },
    {
        "id": "wyckoff_buy_signal",
        "label": "Wyckoff Buy Signal",
        "filename": "wyckoff_buy_signal_{date}.json",
    },
    {
        "id": "wyckoff_sell_signal",
        "label": "Wyckoff Sell Signal",
        "filename": "wyckoff_sell_signal_{date}.json",
    },
    {
        "id": "ftd_sweep",
        "label": "Sweep Success",
        "filename": "ftd_sweep_{date}.json",
    },
    {
        "id": "fearzone_zeiierman",
        "label": "Fearzone Zeiierman",
        "filename": "fearzone_zeiierman_{date}.json",
    },
    {
        "id": "bb_squeeze",
        "label": "BB Squeeze",
        "filename": "bb_squeeze_{date}.json",
    },
    {
        "id": "bollinger_band_breakout",
        "label": "Above Upper Bollinger Band",
        "filename": "bollinger_band_breakout_{date}.json",
    },
    {
        "id": "high_tight_flag",
        "label": "High Tight Flag",
        "filename": "high_tight_flag_{date}.json",
    },
    {
        "id": "high_tight_flag_setup",
        "label": "High Tight Flag Setup",
        "filename": "high_tight_flag_setup_{date}.json",
    },
    {
        "id": "leif_high_tight_flag",
        "label": "Leif High Tight Flag",
        "filename": "leif_high_tight_flag_{date}.json",
    },
    {
        "id": "sepa_vcp",
        "label": "SEPA VCP",
        "filename": "sepa_vcp_{date}.json",
    },
    {
        "id": "weekly_sepa_vcp",
        "label": "Weekly SEPA VCP",
        "filename": "weekly_sepa_vcp_{date}.json",
    },
    {
        "id": "vcp_v3",
        "label": "VCP v3",
        "filename": "vcp_v3_{date}.json",
    },
    {
        "id": "weekly_vcp_v3",
        "label": "Weekly VCP v3",
        "filename": "weekly_vcp_v3_{date}.json",
    },
    {
        "id": "vcp_spec",
        "label": "VCP Spec",
        "filename": "vcp_spec_{date}.json",
    },
    {
        "id": "weekly_vcp_spec",
        "label": "Weekly VCP Spec",
        "filename": "weekly_vcp_spec_{date}.json",
    },
    {
        "id": "rti",
        "label": "Range Tightness Index",
        "filename": "rti_{date}.json",
    },
    {
        "id": "sean_breakout",
        "label": "Sean Breakout",
        "filename": "sean_breakout_{date}.json",
    },
    {
        "id": "vcs_setup_stage",
        "label": "VCS Setup Stage",
        "filename": "vcs_setup_stage_{date}.json",
    },
    {
        "id": "vcs_critical_tightness",
        "label": "VCS Critical Tightness",
        "filename": "vcs_critical_tightness_{date}.json",
    },
    {
        "id": "td9_bullish",
        "label": "TD9 Bullish",
        "filename": "td9_bullish_{date}.json",
    },
    {
        "id": "macd_golden_cross",
        "label": "MACD Golden Cross",
        "filename": "macd_golden_cross_{date}.json",
    },
    {
        "id": "base_detection",
        "label": "Base Detection",
        "filename": "base_detection_{date}.json",
    },
    {
        "id": "cup_detection",
        "label": "Cup Detection",
        "filename": "cup_detection_{date}.json",
    },
    {
        "id": "double_bottom_detection",
        "label": "Double Bottom Detection",
        "filename": "double_bottom_detection_{date}.json",
    },
    {
        "id": "weekly_tight_close",
        "label": "Weekly Tight Close",
        "filename": "weekly_tight_close_{date}.json",
    },
    {
        "id": "weinstein_stage2_early",
        "label": "Weinstein Stage 2 Early",
        "filename": "weinstein_stage2_early_{date}.json",
    },
    {
        "id": "sma200_pullback_buy",
        "label": "200 SMA Pullback Buy",
        "filename": "sma200_pullback_buy_{date}.json",
    },
    {
        "id": "weekly_tight_close_breakout",
        "label": "Weekly Tight Close Breakout",
        "filename": "weekly_tight_close_breakout_{date}.json",
    },
)

PIPELINE_GROUPS: dict[str, dict[str, str]] = {
    "rs": {"bias_group": "bullish", "bullish_subgroup": "leaders"},
    "weekly_rs": {"bias_group": "bullish", "bullish_subgroup": "leaders"},
    "sean_peg": {"bias_group": "bullish", "bullish_subgroup": "leaders"},
    "legacy_peg": {"bias_group": "bullish", "bullish_subgroup": "leaders"},
    "canslim": {"bias_group": "bullish", "bullish_subgroup": "leaders"},
    "vcp": {"bias_group": "bullish", "bullish_subgroup": "leaders"},
    "weekly_vcp": {"bias_group": "bullish", "bullish_subgroup": "leaders"},
    "weekly_vcp_scored": {"bias_group": "bullish", "bullish_subgroup": "leaders"},
    "cup_handle": {"bias_group": "bullish", "bullish_subgroup": "leaders"},
    "weekly_htf_pullback": {"bias_group": "bullish", "bullish_subgroup": "leaders"},
    "eight_week_100_runup": {"bias_group": "bullish", "bullish_subgroup": "leaders"},
    "gap_fill": {"bias_group": "bullish", "bullish_subgroup": "bottoming"},
    "near_200ma": {"bias_group": "bullish", "bullish_subgroup": "bottoming"},
    "hve": {"bias_group": "bullish", "bullish_subgroup": "leaders"},
    "inside_dryup": {"bias_group": "bullish", "bullish_subgroup": "bottoming"},
    "inside_dryup_v2": {"bias_group": "bullish", "bullish_subgroup": "bottoming"},
    "wyckoff_buy_signal": {"bias_group": "bullish", "bullish_subgroup": "bottoming"},
    "wyckoff_sell_signal": {"bias_group": "bearish"},
    "ftd_sweep": {"bias_group": "bullish", "bullish_subgroup": "leaders"},
    "fearzone_zeiierman": {"bias_group": "bullish", "bullish_subgroup": "bottoming"},
    "bb_squeeze": {"bias_group": "bullish", "bullish_subgroup": "leaders"},
    "bollinger_band_breakout": {"bias_group": "bullish", "bullish_subgroup": "leaders"},
    "high_tight_flag": {"bias_group": "bullish", "bullish_subgroup": "leaders"},
    "high_tight_flag_setup": {"bias_group": "bullish", "bullish_subgroup": "leaders"},
    "leif_high_tight_flag": {"bias_group": "bullish", "bullish_subgroup": "leaders"},
    "sepa_vcp": {"bias_group": "bullish", "bullish_subgroup": "leaders"},
    "weekly_sepa_vcp": {"bias_group": "bullish", "bullish_subgroup": "leaders"},
    "vcp_v3": {"bias_group": "bullish", "bullish_subgroup": "leaders"},
    "weekly_vcp_v3": {"bias_group": "bullish", "bullish_subgroup": "leaders"},
    "vcp_spec": {"bias_group": "bullish", "bullish_subgroup": "leaders"},
    "weekly_vcp_spec": {"bias_group": "bullish", "bullish_subgroup": "leaders"},
    "rti": {"bias_group": "bullish", "bullish_subgroup": "leaders"},
    "sean_breakout": {"bias_group": "bullish", "bullish_subgroup": "leaders"},
    "vcs_setup_stage": {"bias_group": "bullish", "bullish_subgroup": "leaders"},
    "vcs_critical_tightness": {"bias_group": "bullish", "bullish_subgroup": "leaders"},
    "td9_bullish": {"bias_group": "bullish", "bullish_subgroup": "bottoming"},
    "macd_golden_cross": {"bias_group": "bullish", "bullish_subgroup": "bottoming"},
    "base_detection": {"bias_group": "bullish", "bullish_subgroup": "bottoming"},
    "cup_detection": {"bias_group": "bullish", "bullish_subgroup": "bottoming"},
    "double_bottom_detection": {"bias_group": "bullish", "bullish_subgroup": "bottoming"},
    "weekly_tight_close": {"bias_group": "bullish", "bullish_subgroup": "leaders"},
    "weinstein_stage2_early": {"bias_group": "bullish", "bullish_subgroup": "leaders"},
    "sma200_pullback_buy": {"bias_group": "bullish", "bullish_subgroup": "pullbacks"},
    "weekly_tight_close_breakout": {"bias_group": "bullish", "bullish_subgroup": "leaders"},
}

DRUG_THEME_TAGS = {
    "health care",
    "biotech",
    "pharmaceuticals",
    "medical devices",
    "health care equipment",
    "health care services",
}

DATE_LABEL_RE = re.compile(r"\d{4}-\d{2}-\d{2}")


def load_watchlist(path: Path | None) -> list[dict[str, object]]:
    if path is None or not path.exists():
        return []
    payload = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, dict)]
    return []


def extract_tickers(entries: list[dict[str, object]]) -> list[str]:
    seen: set[str] = set()
    tickers: list[str] = []
    for item in entries:
        ticker = str(item.get("ticker", "")).strip().upper()
        if not ticker or ticker in seen:
            continue
        seen.add(ticker)
        tickers.append(ticker)
    return tickers


def build_ticker_metadata(
    entries: list[dict[str, object]],
    catalog: list[dict[str, object]],
    overrides: dict[str, list[str]],
) -> dict[str, dict[str, object]]:
    metadata: dict[str, dict[str, object]] = {}
    for item in entries:
        ticker = str(item.get("ticker", "")).strip().upper()
        if not ticker:
            continue
        sector = str(item.get("sector", "") or "").strip() or None
        industry = str(item.get("industry", "") or "").strip() or None
        entry = metadata.setdefault(
            ticker,
            {
                "sector": None,
                "industry": None,
                "theme_tags": [],
                "is_drug_ticker": False,
            },
        )
        if entry["sector"] is None and sector:
            entry["sector"] = sector
        if entry["industry"] is None and industry:
            entry["industry"] = industry
        theme_tags = infer_theme_tags_for_ticker(
            ticker=ticker,
            sector=entry["sector"],
            industry=entry["industry"],
            catalog=catalog,
            overrides=overrides,
        )
        entry["theme_tags"] = theme_tags
        entry["is_drug_ticker"] = any(theme in DRUG_THEME_TAGS for theme in theme_tags)
    return metadata


def resolve_pipeline_path(watchlist_dir: Path, date_label: str, pipeline: dict[str, str]) -> tuple[Path | None, str]:
    artifacts_dir = watchlist_dir.parent
    strategy_id = str(pipeline["id"])
    dated_path = build_screener_artifact_paths(artifacts_dir, strategy_id=strategy_id, date_label=date_label).watchlist_path
    if dated_path.exists():
        return dated_path, "dated"
    for legacy_path in resolve_legacy_paths(artifacts_dir, strategy_id=strategy_id, date_label=date_label)["watchlist"]:
        if legacy_path.exists():
            return legacy_path, "legacy"
    return None, "missing"


def discover_supported_dates(watchlist_dir: Path) -> list[str]:
    dates: set[str] = set()
    screeners_dir = watchlist_dir.parent / "screeners"
    if screeners_dir.exists():
        for path in screeners_dir.glob("*/*/watchlist.json"):
            if len(path.parts) >= 3:
                match = DATE_LABEL_RE.search(path.parts[-3])
                if match:
                    dates.add(match.group(0))
    if not watchlist_dir.exists() and not dates:
        return []
    for pipeline in PIPELINES:
        for key in ("filename", "fallback_filename"):
            pattern = pipeline.get(key)
            if not pattern:
                continue
            prefix = pattern.split("{date}", 1)[0]
            suffix = pattern.split("{date}", 1)[1]
            glob_pattern = f"{prefix}*{suffix}"
            for path in watchlist_dir.glob(glob_pattern):
                match = DATE_LABEL_RE.search(path.name)
                if match:
                    dates.add(match.group(0))
    return sorted(dates, reverse=True)


def build_overlap_payload(date_label: str, watchlist_dir: Path) -> dict[str, object]:
    pipeline_tickers: dict[str, list[str]] = {}
    pipeline_counts: dict[str, int] = {}
    pipeline_status: list[dict[str, object]] = []
    ticker_to_pipelines: dict[str, set[str]] = defaultdict(set)
    ticker_metadata: dict[str, dict[str, object]] = {}
    catalog = load_etf_catalog()
    overrides = load_ticker_theme_overrides()
    labels_by_id = {pipeline["id"]: pipeline["label"] for pipeline in PIPELINES}

    for pipeline in PIPELINES:
        pipeline_id = pipeline["id"]
        path, resolution = resolve_pipeline_path(watchlist_dir, date_label, pipeline)
        entries = load_watchlist(path)
        tickers = extract_tickers(entries)
        metadata = build_ticker_metadata(entries, catalog, overrides)
        pipeline_tickers[pipeline_id] = tickers
        pipeline_counts[pipeline_id] = len(tickers)
        pipeline_status.append(
            {
                "id": pipeline_id,
                "label": pipeline["label"],
                "file_present": path is not None,
                "count": len(tickers),
                "source_filename": path.name if path is not None else "",
                "resolution": resolution,
                "bias_group": PIPELINE_GROUPS.get(pipeline_id, {}).get("bias_group", "other"),
                "bullish_subgroup": PIPELINE_GROUPS.get(pipeline_id, {}).get("bullish_subgroup", ""),
            }
        )
        for ticker in tickers:
            ticker_to_pipelines[ticker].add(pipeline_id)
            if ticker not in ticker_metadata:
                ticker_metadata[ticker] = metadata.get(
                    ticker,
                    {"sector": None, "industry": None, "theme_tags": [], "is_drug_ticker": False},
                )
                continue
            existing = ticker_metadata[ticker]
            incoming = metadata.get(ticker)
            if not incoming:
                continue
            if existing.get("sector") is None and incoming.get("sector") is not None:
                existing["sector"] = incoming["sector"]
            if existing.get("industry") is None and incoming.get("industry") is not None:
                existing["industry"] = incoming["industry"]
            merged_tags = sorted(set(existing.get("theme_tags", [])) | set(incoming.get("theme_tags", [])))
            existing["theme_tags"] = merged_tags
            existing["is_drug_ticker"] = bool(existing.get("is_drug_ticker")) or bool(incoming.get("is_drug_ticker"))

    overlap_two_plus = [
        {
            "ticker": ticker,
            "pipelines": sorted(pipelines),
            "pipeline_labels": [labels_by_id[pipeline_id] for pipeline_id in sorted(pipelines)],
            "pipeline_count": len(pipelines),
            "sector": ticker_metadata.get(ticker, {}).get("sector"),
            "industry": ticker_metadata.get(ticker, {}).get("industry"),
            "theme_tags": ticker_metadata.get(ticker, {}).get("theme_tags", []),
            "is_drug_ticker": bool(ticker_metadata.get(ticker, {}).get("is_drug_ticker", False)),
        }
        for ticker, pipelines in ticker_to_pipelines.items()
        if len(pipelines) >= 2
    ]
    overlap_two_plus.sort(key=lambda item: (-int(item["pipeline_count"]), str(item["ticker"])))
    overlap_three_plus = [item for item in overlap_two_plus if int(item["pipeline_count"]) >= 3]
    fearzone_path, _ = resolve_pipeline_path(
        watchlist_dir,
        date_label,
        {"id": "fearzone", "label": "Fearzone", "filename": "fearzone_{date}.json"},
    )
    fearzone_entries = load_watchlist(fearzone_path)
    fearzone_tickers = extract_tickers(fearzone_entries)

    return {
        "date_label": date_label,
        "pipeline_status": pipeline_status,
        "pipeline_counts": pipeline_counts,
        "pipeline_tickers": pipeline_tickers,
        "present_pipelines": [item["id"] for item in pipeline_status if item["file_present"]],
        "missing_pipelines": [item["id"] for item in pipeline_status if not item["file_present"]],
        "unique_ticker_count": len(ticker_to_pipelines),
        "overlap_two_plus": overlap_two_plus,
        "overlap_three_plus": overlap_three_plus,
        "fearzone_tickers": fearzone_tickers,
    }


def build_text_summary(payload: dict[str, object]) -> str:
    date_label = str(payload["date_label"])
    pipeline_status = payload["pipeline_status"]
    overlap_two_plus = payload["overlap_two_plus"]
    overlap_three_plus = payload["overlap_three_plus"]

    lines = [
        f"Daily overlap summary for {date_label}",
        f"Unique tickers across present pipelines: {payload['unique_ticker_count']}",
        f"Overlap >=2 pipelines: {len(overlap_two_plus)}",
        f"Overlap >=3 pipelines: {len(overlap_three_plus)}",
        "",
        "Pipeline status:",
    ]
    for pipeline in pipeline_status:
        label = pipeline["label"]
        present = "present" if pipeline["file_present"] else "missing"
        source = pipeline.get("source_filename") or "n/a"
        lines.append(f"- {label}: {present}, count={pipeline['count']}, source={source}")

    lines.extend(["", "Top overlaps:"])
    for item in overlap_two_plus[:25]:
        ticker = item["ticker"]
        pipelines = ", ".join(item["pipeline_labels"])
        tags = ", ".join(item.get("theme_tags", [])) or "-"
        drug_flag = " [drug]" if item.get("is_drug_ticker") else ""
        lines.append(f"- {ticker}{drug_flag}: {pipelines} | tags={tags}")
    if len(overlap_two_plus) > 25:
        lines.append(f"- ... and {len(overlap_two_plus) - 25} more")
    return "\n".join(lines) + "\n"


def build_html_summary(payload: dict[str, object]) -> str:
    date_label = escape(str(payload["date_label"]))
    unique_count = int(payload["unique_ticker_count"])
    overlap_two_plus = payload["overlap_two_plus"]
    overlap_three_plus = payload["overlap_three_plus"]
    pipeline_status = payload["pipeline_status"]

    status_cards = []
    for pipeline in pipeline_status:
        badge = "present" if pipeline["file_present"] else "missing"
        badge_class = "ok" if pipeline["file_present"] else "missing"
        source_filename = escape(str(pipeline.get("source_filename") or "n/a"))
        resolution = escape(str(pipeline.get("resolution") or "missing"))
        status_cards.append(
            f"""
            <article class="status-card">
              <div class="status-head">
                <h3>{escape(str(pipeline['label']))}</h3>
                <span class="badge {badge_class}">{badge}</span>
              </div>
              <p>{int(pipeline['count'])} tickers</p>
              <pre>source: {source_filename}\nresolution: {resolution}</pre>
            </article>
            """
        )

    rows = []
    for item in overlap_two_plus[:100]:
        tags = escape(", ".join(item.get("theme_tags", [])) or "-")
        drug_flag = "Yes" if item.get("is_drug_ticker") else "No"
        rows.append(
            f"""
            <tr>
              <td>{escape(str(item['ticker']))}</td>
              <td>{int(item['pipeline_count'])}</td>
              <td>{escape(', '.join(item['pipeline_labels']))}</td>
              <td>{tags}</td>
              <td>{drug_flag}</td>
            </tr>
            """
        )
    table_rows = "".join(rows) or '<tr><td colspan="5">No overlaps found.</td></tr>'

    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Daily Overlap Summary {date_label}</title>
  <style>
    :root {{
      --bg: #0b1220;
      --panel: #111c2f;
      --panel-2: #16233a;
      --ink: #edf2f7;
      --muted: #9fb0c6;
      --line: #24354d;
      --good: #22c55e;
      --warn: #f59e0b;
      --bad: #ef4444;
      --accent: #38bdf8;
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      font-family: "SF Mono", "Menlo", "Consolas", monospace;
      background: radial-gradient(circle at top left, rgba(56, 189, 248, 0.12), transparent 28%), var(--bg);
      color: var(--ink);
    }}
    main {{
      max-width: 1160px;
      margin: 0 auto;
      padding: 32px 20px 56px;
    }}
    header, section {{
      border: 1px solid var(--line);
      border-radius: 20px;
      background: rgba(17, 28, 47, 0.92);
      padding: 24px;
      margin-bottom: 20px;
    }}
    h1, h2, h3 {{
      margin: 0;
    }}
    p {{
      color: var(--muted);
      line-height: 1.6;
    }}
    .stats, .status-grid {{
      display: grid;
      gap: 14px;
    }}
    .stats {{
      grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
      margin-top: 18px;
    }}
    .stat, .status-card {{
      border: 1px solid var(--line);
      border-radius: 16px;
      background: var(--panel-2);
      padding: 16px;
    }}
    .stat strong {{
      display: block;
      font-size: 28px;
      color: var(--accent);
      margin-bottom: 6px;
    }}
    .status-grid {{
      grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
    }}
    .status-head {{
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 12px;
      margin-bottom: 8px;
    }}
    .badge {{
      border-radius: 999px;
      padding: 4px 10px;
      font-size: 12px;
      text-transform: uppercase;
    }}
    .badge.ok {{
      background: rgba(34, 197, 94, 0.16);
      color: #86efac;
    }}
    .badge.missing {{
      background: rgba(239, 68, 68, 0.16);
      color: #fca5a5;
    }}
    pre {{
      margin: 0;
      white-space: pre-wrap;
      color: var(--muted);
      font-size: 12px;
    }}
    table {{
      width: 100%;
      border-collapse: collapse;
    }}
    th, td {{
      text-align: left;
      padding: 12px 10px;
      border-bottom: 1px solid var(--line);
      vertical-align: top;
    }}
    th {{
      color: #cbd5e1;
      font-size: 12px;
      letter-spacing: 0.08em;
      text-transform: uppercase;
    }}
  </style>
</head>
<body>
  <main>
    <header>
      <h1>Daily Overlap Summary</h1>
      <p>{date_label}</p>
      <div class="stats">
        <div class="stat"><strong>{unique_count}</strong>Unique tickers</div>
        <div class="stat"><strong>{len(overlap_two_plus)}</strong>Overlap in 2+ pipelines</div>
        <div class="stat"><strong>{len(overlap_three_plus)}</strong>Overlap in 3+ pipelines</div>
      </div>
    </header>
    <section>
      <h2>Pipeline Status</h2>
      <div class="status-grid">
        {''.join(status_cards)}
      </div>
    </section>
    <section>
      <h2>Top Overlaps</h2>
      <table>
        <thead>
          <tr>
            <th>Ticker</th>
            <th>Count</th>
            <th>Pipelines</th>
            <th>Tags</th>
            <th>Drug</th>
          </tr>
        </thead>
        <tbody>
          {table_rows}
        </tbody>
      </table>
    </section>
  </main>
</body>
</html>
"""
