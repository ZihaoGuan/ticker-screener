from __future__ import annotations

import json
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parent.parent
ETF_CATALOG_PATH = PROJECT_ROOT / "config" / "etf_match_catalog.json"
ETF_THEME_OVERRIDES_PATH = PROJECT_ROOT / "config" / "ticker_etf_theme_overrides.json"


def normalize_match_text(text: str | None) -> str:
    return " ".join((text or "").strip().lower().replace("&", " and ").replace("/", " ").split())


def load_json(path: Path) -> object:
    return json.loads(path.read_text(encoding="utf-8"))


def load_etf_catalog() -> list[dict[str, object]]:
    payload = load_json(ETF_CATALOG_PATH)
    if not isinstance(payload, list):
        raise ValueError("ETF catalog must be a JSON array")
    return [item for item in payload if isinstance(item, dict)]


def load_ticker_theme_overrides() -> dict[str, list[str]]:
    payload = load_json(ETF_THEME_OVERRIDES_PATH)
    if not isinstance(payload, dict):
        raise ValueError("ticker ETF theme overrides must be a JSON object")
    overrides: dict[str, list[str]] = {}
    for ticker, raw_themes in payload.items():
        if not isinstance(ticker, str) or not isinstance(raw_themes, list):
            continue
        overrides[ticker.upper()] = [
            normalize_match_text(str(theme))
            for theme in raw_themes
            if str(theme).strip()
        ]
    return overrides


def _contains_theme(haystack_text: str, theme: str) -> bool:
    if not theme:
        return False
    if len(theme) <= 3 and " " not in theme:
        return theme in haystack_text.split()
    return theme in haystack_text


def infer_theme_tags_for_ticker(
    *,
    ticker: str,
    sector: str | None,
    industry: str | None,
    catalog: list[dict[str, object]],
    overrides: dict[str, list[str]],
) -> list[str]:
    tags: list[str] = list(overrides.get(ticker.upper(), []))
    seen = set(tags)
    haystack = normalize_match_text(f"{sector or ''} {industry or ''}")
    catalog_themes: set[str] = set()
    for item in catalog:
        for theme in item.get("match_themes", []):
            normalized = normalize_match_text(str(theme))
            if normalized:
                catalog_themes.add(normalized)
    for theme in sorted(catalog_themes, key=lambda value: (-len(value), value)):
        if theme in seen:
            continue
        if _contains_theme(haystack, theme):
            tags.append(theme)
            seen.add(theme)
    return tags[:8]


def match_etfs_for_ticker(
    *,
    sector: str | None,
    ticker_themes: list[str],
    catalog: list[dict[str, object]],
) -> list[dict[str, str]]:
    sector_key = normalize_match_text(sector)
    matches: list[dict[str, str]] = []
    seen: set[str] = set()
    for etf in catalog:
        etf_ticker = str(etf.get("ticker", "")).strip().upper()
        if not etf_ticker or etf_ticker in seen:
            continue
        sector_matches = {
            normalize_match_text(str(value))
            for value in etf.get("match_sectors", [])
            if str(value).strip()
        }
        theme_matches = {
            normalize_match_text(str(value))
            for value in etf.get("match_themes", [])
            if str(value).strip()
        }

        reasons: list[str] = []
        if sector_key and sector_key in sector_matches:
            reasons.append(f"sector:{sector}")
        overlapping_themes = [theme for theme in ticker_themes if theme in theme_matches]
        if overlapping_themes:
            reasons.extend(f"theme:{theme}" for theme in overlapping_themes)
        if not reasons:
            continue
        seen.add(etf_ticker)
        matches.append(
            {
                "ticker": etf_ticker,
                "name": str(etf.get("name", "")).strip(),
                "reason": ", ".join(reasons),
            }
        )
    matches.sort(key=lambda item: (item["ticker"], item["name"]))
    return matches
