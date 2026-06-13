from __future__ import annotations

import json
import os
from pathlib import Path
import subprocess

from .models import FinvizProbeResult

PROJECT_ROOT = Path(__file__).resolve().parents[2]
_PROBE_SCRIPT = PROJECT_ROOT / "frontend" / "scripts" / "probe_finviz_playwright.mjs"


class FinvizProbeError(RuntimeError):
    pass


def probe_finviz_ticker(
    ticker: str,
    *,
    timeout_seconds: int = 45,
    env: dict[str, str] | None = None,
) -> FinvizProbeResult:
    normalized = str(ticker or "").strip().upper()
    if not normalized:
        raise FinvizProbeError("Missing ticker.")
    if not _PROBE_SCRIPT.exists():
        raise FinvizProbeError(f"Missing probe script: {_PROBE_SCRIPT}")

    command = ["node", str(_PROBE_SCRIPT), normalized]
    process_env = os.environ.copy()
    if env:
        process_env.update(env)
    result = subprocess.run(
        command,
        cwd=str(PROJECT_ROOT),
        capture_output=True,
        text=True,
        timeout=timeout_seconds,
        check=False,
        env=process_env,
    )
    if result.returncode != 0:
        stderr = (result.stderr or "").strip()
        raise FinvizProbeError(stderr or f"Finviz probe exited with code {result.returncode}")
    stdout = (result.stdout or "").strip()
    try:
        payload = json.loads(stdout)
    except json.JSONDecodeError as exc:
        raise FinvizProbeError(f"Finviz probe returned invalid JSON: {exc}") from exc
    header = payload.get("company_header") if isinstance(payload.get("company_header"), dict) else {}
    metric_pairs_raw = payload.get("metric_pairs")
    metric_pairs: list[tuple[str, str]] = []
    if isinstance(metric_pairs_raw, list):
        for item in metric_pairs_raw:
            if isinstance(item, (list, tuple)) and len(item) >= 2:
                metric_pairs.append((str(item[0]).strip(), str(item[1]).strip()))
    return FinvizProbeResult(
        ticker=normalized,
        source_url=str(payload.get("url") or ""),
        status_code=int(payload["status"]) if payload.get("status") is not None else None,
        final_url=str(payload.get("final_url") or ""),
        title=str(payload.get("title") or ""),
        body_excerpt=str(payload.get("body_excerpt") or ""),
        company_name=str(header.get("company_name") or "") or None,
        sector=str(header.get("sector") or "") or None,
        industry=str(header.get("industry") or "") or None,
        country=str(header.get("country") or "") or None,
        market_cap_class=str(header.get("market_cap_class") or "") or None,
        exchange=str(header.get("exchange") or "") or None,
        metric_pairs=tuple(metric_pairs),
    )


def looks_blocked(probe: FinvizProbeResult) -> bool:
    text = "\n".join((probe.title, probe.body_excerpt)).lower()
    markers = (
        "captcha",
        "verify you are human",
        "access denied",
        "too many requests",
        "unusual traffic",
    )
    return any(marker in text for marker in markers)


def looks_retryable_failure(error_text: str) -> bool:
    lowered = str(error_text or "").lower()
    return any(
        marker in lowered
        for marker in (
            "timeout",
            "timed out",
            "navigation",
            "closed",
            "econnreset",
            "socket hang up",
            "target page, context or browser has been closed",
        )
    )
