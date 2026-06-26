from __future__ import annotations

import datetime as dt
from pathlib import Path
from typing import Any

import pandas as pd
import yfinance as yf

from ...config import load_app_config
from ...flashalpha_gex import render_gamma_exposure_report_svgs
from ...market_data_access import load_daily_bars_frame_from_db
from ...market_extension import build_moving_average, compute_extension_frame, resample_to_weekly
from ...rsi_divergence import find_latest_bearish_rsi_divergence_top
from ...td_sequential_screen import find_recent_td_sequential_hit
from ...universe import UniverseTicker
from ..repositories.dashboard_repository import DashboardRepository
from ..repositories.watchlist_repository import WatchlistRepository


class DashboardService:
    def __init__(self, database_url: str, artifacts_dir: Path) -> None:
        self.dashboard_repository = DashboardRepository(database_url=database_url, artifacts_dir=artifacts_dir)
        self.watchlist_repository = WatchlistRepository(artifacts_dir=artifacts_dir)

    def get_dashboard_context(self, *, include_deprecated_watchlists: bool = True) -> dict[str, Any]:
        overview = self.dashboard_repository.get_overview()
        recent_watchlists = self.watchlist_repository.list_recent_watchlists(limit=8, include_deprecated=include_deprecated_watchlists)
        try:
            market_health = self._build_market_health()
        except Exception:
            benchmark = load_app_config().benchmark_ticker.upper()
            market_health = _build_unavailable_market_health(benchmark=benchmark, data_source="unavailable")
        return {
            "overview": overview,
            "market_health": market_health,
            "recent_watchlists": recent_watchlists,
            "strategy_cards": [
                {"id": "rs", "label": "RS", "description": "Daily RS new high before price."},
                {"id": "vcp", "label": "VCP", "description": "Volatility contraction pattern scan."},
                {"id": "cup_handle", "label": "Cup and Handle", "description": "Breakout candidate scan."},
                {"id": "ftd_sweep", "label": "FTD Sweep", "description": "Recent FTD sweep breakout within the lookback window."},
                {"id": "overlap", "label": "Report", "description": "Daily cross-strategy overlap report."},
            ],
        }

    def _build_market_health(self) -> dict[str, Any]:
        benchmark = load_app_config().benchmark_ticker.upper()
        breadth_score = _build_market_breadth_payload(repository=self.dashboard_repository)
        uptrend_score = _build_uptrend_payload(repository=self.dashboard_repository)
        ibd_distribution = _build_ibd_distribution_payload(repository=self.dashboard_repository)
        exposure_posture = _build_exposure_posture_payload(repository=self.dashboard_repository)
        theme_detector = _build_theme_detector_payload(repository=self.dashboard_repository)
        end_date = dt.date.today()
        start_date = end_date - dt.timedelta(days=900)
        try:
            db_frame = load_daily_bars_frame_from_db(benchmark, start_date, end_date, database_url=self.dashboard_repository.database_url)
        except Exception:
            db_frame = None

        db_payload = _build_payload_if_possible(frame=db_frame, ticker=benchmark, data_source="database", repository=self.dashboard_repository)
        if db_payload is not None and not _market_health_payload_has_no_latest(db_payload):
            db_payload["breadth_score"] = breadth_score
            db_payload["uptrend_score"] = uptrend_score
            db_payload["ibd_distribution"] = ibd_distribution
            db_payload["exposure_posture"] = exposure_posture
            db_payload["theme_detector"] = theme_detector
            return db_payload

        internet_frame = _download_history_frame(benchmark, start_date, end_date)
        internet_payload = _build_payload_if_possible(frame=internet_frame, ticker=benchmark, data_source="internet", repository=self.dashboard_repository)
        if internet_payload is not None and not _market_health_payload_has_no_latest(internet_payload):
            internet_payload["breadth_score"] = breadth_score
            internet_payload["uptrend_score"] = uptrend_score
            internet_payload["ibd_distribution"] = ibd_distribution
            internet_payload["exposure_posture"] = exposure_posture
            internet_payload["theme_detector"] = theme_detector
            return internet_payload

        if db_payload is not None and not _market_health_payload_has_no_latest(db_payload):
            db_payload["breadth_score"] = breadth_score
            db_payload["uptrend_score"] = uptrend_score
            db_payload["ibd_distribution"] = ibd_distribution
            db_payload["exposure_posture"] = exposure_posture
            db_payload["theme_detector"] = theme_detector
            return db_payload

        payload = _build_unavailable_market_health(benchmark=benchmark, data_source="unavailable")
        payload["breadth_score"] = breadth_score
        payload["uptrend_score"] = uptrend_score
        payload["ibd_distribution"] = ibd_distribution
        payload["exposure_posture"] = exposure_posture
        payload["theme_detector"] = theme_detector
        return payload


def _build_market_health_payload(*, frame: pd.DataFrame, ticker: str, data_source: str, repository: DashboardRepository) -> dict[str, Any]:
    weekly = resample_to_weekly(frame[["Open", "High", "Low", "Close", "Volume"]])
    regime = _build_regime_payload(frame=frame, weekly=weekly, ticker=ticker, data_source=data_source)
    rsi_divergence = _build_rsi_divergence_payload(frame=frame, ticker=ticker, data_source=data_source)
    bearish_td9 = _build_bearish_td9_payload(frame=frame, ticker=ticker, data_source=data_source)
    options_positioning = _build_options_positioning_payload(repository=repository, ticker=ticker)
    enriched = compute_extension_frame(weekly, length=10, ma_type="sma", warning_pct=11.0, extreme_pct=15.0)
    latest_valid = enriched.dropna(subset=["moving_average", "extension_pct"]).tail(1)
    latest = None
    if not latest_valid.empty:
        row = latest_valid.iloc[0]
        moving_average = float(row["moving_average"])
        latest = {
            "time": latest_valid.index[-1].date().isoformat(),
            "state": str(row["threshold_state"]),
            "close": round(float(row["Close"]), 2),
            "moving_average": round(moving_average, 2),
            "distance": round(float(row["Close"]) - moving_average, 2),
            "extension_pct": round(float(row["extension_pct"]), 2),
        }
    return {
        "regime": regime,
        "rsi_divergence": rsi_divergence,
        "bearish_td9": bearish_td9,
        "options_positioning": options_positioning,
        "spy_extension": {
            "ticker": ticker,
            "label": "10W SMA",
            "timeframe": "weekly",
            "ma_type": "sma",
            "length": 10,
            "warning_pct": 11.0,
            "extreme_pct": 15.0,
            "data_source": data_source,
            "latest": latest,
        },
    }


def _build_payload_if_possible(*, frame: pd.DataFrame | None, ticker: str, data_source: str, repository: DashboardRepository) -> dict[str, Any] | None:
    if frame is None or frame.empty:
        return None
    try:
        return _build_market_health_payload(frame=frame, ticker=ticker, data_source=data_source, repository=repository)
    except Exception:
        return None


def _market_health_payload_has_no_latest(payload: dict[str, Any]) -> bool:
    market_health = payload or {}
    regime_latest = ((market_health.get("regime") or {}).get("latest")) if isinstance(market_health.get("regime"), dict) else None
    rsi_latest = ((market_health.get("rsi_divergence") or {}).get("latest")) if isinstance(market_health.get("rsi_divergence"), dict) else None
    td9_latest = ((market_health.get("bearish_td9") or {}).get("latest")) if isinstance(market_health.get("bearish_td9"), dict) else None
    options_latest = ((market_health.get("options_positioning") or {}).get("latest")) if isinstance(market_health.get("options_positioning"), dict) else None
    extension_latest = ((market_health.get("spy_extension") or {}).get("latest")) if isinstance(market_health.get("spy_extension"), dict) else None
    return regime_latest is None and rsi_latest is None and td9_latest is None and options_latest is None and extension_latest is None


def _build_unavailable_market_health(*, benchmark: str, data_source: str) -> dict[str, Any]:
    return {
        "regime": {
            "ticker": benchmark,
            "data_source": data_source,
            "latest": None,
        },
        "rsi_divergence": {
            "ticker": benchmark,
            "data_source": data_source,
            "latest": None,
        },
        "bearish_td9": {
            "ticker": benchmark,
            "data_source": data_source,
            "latest": None,
        },
        "options_positioning": {
            "ticker": benchmark,
            "data_source": data_source,
            "latest": None,
        },
        "spy_extension": {
            "ticker": benchmark,
            "label": "10W SMA",
            "timeframe": "weekly",
            "ma_type": "sma",
            "length": 10,
            "warning_pct": 11.0,
            "extreme_pct": 15.0,
            "data_source": data_source,
            "latest": None,
        },
        "breadth_score": {
            "ticker": "S&P 500 Breadth",
            "data_source": data_source,
            "latest": None,
        },
        "uptrend_score": {
            "ticker": "Monty Uptrend Ratio",
            "data_source": data_source,
            "latest": None,
        },
        "ibd_distribution": {
            "ticker": "IBD Distribution Day Monitor",
            "data_source": data_source,
            "latest": None,
        },
        "exposure_posture": {
            "ticker": "Exposure Coach",
            "data_source": data_source,
            "latest": None,
        },
        "theme_detector": {
            "ticker": "Theme Detector",
            "data_source": data_source,
            "latest": None,
        },
    }


def _build_market_breadth_payload(*, repository: DashboardRepository) -> dict[str, Any]:
    artifact = repository.get_cached_market_breadth_analysis()
    if not isinstance(artifact, dict):
        return {
            "ticker": "S&P 500 Breadth",
            "data_source": "unavailable",
            "latest": None,
        }

    metadata = artifact.get("metadata") if isinstance(artifact.get("metadata"), dict) else {}
    composite = artifact.get("composite") if isinstance(artifact.get("composite"), dict) else {}
    trend_summary = artifact.get("trend_summary") if isinstance(artifact.get("trend_summary"), dict) else {}
    freshness = metadata.get("data_freshness") if isinstance(metadata.get("data_freshness"), dict) else {}
    strongest = composite.get("strongest_health") if isinstance(composite.get("strongest_health"), dict) else {}
    weakest = composite.get("weakest_health") if isinstance(composite.get("weakest_health"), dict) else {}
    data_quality = composite.get("data_quality") if isinstance(composite.get("data_quality"), dict) else {}
    trend_entries = trend_summary.get("entries") if isinstance(trend_summary.get("entries"), list) else []
    latest_data_date = str(freshness.get("latest_date") or "") or None
    if latest_data_date is None and trend_entries:
        latest_entry = trend_entries[-1] if isinstance(trend_entries[-1], dict) else {}
        latest_data_date = str(latest_entry.get("data_date") or "") or None

    return {
        "ticker": "S&P 500 Breadth",
        "data_source": "artifact-cache",
        "latest": {
            "generated_at": str(metadata.get("generated_at") or "") or None,
            "data_date": latest_data_date,
            "composite_score": _coerce_optional_float(composite.get("composite_score")),
            "zone": str(composite.get("zone") or "") or None,
            "zone_color": str(composite.get("zone_color") or "") or None,
            "exposure_guidance": str(composite.get("exposure_guidance") or "") or None,
            "guidance": str(composite.get("guidance") or "") or None,
            "strongest_label": str(strongest.get("label") or "") or None,
            "strongest_score": _coerce_optional_float(strongest.get("score")),
            "weakest_label": str(weakest.get("label") or "") or None,
            "weakest_score": _coerce_optional_float(weakest.get("score")),
            "trend_direction": str(trend_summary.get("direction") or "") or None,
            "trend_delta": _coerce_optional_float(trend_summary.get("delta")),
            "trend_observations": len(trend_entries),
            "data_quality_label": str(data_quality.get("label") or "") or None,
            "available_components": _coerce_optional_int(data_quality.get("available_count")),
            "total_components": _coerce_optional_int(data_quality.get("total_components")),
            "latest_data_days_old": _coerce_optional_int(freshness.get("days_old")),
            "freshness_warning": str(freshness.get("warning") or "") or None,
            "source_label": str(metadata.get("data_source") or "") or None,
        },
    }


def _build_uptrend_payload(*, repository: DashboardRepository) -> dict[str, Any]:
    artifact = repository.get_cached_uptrend_analysis()
    if not isinstance(artifact, dict):
        return {
            "ticker": "Monty Uptrend Ratio",
            "data_source": "unavailable",
            "latest": None,
        }

    metadata = artifact.get("metadata") if isinstance(artifact.get("metadata"), dict) else {}
    composite = artifact.get("composite") if isinstance(artifact.get("composite"), dict) else {}
    components = artifact.get("components") if isinstance(artifact.get("components"), dict) else {}
    market_breadth = components.get("market_breadth") if isinstance(components.get("market_breadth"), dict) else {}
    participation = components.get("sector_participation") if isinstance(components.get("sector_participation"), dict) else {}
    rotation = components.get("sector_rotation") if isinstance(components.get("sector_rotation"), dict) else {}
    momentum = components.get("momentum") if isinstance(components.get("momentum"), dict) else {}
    historical = components.get("historical_context") if isinstance(components.get("historical_context"), dict) else {}
    confidence = historical.get("confidence") if isinstance(historical.get("confidence"), dict) else {}
    data_quality = composite.get("data_quality") if isinstance(composite.get("data_quality"), dict) else {}
    strongest = composite.get("strongest_component") if isinstance(composite.get("strongest_component"), dict) else {}
    weakest = composite.get("weakest_component") if isinstance(composite.get("weakest_component"), dict) else {}
    latest_data_date = str(metadata.get("latest_data_date") or "") or None
    latest_data_days_old = None
    if latest_data_date:
        try:
            latest_date_value = dt.date.fromisoformat(latest_data_date)
            latest_data_days_old = max(0, (dt.date.today() - latest_date_value).days)
        except ValueError:
            latest_data_days_old = None

    active_warnings = composite.get("active_warnings") if isinstance(composite.get("active_warnings"), list) else []
    warning_labels = [
        str(item.get("label") or "").strip()
        for item in active_warnings
        if isinstance(item, dict) and str(item.get("label") or "").strip()
    ]

    return {
        "ticker": "Monty Uptrend Ratio",
        "data_source": "artifact-cache",
        "latest": {
            "generated_at": str(metadata.get("generated_at") or "") or None,
            "data_date": latest_data_date,
            "composite_score": _coerce_optional_float(composite.get("composite_score")),
            "zone": str(composite.get("zone") or "") or None,
            "zone_detail": str(composite.get("zone_detail") or composite.get("zone") or "") or None,
            "zone_color": str(composite.get("zone_color") or "") or None,
            "exposure_guidance": str(composite.get("exposure_guidance") or "") or None,
            "guidance": str(composite.get("guidance") or "") or None,
            "warning_penalty": _coerce_optional_int(composite.get("warning_penalty")),
            "warning_labels": warning_labels,
            "strongest_label": str(strongest.get("label") or "") or None,
            "strongest_score": _coerce_optional_float(strongest.get("score")),
            "weakest_label": str(weakest.get("label") or "") or None,
            "weakest_score": _coerce_optional_float(weakest.get("score")),
            "ratio_pct": _coerce_optional_float(market_breadth.get("ratio_pct")),
            "trend_direction": str(market_breadth.get("trend") or "") or None,
            "slope_smoothed": _coerce_optional_float(momentum.get("slope_smoothed")),
            "acceleration_label": str(momentum.get("acceleration_label") or "") or None,
            "sector_uptrend_count": _coerce_optional_int(participation.get("uptrend_count")),
            "sector_total": _coerce_optional_int(participation.get("total_sectors")),
            "cyclical_minus_defensive_pct": _coerce_optional_float(rotation.get("difference_pct")),
            "historical_percentile": _coerce_optional_float(historical.get("percentile")),
            "confidence_label": str(confidence.get("confidence_level") or "") or None,
            "data_quality_label": str(data_quality.get("label") or "") or None,
            "available_components": _coerce_optional_int(data_quality.get("available_count")),
            "total_components": _coerce_optional_int(data_quality.get("total_components")),
            "latest_data_days_old": latest_data_days_old,
        },
    }


def _build_ibd_distribution_payload(*, repository: DashboardRepository) -> dict[str, Any]:
    artifact = repository.get_cached_ibd_distribution_day_monitor()
    if not isinstance(artifact, dict):
        return {
            "ticker": "IBD Distribution Day Monitor",
            "data_source": "unavailable",
            "latest": None,
        }

    state = artifact.get("market_distribution_state") if isinstance(artifact.get("market_distribution_state"), dict) else {}
    portfolio_action = artifact.get("portfolio_action") if isinstance(artifact.get("portfolio_action"), dict) else {}
    audit = artifact.get("audit") if isinstance(artifact.get("audit"), dict) else {}
    index_results = state.get("index_results") if isinstance(state.get("index_results"), list) else []
    qqq_result = _find_ibd_index_result(index_results, "QQQ")
    spy_result = _find_ibd_index_result(index_results, "SPY")
    primary_symbol = str(state.get("primary_signal_symbol") or "") or None
    primary_result = _find_ibd_index_result(index_results, primary_symbol) if primary_symbol else None
    if primary_result is None and index_results:
        primary_result = index_results[0] if isinstance(index_results[0], dict) else None
    latest_as_of = str(state.get("as_of") or "") or None
    latest_data_days_old = None
    if latest_as_of:
        try:
            latest_date_value = dt.date.fromisoformat(latest_as_of)
            latest_data_days_old = max(0, (dt.date.today() - latest_date_value).days)
        except ValueError:
            latest_data_days_old = None

    return {
        "ticker": "IBD Distribution Day Monitor",
        "data_source": "artifact-cache",
        "latest": {
            "generated_at": str(state.get("generated_at") or audit.get("generated_at") or "") or None,
            "as_of": latest_as_of,
            "overall_risk_level": str(state.get("overall_risk_level") or "") or None,
            "primary_signal_symbol": primary_symbol,
            "qqq_d5_count": _coerce_optional_int((qqq_result or {}).get("d5_count")),
            "qqq_d15_count": _coerce_optional_int((qqq_result or {}).get("d15_count")),
            "qqq_d25_count": _coerce_optional_int((qqq_result or {}).get("d25_count")),
            "spy_d5_count": _coerce_optional_int((spy_result or {}).get("d5_count")),
            "spy_d15_count": _coerce_optional_int((spy_result or {}).get("d15_count")),
            "spy_d25_count": _coerce_optional_int((spy_result or {}).get("d25_count")),
            "primary_is_distribution_day_today": _coerce_optional_bool((primary_result or {}).get("is_distribution_day_today")),
            "market_below_21ema_or_50ma": _coerce_optional_bool(((primary_result or {}).get("trend_filters") or {}).get("market_below_21ema_or_50ma")),
            "recommended_action": str(portfolio_action.get("recommended_action") or "") or None,
            "target_exposure_pct": _coerce_optional_int(portfolio_action.get("target_exposure_pct")),
            "current_exposure_pct": _coerce_optional_int(portfolio_action.get("current_exposure_pct")),
            "trailing_stop_pct": _coerce_optional_int(portfolio_action.get("trailing_stop_pct")),
            "alternative_action": str(portfolio_action.get("alternative_action") or "") or None,
            "rationale": str(portfolio_action.get("rationale") or "") or None,
            "audit_flags": [str(flag) for flag in audit.get("audit_flags") or [] if str(flag)],
            "latest_data_days_old": latest_data_days_old,
        },
    }


def _build_exposure_posture_payload(*, repository: DashboardRepository) -> dict[str, Any]:
    artifact = repository.get_cached_exposure_posture()
    if not isinstance(artifact, dict):
        return {
            "ticker": "Exposure Coach",
            "data_source": "unavailable",
            "latest": None,
        }

    generated_at = str(artifact.get("generated_at") or "") or None
    latest_data_days_old = None
    if generated_at:
        generated_date = generated_at[:10]
        try:
            latest_date_value = dt.date.fromisoformat(generated_date)
            latest_data_days_old = max(0, (dt.date.today() - latest_date_value).days)
        except ValueError:
            latest_data_days_old = None
    inputs_provided = artifact.get("inputs_provided") if isinstance(artifact.get("inputs_provided"), list) else []
    inputs_missing = artifact.get("inputs_missing") if isinstance(artifact.get("inputs_missing"), list) else []
    component_scores = artifact.get("component_scores") if isinstance(artifact.get("component_scores"), dict) else {}

    return {
        "ticker": "Exposure Coach",
        "data_source": "artifact-cache",
        "latest": {
            "generated_at": generated_at,
            "exposure_ceiling_pct": _coerce_optional_int(artifact.get("exposure_ceiling_pct")),
            "bias": str(artifact.get("bias") or "") or None,
            "participation": str(artifact.get("participation") or "") or None,
            "recommendation": str(artifact.get("recommendation") or "") or None,
            "confidence": str(artifact.get("confidence") or "") or None,
            "composite_score": _coerce_optional_float(artifact.get("composite_score")),
            "breadth_score": _coerce_optional_int(component_scores.get("breadth_score")),
            "uptrend_score": _coerce_optional_int(component_scores.get("uptrend_score")),
            "top_risk_score": _coerce_optional_int(component_scores.get("top_risk_score")),
            "inputs_provided": [str(item) for item in inputs_provided if str(item)],
            "inputs_missing": [str(item) for item in inputs_missing if str(item)],
            "provided_count": len(inputs_provided),
            "missing_count": len(inputs_missing),
            "rationale": str(artifact.get("rationale") or "") or None,
            "latest_data_days_old": latest_data_days_old,
        },
    }


def _build_theme_detector_payload(*, repository: DashboardRepository) -> dict[str, Any]:
    artifact = repository.get_cached_theme_detector()
    if not isinstance(artifact, dict):
        return {
            "ticker": "Theme Detector",
            "data_source": "unavailable",
            "latest": None,
        }

    metadata = artifact.get("metadata") if isinstance(artifact.get("metadata"), dict) else {}
    summary = artifact.get("summary") if isinstance(artifact.get("summary"), dict) else {}
    themes = artifact.get("themes") if isinstance(artifact.get("themes"), dict) else {}
    bullish = themes.get("bullish") if isinstance(themes.get("bullish"), list) else []
    bearish = themes.get("bearish") if isinstance(themes.get("bearish"), list) else []

    top_bullish = bullish[0] if bullish and isinstance(bullish[0], dict) else {}
    top_bearish = bearish[0] if bearish and isinstance(bearish[0], dict) else {}
    generated_at = str(artifact.get("generated_at") or metadata.get("generated_at") or "") or None
    latest_data_days_old = None
    if generated_at:
        generated_date = generated_at[:10]
        try:
            latest_date_value = dt.date.fromisoformat(generated_date)
            latest_data_days_old = max(0, (dt.date.today() - latest_date_value).days)
        except ValueError:
            latest_data_days_old = None

    all_themes = themes.get("all") if isinstance(themes.get("all"), list) else []
    top_theme_names = [
        str(item.get("name") or "").strip()
        for item in all_themes[:3]
        if isinstance(item, dict) and str(item.get("name") or "").strip()
    ]

    return {
        "ticker": "Theme Detector",
        "data_source": "artifact-cache",
        "latest": {
            "generated_at": generated_at,
            "data_mode": str(metadata.get("data_mode") or "") or None,
            "finviz_mode": str(metadata.get("finviz_mode") or "") or None,
            "total_themes": _coerce_optional_int(summary.get("total_themes")),
            "bullish_count": _coerce_optional_int(summary.get("bullish_count")),
            "bearish_count": _coerce_optional_int(summary.get("bearish_count")),
            "top_theme_names": top_theme_names,
            "top_bullish_name": str(top_bullish.get("name") or summary.get("top_bullish") or "") or None,
            "top_bullish_heat": _coerce_optional_float(top_bullish.get("heat")),
            "top_bullish_stage": str(top_bullish.get("stage") or "") or None,
            "top_bullish_confidence": str(top_bullish.get("confidence") or "") or None,
            "top_bearish_name": str(top_bearish.get("name") or summary.get("top_bearish") or "") or None,
            "top_bearish_heat": _coerce_optional_float(top_bearish.get("heat")),
            "top_bearish_stage": str(top_bearish.get("stage") or "") or None,
            "top_bearish_confidence": str(top_bearish.get("confidence") or "") or None,
            "uptrend_sectors": _coerce_optional_int((metadata.get("data_sources") or {}).get("uptrend_sectors"))
            if isinstance(metadata.get("data_sources"), dict)
            else None,
            "latest_data_days_old": latest_data_days_old,
        },
    }


def _download_history_frame(ticker: str, start_date: dt.date, end_date: dt.date) -> pd.DataFrame | None:
    try:
        history = yf.download(
            tickers=ticker,
            start=start_date.isoformat(),
            end=(end_date + dt.timedelta(days=1)).isoformat(),
            interval="1d",
            auto_adjust=False,
            progress=False,
            threads=False,
        )
    except Exception:
        return None
    if history is None or history.empty:
        return None
    frame = history.copy()
    if isinstance(frame.columns, pd.MultiIndex):
        frame.columns = frame.columns.get_level_values(0)
    frame = frame.rename(columns=str)
    for column in ("Open", "High", "Low", "Close", "Volume"):
        if column in frame.columns:
            frame[column] = pd.to_numeric(frame[column], errors="coerce")
    frame = frame.dropna(subset=["Open", "High", "Low", "Close", "Volume"]).sort_index()
    return frame if not frame.empty else None


def _build_regime_payload(*, frame: pd.DataFrame, weekly: pd.DataFrame, ticker: str, data_source: str) -> dict[str, Any]:
    daily_ema21 = build_moving_average(frame["Close"], length=21, ma_type="ema")
    weekly_ema21 = build_moving_average(weekly["Close"], length=21, ma_type="ema")

    latest_daily = frame.dropna(subset=["Close"]).tail(1)
    latest_weekly = weekly.dropna(subset=["Close"]).tail(1)
    latest_daily_ema = daily_ema21.dropna().tail(1)
    latest_weekly_ema = weekly_ema21.dropna().tail(1)

    if latest_daily.empty or latest_weekly.empty or latest_daily_ema.empty or latest_weekly_ema.empty:
        return {"ticker": ticker, "data_source": data_source, "latest": None}

    daily_close = float(latest_daily["Close"].iloc[0])
    weekly_close = float(latest_weekly["Close"].iloc[0])
    daily_ema = float(latest_daily_ema.iloc[0])
    weekly_ema = float(latest_weekly_ema.iloc[0])

    weekly_uptrend = weekly_close > weekly_ema
    daily_downtrend = daily_close < daily_ema

    if weekly_uptrend and daily_downtrend:
        regime = "healthy_chaos"
        regime_label = "Healthy Chaos"
        summary = "Weekly uptrend, daily reset"
        explanation = (
            "Macro trend still up. Daily weakness is a normal exhale inside the weekly 21EMA uptrend and often "
            "becomes the buy-the-dip zone."
        )
    elif weekly_uptrend and not daily_downtrend:
        regime = "perfect_convergence_bull"
        regime_label = "Perfect Convergence (Bull Market)"
        summary = "Trend and short-term action aligned"
        explanation = "Price is above both weekly and daily 21EMA. Trend is your friend here: ride, hold, or add carefully."
    elif (not weekly_uptrend) and daily_downtrend:
        regime = "perfect_convergence_bear"
        regime_label = "Perfect Convergence (Bear Market)"
        summary = "Maximum chaos"
        explanation = "Price is below both weekly and daily 21EMA. Structural trend weak, short-term action weak, capital preservation first."
    else:
        regime = "bear_market_rally"
        regime_label = "Bear Market Rally"
        summary = "Short-term euphoria in structural downtrend"
        explanation = (
            "Daily reclaimed 21EMA, but weekly trend is still below its 21EMA anchor. Treat strength as a bounce attempt "
            "until macro trend improves."
        )

    return {
        "ticker": ticker,
        "data_source": data_source,
        "latest": {
            "date": latest_daily.index[-1].date().isoformat(),
            "weekly_bar_date": latest_weekly.index[-1].date().isoformat(),
            "daily_close": round(daily_close, 2),
            "daily_ema21": round(daily_ema, 2),
            "weekly_close": round(weekly_close, 2),
            "weekly_ema21": round(weekly_ema, 2),
            "weekly_uptrend": weekly_uptrend,
            "daily_downtrend": daily_downtrend,
            "regime": regime,
            "regime_label": regime_label,
            "summary": summary,
            "explanation": explanation,
            "daily_distance_pct": round(((daily_close / daily_ema) - 1.0) * 100.0, 2) if daily_ema else None,
            "weekly_distance_pct": round(((weekly_close / weekly_ema) - 1.0) * 100.0, 2) if weekly_ema else None,
        },
    }


def _build_rsi_divergence_payload(*, frame: pd.DataFrame, ticker: str, data_source: str) -> dict[str, Any]:
    signal = find_latest_bearish_rsi_divergence_top(frame)
    if signal is None:
        return {"ticker": ticker, "data_source": data_source, "latest": None}

    explanation = "Daily RSI divergence top from Charles Edwards style pivot logic, with fresh, active, lifted, and invalidated states."
    return {
        "ticker": ticker,
        "data_source": data_source,
        "latest": {
            **signal.to_dict(),
            "explanation": explanation,
        },
    }


def _build_bearish_td9_payload(*, frame: pd.DataFrame, ticker: str, data_source: str) -> dict[str, Any]:
    signal = find_recent_td_sequential_hit(frame, ticker=UniverseTicker(symbol=ticker), direction="bearish")
    if signal is None:
        return {"ticker": ticker, "data_source": data_source, "latest": None}

    latest_close = float(frame["Close"].dropna().iloc[-1]) if "Close" in frame.columns and not frame["Close"].dropna().empty else None
    distance_pct = None
    if latest_close and signal.comparison_close:
        distance_pct = round(((latest_close / signal.comparison_close) - 1.0) * 100.0, 2)

    return {
        "ticker": ticker,
        "data_source": data_source,
        "latest": {
            **signal.to_dict(),
            "label": "Bearish TD9",
            "explanation": "Nine straight closes above the close four bars earlier. Often marks short-term upside exhaustion.",
            "distance_from_compare_pct": distance_pct,
        },
    }


def _build_options_positioning_payload(*, repository: DashboardRepository, ticker: str) -> dict[str, Any]:
    options_ticker = "SPX"
    cached_plot = repository.get_cached_gamma_exposure_plot(symbol=options_ticker)
    if isinstance(cached_plot, dict):
        cached_plots = _coerce_cached_gex_plots(cached_plot)
        return {
            "ticker": str(cached_plot.get("symbol") or options_ticker).strip().upper(),
            "data_source": "artifact-cache",
            "latest": {
                "as_of": str(cached_plot.get("as_of") or ""),
                "spot": _to_number(cached_plot.get("underlying_price")),
                "net_gex": _to_number(cached_plot.get("net_gex")),
                "gex_regime": "negative" if (_to_number(cached_plot.get("net_gex")) or 0.0) < 0 else "positive",
                "gex_label": "Negative Gamma" if (_to_number(cached_plot.get("net_gex")) or 0.0) < 0 else "Positive Gamma",
                "gamma_flip": _to_number(cached_plot.get("gamma_flip")),
                "distance_to_flip_pct": _distance_to_flip_pct(
                    spot=_to_number(cached_plot.get("underlying_price")),
                    gamma_flip=_to_number(cached_plot.get("gamma_flip")),
                ),
                "call_wall": _to_number(cached_plot.get("call_wall")),
                "put_wall": _to_number(cached_plot.get("put_wall")),
                "atm_pin_strike": _to_number(cached_plot.get("atm_pin_strike")),
                "put_call_oi_ratio": _to_number(cached_plot.get("put_call_oi_ratio")),
                "strike_count": _to_number(cached_plot.get("strike_count")),
                "implied_move_pct": None,
                "front_expiry": str(cached_plot.get("next_expiry") or ""),
                "summary": str(cached_plot.get("summary") or ""),
                "methodology": str(cached_plot.get("methodology") or ""),
                "source_symbol": str(cached_plot.get("source_symbol") or ""),
                "source_url": str(cached_plot.get("source_url") or ""),
                "next_expiry": str(cached_plot.get("next_expiry") or ""),
                "next_monthly_expiry": str(cached_plot.get("next_monthly_expiry") or ""),
                "plots": cached_plots,
            },
        }

    summary = repository.get_latest_screen_run_summary(strategy_id="flashalpha_gex_close", preferred_ticker=options_ticker)
    if not isinstance(summary, dict):
        return {
            "ticker": options_ticker,
            "data_source": "unavailable",
            "latest": None,
        }
    return {
        "ticker": str(summary.get("ticker") or options_ticker).strip().upper(),
        "data_source": "database",
        "latest": {
            "as_of": str(summary.get("api_as_of") or summary.get("as_of_date") or ""),
            "spot": _to_number(summary.get("spot")),
            "net_gex": _to_number(summary.get("net_gex")),
            "gex_regime": "negative" if str(summary.get("gex_regime") or "").strip().lower() == "negative" else "positive",
            "gex_label": str(summary.get("gex_label") or "Unavailable"),
            "gamma_flip": _to_number(summary.get("gamma_flip")),
            "distance_to_flip_pct": _to_number(summary.get("distance_to_flip_pct")),
            "call_wall": _to_number(summary.get("call_wall")),
            "put_wall": _to_number(summary.get("put_wall")),
            "atm_pin_strike": _to_number(summary.get("atm_pin_strike")),
            "put_call_oi_ratio": _to_number(summary.get("put_call_oi_ratio")),
            "strike_count": _to_number(summary.get("strike_count")),
            "implied_move_pct": None,
            "front_expiry": str(summary.get("front_expiry") or ""),
            "summary": str(summary.get("summary") or ""),
            "methodology": str(summary.get("methodology") or ""),
            "source_symbol": "",
            "source_url": "",
            "next_expiry": str(summary.get("front_expiry") or ""),
            "next_monthly_expiry": "",
            "plots": None,
        },
    }


def _coerce_cached_gex_plots(cached_plot: dict[str, Any]) -> dict[str, str] | None:
    raw_plots = cached_plot.get("plots")
    if not isinstance(raw_plots, dict) or not raw_plots:
        if isinstance(cached_plot.get("strikes"), list):
            raw_plots = render_gamma_exposure_report_svgs(cached_plot)
        else:
            return None
    return {
        "absolute": str(raw_plots.get("absolute") or ""),
        "by_option_type": str(raw_plots.get("by_option_type") or ""),
        "profile": str(raw_plots.get("profile") or ""),
        "v2": str(raw_plots.get("v2") or ""),
    }


def _to_number(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _coerce_optional_float(value: Any) -> float | None:
    return _to_number(value)


def _coerce_optional_int(value: Any) -> int | None:
    if value in (None, ""):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _coerce_optional_bool(value: Any) -> bool | None:
    if value in (None, ""):
        return None
    if isinstance(value, bool):
        return value
    return None


def _find_ibd_index_result(index_results: list[Any], symbol: str | None) -> dict[str, Any] | None:
    normalized_symbol = str(symbol or "").strip().upper()
    if not normalized_symbol:
        return None
    for item in index_results:
        if not isinstance(item, dict):
            continue
        if str(item.get("symbol") or "").strip().upper() == normalized_symbol:
            return item
    return None


def _distance_to_flip_pct(*, spot: float | None, gamma_flip: float | None) -> float | None:
    if spot in (None, 0.0) or gamma_flip in (None, 0.0):
        return None
    return round(((spot / gamma_flip) - 1.0) * 100.0, 2)
