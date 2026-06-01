from __future__ import annotations

import datetime as dt

from fastapi import APIRouter, Body, Depends, HTTPException, Query, Request, Response
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse

from src.webapp.access_control import Principal
from src.webapp.services.admin_service import AdminService
from src.webapp.services.audit_service import AuditService
from src.webapp.services.backtest_service import BacktestService, DEFAULT_EXIT_RULES
from src.webapp.services.auth_service import AuthService, UserAdminService
from src.webapp.services.dashboard_service import DashboardService
from src.webapp.services.overlap_service import OverlapService
from src.webapp.services.rrg_service import RrgService
from src.webapp.services.ad_hoc_screen_service import AdHocScreenService
from src.webapp.services.run_service import RunService
from src.webapp.services.screener_history_service import ScreenerHistoryService
from src.webapp.services.watchlist_service import WatchlistService
from web.dependencies import (
    get_ad_hoc_screen_service,
    get_admin_service,
    clear_auth_cookie,
    config,
    get_audit_service,
    get_backtest_service,
    get_auth_service,
    get_current_principal,
    get_dashboard_service,
    get_overlap_service,
    get_rrg_service,
    get_run_service,
    get_screener_history_service,
    get_user_admin_service,
    get_watchlist_service,
    require_manage_exclusions,
    require_manage_users,
    require_run_backtests,
    require_run_screeners,
    require_sync_history,
    set_auth_cookie,
)


router = APIRouter(prefix="/api", tags=["api"])


def _record_audit(
    *,
    audit_service: AuditService,
    principal: Principal | None,
    request: Request | None,
    action: str,
    resource_type: str,
    resource_id: str = "",
    resource_label: str = "",
    message: str = "",
    metadata: dict[str, object] | None = None,
    actor_email_override: str = "",
    actor_role_override: str = "",
) -> None:
    if not audit_service.is_configured():
        return
    audit_service.record_event(
        principal=principal,
        request=request,
        action=action,
        resource_type=resource_type,
        resource_id=resource_id,
        resource_label=resource_label,
        message=message,
        metadata=metadata or {},
        actor_email_override=actor_email_override,
        actor_role_override=actor_role_override,
    )


@router.get("/dashboard", response_class=JSONResponse)
def dashboard_data(service: DashboardService = Depends(get_dashboard_service)) -> JSONResponse:
    return JSONResponse(service.get_dashboard_context())


@router.get("/auth/me", response_class=JSONResponse)
def auth_me(principal: Principal = Depends(get_current_principal)) -> JSONResponse:
    return JSONResponse(jsonable_encoder({"authenticated": principal.authenticated, "user": principal.to_dict() if principal.authenticated else None, "role": principal.role, "capabilities": list(principal.capabilities)}))


@router.post("/auth/request-link", response_class=JSONResponse)
def request_magic_link(
    request: Request,
    payload: dict[str, object] | None = Body(default=None),
    service: AuthService = Depends(get_auth_service),
) -> JSONResponse:
    request_payload = payload or {}
    try:
        result = service.request_magic_link(
            email=str(request_payload.get("email") or ""),
            request_ip=request.client.host if request.client else "",
            request_user_agent=request.headers.get("user-agent", ""),
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return JSONResponse(jsonable_encoder(result))


@router.post("/auth/request-premium", response_class=JSONResponse)
def request_premium_access(
    request: Request,
    payload: dict[str, object] | None = Body(default=None),
    service: AuthService = Depends(get_auth_service),
    audit_service: AuditService = Depends(get_audit_service),
) -> JSONResponse:
    request_payload = payload or {}
    try:
        result = service.request_premium_access(email=str(request_payload.get("email") or ""))
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    _record_audit(
        audit_service=audit_service,
        principal=None,
        request=request,
        action="auth.request_premium",
        resource_type="access_request",
        resource_id=str(result.get("email") or ""),
        resource_label=str(result.get("email") or ""),
        message=str(result.get("message") or "Premium access request submitted."),
        metadata={"requested_role": "premium", "request_status": str(result.get("status") or "pending")},
        actor_email_override=str(result.get("email") or ""),
        actor_role_override="visitor",
    )
    return JSONResponse(jsonable_encoder(result))


@router.post("/auth/verify-link", response_class=JSONResponse)
def verify_magic_link(
    request: Request,
    response: Response,
    payload: dict[str, object] | None = Body(default=None),
    service: AuthService = Depends(get_auth_service),
) -> JSONResponse:
    request_payload = payload or {}
    try:
        result = service.verify_magic_link(
            token=str(request_payload.get("token") or ""),
            request_ip=request.client.host if request.client else "",
            request_user_agent=request.headers.get("user-agent", ""),
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    json_response = JSONResponse(
        jsonable_encoder(
            {
                "ok": True,
                "authenticated": True,
                "user": result["principal"],
                "role": result["principal"]["role"],
                "capabilities": result["principal"]["capabilities"],
            }
        )
    )
    set_auth_cookie(json_response, str(result["session_cookie_value"]))
    return json_response


@router.post("/auth/logout", response_class=JSONResponse)
def logout(
    request: Request,
    service: AuthService = Depends(get_auth_service),
) -> JSONResponse:
    service.logout(signed_session=request.cookies.get(config.auth_session_cookie_name))
    response = JSONResponse({"ok": True})
    clear_auth_cookie(response)
    return response


@router.get("/jobs", response_class=JSONResponse)
def jobs_data(
    service: RunService = Depends(get_run_service),
    _: Principal = Depends(require_run_screeners),
) -> JSONResponse:
    return JSONResponse({"actions": service.list_actions(), "jobs": service.list_jobs()})


@router.get("/jobs/{job_id}", response_class=JSONResponse)
def job_detail(job_id: str, service: RunService = Depends(get_run_service), _: Principal = Depends(require_run_screeners)) -> JSONResponse:
    try:
        return JSONResponse(service.get_job(job_id))
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.post("/jobs/{job_id}/cancel", response_class=JSONResponse)
def cancel_job(
    request: Request,
    job_id: str,
    service: RunService = Depends(get_run_service),
    principal: Principal = Depends(require_run_screeners),
    audit_service: AuditService = Depends(get_audit_service),
) -> JSONResponse:
    try:
        job = service.cancel(job_id)
    except ValueError as exc:
        message = str(exc)
        status_code = 404 if message.startswith("Unknown job") else 400
        raise HTTPException(status_code=status_code, detail=message) from exc
    _record_audit(
        audit_service=audit_service,
        principal=principal,
        request=request,
        action="runs.cancel",
        resource_type="run_job",
        resource_id=job_id,
        resource_label=str(job.get("label") or job_id),
        message=f"Requested cancellation for job {job_id}.",
        metadata={"job_id": job_id, "action_id": job.get("action_id")},
    )
    return JSONResponse({"ok": True, "job": job})


@router.post("/runs/{action_id}", response_class=JSONResponse)
def run_action(
    request: Request,
    action_id: str,
    payload: dict[str, object] | None = Body(default=None),
    service: RunService = Depends(get_run_service),
    principal: Principal = Depends(require_run_screeners),
    audit_service: AuditService = Depends(get_audit_service),
) -> JSONResponse:
    try:
        job_id = service.launch(action_id, options=payload or {})
    except ValueError as exc:
        status_code = 404 if str(exc).startswith("Unknown run action") else 400
        raise HTTPException(status_code=status_code, detail=str(exc)) from exc
    _record_audit(
        audit_service=audit_service,
        principal=principal,
        request=request,
        action="runs.launch",
        resource_type="run_job",
        resource_id=job_id,
        resource_label=action_id,
        message=f"Queued screener run {action_id}.",
        metadata={"action_id": action_id, "job_id": job_id, "options": payload or {}},
    )
    return JSONResponse({"ok": True, "job_id": job_id})


@router.post("/ad-hoc-screen", response_class=JSONResponse)
def ad_hoc_screen(
    payload: dict[str, object] | None = Body(default=None),
    service: AdHocScreenService = Depends(get_ad_hoc_screen_service),
) -> JSONResponse:
    request_payload = payload or {}
    ticker = str(request_payload.get("ticker") or "").strip()
    as_of_date_raw = str(request_payload.get("as_of_date") or "").strip()
    screener_ids_raw = request_payload.get("screeners")
    if not ticker:
        raise HTTPException(status_code=400, detail="ticker is required")
    if not as_of_date_raw:
        raise HTTPException(status_code=400, detail="as_of_date is required")
    try:
        as_of_date = dt.date.fromisoformat(as_of_date_raw)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail="as_of_date must be YYYY-MM-DD") from exc
    if not isinstance(screener_ids_raw, list) or not screener_ids_raw:
        raise HTTPException(status_code=400, detail="screeners must be a non-empty array")
    screener_ids = [str(item).strip() for item in screener_ids_raw if str(item).strip()]
    try:
        result = service.run(ticker=ticker, as_of_date=as_of_date, screener_ids=screener_ids)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return JSONResponse(result)


@router.get("/screener-runs", response_class=JSONResponse)
def screener_runs_data(
    strategy_id: str = Query(default="", alias="strategyId"),
    from_date: str = Query(default="", alias="from"),
    to_date: str = Query(default="", alias="to"),
    include_deleted: bool = Query(default=False, alias="includeDeleted"),
    config_hash: str = Query(default="", alias="configHash"),
    has_hits_raw: str = Query(default="", alias="hasHits"),
    limit: int = Query(default=50, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
    service: ScreenerHistoryService = Depends(get_screener_history_service),
    run_service: RunService = Depends(get_run_service),
) -> JSONResponse:
    start_date = dt.date.fromisoformat(from_date) if from_date else None
    end_date = dt.date.fromisoformat(to_date) if to_date else None
    has_hits = None
    if has_hits_raw.strip().lower() in {"true", "1", "yes"}:
        has_hits = True
    elif has_hits_raw.strip().lower() in {"false", "0", "no"}:
        has_hits = False
    payload = {
        "configured": service.is_configured(),
        "runs": service.list_runs(
            strategy_id=strategy_id,
            start_date=start_date,
            end_date=end_date,
            include_deleted=include_deleted,
            config_hash=config_hash,
            has_hits=has_hits,
            limit=limit,
            offset=offset,
        ),
        "coverage": service.list_signal_cache_summary(start_date=start_date, end_date=end_date),
        "available_strategies": [{"id": item["id"], "label": item["label"]} for item in run_service.list_actions()],
    }
    return JSONResponse(jsonable_encoder(payload))


@router.get("/screener-runs/cache-calendar", response_class=JSONResponse)
def screener_runs_cache_calendar(
    from_date: str = Query(alias="from"),
    to_date: str = Query(alias="to"),
    strategy_ids_raw: list[str] | None = Query(default=None, alias="strategyIds"),
    include_deleted: bool = Query(default=False, alias="includeDeleted"),
    service: ScreenerHistoryService = Depends(get_screener_history_service),
) -> JSONResponse:
    try:
        start_date = dt.date.fromisoformat(from_date)
        end_date = dt.date.fromisoformat(to_date)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail="from and to must be YYYY-MM-DD") from exc
    if end_date < start_date:
        raise HTTPException(status_code=400, detail="to must be on or after from")
    strategy_ids: list[str] = []
    for item in strategy_ids_raw or []:
        for part in str(item).split(","):
            normalized = part.strip()
            if normalized and normalized not in strategy_ids:
                strategy_ids.append(normalized)
    days = service.list_signal_cache_calendar(
        strategy_ids=strategy_ids or None,
        start_date=start_date,
        end_date=end_date,
        include_deleted=include_deleted,
    )
    return JSONResponse(
        jsonable_encoder(
            {
                "from": start_date.isoformat(),
                "to": end_date.isoformat(),
                "strategy_ids": strategy_ids,
                "include_deleted": include_deleted,
                "days": days,
            }
        )
    )


@router.get("/screener-runs/{run_id}", response_class=JSONResponse)
def screener_run_detail(
    run_id: int,
    include_hits: bool = Query(default=True, alias="includeHits"),
    hit_limit: int = Query(default=200, alias="hitLimit", ge=1, le=1000),
    hit_offset: int = Query(default=0, alias="hitOffset", ge=0),
    service: ScreenerHistoryService = Depends(get_screener_history_service),
) -> JSONResponse:
    payload = service.get_run(run_id, include_hits=include_hits, hit_limit=hit_limit, hit_offset=hit_offset)
    if payload is None:
        raise HTTPException(status_code=404, detail=f"Unknown screener run: {run_id}")
    return JSONResponse(jsonable_encoder(payload))


@router.post("/screener-runs/{run_id}/delete", response_class=JSONResponse)
def soft_delete_screener_run(
    request: Request,
    run_id: int,
    payload: dict[str, object] | None = Body(default=None),
    service: ScreenerHistoryService = Depends(get_screener_history_service),
    principal: Principal = Depends(require_run_backtests),
    audit_service: AuditService = Depends(get_audit_service),
) -> JSONResponse:
    request_payload = payload or {}
    deleted = service.soft_delete(run_id, reason=str(request_payload.get("reason") or ""))
    if not deleted:
        raise HTTPException(status_code=404, detail=f"Unknown screener run: {run_id}")
    _record_audit(
        audit_service=audit_service,
        principal=principal,
        request=request,
        action="screener_runs.soft_delete",
        resource_type="screener_run",
        resource_id=str(run_id),
        resource_label=str(run_id),
        message=f"Soft-deleted screener run {run_id}.",
        metadata={"run_id": run_id, "reason": str(request_payload.get("reason") or "")},
    )
    return JSONResponse({"ok": True})


@router.post("/screener-runs/batch", response_class=JSONResponse)
def launch_screener_history_batch(
    request: Request,
    payload: dict[str, object] | None = Body(default=None),
    service: RunService = Depends(get_run_service),
    principal: Principal = Depends(require_run_backtests),
    audit_service: AuditService = Depends(get_audit_service),
) -> JSONResponse:
    request_payload = payload or {}
    strategy_ids = request_payload.get("strategy_ids")
    start_date = str(request_payload.get("start_date") or "").strip()
    end_date = str(request_payload.get("end_date") or "").strip()
    if not isinstance(strategy_ids, list) or not strategy_ids:
        raise HTTPException(status_code=400, detail="strategy_ids must be a non-empty array")
    if not start_date or not end_date:
        raise HTTPException(status_code=400, detail="start_date and end_date are required")
    scope = request_payload.get("scope")
    if scope is not None and not isinstance(scope, dict):
        raise HTTPException(status_code=400, detail="scope must be an object")
    options = {
        "strategy_ids": strategy_ids,
        "start_date": start_date,
        "end_date": end_date,
        "market_data_source": str(request_payload.get("market_data_mode") or "database-first"),
        "overwrite_policy": str(request_payload.get("overwrite_policy") or "skip_existing"),
        "scope": dict(scope or {}),
    }
    job_id = service.launch("screener_history_batch", options=options)
    _record_audit(
        audit_service=audit_service,
        principal=principal,
        request=request,
        action="screener_runs.batch_launch",
        resource_type="run_job",
        resource_id=job_id,
        resource_label="screener_history_batch",
        message="Queued screener history batch.",
        metadata={"job_id": job_id, **options},
    )
    return JSONResponse({"ok": True, "job_id": job_id})


@router.get("/watchlists", response_class=JSONResponse)
def watchlists_data(service: WatchlistService = Depends(get_watchlist_service)) -> JSONResponse:
    return JSONResponse({"watchlists": service.list_recent()})


@router.get("/watchlists/{stem}", response_class=JSONResponse)
def watchlist_detail_data(stem: str, service: WatchlistService = Depends(get_watchlist_service)) -> JSONResponse:
    return JSONResponse(service.get_watchlist_detail(stem))


@router.get("/watchlists/{stem}/chart/{ticker}", response_class=JSONResponse)
def watchlist_chart_data(
    stem: str,
    ticker: str,
    period: str = Query(default="18mo"),
    service: WatchlistService = Depends(get_watchlist_service),
) -> JSONResponse:
    _ = stem
    return JSONResponse(service.get_chart_payload(ticker=ticker.upper(), period=period))


@router.get("/overlap/latest", response_class=JSONResponse)
def overlap_latest(service: OverlapService = Depends(get_overlap_service)) -> JSONResponse:
    return JSONResponse(service.get_latest_summary())


@router.get("/rrg/latest", response_class=JSONResponse)
def rrg_latest(service: RrgService = Depends(get_rrg_service)) -> JSONResponse:
    return JSONResponse(service.get_latest_report())


@router.get("/rrg/{universe}", response_class=JSONResponse)
def rrg_universe_data(
    universe: str,
    benchmark: str = Query(default="SPY"),
    period: str = Query(default="3y"),
    trail_weeks: int = Query(default=12, alias="trailWeeks", ge=4, le=52),
    cadence: str = Query(default="weekly"),
    service: RrgService = Depends(get_rrg_service),
) -> JSONResponse:
    if universe not in {"sector", "industry", "theme"}:
        raise HTTPException(status_code=404, detail=f"Unsupported RRG universe: {universe}")
    if cadence not in {"weekly", "daily-2m"}:
        raise HTTPException(status_code=400, detail=f"Unsupported RRG cadence: {cadence}")
    return JSONResponse(
        service.get_universe_report(
            universe=universe,
            benchmark=benchmark,
            period=period,
            trail_weeks=trail_weeks,
            cadence=cadence,
        )
    )


@router.get("/overlap/{date_label}", response_class=JSONResponse)
def overlap_by_date(date_label: str, service: OverlapService = Depends(get_overlap_service)) -> JSONResponse:
    return JSONResponse(service.get_summary(date_label))


@router.get("/backtests", response_class=JSONResponse)
def backtests_data(
    service: BacktestService = Depends(get_backtest_service),
    history_service: ScreenerHistoryService = Depends(get_screener_history_service),
    run_service: RunService = Depends(get_run_service),
) -> JSONResponse:
    return JSONResponse(
        jsonable_encoder(
            {
                "backtest_templates": service.default_templates(),
                "backtest_runs": service.list_runs(limit=20),
                "signal_cache": history_service.list_signal_cache_summary(),
                "available_strategies": [{"id": item["id"], "label": item["label"]} for item in run_service.list_actions()],
                "default_exit_rules": DEFAULT_EXIT_RULES,
            }
        )
    )


@router.post("/backtests", response_class=JSONResponse)
def launch_backtest(
    request: Request,
    payload: dict[str, object] | None = Body(default=None),
    service: RunService = Depends(get_run_service),
    principal: Principal = Depends(require_run_backtests),
    audit_service: AuditService = Depends(get_audit_service),
) -> JSONResponse:
    request_payload = payload or {}
    entry_rule = request_payload.get("entry_rule")
    date_range = request_payload.get("date_range")
    exit_rules = request_payload.get("exit_rules")
    position_rules = request_payload.get("position_rules")
    if not isinstance(entry_rule, dict):
        raise HTTPException(status_code=400, detail="entry_rule must be an object")
    if not isinstance(date_range, dict):
        raise HTTPException(status_code=400, detail="date_range must be an object")
    if exit_rules is not None and not isinstance(exit_rules, list):
        raise HTTPException(status_code=400, detail="exit_rules must be an array")
    if position_rules is not None and not isinstance(position_rules, dict):
        raise HTTPException(status_code=400, detail="position_rules must be an object")
    options = {
        "entry_rule": entry_rule,
        "date_range": date_range,
        "exit_rules": list(exit_rules or DEFAULT_EXIT_RULES),
        "position_rules": dict(position_rules or {}),
        "signal_cache_policy": str(request_payload.get("signal_cache_policy") or "reuse_then_fill"),
        "market_data_mode": str(request_payload.get("market_data_mode") or "database_only"),
    }
    job_id = service.launch("backtest_v1", options=options)
    _record_audit(
        audit_service=audit_service,
        principal=principal,
        request=request,
        action="backtests.launch",
        resource_type="run_job",
        resource_id=job_id,
        resource_label="backtest_v1",
        message="Queued backtest run.",
        metadata={"job_id": job_id, **options},
    )
    return JSONResponse({"ok": True, "job_id": job_id})


@router.get("/admin/exclusions", response_class=JSONResponse)
def exclusions_data(
    coverage_start: str = Query(default="2020-01-01", alias="coverageStart"),
    service: AdminService = Depends(get_admin_service),
    _: Principal = Depends(require_manage_exclusions),
) -> JSONResponse:
    return JSONResponse(service.get_context(coverage_start=coverage_start))


@router.get("/admin/partial-tickers/{ticker}", response_class=JSONResponse)
def partial_ticker_detail(
    ticker: str,
    coverage_start: str = Query(default="2020-01-01", alias="coverageStart"),
    service: AdminService = Depends(get_admin_service),
    _: Principal = Depends(require_manage_exclusions),
) -> JSONResponse:
    try:
        return JSONResponse(service.get_partial_ticker_detail(ticker=ticker, coverage_start=coverage_start))
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/admin/exclusions", response_class=JSONResponse)
def add_exclusion(
    request: Request,
    payload: dict[str, object] | None = Body(default=None),
    service: AdminService = Depends(get_admin_service),
    principal: Principal = Depends(require_manage_exclusions),
    audit_service: AuditService = Depends(get_audit_service),
) -> JSONResponse:
    request_payload = payload or {}
    try:
        entry = service.add_exclusion(
            ticker=str(request_payload.get("ticker") or ""),
            reason=str(request_payload.get("reason") or ""),
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    _record_audit(
        audit_service=audit_service,
        principal=principal,
        request=request,
        action="admin.exclusion.add",
        resource_type="exclusion",
        resource_id=str(entry.get("ticker") or ""),
        resource_label=str(entry.get("ticker") or ""),
        message=f"Added exclusion for {entry.get('ticker')}.",
        metadata={"ticker": entry.get("ticker"), "reason": entry.get("reason")},
    )
    return JSONResponse({"ok": True, "entry": entry})


@router.post("/admin/exclusions/{ticker}/remove", response_class=JSONResponse)
def remove_exclusion(
    request: Request,
    ticker: str,
    payload: dict[str, object] | None = Body(default=None),
    service: AdminService = Depends(get_admin_service),
    principal: Principal = Depends(require_manage_exclusions),
    audit_service: AuditService = Depends(get_audit_service),
) -> JSONResponse:
    request_payload = payload or {}
    try:
        entry = service.remove_exclusion(
            ticker=ticker,
            reason=str(request_payload.get("reason") or ""),
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    _record_audit(
        audit_service=audit_service,
        principal=principal,
        request=request,
        action="admin.exclusion.remove",
        resource_type="exclusion",
        resource_id=str(entry.get("ticker") or ticker),
        resource_label=str(entry.get("ticker") or ticker),
        message=f"Removed exclusion for {entry.get('ticker') or ticker}.",
        metadata={"ticker": entry.get("ticker") or ticker, "reason": entry.get("reason"), "removed_from": entry.get("removed_from", [])},
    )
    return JSONResponse({"ok": True, "entry": entry})


@router.post("/admin/history-sync", response_class=JSONResponse)
def launch_history_sync(
    request: Request,
    payload: dict[str, object] | None = Body(default=None),
    service: RunService = Depends(get_run_service),
    principal: Principal = Depends(require_sync_history),
    audit_service: AuditService = Depends(get_audit_service),
) -> JSONResponse:
    request_payload = payload or {}
    options: dict[str, object] = {}
    for key in ("start_date", "end_date", "tickers", "chunk_size"):
        if key in request_payload:
            options[key] = request_payload[key]
    try:
        job_id = service.launch("sync_postgres_market_data", options=options)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    _record_audit(
        audit_service=audit_service,
        principal=principal,
        request=request,
        action="history_sync.launch",
        resource_type="run_job",
        resource_id=job_id,
        resource_label="sync_postgres_market_data",
        message="Queued history sync.",
        metadata={"job_id": job_id, **options},
    )
    return JSONResponse({"ok": True, "job_id": job_id})


@router.get("/admin/users", response_class=JSONResponse)
def admin_users(
    service: UserAdminService = Depends(get_user_admin_service),
    _: Principal = Depends(require_manage_users),
) -> JSONResponse:
    return JSONResponse(jsonable_encoder({"users": service.list_users(), "access_requests": service.list_access_requests()}))


@router.post("/admin/users/invite", response_class=JSONResponse)
def admin_invite_user(
    request: Request,
    payload: dict[str, object] | None = Body(default=None),
    service: UserAdminService = Depends(get_user_admin_service),
    principal: Principal = Depends(require_manage_users),
    audit_service: AuditService = Depends(get_audit_service),
) -> JSONResponse:
    request_payload = payload or {}
    try:
        user = service.invite_or_create_user(
            email=str(request_payload.get("email") or ""),
            role=str(request_payload.get("role") or "visitor"),
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    _record_audit(
        audit_service=audit_service,
        principal=principal,
        request=request,
        action="admin.user.invite",
        resource_type="user",
        resource_id=str(user.get("id") or ""),
        resource_label=str(user.get("email") or ""),
        message=f"Invited or updated user {user.get('email')}.",
        metadata={"user_id": user.get("id"), "email": user.get("email"), "role": user.get("role"), "is_active": user.get("is_active")},
    )
    return JSONResponse(jsonable_encoder({"ok": True, "user": user}))


@router.post("/admin/users/{user_id}/role", response_class=JSONResponse)
def admin_update_user_role(
    request: Request,
    user_id: int,
    payload: dict[str, object] | None = Body(default=None),
    service: UserAdminService = Depends(get_user_admin_service),
    principal: Principal = Depends(require_manage_users),
    audit_service: AuditService = Depends(get_audit_service),
) -> JSONResponse:
    request_payload = payload or {}
    previous = service.get_user(user_id=user_id)
    try:
        user = service.update_role(user_id=user_id, role=str(request_payload.get("role") or "visitor"))
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    _record_audit(
        audit_service=audit_service,
        principal=principal,
        request=request,
        action="admin.user.role_update",
        resource_type="user",
        resource_id=str(user_id),
        resource_label=str(user.get("email") or user_id),
        message=f"Updated role for {user.get('email')}.",
        metadata={"user_id": user_id, "email": user.get("email"), "role_before": previous.get("role") if previous else None, "role_after": user.get("role")},
    )
    return JSONResponse(jsonable_encoder({"ok": True, "user": user}))


@router.post("/admin/users/{user_id}/deactivate", response_class=JSONResponse)
def admin_deactivate_user(
    request: Request,
    user_id: int,
    service: UserAdminService = Depends(get_user_admin_service),
    principal: Principal = Depends(require_manage_users),
    audit_service: AuditService = Depends(get_audit_service),
) -> JSONResponse:
    previous = service.get_user(user_id=user_id)
    try:
        user = service.deactivate(user_id=user_id)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    _record_audit(
        audit_service=audit_service,
        principal=principal,
        request=request,
        action="admin.user.deactivate",
        resource_type="user",
        resource_id=str(user_id),
        resource_label=str(user.get("email") or user_id),
        message=f"Deactivated user {user.get('email')}.",
        metadata={"user_id": user_id, "email": user.get("email"), "was_active": previous.get("is_active") if previous else None, "is_active": user.get("is_active")},
    )
    return JSONResponse(jsonable_encoder({"ok": True, "user": user}))


@router.post("/admin/users/{user_id}/reactivate", response_class=JSONResponse)
def admin_reactivate_user(
    request: Request,
    user_id: int,
    service: UserAdminService = Depends(get_user_admin_service),
    principal: Principal = Depends(require_manage_users),
    audit_service: AuditService = Depends(get_audit_service),
) -> JSONResponse:
    previous = service.get_user(user_id=user_id)
    try:
        user = service.reactivate(user_id=user_id)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    _record_audit(
        audit_service=audit_service,
        principal=principal,
        request=request,
        action="admin.user.reactivate",
        resource_type="user",
        resource_id=str(user_id),
        resource_label=str(user.get("email") or user_id),
        message=f"Reactivated user {user.get('email')}.",
        metadata={"user_id": user_id, "email": user.get("email"), "was_active": previous.get("is_active") if previous else None, "is_active": user.get("is_active")},
    )
    return JSONResponse(jsonable_encoder({"ok": True, "user": user}))


@router.get("/admin/access-requests", response_class=JSONResponse)
def admin_access_requests(
    status: str = Query(default="", alias="status"),
    service: UserAdminService = Depends(get_user_admin_service),
    _: Principal = Depends(require_manage_users),
) -> JSONResponse:
    normalized_status = status.strip().lower() or None
    return JSONResponse(jsonable_encoder({"access_requests": service.list_access_requests(status=normalized_status)}))


@router.get("/admin/audit-events", response_class=JSONResponse)
def admin_audit_events(
    actor_email: str = Query(default="", alias="actorEmail"),
    action: str = Query(default=""),
    resource_type: str = Query(default="", alias="resourceType"),
    from_date: str = Query(default="", alias="from"),
    to_date: str = Query(default="", alias="to"),
    limit: int = Query(default=50, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
    service: AuditService = Depends(get_audit_service),
    _: Principal = Depends(require_manage_users),
) -> JSONResponse:
    try:
        payload = service.list_events(
            actor_email=actor_email,
            action=action,
            resource_type=resource_type,
            from_date=from_date,
            to_date=to_date,
            limit=limit,
            offset=offset,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return JSONResponse(jsonable_encoder(payload))


@router.post("/admin/access-requests/{request_id}/approve", response_class=JSONResponse)
def admin_approve_access_request(
    request: Request,
    request_id: int,
    principal: Principal = Depends(require_manage_users),
    service: UserAdminService = Depends(get_user_admin_service),
    audit_service: AuditService = Depends(get_audit_service),
) -> JSONResponse:
    if principal.user_id is None:
        raise HTTPException(status_code=400, detail="Authenticated admin user id is required.")
    previous = service.get_access_request(request_id=request_id)
    try:
        access_request = service.approve_access_request(request_id=request_id, reviewed_by_user_id=int(principal.user_id))
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    _record_audit(
        audit_service=audit_service,
        principal=principal,
        request=request,
        action="admin.access_request.approve",
        resource_type="access_request",
        resource_id=str(request_id),
        resource_label=str(access_request.get("email") or request_id),
        message=f"Approved premium access for {access_request.get('email')}.",
        metadata={
            "request_id": request_id,
            "email": access_request.get("email"),
            "requested_role": previous.get("requested_role") if previous else access_request.get("requested_role"),
            "status_before": previous.get("status") if previous else None,
            "status_after": access_request.get("status"),
            "invited_user_id": access_request.get("invited_user_id"),
        },
    )
    return JSONResponse(jsonable_encoder({"ok": True, "access_request": access_request}))


@router.post("/admin/access-requests/{request_id}/deny", response_class=JSONResponse)
def admin_deny_access_request(
    request: Request,
    request_id: int,
    payload: dict[str, object] | None = Body(default=None),
    principal: Principal = Depends(require_manage_users),
    service: UserAdminService = Depends(get_user_admin_service),
    audit_service: AuditService = Depends(get_audit_service),
) -> JSONResponse:
    if principal.user_id is None:
        raise HTTPException(status_code=400, detail="Authenticated admin user id is required.")
    request_payload = payload or {}
    previous = service.get_access_request(request_id=request_id)
    try:
        access_request = service.deny_access_request(
            request_id=request_id,
            reviewed_by_user_id=int(principal.user_id),
            deny_reason=str(request_payload.get("deny_reason") or ""),
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    _record_audit(
        audit_service=audit_service,
        principal=principal,
        request=request,
        action="admin.access_request.deny",
        resource_type="access_request",
        resource_id=str(request_id),
        resource_label=str(access_request.get("email") or request_id),
        message=f"Denied premium access for {access_request.get('email')}.",
        metadata={
            "request_id": request_id,
            "email": access_request.get("email"),
            "requested_role": previous.get("requested_role") if previous else access_request.get("requested_role"),
            "status_before": previous.get("status") if previous else None,
            "status_after": access_request.get("status"),
            "deny_reason": access_request.get("deny_reason"),
        },
    )
    return JSONResponse(jsonable_encoder({"ok": True, "access_request": access_request}))
