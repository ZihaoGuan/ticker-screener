from __future__ import annotations

import asyncio
import datetime as dt
import json
from pathlib import Path
from urllib.parse import urlencode

from fastapi import APIRouter, Body, Depends, Header, HTTPException, Query, Request, Response, status
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse, RedirectResponse, StreamingResponse

from src.webapp.access_control import Principal
from src.webapp.services.admin_service import AdminService
from src.webapp.services.audit_service import AuditService
from src.webapp.services.auth_service import AuthService, UserAdminService
from src.webapp.services.dashboard_service import DashboardService
from src.webapp.services.earnings_calendar_service import EarningsCalendarService
from src.webapp.services.overlap_backtest_service import OverlapBacktestService
from src.webapp.services.overlap_service import OverlapService
from src.webapp.services.portfolio_service import PortfolioService
from src.webapp.services.rrg_service import RrgService
from src.webapp.services.ad_hoc_screen_service import AdHocScreenService
from src.webapp.services.run_service import RunService
from src.webapp.services.scheduled_job_service import ScheduledJobService
from src.webapp.services.screener_history_service import ScreenerHistoryService
from src.webapp.services.watchlist_service import WatchlistService
from web.dependencies import (
    get_ad_hoc_screen_service,
    get_admin_service,
    clear_auth_cookie,
    config,
    get_audit_service,
    get_auth_service,
    get_chart_watchlist_service,
    get_current_principal,
    get_dashboard_service,
    get_earnings_calendar_service,
    get_overlap_backtest_service,
    get_overlap_service,
    get_portfolio_service,
    get_rrg_service,
    get_run_service,
    get_scheduled_job_service,
    get_screener_history_service,
    get_user_admin_service,
    get_watchlist_service,
    require_member_access,
    require_manage_exclusions,
    require_manage_users,
    require_run_screeners,
    require_sync_history,
    set_auth_cookie,
)


router = APIRouter(prefix="/api", tags=["api"])

_JOB_STREAM_POLL_SECONDS = 1.0
_JOB_STREAM_HEARTBEAT_SECONDS = 15.0
_JOB_STREAM_MAX_CHUNK_BYTES = 16_384


def _format_sse(event: str, data: dict[str, object], *, event_id: str | None = None) -> str:
    lines: list[str] = []
    if event_id is not None:
        lines.append(f"id: {event_id}")
    lines.append(f"event: {event}")
    payload = json.dumps(data, separators=(",", ":"))
    for line in payload.splitlines():
        lines.append(f"data: {line}")
    return "\n".join(lines) + "\n\n"


def _coerce_stream_cursor(cursor: int | None, last_event_id: str | None) -> int:
    if last_event_id is not None:
        try:
            return max(0, int(last_event_id))
        except (TypeError, ValueError):
            pass
    if cursor is None:
        return 0
    return max(0, int(cursor))


def _resolve_job_log_path(service: RunService, job: dict[str, object]) -> Path | None:
    raw_path = str(job.get("log_file") or "").strip()
    if not raw_path:
        return None
    configured_artifacts_dir = service.artifacts_dir.resolve()
    allowed_roots = [service.project_root.resolve(), configured_artifacts_dir]
    raw_candidate = Path(raw_path)
    candidate_options: list[Path] = []

    if raw_candidate.is_absolute():
        candidate_options.append(raw_candidate.resolve())
        raw_parts = raw_candidate.parts
        artifacts_name = configured_artifacts_dir.name
        if len(raw_parts) >= 2 and raw_parts[1] == artifacts_name:
            candidate_options.append((configured_artifacts_dir / Path(*raw_parts[2:])).resolve())
    else:
        candidate_options.append((service.project_root / raw_candidate).resolve())
        candidate_options.append((configured_artifacts_dir / raw_candidate).resolve())

    for candidate in candidate_options:
        try:
            if not any(candidate.is_relative_to(root) for root in allowed_roots):
                continue
        except AttributeError:
            try:
                candidate.relative_to(service.project_root.resolve())
                inside_allowed_root = True
            except ValueError:
                try:
                    candidate.relative_to(configured_artifacts_dir)
                    inside_allowed_root = True
                except ValueError:
                    inside_allowed_root = False
            if not inside_allowed_root:
                continue
        if candidate.exists() and candidate.is_file():
            return candidate
    return None


def _current_log_cursor(log_path: Path | None) -> int:
    if log_path is None:
        return 0
    try:
        return max(0, int(log_path.stat().st_size))
    except OSError:
        return 0


def _read_job_log_update(log_path: Path | None, cursor: int, pending_fragment: str) -> tuple[int, str, list[str]]:
    if log_path is None:
        return cursor, pending_fragment, []
    try:
        with log_path.open("rb") as handle:
            handle.seek(cursor)
            chunk = handle.read(_JOB_STREAM_MAX_CHUNK_BYTES)
            next_cursor = int(handle.tell())
    except OSError:
        return cursor, pending_fragment, []
    if not chunk:
        return cursor, pending_fragment, []
    text = pending_fragment + chunk.decode("utf-8", errors="replace")
    lines = text.splitlines()
    next_fragment = ""
    if text and not text.endswith(("\n", "\r")):
        if lines:
            next_fragment = lines.pop()
        else:
            next_fragment = text
    return next_cursor, next_fragment, lines


def _job_stream_signature(job: dict[str, object]) -> str:
    payload = {
        "status": job.get("status"),
        "return_code": job.get("return_code"),
        "finished_at": job.get("finished_at"),
        "progress_current": job.get("progress_current"),
        "progress_total": job.get("progress_total"),
        "progress_percent": job.get("progress_percent"),
        "progress_label": job.get("progress_label"),
        "success_count": job.get("success_count"),
        "watchlist_file": job.get("watchlist_file"),
        "summary_file": job.get("summary_file"),
        "raw_results_file": job.get("raw_results_file"),
        "child_job_summary": job.get("child_job_summary"),
    }
    return json.dumps(payload, sort_keys=True, default=str)


def _jobs_payload(service: RunService) -> dict[str, object]:
    return {
        "actions": service.list_actions(),
        "jobs": service.list_jobs(),
    }


def _jobs_payload_signature(payload: dict[str, object]) -> str:
    return json.dumps(payload, sort_keys=True, default=str)


def _build_job_stream_response(
    *,
    request: Request,
    service: RunService,
    stream_key: str,
    initial_job: dict[str, object],
    cursor: int | None,
    last_event_id: str | None,
    resolver,
) -> StreamingResponse:
    async def event_stream():
        current_job = initial_job
        log_path = _resolve_job_log_path(service, current_job)
        has_resume_cursor = cursor is not None or last_event_id is not None
        resume_cursor = _coerce_stream_cursor(cursor, last_event_id)
        pending_fragment = ""
        current_cursor = resume_cursor if has_resume_cursor else _current_log_cursor(log_path)
        recent_lines = [] if has_resume_cursor else [line for line in str(current_job.get("log_tail") or "").splitlines() if line]
        yield _format_sse(
            "snapshot",
            {"job": current_job, "cursor": current_cursor, "recent_lines": recent_lines},
            event_id=str(current_cursor),
        )
        last_signature = _job_stream_signature(current_job)
        last_heartbeat_at = asyncio.get_running_loop().time()

        while True:
            if await request.is_disconnected():
                break

            current_job = resolver()
            log_path = _resolve_job_log_path(service, current_job)
            next_cursor, pending_fragment, new_lines = _read_job_log_update(log_path, current_cursor, pending_fragment)
            current_cursor = next_cursor
            for line in new_lines:
                yield _format_sse(
                    "log",
                    {"job_id": stream_key, "cursor": current_cursor, "line": line},
                    event_id=str(current_cursor),
                )

            next_signature = _job_stream_signature(current_job)
            if next_signature != last_signature:
                yield _format_sse(
                    "status",
                    {"job": current_job, "cursor": current_cursor},
                    event_id=str(current_cursor),
                )
                last_signature = next_signature

            if str(current_job.get("status") or "") not in {"queued", "running"}:
                if pending_fragment:
                    yield _format_sse(
                        "log",
                        {"job_id": stream_key, "cursor": current_cursor, "line": pending_fragment},
                        event_id=str(current_cursor),
                    )
                    pending_fragment = ""
                yield _format_sse(
                    "eof",
                    {"job": current_job, "cursor": current_cursor},
                    event_id=str(current_cursor),
                )
                break

            now = asyncio.get_running_loop().time()
            if now - last_heartbeat_at >= _JOB_STREAM_HEARTBEAT_SECONDS:
                yield _format_sse("heartbeat", {"job_id": stream_key, "cursor": current_cursor}, event_id=str(current_cursor))
                last_heartbeat_at = now

            await asyncio.sleep(_JOB_STREAM_POLL_SECONDS)

    return StreamingResponse(
        event_stream(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


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
def dashboard_data(
    service: DashboardService = Depends(get_dashboard_service),
    principal: Principal = Depends(require_member_access),
) -> JSONResponse:
    return JSONResponse(service.get_dashboard_context(include_deprecated_watchlists=principal.role == "admin"))


@router.get("/earnings-calendar", response_class=JSONResponse)
def earnings_calendar_data(
    reference_date: dt.date | None = Query(default=None, alias="referenceDate"),
    week_offset: int = Query(default=0, alias="weekOffset", ge=0, le=2),
    exclude_sectors: list[str] = Query(default=[], alias="excludeSector"),
    exclude_industries: list[str] = Query(default=[], alias="excludeIndustry"),
    only_criteria: bool = Query(default=False, alias="onlyCriteria"),
    service: EarningsCalendarService = Depends(get_earnings_calendar_service),
) -> JSONResponse:
    return JSONResponse(
        service.get_next_week_calendar(
            reference_date=reference_date,
            week_offset=week_offset,
            exclude_sectors=exclude_sectors,
            exclude_industries=exclude_industries,
            only_criteria=only_criteria,
        )
    )


@router.get("/auth/me", response_class=JSONResponse)
def auth_me(principal: Principal = Depends(get_current_principal)) -> JSONResponse:
    return JSONResponse(jsonable_encoder({"authenticated": principal.authenticated, "user": principal.to_dict() if principal.authenticated else None, "role": principal.role, "capabilities": list(principal.capabilities)}))


@router.get("/auth/google/start", response_class=RedirectResponse)
def google_auth_start(
    next_path: str = Query(default="/", alias="next"),
    service: AuthService = Depends(get_auth_service),
) -> RedirectResponse:
    try:
        payload = service.begin_google_oauth(next_path=next_path)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    response = RedirectResponse(url=str(payload["authorization_url"]), status_code=302)
    response.set_cookie(
        key=config.auth_oauth_state_cookie_name,
        value=str(payload["state_cookie_value"]),
        httponly=True,
        secure=config.auth_cookie_secure,
        samesite=config.auth_cookie_samesite,
        max_age=600,
        path="/",
    )
    return response


@router.get("/auth/google/callback", response_class=RedirectResponse)
def google_auth_callback(
    request: Request,
    code: str = Query(default=""),
    state: str = Query(default=""),
    error: str = Query(default=""),
    error_description: str = Query(default="", alias="error_description"),
    service: AuthService = Depends(get_auth_service),
) -> RedirectResponse:
    next_path = "/"
    if error.strip():
        next_path = str(request.query_params.get("next") or "/")
        response = RedirectResponse(
            url=f"/login?{urlencode({'next': next_path, 'error': error_description.strip() or error.strip()})}",
            status_code=302,
        )
        response.delete_cookie(
            key=config.auth_oauth_state_cookie_name,
            path="/",
            secure=config.auth_cookie_secure,
            samesite=config.auth_cookie_samesite,
        )
        return response
    try:
        result = service.complete_google_oauth(
            code=code,
            state=state,
            signed_state_cookie=request.cookies.get(config.auth_oauth_state_cookie_name),
            request_ip=request.client.host if request.client else "",
            request_user_agent=request.headers.get("user-agent", ""),
        )
        next_path = str(result.get("next_path") or "/")
        response = RedirectResponse(url=next_path, status_code=302)
        set_auth_cookie(response, str(result["session_cookie_value"]))
    except ValueError as exc:
        response = RedirectResponse(url=f"/login?{urlencode({'error': str(exc)})}", status_code=302)
    response.delete_cookie(
        key=config.auth_oauth_state_cookie_name,
        path="/",
        secure=config.auth_cookie_secure,
        samesite=config.auth_cookie_samesite,
    )
    return response


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


@router.get("/jobs/stream")
async def jobs_data_stream(
    request: Request,
    service: RunService = Depends(get_run_service),
    _: Principal = Depends(require_run_screeners),
) -> StreamingResponse:
    async def event_stream():
        payload = _jobs_payload(service)
        yield _format_sse("snapshot", payload)
        last_signature = _jobs_payload_signature(payload)
        last_heartbeat_at = asyncio.get_running_loop().time()

        while True:
            if await request.is_disconnected():
                break
            payload = _jobs_payload(service)
            signature = _jobs_payload_signature(payload)
            if signature != last_signature:
                yield _format_sse("jobs", payload)
                last_signature = signature
            now = asyncio.get_running_loop().time()
            if now - last_heartbeat_at >= _JOB_STREAM_HEARTBEAT_SECONDS:
                yield _format_sse("heartbeat", {"ok": True})
                last_heartbeat_at = now
            await asyncio.sleep(_JOB_STREAM_POLL_SECONDS)

    return StreamingResponse(
        event_stream(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


@router.get("/jobs/{job_id}", response_class=JSONResponse)
def job_detail(job_id: str, service: RunService = Depends(get_run_service), _: Principal = Depends(require_run_screeners)) -> JSONResponse:
    try:
        return JSONResponse(service.get_job(job_id))
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.get("/jobs/{job_id}/stream")
async def job_detail_stream(
    request: Request,
    job_id: str,
    cursor: int | None = Query(default=None),
    last_event_id: str | None = Header(default=None, alias="Last-Event-ID"),
    service: RunService = Depends(get_run_service),
    _: Principal = Depends(require_run_screeners),
) -> StreamingResponse:
    try:
        initial_job = service.get_job(job_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    return _build_job_stream_response(
        request=request,
        service=service,
        stream_key=job_id,
        initial_job=initial_job,
        cursor=cursor,
        last_event_id=last_event_id,
        resolver=lambda: service.get_job(job_id),
    )


@router.get("/child-jobs/{child_job_run_id}/stream")
async def child_job_detail_stream(
    request: Request,
    child_job_run_id: int,
    cursor: int | None = Query(default=None),
    last_event_id: str | None = Header(default=None, alias="Last-Event-ID"),
    service: RunService = Depends(get_run_service),
    _: Principal = Depends(require_run_screeners),
) -> StreamingResponse:
    try:
        initial_job = service.get_child_job(child_job_run_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    return _build_job_stream_response(
        request=request,
        service=service,
        stream_key=str(child_job_run_id),
        initial_job=initial_job,
        cursor=cursor,
        last_event_id=last_event_id,
        resolver=lambda: service.get_child_job(child_job_run_id),
    )


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


@router.post("/runs/{action_id}/precheck", response_class=JSONResponse)
def run_action_precheck(
    action_id: str,
    payload: dict[str, object] | None = Body(default=None),
    service: RunService = Depends(get_run_service),
    _: Principal = Depends(require_run_screeners),
) -> JSONResponse:
    try:
        result = service.precheck(action_id, options=payload or {})
    except ValueError as exc:
        status_code = 404 if str(exc).startswith("Unknown run action") else 400
        raise HTTPException(status_code=status_code, detail=str(exc)) from exc
    return JSONResponse(jsonable_encoder(result))


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
    _: Principal = Depends(require_member_access),
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
    _: Principal = Depends(require_member_access),
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
    _: Principal = Depends(require_member_access),
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
    principal: Principal = Depends(require_run_screeners),
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
    principal: Principal = Depends(require_run_screeners),
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


@router.get("/overlap-warm/coverage", response_class=JSONResponse)
def overlap_warm_coverage(
    from_date: str = Query(alias="from"),
    to_date: str = Query(alias="to"),
    strategy_ids_raw: list[str] | None = Query(default=None, alias="strategyIds"),
    candidate_threshold: int = Query(default=4, alias="candidateThreshold", ge=2, le=20),
    service: OverlapBacktestService = Depends(get_overlap_backtest_service),
    _: Principal = Depends(require_member_access),
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
    if not strategy_ids:
        raise HTTPException(status_code=400, detail="strategyIds is required")
    days = service.list_overlap_coverage(
        strategy_ids=strategy_ids,
        start_date=start_date,
        end_date=end_date,
        candidate_threshold=candidate_threshold,
    )
    return JSONResponse(
        jsonable_encoder(
            {
                "configured": service.is_configured(),
                "from": start_date.isoformat(),
                "to": end_date.isoformat(),
                "strategy_ids": strategy_ids,
                "candidate_threshold": candidate_threshold,
                "days": days,
            }
        )
    )


@router.get("/backtests-v1", response_class=JSONResponse)
def backtests_v1(
    limit: int = Query(default=20, ge=1, le=100),
    service: OverlapBacktestService = Depends(get_overlap_backtest_service),
    _: Principal = Depends(require_member_access),
) -> JSONResponse:
    return JSONResponse({"configured": service.is_configured(), "runs": jsonable_encoder(service.list_backtest_runs(limit=limit))})


@router.get("/backtests-v1/{run_id}", response_class=JSONResponse)
def backtest_v1_detail(
    run_id: int,
    service: OverlapBacktestService = Depends(get_overlap_backtest_service),
    _: Principal = Depends(require_member_access),
) -> JSONResponse:
    payload = service.get_backtest_run(run_id)
    if payload is None:
        raise HTTPException(status_code=404, detail=f"Unknown backtest run: {run_id}")
    return JSONResponse(jsonable_encoder(payload))


@router.get("/watchlists", response_class=JSONResponse)
def watchlists_data(
    service: WatchlistService = Depends(get_watchlist_service),
    principal: Principal = Depends(require_member_access),
) -> JSONResponse:
    return JSONResponse({"watchlists": service.list_recent(include_deprecated=principal.role == "admin")})


@router.get("/scanner-board", response_class=JSONResponse)
def scanner_board_data(
    service: WatchlistService = Depends(get_watchlist_service),
    _: Principal = Depends(require_member_access),
) -> JSONResponse:
    return JSONResponse(service.get_scanner_board())


@router.get("/scanner-board/top-hits", response_class=JSONResponse)
def scanner_top_hits_data(
    service: WatchlistService = Depends(get_watchlist_service),
    rrg_service: RrgService = Depends(get_rrg_service),
    _: Principal = Depends(require_member_access),
) -> JSONResponse:
    return JSONResponse(service.get_scanner_top_hits_payload(rrg_service=rrg_service))


@router.get("/watchlists/weekly", response_class=JSONResponse)
def weekly_watchlist_data(
    stem: str | None = Query(default=None),
    service: WatchlistService = Depends(get_watchlist_service),
    _: Principal = Depends(require_member_access),
) -> JSONResponse:
    return JSONResponse(service.get_weekly_watchlist_board(stem=stem))


@router.get("/watchlists/{stem}", response_class=JSONResponse)
def watchlist_detail_data(
    stem: str,
    service: WatchlistService = Depends(get_watchlist_service),
    principal: Principal = Depends(require_member_access),
) -> JSONResponse:
    try:
        return JSONResponse(service.get_watchlist_detail(stem, allow_deprecated=principal.role == "admin"))
    except ValueError as exc:
        message = str(exc)
        status_code = 403 if message.startswith("Deprecated watchlist is admin-only:") else 404
        raise HTTPException(status_code=status_code, detail=message) from exc


@router.get("/watchlists/{stem}/chart/{ticker}", response_class=JSONResponse)
def watchlist_chart_data(
    stem: str,
    ticker: str,
    period: str = Query(default="18mo"),
    as_of_date: dt.date | None = Query(default=None, alias="asOfDate"),
    include_setup_markers: bool = Query(default=False, alias="includeSetupMarkers"),
    service: WatchlistService = Depends(get_chart_watchlist_service),
    _: Principal = Depends(require_member_access),
) -> JSONResponse:
    _ = stem
    return JSONResponse(
        service.get_chart_payload(
            ticker=ticker.upper(),
            period=period,
            as_of_date=as_of_date,
            include_setup_markers=include_setup_markers,
        )
    )


@router.get("/charts/{ticker}", response_class=JSONResponse)
def ticker_chart_data(
    ticker: str,
    period: str = Query(default="18mo"),
    as_of_date: dt.date | None = Query(default=None, alias="asOfDate"),
    include_setup_markers: bool = Query(default=False, alias="includeSetupMarkers"),
    service: WatchlistService = Depends(get_chart_watchlist_service),
) -> JSONResponse:
    return JSONResponse(
        service.get_chart_payload(
            ticker=ticker.upper(),
            period=period,
            as_of_date=as_of_date,
            include_setup_markers=include_setup_markers,
        )
    )


@router.get("/chart-overlays/{ticker}", response_class=JSONResponse)
def ticker_chart_overlays_data(
    ticker: str,
    period: str = Query(default="18mo"),
    as_of_date: dt.date | None = Query(default=None, alias="asOfDate"),
    include_setup_markers: bool = Query(default=False, alias="includeSetupMarkers"),
    service: WatchlistService = Depends(get_chart_watchlist_service),
) -> JSONResponse:
    return JSONResponse(
        service.get_chart_overlays_payload(
            ticker=ticker.upper(),
            period=period,
            as_of_date=as_of_date,
            include_setup_markers=include_setup_markers,
        )
    )


@router.get("/chart-fundamentals/{ticker}", response_class=JSONResponse)
def chart_fundamentals_data(
    ticker: str,
    earnings_limit: int = Query(default=4, alias="earningsLimit", ge=1, le=24),
    service: WatchlistService = Depends(get_watchlist_service),
) -> JSONResponse:
    return JSONResponse(service.get_chart_fundamentals_payload(ticker=ticker.upper(), earnings_limit=earnings_limit))


@router.get("/ratings/top", response_class=JSONResponse)
def top_ratings_data(
    as_of_date: dt.date | None = Query(default=None, alias="asOfDate"),
    limit: int = Query(default=100, ge=1, le=500),
    rating_status: str = Query(default="ok", alias="ratingStatus"),
    sector: str = Query(default=""),
    service: WatchlistService = Depends(get_watchlist_service),
    _: Principal = Depends(require_member_access),
) -> JSONResponse:
    return JSONResponse(
        service.get_top_ratings_payload(
            as_of_date=as_of_date,
            limit=limit,
            rating_status=rating_status,
            sector=sector,
        )
    )


@router.get("/ratings/technical/top", response_class=JSONResponse)
def top_technical_ratings_data(
    as_of_date: dt.date | None = Query(default=None, alias="asOfDate"),
    limit: int = Query(default=100, ge=1, le=500),
    technical_status: str = Query(default="ok", alias="technicalStatus"),
    sector: str = Query(default=""),
    service: WatchlistService = Depends(get_watchlist_service),
    _: Principal = Depends(require_member_access),
) -> JSONResponse:
    return JSONResponse(
        service.get_top_technical_ratings_payload(
            as_of_date=as_of_date,
            limit=limit,
            technical_status=technical_status,
            sector=sector,
        )
    )


@router.get("/chart-insider/{ticker}", response_class=JSONResponse)
def chart_insider_data(
    ticker: str,
    lookback_days: int = Query(default=14, alias="lookbackDays", ge=1, le=120),
    as_of_date: dt.date | None = Query(default=None, alias="asOfDate"),
    service: WatchlistService = Depends(get_watchlist_service),
) -> JSONResponse:
    return JSONResponse(
        service.get_chart_insider_payload(
            ticker=ticker.upper(),
            lookback_days=lookback_days,
            as_of_date=as_of_date,
        )
    )


@router.get("/overlap/latest", response_class=JSONResponse)
def overlap_latest(
    service: OverlapService = Depends(get_overlap_service),
    _: Principal = Depends(require_member_access),
) -> JSONResponse:
    return JSONResponse(service.get_latest_summary())


@router.get("/rrg/latest", response_class=JSONResponse)
def rrg_latest(
    service: RrgService = Depends(get_rrg_service),
    _: Principal = Depends(require_member_access),
) -> JSONResponse:
    return JSONResponse(service.get_latest_report())


@router.get("/rrg/{universe}", response_class=JSONResponse)
def rrg_universe_data(
    universe: str,
    benchmark: str = Query(default="SPY"),
    period: str = Query(default="3y"),
    trail_weeks: int = Query(default=12, alias="trailWeeks", ge=4, le=52),
    cadence: str = Query(default="weekly"),
    service: RrgService = Depends(get_rrg_service),
    _: Principal = Depends(require_member_access),
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
def overlap_by_date(
    date_label: str,
    service: OverlapService = Depends(get_overlap_service),
    _: Principal = Depends(require_member_access),
) -> JSONResponse:
    return JSONResponse(service.get_summary(date_label))


@router.get("/admin/exclusions", response_class=JSONResponse)
def exclusions_data(
    coverage_start: str = Query(default="2020-01-01", alias="coverageStart"),
    service: AdminService = Depends(get_admin_service),
    _: Principal = Depends(require_manage_exclusions),
) -> JSONResponse:
    return JSONResponse(service.get_context(coverage_start=coverage_start))


@router.get("/admin/ratings-status", response_class=JSONResponse)
def ratings_status_data(
    service: AdminService = Depends(get_admin_service),
    _: Principal = Depends(require_manage_exclusions),
) -> JSONResponse:
    return JSONResponse(service.get_ratings_status())


@router.get("/admin/missing-sectors", response_class=JSONResponse)
def missing_sector_data(
    service: AdminService = Depends(get_admin_service),
    _: Principal = Depends(require_manage_exclusions),
) -> JSONResponse:
    return JSONResponse(service.get_missing_sector_context())


@router.get("/admin/ticker-lists/{ticker}", response_class=JSONResponse)
def ticker_list_status(
    ticker: str,
    service: AdminService = Depends(get_admin_service),
    _: Principal = Depends(require_manage_exclusions),
) -> JSONResponse:
    try:
        return JSONResponse(service.get_ticker_list_status(ticker=ticker))
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


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


@router.get("/admin/scheduled-jobs", response_class=JSONResponse)
def scheduled_jobs_data(
    service: AdminService = Depends(get_admin_service),
    _: Principal = Depends(require_manage_exclusions),
) -> JSONResponse:
    return JSONResponse({"jobs": service.list_scheduled_jobs()})


@router.get("/admin/schedules", response_class=JSONResponse)
def schedule_config_data(
    service: ScheduledJobService = Depends(get_scheduled_job_service),
    _: Principal = Depends(require_manage_exclusions),
) -> JSONResponse:
    return JSONResponse(service.get_context())


@router.post("/admin/schedules", response_class=JSONResponse)
def upsert_schedule_config(
    request: Request,
    payload: dict[str, object] | None = Body(default=None),
    service: ScheduledJobService = Depends(get_scheduled_job_service),
    principal: Principal = Depends(require_manage_exclusions),
    audit_service: AuditService = Depends(get_audit_service),
) -> JSONResponse:
    request_payload = payload or {}
    try:
        job = service.upsert_job(
            job_id=str(request_payload.get("job_id") or ""),
            job_label=str(request_payload.get("job_label") or ""),
            action_id=str(request_payload.get("action_id") or ""),
            cron_expr=str(request_payload.get("cron_expr") or ""),
            cron_tz=str(request_payload.get("cron_tz") or ""),
            enabled=bool(request_payload.get("enabled", True)),
            options=request_payload.get("options") if isinstance(request_payload.get("options"), dict) else {},
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    _record_audit(
        audit_service=audit_service,
        principal=principal,
        request=request,
        action="admin.schedule.upsert",
        resource_type="scheduled_job",
        resource_id=job["job_id"],
        resource_label=job["job_label"],
        message=f"Saved scheduled job {job['job_id']}.",
        metadata=job,
    )
    return JSONResponse({"ok": True, "job": job})


@router.post("/admin/schedules/{job_id}/delete", response_class=JSONResponse)
def delete_schedule_config(
    request: Request,
    job_id: str,
    service: ScheduledJobService = Depends(get_scheduled_job_service),
    principal: Principal = Depends(require_manage_exclusions),
    audit_service: AuditService = Depends(get_audit_service),
) -> JSONResponse:
    try:
        service.delete_job(job_id=job_id)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    _record_audit(
        audit_service=audit_service,
        principal=principal,
        request=request,
        action="admin.schedule.delete",
        resource_type="scheduled_job",
        resource_id=job_id,
        resource_label=job_id,
        message=f"Deleted scheduled job {job_id}.",
        metadata={"job_id": job_id},
    )
    return JSONResponse({"ok": True})


@router.post("/admin/schedules/settings", response_class=JSONResponse)
def update_schedule_settings(
    request: Request,
    payload: dict[str, object] | None = Body(default=None),
    service: ScheduledJobService = Depends(get_scheduled_job_service),
    principal: Principal = Depends(require_manage_exclusions),
    audit_service: AuditService = Depends(get_audit_service),
) -> JSONResponse:
    request_payload = payload or {}
    try:
        max_parallel_jobs = service.update_max_parallel_jobs(int(request_payload.get("max_parallel_jobs") or 0))
    except (ValueError, TypeError) as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    _record_audit(
        audit_service=audit_service,
        principal=principal,
        request=request,
        action="admin.schedule.settings",
        resource_type="scheduled_job_settings",
        resource_id="max_parallel_jobs",
        resource_label="max_parallel_jobs",
        message=f"Updated scheduler max parallel jobs to {max_parallel_jobs}.",
        metadata={"max_parallel_jobs": max_parallel_jobs},
    )
    return JSONResponse({"ok": True, "max_parallel_jobs": max_parallel_jobs})


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


@router.post("/admin/inclusions", response_class=JSONResponse)
def add_inclusion(
    request: Request,
    payload: dict[str, object] | None = Body(default=None),
    service: AdminService = Depends(get_admin_service),
    principal: Principal = Depends(require_manage_exclusions),
    audit_service: AuditService = Depends(get_audit_service),
) -> JSONResponse:
    request_payload = payload or {}
    try:
        entry = service.add_inclusion(
            ticker=str(request_payload.get("ticker") or ""),
            reason=str(request_payload.get("reason") or ""),
            remove_from_exclusions=bool(request_payload.get("remove_from_exclusions", True)),
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    _record_audit(
        audit_service=audit_service,
        principal=principal,
        request=request,
        action="admin.inclusion.add",
        resource_type="inclusion",
        resource_id=str(entry.get("ticker") or ""),
        resource_label=str(entry.get("ticker") or ""),
        message=f"Added inclusion for {entry.get('ticker')}.",
        metadata={"ticker": entry.get("ticker"), "reason": entry.get("reason"), "removed_from": entry.get("removed_from", [])},
    )
    return JSONResponse({"ok": True, "entry": entry})


@router.post("/admin/ticker-sectors/{ticker}", response_class=JSONResponse)
def update_ticker_sector(
    request: Request,
    ticker: str,
    payload: dict[str, object] | None = Body(default=None),
    service: AdminService = Depends(get_admin_service),
    principal: Principal = Depends(require_manage_exclusions),
    audit_service: AuditService = Depends(get_audit_service),
) -> JSONResponse:
    request_payload = payload or {}
    try:
        entry = service.update_ticker_sector(
            ticker=ticker,
            sector=str(request_payload.get("sector") or ""),
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    _record_audit(
        audit_service=audit_service,
        principal=principal,
        request=request,
        action="admin.ticker_sector.update",
        resource_type="ticker_metadata",
        resource_id=str(entry.get("ticker") or ""),
        resource_label=str(entry.get("ticker") or ""),
        message=f"Updated sector for {entry.get('ticker')} to {entry.get('sector')}.",
        metadata={"ticker": entry.get("ticker"), "sector": entry.get("sector")},
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


@router.post("/admin/inclusions/{ticker}/remove", response_class=JSONResponse)
def remove_inclusion(
    request: Request,
    ticker: str,
    payload: dict[str, object] | None = Body(default=None),
    service: AdminService = Depends(get_admin_service),
    principal: Principal = Depends(require_manage_exclusions),
    audit_service: AuditService = Depends(get_audit_service),
) -> JSONResponse:
    request_payload = payload or {}
    try:
        entry = service.remove_inclusion(
            ticker=ticker,
            reason=str(request_payload.get("reason") or ""),
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    _record_audit(
        audit_service=audit_service,
        principal=principal,
        request=request,
        action="admin.inclusion.remove",
        resource_type="inclusion",
        resource_id=str(entry.get("ticker") or ticker),
        resource_label=str(entry.get("ticker") or ticker),
        message=f"Removed inclusion for {entry.get('ticker') or ticker}.",
        metadata={"ticker": entry.get("ticker") or ticker, "reason": entry.get("reason")},
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
    for key in ("start_date", "end_date", "tickers", "chunk_size", "include_excluded_tickers"):
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


@router.get("/admin/portfolio", response_class=JSONResponse)
def admin_portfolio_context(
    service: PortfolioService = Depends(get_portfolio_service),
    _: Principal = Depends(require_manage_exclusions),
) -> JSONResponse:
    try:
        return JSONResponse(jsonable_encoder(service.get_context()))
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/admin/portfolio/positions", response_class=JSONResponse)
def admin_create_portfolio_position(
    request: Request,
    payload: dict[str, object] | None = Body(default=None),
    service: PortfolioService = Depends(get_portfolio_service),
    principal: Principal = Depends(require_manage_exclusions),
    audit_service: AuditService = Depends(get_audit_service),
) -> JSONResponse:
    request_payload = payload or {}
    try:
        position = service.create_position(
            ticker=str(request_payload.get("ticker") or ""),
            shares=request_payload.get("shares"),
            entry_price=request_payload.get("entry_price"),
            opened_at=request_payload.get("opened_at"),
            notes=str(request_payload.get("notes") or ""),
            portfolio_name=str(request_payload.get("portfolio_name") or ""),
            actor_user_id=principal.user_id,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    _record_audit(
        audit_service=audit_service,
        principal=principal,
        request=request,
        action="portfolio.position.create",
        resource_type="portfolio_position",
        resource_id=str(position.get("id") or ""),
        resource_label=str(position.get("ticker") or ""),
        message=f"Added portfolio position for {position.get('ticker')}.",
        metadata=position,
    )
    return JSONResponse({"ok": True, "position": position})


@router.post("/admin/portfolio/positions/import", response_class=JSONResponse)
def admin_import_portfolio_positions(
    request: Request,
    payload: dict[str, object] | None = Body(default=None),
    service: PortfolioService = Depends(get_portfolio_service),
    principal: Principal = Depends(require_manage_exclusions),
    audit_service: AuditService = Depends(get_audit_service),
) -> JSONResponse:
    request_payload = payload or {}
    try:
        result = service.import_csv(
            csv_text=str(request_payload.get("csv_text") or ""),
            portfolio_name=str(request_payload.get("portfolio_name") or ""),
            actor_user_id=principal.user_id,
            source_name=str(request_payload.get("source_name") or "portfolio.csv"),
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    _record_audit(
        audit_service=audit_service,
        principal=principal,
        request=request,
        action="portfolio.import",
        resource_type="portfolio_import_batch",
        resource_id=str(result.get("import_batch_id") or ""),
        resource_label=str(result.get("portfolio_name") or "portfolio"),
        message=f"Imported {result.get('accepted_count')} portfolio row(s).",
        metadata={
            "portfolio_name": result.get("portfolio_name"),
            "accepted_count": result.get("accepted_count"),
            "error_count": result.get("error_count"),
            "source_name": str(request_payload.get("source_name") or "portfolio.csv"),
        },
    )
    return JSONResponse(jsonable_encoder(result))


@router.post("/admin/portfolio/advice/refresh", response_class=JSONResponse)
def admin_refresh_portfolio_advice(
    request: Request,
    payload: dict[str, object] | None = Body(default=None),
    service: PortfolioService = Depends(get_portfolio_service),
    principal: Principal = Depends(require_manage_exclusions),
    audit_service: AuditService = Depends(get_audit_service),
) -> JSONResponse:
    request_payload = payload or {}
    position_id = request_payload.get("position_id")
    parsed_position_id = int(position_id) if position_id not in (None, "") else None
    try:
        result = service.refresh_advice(
            position_id=parsed_position_id,
            ticker=str(request_payload.get("ticker") or ""),
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    _record_audit(
        audit_service=audit_service,
        principal=principal,
        request=request,
        action="portfolio.advice.refresh",
        resource_type="portfolio_position",
        resource_id=str(parsed_position_id or request_payload.get("ticker") or "all"),
        resource_label=str(request_payload.get("ticker") or "portfolio"),
        message=f"Refreshed advice for {result.get('refreshed_count')} portfolio position(s).",
        metadata=result,
    )
    return JSONResponse(jsonable_encoder(result))


@router.post("/admin/portfolio/positions/{position_id}", response_class=JSONResponse)
def admin_update_portfolio_position(
    request: Request,
    position_id: int,
    payload: dict[str, object] | None = Body(default=None),
    service: PortfolioService = Depends(get_portfolio_service),
    principal: Principal = Depends(require_manage_exclusions),
    audit_service: AuditService = Depends(get_audit_service),
) -> JSONResponse:
    request_payload = payload or {}
    try:
        position = service.update_position(
            position_id,
            ticker=str(request_payload.get("ticker") or ""),
            shares=request_payload.get("shares"),
            entry_price=request_payload.get("entry_price"),
            opened_at=request_payload.get("opened_at"),
            notes=str(request_payload.get("notes") or ""),
            portfolio_name=str(request_payload.get("portfolio_name") or ""),
            actor_user_id=principal.user_id,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    _record_audit(
        audit_service=audit_service,
        principal=principal,
        request=request,
        action="portfolio.position.update",
        resource_type="portfolio_position",
        resource_id=str(position_id),
        resource_label=str(position.get("ticker") or position_id),
        message=f"Updated portfolio position {position_id}.",
        metadata=position,
    )
    return JSONResponse({"ok": True, "position": position})


@router.post("/admin/portfolio/positions/{position_id}/transactions", response_class=JSONResponse)
def admin_create_portfolio_transaction(
    request: Request,
    position_id: int,
    payload: dict[str, object] | None = Body(default=None),
    service: PortfolioService = Depends(get_portfolio_service),
    principal: Principal = Depends(require_manage_exclusions),
    audit_service: AuditService = Depends(get_audit_service),
) -> JSONResponse:
    request_payload = payload or {}
    try:
        transaction = service.record_transaction(
            position_id,
            side=str(request_payload.get("side") or ""),
            shares=request_payload.get("shares"),
            price=request_payload.get("price"),
            trade_date=request_payload.get("trade_date"),
            fees=request_payload.get("fees"),
            notes=str(request_payload.get("notes") or ""),
            actor_user_id=principal.user_id,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    _record_audit(
        audit_service=audit_service,
        principal=principal,
        request=request,
        action="portfolio.transaction.create",
        resource_type="portfolio_transaction",
        resource_id=str(transaction.get("id") or ""),
        resource_label=str(transaction.get("side") or ""),
        message=f"Recorded {transaction.get('side')} transaction for position {position_id}.",
        metadata=transaction,
    )
    return JSONResponse({"ok": True, "transaction": transaction})


@router.post("/admin/portfolio/positions/{position_id}/delete", response_class=JSONResponse)
def admin_delete_portfolio_position(
    request: Request,
    position_id: int,
    service: PortfolioService = Depends(get_portfolio_service),
    principal: Principal = Depends(require_manage_exclusions),
    audit_service: AuditService = Depends(get_audit_service),
) -> JSONResponse:
    try:
        service.delete_position(position_id)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    _record_audit(
        audit_service=audit_service,
        principal=principal,
        request=request,
        action="portfolio.position.delete",
        resource_type="portfolio_position",
        resource_id=str(position_id),
        resource_label=str(position_id),
        message=f"Deleted portfolio position {position_id}.",
        metadata={"position_id": position_id},
    )
    return JSONResponse({"ok": True})


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
        message=f"Saved or updated user {user.get('email')}.",
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
