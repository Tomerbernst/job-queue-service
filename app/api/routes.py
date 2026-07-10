"""API routes: submit, status, list, cancel, retry, logs, health."""
import json
from typing import Annotated

import structlog
from fastapi import APIRouter, Body, Depends, HTTPException, Query, Request, Response
from pydantic import ValidationError
from sqlalchemy import func, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from app import state
from app.jobs.registry import JOB_TYPES, get_job_type
from app.models import Job, JobLog, JobStatus, new_id, utcnow
from app.schemas import JobListResponse, JobResponse, JobSubmit

router = APIRouter()
logger = structlog.get_logger()

# Ready-to-use request bodies shown as a dropdown in the /docs "Try it out"
# panel, one per job type — so the payload shape for each type is discoverable
# without reading the source. (The `payload` field is a per-type object
# validated server-side, so it can't be inferred by Swagger automatically.)
SUBMIT_EXAMPLES = {
    "email": {
        "summary": "Email job",
        "value": {
            "type": "email",
            "payload": {"to": "user@example.com", "subject": "Welcome", "body": "Hello"},
        },
    },
    "webhook": {
        "summary": "Webhook job (80% success, 20% simulated failure)",
        "value": {
            "type": "webhook",
            "payload": {"url": "https://example.com/hook", "event": "order.created"},
        },
    },
    "report": {
        "summary": "Report job (returns a mock file URL)",
        "value": {
            "type": "report",
            "payload": {"report_type": "monthly_sales", "params": {"month": "2026-07"}},
        },
    },
    "batch": {
        "summary": "Batch job (tracks progress percentage)",
        "value": {"type": "batch", "payload": {"items": [1, 2, 3, 4, 5]}},
    },
    "with_options": {
        "summary": "With priority, retries and idempotency key",
        "value": {
            "type": "email",
            "payload": {"to": "a@b.co", "subject": "urgent"},
            "priority": 10,
            "max_attempts": 5,
            "idempotency_key": "order-42",
        },
    },
    "scheduled": {
        "summary": "Scheduled for future execution",
        "value": {
            "type": "report",
            "payload": {"report_type": "eod"},
            "scheduled_at": "2030-01-01T00:00:00Z",
        },
    },
}


def get_session_factory(request: Request):
    return request.app.state.session_factory


def get_queue(request: Request):
    return request.app.state.queue


def get_settings_dep(request: Request):
    return request.app.state.settings


async def _get_job_or_404(session: AsyncSession, job_id: str) -> Job:
    job = await session.get(Job, job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="job not found")
    return job


async def _existing_by_idempotency_key(session: AsyncSession, key: str) -> Job | None:
    return (
        await session.execute(select(Job).where(Job.idempotency_key == key))
    ).scalar_one_or_none()


@router.post("/jobs", status_code=201, response_model=JobResponse)
async def submit_job(
    body: Annotated[JobSubmit, Body(openapi_examples=SUBMIT_EXAMPLES)],
    response: Response,
    session_factory=Depends(get_session_factory),
    queue=Depends(get_queue),
    settings=Depends(get_settings_dep),
):
    """Submit a job for background processing.

    `type` must be one of: email, webhook, report, batch. `payload` is
    validated against that type's schema (see the examples dropdown). Optional:
    `priority` (higher runs first), `scheduled_at` (future execution),
    `max_attempts`, and `idempotency_key` (resubmitting the same key returns the
    existing job instead of creating a duplicate).
    """
    job_type = get_job_type(body.type)
    if job_type is None:
        raise HTTPException(
            status_code=422,
            detail=f"unknown job type '{body.type}'; known types: {sorted(JOB_TYPES)}",
        )

    # strict per-type payload validation — malformed jobs never reach the queue
    try:
        validated = job_type.payload_model.model_validate(body.payload)
    except ValidationError as exc:
        raise HTTPException(status_code=422, detail=json.loads(exc.json())) from exc
    payload = validated.model_dump(mode="json")
    if len(json.dumps(payload).encode("utf-8")) > settings.max_payload_bytes:
        raise HTTPException(status_code=413, detail="payload too large")

    async with session_factory() as session:
        # idempotency fast path
        if body.idempotency_key:
            existing = await _existing_by_idempotency_key(session, body.idempotency_key)
            if existing is not None:
                response.status_code = 200
                return JobResponse(**existing.to_dict())

        now = utcnow()
        is_future = body.scheduled_at is not None and body.scheduled_at > now
        job = Job(
            id=new_id(),
            type=body.type,
            payload=payload,
            status=JobStatus.SCHEDULED if is_future else JobStatus.PENDING,
            priority=body.priority,
            max_attempts=body.max_attempts,
            scheduled_at=body.scheduled_at,
            idempotency_key=body.idempotency_key,
        )
        session.add(job)
        await state.add_job_log(
            session, job.id, "info", "job submitted",
            {"type": body.type, "priority": body.priority,
             "scheduled": is_future},
        )
        try:
            await session.commit()
        except IntegrityError:
            # two racing submits with the same idempotency key: the unique
            # index is the authoritative guard — return the winner's job. Only
            # the idempotency index can cause this; if there's no key, the
            # violation is something else and must not be swallowed.
            await session.rollback()
            if not body.idempotency_key:
                raise
            existing = await _existing_by_idempotency_key(session, body.idempotency_key)
            if existing is None:
                raise
            response.status_code = 200
            return JobResponse(**existing.to_dict())

        if not is_future:
            await queue.enqueue(job.id, job.priority, job.created_at)

        logger.info(
            "job submitted", job_id=job.id, job_type=job.type,
            priority=job.priority, status=job.status.value,
        )
        return JobResponse(**job.to_dict())


@router.get("/jobs", response_model=JobListResponse)
async def list_jobs(
    status: Annotated[JobStatus | None, Query(description="Filter by status")] = None,
    type: Annotated[str | None, Query(description="Filter by job type, e.g. email")] = None,
    limit: Annotated[int, Query(ge=1, le=200, description="Page size")] = 50,
    offset: Annotated[int, Query(ge=0, description="Rows to skip")] = 0,
    session_factory=Depends(get_session_factory),
):
    """List jobs newest-first, optionally filtered by status and/or type. Paged."""
    async with session_factory() as session:
        query = select(Job)
        count_query = select(func.count()).select_from(Job)
        if status is not None:
            query = query.where(Job.status == status)
            count_query = count_query.where(Job.status == status)
        if type is not None:
            query = query.where(Job.type == type)
            count_query = count_query.where(Job.type == type)

        total = (await session.execute(count_query)).scalar_one()
        jobs = (
            (await session.execute(
                query.order_by(Job.created_at.desc()).limit(limit).offset(offset)
            ))
            .scalars()
            .all()
        )
        return JobListResponse(
            jobs=[JobResponse(**j.to_dict()) for j in jobs],
            total=total, limit=limit, offset=offset,
        )


@router.get("/jobs/{job_id}", response_model=JobResponse)
async def get_job(job_id: str, session_factory=Depends(get_session_factory)):
    """Get one job's current status, result or error. 404 if unknown."""
    async with session_factory() as session:
        job = await _get_job_or_404(session, job_id)
        return JobResponse(**job.to_dict())


@router.get("/jobs/{job_id}/logs")
async def get_job_logs(
    job_id: str,
    limit: Annotated[int, Query(ge=1, le=500, description="Page size")] = 100,
    offset: Annotated[int, Query(ge=0, description="Rows to skip")] = 0,
    session_factory=Depends(get_session_factory),
):
    """Get a job's state-transition log trail (submitted, claimed, completed, ...). Paged."""
    async with session_factory() as session:
        await _get_job_or_404(session, job_id)
        logs = (
            (await session.execute(
                select(JobLog)
                .where(JobLog.job_id == job_id)
                .order_by(JobLog.created_at)
                .limit(limit)
                .offset(offset)
            ))
            .scalars()
            .all()
        )
        return {"job_id": job_id, "logs": [entry.to_dict() for entry in logs]}


@router.post("/jobs/{job_id}/cancel", response_model=JobResponse)
async def cancel_job(job_id: str, session_factory=Depends(get_session_factory)):
    """Cancel a job. Only pending/scheduled jobs can be cancelled (409 otherwise)."""
    async with session_factory() as session:
        job = await _get_job_or_404(session, job_id)
        cancelled = await state.cancel_job(session, job_id)
        if not cancelled:
            raise HTTPException(
                status_code=409,
                detail=f"cannot cancel job in status '{job.status.value}' "
                       "(only pending/scheduled jobs can be cancelled)",
            )
        await session.commit()
        await session.refresh(job)
        logger.info("job cancelled", job_id=job_id, job_type=job.type)
        return JobResponse(**job.to_dict())


@router.post("/jobs/{job_id}/retry", response_model=JobResponse)
async def retry_job(
    job_id: str,
    session_factory=Depends(get_session_factory),
    queue=Depends(get_queue),
):
    """Re-queue a permanently failed job with a fresh attempt budget (409 if not failed)."""
    async with session_factory() as session:
        job = await _get_job_or_404(session, job_id)
        retried = await state.retry_job(session, job_id)
        if retried is None:
            raise HTTPException(
                status_code=409,
                detail=f"cannot retry job in status '{job.status.value}' "
                       "(only failed jobs can be retried)",
            )
        await session.commit()
        await queue.enqueue(retried.id, retried.priority, retried.created_at)
        logger.info("job retried", job_id=job_id, job_type=retried.type)
        return JobResponse(**retried.to_dict())


@router.get("/health")
async def health(
    session_factory=Depends(get_session_factory),
    queue=Depends(get_queue),
    settings=Depends(get_settings_dep),
):
    """Liveness plus operational stats: DB/Redis reachability, queue depth,
    dead-letter size, live workers, and a count of jobs in each status."""
    db_ok = True
    counts: dict = {}
    try:
        async with session_factory() as session:
            counts = await state.status_counts(session)
    except Exception:
        db_ok = False

    redis_ok = await queue.ping()
    body = {
        "status": "ok" if (db_ok and redis_ok) else "degraded",
        "database": "ok" if db_ok else "unavailable",
        "redis": "ok" if redis_ok else "unavailable",
    }
    if redis_ok:
        body["queue"] = {
            "depth": await queue.depth(),
            "dead_letter": await queue.dlq_length(),
        }
        body["workers"] = await queue.active_workers(settings.worker_ttl)
    if db_ok:
        body["jobs"] = counts
    return body
