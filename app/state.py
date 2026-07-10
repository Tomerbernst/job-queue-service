"""Job state transitions.

Every transition is a compare-and-set UPDATE (``WHERE status = <expected>``),
so concurrent workers / API calls / the reaper can never double-apply a
transition: exactly one racer's UPDATE matches, the rest see rowcount 0.

Used by both the API layer and the worker — and tested independently of
the API.
"""
from datetime import datetime, timedelta

import structlog
from sqlalchemy import func, or_, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import Settings
from app.models import Job, JobLog, JobStatus, utcnow
from app.redis_client import JobQueue

logger = structlog.get_logger()


def backoff_delay(failed_attempts: int, settings: Settings) -> float:
    """Exponential backoff: 30s after attempt 1, 120s after attempt 2 (base 30, factor 4)."""
    delay = settings.retry_base_delay * settings.retry_backoff_factor ** (failed_attempts - 1)
    return min(delay, settings.retry_max_delay)


async def add_job_log(
    session: AsyncSession,
    job_id: str,
    level: str,
    message: str,
    metadata: dict | None = None,
) -> None:
    session.add(JobLog(job_id=job_id, level=level, message=message, meta=metadata))


async def claim_job(
    session: AsyncSession, job_id: str, worker_id: str, now: datetime | None = None
) -> Job | None:
    """Claim a job for processing. Returns the claimed Job or None.

    Accepts PENDING, and also SCHEDULED-and-due (covers the sweep's
    enqueue-then-flip window). Cancelled/processing/completed jobs whose
    id is still in Redis are skipped here — the CAS is the second guard
    behind the atomic BZPOPMAX.
    """
    now = now or utcnow()
    result = await session.execute(
        update(Job)
        .where(
            Job.id == job_id,
            Job.status.in_([JobStatus.PENDING, JobStatus.SCHEDULED]),
            or_(Job.scheduled_at.is_(None), Job.scheduled_at <= now),
        )
        .values(
            status=JobStatus.PROCESSING,
            attempts=Job.attempts + 1,
            worker_id=worker_id,
            started_at=now,
            heartbeat_at=now,
        )
        .returning(Job)
    )
    job = result.scalar_one_or_none()
    if job is not None:
        await add_job_log(
            session, job_id, "info", "job claimed",
            {"worker_id": worker_id, "attempt": job.attempts},
        )
    return job


async def complete_job(
    session: AsyncSession, job_id: str, worker_id: str, result: dict | None
) -> bool:
    res = await session.execute(
        update(Job)
        .where(
            Job.id == job_id,
            Job.status == JobStatus.PROCESSING,
            Job.worker_id == worker_id,
        )
        .values(
            status=JobStatus.COMPLETED,
            result=result,
            progress=100,
            completed_at=utcnow(),
        )
    )
    if res.rowcount:
        await add_job_log(session, job_id, "info", "job completed", None)
    return bool(res.rowcount)


async def fail_job(
    session: AsyncSession,
    queue: JobQueue,
    job: Job,
    error: dict,
    settings: Settings,
) -> JobStatus:
    """Handle a failed attempt: schedule a retry with backoff, or mark
    permanently FAILED (and dead-letter) once attempts are exhausted.
    Returns the resulting status."""
    if job.attempts >= job.max_attempts:
        res = await session.execute(
            update(Job)
            .where(Job.id == job.id, Job.status == JobStatus.PROCESSING)
            .values(status=JobStatus.FAILED, error=error, completed_at=utcnow())
        )
        if res.rowcount:
            await add_job_log(
                session, job.id, "error",
                f"job failed permanently after {job.attempts} attempts", error,
            )
            await queue.dlq_push(
                {"job_id": job.id, "type": job.type, "error": error,
                 "attempts": job.attempts, "failed_at": utcnow().isoformat()}
            )
        return JobStatus.FAILED

    delay = backoff_delay(job.attempts, settings)
    retry_at = utcnow() + timedelta(seconds=delay)
    res = await session.execute(
        update(Job)
        .where(Job.id == job.id, Job.status == JobStatus.PROCESSING)
        .values(status=JobStatus.SCHEDULED, scheduled_at=retry_at, error=error)
    )
    if res.rowcount:
        await add_job_log(
            session, job.id, "warning",
            f"attempt {job.attempts} failed, retry in {delay:.0f}s", error,
        )
    return JobStatus.SCHEDULED


async def cancel_job(session: AsyncSession, job_id: str) -> bool:
    """Cancel a job that has not started. The id may still sit in Redis;
    the worker's claim CAS skips cancelled jobs."""
    res = await session.execute(
        update(Job)
        .where(
            Job.id == job_id,
            Job.status.in_([JobStatus.PENDING, JobStatus.SCHEDULED]),
        )
        .values(status=JobStatus.CANCELLED, completed_at=utcnow())
    )
    if res.rowcount:
        await add_job_log(session, job_id, "info", "job cancelled", None)
    return bool(res.rowcount)


async def retry_job(session: AsyncSession, job_id: str) -> Job | None:
    """Manual retry of a permanently FAILED job: fresh attempt budget."""
    res = await session.execute(
        update(Job)
        .where(Job.id == job_id, Job.status == JobStatus.FAILED)
        .values(
            status=JobStatus.PENDING,
            attempts=0,
            error=None,
            result=None,
            progress=0,
            scheduled_at=None,
            started_at=None,
            completed_at=None,
            worker_id=None,
        )
        .returning(Job)
    )
    job = res.scalar_one_or_none()
    if job is not None:
        await add_job_log(session, job_id, "info", "manual retry requested", None)
    return job


async def set_progress(session: AsyncSession, job_id: str, progress: int) -> None:
    await session.execute(
        update(Job)
        .where(Job.id == job_id, Job.status == JobStatus.PROCESSING)
        .values(progress=max(0, min(100, progress)))
    )


async def touch_heartbeat(session: AsyncSession, job_id: str, worker_id: str) -> None:
    await session.execute(
        update(Job)
        .where(
            Job.id == job_id,
            Job.status == JobStatus.PROCESSING,
            Job.worker_id == worker_id,
        )
        .values(heartbeat_at=utcnow())
    )


# --- maintenance sweep operations -------------------------------------------

async def promote_due_jobs(
    session: AsyncSession, queue: JobQueue, now: datetime | None = None, limit: int = 200
) -> int:
    """Move due SCHEDULED jobs (future-dated submissions AND backoff
    retries) into the Redis queue.

    Enqueue happens *before* the status flip: if we crash in between, the
    job is claimable anyway (claim accepts scheduled-and-due) and a later
    sweep converges. Duplicate ZADDs are harmless — a ZSET member is
    delivered once.
    """
    now = now or utcnow()
    rows = (
        await session.execute(
            select(Job.id, Job.priority, Job.created_at)
            .where(Job.status == JobStatus.SCHEDULED, Job.scheduled_at <= now)
            .order_by(Job.scheduled_at)
            .limit(limit)
        )
    ).all()
    if not rows:
        return 0

    for job_id, priority, created_at in rows:
        await queue.enqueue(job_id, priority, created_at)

    await session.execute(
        update(Job)
        .where(Job.id.in_([r[0] for r in rows]), Job.status == JobStatus.SCHEDULED)
        .values(status=JobStatus.PENDING)
    )
    return len(rows)


async def requeue_orphaned_pending(
    session: AsyncSession,
    queue: JobQueue,
    settings: Settings,
    now: datetime | None = None,
    limit: int = 200,
) -> int:
    """Re-enqueue PENDING jobs whose id is missing from the Redis queue.

    A job can be PENDING in Postgres yet absent from the queue if a worker
    died between ``BZPOPMAX`` (which removes the id) and committing the
    claim, if the API died between committing the row and enqueueing it, or
    if Redis lost data. Nothing else recovers these — the reaper only looks
    at PROCESSING rows and the scheduler only at SCHEDULED rows.

    A grace period (``stale_after``) avoids racing a fresh submit whose
    ``queue.enqueue`` is milliseconds behind its commit. Re-enqueue is safe
    regardless: ``enqueue`` is an idempotent ZADD (delivered once) and the
    claim is a CAS, so a double-enqueue can never cause double processing.
    """
    now = now or utcnow()
    cutoff = now - timedelta(seconds=settings.stale_after)
    candidates = (
        await session.execute(
            select(Job.id, Job.priority, Job.created_at)
            .where(
                Job.status == JobStatus.PENDING,
                Job.created_at < cutoff,
                or_(Job.scheduled_at.is_(None), Job.scheduled_at <= now),
            )
            .order_by(Job.created_at)
            .limit(limit)
        )
    ).all()

    requeued = 0
    for job_id, priority, created_at in candidates:
        if not await queue.is_queued(job_id):
            await queue.enqueue(job_id, priority, created_at)
            await add_job_log(
                session, job_id, "warning",
                "re-enqueued orphaned pending job (missing from queue)", None,
            )
            logger.warning("re-enqueued orphaned pending job", job_id=job_id)
            requeued += 1
    return requeued


async def reap_stale_jobs(
    session: AsyncSession, queue: JobQueue, settings: Settings, now: datetime | None = None
) -> int:
    """Recover jobs whose worker died mid-processing.

    A PROCESSING job whose heartbeat is older than ``stale_after`` is
    presumed crashed: retried with backoff if attempts remain, otherwise
    failed permanently and dead-lettered.
    """
    now = now or utcnow()
    threshold = now - timedelta(seconds=settings.stale_after)
    stale = (
        (
            await session.execute(
                select(Job).where(
                    Job.status == JobStatus.PROCESSING, Job.heartbeat_at < threshold
                )
            )
        )
        .scalars()
        .all()
    )
    recovered = 0
    for job in stale:
        error = {
            "type": "WorkerCrash",
            "message": f"worker {job.worker_id} stopped heartbeating; presumed crashed",
        }
        new_status = await fail_job(session, queue, job, error, settings)
        logger.warning(
            "reaped stale job", job_id=job.id, job_type=job.type,
            dead_worker=job.worker_id, new_status=new_status.value,
        )
        recovered += 1
    return recovered


async def status_counts(session: AsyncSession) -> dict[str, int]:
    rows = (
        await session.execute(select(Job.status, func.count()).group_by(Job.status))
    ).all()
    counts = {s.value: 0 for s in JobStatus}
    for status, count in rows:
        counts[status.value] = count
    return counts
