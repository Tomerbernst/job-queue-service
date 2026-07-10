"""Worker logic tested independently of the API.

Exercises claim CAS, crash recovery via stale heartbeat, scheduled-job
promotion, graceful shutdown and no-duplicate-processing — all by driving
the worker/state layer directly.
"""
import asyncio
from datetime import timedelta

from pydantic import BaseModel
from sqlalchemy import update

from app import state
from app.jobs.registry import job_handler
from app.models import Job, JobStatus, new_id, utcnow


class _AnyPayload(BaseModel):
    model_config = {"extra": "allow"}


@job_handler("test_timeout", payload_model=_AnyPayload, timeout=0.05)
async def _slow_handler(ctx):
    await asyncio.sleep(1)  # real sleep, exceeds the 0.05s timeout
    return {}


@job_handler("test_boom", payload_model=_AnyPayload)
async def _boom_handler(ctx):
    raise ValueError("unexpected kaboom")


async def _insert_job(session_factory, **overrides) -> str:
    job_id = new_id()
    async with session_factory() as session:
        job = Job(
            id=job_id,
            type=overrides.pop("type", "email"),
            payload=overrides.pop("payload", {"to": "a@b.co", "subject": "x"}),
            status=overrides.pop("status", JobStatus.PENDING),
            priority=overrides.pop("priority", 0),
            **overrides,
        )
        session.add(job)
        await session.commit()
    return job_id


async def test_claim_is_exclusive(session_factory, worker):
    """Two workers racing on the same id: exactly one claim succeeds (CAS)."""
    job_id = await _insert_job(session_factory)

    async with session_factory() as s1, session_factory() as s2:
        a = await state.claim_job(s1, job_id, "worker-a")
        await s1.commit()
        b = await state.claim_job(s2, job_id, "worker-b")
        await s2.commit()

    assert (a is None) != (b is None)             # exactly one won

    async with session_factory() as session:
        job = await session.get(Job, job_id)
    assert job.status == JobStatus.PROCESSING
    assert job.attempts == 1                       # incremented exactly once


async def test_cancelled_job_cannot_be_claimed(session_factory, worker):
    job_id = await _insert_job(session_factory, status=JobStatus.CANCELLED)
    async with session_factory() as session:
        claimed = await state.claim_job(session, job_id, "worker-a")
        await session.commit()
    assert claimed is None


async def test_crash_recovery_reaps_stale_processing_job(session_factory, queue, worker, settings):
    """A processing job with an old heartbeat is presumed crashed and retried."""
    job_id = await _insert_job(session_factory)
    async with session_factory() as session:
        await state.claim_job(session, job_id, "dead-worker")
        # backdate heartbeat well past the stale threshold
        await session.execute(
            update(Job).where(Job.id == job_id).values(
                heartbeat_at=utcnow() - timedelta(seconds=settings.stale_after + 10)
            )
        )
        await session.commit()

    async with session_factory() as session:
        reaped = await state.reap_stale_jobs(session, queue, settings)
        await session.commit()
    assert reaped == 1

    async with session_factory() as session:
        job = await session.get(Job, job_id)
    # attempts (1) < max (3) → rescheduled for a backoff retry, not failed
    assert job.status == JobStatus.SCHEDULED
    assert job.error["type"] == "WorkerCrash"


async def test_fresh_heartbeat_is_not_reaped(session_factory, queue, worker, settings):
    job_id = await _insert_job(session_factory)
    async with session_factory() as session:
        await state.claim_job(session, job_id, "live-worker")   # heartbeat = now
        await session.commit()

    async with session_factory() as session:
        reaped = await state.reap_stale_jobs(session, queue, settings)
        await session.commit()
    assert reaped == 0


async def test_scheduled_job_promoted_when_due(session_factory, queue, worker):
    past = utcnow() - timedelta(seconds=1)
    future = utcnow() + timedelta(hours=1)
    due_id = await _insert_job(
        session_factory, status=JobStatus.SCHEDULED, scheduled_at=past
    )
    not_due_id = await _insert_job(
        session_factory, status=JobStatus.SCHEDULED, scheduled_at=future
    )

    async with session_factory() as session:
        promoted = await state.promote_due_jobs(session, queue)
        await session.commit()
    assert promoted == 1
    assert await queue.dequeue(timeout=0.1) == due_id

    async with session_factory() as session:
        assert (await session.get(Job, due_id)).status == JobStatus.PENDING
        assert (await session.get(Job, not_due_id)).status == JobStatus.SCHEDULED

    # the scheduled -> pending transition is logged (observability: no invisible hop)
    from sqlalchemy import select

    from app.models import JobLog

    async with session_factory() as session:
        messages = (
            await session.execute(
                select(JobLog.message).where(JobLog.job_id == due_id)
            )
        ).scalars().all()
    assert any("promoted to pending" in m for m in messages)


async def test_graceful_shutdown_finishes_inflight_job(session_factory, queue, worker):
    """run() must drain the in-flight job after a shutdown request."""
    job_id = await _insert_job(session_factory)
    await queue.enqueue(job_id, 0, utcnow())

    async def stop_soon():
        await asyncio.sleep(0.2)
        worker.request_shutdown()

    await asyncio.gather(worker.run(), stop_soon())

    async with session_factory() as session:
        job = await session.get(Job, job_id)
    assert job.status == JobStatus.COMPLETED


async def test_no_duplicate_processing_under_concurrency(session_factory, queue, settings):
    """Many workers, many jobs: each job completes exactly once."""
    from app.worker.worker import Worker

    n = 30
    for _ in range(n):
        job_id = await _insert_job(session_factory)
        await queue.enqueue(job_id, 0, utcnow())

    workers = [Worker(session_factory, queue, settings, worker_id=f"w{i}") for i in range(5)]

    async def drain(w):
        while True:
            job_id = await queue.dequeue(timeout=0.1)
            if job_id is None:
                return
            await w.process_job(job_id)

    await asyncio.gather(*(drain(w) for w in workers))

    async with session_factory() as session:
        from sqlalchemy import func, select

        completed = (
            await session.execute(
                select(func.count()).select_from(Job).where(Job.status == JobStatus.COMPLETED)
            )
        ).scalar_one()
        # each completed job was claimed exactly once → attempts == 1 everywhere
        max_attempts = (
            await session.execute(select(func.max(Job.attempts)))
        ).scalar_one()
    assert completed == n
    assert max_attempts == 1


async def test_job_timeout_is_recorded_and_retried(session_factory, queue, worker):
    """A handler exceeding its type's timeout fails with JobTimeout and, with
    attempts remaining, is scheduled for a retry."""
    job_id = await _insert_job(session_factory, type="test_timeout")
    await worker.process_job(job_id)

    async with session_factory() as session:
        job = await session.get(Job, job_id)
    assert job.status == JobStatus.SCHEDULED
    assert job.error["type"] == "JobTimeout"


async def test_unexpected_exception_captures_traceback(session_factory, queue, worker):
    """A handler raising a bare exception (not JobFailure) is recorded with its
    type and a traceback, then retried."""
    job_id = await _insert_job(session_factory, type="test_boom")
    await worker.process_job(job_id)

    async with session_factory() as session:
        job = await session.get(Job, job_id)
    assert job.status == JobStatus.SCHEDULED
    assert job.error["type"] == "ValueError"
    assert "traceback" in job.error


async def test_unknown_job_type_fails_permanently(session_factory, queue, worker):
    """A job whose type has no handler can never succeed, so it is failed
    permanently and dead-lettered on the first attempt — not retried."""
    job_id = await _insert_job(session_factory, type="ghost")
    await worker.process_job(job_id)

    async with session_factory() as session:
        job = await session.get(Job, job_id)
    assert job.status == JobStatus.FAILED   # not SCHEDULED — no wasted retries
    assert job.attempts == 1
    assert job.error["type"] == "UnknownJobType"
    assert await queue.dlq_length() == 1


async def test_orphaned_pending_is_requeued(session_factory, queue, worker, settings):
    """A PENDING job missing from the queue (worker died between BZPOPMAX and
    the claim commit) is recovered by the sweep and then completes."""
    old = utcnow() - timedelta(seconds=settings.stale_after + 5)
    job_id = await _insert_job(session_factory, created_at=old)
    assert await queue.depth() == 0  # id was popped and lost, never claimed

    async with session_factory() as session:
        requeued = await state.requeue_orphaned_pending(session, queue, settings)
        await session.commit()
    assert requeued == 1
    assert await queue.is_queued(job_id)

    from tests.conftest import drain_queue

    await drain_queue(worker, queue)
    async with session_factory() as session:
        assert (await session.get(Job, job_id)).status == JobStatus.COMPLETED


async def test_fresh_pending_not_prematurely_requeued(session_factory, queue, worker, settings):
    """A just-submitted PENDING job whose enqueue is momentarily behind its
    commit must not be swept (grace period), to avoid churn."""
    job_id = await _insert_job(session_factory)  # created_at = now
    async with session_factory() as session:
        requeued = await state.requeue_orphaned_pending(session, queue, settings)
        await session.commit()
    assert requeued == 0
    assert not await queue.is_queued(job_id)


async def test_requeue_ignores_processing_jobs(session_factory, queue, worker, settings):
    """The sweep only targets PENDING rows; an in-flight PROCESSING job is
    never re-enqueued even though its id is not in the queue."""
    old = utcnow() - timedelta(seconds=settings.stale_after + 5)
    job_id = await _insert_job(session_factory, created_at=old)
    async with session_factory() as session:
        await state.claim_job(session, job_id, "live-worker")
        await session.commit()

    async with session_factory() as session:
        requeued = await state.requeue_orphaned_pending(session, queue, settings)
        await session.commit()
    assert requeued == 0
    assert await queue.depth() == 0
