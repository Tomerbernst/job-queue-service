"""Worker: pulls job ids from Redis, claims them in Postgres, executes
handlers with heartbeats + timeout, and finalizes state.

Concurrency model: one process runs `worker_concurrency` consumer tasks
on a single asyncio loop (jobs here are IO-bound sleeps). Scale out with
more worker containers (`docker compose up --scale worker=N`).
"""
import asyncio
import contextlib
import traceback
import uuid

import structlog
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app import state
from app.config import Settings
from app.jobs.registry import JobContext, JobFailure, get_job_type
from app.models import Job
from app.redis_client import JobQueue


class Worker:
    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        queue: JobQueue,
        settings: Settings,
        worker_id: str | None = None,
    ):
        self.session_factory = session_factory
        self.queue = queue
        self.settings = settings
        self.worker_id = worker_id or f"worker-{uuid.uuid4().hex[:8]}"
        self.logger = structlog.get_logger().bind(worker_id=self.worker_id)
        self._stop = asyncio.Event()

    # --- lifecycle -----------------------------------------------------

    def request_shutdown(self) -> None:
        """Graceful shutdown: consumers stop dequeuing and finish their
        current job before exiting."""
        self.logger.info("shutdown requested, finishing in-flight jobs")
        self._stop.set()

    async def run(self) -> None:
        self.logger.info(
            "worker starting", concurrency=self.settings.worker_concurrency
        )
        consumers = [
            asyncio.create_task(self._consume_loop(i))
            for i in range(self.settings.worker_concurrency)
        ]
        maintenance = asyncio.create_task(self._maintenance_loop())
        await asyncio.gather(*consumers)
        maintenance.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await maintenance
        await self.queue.worker_deregister(self.worker_id)
        self.logger.info("worker stopped")

    # --- consuming -----------------------------------------------------

    async def _consume_loop(self, slot: int) -> None:
        while not self._stop.is_set():
            try:
                job_id = await self.queue.dequeue(timeout=self.settings.dequeue_timeout)
            except Exception:
                self.logger.exception("dequeue error, backing off")
                await asyncio.sleep(1)
                continue
            if job_id is None:
                continue
            try:
                await self.process_job(job_id)
            except Exception:
                # never let one bad job kill the consumer loop
                self.logger.exception("unexpected error processing job", job_id=job_id)

    async def process_job(self, job_id: str) -> None:
        """Claim → execute (with heartbeat + timeout) → finalize."""
        async with self.session_factory() as session:
            job = await state.claim_job(session, job_id, self.worker_id)
            await session.commit()
        if job is None:
            # already cancelled / claimed by someone else / not due — the
            # CAS guard behind the atomic pop
            self.logger.info("skipped unclaimable job", job_id=job_id)
            return

        log = self.logger.bind(job_id=job.id, job_type=job.type, attempt=job.attempts)
        log.info("job started")

        job_type = get_job_type(job.type)
        if job_type is None:
            # poison message: unknown type slipped past API validation
            await self._finalize_failure(
                job, {"type": "UnknownJobType", "message": f"no handler for '{job.type}'"}
            )
            return

        heartbeat = asyncio.create_task(self._heartbeat_loop(job.id))
        try:
            ctx = self._make_context(job)
            result = await asyncio.wait_for(
                job_type.handler(ctx), timeout=job_type.timeout
            )
        except asyncio.TimeoutError:
            await self._finalize_failure(
                job,
                {"type": "JobTimeout",
                 "message": f"exceeded {job_type.timeout}s timeout"},
            )
            log.warning("job timed out")
        except JobFailure as exc:
            await self._finalize_failure(job, {"type": "JobFailure", "message": str(exc)})
            log.warning("job failed", error=str(exc))
        except Exception as exc:
            await self._finalize_failure(
                job,
                {"type": exc.__class__.__name__, "message": str(exc),
                 "traceback": traceback.format_exc(limit=10)},
            )
            log.error("job crashed", error=str(exc))
        else:
            async with self.session_factory() as session:
                ok = await state.complete_job(session, job.id, self.worker_id, result)
                await session.commit()
            if ok:
                log.info("job completed")
            else:
                # e.g. the reaper re-scheduled us while we were slow —
                # at-least-once semantics, documented in DECISIONS.md
                log.warning("completion CAS lost; job was re-owned elsewhere")
        finally:
            heartbeat.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await heartbeat

    async def _finalize_failure(self, job: Job, error: dict) -> None:
        async with self.session_factory() as session:
            await state.fail_job(session, self.queue, job, error, self.settings)
            await session.commit()

    def _make_context(self, job: Job) -> JobContext:
        async def set_progress(progress: int) -> None:
            async with self.session_factory() as session:
                await state.set_progress(session, job.id, progress)
                await session.commit()

        async def log(level: str, message: str, **metadata) -> None:
            # metadata goes under a nested key so a handler field named
            # e.g. "event" can't collide with structlog's reserved keys
            emit = getattr(self.logger, level, self.logger.info)
            emit(message, job_id=job.id, job_type=job.type, meta=metadata or None)
            async with self.session_factory() as session:
                await state.add_job_log(session, job.id, level, message, metadata or None)
                await session.commit()

        return JobContext(
            job_id=job.id,
            payload=job.payload,
            attempt=job.attempts,
            settings=self.settings,
            set_progress=set_progress,
            log=log,
        )

    async def _heartbeat_loop(self, job_id: str) -> None:
        while True:
            await asyncio.sleep(self.settings.heartbeat_interval)
            try:
                async with self.session_factory() as session:
                    await state.touch_heartbeat(session, job_id, self.worker_id)
                    await session.commit()
            except asyncio.CancelledError:
                raise
            except Exception:
                # a transient DB blip must not stop heartbeats — that would
                # let the reaper presume this live job crashed and re-run it
                self.logger.warning("heartbeat update failed, will retry", job_id=job_id)

    # --- maintenance (scheduler + reaper) --------------------------------

    async def run_maintenance_once(self) -> dict:
        """One sweep: promote due scheduled jobs, reap crashed workers'
        jobs. Safe to run from any number of workers (all ops are CAS);
        the Redis lock merely elects one to avoid redundant sweeps."""
        async with self.session_factory() as session:
            promoted = await state.promote_due_jobs(session, self.queue)
            reaped = await state.reap_stale_jobs(session, self.queue, self.settings)
            requeued = await state.requeue_orphaned_pending(
                session, self.queue, self.settings
            )
            await session.commit()
        return {"promoted": promoted, "reaped": reaped, "requeued": requeued}

    async def _maintenance_loop(self) -> None:
        while True:
            try:
                await self.queue.worker_heartbeat(self.worker_id)
                if await self.queue.try_acquire_maintenance_lock(
                    self.worker_id, self.settings.worker_ttl
                ):
                    stats = await self.run_maintenance_once()
                    if any(stats.values()):
                        self.logger.info("maintenance sweep", **stats)
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger.exception("maintenance sweep failed")
            await asyncio.sleep(self.settings.maintenance_interval)
