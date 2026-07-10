"""Test fixtures.

Fully isolated from external services: SQLite (in-memory, async) stands in
for Postgres and fakeredis for Redis. All state transitions are plain
SQL/redis-protocol operations, so the logic under test is identical.
"""
import os
import tempfile

import fakeredis.aioredis
import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient
from sqlalchemy.ext.asyncio import create_async_engine

import app.jobs  # noqa: F401  (registers job types)
from app.api.main import create_app
from app.config import Settings
from app.db import create_session_factory
from app.models import Base
from app.redis_client import JobQueue
from app.worker.worker import Worker


@pytest_asyncio.fixture
async def engine():
    """Temp-file SQLite in WAL mode.

    A file (not in-memory StaticPool) so each concurrent session gets its
    own connection — the worker runs its heartbeat loop and job flow as
    overlapping sessions, which a single shared connection cannot serve.
    WAL + a busy timeout let those connections write concurrently without
    "database is locked". (Production uses Postgres, which needs none of
    this; it is purely a test-harness accommodation for SQLite.)
    """
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    engine = create_async_engine(
        f"sqlite+aiosqlite:///{path}",
        connect_args={"timeout": 30},
    )
    async with engine.begin() as conn:
        await conn.exec_driver_sql("PRAGMA journal_mode=WAL")
        await conn.run_sync(Base.metadata.create_all)
    yield engine
    await engine.dispose()
    for suffix in ("", "-wal", "-shm"):
        try:
            os.unlink(path + suffix)
        except OSError:
            pass


@pytest_asyncio.fixture
async def session_factory(engine):
    return create_session_factory(engine)


@pytest_asyncio.fixture
async def queue():
    redis = fakeredis.aioredis.FakeRedis(decode_responses=True)
    yield JobQueue(redis)
    await redis.aclose()


@pytest.fixture
def settings():
    return Settings(
        database_url="unused-in-tests",
        redis_url="unused-in-tests",
        job_speed_factor=0.0,      # simulated sleeps become ~0
        retry_base_delay=0.05,     # fast backoff so retry tests run quickly
        retry_backoff_factor=1.0,
        heartbeat_interval=0.05,
        stale_after=0.5,
        dequeue_timeout=0.1,
        maintenance_interval=0.02,
        worker_concurrency=2,
        worker_ttl=5.0,
    )


@pytest.fixture
def worker(session_factory, queue, settings):
    return Worker(session_factory, queue, settings, worker_id="test-worker")


@pytest_asyncio.fixture
async def api_client(session_factory, queue, settings):
    application = create_app(
        settings=settings,
        session_factory=session_factory,
        queue=queue,
        manage_resources=False,
    )
    transport = ASGITransport(app=application)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        yield client


async def drain_queue(worker: Worker, queue: JobQueue, max_jobs: int = 50) -> int:
    """Process everything currently in the queue with the given worker."""
    processed = 0
    while processed < max_jobs:
        job_id = await queue.dequeue(timeout=0.05)
        if job_id is None:
            break
        await worker.process_job(job_id)
        processed += 1
    return processed
