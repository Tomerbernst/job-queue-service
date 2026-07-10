"""Worker entrypoint: python -m app.worker.main"""
import asyncio
import signal

from sqlalchemy import select

import app.jobs  # noqa: F401  (registers job types)
from app.config import get_settings
from app.db import create_engine, create_session_factory
from app.logging import configure_logging, get_logger
from app.models import Job
from app.redis_client import JobQueue, create_redis
from app.worker.worker import Worker


async def wait_for_schema(session_factory, logger, attempts: int = 60) -> None:
    """The API container owns table creation; wait until it's done."""
    for _ in range(attempts):
        try:
            async with session_factory() as session:
                await session.execute(select(Job.id).limit(1))
            return
        except Exception:
            logger.info("waiting for database schema...")
            await asyncio.sleep(2)
    raise RuntimeError("database schema never became available")


async def main() -> None:
    configure_logging()
    settings = get_settings()
    logger = get_logger(service="worker")

    engine = create_engine(settings.database_url)
    session_factory = create_session_factory(engine)
    redis = create_redis(settings.redis_url)
    queue = JobQueue(redis)

    await wait_for_schema(session_factory, logger)

    worker = Worker(session_factory, queue, settings)

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, worker.request_shutdown)
        except NotImplementedError:  # Windows / non-main loop fallback
            signal.signal(sig, lambda *_: worker.request_shutdown())

    try:
        await worker.run()
    finally:
        await redis.aclose()
        await engine.dispose()


if __name__ == "__main__":
    asyncio.run(main())
