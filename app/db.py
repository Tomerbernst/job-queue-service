"""Async database engine/session helpers."""
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from app.models import Base


def create_engine(
    database_url: str, pool_size: int | None = None, max_overflow: int | None = None
) -> AsyncEngine:
    # SQLite (tests) ignores pool sizing; only pass it for real backends so a
    # worker's several concurrent sessions per job aren't throttled by the
    # default pool of 5.
    kwargs: dict = {"pool_pre_ping": True}
    if not database_url.startswith("sqlite") and pool_size is not None:
        kwargs["pool_size"] = pool_size
        kwargs["max_overflow"] = max_overflow if max_overflow is not None else pool_size
    return create_async_engine(database_url, **kwargs)


def create_session_factory(engine: AsyncEngine) -> async_sessionmaker[AsyncSession]:
    return async_sessionmaker(engine, expire_on_commit=False)


async def init_db(engine: AsyncEngine) -> None:
    """Create tables if missing.

    Kept deliberately simple (no Alembic) for assignment scope; the API
    container owns schema creation and the worker waits for it.
    """
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
