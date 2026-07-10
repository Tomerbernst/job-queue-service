"""Async database engine/session helpers."""
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from app.models import Base


def create_engine(database_url: str) -> AsyncEngine:
    return create_async_engine(database_url, pool_pre_ping=True)


def create_session_factory(engine: AsyncEngine) -> async_sessionmaker[AsyncSession]:
    return async_sessionmaker(engine, expire_on_commit=False)


async def init_db(engine: AsyncEngine) -> None:
    """Create tables if missing.

    Kept deliberately simple (no Alembic) for assignment scope; the API
    container owns schema creation and the worker waits for it.
    """
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
