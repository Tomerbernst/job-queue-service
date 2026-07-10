"""FastAPI app factory. The API owns schema creation on startup."""
from contextlib import asynccontextmanager

from fastapi import FastAPI

import app.jobs as _jobs  # noqa: F401  (import registers job types; alias avoids shadowing the FastAPI `app` defined below)
from app.api.routes import router
from app.config import Settings, get_settings
from app.db import create_engine, create_session_factory, init_db
from app.logging import configure_logging
from app.redis_client import JobQueue, create_redis


def create_app(
    settings: Settings | None = None,
    session_factory=None,
    queue: JobQueue | None = None,
    manage_resources: bool = True,
) -> FastAPI:
    """`session_factory`/`queue` injection keeps tests free of real
    Postgres/Redis; production builds them from settings."""
    settings = settings or get_settings()

    @asynccontextmanager
    async def lifespan(application: FastAPI):
        configure_logging()
        if manage_resources:
            engine = create_engine(settings.database_url)
            await init_db(engine)
            redis = create_redis(settings.redis_url)
            application.state.session_factory = create_session_factory(engine)
            application.state.queue = JobQueue(redis)
            yield
            await redis.aclose()
            await engine.dispose()
        else:
            yield

    application = FastAPI(title="Job Queue Service", version="1.0.0", lifespan=lifespan)
    application.state.settings = settings
    if not manage_resources:
        # injected resources (tests): available immediately, no lifespan needed
        application.state.session_factory = session_factory
        application.state.queue = queue
    application.include_router(router)
    return application


app = create_app()
