"""Job-type registry.

Adding a new job type is one decorated async function plus a payload
schema — the API, worker, retry and timeout machinery pick it up
automatically.
"""
import asyncio
from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Type

from pydantic import BaseModel

from app.config import Settings


class JobFailure(Exception):
    """Raised by handlers to signal a (retryable) business failure."""


@dataclass
class JobContext:
    """Everything a handler may touch. Handlers never see the DB session
    for job state — progress/logging go through narrow callbacks."""

    job_id: str
    payload: dict[str, Any]
    attempt: int
    settings: Settings
    set_progress: Callable[[int], Awaitable[None]]
    log: Callable[..., Awaitable[None]]  # (level, message, **metadata)

    async def sleep(self, seconds: float) -> None:
        await asyncio.sleep(seconds * self.settings.job_speed_factor)


JobHandler = Callable[["JobContext"], Awaitable[dict[str, Any]]]

DEFAULT_TIMEOUT = 60.0


@dataclass
class JobTypeDef:
    name: str
    handler: JobHandler
    payload_model: Type[BaseModel]
    timeout: float = DEFAULT_TIMEOUT


JOB_TYPES: dict[str, JobTypeDef] = {}


def job_handler(name: str, payload_model: Type[BaseModel], timeout: float = DEFAULT_TIMEOUT):
    def decorator(fn: JobHandler) -> JobHandler:
        if name in JOB_TYPES:
            raise ValueError(f"duplicate job type registration: {name!r}")
        JOB_TYPES[name] = JobTypeDef(
            name=name, handler=fn, payload_model=payload_model, timeout=timeout
        )
        return fn

    return decorator


def get_job_type(name: str) -> JobTypeDef | None:
    return JOB_TYPES.get(name)
