"""Pydantic schemas: API contracts and strict per-job-type payload validation.

Payload models use extra="forbid" plus field-level constraints so untrusted
payload data is rejected at the API boundary — malformed jobs never reach
the queue (queue-poisoning defence).
"""
import re
from datetime import datetime, timezone
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator

_EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
_URL_RE = re.compile(r"^https?://[^\s]+$")


# --- per-job-type payloads -------------------------------------------------

class EmailPayload(BaseModel):
    model_config = ConfigDict(extra="forbid")

    to: str = Field(max_length=254)
    subject: str = Field(max_length=200)
    body: str = Field(default="", max_length=10_000)

    @field_validator("to")
    @classmethod
    def _valid_email(cls, v: str) -> str:
        if not _EMAIL_RE.match(v):
            raise ValueError("invalid email address")
        return v


class WebhookPayload(BaseModel):
    model_config = ConfigDict(extra="forbid")

    url: str = Field(max_length=2000)
    event: str = Field(max_length=100)
    data: dict[str, Any] = Field(default_factory=dict)
    # test hooks for deterministic failure-path testing
    fail_always: bool = False
    succeed_always: bool = False

    @field_validator("url")
    @classmethod
    def _valid_url(cls, v: str) -> str:
        if not _URL_RE.match(v):
            raise ValueError("invalid http(s) URL")
        return v


class ReportPayload(BaseModel):
    model_config = ConfigDict(extra="forbid")

    report_type: str = Field(max_length=100)
    params: dict[str, Any] = Field(default_factory=dict)


class BatchPayload(BaseModel):
    model_config = ConfigDict(extra="forbid")

    items: list[Any] = Field(min_length=1, max_length=1000)


# --- API contracts -----------------------------------------------------------

class JobSubmit(BaseModel):
    model_config = ConfigDict(extra="forbid")

    type: str
    payload: dict[str, Any] = Field(default_factory=dict)
    priority: int = Field(default=0, ge=0, le=100)
    scheduled_at: datetime | None = None
    max_attempts: int = Field(default=3, ge=1, le=10)
    idempotency_key: str | None = Field(default=None, min_length=1, max_length=255)

    @field_validator("scheduled_at")
    @classmethod
    def _tz_aware(cls, v: datetime | None) -> datetime | None:
        if v is not None and v.tzinfo is None:
            v = v.replace(tzinfo=timezone.utc)
        return v


class JobResponse(BaseModel):
    id: str
    type: str
    payload: dict[str, Any]
    status: str
    priority: int
    attempts: int
    max_attempts: int
    error: dict[str, Any] | None
    progress: int
    result: dict[str, Any] | None
    idempotency_key: str | None
    worker_id: str | None
    scheduled_at: str | None
    created_at: str | None
    started_at: str | None
    completed_at: str | None


class JobListResponse(BaseModel):
    jobs: list[JobResponse]
    total: int
    limit: int
    offset: int
