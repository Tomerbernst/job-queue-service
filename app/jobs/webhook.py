"""Webhook job: simulates an external call. 80% success / 20% simulated
failure to exercise the retry path. The URL is never actually called."""
import random

from app.jobs.registry import JobContext, JobFailure, job_handler
from app.schemas import WebhookPayload

FAILURE_RATE = 0.2


@job_handler("webhook", payload_model=WebhookPayload, timeout=15.0)
async def call_webhook(ctx: JobContext) -> dict:
    payload = WebhookPayload.model_validate(ctx.payload)
    await ctx.log("info", "calling webhook", url=payload.url, event=payload.event)
    await ctx.sleep(random.uniform(1.0, 2.0))

    failed = payload.fail_always or (
        not payload.succeed_always and random.random() < FAILURE_RATE
    )
    if failed:
        raise JobFailure(f"simulated webhook failure: 502 Bad Gateway from {payload.url}")

    return {"status_code": 200, "url": payload.url, "event": payload.event}
