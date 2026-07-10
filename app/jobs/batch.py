"""Batch job: processes items one by one, tracking progress percentage."""
import random

from app.jobs.registry import JobContext, job_handler
from app.schemas import BatchPayload


@job_handler("batch", payload_model=BatchPayload, timeout=300.0)
async def process_batch(ctx: JobContext) -> dict:
    payload = BatchPayload.model_validate(ctx.payload)
    total = len(payload.items)
    succeeded = 0
    failed = 0
    last_reported = -1

    await ctx.log("info", "batch started", total_items=total)
    for i, _item in enumerate(payload.items):
        await ctx.sleep(random.uniform(0.05, 0.2))
        succeeded += 1

        progress = int((i + 1) / total * 100)
        if progress != last_reported:  # avoid a DB write per item at same %
            await ctx.set_progress(progress)
            last_reported = progress

    return {"total": total, "succeeded": succeeded, "failed": failed}
