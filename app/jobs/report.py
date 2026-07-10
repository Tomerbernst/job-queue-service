"""Report job: simulates report generation (sleep 3-5s, mock file URL)."""
import random
import uuid

from app.jobs.registry import JobContext, job_handler
from app.schemas import ReportPayload


@job_handler("report", payload_model=ReportPayload, timeout=30.0)
async def generate_report(ctx: JobContext) -> dict:
    payload = ReportPayload.model_validate(ctx.payload)
    await ctx.log("info", "generating report", report_type=payload.report_type)
    await ctx.sleep(random.uniform(3.0, 5.0))
    file_id = uuid.uuid4().hex
    return {
        "report_type": payload.report_type,
        "file_url": f"https://storage.mock.local/reports/{file_id}.pdf",
        "size_bytes": random.randint(10_000, 5_000_000),
    }
