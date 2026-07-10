"""Email job: simulates sending an email (sleep 1-3s, mock message id)."""
import random
import uuid

from app.jobs.registry import JobContext, job_handler
from app.schemas import EmailPayload


@job_handler("email", payload_model=EmailPayload, timeout=15.0)
async def send_email(ctx: JobContext) -> dict:
    payload = EmailPayload.model_validate(ctx.payload)
    await ctx.log("info", "sending email", to=payload.to, subject=payload.subject)
    await ctx.sleep(random.uniform(1.0, 3.0))
    message_id = f"<{uuid.uuid4().hex}@mock-mailer.local>"
    return {"message_id": message_id, "to": payload.to}
