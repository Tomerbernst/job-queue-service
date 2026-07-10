"""Failure handling: exponential-backoff retries, exhaustion → FAILED + DLQ,
and manual retry of a permanently failed job."""
import asyncio

from app.config import Settings
from app.state import backoff_delay
from tests.conftest import drain_queue

FAILING_WEBHOOK = {
    "type": "webhook",
    "payload": {"url": "https://example.com/hook", "event": "x", "fail_always": True},
}


async def test_failed_attempt_schedules_backoff_retry(api_client, worker, queue):
    resp = await api_client.post("/jobs", json=FAILING_WEBHOOK)
    job_id = resp.json()["id"]

    await drain_queue(worker, queue)

    job = (await api_client.get(f"/jobs/{job_id}")).json()
    assert job["status"] == "scheduled"          # awaiting backoff retry
    assert job["attempts"] == 1
    assert job["error"]["type"] == "JobFailure"
    assert job["scheduled_at"] is not None


async def test_retry_exhaustion_marks_failed_and_dead_letters(api_client, worker, queue):
    resp = await api_client.post("/jobs", json=FAILING_WEBHOOK)
    job_id = resp.json()["id"]

    # attempt → fail → wait out backoff → sweep re-enqueues → repeat
    for _ in range(3):
        await drain_queue(worker, queue)
        await asyncio.sleep(0.06)                # test backoff is 0.05s flat
        await worker.run_maintenance_once()

    job = (await api_client.get(f"/jobs/{job_id}")).json()
    assert job["status"] == "failed"
    assert job["attempts"] == 3
    assert await queue.dlq_length() == 1


async def test_manual_retry_of_failed_job(api_client, worker, queue):
    resp = await api_client.post("/jobs", json=FAILING_WEBHOOK)
    job_id = resp.json()["id"]
    for _ in range(3):
        await drain_queue(worker, queue)
        await asyncio.sleep(0.06)
        await worker.run_maintenance_once()
    assert (await api_client.get(f"/jobs/{job_id}")).json()["status"] == "failed"

    resp = await api_client.post(f"/jobs/{job_id}/retry")
    assert resp.status_code == 200
    job = resp.json()
    assert job["status"] == "pending"
    assert job["attempts"] == 0
    assert job["error"] is None
    assert await queue.depth() == 1               # re-enqueued


async def test_retry_only_allowed_for_failed_jobs(api_client):
    resp = await api_client.post(
        "/jobs", json={"type": "email", "payload": {"to": "a@b.co", "subject": "x"}}
    )
    job_id = resp.json()["id"]
    resp = await api_client.post(f"/jobs/{job_id}/retry")
    assert resp.status_code == 409                # pending, not failed


def test_backoff_timing_matches_spec():
    """Spec example: attempt 1 immediate, retry after ~30s, then ~2min."""
    settings = Settings(retry_base_delay=30, retry_backoff_factor=4)
    assert backoff_delay(1, settings) == 30
    assert backoff_delay(2, settings) == 120
    settings_capped = Settings(retry_base_delay=30, retry_backoff_factor=4, retry_max_delay=60)
    assert backoff_delay(2, settings_capped) == 60
