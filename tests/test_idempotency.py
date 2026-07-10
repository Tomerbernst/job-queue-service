"""Duplicate submissions with the same idempotency key return the existing job."""
from sqlalchemy import func, select

from app.models import Job

EMAIL_JOB = {
    "type": "email",
    "payload": {"to": "a@b.co", "subject": "x"},
    "idempotency_key": "order-1234-confirmation",
}


async def test_duplicate_submission_returns_existing_job(api_client, session_factory):
    first = await api_client.post("/jobs", json=EMAIL_JOB)
    assert first.status_code == 201

    second = await api_client.post("/jobs", json=EMAIL_JOB)
    assert second.status_code == 200              # not created again
    assert second.json()["id"] == first.json()["id"]

    async with session_factory() as session:
        count = (await session.execute(select(func.count()).select_from(Job))).scalar_one()
    assert count == 1


async def test_duplicate_submission_does_not_enqueue_twice(api_client, queue):
    await api_client.post("/jobs", json=EMAIL_JOB)
    await api_client.post("/jobs", json=EMAIL_JOB)
    assert await queue.depth() == 1


async def test_different_keys_create_different_jobs(api_client):
    a = await api_client.post("/jobs", json={**EMAIL_JOB, "idempotency_key": "key-a"})
    b = await api_client.post("/jobs", json={**EMAIL_JOB, "idempotency_key": "key-b"})
    assert a.json()["id"] != b.json()["id"]


async def test_idempotency_key_survives_completion(api_client, worker, queue):
    """Key collision applies for the job's whole lifetime (≥24h requirement:
    rows are never expired), not just while pending."""
    from tests.conftest import drain_queue

    first = await api_client.post("/jobs", json=EMAIL_JOB)
    await drain_queue(worker, queue)

    second = await api_client.post("/jobs", json=EMAIL_JOB)
    assert second.status_code == 200
    assert second.json()["id"] == first.json()["id"]
    assert second.json()["status"] == "completed"
