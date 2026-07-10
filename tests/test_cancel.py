"""Cancellation of pending/scheduled jobs; terminal jobs cannot be cancelled."""
from tests.conftest import drain_queue

EMAIL_JOB = {"type": "email", "payload": {"to": "a@b.co", "subject": "x"}}


async def test_cancel_pending_job(api_client):
    job_id = (await api_client.post("/jobs", json=EMAIL_JOB)).json()["id"]
    resp = await api_client.post(f"/jobs/{job_id}/cancel")
    assert resp.status_code == 200
    assert resp.json()["status"] == "cancelled"


async def test_cancelled_job_is_never_executed(api_client, worker, queue):
    """The id is still in Redis after cancel — the worker's claim CAS must skip it."""
    job_id = (await api_client.post("/jobs", json=EMAIL_JOB)).json()["id"]
    await api_client.post(f"/jobs/{job_id}/cancel")
    assert await queue.depth() == 1               # still enqueued...

    await drain_queue(worker, queue)              # ...but claim refuses it

    job = (await api_client.get(f"/jobs/{job_id}")).json()
    assert job["status"] == "cancelled"
    assert job["started_at"] is None
    assert job["result"] is None


async def test_cancel_scheduled_job(api_client):
    resp = await api_client.post(
        "/jobs",
        json={**EMAIL_JOB, "scheduled_at": "2099-01-01T00:00:00Z"},
    )
    job = resp.json()
    assert job["status"] == "scheduled"
    resp = await api_client.post(f"/jobs/{job['id']}/cancel")
    assert resp.json()["status"] == "cancelled"


async def test_cannot_cancel_completed_job(api_client, worker, queue):
    job_id = (await api_client.post("/jobs", json=EMAIL_JOB)).json()["id"]
    await drain_queue(worker, queue)
    resp = await api_client.post(f"/jobs/{job_id}/cancel")
    assert resp.status_code == 409
