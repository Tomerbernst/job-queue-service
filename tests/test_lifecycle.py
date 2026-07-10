"""End-to-end completion flow: submit → queue → claim → execute → completed."""
from tests.conftest import drain_queue


async def test_email_job_completes(api_client, worker, queue):
    resp = await api_client.post(
        "/jobs",
        json={"type": "email", "payload": {"to": "a@b.co", "subject": "hi"}},
    )
    job_id = resp.json()["id"]

    assert await drain_queue(worker, queue) == 1

    job = (await api_client.get(f"/jobs/{job_id}")).json()
    assert job["status"] == "completed"
    assert job["attempts"] == 1
    assert job["progress"] == 100
    assert job["result"]["message_id"].startswith("<")
    assert job["started_at"] is not None
    assert job["completed_at"] is not None
    assert job["worker_id"] == "test-worker"

    # observability: state transitions are recorded as job logs
    logs = (await api_client.get(f"/jobs/{job_id}/logs")).json()["logs"]
    messages = [entry["message"] for entry in logs]
    assert "job submitted" in messages
    assert "job claimed" in messages
    assert "job completed" in messages


async def test_report_job_returns_file_url(api_client, worker, queue):
    resp = await api_client.post(
        "/jobs", json={"type": "report", "payload": {"report_type": "sales"}}
    )
    job_id = resp.json()["id"]
    await drain_queue(worker, queue)

    job = (await api_client.get(f"/jobs/{job_id}")).json()
    assert job["status"] == "completed"
    assert job["result"]["file_url"].startswith("https://")


async def test_batch_job_tracks_progress(api_client, worker, queue):
    items = list(range(20))
    resp = await api_client.post("/jobs", json={"type": "batch", "payload": {"items": items}})
    job_id = resp.json()["id"]
    await drain_queue(worker, queue)

    job = (await api_client.get(f"/jobs/{job_id}")).json()
    assert job["status"] == "completed"
    assert job["progress"] == 100
    assert job["result"]["total"] == 20
    assert job["result"]["succeeded"] + job["result"]["failed"] == 20
